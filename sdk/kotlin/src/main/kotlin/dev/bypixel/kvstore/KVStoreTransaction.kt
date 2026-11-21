package dev.bypixel.kvstore

import com.google.protobuf.ByteString
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import kvstore.v1.Kvstore.GetResponse
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

internal class KVStoreLeaseContext(val runtime: KVStoreRuntime, inherited: Map<KVStoreKey, KVStoreLease>) :
    AbstractCoroutineContextElement(Key) {
    val leases = ConcurrentHashMap(inherited)
    companion object Key : CoroutineContext.Key<KVStoreLeaseContext>
}

class KVStoreTransaction internal constructor(
    private val scope: CoroutineScope,
    private val runtime: KVStoreRuntime,
    private val context: KVStoreLeaseContext,
) {
    private sealed interface Mutation {
        data class Set(val value: ByteString, val ttlMs: Long?) : Mutation
        data object Delete : Mutation
    }

    private val touchedLeases = linkedMapOf<KVStoreKey, KVStoreLease>()
    private val ownedLeases = linkedMapOf<KVStoreKey, KVStoreLease>()
    private val observed = linkedMapOf<KVStoreKey, GetResponse>()
    private val staged = linkedMapOf<KVStoreKey, Mutation>()

    internal var committed = false
        private set
    internal var rolledBack = false
        private set

    suspend fun readModifyWrite(
        storeName: String,
        key: String,
        transformer: suspend (String?) -> String,
    ): String {
        val value = transformer(get(storeName, key))
        set(storeName, key, value)
        return value
    }

    suspend fun <T> withKeyLock(storeName: String, key: String, block: suspend () -> T): T {
        lock(KVStoreKey(storeName, key))
        return withContext(context) { block() }
    }

    suspend fun <T> withKeyLocks(keys: Collection<Pair<String, String>>, block: suspend () -> T): T {
        for (ref in keys.map { KVStoreKey(it.first, it.second) }.distinct().sortedBy { it.leaseKey }) {
            lock(ref)
        }
        return withContext(context) { block() }
    }

    suspend fun get(storeName: String, key: String): String? = getBytes(storeName, key)?.toStringUtf8()

    suspend fun getBytes(storeName: String, key: String): ByteString? {
        val ref = KVStoreKey(storeName, key)
        lock(ref)
        return when (val mutation = staged[ref]) {
            is Mutation.Set -> mutation.value
            Mutation.Delete -> null
            null -> read(ref).takeIf { it.found }?.value
        }
    }

    suspend fun set(storeName: String, key: String, value: String, ttlMs: Long? = null) =
        setBytes(storeName, key, ByteString.copyFromUtf8(value), ttlMs)

    suspend fun setBytes(storeName: String, key: String, value: ByteString, ttlMs: Long? = null) {
        require(ttlMs == null || ttlMs > 0) { "ttlMs must be > 0 when specified" }
        val ref = KVStoreKey(storeName, key)
        lock(ref)
        read(ref)
        staged[ref] = Mutation.Set(value, ttlMs)
    }

    suspend fun delete(storeName: String, key: String) {
        val ref = KVStoreKey(storeName, key)
        lock(ref)
        read(ref)
        staged[ref] = Mutation.Delete
    }

    suspend fun commit() {
        requireActive()
        val checks = context.leases.values.map { it.ownerCheck } +
            observed.map { (ref, record) -> revisionCheck(ref, if (record.found) record.revision else 0) }
        val now = System.currentTimeMillis()
        val writes = staged.map { (ref, mutation) ->
            when (mutation) {
                is Mutation.Set -> putValue(ref, mutation.value, mutation.ttlMs?.let { expiresAfter(it, now) } ?: 0)
                Mutation.Delete -> deleteValue(ref)
            }
        }
        try {
            if (checks.isNotEmpty() || writes.isNotEmpty()) {
                if (!runtime.transact(transactionRequest(checks, writes))) {
                    throw TransactionCommitException("KVStore transaction checks no longer hold", emptyList())
                }
            }
        } catch (failure: Throwable) {
            staged.keys.forEach { runtime.cache.invalidate(it) }
            throw failure
        }
        committed = true

        withContext(NonCancellable) {
            var failure: Throwable? = null
            try {
                for ((ref, mutation) in staged) {
                    val deletedRevision = if (mutation == Mutation.Delete) observed[ref]?.revision ?: 0 else 0
                    runtime.cache.refresh(ref, deletedRevision)
                }
            } catch (error: Throwable) {
                staged.keys.forEach { runtime.cache.invalidate(it) }
                failure = error
            }
            try {
                releaseAll()
            } catch (error: Throwable) {
                if (failure == null) failure = error else failure.addSuppressed(error)
            }
            failure?.let { throw TransactionPostCommitSyncException("KVStore transaction committed but local synchronization failed", it) }
        }
    }

    suspend fun rollback() {
        if (committed || rolledBack) return
        rolledBack = true
        staged.clear()
        releaseAll()
    }

    private suspend fun read(ref: KVStoreKey): GetResponse {
        val cached = observed[ref]
        if (cached != null) return cached
        val record = runtime.read(ref.namespace, ref.key)
        observed[ref] = record
        return record
    }

    private suspend fun lock(ref: KVStoreKey) {
        requireActive()
        // The coordinator gate orders dynamically acquired transaction locks.
        if (touchedLeases.isEmpty()) acquire(TRANSACTION_GATE)
        acquire(ref)
    }

    private suspend fun acquire(ref: KVStoreKey) {
        if (ref in touchedLeases) return
        val inherited = context.leases[ref] ?: runtime.inheritedLeases()[ref]
        val lease = inherited ?: KVStoreLease.acquire(runtime, scope, ref).also { ownedLeases[ref] = it }
        lease.requireActive()
        context.leases[ref] = lease
        touchedLeases[ref] = lease
    }

    private suspend fun releaseAll() {
        var failure: Throwable? = null
        for ((ref, lease) in ownedLeases.entries.toList().asReversed()) {
            context.leases.remove(ref, lease)
            try {
                lease.release()
            } catch (error: Throwable) {
                if (failure == null) failure = error else failure.addSuppressed(error)
            }
        }
        ownedLeases.clear()
        touchedLeases.clear()
        failure?.let { throw it }
    }

    private fun requireActive() {
        check(!rolledBack) { "Transaction has been rolled back" }
        check(!committed) { "Transaction has already been committed" }
        context.leases.values.forEach { it.requireActive() }
    }
}

class TransactionPostCommitSyncException(message: String, cause: Throwable) : IllegalStateException(message, cause)
class TransactionCommitException(message: String, val failures: List<Throwable>) :
    IllegalStateException(message, failures.firstOrNull())
