package dev.bypixel.kvstore

import com.google.protobuf.ByteString
import io.grpc.Status
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kvstore.v1.Kvstore.BatchWriteOperation
import kvstore.v1.Kvstore.TransactionCheck
import kvstore.v1.Kvstore.TransactionRequest
import kotlin.coroutines.CoroutineContext
import kotlin.time.TimeSource

internal class KVStoreRuntime(
    val client: KVStoreClient,
    watchContext: CoroutineContext,
    val logError: (String, Throwable) -> Unit,
) {
    private val watchJob = SupervisorJob()
    val watchScope = CoroutineScope(watchContext.minusKey(Job) + watchJob)
    val cache = KVStoreCache(this)

    fun close() {
        watchJob.cancel()
        cache.clear()
    }

    suspend fun inheritedLeases(): Map<KVStoreKey, KVStoreLease> =
        currentCoroutineContext()[KVStoreLeaseContext]?.takeIf { it.runtime === this }?.leases.orEmpty()

    suspend fun transact(request: TransactionRequest): Boolean = unary { client.transact(request) }

    suspend fun read(namespace: String, key: String) = unary { client.read(namespace, key) }

    suspend fun head() = unary { client.head() }

    private suspend fun <T : Any> unary(block: suspend () -> T): T =
        withTimeoutOrNull(RETRY_BUDGET_MS) { retryTransient(block) }
            ?: throw Status.DEADLINE_EXCEEDED.withDescription("KVStore request retry deadline exceeded").asRuntimeException()

    // A long successful scan may outlive the retry budget. Failed scans restart
    // only while there is time left, and never publish their partial contents.
    suspend fun <T> retryTransient(block: suspend () -> T): T {
        val started = TimeSource.Monotonic.markNow()
        var backoffMs = 25L
        while (true) {
            try {
                return block()
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                val retryable = when (Status.fromThrowable(failure).code) {
                    Status.Code.ABORTED, Status.Code.UNAVAILABLE, Status.Code.DEADLINE_EXCEEDED -> true
                    else -> false
                }
                if (!retryable || started.elapsedNow().inWholeMilliseconds + backoffMs >= RETRY_BUDGET_MS) throw failure
                delay(backoffMs)
                backoffMs = (backoffMs * 2).coerceAtMost(250)
            }
        }
    }

    suspend fun writeText(storeName: String, key: String, value: String, ttlMs: Long? = null) {
        require(ttlMs == null || ttlMs > 0) { "ttlMs must be > 0 when specified" }
        withKeyLock(storeName, key) {
            val ref = KVStoreKey(storeName, key)
            val expiresAt = ttlMs?.let { expiresAfter(it) } ?: 0
            mutate(ref, putValue(ref, ByteString.copyFromUtf8(value), expiresAt))
        }
    }

    suspend fun deleteText(storeName: String, key: String) = withKeyLock(storeName, key) {
        val ref = KVStoreKey(storeName, key)
        val existing = read(storeName, key)
        mutate(ref, deleteValue(ref), deletedRevision = existing.revision)
    }

    private suspend fun mutate(
        ref: KVStoreKey,
        operation: BatchWriteOperation,
        check: TransactionCheck? = null,
        deletedRevision: Long = 0,
    ) {
        val held = inheritedLeases()
        checkNotNull(held[ref])
        val leases = held.values.toList()
        leases.forEach { it.requireActive() }
        val checks = leases.map { it.ownerCheck } + listOfNotNull(check)
        try {
            if (!transact(transactionRequest(checks, listOf(operation)))) {
                throw LockLostException("KVStore write checks no longer hold for ${ref.namespace}:${ref.key}")
            }
        } catch (failure: Throwable) {
            cache.invalidate(ref)
            throw failure
        }
        try {
            cache.refresh(ref, deletedRevision)
        } catch (failure: Throwable) {
            cache.invalidate(ref)
            throw TransactionPostCommitSyncException("KVStore write committed but local synchronization failed", failure)
        }
    }

    suspend fun <T> withKeyLock(
        storeName: String,
        key: String,
        ttlMs: Long = DEFAULT_LEASE_TTL_MS,
        block: suspend () -> T,
    ): T = coroutineScope {
        require(ttlMs >= MIN_LEASE_TTL_MS) { "ttlMs must be at least ${MIN_LEASE_TTL_MS}ms" }
        val ref = KVStoreKey(storeName, key)
        val inherited = inheritedLeases()
        inherited[ref]?.let { lease ->
            lease.requireActive()
            return@coroutineScope block()
        }

        val lease = KVStoreLease.acquire(this@KVStoreRuntime, this, ref, ttlMs, failFast = true)
        var originalFailure: Throwable? = null
        try {
            withContext(KVStoreLeaseContext(this@KVStoreRuntime, inherited + (ref to lease))) { block() }
        } catch (failure: Throwable) {
            originalFailure = failure
            throw failure
        } finally {
            try {
                lease.release()
            } catch (failure: Throwable) {
                if (originalFailure == null) throw failure
                originalFailure.addSuppressed(failure)
            }
        }
    }

    suspend fun readModifyWrite(storeName: String, key: String, transformer: suspend (String?) -> String): String =
        withKeyLock(storeName, key) {
            val ref = KVStoreKey(storeName, key)
            val current = read(storeName, key)
            val next = transformer(current.takeIf { it.found }?.value?.toStringUtf8())
            mutate(ref, putValue(ref, ByteString.copyFromUtf8(next)), revisionCheck(ref, current.revision))
            next
        }

    suspend fun <T> transaction(block: suspend (KVStoreTransaction) -> T): T = withContext(Dispatchers.IO) {
        coroutineScope {
            val leaseContext = KVStoreLeaseContext(this@KVStoreRuntime, inheritedLeases())
            val txn = KVStoreTransaction(this, this@KVStoreRuntime, leaseContext)
            try {
                withContext(leaseContext) {
                    val result = block(txn)
                    if (!txn.committed && !txn.rolledBack) txn.commit()
                    result
                }
            } catch (failure: Throwable) {
                try {
                    withContext(NonCancellable) { txn.rollback() }
                } catch (rollbackFailure: Throwable) {
                    failure.addSuppressed(rollbackFailure)
                }
                throw failure
            }
        }
    }

    private companion object {
        const val RETRY_BUDGET_MS = 8_000L
    }
}
