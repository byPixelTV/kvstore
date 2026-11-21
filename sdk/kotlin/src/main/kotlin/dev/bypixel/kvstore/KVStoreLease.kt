package dev.bypixel.kvstore

import com.google.protobuf.ByteString
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kvstore.v1.Kvstore.BatchWriteOperation
import kvstore.v1.Kvstore.TransactionCheck
import kvstore.v1.Kvstore.TransactionRequest
import java.util.Base64
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.TimeSource

internal const val LEASE_NAMESPACE = "kvstore-leases"
internal const val DEFAULT_LEASE_TTL_MS = 10_000L
internal const val MIN_LEASE_TTL_MS = 5_000L
internal const val LEASE_ACQUIRE_TIMEOUT_MS = 30_000L
private const val TRANSACTION_LIFETIME_MS = 3_000L
internal val TRANSACTION_GATE = KVStoreKey("__kvstore_internal_v1__", "transaction-gate")

internal data class KVStoreKey(val namespace: String, val key: String) {
    val leaseKey: String
        get() = "${encoder.encodeToString(namespace.encodeToByteArray())}:${encoder.encodeToString(key.encodeToByteArray())}"

    companion object {
        private val encoder = Base64.getUrlEncoder().withoutPadding()
    }
}

internal fun transactionRequest(
    checks: Iterable<TransactionCheck>,
    writes: Iterable<BatchWriteOperation>,
): TransactionRequest = TransactionRequest.newBuilder()
    .setRequestId(UUID.randomUUID().toString())
    .addAllChecks(checks)
    .addAllOperations(writes)
    .build()

internal fun putValue(ref: KVStoreKey, value: ByteString, expiresAt: Long = 0): BatchWriteOperation =
    BatchWriteOperation.newBuilder().setNamespace(ref.namespace).setKey(ref.key)
        .setValue(value).setExpiresAtUnixMs(expiresAt).build()

internal fun deleteValue(ref: KVStoreKey): BatchWriteOperation =
    BatchWriteOperation.newBuilder().setNamespace(ref.namespace).setKey(ref.key).setDelete(true).build()

internal fun revisionCheck(ref: KVStoreKey, revision: Long): TransactionCheck =
    TransactionCheck.newBuilder().setNamespace(ref.namespace).setKey(ref.key).setRevision(revision).build()

internal fun expiresAfter(ttlMs: Long, now: Long = System.currentTimeMillis()): Long =
    Math.addExact(now, ttlMs)

internal class KVStoreLease private constructor(
    private val runtime: KVStoreRuntime,
    private val key: KVStoreKey,
    private val owner: ByteString,
    private val ref: KVStoreKey,
) {
    val ownerCheck: TransactionCheck
        get() = TransactionCheck.newBuilder().setNamespace(key.namespace).setKey(key.key).setValue(owner).build()

    private val renewalFailure = AtomicReference<Throwable?>()
    private lateinit var renewal: Job

    fun requireActive() {
        renewalFailure.get()?.let { throw it }
    }

    suspend fun release() = withContext(NonCancellable + Dispatchers.IO) {
        renewal.cancelAndJoin()
        if (withTimeoutOrNull(5_000) { update(deleteValue(key)) } == null) throw lost()
        Unit
    }

    private fun lost() = LockLostException("Lost distributed lock on ${ref.namespace}:${ref.key}")

    private suspend fun update(write: BatchWriteOperation) {
        while (!runtime.transact(transactionRequest(listOf(ownerCheck), listOf(write)))) {
            val current = runtime.read(key.namespace, key.key)
            if (!current.found || current.value != owner) throw lost()
            // A transaction can temporarily reserve the lease key for its ownership check.
            delay(25)
        }
    }

    private fun startRenewing(scope: CoroutineScope, ttlMs: Long, initialExpiry: Long, failFast: Boolean) {
        renewal = scope.launch(Dispatchers.IO) {
            var expiresAt = initialExpiry
            // A sharded transaction rejects checks on records expiring within its decision window.
            val renewAfter = minOf(ttlMs / 3, (ttlMs - TRANSACTION_LIFETIME_MS) / 2)
            try {
                while (isActive) {
                    delay(expiresAt - (ttlMs - renewAfter) - System.currentTimeMillis())
                    val remaining = expiresAt - System.currentTimeMillis()
                    if (remaining <= 0) throw lost()
                    val nextExpiry = expiresAfter(ttlMs)
                    val write = putValue(key, owner, nextExpiry)
                    if (withTimeoutOrNull(remaining) { update(write) } == null) throw lost()
                    expiresAt = nextExpiry
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                renewalFailure.set(failure)
                if (failFast) throw failure
            }
        }
    }

    companion object {
        suspend fun acquire(
            runtime: KVStoreRuntime,
            scope: CoroutineScope,
            ref: KVStoreKey,
            ttlMs: Long = DEFAULT_LEASE_TTL_MS,
            failFast: Boolean = false,
        ): KVStoreLease {
            require(ttlMs >= MIN_LEASE_TTL_MS) { "ttlMs must be at least ${MIN_LEASE_TTL_MS}ms" }
            val key = KVStoreKey(LEASE_NAMESPACE, ref.leaseKey)
            val owner = ByteString.copyFromUtf8(UUID.randomUUID().toString())
            val started = TimeSource.Monotonic.markNow()
            val check = revisionCheck(key, 0)
            while (true) {
                if (!runtime.read(key.namespace, key.key).found) {
                    val expiry = expiresAfter(ttlMs)
                    val write = putValue(key, owner, expiry)
                    if (runtime.transact(transactionRequest(listOf(check), listOf(write)))) {
                        return KVStoreLease(runtime, key, owner, ref).also { it.startRenewing(scope, ttlMs, expiry, failFast) }
                    }
                }
                if (started.elapsedNow().inWholeMilliseconds >= LEASE_ACQUIRE_TIMEOUT_MS) {
                    throw LockAcquisitionException("Failed to acquire distributed lock on ${ref.namespace}:${ref.key}")
                }
                delay(25)
            }
        }
    }
}

class LockAcquisitionException(message: String) : Exception(message)
class LockLostException(message: String) : Exception(message)
