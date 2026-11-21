package dev.bypixel.kvstore

import com.google.protobuf.ByteString
import io.grpc.ChannelCredentials
import io.grpc.Grpc
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.TlsChannelCredentials
import io.grpc.stub.MetadataUtils
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.filterNot
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlinx.serialization.json.Json
import kvstore.v1.KVStoreGrpcKt.KVStoreCoroutineStub
import kvstore.v1.Kvstore.*
import java.util.concurrent.TimeUnit.MILLISECONDS

/** Supply a revision for a single group, or the opaque vector for a sharded endpoint. */
data class Cursor(val revision: ULong = 0u, val vector: String = "") {
    companion object {
        fun from(head: HeadResponse): Cursor =
            if (head.cursor.isEmpty()) Cursor(head.revision.toULong()) else Cursor(vector = head.cursor)

        fun from(change: WatchResponse): Cursor =
            if (change.cursor.isEmpty()) Cursor(change.revision.toULong()) else Cursor(vector = change.cursor)

        /** Use the checkpoint from a completed scan. Sharded scans checkpoint at the end. */
        fun from(checkpoint: ScanResponse): Cursor {
            require(checkpoint.checkpoint) { "Expected a scan checkpoint" }
            return if (checkpoint.cursor.isEmpty()) Cursor(checkpoint.snapshotRevision.toULong())
            else Cursor(vector = checkpoint.cursor)
        }
    }
}

fun Throwable.needsKvStoreRescan(): Boolean = when (Status.fromThrowable(this).code) {
    Status.Code.OUT_OF_RANGE, Status.Code.FAILED_PRECONDITION -> true
    else -> false
}

/** Coroutine-based KVStore client. Close it when the application shuts down. */
class KVStoreClient private constructor(
    private val channel: ManagedChannel,
    apiKey: String?,
    val json: Json,
    private val timeoutMillis: Long,
    private val cacheTtlMillis: Long,
    private val maxCacheEntries: Int,
) : AutoCloseable {
    private val stub = KVStoreCoroutineStub(channel).let { stub ->
        if (apiKey.isNullOrEmpty()) stub else {
            val headers = Metadata().apply {
                put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer $apiKey")
            }
            stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
        }
    }

    private data class Key(val namespace: String, val key: String)
    private data class Cached(val response: GetResponse, val startedAtNanos: Long)
    private val cache = LinkedHashMap<Key, Cached>(16, 0.75f, true)
    private var cacheGeneration = 0L

    companion object {
        fun connect(
            target: String,
            apiKey: String? = null,
            credentials: ChannelCredentials = TlsChannelCredentials.create(),
            json: Json = Json { ignoreUnknownKeys = true; encodeDefaults = true },
            timeoutMillis: Long = 5_000,
            cacheTtlMillis: Long = 5_000,
            maxCacheEntries: Int = 4_096,
        ): KVStoreClient {
            require(timeoutMillis > 0) { "timeoutMillis must be positive" }
            require(cacheTtlMillis >= 0 && maxCacheEntries >= 0) { "Cache limits must be nonnegative" }
            val channel = Grpc.newChannelBuilder(target, credentials).build()
            return KVStoreClient(channel, apiKey, json, timeoutMillis, cacheTtlMillis, maxCacheEntries)
        }
    }

    override fun close() {
        channel.shutdownNow()
        synchronized(cache) {
            cacheGeneration++
            cache.clear()
        }
    }

    private fun unary() = stub.withDeadlineAfter(timeoutMillis, MILLISECONDS)

    suspend fun read(
        namespace: String,
        key: String,
        consistency: ReadConsistency = ReadConsistency.READ_CONSISTENCY_STRONG,
    ): GetResponse = unary().get(
        GetRequest.newBuilder().setNamespace(namespace).setKey(key).setConsistency(consistency).build(),
    )

    suspend fun setRaw(namespace: String, key: String, value: ByteArray, expiresAtUnixMs: Long = 0): SetResponse =
        try {
            unary().set(
                SetRequest.newBuilder().setNamespace(namespace).setKey(key)
                    .setValue(ByteString.copyFrom(value)).setExpiresAtUnixMs(expiresAtUnixMs).build(),
            )
        } finally {
            invalidate(namespace, key)
        }

    suspend fun delete(namespace: String, key: String): DeleteResponse = try {
        unary().delete(DeleteRequest.newBuilder().setNamespace(namespace).setKey(key).build())
    } finally {
        invalidate(namespace, key)
    }

    /** Results are ordered. A transport failure can leave the outcome unknown. */
    suspend fun batchWrite(operations: List<BatchWriteOperation>): List<BatchWriteResult> = try {
        unary().batchWrite(BatchWriteRequest.newBuilder().addAllOperations(operations).build()).resultsList
    } finally {
        operations.forEach { invalidate(it.namespace, it.key) }
    }

    suspend inline fun <reified T> get(namespace: String, key: String): T? {
        val response = read(namespace, key)
        return if (response.found) json.decodeFromString<T>(response.value.toStringUtf8()) else null
    }

    suspend inline fun <reified T> set(namespace: String, key: String, value: T, expiresAtUnixMs: Long = 0): SetResponse =
        setRaw(namespace, key, json.encodeToString(value).encodeToByteArray(), expiresAtUnixMs)

    /** Cache hits are local. Misses use a strong read. [watch] invalidates observed changes. */
    suspend inline fun <reified T> getCached(namespace: String, key: String): T? =
        getCachedRaw(namespace, key)?.let { json.decodeFromString<T>(it.decodeToString()) }

    suspend fun getCachedRaw(namespace: String, key: String): ByteArray? {
        if (cacheTtlMillis == 0L || maxCacheEntries == 0) {
            val response = read(namespace, key)
            return if (response.found) response.value.toByteArray() else null
        }
        val ref = Key(namespace, key)
        val startedAt = System.nanoTime()
        val generation = synchronized(cache) {
            val existing = cache[ref]
            if (existing != null && isLive(existing)) return existing.response.value.toByteArray()
            cache.remove(ref)
            cacheGeneration
        }
        val response = read(namespace, key)
        if (!response.found) return null
        val entry = Cached(response, startedAt)
        synchronized(cache) {
            if (generation == cacheGeneration && isLive(entry) && !channel.isShutdown) {
                // A concurrent read started later may already have populated the entry.
                if ((cache[ref]?.startedAtNanos ?: startedAt) - startedAt <= 0) {
                    cache[ref] = entry
                    if (cache.size > maxCacheEntries) {
                        cache.entries.iterator().apply { next(); remove() }
                    }
                }
            }
        }
        return response.value.toByteArray()
    }

    private fun isLive(entry: Cached): Boolean {
        val expiresAt = entry.response.expiresAtUnixMs
        return System.nanoTime() - entry.startedAtNanos < MILLISECONDS.toNanos(cacheTtlMillis) &&
            (expiresAt == 0L || expiresAt > System.currentTimeMillis())
    }

    fun invalidate(namespace: String, key: String? = null) {
        synchronized(cache) {
            cacheGeneration++
            if (key == null) cache.keys.removeIf { it.namespace == namespace }
            else cache.remove(Key(namespace, key))
        }
    }

    suspend fun head(): HeadResponse = unary().head(HeadRequest.newBuilder().setUseVectorCursor(true).build())

    fun scan(namespace: String): Flow<ScanResponse> =
        stub.scan(ScanRequest.newBuilder().setNamespace(namespace).setUseVectorCursor(true).build())

    /** Cold flow including cursor checkpoints. Cancel collection to stop the RPC. */
    fun watch(namespace: String, key: String = "", after: Cursor? = null): Flow<WatchResponse> = flow {
        invalidate(namespace, key.ifEmpty { null })
        try {
            val cursor = after ?: Cursor.from(head())
            val request = WatchRequest.newBuilder().setNamespace(namespace).setKey(key)
                .setUseVectorCursor(true).setCursor(cursor.vector)
                .setAfterRevision(if (cursor.vector.isEmpty()) cursor.revision.toLong() else 0)
                .build()
            emitAll(stub.watch(request).onEach { change ->
                if (!change.checkpoint) invalidate(namespace, change.key)
            })
        } finally {
            invalidate(namespace, key.ifEmpty { null })
        }
    }

    /** Cold flow of committed set and delete events. Use [watch] to receive checkpoints too. */
    fun onChange(namespace: String, key: String = "", after: Cursor? = null): Flow<WatchResponse> =
        watch(namespace, key, after).filterNot { it.checkpoint }
}
