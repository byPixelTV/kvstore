package dev.bypixel.kvstore

import com.google.protobuf.ByteString
import io.grpc.ChannelCredentials
import io.grpc.Grpc
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.TlsChannelCredentials
import io.grpc.stub.MetadataUtils
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.filterNot
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlinx.serialization.json.Json
import kvstore.v1.ConsoleGrpcKt.ConsoleCoroutineStub
import kvstore.v1.ConsoleOuterClass.ListNamespacesRequest
import kvstore.v1.KVStoreGrpcKt.KVStoreCoroutineStub
import kvstore.v1.Kvstore.*
import java.util.concurrent.TimeUnit.MILLISECONDS
import kotlin.coroutines.CoroutineContext

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
    watchContext: CoroutineContext,
    onWatchError: (String, Throwable) -> Unit,
) : AutoCloseable {
    private val authentication = apiKey?.takeIf { it.isNotEmpty() }?.let {
        val headers = Metadata().apply {
            put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer $it")
        }
        MetadataUtils.newAttachHeadersInterceptor(headers)
    }
    private val stub = KVStoreCoroutineStub(channel).let { stub ->
        authentication?.let { stub.withInterceptors(it) } ?: stub
    }
    private val console = ConsoleCoroutineStub(channel).let { stub ->
        authentication?.let { stub.withInterceptors(it) } ?: stub
    }

    private data class Key(val namespace: String, val key: String)
    private data class Cached(val response: GetResponse, val startedAtNanos: Long)
    private val cache = LinkedHashMap<Key, Cached>(16, 0.75f, true)
    private var cacheGeneration = 0L
    private val runtime = KVStoreRuntime(this, watchContext, onWatchError)

    companion object {
        fun connect(
            target: String,
            apiKey: String? = null,
            credentials: ChannelCredentials = TlsChannelCredentials.create(),
            json: Json = Json { ignoreUnknownKeys = true; encodeDefaults = true },
            timeoutMillis: Long = 5_000,
            cacheTtlMillis: Long = 5_000,
            maxCacheEntries: Int = 4_096,
        ): KVStoreClient = connect(
            target, apiKey, credentials, json, timeoutMillis, cacheTtlMillis, maxCacheEntries,
            Dispatchers.IO, defaultWatchError,
        )

        fun connect(
            target: String,
            apiKey: String? = null,
            credentials: ChannelCredentials = TlsChannelCredentials.create(),
            json: Json = Json { ignoreUnknownKeys = true; encodeDefaults = true },
            timeoutMillis: Long = 5_000,
            cacheTtlMillis: Long = 5_000,
            maxCacheEntries: Int = 4_096,
            watchContext: CoroutineContext,
            onWatchError: (String, Throwable) -> Unit = defaultWatchError,
        ): KVStoreClient {
            require(timeoutMillis > 0) { "timeoutMillis must be positive" }
            require(cacheTtlMillis >= 0 && maxCacheEntries >= 0) { "Cache limits must be nonnegative" }
            val channel = Grpc.newChannelBuilder(target, credentials).build()
            return KVStoreClient(channel, apiKey, json, timeoutMillis, cacheTtlMillis, maxCacheEntries, watchContext, onWatchError)
        }

        private val defaultWatchError: (String, Throwable) -> Unit = { message, error ->
            System.getLogger(KVStoreClient::class.java.name).log(System.Logger.Level.WARNING, message, error)
        }
    }

    override fun close() {
        runtime.close()
        channel.shutdownNow()
        synchronized(cache) {
            cacheGeneration++
            cache.clear()
        }
    }

    private fun unary() = stub.withDeadlineAfter(timeoutMillis, MILLISECONDS)

    /** Probe the connection, retrying transient errors within the SDK retry budget. */
    suspend fun checkConnection() {
        runtime.head()
    }

    /** Load a complete UTF-8 namespace and maintain it via a resumable watch. */
    suspend fun loadNamespace(storeName: String): List<NamespaceEntry> = runtime.cache.load(storeName)

    suspend fun namespaceEntries(storeName: String): List<NamespaceEntry> = runtime.cache.all(storeName)
    suspend fun namespaceKeys(storeName: String): Set<String> = namespaceEntries(storeName).mapTo(linkedSetOf()) { it.key }
    suspend fun namespaceValues(storeName: String): List<String> = namespaceEntries(storeName).map { it.value }
    fun cachedNamespace(storeName: String): CachedNamespace? = runtime.cache.cachedNamespace(storeName)
    fun cachedText(storeName: String, key: String): String? = runtime.cache.cachedEntry(storeName, key)?.value
    fun cachedBytes(storeName: String, key: String): ByteString? = runtime.cache.cachedEntry(storeName, key)?.bytes
    fun cachedKeys(storeName: String): Set<String>? = runtime.cache.cachedEntries(storeName)?.mapTo(linkedSetOf()) { it.key }
    fun cachedValues(storeName: String): List<String>? = runtime.cache.cachedEntries(storeName)?.map { it.value }

    /** Subscribe to cached key changes. A null key signals a full rescan. */
    fun subscribeToNamespace(storeName: String, listener: (key: String?) -> Unit): AutoCloseable =
        runtime.cache.subscribe(storeName, listener)

    suspend fun readText(storeName: String, key: String): String? = runtime.cache.read(KVStoreKey(storeName, key))

    /** Write UTF-8 bytes under a renewable key lease and update the local namespace cache. */
    suspend fun writeText(storeName: String, key: String, value: String, ttlMs: Long? = null) =
        runtime.writeText(storeName, key, value, ttlMs)

    suspend fun deleteText(storeName: String, key: String) = runtime.deleteText(storeName, key)

    suspend fun readModifyWrite(storeName: String, key: String, transformer: suspend (String?) -> String): String =
        runtime.readModifyWrite(storeName, key, transformer)

    /** Hold a renewable distributed lease with an initial TTL of at least five seconds. */
    suspend fun <T> withKeyLock(
        storeName: String,
        key: String,
        ttlMs: Long = DEFAULT_LEASE_TTL_MS,
        block: suspend () -> T,
    ): T = runtime.withKeyLock(storeName, key, ttlMs, block)

    /** Execute a callback once and commit its staged writes atomically across shards. */
    suspend fun <T> transaction(block: suspend (KVStoreTransaction) -> T): T = runtime.transaction(block)

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

    /** Reuse the same request ID and input after an ambiguous transport failure. */
    suspend fun transact(request: TransactionRequest): Boolean = try {
        unary().transact(request).committed
    } finally {
        request.operationsList.forEach { invalidate(it.namespace, it.key) }
    }

    suspend fun listNamespaces(): List<String> =
        console.withDeadlineAfter(timeoutMillis, MILLISECONDS)
            .listNamespaces(ListNamespacesRequest.getDefaultInstance()).namespacesList

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
            return if (response.found && isUnexpired(response)) response.value.toByteArray() else null
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
        if (!response.found || !isUnexpired(response)) return null
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

    private fun isLive(entry: Cached): Boolean =
        System.nanoTime() - entry.startedAtNanos < MILLISECONDS.toNanos(cacheTtlMillis) && isUnexpired(entry.response)

    private fun isUnexpired(response: GetResponse): Boolean =
        response.expiresAtUnixMs == 0L || response.expiresAtUnixMs > System.currentTimeMillis()

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
