package dev.bypixel.kvstore

import com.github.benmanes.caffeine.cache.Caffeine
import com.google.protobuf.ByteString
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kvstore.v1.Kvstore.GetResponse
import kvstore.v1.Kvstore.WatchResponse
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicLong

/** A stored value and its absolute expiry, or null for a key without an expiry. */
data class NamespaceEntry(val key: String, val bytes: ByteString, val expiresAt: Long? = null) {
    constructor(key: String, value: String, expiresAt: Long? = null) :
        this(key, ByteString.copyFromUtf8(value), expiresAt)

    val value: String get() = bytes.toStringUtf8()
}

/** Read-only live view of the cached entries with the local version at the time of access. */
data class CachedNamespace(val records: Map<String, NamespaceEntry>, val version: Long)

internal class KVStoreCache(
    private val runtime: KVStoreRuntime,
) {
    private class Namespace {
        val mutex = Mutex()
        val listeners = CopyOnWriteArraySet<(String?) -> Unit>()
        val revisions = mutableMapOf<String, Long>()
        val awaitingDelete = mutableMapOf<String, Long>()
        var complete = false
        var watcher: Job? = null
    }

    private class Records(val entries: ConcurrentHashMap<String, NamespaceEntry> = ConcurrentHashMap()) {
        val view: Map<String, NamespaceEntry> = Collections.unmodifiableMap(entries)
    }

    // Namespace maps remain live until replaced by a new complete scan.
    private val cache = ConcurrentHashMap<String, Records>()
    private val versions = ConcurrentHashMap<String, AtomicLong>()
    private val namespaces = ConcurrentHashMap<String, Namespace>()
    private val misses = Caffeine.newBuilder().expireAfterWrite(java.time.Duration.ofSeconds(3))
        .maximumSize(100_000).build<KVStoreKey, Boolean>()
    private val recentDeletes = Caffeine.newBuilder().expireAfterWrite(java.time.Duration.ofMinutes(1))
        .maximumSize(100_000).build<KVStoreKey, Long>()

    private fun namespace(storeName: String) = namespaces.computeIfAbsent(storeName) { Namespace() }
    private fun changed(storeName: String) = versions.computeIfAbsent(storeName) { AtomicLong() }.incrementAndGet()

    fun cachedNamespace(storeName: String): CachedNamespace? {
        val records = cache[storeName] ?: return null
        return CachedNamespace(records.view, versions[storeName]?.get() ?: 0)
    }

    fun cachedEntry(storeName: String, key: String): NamespaceEntry? {
        val records = cache[storeName]?.entries ?: return null
        val record = records[key] ?: return null
        return record.takeUnless { expire(storeName, records, it) }
    }

    fun cachedEntries(storeName: String): List<NamespaceEntry>? {
        val records = cache[storeName]?.entries ?: return null
        return records.values.filterNot { expire(storeName, records, it) }
    }

    fun clear() {
        cache.clear()
        misses.invalidateAll()
        recentDeletes.invalidateAll()
        namespaces.clear()
        versions.clear()
    }

    suspend fun load(storeName: String): List<NamespaceEntry> {
        val state = namespace(storeName)
        return state.mutex.withLock {
            val cursor = snapshot(storeName, state)
            state.watcher?.cancel()
            state.watcher = startWatcher(storeName, state, cursor)
            cache[storeName]?.entries.orEmpty().values.toList()
        }
    }

    private suspend fun snapshot(storeName: String, state: Namespace): Cursor {
        val (entries, revisions, checkpoint) = runtime.retryTransient {
            val entries = ConcurrentHashMap<String, NamespaceEntry>()
            val revisions = mutableMapOf<String, Long>()
            var cursor: Cursor? = null
            runtime.client.scan(storeName).collect { frame ->
                if (frame.checkpoint) {
                    cursor = Cursor.from(frame)
                } else {
                    entries[frame.key] = entry(frame.key, frame.value, frame.expiresAtUnixMs)
                    revisions[frame.key] = frame.revision
                }
            }
            Triple(entries, revisions, checkNotNull(cursor) { "KVStore scan returned no checkpoint for $storeName" })
        }
        cache[storeName] = Records(entries)
        state.revisions.clear()
        state.revisions.putAll(revisions)
        state.awaitingDelete.clear()
        state.complete = true
        misses.asMap().keys.removeIf { it.namespace == storeName }
        recentDeletes.asMap().keys.removeIf { it.namespace == storeName }
        changed(storeName)
        return checkpoint
    }

    suspend fun all(storeName: String): List<NamespaceEntry> {
        val state = namespace(storeName)
        state.mutex.withLock {
            val records = cache[storeName]?.entries
            if (state.complete && records != null) {
                return records.values.filterNot { expire(storeName, records, it) }
            }
        }
        return load(storeName)
    }

    suspend fun read(ref: KVStoreKey): String? {
        val state = namespace(ref.namespace)
        return state.mutex.withLock {
            watchIfNeeded(ref.namespace, state)
            val records = cache[ref.namespace]?.entries
            records?.get(ref.key)?.let { if (!expire(ref.namespace, records, it)) return@withLock it.value }
            if (misses.getIfPresent(ref) == true) return@withLock null
            val record = runtime.read(ref.namespace, ref.key)
            applyRead(ref, state, record)
            if (record.found) record.value.toStringUtf8() else null
        }
    }

    suspend fun refresh(ref: KVStoreKey, deletedRevision: Long) {
        val state = namespace(ref.namespace)
        state.mutex.withLock {
            val wasWatching = state.watcher?.isActive == true
            watchIfNeeded(ref.namespace, state)
            val record = runtime.read(ref.namespace, ref.key)
            if (!record.found && wasWatching && deletedRevision != 0L &&
                !newer(recentDeletes.getIfPresent(ref), deletedRevision)
            ) {
                state.awaitingDelete[ref.key] = deletedRevision
            }
            applyRead(ref, state, record)
        }
    }

    fun invalidate(ref: KVStoreKey) {
        val records = cache[ref.namespace]?.entries
        records?.remove(ref.key)
        misses.invalidate(ref)
        changed(ref.namespace)
    }

    private fun applyRead(ref: KVStoreKey, state: Namespace, record: GetResponse) {
        if (record.found) {
            val records = cache.computeIfAbsent(ref.namespace) { Records() }.entries
            records[ref.key] = entry(ref.key, record.value, record.expiresAtUnixMs)
            state.revisions[ref.key] = record.revision
            state.awaitingDelete.remove(ref.key)
            misses.invalidate(ref)
        } else {
            cache[ref.namespace]?.entries?.remove(ref.key)
            // Absence has no revision. Keep a locally observed deletion pending
            // until its ordered watch event catches up.
            misses.put(ref, true)
        }
        changed(ref.namespace)
    }

    private fun expire(storeName: String, records: ConcurrentHashMap<String, NamespaceEntry>, entry: NamespaceEntry): Boolean {
        if (entry.expiresAt?.let { it <= System.currentTimeMillis() } != true) return false
        if (records.remove(entry.key, entry)) changed(storeName)
        return true
    }

    fun subscribe(storeName: String, listener: (String?) -> Unit): AutoCloseable {
        val state = namespace(storeName)
        state.listeners.add(listener)
        val startup = runtime.watchScope.launch {
            while (isActive) {
                try {
                    state.mutex.withLock { watchIfNeeded(storeName, state) }
                    break
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (error: Exception) {
                    runtime.logError("Could not start KVStore watch for $storeName", error)
                }
                delay(250)
            }
        }
        return AutoCloseable {
            startup.cancel()
            state.listeners.remove(listener)
        }
    }

    private suspend fun watchIfNeeded(storeName: String, state: Namespace) {
        if (state.watcher?.isActive == true) return
        val cursor = Cursor.from(runtime.head())
        state.watcher = startWatcher(storeName, state, cursor)
    }

    private fun startWatcher(storeName: String, state: Namespace, initial: Cursor): Job = runtime.watchScope.launch {
        var cursor = initial
        var rescan = false
        while (currentCoroutineContext().isActive) {
            try {
                if (rescan) {
                    cursor = state.mutex.withLock { snapshot(storeName, state) }
                    state.listeners.forEach { it(null) }
                    rescan = false
                }
                runtime.client.watch(storeName, after = cursor).collect { event ->
                    if (!event.checkpoint) {
                        state.mutex.withLock { applyChange(storeName, state, event) }
                        state.listeners.forEach { it(event.key) }
                    }
                    cursor = Cursor.from(event)
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Exception) {
                runtime.logError("KVStore watch failed for $storeName", error)
                if (error.needsKvStoreRescan()) {
                    rescan = true
                    state.mutex.withLock {
                        state.complete = false
                        cache.remove(storeName)
                        misses.asMap().keys.removeIf { it.namespace == storeName }
                        changed(storeName)
                    }
                }
            }
            delay(250)
        }
    }

    private fun applyChange(storeName: String, state: Namespace, event: WatchResponse) {
        val ref = KVStoreKey(storeName, event.key)
        val pending = state.awaitingDelete[ref.key]
        if (pending != null) {
            if (!event.deleted || !newer(event.revision, pending)) return
            state.awaitingDelete.remove(ref.key)
        }
        if (newer(state.revisions[ref.key], event.revision)) return
        val records = cache[storeName]?.entries
        if (event.deleted) {
            records?.remove(ref.key)
            changed(storeName)
            state.revisions.remove(ref.key)
            recentDeletes.put(ref, event.revision)
            misses.put(ref, true)
        } else {
            records?.set(ref.key, entry(ref.key, event.value, event.expiresAtUnixMs))
            changed(storeName)
            if (records != null) state.revisions[ref.key] = event.revision
            misses.invalidate(ref)
        }
    }

    private fun newer(candidate: Long?, base: Long): Boolean =
        candidate != null && candidate.ushr(56) == base.ushr(56) && candidate.toULong() > base.toULong()

    private fun entry(key: String, value: ByteString, expiry: Long): NamespaceEntry {
        val expiresAt = expiry.takeIf { it > 0 }
        return NamespaceEntry(key, value, expiresAt)
    }
}
