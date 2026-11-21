package dev.bypixel.kvstore

import com.google.protobuf.ByteString
import io.grpc.InsecureChannelCredentials
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kvstore.v1.Kvstore.BatchWriteOperation
import org.junit.Assume.assumeNotNull
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class KVStoreClientTest {
    @Serializable
    data class Player(val coins: Int)

    @Test
    fun transactionRollbackAndNamespaceCache() = runBlocking<Unit> {
        val target = System.getenv("KVSTORE_SDK_TEST_TARGET")
        assumeNotNull(target)
        val token = System.getenv("KVSTORE_SDK_TEST_KEY")
        val plain = InsecureChannelCredentials.create()
        val first = "sdk-kotlin-a-${UUID.randomUUID()}"
        val second = generateSequence { "sdk-kotlin-b-${UUID.randomUUID()}" }
            .first { shard(it, "value") != shard(first, "counter") }

        KVStoreClient.connect(target!!, token, plain).use { client ->
            KVStoreClient.connect(target, token, plain).use { observer ->
                withTimeout(30_000) {
                    client.checkConnection()
                    client.writeText(first, "counter", "1")
                    assertEquals("1", observer.loadNamespace(first).single().value)
                    val initialCache = checkNotNull(observer.cachedNamespace(first))
                    assertSame(initialCache.records, observer.cachedNamespace(first)?.records)
                    val watchUpdate = CompletableDeferred<Unit>()
                    observer.subscribeToNamespace(first) { key ->
                        if (key == "counter") watchUpdate.complete(Unit)
                    }.use {
                        client.transaction { tx ->
                            assertEquals("1", tx.get(first, "counter"))
                            tx.set(first, "counter", "2")
                            tx.set(second, "value", "written")
                            assertEquals("2", tx.get(first, "counter"))
                        }
                        watchUpdate.await()
                    }
                    assertEquals("2", observer.cachedText(first, "counter"))
                    assertTrue(checkNotNull(observer.cachedNamespace(first)).version > initialCache.version)
                    assertEquals("written", observer.readText(second, "value"))

                    assertFailsWith<IllegalArgumentException> {
                        client.transaction { tx ->
                            tx.delete(first, "counter")
                            tx.set(second, "value", "discarded")
                            throw IllegalArgumentException("stop")
                        }
                    }
                    assertEquals("2", observer.read(first, "counter").value.toStringUtf8())
                    assertEquals("written", observer.read(second, "value").value.toStringUtf8())

                    assertFailsWith<TransactionCommitException> {
                        client.transaction { tx ->
                            assertEquals("2", tx.get(first, "counter"))
                            tx.set(second, "value", "must not commit")
                            // A writer using the raw API bypasses the cooperative lease.
                            observer.setRaw(first, "counter", "3".encodeToByteArray())
                        }
                    }
                    val firstRecord = observer.read(first, "counter")
                    val secondRecord = observer.read(second, "value")
                    assertEquals("3", firstRecord.value.toStringUtf8())
                    assertEquals("written", secondRecord.value.toStringUtf8())
                    if (System.getenv("KVSTORE_SDK_TEST_SHARDED") == "1") {
                        assertTrue(firstRecord.revision.ushr(56) != secondRecord.revision.ushr(56))
                    }

                    client.writeText(first, "ttl", "brief", ttlMs = 1_000)
                    observer.loadNamespace(first)
                    assertEquals("brief", observer.cachedText(first, "ttl"))
                    delay(1_050)
                    assertNull(observer.cachedText(first, "ttl"))

                    val binary = byteArrayOf(0, -1, -128)
                    client.transaction { tx ->
                        tx.setBytes(first, "binary", ByteString.copyFrom(binary))
                        assertContentEquals(binary, tx.getBytes(first, "binary")?.toByteArray())
                    }
                    observer.loadNamespace(first)
                    assertContentEquals(binary, observer.cachedBytes(first, "binary")?.toByteArray())
                }
            }
        }
    }

    // The current versioned router layout uses FNV-1a over namespace, NUL and key.
    private fun shard(namespace: String, key: String): Int {
        var hash = 14695981039346656037uL
        for (byte in "$namespace\u0000$key".encodeToByteArray()) {
            hash = (hash xor byte.toUByte().toULong()) * 1099511628211uL
        }
        return (hash % 4096uL % 3uL).toInt()
    }

    @Test
    fun renewableKeyLeasePreventsASecondClientFromEntering() = runBlocking<Unit> {
        val target = System.getenv("KVSTORE_SDK_TEST_TARGET")
        assumeNotNull(target)
        val token = System.getenv("KVSTORE_SDK_TEST_KEY")
        val plain = InsecureChannelCredentials.create()
        val namespace = "sdk-kotlin-lock-${UUID.randomUUID()}"

        KVStoreClient.connect(target!!, token, plain).use { holder ->
            KVStoreClient.connect(target, token, plain).use { contender ->
                withTimeout(20_000) {
                    val attempted = CompletableDeferred<Unit>()
                    val entered = CompletableDeferred<Unit>()
                    val pending = holder.withKeyLock(namespace, "key", ttlMs = 5_000) {
                        val competing = async {
                            attempted.complete(Unit)
                            contender.withKeyLock(namespace, "key", ttlMs = 5_000) {
                                entered.complete(Unit)
                            }
                        }
                        attempted.await()
                        delay(5_250)
                        assertTrue(!entered.isCompleted)
                        holder.withKeyLock(namespace, "key") {
                            holder.writeText(namespace, "key", "leased")
                        }
                        competing
                    }
                    pending.await()
                    assertEquals("leased", contender.readText(namespace, "key"))
                }
            }
        }
    }

    @Test
    fun cacheAndCommittedChangefeed() = runBlocking<Unit> {
        val target = System.getenv("KVSTORE_SDK_TEST_TARGET")
        assumeNotNull(target)
        val token = System.getenv("KVSTORE_SDK_TEST_KEY")
        val plain = InsecureChannelCredentials.create()
        val namespace = "sdk-kotlin-${UUID.randomUUID()}"

        KVStoreClient.connect(target!!, token, plain, cacheTtlMillis = 60_000).use { client ->
            KVStoreClient.connect(target, token, plain).use { writer ->
                withTimeout(15_000) {
                    assertNull(client.getCached<Player>(namespace, "alice"))
                    writer.set(namespace, "alice", Player(7))
                    assertEquals(Player(7), client.getCached<Player>(namespace, "alice"))
                    writer.set(namespace, "alice", Player(8))
                    assertEquals(Player(7), client.getCached<Player>(namespace, "alice"))
                    val ready = CompletableDeferred<Unit>()
                    val watching = async {
                        client.watch(namespace, "alice").first { event ->
                            if (event.checkpoint) {
                                ready.complete(Unit)
                                false
                            } else {
                                assertEquals("alice", event.key)
                                assertEquals(Player(9), client.getCached<Player>(namespace, "alice"))
                                true
                            }
                        }
                    }
                    ready.await()
                    assertEquals(Player(8), client.getCached<Player>(namespace, "alice"))
                    writer.set(namespace, "alice", Player(9))
                    watching.await()

                    val snapshot = client.scan(namespace).toList()
                    assertEquals(listOf("alice"), snapshot.filterNot { it.checkpoint }.map { it.key })
                    val resume = Cursor.from(snapshot.single { it.checkpoint })
                    val result = writer.batchWrite(listOf(
                        BatchWriteOperation.newBuilder().setNamespace(namespace).setKey("alice").setDelete(true).build(),
                    )).single()
                    assertEquals(0, result.grpcCode)
                    assertTrue(client.onChange(namespace, "alice", resume).first().deleted)
                    assertNull(client.getCached<Player>(namespace, "alice"))

                    val expiresAt = System.currentTimeMillis() + 1_000
                    client.set(namespace, "ttl", Player(9), expiresAt)
                    assertEquals(Player(9), client.getCached<Player>(namespace, "ttl"))
                    delay((expiresAt - System.currentTimeMillis()).coerceAtLeast(0) + 50)
                    assertNull(client.getCached<Player>(namespace, "ttl"))
                }
            }
        }
    }
}
