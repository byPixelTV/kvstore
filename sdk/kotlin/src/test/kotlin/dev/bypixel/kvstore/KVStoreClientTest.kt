package dev.bypixel.kvstore

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
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KVStoreClientTest {
    @Serializable
    data class Player(val coins: Int)

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
