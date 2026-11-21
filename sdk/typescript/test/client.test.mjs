import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { KVStoreClient, cursorFromChange, cursorFromScan, needsRescan } from "@bypixeltv/kvstore";

const target = process.env.KVSTORE_SDK_TEST_TARGET;

test("preserves vector cursors and uint64 revisions", () => {
  assert.deepEqual(cursorFromChange({ revision: "18446744073709551615", cursor: "" }), {
    revision: "18446744073709551615",
  });
  assert.deepEqual(cursorFromChange({ revision: "12", cursor: "opaque" }), { vector: "opaque" });
  assert.deepEqual(cursorFromScan({ checkpoint: true, snapshotRevision: "0", cursor: "opaque" }), { vector: "opaque" });
  assert.throws(() => cursorFromScan({ checkpoint: false }), TypeError);
  assert.equal(needsRescan({ code: 11 }), true);
  assert.equal(needsRescan({ code: 9 }), true);
  assert.equal(needsRescan({ code: 14 }), false);
});

test("reads and watches an authenticated KVStore", { skip: !target, timeout: 15_000 }, async () => {
  const client = new KVStoreClient(target, { plaintext: true, apiKey: process.env.KVSTORE_SDK_TEST_KEY });
  const namespace = `sdk-ts-${randomUUID()}`;
  const signal = AbortSignal.timeout(10_000);
  try {
    const watch = client.watch(namespace, { key: "alice", signal });
    assert.equal((await watch.next()).value.checkpoint, true);
    await client.setJson(namespace, "alice", { coins: 7 });
    const change = (await watch.next()).value;
    assert.equal(change.key, "alice");
    assert.equal(change.deleted, false);
    assert.deepEqual(JSON.parse(change.value.toString()), { coins: 7 });
    assert.deepEqual(await client.getJson(namespace, "alice"), { coins: 7 });
    await watch.return();

    const items = [];
    for await (const record of client.scan(namespace, signal)) items.push(record);
    assert.deepEqual(items.filter(record => !record.checkpoint).map(record => record.key), ["alice"]);
    const after = cursorFromScan(items.find(record => record.checkpoint));
    const results = await client.batchWrite([{ namespace, key: "alice", delete: true }]);
    assert.equal(results[0].grpcCode, 0);
    assert.equal(results[0].deleted, true);
    for await (const item of client.onChange(namespace, { key: "alice", after, signal })) {
      assert.equal(item.deleted, true);
      assert.equal(item.key, "alice");
      break;
    }
    assert.equal(await client.getJson(namespace, "alice"), undefined);

    const bytes = new Uint8Array([0, 255, 1]);
    const [set] = await client.batchWrite([{ namespace, key: "binary", value: bytes }]);
    assert.equal(set.grpcCode, 0);
    assert.deepEqual((await client.get(namespace, "binary")).value, Buffer.from(bytes));

    const cancel = new AbortController();
    const idle = client.watch(namespace, { signal: cancel.signal });
    assert.equal((await idle.next()).value.checkpoint, true);
    const next = idle.next();
    cancel.abort();
    await assert.rejects(next, { code: 1 });
  } finally {
    client.close();
  }
});
