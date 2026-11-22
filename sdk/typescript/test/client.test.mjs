import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  KVStoreClient, TransactionConflictError, cursorFromChange, cursorFromScan, leaseNamespace, needsRescan,
} from "@bypixeltv/kvstore";

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
    let change;
    do {
      change = (await watch.next()).value;
    } while (change.checkpoint);
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
    const cancelled = (async () => {
      for await (const frame of idle) assert.equal(frame.checkpoint, true);
    })();
    cancel.abort();
    await assert.rejects(cancelled, { code: 1 });
  } finally {
    client.close();
  }
});

test("runs a callback once and retries an uncertain commit with identical input", async () => {
  const client = new KVStoreClient("127.0.0.1:1", { plaintext: true });
  const requests = [];
  let reads = 0;
  let callbacks = 0;
  client.get = async () => {
    reads++;
    return { found: true, value: Buffer.from("first"), revision: "18446744073709551615", expiresAtUnixMs: "0" };
  };
  client.transact = async request => {
    requests.push(request);
    if (requests.length === 1) throw Object.assign(new Error("uncertain"), { code: 14 });
    return true;
  };
  try {
    const result = await client.transaction(async tx => {
      callbacks++;
      const first = await tx.get("players", "alice");
      first[0] = 0;
      assert.equal((await tx.get("players", "alice")).toString(), "first");
      tx.set("players", "alice", Buffer.from("second"));
      assert.equal((await tx.get("players", "alice")).toString(), "second");
      tx.delete("players", "bob");
      assert.equal(await tx.get("players", "bob"), undefined);
      return 7;
    });
    assert.equal(result, 7);
    assert.equal(callbacks, 1);
    assert.equal(reads, 1);
    assert.equal(requests.length, 2);
    assert.deepEqual(requests[0], requests[1]);
    assert.deepEqual(requests[0].checks, [{ namespace: "players", key: "alice", revision: "18446744073709551615" }]);
    assert.deepEqual(requests[0].operations.map(operation => operation.key), ["alice", "bob"]);
  } finally {
    client.close();
  }
});

test("keeps a renewable scope during nested transactions and retries a contended release", async () => {
  const client = new KVStoreClient("127.0.0.1:1", { plaintext: true });
  const key = Buffer.from("café").toString("base64url") + ":" + Buffer.from("alice").toString("base64url");
  let owner;
  let releases = 0;
  let acquisitions = 0;
  let guarded = 0;
  client.get = async (namespace, recordKey) => {
    assert.equal(namespace, leaseNamespace);
    assert.equal(recordKey, key);
    return { found: owner !== undefined, value: owner ?? Buffer.alloc(0), revision: "1", expiresAtUnixMs: "0" };
  };
  client.transact = async ({ checks, operations }) => {
    assert.equal(checks[0].namespace, leaseNamespace);
    assert.equal(checks[0].key, key);
    if (checks[0].revision === "0") {
      acquisitions++;
      owner = operations[0].value;
      return true;
    }
    assert.deepEqual(checks[0].value, owner);
    if (operations[0].delete) {
      if (++releases === 1) return false;
      owner = undefined;
    } else {
      guarded++;
      assert.equal(operations[0].namespace, "players");
    }
    return true;
  };
  try {
    await client.withKeyLock("café", "alice", () => client.withKeyLock("café", "alice", () =>
      client.transaction(tx => tx.setJson("players", "alice", { coins: 5 }))));
    assert.deepEqual({ acquisitions, releases, guarded }, { acquisitions: 1, releases: 2, guarded: 1 });
    assert.equal(owner, undefined);
  } finally {
    client.close();
  }
});

test("materialized cache resumes from the last cursor and rebuilds after invalidation", { timeout: 5_000 }, async () => {
  const client = new KVStoreClient("127.0.0.1:1", { plaintext: true });
  const cursors = [];
  let scans = 0;
  client.scan = async function* () {
    const second = ++scans === 2;
    yield { key: second ? "charlie" : "alice", value: Buffer.from("scan"), revision: String(scans), expiresAtUnixMs: "0" };
    yield { key: "expired", value: Buffer.from("old"), revision: "1", expiresAtUnixMs: "1" };
    yield { checkpoint: true, cursor: second ? "reloaded" : "initial" };
  };
  client.watch = async function* (_namespace, { after, signal }) {
    cursors.push(after);
    if (cursors.length === 1) {
      yield { key: "bob", value: Buffer.from("first watch"), revision: "2", cursor: "replay", expiresAtUnixMs: "0" };
      throw Object.assign(new Error("disconnected"), { code: 14 });
    }
    if (cursors.length === 2) throw Object.assign(new Error("cursor expired"), { code: 11 });
    await sleep(10_000, undefined, { signal });
  };
  try {
    const cache = await client.loadNamespaceCache("players");
    const expires = AbortSignal.timeout(2000);
    while (cursors.length < 3) await sleep(5, undefined, { signal: expires });
    assert.deepEqual(cursors, [{ vector: "initial" }, { vector: "replay" }, { vector: "reloaded" }]);
    assert.equal(cache.ready, true);
    assert.deepEqual([...cache.snapshot().keys()], ["charlie"]);
    const copy = cache.get("charlie");
    copy.value[0] = 0;
    assert.equal(cache.get("charlie").value.toString(), "scan");
    assert.equal(cache.get("expired"), undefined);
    cache.close();
    await cache.finished;
    assert.equal(cache.ready, false);
    assert.equal(cache.snapshot().size, 0);
  } finally {
    client.close();
  }
});

test("commits conditional writes, renews a lease and maintains a local snapshot", { skip: !target, timeout: 25_000 }, async () => {
  const options = { plaintext: true, apiKey: process.env.KVSTORE_SDK_TEST_KEY };
  const client = new KVStoreClient(target, options);
  const other = new KVStoreClient(target, options);
  // Use distinct namespaces as well as keys to exercise routed cross-shard checks.
  const namespace = `sdk-ts-${randomUUID()}`;
  const receipts = `${namespace}-receipts`;
  const signal = AbortSignal.timeout(15_000);
  try {
    await client.setJson(namespace, "alice", { coins: 1 });
    assert.deepEqual(await client.getJson(namespace, "alice"), { coins: 1 });
    await client.set(namespace, "empty", Buffer.alloc(0));
    assert.equal(await client.getJson(namespace, "empty", { emptyAsMissing: true }), undefined);
    await assert.rejects(client.getJson(namespace, "empty"), SyntaxError);
    const cache = await client.loadNamespaceCache(namespace, signal);
    assert.equal(cache.get("alice").value.toString(), '{"coins":1}');
    await client.withKeyLock(namespace, "alice", async held => {
      const key = Buffer.from(namespace).toString("base64url") + ":" + Buffer.from("alice").toString("base64url");
      const { value, expiresAtUnixMs } = await other.get(leaseNamespace, key);
      assert.ok(value.length > 0);
      let renewed;
      while (!renewed) {
        await sleep(100, undefined, { signal: held });
        const lease = await other.get(leaseNamespace, key).catch(error => {
          if (error.code === 14) return undefined;
          throw error;
        });
        renewed = lease && Number(lease.expiresAtUnixMs) > Number(expiresAtUnixMs);
      }
      await client.transaction(async tx => {
        assert.deepEqual(await tx.getJson(namespace, "alice"), { coins: 1 });
        tx.setJson(namespace, "alice", { coins: 2 });
        tx.setJson(receipts, "payment", { delivered: true });
      });
    }, { ttlMs: 5000, signal });
    assert.deepEqual(await other.getJson(receipts, "payment"), { delivered: true });
    while (cache.get("alice").value.toString() !== '{"coins":2}') await sleep(10, undefined, { signal });
    await assert.rejects(client.transaction(async tx => {
      await tx.get(namespace, "alice");
      await other.setJson(namespace, "alice", { coins: 3 });
      tx.setJson(receipts, "payment", { delivered: false });
    }, signal), TransactionConflictError);
    assert.deepEqual(await other.getJson(receipts, "payment"), { delivered: true });
    cache.close();
    await cache.finished;
  } finally {
    client.close();
    other.close();
  }
});
