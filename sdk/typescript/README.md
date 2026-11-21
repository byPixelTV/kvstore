# @bypixeltv/kvstore

Node.js 22+ gRPC client for [KVStore](https://github.com/byPixelTV/kvstore).

Install the stable SDK with `npm install @bypixeltv/kvstore`.

```ts
import { KVStoreClient } from "@bypixeltv/kvstore";

const kvstore = new KVStoreClient("kv.example.com:50051", {
  apiKey: process.env.KVSTORE_API_KEY,
});
const controller = new AbortController();

try {
  await kvstore.setJson("players", "alice", { coins: 100 });
  console.log(await kvstore.getJson<{ coins: number }>("players", "alice"));

  for await (const change of kvstore.onChange("players", { signal: controller.signal })) {
    console.log(change.key, change.deleted);
  }
} finally {
  kvstore.close();
}
```

Use `controller.abort()` to unsubscribe. Use `watch` and `cursorFromChange` for checkpoints
and replay after a disconnect. Connect to the router for a sharded deployment.
See the [KVStore SDK documentation](https://github.com/byPixelTV/kvstore#sdks) for recovery,
wire types and release channels.

## Conditional transactions and locks

```ts
await kvstore.withKeyLock("players", "alice", async () => {
  await kvstore.transaction(async tx => {
    const player = await tx.getJson<{ coins: number }>("players", "alice");
    tx.setJson("players", "alice", { coins: (player?.coins ?? 0) + 1 });
  });
});
```

`transaction` runs the callback once. Commit checks every record read by the callback
and every lock held in its async scope. A failed check throws `TransactionConflictError`
and commits no writes. For explicit request IDs and retries, use `transact` with the same
ID, checks, and operations. The transaction write methods accept absolute expiry times in
milliseconds, like the point write methods.

Locks have a renewable TTL of at least five seconds. The callback receives an
`AbortSignal` that cancels when the lease is lost or the caller aborts. Use
`transaction` for writes that must validate lease ownership at commit. Point writes
remain unconditional. Allow read and write access to the `kvstore-leases` namespace for
clients that acquire locks. Lock keys are compatible with the Go and Kotlin SDKs.

## Local namespace cache

```ts
const players = await kvstore.loadNamespaceCache("players");
try {
  console.log(players.get("alice")?.value.toString("utf8"));
  console.log(players.snapshot().size);
} finally {
  players.close();
  await players.finished;
}
```

The cache loads a completed scan and follows changes from its checkpoint. On a transient
disconnect it replays from the last cursor. An invalid cursor invalidates the local
snapshot until a fresh scan completes. `get` and `snapshot` exclude expired records.
Use `ready` to distinguish an invalid snapshot from an empty namespace and `finished`
to observe terminal stream errors or wait for shutdown.
