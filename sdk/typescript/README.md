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
