# KVStore Go SDK

Import `github.com/byPixelTV/kvstore/sdk/go` as `kvstore`. Use the existing strong reads and transactions when a decision needs current server state.

## Materialized namespace cache

`CacheNamespace` loads a complete namespace snapshot and follows changes in the background. It resumes from the last processed cursor after a disconnect and replaces the snapshot if the cursor becomes invalid. Cached reads are synchronous and may lag server writes. Entries with an expired TTL are removed locally.

```go
cache, err := client.CacheNamespace(ctx, "players")
if err != nil {
    return err
}
defer cache.Close()

if player, ok := cache.Get("alice"); ok {
    fmt.Println(string(player.Value))
}

snapshot, version := cache.Snapshot()
fmt.Println(len(snapshot))
_, err = cache.WaitForChange(ctx, version)
return err
```

`Get` and `Snapshot` return copies. `Version` reports a cache-local generation. Use `Snapshot` together with `WaitForChange` to wait for an update without missing a notification. When the watch cursor becomes invalid, `Stale` reports that reads use the previous snapshot until a full scan replaces it. Cancel the context or close the cache or client to stop updates. If updates stop, `Done` closes, `Stale` reports true, and `Err` reports the reason.
