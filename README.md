<h1 align="center">KVStore</h1>

<p align="center">
  <strong>A persistent key-value database with a native gRPC API and resumable change streams.</strong>
</p>

<p align="center">
  <a href="https://github.com/byPixelTV/kvstore/releases"><img src="https://cdn.jsdelivr.net/npm/@intergrav/devins-badges@3/assets/compact/available/github_vector.svg" height="32" alt="Available on GitHub"></a>
  <a href="#go"><img src="https://cdn.jsdelivr.net/npm/@intergrav/devins-badges@3/assets/compact/built-with/go_vector.svg" height="32" alt="Built with Go"></a>
  <a href="#kotlin"><img src="https://cdn.jsdelivr.net/npm/@intergrav/devins-badges@3/assets/compact/built-with/kotlin_vector.svg" height="32" alt="Built with Kotlin"></a>
  <a href="#typescript"><img src="https://cdn.jsdelivr.net/npm/@intergrav/devins-badges@3/assets/compact/built-with/typescript_vector.svg" height="32" alt="Built with TypeScript"></a>
</p>

<p align="center">
  <a href="https://github.com/byPixelTV/kvstore/actions/workflows/ci.yml"><img src="https://github.com/byPixelTV/kvstore/actions/workflows/ci.yml/badge.svg?branch=nightly&amp;event=push" alt="Nightly CI"></a>
  <a href="https://github.com/byPixelTV/kvstore/actions/workflows/release.yml"><img src="https://github.com/byPixelTV/kvstore/actions/workflows/release.yml/badge.svg?event=workflow_run" alt="Release workflow"></a>
</p>

<p align="center">
  <a href="#quickstart">Quickstart</a> ·
  <a href="#sdks">SDKs</a> ·
  <a href="#change-events-and-recovery">Change streams</a> ·
  <a href="#deployment">Deployment</a> ·
  <a href="https://github.com/byPixelTV/kvstore/releases">Releases</a>
</p>

KVStore stores binary values under namespace and key pairs in Pebble. It runs as a single
process for local development or uses Raft replication and a routing layer for sharded
deployments. Applications can read and write through Kotlin, Go or TypeScript and subscribe to
committed changes through the same API.

| Capability | What you can do |
| --- | --- |
| **Read and write** | Choose strong or eventual reads, set optional key expirations, and batch writes within a shard. |
| **Watch and recover** | Subscribe to a key or namespace, resume from a cursor, and rebuild state from a snapshot. |
| **Operate** | Browse records, watch changes and manage API keys in the [`kvctl` terminal console](deploy/kvctl.md). |
| **Secure access** | Use bearer API keys or TLS with client certificates and namespace grants. See [production configuration](deploy/production.md). |

## Quickstart

From a checkout with Docker Compose installed, build and start one standalone node:

```sh
docker compose up -d --build
docker compose ps
```

The local plaintext gRPC endpoint is `127.0.0.1:50051`. The default Compose profile binds it
to loopback and uses a named volume for data. Once the service is healthy, try a write and a
read with [grpcurl](https://github.com/fullstorydev/grpcurl):

```sh
grpcurl -plaintext -import-path api/proto -proto kvstore/v1/kvstore.proto \
  -d '{"namespace":"players","key":"alice","value":"eyJjb2lucyI6MTAwfQ=="}' \
  127.0.0.1:50051 kvstore.v1.KVStore/Set

grpcurl -plaintext -import-path api/proto -proto kvstore/v1/kvstore.proto \
  -d '{"namespace":"players","key":"alice"}' \
  127.0.0.1:50051 kvstore.v1.KVStore/Get
```

The read returns `found: true` and the bytes for `{"coins":100}`. Protobuf JSON represents byte
fields as base64. Stop the local server with `docker compose down` when you are done.

For a hands-on terminal console, try the [Windows demo](deploy/kvctl.md#try-the-windows-demo).

## SDKs

Connect to the server endpoint in standalone or single-Raft mode, or to the router in a
sharded deployment. See [release channels](#release-channels) for package versions and feeds.

| Runtime | SDK | Stable package |
| --- | --- | --- |
| Kotlin/JVM 17+ | [sdk/kotlin](sdk/kotlin) | `dev.bypixel:kvstore` on [ByPixel Maven](https://repo.bypixel.dev/releases/) |
| Go 1.27+ | [sdk/go](sdk/go) | `github.com/byPixelTV/kvstore/sdk/go` |
| Node.js 22+ | [sdk/typescript](sdk/typescript) | `@bypixeltv/kvstore` on npm |

Clients use TLS by default. Pass an API key when the server requires bearer authentication.
All three accept custom trust roots and client certificates. Use explicit plaintext settings for a local plaintext endpoint.

KVStore stores bytes. The typed helpers store UTF-8 JSON, so the same value can be read from
Kotlin, Go and TypeScript. EraMC's `storeName` maps to `namespace` and its key stays the same.
Use the raw operations for binary data or a custom codec.

### Kotlin

```kotlin
repositories {
    mavenCentral()
    maven("https://repo.bypixel.dev/releases/")
}
dependencies {
    implementation("dev.bypixel:kvstore:0.1.0")
}
```

Apply the Kotlin serialization compiler plugin for `@Serializable` data classes.

```kotlin
import dev.bypixel.kvstore.KVStoreClient
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable

@Serializable
data class Player(val coins: Int)

// Inside a coroutine scope owned by your application or plugin:
val kvstore = KVStoreClient.connect("kv.example.com:50051", apiKey = System.getenv("KVSTORE_API_KEY"))
kvstore.set("players", "alice", Player(100))
val alice = kvstore.getCached<Player>("players", "alice")
val listener = launch {
    kvstore.onChange("players").collect { change ->
        if (change.deleted) println("Deleted ${change.key}")
        else println(kvstore.json.decodeFromString<Player>(change.value.toStringUtf8()))
    }
}
// At shutdown:
listener.cancel()
kvstore.close()
```

`getCached<T>` uses a bounded, expiring local cache and reads strongly on a miss.
The defaults are 4,096 entries and five seconds. Local writes evict affected keys.
Collect `watch` or `onChange` for the keys you want invalidated by remote writes as changes arrive.
Values with a server expiration also expire locally. Use `get<T>` for a fresh strong read.
Pass a custom `Json` instance to reuse application serializers.

### Go

```sh
go get github.com/byPixelTV/kvstore/sdk/go@v0.1.0
```

```go
import kvstore "github.com/byPixelTV/kvstore/sdk/go"

type Player struct {
    Coins int `json:"coins"`
}

client, err := kvstore.Dial("kv.example.com:50051", kvstore.Options{APIKey: os.Getenv("KVSTORE_API_KEY")})
if err != nil { return err }
defer client.Close()

if _, err := kvstore.SetJSON(ctx, client, "players", "alice", Player{Coins: 100}, 0); err != nil {
    return err
}
player, found, err := kvstore.GetJSON[Player](ctx, client, "players", "alice")
// Watch returns a stream. Call Recv() until a stream error or ctx cancellation.
// Pass a cursor to resume, or nil to start at the current head.
stream, err := client.Watch(ctx, "players", "", nil)
```

For a private GitHub checkout, authenticate Git and set `GOPRIVATE=github.com/byPixelTV/kvstore`.

### TypeScript

```sh
npm install @bypixeltv/kvstore
```

```ts
import { KVStoreClient, cursorFromChange } from "@bypixeltv/kvstore";

type Player = { coins: number };
const client = new KVStoreClient("kv.example.com:50051", { apiKey: process.env.KVSTORE_API_KEY });
await client.setJson("players", "alice", { coins: 100 });
const alice = await client.getJson<Player>("players", "alice");
const controller = new AbortController();

try {
  for await (const change of client.watch("players", { signal: controller.signal })) {
    if (!change.checkpoint) console.log(change.key, change.deleted);
    const cursor = cursorFromChange(change);
    // Persist cursor after processing the frame if you need to resume.
  }
} finally {
  client.close();
}
```

Call `controller.abort()` from your application's shutdown or unsubscribe handler.
Revisions and expiration timestamps in TypeScript responses are decimal strings to preserve 64-bit precision.

## Change events and recovery

- Watch a namespace or a single key. Changes describe committed sets and deletes. Checkpoints
  advance the resume position during idle periods and when other keys change. Watches with no
  starting cursor fetch the current head and require permission to call `Head`.
- Persist the last processed cursor, including checkpoints, and pass it back on reconnect.
  Process replays idempotently. In a sharded deployment, the cursor is opaque and ordering is per shard.
- On `OUT_OF_RANGE` or `FAILED_PRECONDITION`, replace your local state with a completed namespace
  scan and restart the watch from the scan checkpoint. Standalone and single-Raft scans emit that
  checkpoint first. Router scans emit it last. Save the checkpoint only after the scan succeeds.
- Respect `expiresAtUnixMs` in application state. Expiration is enforced by the server on reads
  and scans. Handle TTL locally if displaying state from a change stream.
- `BatchWrite` groups ordered writes atomically within a shard. Check every result for a
  multi-shard batch. A transport failure can leave the write outcome unknown.

## Deployment

In sharded mode, the gRPC router maps each key to a Raft group. The groups replicate their writes
independently. The checked-in Compose topology runs three voters per group.

```mermaid
flowchart LR
    Client["SDKs and kvctl"] -->|gRPC| Router["KVStore router"]
    Router --> S0["Raft group · shard 0"]
    Router --> S1["Raft group · shard 1"]
    Router --> S2["Raft group · shard 2"]
```

| Mode | Entry point | Guide |
| --- | --- | --- |
| **Standalone** | [`compose.yml`](compose.yml) | [Quickstart](#quickstart) |
| **Single Raft group** | [`compose.cluster.yml`](compose.cluster.yml) | [Linux deployment](deploy/production.md) |
| **Sharded** | [`compose.sharded.yml`](compose.sharded.yml) | [Production profiles and TLS](deploy/production.md#production-with-compose) |
| **Kubernetes** | [`deploy/kubernetes/`](deploy/kubernetes/) | [Kubernetes setup](deploy/kubernetes/README.md) |

The [gRPC specification](api/proto/kvstore/v1/kvstore.proto) documents the data API and
cursor formats.

## Release channels

After CI tests the SDKs against an authenticated KVStore and installs the built npm tarball,
the release workflow publishes the packages and existing server artifacts from the tested commit.

| Channel | Go module tag | Maven | npm |
| --- | --- | --- | --- |
| `stable` | `v<VERSION>` | Releases repository, `<VERSION>` | `latest`, `<VERSION>` |
| `nightly` | `v<VERSION>-SNAPSHOT.<run>.<attempt>` | Snapshots repository, `<VERSION>-SNAPSHOT` | `snapshot`, `<VERSION>-SNAPSHOT.<run>` |

Set `BYPIXEL_REPO_USER` and `BYPIXEL_REPO_TOKEN` in GitHub Actions for Maven publishing.
Use the Reposilite token name as the user and its secret as the token. Give the token read and
write access to these two routes:

- `/releases/dev/bypixel/kvstore/`
- `/snapshots/dev/bypixel/kvstore/`

Reposilite matches literal path prefixes. Keep the trailing slash to scope the routes to this
artifact. Adding `*` to the route causes it to miss uploads. [Reposilite route rules](https://reposilite.com/guide/routes)

npm publishing uses GitHub Actions OIDC. Go uses the GitHub release tag.
Set the release version in [VERSION](VERSION). The nightly Maven repository is
[ByPixel Snapshots](https://repo.bypixel.dev/snapshots/). Install the nightly npm package with
`npm install @bypixeltv/kvstore@snapshot`.

### First npm publication

npm configures trusted publishers per package. If `@bypixeltv/kvstore` has no package page yet,
publish the current SDK once as an interactive bootstrap using an npm account with write access
to `@bypixeltv`. Run these commands from the repository root before triggering a release:

```sh
cd sdk/typescript
npm ci
npm login --registry=https://registry.npmjs.org
npm publish --registry=https://registry.npmjs.org --access public --tag bootstrap
```

Approve the npm 2FA prompt. The package manifest uses version `0.0.0` for this local publish.
The `bootstrap` tag keeps `latest` and the first stable version free for GitHub Actions.

In the npm package settings under **Trusted Publisher**, choose **GitHub Actions** and enter
organization or user `byPixelTV`, repository `kvstore`, and workflow filename `release.yml`.
Leave the environment field empty and enable **Allow npm publish**. Trigger a successful
GitHub release within two days of saving the configuration.
See the [npm Trusted Publishing docs](https://docs.npmjs.com/trusted-publishers/).

## Development

Run the server tests with the race detector on Linux, then test and package the SDKs against an
isolated KVStore instance. The second command also needs JDK 21 and Node.js 24.

```sh
go test -race ./...
bash scripts/test-sdks.sh 0.1.0 "$PWD/tmp/sdk-packages"
```
