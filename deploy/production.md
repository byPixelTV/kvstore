# Running KVStore on Linux

The KVStore image includes the server, `kvbench`, `kvshardctl`, and `kvimport`. By default, it runs as a persistent standalone node. Use cluster mode for replicated writes. `compose.sharded.yml` starts three Raft groups with three voters each and one gRPC router. The same performance defaults apply when running the containers outside Compose.

| Cluster default | Value |
| --- | --- |
| Write batching window / maximum window | 1.5 ms / 3 ms |
| Write batch target / maximum size | 64 / 256 |
| Raft in-flight limit / commit timeout | 8 / 50 ms |
| Raft AppendEntries / Log-Cache | 128 / 512 |
| STRONG read batching / concurrent quorum checks | 300 µs / 2 |
| Router connections per backend | 8 |

The benchmark throughput assumes 50% STRONG GETs and 50% logical writes through `BatchWrite`, with 16 operations per RPC. Applications can call `BatchWrite` directly for independent keys. A batch that spans multiple shards returns a status for each operation. A timeout can occur after a commit. If the outcome is unknown, check the application state before retrying the mutation.

## Starting containers

Set up Docker Compose and persistent SSD volumes on the Linux host. The following commands build the image from the checkout without GHCR access. Compose requires two separate secrets. Set them through environment variables or a protected local `.env` file.

```sh
export KVSTORE_JOIN_TOKEN="$(openssl rand -hex 32)"
export KVSTORE_ROUTER_ADMIN_TOKEN="$(openssl rand -hex 32)"
export KVSTORE_IMAGE=kvstore:multiraft-dev
docker build -t "$KVSTORE_IMAGE" .
docker compose -f compose.sharded.yml up -d --no-build
docker compose -f compose.sharded.yml ps
```

If you have access to a published image, set `KVSTORE_IMAGE` to its tag or digest and run
`docker compose -f compose.sharded.yml pull` before starting the services.

Keep the same secrets across restarts. The router publishes gRPC on `127.0.0.1:50081` for local checks. Set `KVSTORE_SHARDED_GRPC_PORT` to use a different local port. The join token protects the HTTP admin API on the voters. Internal Raft and gRPC traffic uses plaintext, so use a private encrypted network for traffic between hosts.

Each voter needs its own persistent `/data` mount and a stable identity. When upgrading within the same Raft WAL version, update one voter per group at a time. Wait for it to return before stopping the next one. Use the join token to read admin metrics at `GET /v1/cluster/performance`. Before migrating a legacy Raft BoltDB volume, test the export and import procedure separately. The Compose topology uses separate volumes for each shard. Rehearse restoring from off-host backups in staging.

Compose sets `KVSTORE_CLUSTER_MIN_VOTERS=3`. The leader starts serving client RPCs after a Raft barrier confirms the expected voter configuration. Membership and bootstrap proceed independently through Raft and the admin API. With a custom orchestrator, set the same variable on all voters.

## Production with Compose

### Quick setup for one Linux host

Save [`compose.solo.yml`](../compose.solo.yml) as `compose.yaml` on the deployment
host. Put a KVStore token in the adjacent `.env` as `KVSTORE_API_KEY`. To generate
a new token when creating that file, run:

```sh
(umask 077
set -C
printf 'KVSTORE_API_KEY=kvs_%s\n' "$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n')" > .env
)
```

Then start the service:

```sh
docker compose up -d --wait
```

The default image is `ghcr.io/bypixeltv/kvstore:latest` from the stable release
workflow. Compose checks for a current image when you run `up`. To pin a version
or digest, set `KVSTORE_IMAGE` in `.env`. The listener is published on
`127.0.0.1:50051`. Set `KVSTORE_PORT` to change the host port.

On first start, KVStore saves the initial key's digest and management grants in
the persistent `/data/api-keys` directory. The persisted registry is used on
later starts, including when the initial value is removed from `.env`. Changing
that variable does not rotate an existing key. To create separate application
keys, use the API key command against the persisted registry:

```sh
docker compose exec kvstore kvstore apikey create --dir /data/api-keys --name moonlight --read --write
```

Set `KVSTORE_ADDR=127.0.0.1:50051`, `KVSTORE_PLAINTEXT=true`, and the application's
`KVSTORE_API_KEY` in the application's environment. Docker clients can join the
`kvstore-solo_default` network and use `kvstore:50051`. Keep their existing network
memberships and declare the KVStore network as external in their Compose files.
Use the TLS profile below for clients on another host.

If an existing deployment uses a separate API key registry, keep mounting and
using that registry while migrating its configuration. The one-file example
initializes a registry inside its data volume. For V1 data, follow
[import and verification](#importing-kvstore-v1-from-mongodb) before starting the
migrated applications.

### TLS configuration

For a single server, add `compose.solo.production.yml`. For a sharded cluster, combine `compose.sharded.yml` and `compose.production.yml`. Set `KVSTORE_IMAGE` to the approved image digest for either profile. Put `server.crt` with its certificate chain, `server.key`, and `client-ca.crt` for the dedicated client CA in `KVSTORE_TLS_DIR`. The container runs as UID 65532 and needs read access to these files. The gRPC address used in `-addr` or in the application must match the server certificate.

```sh
export KVSTORE_TLS_DIR=/path/to/kvstore-tls
export KVSTORE_IMAGE=registry.example/kvstore@sha256:IMAGE_DIGEST
docker compose -p kvstore-solo -f compose.yml -f compose.solo.production.yml up -d --no-build
```

For shards, also set `KVSTORE_JOIN_TOKEN` and `KVSTORE_ROUTER_ADMIN_TOKEN` from the secret store. The production overlay disables automatic Raft bootstrap. After starting with three empty volumes per group, initialize the designated voter once. All three admin endpoints in the group must be reachable and report the same bootstrap plan. An existing group keeps its data and membership on its volumes. To replace a failed voter, start the replacement with the same ID and address. It catches up from the surviving majority.

```sh
docker compose -p kvstore-live -f compose.sharded.yml -f compose.production.yml up -d --no-build
# Initial setup with empty volumes only. Wait until all voters have started.
for port in 17101 17105 17109; do
  printf 'Authorization: Bearer %s\n' "$KVSTORE_JOIN_TOKEN" | \
    curl --fail --silent --show-error --retry 12 --retry-connrefused --retry-delay 1 \
      --header @- --request POST "http://127.0.0.1:$port/v1/cluster/bootstrap" || break
done
```

The data endpoint uses native gRPC mTLS and binds to `127.0.0.1:5443` by default. For external clients, set `KVSTORE_INGRESS_IP` to the intended host address. Published admin and metrics ports bind only to the host loopback address. KVStore reads the TLS files and access policy on startup. After changing either, restart the standalone instance during a maintenance window or restart the routers one at a time.

When upgrading from the old Envoy overlay, stop the project's existing `ingress` container during a maintenance window so the router can bind to the TLS port. Keep the Raft volumes.

### Namespace permissions

To enable an access policy, set `KVSTORE_ACCESS_POLICY_FILE=/etc/kvstore/tls/policy.json` and put the file in the TLS directory. Each client certificate must have exactly one URI SAN. Each rule applies to that exact identity and to the listed namespaces. `read` grants `Get`, `Scan`, and `Watch`. `write` grants `Set`, `Delete`, and `BatchWrite`. If any operation lacks write permission, KVStore rejects the entire batch. `allow_head` explicitly grants access to the global cursor. `kvimport` requires `allow_head` and read and write permissions for the target namespaces.

```json
{
  "clients": [
    {"uri": "spiffe://example.org/apps/game", "read": ["players"], "write": ["players"], "allow_head": true},
    {"uri": "spiffe://example.org/apps/reporting", "read": ["players"]}
  ]
}
```

Without a policy, any client with a certificate from the client CA can access the data API. The production profiles allow up to 1024 concurrent unary requests and 256 concurrent streams per standalone instance or router by default. Configure these limits with `KVSTORE_MAX_INFLIGHT_RPCS` and `KVSTORE_MAX_STREAMS`. Requests that exceed a limit receive `RESOURCE_EXHAUSTED` before execution.

### Named API keys

Standalone instances and routers support API keys with or without TLS. `KVSTORE_API_KEYS_DIR` enables API key checks on the data endpoint. In the TLS Compose profile, the registry can live in the TLS bind mount. The container must be able to read the files as UID or GID 65532. On Linux, new entries inherit the group of the example directory.

```sh
keydir="$KVSTORE_TLS_DIR/api-keys"
install -d -m 2750 -g 65532 "$keydir"
kvstore apikey create --dir "$keydir" --name reporting --read --namespace players
kvstore apikey create --dir "$keydir" --name game --read --write --namespace players
kvstore apikey list --dir "$keydir"
kvstore apikey revoke --dir "$keydir" --name reporting
```

`create` prints the random key once. The registry stores its SHA-256 digest and permissions. `list` shows names and permissions. `--read` grants `Get`, `Head`, `Scan`, and `Watch`. `--write` grants `Set`, `Delete`, and `BatchWrite`. `Head` returns a global cursor. Repeat `--namespace` to restrict data access to specific namespaces. By default, permissions cover all namespaces.

```sh
export KVSTORE_API_KEYS_DIR=/etc/kvstore/tls/api-keys
# Set this for API-key-only access without client certificates.
export KVSTORE_TLS_CLIENT_CA_FILE=''
```

Clients send `authorization: Bearer <key>` as gRPC metadata. `kvbench` and `kvimport` accept this key through `KVSTORE_API_KEY`. For TLS, the server needs `server.crt` and `server.key`. Use `-tls` in the tools to use the system trust store or `-tls-ca` to use a custom CA file. With a client CA configured, existing clients can keep using their certificates. If a request includes a bearer key, its permissions apply. KVStore rejects an invalid bearer key even if the client certificate is valid. For API-key-only access, leave the certificate policy `KVSTORE_ACCESS_POLICY_FILE` empty.

Each endpoint reloads changes to the directory once per second. Revoking a key also closes its open streams. With multiple routers, distribute the current registry to every router and allow time for revocation to take effect at each endpoint. A read-only server mount is sufficient for local key administration. To manage keys remotely with `kvctl`, give the endpoint write access to the registry. [Terminal console setup](kvctl.md)

#### Standalone without TLS

For a local or private standalone endpoint, `compose.solo.apikey.yml` uses the registry without TLS. It binds to `127.0.0.1:50051` by default. Without TLS, the key and payload travel in plaintext. Use the TLS profile for public endpoints.

```sh
export KVSTORE_API_KEYS_HOST_DIR="$PWD/data/kvstore-api-keys"
install -d -m 2770 -g 65532 "$KVSTORE_API_KEYS_HOST_DIR"
kvstore apikey create --dir "$KVSTORE_API_KEYS_HOST_DIR" --name minecraft --read --write
docker compose -f compose.yml -f compose.solo.apikey.yml up -d --build
```

When running the standalone server or router directly, set `KVSTORE_API_KEYS_DIR` and leave the TLS file variables empty. For this endpoint, run `kvbench` and `kvimport` with `KVSTORE_API_KEY` and omit the TLS flags.

Create a management key on the host for the first TUI sign-in. Use the directory the server reads its keys from. The command also works with an empty registry.

```sh
kvstore apikey create --dir "$KVSTORE_API_KEYS_HOST_DIR" --name admin --manage --read --write
kvctl --addr 127.0.0.1:50051
```

### Endpoint validation

```sh
kvbench -addr kvstore.example.org:5443 -namespace staging-perf \
  -tls-ca server-ca.crt -tls-cert client.crt -tls-key client.key \
  -mode mixed -consistency strong -read-percent 50 -batch-size 16 \
  -concurrency 256 -connections 16 -keys 16384 -target-ops 23000 -duration 5m -json
```

In staging, also test a connection without a client certificate and one with a certificate from an untrusted CA. If an access policy is configured, test a client whose valid certificate grants no access to the target namespace. The first `Get` from `kvbench` checks connectivity before the benchmark writes any data.

### Optional monitoring with Prometheus

Standalone servers, Raft voters, and sharding routers start with `/metrics` disabled. In Compose, set `KVSTORE_METRICS_TOKEN` to enable the authenticated endpoint for all KVStore services in the selected profile. In Kubernetes, use the optional `kvstore-metrics` Secret. The Raft and router admin ports remain available for bootstrap, replica management, and diagnostics. [Kubernetes monitoring](kubernetes/README.md#metrics)

To scrape a standalone instance from the Compose host, add `compose.solo.metrics.yml`. The overlay publishes the endpoint on `127.0.0.1:17110`. If an existing standalone deployment already has a scraper on this port, add the overlay the next time you run `up` and keep the existing token. A scraper on the same Compose network can use `kvstore:7002` directly with the token configured.

```sh
# On first setup, generate a token and save it in the secret store.
export KVSTORE_METRICS_TOKEN="$(openssl rand -hex 32)"
docker compose -p kvstore-solo \
  -f compose.yml -f compose.solo.production.yml -f compose.solo.metrics.yml \
  up -d --no-build
```

On the Compose host, use `deploy/prometheus/prometheus.yml` for shards or `deploy/prometheus/prometheus-standalone.yml` for a standalone instance. The development Raft cluster publishes its voter admin ports on loopback ports `17001` through `17003`. Save `KVSTORE_METRICS_TOKEN` in a file named `token` next to the configuration and grant read access only to the scraper. The Prometheus target must report `UP`. The rules check scrape health, shard routes, mutation failures, p99 latency, resource limits, low free disk space, and Pebble compaction failures. On an isolated staging instance, temporarily set the unary limit to `1` and use 32 concurrent writers to repeatedly trigger `ResourceExhausted`. Confirm that Alertmanager receives `KVStoreResourceExhausted`, then restore the production limit.

## Offline backup and restore

### Solo

Stop write traffic and stop the container. Prevent manual and automatic project restarts during backup and restore. The backup script finds the persistent `/data` volume from the stopped Compose service. Use a new backup directory. Afterward, store an encrypted copy of the entire directory, including checksums, off the host.

```sh
docker compose -p kvstore-solo -f compose.yml -f compose.solo.production.yml stop kvstore
bash scripts/backup-standalone-linux.sh backup kvstore-solo /path/to/new/kvstore-backup
docker compose -p kvstore-solo -f compose.yml -f compose.solo.production.yml start kvstore
```

After disk loss, create a new Compose project on another host with an empty volume and the same image digest. `create` prepares the container so the restore script can identify and verify the target volume.

```sh
docker compose -p kvstore-solo-restore -f compose.yml -f compose.solo.production.yml create --no-build kvstore
bash scripts/backup-standalone-linux.sh restore kvstore-solo-restore /path/to/kvstore-backup
docker compose -p kvstore-solo-restore -f compose.yml -f compose.solo.production.yml start kvstore
```

Before admitting clients, check that values, revisions, and active TTL deadlines on the restored endpoint match the backup. For an isolated rehearsal, set `KVSTORE_TEST_IMAGE` to the locally available image and run `bash scripts/test-backup-standalone-linux.sh`. If the standalone host fails, the service remains offline during recovery. After disk loss, the last off-host backup determines the recovery point.

### Shards

This procedure backs up all nine Compose volumes together. It preserves Raft logs, Pebble data, revisions, and routing metadata. Keep the source deployment stopped for the entire backup. Before stopping it, finish any slot migrations in progress and block new client traffic.

```sh
docker compose -p kvstore-live -f compose.sharded.yml stop
bash scripts/backup-sharded-linux.sh backup kvstore-live /path/to/new/kvstore-backup
docker compose -p kvstore-live -f compose.sharded.yml start
```

If running with mTLS, specify both Compose files for `stop` and `start`. Store an encrypted copy of the resulting directory, including `SHA256SUMS`, off the host. Use a new backup directory. If the backup fails, retry with another new directory. The archives contain the complete application data.

Restore on an isolated target host using the same image digest and internal Compose service names. The project name can differ. Create the target containers and leave them stopped until the restore finishes.

```sh
docker compose -p kvstore-restore -f compose.sharded.yml create --no-build
bash scripts/backup-sharded-linux.sh restore kvstore-restore /path/to/kvstore-backup
docker compose -p kvstore-restore -f compose.sharded.yml start
```

The script verifies all nine checksums and requires empty target volumes. If a restore is interrupted, start over in a new empty project. After startup, wait for all services to become ready. Before admitting clients, compare known keys, raw byte values, and revisions from several shards against the records captured before the backup. The helper uses GNU tar and GNU coreutils from `debian:bookworm-slim`. Set `KVSTORE_BACKUP_HELPER_IMAGE` to pin its image digest.

## Validation on target hardware

Run this from the repository on a dedicated test host. The sustained load test uses its own containers and volumes. It starts three voters per shard and writes continuously for five minutes.

```sh
bash scripts/benchmark-final-slo-linux.sh
```

To pass, the test requires at least 10,000 STRONG GETs/s and 10,000 replicated SETs/s, with p99 latency below 100 ms for both. It also requires zero errors, zero missing records, and successful Pebble compactions with no compaction errors. The results directory contains client measurements, Raft WAL and Pebble metrics, and CPU and I/O pressure. The test stops its Compose containers and keeps their volumes for diagnosis. Use the project name recorded in the results to remove the test project afterward.

Set `DURATION_SECONDS=1800` for a longer run. For production-like validation, also test mTLS access with a valid client certificate, without one, and with a certificate from an untrusted client CA. If Prometheus is enabled, trigger an alert. Rehearse a restore onto fresh volumes.

Run the recovery test with the same locally available image and Docker Compose 2.24.4 or later. It uses its own project with random loopback ports and removes its test volumes after the run.

```sh
KVSTORE_TEST_IMAGE=kvstore:multiraft-dev go test -tags integration -count=1 -v ./cmd/kvbench -run '^TestDockerClusterRecovery$'
```

The recovery test sends continuous batch writes through the router, abruptly stops a shard leader, and waits for the new leader to acknowledge writes. During the subsequent quorum loss, it checks that the affected shard rejects STRONG reads and stops acknowledging writes, and that the other shards still accept writes. It starts the original bootstrap voter with an empty replacement volume and checks the replicated state with a local read. After abruptly restarting the entire deployment, the test compares all acknowledged keys, values, and revisions from the client with the persisted state.

## Multiple hosts and Kubernetes

Place one voter from each of the three groups on each of three physical hosts. Within each group, configure the same bootstrap voter and admin seed list on all three voters. Keep `KVSTORE_NODE_ID`, `KVSTORE_RAFT_ADVERTISE_ADDR`, `KVSTORE_GRPC_ADVERTISE_ADDR`, and `KVSTORE_ADMIN_ADVERTISE_ADDR` stable for each voter. If several voters publish ports from the same host, assign each voter distinct ports. Configure the router's `KVSTORE_SHARDS` with the gRPC endpoints grouped the same way. The single-host Compose deployment is suitable for local validation. Before deploying across hosts, run a load test through a production-like router and test the loss of an entire failure domain in staging.

Each of the three groups uses `KVSTORE_RAFT_BOOTSTRAP_MODE=explicit` and `KVSTORE_CLUSTER_MIN_VOTERS=3`. Bootstrap each group only during initial installation. After losing a host, use the same voter identities and reachable addresses on the replacement host. First make sure the old host has stopped using those identities. Run the public gRPC endpoint with mTLS through at least two routers on separate hosts and a redundant load balancer.

The [Kubernetes profiles](kubernetes/README.md) cover a standalone instance, a single Raft group, and the full sharded topology. Validation includes an actual worker outage under write load, a replacement voter catching up on fresh storage, and a controlled rollout.

The external recovery test sends batch writes to a dedicated staging namespace during a window it announces before the test. It reports the longest gap between acknowledgements per shard and then verifies all acknowledged values and revisions over mTLS. Shut down the host during the indicated window and independently confirm the outage from infrastructure events. The operator controls the host shutdown.

```sh
KVSTORE_HOST_TEST_ADDR=kvstore-staging.example.org:5443 \
KVSTORE_HOST_TEST_NAMESPACE=staging-host-recovery \
KVSTORE_HOST_TEST_TLS_CA=server-ca.crt \
KVSTORE_HOST_TEST_TLS_CERT=client.crt \
KVSTORE_HOST_TEST_TLS_KEY=client.key \
KVSTORE_HOST_TEST_WINDOW=2m \
  go test -tags integration -count=1 -v ./cmd/kvbench -run '^TestExternalHostRecovery$'
```

## Importing KVStore v1 from MongoDB

The mapping follows the v1 implementations in `kvstore-js` and `kvstore-go`. Before importing, finish v1 transaction recovery and reconcile counters between Redis and MongoDB. Pause writes to both the source and the destination for the entire import. The importer aborts if the v1 transaction journal contains entries. Read-only MongoDB access is sufficient.

| MongoDB | Target namespace | Target key | Target value |
| --- | --- | --- | --- |
| `kv_store_<store>` | `<store>` | `key` | UTF-8 bytes of `value` |
| `single_value` | `legacy-v1-single` | `storeName` | UTF-8 bytes of `value` |
| `kv_counters` | `legacy-v1-counter/<storeName>` | `key` | Decimal number encoded as UTF-8 bytes |

Set `KVSTORE_MONGO_URI` in the environment. `-dry-run` validates the source. With `-addr`, it also reads the destination and counts new keys, matching values, and conflicts. A conflict makes the dry run fail. The command defaults to a dry run. Use `-apply` to write after source validation.

```sh
go run ./cmd/kvimport -database v1_database -dry-run
go run ./cmd/kvimport -database v1_database -addr 127.0.0.1:50081 -dry-run
go run ./cmd/kvimport -database v1_database -addr 127.0.0.1:50081 -apply
```

Use `-dry-run -addr … -show-conflicts` to print the namespace and key of each conflict. The output omits values. If source counts change during a run, the importer reports an error. Previously acknowledged writes stay committed.

For an mTLS endpoint, pass `-tls-ca`, `-tls-cert`, and `-tls-key`, and use the DNS name from the server certificate. If an import fails after acknowledging writes, rerun it while the data is still frozen. It skips matching destination values and aborts on conflicts.

The import preserves active TTLs as absolute Unix timestamps in milliseconds. If both `expiresAt` and `expiresAtDate` are set, the earlier deadline applies. The importer skips records that have already expired. Use `-skip-expiring` for a rehearsal that deliberately omits TTL records. The importer copies the stored MongoDB counter values. The v1 counter API and Redis coordination are outside the import scope.

Before the first TTL import, upgrade all routers and Raft voters to a TTL-capable version and synchronize their clocks. `Get` and `Scan` return the deadline and hide the value at the stored time. `Watch` returns the same deadline so client caches can discard the value when it expires. The server hides the value immediately at the deadline. Expired bytes remain on disk until an overwrite or explicit deletion.

### Repeatable import rehearsal

The isolated Compose cluster uses fresh volumes per project and publishes only the router on loopback. Start it with `./scripts/start-import-rehearsal.ps1` in PowerShell 7. The default port is `18082`. To run again with empty volumes, choose a new project name and a free port. For example, run `./scripts/start-import-rehearsal.ps1 -Project kvstore-import-rehearsal-run2 -Port 18084 -SkipBuild`. Reusing a project name retains its data.

For a consistent capture, use an unchanged MongoDB test copy or pause v1 writers during export. The exporter reads across multiple collections. Exporting while writes continue creates a fixed rehearsal file whose collections can represent different points in time. Set `KVSTORE_MONGO_URI` as shown above. The snapshot contains application data, so keep it in a private directory. Git and the Docker build context exclude `data/` and files ending in `.kvsnapshot`.

```powershell
New-Item -ItemType Directory -Force ./data/import-rehearsal | Out-Null
$snapshot = "./data/import-rehearsal/betterattack-$(Get-Date -Format yyyyMMdd-HHmmss).kvsnapshot"
$target = '127.0.0.1:18084'
go run ./cmd/kvimport -database 'betterattack' -export-file $snapshot
```

For a fresh TTL rehearsal cluster on the example port, run `./scripts/start-import-rehearsal.ps1 -Project kvstore-import-rehearsal-ttl -Port 18084`. The exporter requires a new filename and reports the number of included TTL records as `expiring`. If you created an older snapshot with `-skip-expiring`, export it again with TTL records included. Use the same snapshot file for all the remaining commands. Only the export needs a MongoDB connection.

```powershell
go run ./cmd/kvimport -input-file $snapshot -addr $target -dry-run -show-conflicts
go run ./cmd/kvimport -input-file $snapshot -addr $target -apply
go run ./cmd/kvimport -input-file $snapshot -addr $target -verify -show-conflicts
```

Run each command after the previous one succeeds. On a fresh destination, the dry-run counts `would_write` and `expired_since_validation` should add up to the snapshot record count. After the import, verification must report `missing=0 changed=0 extra=0`. The sum of `matching + expired_since_export` should equal the snapshot record count. Verification compares values and deadlines in every namespace captured in the snapshot. Use fresh target volumes to rule out extra data in namespaces beyond the ones the importer scans. Reserve the destination for the importer during the rehearsal and verification.

The snapshot importer scans the destination once per namespace. It aborts on conflicts before writing and inserts missing entries in bounded Raft batches. After an interruption, stop the running importer before applying the same snapshot again. The next destination scan recognizes acknowledged records as matches. If a batch fails, some shards may already have acknowledged writes. The importer reports those acknowledgements and stops so the next run can reconcile the destination before writing.
