#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C
# Isolated Multi-Raft benchmark; NEVER stops unrelated Docker services.
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
for tool in docker date ss; do command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 1; }; done
docker info >/dev/null
docker compose version >/dev/null

RUNS="${RUNS:-2}"
CONCURRENCIES="${CONCURRENCIES:-256 512}"
DURATION_SECONDS="${DURATION_SECONDS:-10}"
WARMUP_SECONDS="${WARMUP_SECONDS:-2}"
KEYSPACE="${KEYSPACE:-16384}"
VALUE_SIZE="${VALUE_SIZE:-256}"
CONNECTIONS="${CONNECTIONS:-16}"
NODE_MEMORY="${NODE_MEMORY:-2g}"
[[ "$RUNS" =~ ^[1-9][0-9]*$ ]] || { echo 'RUNS must be positive' >&2; exit 2; }
# These ports are only bound to localhost; don't interrupt any existing services.
if ss -H -lnt | awk '$4 ~ /:(50081|1710[1-9]|17110)$/ {print; clash=1} END {exit clash ? 0 : 1}'; then
 echo 'Sharded benchmark port already in use; nothing started.' >&2; exit 1
fi
RUN_ID="sharded-$(date -u +%Y%m%dT%H%M%SZ)-$$"
PROJECT="kvbench-${RUN_ID,,}"
OUT="$ROOT/benchmarks/results/live-linux/$RUN_ID"
mkdir -p "$OUT" "$ROOT/bin"
echo "$PROJECT" > "$OUT/project-name.txt"
export KVSTORE_JOIN_TOKEN="${KVSTORE_JOIN_TOKEN:-$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')}"
export KVSTORE_ROUTER_ADMIN_TOKEN="${KVSTORE_ROUTER_ADMIN_TOKEN:-$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')}"
cat > "$OUT/override.yml" <<YAML
x-limits: &limits
  image: kvstore:multiraft-dev
  mem_limit: ${NODE_MEMORY}
  pids_limit: 512
  logging:
    driver: local
    options: {max-size: "10m", max-file: "2"}
services:
YAML
for s in 0 1 2; do
 for n in 0 1 2; do
  printf '  s%sn%s:\n    <<: *limits\n' "$s" "$n" >> "$OUT/override.yml"
 done
done
cat >> "$OUT/override.yml" <<YAML
  router:
    image: kvstore:multiraft-dev
    mem_limit: 1g
    pids_limit: 512
    logging:
      driver: local
      options: {max-size: "10m", max-file: "2"}
YAML
compose=(docker compose -p "$PROJECT" -f "$ROOT/compose.sharded.yml" -f "$OUT/override.yml")
cleanup() {
 local rc=$?
 trap - EXIT
 "${compose[@]}" logs --tail 500 > "$OUT/sharded-logs.txt" 2>&1 || true
 if command -v curl >/dev/null; then
  for s in 0 1 2; do for n in 0 1 2; do
   port=$((17101+s*3+n))
   curl -fsS --max-time 2 -H "Authorization: Bearer $KVSTORE_JOIN_TOKEN" \
    "http://127.0.0.1:${port}/v1/cluster/performance" \
    > "$OUT/shard${s}-node${n}-performance.json" 2>/dev/null || true
  done; done
 fi
 docker ps --format '{{.ID}} {{.Names}} {{.Status}}' > "$OUT/containers-after.txt" 2>/dev/null || true
 if [[ "${KEEP_RUNNING:-0}" != 1 || "$rc" -ne 0 ]]; then "${compose[@]}" stop >/dev/null 2>&1 || true; fi
 echo "Results: $OUT"
 echo "Only isolated project $PROJECT was stopped; volumes preserved."
 exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
{
 date -u '+UTC %Y-%m-%d %H:%M:%S'
 echo "git_head=$(git rev-parse HEAD 2>/dev/null || echo unavailable)"
 echo "git_changes=$(git status --porcelain | wc -l)"
 echo "runs=$RUNS concurrencies=$CONCURRENCIES duration=$DURATION_SECONDS warmup=$WARMUP_SECONDS keys=$KEYSPACE bytes=$VALUE_SIZE connections=$CONNECTIONS"
 echo "raft_inflight=${KVSTORE_RAFT_MAX_INFLIGHT:-8} raft_max_append=${KVSTORE_RAFT_MAX_APPEND_ENTRIES:-128} raft_commit_timeout=${KVSTORE_RAFT_COMMIT_TIMEOUT:-50ms}"
 echo "read_window=${KVSTORE_READ_BATCH_WINDOW:-300us} read_verify_parallelism=${KVSTORE_READ_VERIFY_PARALLELISM:-2}"
 echo "write_window=${KVSTORE_WRITE_BATCH_WINDOW:-1500us} write_target=${KVSTORE_WRITE_BATCH_TARGET:-24} write_max_window=${KVSTORE_WRITE_BATCH_MAX_WINDOW:-5ms}"
 uname -a
 command -v lscpu >/dev/null && lscpu || true
} > "$OUT/system.txt"
"${compose[@]}" config -q
docker ps --format '{{.ID}} {{.Names}} {{.Status}}' > "$OUT/containers-before.txt"
if [[ "${SKIP_BUILD:-0}" != 1 ]]; then
 echo '== Building image (9 backend containers + router share one image) =='
 docker build --platform linux/amd64 -t kvstore:multiraft-dev .
fi
# Benchmark binary built by the SAME Dockerfile/protobuf version as the server.
container_id="$(docker create kvstore:multiraft-dev)"
if ! docker cp "$container_id:/usr/local/bin/kvbench" "$ROOT/bin/kvbench-sharded"; then
 docker rm "$container_id" >/dev/null
 exit 1
fi
docker cp "$container_id:/usr/local/bin/kvshardctl" "$ROOT/bin/kvshardctl-sharded" >/dev/null
 docker rm "$container_id" >/dev/null
 chmod +x "$ROOT/bin/kvbench-sharded" "$ROOT/bin/kvshardctl-sharded"
echo '== Starting 9 Raft backends and 1 sharding router =='
"${compose[@]}" up -d --no-build
router_id="$("${compose[@]}" ps -q router)"
[[ -n "$router_id" ]] || { echo 'Router container not created' >&2; exit 1; }
echo '== Waiting for all three Raft groups to elect/register leaders =='
ready=0
for i in $(seq 1 90); do
 state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' "$router_id" 2>/dev/null || true)"
 if [[ "$state" == healthy ]]; then ready=1; break; fi
 sleep 2
done
[[ "$ready" == 1 ]] || { echo 'Router not ready; inspect sharded-logs.txt' >&2; exit 1; }
echo '== Running 50/50 STRONG point-operation workloads =='
for concurrency in $CONCURRENCIES; do
 for ((run=1;run<=RUNS;run++)); do
  output="$OUT/sharded-mixed-strong-c${concurrency}-r${run}.json"
  # Capture host I/O pressure and per-shard Raft counters per individual run.
  cat /proc/pressure/io > "$OUT/io-before-c${concurrency}-r${run}.txt" 2>/dev/null || true
  cat /proc/pressure/cpu > "$OUT/cpu-before-c${concurrency}-r${run}.txt" 2>/dev/null || true
  cat /proc/pressure/memory > "$OUT/memory-before-c${concurrency}-r${run}.txt" 2>/dev/null || true
  cat /proc/diskstats > "$OUT/diskstats-before-c${concurrency}-r${run}.txt" 2>/dev/null || true
  snapshot_metrics() {
   local prefix="$1"
   if command -v curl >/dev/null; then
    for s in 0 1 2; do for n in 0 1 2; do
     local port=$((17101+s*3+n))
     curl -fsS --max-time 2 -H "Authorization: Bearer $KVSTORE_JOIN_TOKEN" \
       "http://127.0.0.1:${port}/v1/cluster/performance" \
       > "$OUT/${prefix}-s${s}n${n}.json" 2>/dev/null &
    done; done
    wait || true
   fi
  }
  snapshot_metrics "before-c${concurrency}-r${run}"
  echo "[sharded/strong/mixed] workers=$concurrency run=$run/$RUNS"
  "$ROOT/bin/kvbench-sharded" -addr 127.0.0.1:50081 -mode mixed \
   -consistency strong -read-percent 50 -duration "${DURATION_SECONDS}s" \
   -warmup "${WARMUP_SECONDS}s" -concurrency "$concurrency" \
   -connections "$CONNECTIONS" -keys "$KEYSPACE" \
   -value-size "$VALUE_SIZE" -json > "${output}.tmp"
  mv "${output}.tmp" "$output"
  snapshot_metrics "after-c${concurrency}-r${run}"
  cat /proc/pressure/io > "$OUT/io-after-c${concurrency}-r${run}.txt" 2>/dev/null || true
  cat /proc/pressure/cpu > "$OUT/cpu-after-c${concurrency}-r${run}.txt" 2>/dev/null || true
  cat /proc/pressure/memory > "$OUT/memory-after-c${concurrency}-r${run}.txt" 2>/dev/null || true
  cat /proc/diskstats > "$OUT/diskstats-after-c${concurrency}-r${run}.txt" 2>/dev/null || true
  if command -v jq >/dev/null; then
   jq -er '"  GET: \(.get.ops_sec|floor)/s p99=\(.get.latency.p99_ms)ms; SET: \(.set.ops_sec|floor)/s p99=\(.set.latency.p99_ms)ms; errors=\(.get.errors+.set.errors) missing=\(.get.not_found)"' "$output"
   jq -e '(.get.errors+.set.errors)==0 and .get.not_found==0 and .get.ops_sec>0 and .set.ops_sec>0' "$output" >/dev/null || { echo "Benchmark errors: $output" >&2; exit 1; }
  fi
 done
done
echo '== Finished. Upload the entire result directory as ZIP. =='
