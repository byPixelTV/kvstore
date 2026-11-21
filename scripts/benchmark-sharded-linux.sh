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
BATCH_SIZE="${BATCH_SIZE:-1}"
TARGET_OPS="${TARGET_OPS:-0}"
METRICS_INTERVAL="${METRICS_INTERVAL:-0}"
REQUIRE_SLO="${REQUIRE_SLO:-0}"
KVSTORE_IMAGE="${KVSTORE_IMAGE:-kvstore:multiraft-dev}"
KVSTORE_SHARDED_GRPC_PORT="${KVSTORE_SHARDED_GRPC_PORT:-50081}"
export KVSTORE_IMAGE KVSTORE_SHARDED_GRPC_PORT
if [[ "$REQUIRE_SLO" == 1 ]]; then
 for tool in jq curl; do command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 1; }; done
fi
[[ "$METRICS_INTERVAL" =~ ^[0-9]+$ ]] || { echo 'METRICS_INTERVAL must be a nonnegative integer' >&2; exit 2; }
[[ "$DURATION_SECONDS" =~ ^[1-9][0-9]*$ && "$WARMUP_SECONDS" =~ ^(0|[1-9][0-9]*)$ ]] ||
 { echo 'DURATION_SECONDS must be positive and WARMUP_SECONDS nonnegative' >&2; exit 2; }
[[ "$KVSTORE_SHARDED_GRPC_PORT" =~ ^[1-9][0-9]{0,4}$ ]] && (( KVSTORE_SHARDED_GRPC_PORT <= 65535 )) ||
 { echo 'KVSTORE_SHARDED_GRPC_PORT must be 1..65535' >&2; exit 2; }
[[ "$TARGET_OPS" =~ ^[0-9]+$ ]] || { echo "TARGET_OPS must be a nonnegative integer" >&2; exit 2; }
[[ "$BATCH_SIZE" =~ ^[1-9][0-9]*$ ]] && (( BATCH_SIZE <= 128 )) || { echo "BATCH_SIZE must be 1..128" >&2; exit 2; }
NODE_MEMORY="${NODE_MEMORY:-2g}"
[[ "$RUNS" =~ ^[1-9][0-9]*$ ]] || { echo 'RUNS must be positive' >&2; exit 2; }
# These ports are only bound to localhost; don't interrupt any existing services.
if ss -H -lnt | awk -v port="$KVSTORE_SHARDED_GRPC_PORT" '$4 ~ (":(" port "|1710[1-9]|17110)$") {print; clash=1} END {exit clash ? 0 : 1}'; then
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
 if [[ -n "${bench_pid:-}" ]]; then
  kill "$bench_pid" 2>/dev/null || true
  wait "$bench_pid" 2>/dev/null || true
 fi
 if [[ -n "${container_id:-}" ]]; then
  docker rm "$container_id" >/dev/null 2>&1 || true
 fi
 rm -f "$OUT/kvbench" "$OUT/kvshardctl"
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
 if [[ "${KEEP_RUNNING:-0}" != 1 || "$rc" -ne 0 ]]; then
  "${compose[@]}" stop >/dev/null 2>&1 || true
  echo "Only isolated project $PROJECT was stopped; volumes preserved."
 fi
 echo "Results: $OUT"
 exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
{
 date -u '+UTC %Y-%m-%d %H:%M:%S'
 echo "git_head=$(git rev-parse HEAD 2>/dev/null || echo unavailable)"
 echo "git_changes=$(git status --porcelain | wc -l)"
 echo "runs=$RUNS concurrencies=$CONCURRENCIES duration=$DURATION_SECONDS warmup=$WARMUP_SECONDS keys=$KEYSPACE bytes=$VALUE_SIZE connections=$CONNECTIONS batch_size=$BATCH_SIZE target_ops=$TARGET_OPS"
 echo "image=$KVSTORE_IMAGE"
 env | sort | awk '/^KVSTORE_(READ_BATCH_|READ_VERIFY_|WRITE_BATCH_|WRITE_MAX_QUEUE_|RAFT_MAX_|RAFT_COMMIT_|RAFT_LOG_CACHE_|SHARD_DISCOVERY_|ROUTER_CONNECTIONS=)/'
 uname -a
 command -v lscpu >/dev/null && lscpu || true
} > "$OUT/system.txt"
"${compose[@]}" config -q
docker ps --format '{{.ID}} {{.Names}} {{.Status}}' > "$OUT/containers-before.txt"
if [[ "${SKIP_BUILD:-0}" != 1 ]]; then
 echo '== Building image (9 backend containers + router share one image) =='
 docker build -t "$KVSTORE_IMAGE" .
fi
docker image inspect --format '{{.Id}} {{.Os}}/{{.Architecture}}' "$KVSTORE_IMAGE" >> "$OUT/system.txt"
# Use client tools from the same build. Keep per-run copies to avoid replacing
# an active benchmark executable and preserve the paths used by resharding.
container_id="$(docker create "$KVSTORE_IMAGE")"
docker cp "$container_id:/usr/local/bin/kvbench" "$OUT/kvbench"
docker cp "$container_id:/usr/local/bin/kvshardctl" "$OUT/kvshardctl"
docker rm "$container_id" >/dev/null
unset container_id
chmod +x "$OUT/kvbench" "$OUT/kvshardctl"
mv "$OUT/kvshardctl" "$ROOT/bin/kvshardctl-sharded"
cp "$OUT/kvbench" "$ROOT/bin/kvbench-sharded"
echo '== Starting 9 Raft backends and 1 sharding router =='
"${compose[@]}" up -d --no-build
router_id="$("${compose[@]}" ps -q router)"
[[ -n "$router_id" ]] || { echo 'Router container not created' >&2; exit 1; }
echo '== Waiting for all three Raft groups to elect/register leaders =='
ready=0
for i in $(seq 1 90); do
 state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' "$router_id" 2>/dev/null || true)"
 if [[ "$state" == healthy ]]; then
  ready=1
  if [[ "$REQUIRE_SLO" == 1 ]]; then
   for port in {17101..17109}; do
    if ! curl -fsS --max-time 2 -H "Authorization: Bearer $KVSTORE_JOIN_TOKEN" \
     "http://127.0.0.1:${port}/v1/cluster/performance" 2>/dev/null | jq -e '.voters == 3' >/dev/null; then
     ready=0
     break
    fi
   done
  fi
  [[ "$ready" == 0 ]] || break
 fi
 sleep 2
done
[[ "$ready" == 1 ]] || { echo 'Router not ready; inspect sharded-logs.txt' >&2; exit 1; }
echo '== Running 50/50 STRONG point-operation workloads =='
for concurrency in $CONCURRENCIES; do
 for ((run=1;run<=RUNS;run++)); do
  output="$OUT/sharded-mixed-strong-b${BATCH_SIZE}-c${concurrency}-r${run}.json"
  # Capture host I/O pressure and per-shard Raft counters per individual run.
  cat /proc/pressure/io > "$OUT/io-before-c${concurrency}-r${run}.txt" 2>/dev/null || true
  cat /proc/pressure/cpu > "$OUT/cpu-before-c${concurrency}-r${run}.txt" 2>/dev/null || true
  cat /proc/pressure/memory > "$OUT/memory-before-c${concurrency}-r${run}.txt" 2>/dev/null || true
  cat /proc/diskstats > "$OUT/diskstats-before-c${concurrency}-r${run}.txt" 2>/dev/null || true
  snapshot_metrics() {
   local prefix="$1"
   local port pid
   local -a pids=()
   if command -v curl >/dev/null; then
    local s n
    for s in 0 1 2; do for n in 0 1 2; do
     port=$((17101+s*3+n))
     curl -fsS --max-time 2 -H "Authorization: Bearer $KVSTORE_JOIN_TOKEN" \
       "http://127.0.0.1:${port}/v1/cluster/performance" \
       > "$OUT/${prefix}-s${s}n${n}.json" 2>/dev/null &
     pids+=("$!")
    done; done
    for pid in "${pids[@]}"; do wait "$pid" || true; done
   fi
  }
  snapshot_metrics "before-c${concurrency}-r${run}"
  echo "[sharded/strong/mixed] batch_size=$BATCH_SIZE workers=$concurrency run=$run/$RUNS"
  "$OUT/kvbench" -addr "127.0.0.1:$KVSTORE_SHARDED_GRPC_PORT" -mode mixed \
   -consistency strong -read-percent 50 -duration "${DURATION_SECONDS}s" \
   -warmup "${WARMUP_SECONDS}s" -concurrency "$concurrency" \
   -connections "$CONNECTIONS" -keys "$KEYSPACE" \
   -value-size "$VALUE_SIZE" -batch-size "$BATCH_SIZE" -target-ops "$TARGET_OPS" -json > "${output}.tmp" &
  bench_pid=$!
  if (( METRICS_INTERVAL > 0 )); then
   for ((second=1; second<=DURATION_SECONDS+WARMUP_SECONDS; second++)); do
    kill -0 "$bench_pid" 2>/dev/null || break
    sleep 1
    if (( second % METRICS_INTERVAL == 0 )); then
     snapshot_metrics "during-c${concurrency}-r${run}-t${second}"
    fi
   done
  fi
  wait "$bench_pid"
  unset bench_pid
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
  if [[ "$REQUIRE_SLO" == 1 ]]; then
   compactions=0
   for s in 0 1 2; do for n in 0 1 2; do
    delta="$(jq -es '
      if length == 2 and all(.[]; .voters == 3 and (.pebble.compactions | type == "number")) and
        .[1].pebble.compactions >= .[0].pebble.compactions and
        all(.[]; .pebble.failed_compactions == 0)
      then .[1].pebble.compactions - .[0].pebble.compactions else error("incomplete or unhealthy Pebble metrics") end
    ' "$OUT/before-c${concurrency}-r${run}-s${s}n${n}.json" "$OUT/after-c${concurrency}-r${run}-s${s}n${n}.json")"
    compactions=$((compactions + delta))
   done; done
   echo "Pebble compactions across replicas: $compactions"
   (( compactions > 0 )) || { echo 'No Pebble compactions observed' >&2; exit 1; }
   jq -e '.get.ops_sec >= 10000 and .set.ops_sec >= 10000 and .get.latency.p99_ms < 100 and .set.latency.p99_ms < 100' "$output" >/dev/null ||
    { echo "SLO=NOT_MET ($output)" >&2; exit 1; }
   echo 'SLO=PASS'
  fi
 done
done
echo '== Finished. Upload the entire result directory as ZIP. =='
