#!/usr/bin/env bash
set -Eeuo pipefail

# Linux bare-metal benchmark runner for an isolated KVStore project.
# Place this file in scripts/ of the KVStore repository.
# Never stops/removes existing non-benchmark containers; never prunes Docker.
# Compiles KVStore and the benchmark client on this Linux host.
# CPU: unlimited. RAM: limited for benchmark containers to avoid host OOM.

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

[[ "$(uname -s)" == Linux ]] || { echo 'This script must run on Linux.' >&2; exit 1; }
for cmd in docker ss date; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "Missing: $cmd" >&2; exit 1; }
done
docker info >/dev/null 2>&1 || { echo 'Docker is not available.' >&2; exit 1; }
docker compose version >/dev/null || { echo 'Docker Compose v2 is required.' >&2; exit 1; }
[[ -f ./compose.yml && -f ./compose.cluster.yml && -f ./Dockerfile && -f ./go.mod ]] || {
  echo 'Run this from an extracted full KVStore repository.' >&2; exit 1;
}
[[ -f ./deploy/envoy/cluster.yaml ]] || { echo 'Missing Envoy config.' >&2; exit 1; }

# Abort if ports for our own isolated test stack are already in use.
if ss -H -lnt | awk '$4 ~ /:(50051|50061|50062|50063|17001|17002|17003)$/ { print; clash=1 } END { exit clash ? 0 : 1 }'; then
  echo 'One or more benchmark ports are in use. Nothing was changed.' >&2
  exit 1
fi

RUN_ID="write-$(date -u +%Y%m%dT%H%M%SZ)-$$"
OUT="$ROOT/benchmarks/results/live-linux/$RUN_ID"
mkdir -p "$OUT" "$ROOT/bin"
STANDALONE_PROJECT="kvbench-standalone-$RUN_ID"
CLUSTER_PROJECT="kvbench-cluster-$RUN_ID"
# Docker Compose project identifiers: lowercase letters, digits and dashes only.
STANDALONE_PROJECT="${STANDALONE_PROJECT,,}"
CLUSTER_PROJECT="${CLUSTER_PROJECT,,}"

DURATION_SECONDS="${DURATION_SECONDS:-10}"
WARMUP_SECONDS="${WARMUP_SECONDS:-2}"
RUNS="${RUNS:-1}"
VALUE_SIZE="${VALUE_SIZE:-256}"
KEYSPACE="${KEYSPACE:-256}"
CONCURRENCIES="${CONCURRENCIES:-32 128}"
WORKLOADS="${WORKLOADS:-set mixed}"
READ_PERCENT="${READ_PERCENT:-50}"
LEADER_MAP="${LEADER_MAP:-}"
CONNECTIONS="${CONNECTIONS:-8}"
WATCHERS="${WATCHERS:-5}"
WATCH_RATE="${WATCH_RATE:-300}"
# Memory limits retain plenty of headroom for other production services.
NODE_MEMORY="${NODE_MEMORY:-3g}"
export KVSTORE_JOIN_TOKEN="${KVSTORE_JOIN_TOKEN:-$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')}"

cat > "$OUT/standalone.override.yml" <<YAML
services:
  kvstore:
    image: kvstore:linux-bench
    mem_limit: ${NODE_MEMORY}
    pids_limit: 512
    logging:
      driver: local
      options:
        max-size: "10m"
        max-file: "2"
YAML
cat > "$OUT/cluster.override.yml" <<YAML
x-limits: &limits
  image: kvstore:linux-bench
  mem_limit: ${NODE_MEMORY}
  pids_limit: 512
  logging:
    driver: local
    options:
      max-size: "10m"
      max-file: "2"
services:
  node0:
    <<: *limits
  node1:
    <<: *limits
  node2:
    <<: *limits
  envoy:
    mem_limit: 512m
    pids_limit: 512
    logging:
      driver: local
      options:
        max-size: "10m"
        max-file: "2"
YAML

standalone=(docker compose -p "$STANDALONE_PROJECT" -f "$ROOT/compose.yml" -f "$OUT/standalone.override.yml")
cluster=(docker compose -p "$CLUSTER_PROJECT" -f "$ROOT/compose.cluster.yml" -f "$OUT/cluster.override.yml")

cleanup() {
  local exit_code=$?
  trap - EXIT
  if [[ "${KEEP_RUNNING:-0}" != 1 ]]; then
    "${standalone[@]}" stop >/dev/null 2>&1 || true
    "${cluster[@]}" stop >/dev/null 2>&1 || true
  fi
  echo "Reports: $OUT"
  echo "Isolated projects: $STANDALONE_PROJECT / $CLUSTER_PROJECT"
  echo 'Benchmark containers are stopped by default; volumes are preserved.'
  echo 'No unrelated Docker containers were stopped or modified.'
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

{
  date -u '+UTC: %Y-%m-%d %H:%M:%S'
  uname -a
  echo "CPU:"
  command -v lscpu >/dev/null && lscpu || true
  echo "MEMORY:"
  command -v free >/dev/null && free -h || true
  echo "DISKS:"
  df -h "$ROOT"
  echo "DOCKER:"
  docker version --format '{{.Server.Version}}' 2>/dev/null || true
  echo "RUN CONFIG:"
  printf 'duration=%ss warmup=%ss runs=%s concurrency="%s" connections=%s keys=%s bytes=%s\n' \
    "$DURATION_SECONDS" "$WARMUP_SECONDS" "$RUNS" "$CONCURRENCIES" "$CONNECTIONS" "$KEYSPACE" "$VALUE_SIZE"
} > "$OUT/system.txt"
docker ps --format '{{.ID}} {{.Names}} {{.Status}}' > "$OUT/containers-before.txt"

if [[ "${SKIP_BUILD:-0}" != 1 ]]; then
  echo '== Building KVStore Linux Docker image on server =='
  docker build --platform linux/amd64 -t kvstore:linux-bench .
fi

if [[ "${SKIP_BUILD:-0}" != 1 || ! -x ./bin/kvbench ]]; then
  echo '== Building Linux-native kvbench =='
  if command -v go >/dev/null 2>&1; then
    go build -trimpath -o ./bin/kvbench ./cmd/kvbench
  else
    docker run --rm -v "$ROOT:/src" -w /src golang:1.27-bookworm \
      sh -c 'mkdir -p bin && go build -trimpath -o ./bin/kvbench ./cmd/kvbench'
  fi
fi

# Used for initial readiness; this prefill is outside all measured intervals.
ready() {
  local label="$1" i
  echo "Waiting for $label gRPC..."
  for ((i=1;i<=60;i++)); do
    if ./bin/kvbench -addr 127.0.0.1:50051 -mode get -consistency strong \
      -duration 1s -warmup 0s -concurrency 1 -connections 1 \
      -keys 1 -value-size 16 >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "$label did not become ready; inspect the Compose logs." >&2
  return 1
}

run_one() {
  local deployment="$1" mode="$2" consistency="$3" concurrency="$4" addr="$5" run="$6" label="$7"
  local output="$OUT/${deployment}-${label}-${mode}-${consistency}-c${concurrency}-r${run}.json"
  echo "[$deployment/$label/$mode/$consistency] workers=$concurrency run=$run/$RUNS"
  local -a leader_flags=()
  if [[ "$deployment" == cluster && "$label" == leader && -n "$LEADER_MAP" ]]; then
    leader_flags=(-leader-map "$LEADER_MAP")
  fi
  ./bin/kvbench \
    -addr "$addr" -mode "$mode" -consistency "$consistency" \
    "${leader_flags[@]}" -read-percent "$READ_PERCENT" \
    -duration "${DURATION_SECONDS}s" -warmup "${WARMUP_SECONDS}s" \
    -concurrency "$concurrency" -connections "$CONNECTIONS" \
    -keys "$KEYSPACE" -value-size "$VALUE_SIZE" \
    -watchers "$WATCHERS" -watch-rate "$WATCH_RATE" -json > "${output}.tmp"
  mv -- "${output}.tmp" "$output"
  if command -v jq >/dev/null 2>&1; then
    jq -r '"  GET: \(.get.ops_sec|floor)/s p99=\(.get.latency.p99_ms)ms err=\(.get.errors) missing=\(.get.not_found); SET: \(.set.ops_sec|floor)/s p99=\(.set.latency.p99_ms)ms err=\(.set.errors)"' "$output"
  fi
}

benchmark_set() {
  local deployment="$1" consistency="$2" addr="$3" label="$4"
  local conc mode r
  for conc in $CONCURRENCIES; do
    for mode in $WORKLOADS; do
      for ((r=1;r<=RUNS;r++)); do
        run_one "$deployment" "$mode" "$consistency" "$conc" "$addr" "$r" "$label"
      done
    done
  done
}

echo '== Starting isolated standalone =='
"${standalone[@]}" up -d --no-build --wait --wait-timeout 120
ready standalone
benchmark_set standalone strong 127.0.0.1:50051 grpc
"${standalone[@]}" logs --tail 200 > "$OUT/standalone-logs.txt" 2>&1 || true
"${standalone[@]}" stop

echo '== Starting isolated 3-node Raft cluster + Envoy =='
"${cluster[@]}" up -d --no-build --wait --wait-timeout 150
ready cluster
benchmark_set cluster strong 127.0.0.1:50051 envoy
if [[ -n "$LEADER_MAP" ]]; then
  echo '== Running same cluster workloads with direct leader routing =='
  benchmark_set cluster strong 127.0.0.1:50051 leader
fi

"${cluster[@]}" logs --tail 300 > "$OUT/cluster-logs.txt" 2>&1 || true
if command -v curl >/dev/null 2>&1; then
  for node in 0 1 2; do
    curl -fsS --max-time 3 -H "Authorization: Bearer $KVSTORE_JOIN_TOKEN" \
      "http://127.0.0.1:$((17001+node))/v1/cluster/performance" \
      > "$OUT/node${node}-performance.json" || true
  done
fi

docker ps --format '{{.ID}} {{.Names}} {{.Status}}' > "$OUT/containers-after.txt"
echo '== Completed QUICK Linux WRITE benchmark =='
