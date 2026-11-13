#!/usr/bin/env bash
set -Eeuo pipefail

# Place this file in scripts/ of the KVStore repository.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DURATION_SECONDS=15
WARMUP_SECONDS=3
CONCURRENCY=16
CONNECTIONS=4
VALUE_SIZE=256
KEYSPACE=1000
WATCHERS=5
WATCH_RATE=100

usage() {
  cat <<'HELP'
Usage: ./scripts/benchmark-compose.sh [options]

  --duration-seconds N   Measurement duration per workload (default: 15)
  --warmup-seconds N     Warmup duration per workload (default: 3)
  --concurrency N        Parallel workers (default: 16)
  --connections N        gRPC connections (default: 4)
  --value-size N         Payload bytes (default: 256)
  --keyspace N           Number of distinct keys (default: 1000)
  --watchers N           Concurrent Watch subscribers (default: 5)
  --watch-rate N         Target writes/second for Watch (default: 100)
  -h, --help             Show this help

This script stops and starts the local standalone/cluster Compose stacks.
It does not delete Docker volumes. The cluster remains running afterwards.
JSON results are stored in benchmarks/results/.
HELP
}

while (($#)); do
  case "$1" in
    --duration-seconds|--duration) DURATION_SECONDS="${2:?Missing value for $1}"; shift 2 ;;
    --warmup-seconds|--warmup) WARMUP_SECONDS="${2:?Missing value for $1}"; shift 2 ;;
    --concurrency) CONCURRENCY="${2:?Missing value for $1}"; shift 2 ;;
    --connections) CONNECTIONS="${2:?Missing value for $1}"; shift 2 ;;
    --value-size) VALUE_SIZE="${2:?Missing value for $1}"; shift 2 ;;
    --keyspace|--keys) KEYSPACE="${2:?Missing value for $1}"; shift 2 ;;
    --watchers) WATCHERS="${2:?Missing value for $1}"; shift 2 ;;
    --watch-rate) WATCH_RATE="${2:?Missing value for $1}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

for name in DURATION_SECONDS WARMUP_SECONDS CONCURRENCY CONNECTIONS VALUE_SIZE KEYSPACE WATCHERS WATCH_RATE; do
  value="${!name}"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "Invalid value for $name: $value (expected nonnegative integer)" >&2
    exit 2
  fi
done

for tool in go docker; do
  command -v "$tool" >/dev/null 2>&1 || { echo "Error: $tool not found" >&2; exit 1; }
done
docker compose version >/dev/null || { echo 'Error: Docker Compose v2 is required' >&2; exit 1; }

mkdir -p bin benchmarks/results

echo 'Building kvbench...'
go build -o ./bin/kvbench ./cmd/kvbench

# Required by compose.cluster.yml (only used for local development here).
if [[ -z "${KVSTORE_JOIN_TOKEN:-}" ]]; then
  KVSTORE_JOIN_TOKEN="$(od -An -N24 -tx1 /dev/urandom | tr -d '[:space:]')"
  export KVSTORE_JOIN_TOKEN
fi

run_workload() {
  local deployment="$1"
  local mode="$2"
  local out="benchmarks/results/${deployment}-${mode}.json"
  local attempt
  local -a args=(
    -addr 127.0.0.1:50051
    -mode "$mode"
    -duration "${DURATION_SECONDS}s"
    -warmup "${WARMUP_SECONDS}s"
    -concurrency "$CONCURRENCY"
    -connections "$CONNECTIONS"
    -keys "$KEYSPACE"
    -value-size "$VALUE_SIZE"
    -watchers "$WATCHERS"
    -watch-rate "$WATCH_RATE"
    -json
  )

  for attempt in 1 2 3 4; do
    echo "[$deployment/$mode] attempt $attempt/4..."
    if ./bin/kvbench "${args[@]}" > "${out}.tmp"; then
      mv -f -- "${out}.tmp" "$out"
      if command -v jq >/dev/null 2>&1; then
        jq -r --arg deployment "$deployment" --arg mode "$mode" \
          '"\($deployment) / \($mode): GET \(.get.ops_sec | round) ops/s; SET \(.set.ops_sec | round) ops/s; GET p99 \(.get.latency.p99_ms)ms; SET p99 \(.set.latency.p99_ms)ms", (if .watch then "  Watch delivery: \(.watch.deliveries_per_sec | round) events/s; p99 \(.watch.delivery_latency.p99_ms)ms" else empty end)' \
          "$out"
      else
        echo "Saved $out (install 'jq' to see metrics in the terminal)."
      fi
      return 0
    fi
    rm -f -- "${out}.tmp"
    if [[ "$attempt" == 4 ]]; then
      echo "Benchmark $deployment/$mode failed after 4 attempts" >&2
      return 1
    fi
    sleep 3
  done
}

echo 'Starting standalone...'
docker compose -f compose.cluster.yml down
docker compose up -d --build
sleep 5
for mode in get set mixed watch; do
  run_workload standalone "$mode"
done

echo 'Starting Raft cluster (3 nodes + Envoy)...'
docker compose down
docker compose -f compose.cluster.yml up -d --build
sleep 10
for mode in get set mixed watch; do
  run_workload cluster "$mode"
done

echo 'Done. JSON reports: benchmarks/results/'
echo 'Cluster remains running. No Docker volumes were deleted.'
