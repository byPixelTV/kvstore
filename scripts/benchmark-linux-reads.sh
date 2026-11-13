#!/usr/bin/env bash
set -Eeuo pipefail

# Linux-only diagnostic reads benchmark. Uses a running 3-node Compose cluster.
# Reports gRPC performance through Envoy and directly to individual nodes.
# Never deletes volumes, stops nodes or modifies cluster membership.
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "$(uname -s)" != Linux ]]; then
  echo 'This benchmark is intended for Linux hosts.' >&2
  exit 1
fi
for tool in go docker; do
  command -v "$tool" >/dev/null || { echo "Missing dependency: $tool" >&2; exit 1; }
done

DURATION=${DURATION_SECONDS:-30}
WARMUP=${WARMUP_SECONDS:-5}
CONCURRENCY=${CONCURRENCY:-32}
CONNECTIONS=${CONNECTIONS:-8}
VALUE_SIZE=${VALUE_SIZE:-256}
RUNS=${RUNS:-3}

mkdir -p bin benchmarks/results/linux
echo 'Building Linux-native benchmark client...'
go build -o bin/kvbench ./cmd/kvbench

if ! docker compose -f compose.cluster.yml ps --status running --services | grep -qx envoy; then
  echo 'Start the cluster first: docker compose -f compose.cluster.yml up -d --build' >&2
  exit 1
fi

RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="benchmarks/results/linux/$RUN_STAMP"
mkdir -p "$OUT"
{
  echo "timestamp_utc=$RUN_STAMP"
  echo "kernel=$(uname -srmo)"
  echo "go=$(go version)"
  echo "docker=$(docker version --format '{{.Server.Version}}' 2>/dev/null || true)"
  if command -v lscpu >/dev/null; then lscpu; fi
  if command -v docker >/dev/null; then docker info --format 'docker_operating_system={{.OperatingSystem}}'; fi
} > "$OUT/system.txt"

leader_port=""
if [[ -n "${KVSTORE_JOIN_TOKEN:-}" ]] && command -v curl >/dev/null && command -v jq >/dev/null; then
  for n in 0 1 2; do
    admin=$((17001+n))
    response="$(curl -fsS --max-time 3 \
       -H "Authorization: Bearer ${KVSTORE_JOIN_TOKEN}" \
       "http://127.0.0.1:${admin}/v1/cluster/performance" 2>/dev/null || true)"
    if [[ -n "$response" ]]; then
      printf '%s\n' "$response" > "$OUT/node${n}-before.json"
      if [[ "$(printf '%s' "$response" | jq -r '.raft_state')" == Leader ]]; then
        leader_port=$((50061+n))
      fi
    fi
  done
fi

# Do NOT assume node0 is always the leader after restarts.
# Strong reads on a follower still forward to whichever node is leader.
scenarios=(
  'envoy-strong|50051|strong'
  'envoy-eventual|50051|eventual'
  'node0-strong|50061|strong'
  'node0-eventual|50061|eventual'
)
if [[ -n "$leader_port" ]]; then
  scenarios+=("leader-strong|${leader_port}|strong")
fi

printf 'Linux benchmark output: %s\n' "$OUT"
for scenario in "${scenarios[@]}"; do
  IFS='|' read -r label port consistency <<< "$scenario"
  for ((n=1;n<=RUNS;n++)); do
    file="$OUT/${label}-run${n}.json"
    echo "[$label] run $n/$RUNS"
    ./bin/kvbench \
      -addr "127.0.0.1:${port}" \
      -mode get -consistency "$consistency" \
      -duration "${DURATION}s" -warmup "${WARMUP}s" \
      -concurrency "$CONCURRENCY" -connections "$CONNECTIONS" \
      -value-size "$VALUE_SIZE" -json > "$file"
    if command -v jq >/dev/null; then
      jq -r --arg label "$label" \
        '"  \($label): \(.get.ops_sec | floor) GET/s, p50 \(.get.latency.p50_ms) ms, p95 \(.get.latency.p95_ms) ms, p99 \(.get.latency.p99_ms) ms, errors \(.get.errors), missing \(.get.not_found)"' \
        "$file"
    fi
  done
done

if [[ -n "${KVSTORE_JOIN_TOKEN:-}" ]] && command -v curl >/dev/null; then
  for n in 0 1 2; do
    curl -fsS --max-time 3 -H "Authorization: Bearer ${KVSTORE_JOIN_TOKEN}" \
      "http://127.0.0.1:$((17001+n))/v1/cluster/performance" \
      > "$OUT/node${n}-after.json" || true
  done
fi

echo "Done: $OUT"
echo 'Reports are JSON. Never compare these directly to earlier Windows Docker Desktop numbers.'
