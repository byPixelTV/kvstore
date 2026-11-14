#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# ONE isolated 3x3 Raft cluster, ONE short workload. No unrelated containers
# or volumes are stopped/deleted. The existing sharded runner owns cleanup.
export RUNS="${RUNS:-1}"
export DURATION_SECONDS="${DURATION_SECONDS:-12}"
export WARMUP_SECONDS="${WARMUP_SECONDS:-2}"
export CONCURRENCIES="${CONCURRENCIES:-256}"
export KEYSPACE="${KEYSPACE:-16384}"
export BATCH_SIZE="${BATCH_SIZE:-16}"
export TARGET_OPS="${TARGET_OPS:-0}"
export KVSTORE_WRITE_BATCH_TARGET="${KVSTORE_WRITE_BATCH_TARGET:-64}"
export KVSTORE_WRITE_BATCH_WINDOW="${KVSTORE_WRITE_BATCH_WINDOW:-1500us}"
export KVSTORE_WRITE_BATCH_MAX_WINDOW="${KVSTORE_WRITE_BATCH_MAX_WINDOW:-3ms}"
export KVSTORE_RAFT_MAX_INFLIGHT="${KVSTORE_RAFT_MAX_INFLIGHT:-8}"
# Optional: set KVSTORE_WRITE_MAX_QUEUE_DELAY=75ms to reject requests that
# waited too long BEFORE entering Raft. Keep disabled for the first capacity
# run to measure raw throughput and spot overload in queue metrics.
export KVSTORE_WRITE_MAX_QUEUE_DELAY="${KVSTORE_WRITE_MAX_QUEUE_DELAY:-0s}"

prior="$(find "$ROOT/benchmarks/results/live-linux" -maxdepth 1 -type d -name 'sharded-*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2- || true)"
./scripts/benchmark-sharded-linux.sh
latest="$(find "$ROOT/benchmarks/results/live-linux" -maxdepth 1 -type d -name 'sharded-*' -printf '%T@ %p\n' | sort -nr | head -1 | cut -d' ' -f2-)"
if [[ -z "$latest" || "$latest" == "$prior" ]]; then echo 'No fresh sharded results found' >&2; exit 1; fi

echo '== Successful logical GET/SET operations and p99 latency =='
for f in "$latest"/sharded-mixed-strong-*.json; do
  [[ -f "$f" ]] || { echo 'Benchmark JSON missing' >&2; exit 1; }
  jq -r '"GET/s=\(.get.ops_sec|floor) SET/s=\(.set.ops_sec|floor) GET_p99_ms=\(.get.latency.p99_ms) SET_p99_ms=\(.set.latency.p99_ms) errors=\(.get.errors+.set.errors)"' "$f"
done

echo '== Raft leader write aggregation metrics =='
for f in "$latest"/shard*-node*-performance.json; do
  [[ -f "$f" ]] || continue
  jq -r 'select(.raft_state=="Leader") | "node=\(.node_id) logical_writes_per_log=\(.writes.average_batch_size // "n/a") raft_log_write_ms=\(.raft_log.average_write_ms // "n/a") pebble_commit_ms=\(.fsm.average_pebble_commit_ms // "n/a") queue_wait_ms=\(.writes.average_queue_wait_ms // "n/a") rejected_before_raft=\(.writes.rejected_before_raft // 0)"' "$f" || true
done
archive="/root/kvstore-aggregated-commit-bench.zip"
if command -v zip >/dev/null; then
  zip -qr "$archive" "${latest#$ROOT/}" -x '*override.yml'
  echo "ZIP: $archive"
fi
