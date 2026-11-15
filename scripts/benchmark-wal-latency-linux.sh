#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
for cmd in jq zip docker; do command -v "$cmd" >/dev/null || { echo "Missing command: $cmd" >&2; exit 2; }; done

# Two short, identical workloads on FRESH, ISOLATED multi-Raft projects.
# Never stop / remove unrelated Docker containers or volumes.
# Each mode runs a new 3x3 cluster so the configuration is consistent.
export RUNS=1
export DURATION_SECONDS="${DURATION_SECONDS:-12}"
export WARMUP_SECONDS="${WARMUP_SECONDS:-3}"
export CONCURRENCIES="${CONCURRENCIES:-256}"
export KEYSPACE="${KEYSPACE:-16384}"
export VALUE_SIZE="${VALUE_SIZE:-256}"
export BATCH_SIZE="${BATCH_SIZE:-16}"
export CONNECTIONS="${CONNECTIONS:-16}"
export TARGET_OPS=0
export KVSTORE_RAFT_MAX_INFLIGHT="${KVSTORE_RAFT_MAX_INFLIGHT:-8}"
export KVSTORE_WRITE_BATCH_TARGET="${KVSTORE_WRITE_BATCH_TARGET:-64}"
export KVSTORE_READ_BATCH_WINDOW="${KVSTORE_READ_BATCH_WINDOW:-300us}"

latest() {
  find "$ROOT/benchmarks/results/live-linux" -maxdepth 1 -type d -name 'sharded-*' \
    -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2- || true
}

run_variant() {
  local name="$1" window="$2" max_window="$3" commit_timeout="$4" skip_build="$5"
  local before dir
  before="$(latest)"
  export KVSTORE_WRITE_BATCH_WINDOW="$window"
  export KVSTORE_WRITE_BATCH_MAX_WINDOW="$max_window"
  export KVSTORE_RAFT_COMMIT_TIMEOUT="$commit_timeout"
  export SKIP_BUILD="$skip_build"
  echo "== $name: window=$window max_window=$max_window raft_commit_timeout=$commit_timeout ==" >&2
  ./scripts/benchmark-sharded-linux.sh >&2
  dir="$(latest)"
  if [[ -z "$dir" || "$dir" == "$before" ]]; then
    echo "Missing new benchmark output for $name" >&2
    return 1
  fi
  echo "$dir"
}

baseline="$(run_variant baseline 1500us 3ms 50ms 0)"
tuned="$(run_variant low-latency 600us 1500us 15ms 1)"

summarize() {
  local name="$1" dir="$2" file
  for file in "$dir"/sharded-mixed-strong-*.json; do
    [[ -f "$file" ]] || { echo "Missing results in $dir" >&2; return 1; }
    jq -r --arg name "$name" '
      "\($name): GET/s=\(.get.ops_sec|floor) SET/s=\(.set.ops_sec|floor) GET_p99=\(.get.latency.p99_ms)ms SET_p99=\(.set.latency.p99_ms)ms errors=\(.get.errors+.set.errors)"
    ' "$file"
    jq -r --arg name "$name" '
      if (.get.ops_sec >= 10000 and .set.ops_sec >= 10000 and .get.latency.p99_ms < 100 and .set.latency.p99_ms < 100 and (.get.errors+.set.errors)==0)
      then "\($name): PASS 10k GET + 10k SET with p99 < 100ms"
      else "\($name): SLO not met (keep the full results for analysis)" end
    ' "$file"
  done
  for f in "$dir"/shard*-node*-performance.json; do
    [[ -f "$f" ]] || continue
    jq -r --arg name "$name" '
      select(.raft_state == "Leader") |
      "\($name) leader \(.node_id): queue_p99_upper_ms=\(.writes.p99_queue_wait_ms_upper // "n/a") admission_p99_upper_ms=\(.writes.p99_raft_admission_ms_upper // "n/a") apply_p99_upper_ms=\(.writes.p99_raft_apply_ms_upper // "n/a")"
    ' "$f" || true
  done
}

echo '== SLO comparison (one run per mode; not a reproducibility claim) =='
summarize baseline "$baseline"
summarize low-latency "$tuned"
archive="/root/kvstore-wal-latency-bench.zip"
zip -qr "$archive" "${baseline#"$ROOT"/}" "${tuned#"$ROOT"/}" -x '*/override.yml'
echo "ZIP: $archive"
echo 'No unrelated containers/volumes were stopped or deleted.'
