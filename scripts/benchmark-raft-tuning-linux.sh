#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C

# Two short, independent isolated Compose runs on the same Linux host.
# Does not stop, prune, or reconfigure any non-benchmark containers.
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LEADER_MAP="${LEADER_MAP:-node0:50051=127.0.0.1:50061,node1:50051=127.0.0.1:50062,node2:50051=127.0.0.1:50063}"
export LEADER_MAP
export BENCH_STANDALONE=0 BENCH_ENVOY=0
export RUNS="${RUNS:-1}" DURATION_SECONDS="${DURATION_SECONDS:-10}" WARMUP_SECONDS="${WARMUP_SECONDS:-2}"
export WORKLOADS="${WORKLOADS:-mixed}" READ_PERCENT=50
export CONCURRENCIES="${CONCURRENCIES:-128 256}"
export KEYSPACE="${KEYSPACE:-1024}" VALUE_SIZE="${VALUE_SIZE:-256}"
export LEADER_REFRESH="${LEADER_REFRESH:-2s}"

mkdir -p benchmarks/results/live-linux
RESULTS="$(mktemp -d "${ROOT}/benchmarks/results/live-linux/tuning-XXXXXXXX")"
trap 'echo "A/B logs: $RESULTS"' EXIT

# Baseline uses the previous known Raft defaults with the new code.
echo '== A/B variant 1: 8 in-flight batches, 300us read fence =='
KVSTORE_RAFT_MAX_INFLIGHT=8 KVSTORE_READ_BATCH_WINDOW=300us \
KVSTORE_WRITE_BATCH_WINDOW=1500us SKIP_BUILD=0 \
  bash ./scripts/benchmark-write-linux.sh | tee "$RESULTS/baseline.log"

# Increased pipeline concurrency and a longer read-fence collection window.
# These values are *candidates*, not safe speedups until measured.
echo '== A/B variant 2: 16 in-flight batches, 750us read fence =='
KVSTORE_RAFT_MAX_INFLIGHT=16 KVSTORE_READ_BATCH_WINDOW=750us \
KVSTORE_WRITE_BATCH_WINDOW=1500us SKIP_BUILD=1 \
  bash ./scripts/benchmark-write-linux.sh | tee "$RESULTS/tuned.log"

if command -v jq >/dev/null 2>&1; then
  echo '== Direct-leader throughput comparison (cluster, 50/50) =='
  for variant in baseline tuned; do
    directory="$(sed -n 's/^Reports: //p' "$RESULTS/${variant}.log" | tail -n 1)"
    if [[ ! -d "$directory" ]]; then
      echo "No result directory found for $variant" >&2
      exit 1
    fi
    for json in "$directory"/cluster-leader-mixed-strong-*.json; do
      [[ -f "$json" ]] || { echo "Missing direct-leader result $json" >&2; exit 1; }
      jq -r --arg name "$variant" '
        $name + ": " + (.concurrency|tostring) + " workers: " +
        (.get.ops_sec|floor|tostring) + " GET/s + " +
        (.set.ops_sec|floor|tostring) + " SET/s, GET p99=" +
        (.get.latency.p99_ms|tostring) + "ms, SET p99=" +
        (.set.latency.p99_ms|tostring) + "ms"
      ' "$json" | tee -a "$RESULTS/summary.txt"
    done
  done
fi

echo "Complete. Send the two newest write-* result directories as ZIP, plus $RESULTS."
