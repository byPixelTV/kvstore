#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
RUNS="${RUNS:-2}"
DURATION_SECONDS="${DURATION_SECONDS:-10}"
WARMUP_SECONDS="${WARMUP_SECONDS:-2}"
CONCURRENCIES="${CONCURRENCIES:-256}"
KEYSPACE="${KEYSPACE:-16384}"
VALUE_SIZE="${VALUE_SIZE:-256}"
BATCH_SIZES="${BATCH_SIZES:-1 16}"
P99_TARGET_MS="${P99_TARGET_MS:-100}"
OUT="$ROOT/benchmarks/results/live-linux/batchwrite-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$OUT"
{
  echo "git_head=$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "batches=$BATCH_SIZES runs=$RUNS duration=$DURATION_SECONDS warmup=$WARMUP_SECONDS concurrency=$CONCURRENCIES keys=$KEYSPACE value_size=$VALUE_SIZE"
  echo "target_p99_ms=$P99_TARGET_MS"
} > "$OUT/metadata.txt"

# Same source code, topology, keyspace and load conditions for every profile.
# Each invocation starts/stops only its own kvbench Compose project.
for batch in $BATCH_SIZES; do
  [[ "$batch" =~ ^[1-9][0-9]*$ ]] && (( batch <= 128 )) || { echo "Invalid batch size $batch" >&2; exit 2; }
  echo "=== BatchWrite profile: $batch logical writes / RPC ==="
  previous="$(find "$ROOT/benchmarks/results/live-linux" -maxdepth 1 -type d -name 'sharded-*' -printf '%T@ %p\n' | sort -nr | head -1 | cut -d' ' -f2- || true)"
  RUNS="$RUNS" DURATION_SECONDS="$DURATION_SECONDS" WARMUP_SECONDS="$WARMUP_SECONDS" CONCURRENCIES="$CONCURRENCIES" KEYSPACE="$KEYSPACE" VALUE_SIZE="$VALUE_SIZE" BATCH_SIZE="$batch" \
      bash "$ROOT/scripts/benchmark-sharded-linux.sh" 2>&1 | tee "$OUT/batch-${batch}.log"
  newest="$(find "$ROOT/benchmarks/results/live-linux" -maxdepth 1 -type d -name 'sharded-*' -printf '%T@ %p\n' | sort -nr | head -1 | cut -d' ' -f2-)"
  [[ -n "$newest" && "$newest" != "$previous" ]] || { echo "No new output for batch $batch" >&2; exit 1; }
  cp -a -- "$newest" "$OUT/batch-${batch}"
done

# Print actual tail-latency outcome rather than promising sub-100ms on a
# storage host that is already showing I/O pressure.
if command -v jq >/dev/null; then
  failures=0
  for batch in $BATCH_SIZES; do
    for file in "$OUT/batch-${batch}"/sharded-mixed-strong-*.json; do
      [[ -f "$file" ]] || continue
      jq -r '"batch_size=\(.batch_size) GET/s=\(.get.ops_sec|floor) SET/s=\(.set.ops_sec|floor) GET_p99=\(.get.latency.p99_ms)ms SET_p99=\(.set.latency.p99_ms)ms errors=\(.get.errors+.set.errors)"' "$file"
      if ! jq -e --argjson target "$P99_TARGET_MS" '(.get.errors+.set.errors)==0 and .get.not_found==0 and .get.latency.p99_ms < $target and .set.latency.p99_ms < $target' "$file" >/dev/null; then
        failures=$((failures+1))
      fi
    done
  done
  if (( failures > 0 )); then
    echo "SLO check: $failures runs failed p99 < ${P99_TARGET_MS}ms and zero errors; data preserved" >&2
  else
    echo "SLO check: all runs passed p99 < ${P99_TARGET_MS}ms with zero errors"
  fi
fi
printf 'Reports: %s\n' "$OUT"
echo "Archive safely (review configuration files before sharing):"
echo "  zip -r /root/kvstore-batchwrite-bench.zip \"${OUT#$ROOT/}\" -x '*override.yml'"
