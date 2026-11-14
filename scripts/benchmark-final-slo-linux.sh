#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# ONE isolated cluster, ONE short run. Do not stop or purge unrelated Docker
# containers. The underlying benchmark-sharded-linux.sh owns cleanup.
: "${RUNS:=1}" "${DURATION_SECONDS:=12}" "${WARMUP_SECONDS:=3}" \
  "${CONCURRENCIES:=256}" "${KEYSPACE:=16384}" "${BATCH_SIZE:=16}" \
  "${TARGET_OPS:=20000}"
export RUNS DURATION_SECONDS WARMUP_SECONDS CONCURRENCIES KEYSPACE BATCH_SIZE TARGET_OPS
if ! command -v jq >/dev/null; then echo 'jq required for SLO analysis' >&2; exit 2; fi
prior="$(find "$ROOT/benchmarks/results/live-linux" -maxdepth 1 -type d -name 'sharded-*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2- || true)"
./scripts/benchmark-sharded-linux.sh
latest="$(find "$ROOT/benchmarks/results/live-linux" -maxdepth 1 -type d -name 'sharded-*' -printf '%T@ %p\n' | sort -nr | head -1 | cut -d' ' -f2-)"
if [[ -z "$latest" || "$latest" == "$prior" ]]; then echo 'No new benchmark result found' >&2; exit 1; fi
printf '\n=== STRONG 50/50 SLO (logical operations, not RPC count) ===\n'
for file in "$latest"/sharded-mixed-strong-*.json; do
  [[ -f "$file" ]] || { echo 'Expected JSON not found' >&2; exit 1; }
  jq -r --argjson target 100 --argjson goal 10000 '
    "GET/s=\(.get.ops_sec|floor) SET/s=\(.set.ops_sec|floor) GET-p99=\(.get.latency.p99_ms)ms SET-p99=\(.set.latency.p99_ms)ms errors=\(.get.errors+.set.errors) missing=\(.get.not_found)",
    "SLO=\(if .get.ops_sec >= $goal and .set.ops_sec >= $goal and .get.latency.p99_ms < $target and .set.latency.p99_ms < $target and (.get.errors+.set.errors)==0 and .get.not_found==0 then "PASS" else "NOT_MET" end)"' "$file"
done
printf '\n=== Raft backend sync timings per shard leader (including prefill/warmup) ===\n'
for after in "$latest"/after-c*-r*-s*n*.json; do
  [[ -f "$after" ]] || continue
  before="${after/\/after-/\/before-}"
  [[ -f "$before" ]] || continue
  jq -nr --slurpfile a "$after" --slurpfile b "$before" '
   $a[0] as $a | $b[0] as $b |
   select($a.raft_state=="Leader") |
   ($a.raft_log.write_calls//0) as $aw |
   ($b.raft_log.write_calls//0) as $bw |
   ($a.fsm.commit_batches//0) as $ac |
   ($b.fsm.commit_batches//0) as $bc |
   ((($a.raft_log.average_write_ms//0)*$aw-($b.raft_log.average_write_ms//0)*$bw)/(if $aw>$bw then $aw-$bw else 1 end)) as $bolt |
   ((($a.fsm.average_pebble_commit_ms//0)*$ac-($b.fsm.average_pebble_commit_ms//0)*$bc)/(if $ac>$bc then $ac-$bc else 1 end)) as $pebble |
   "\($a.node_id) bolt_writes=\($aw-$bw) avg_bolt_write_ms=\($bolt*100|round/100) avg_pebble_ms=\($pebble*100|round/100)"'
done
printf '\nReports: %s\n' "$latest"
if command -v zip >/dev/null; then
  archive="/root/kvstore-final-slo-bench.zip"
  zip -qr "$archive" "${latest#$ROOT/}" -x '*override.yml'
  printf 'ZIP: %s\n' "$archive"
else
  echo 'zip unavailable; archive the result folder manually.'
fi
