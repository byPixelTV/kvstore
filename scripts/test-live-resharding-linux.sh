#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C
# Destructive only to its OWN fresh benchmark Docker Compose project/volumes.
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
for tool in docker jq curl grep; do command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 1; }; done
export KVSTORE_JOIN_TOKEN="${KVSTORE_JOIN_TOKEN:-$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')}"
export KVSTORE_ROUTER_ADMIN_TOKEN="${KVSTORE_ROUTER_ADMIN_TOKEN:-$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')}"
OUT=""
PROJECT=""
cleanup() {
 local rc=$?
 trap - EXIT
 if [[ -n "$PROJECT" && -n "$OUT" ]]; then
  docker compose -p "$PROJECT" -f "$ROOT/compose.sharded.yml" -f "$OUT/override.yml" logs --tail 500 > "$OUT/migration-cluster-logs.txt" 2>&1 || true
  docker compose -p "$PROJECT" -f "$ROOT/compose.sharded.yml" -f "$OUT/override.yml" stop >/dev/null 2>&1 || true
 fi
 echo "Migration smoke-test reports: ${OUT:-not created}"
 exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# The sharded benchmark uses a unique project name and checks that none of
# its localhost ports conflict with existing workloads.
previous="$(find benchmarks/results/live-linux -maxdepth 1 -mindepth 1 -type d -name 'sharded-*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2- || true)"
echo '== Start isolated 3 x 3 Raft cluster and router =='
KEEP_RUNNING=1 RUNS=1 DURATION_SECONDS=2 WARMUP_SECONDS=1 CONCURRENCIES=16 KEYSPACE=128 ./scripts/benchmark-sharded-linux.sh
OUT="$(find benchmarks/results/live-linux -maxdepth 1 -mindepth 1 -type d -name 'sharded-*' -printf '%T@ %p\n' | sort -nr | head -1 | cut -d' ' -f2-)"
if [[ "$OUT" == "$previous" || ! -f "$OUT/project-name.txt" ]]; then echo 'Cannot identify new benchmark project for cleanup' >&2; exit 1; fi
PROJECT="$(cat "$OUT/project-name.txt")"
NAMESPACE="migration-smoke-$(date -u +%Y%m%dT%H%M%S)"
echo "== 50/50 STRONG load namespace=$NAMESPACE =="
"$ROOT/bin/kvbench-sharded" -addr 127.0.0.1:50081 -namespace "$NAMESPACE" -mode mixed \
 -consistency strong -read-percent 50 -duration 30s -warmup 3s -concurrency 128 \
 -connections 16 -keys 4096 -hot-key-index 0 -hot-percent 20 -value-size 256 -json > "$OUT/live-load.json" \
 2> "$OUT/live-load-stderr.txt" &
BENCH_PID=$!
ready=0
for ((i=0;i<120;i++)); do
 if grep -q 'prefill complete' "$OUT/live-load-stderr.txt"; then ready=1;break;fi
 if ! kill -0 "$BENCH_PID" 2>/dev/null; then break;fi
 sleep 1
done
if [[ "$ready" != 1 ]];then wait "$BENCH_PID" || true;echo 'Bench load did not reach prefill complete' >&2;exit 1;fi
sleep 2
# Move the slot containing key-0000000 from its default owner to another group.
# The CLI computes the stable 4096-slot FNV hash and selects another group.
echo '== Move one hot slot while the benchmark is still writing =='
"$ROOT/bin/kvshardctl-sharded" -action migrate -namespace "$NAMESPACE" -key key-0000000 \
 -admin http://127.0.0.1:17110 > "$OUT/migration-result.json"
wait "$BENCH_PID"
jq -e '(.get.errors + .set.errors)==0 and .get.not_found==0 and .get.ok>0 and .set.ok>0' "$OUT/live-load.json" >/dev/null
jq '{GET: .get.ops_sec, SET: .set.ops_sec, errors: (.get.errors + .set.errors), missing: .get.not_found}' "$OUT/live-load.json"
echo '== Verify persisted owner after migration =='
"$ROOT/bin/kvshardctl-sharded" -action layout -admin http://127.0.0.1:17110 > "$OUT/routing-layout.json"
echo '== Smoke test succeeded: no failed or missing operations =='
