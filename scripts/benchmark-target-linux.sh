#!/usr/bin/env bash
set -Eeuo pipefail

# Benchmarks an ALREADY RUNNING standalone or cluster endpoint. It neither
# starts nor stops any containers. Use only test namespaces (kvbench does so).
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
BENCH="${BENCH_BIN:-./bin/kvbench}"
[[ -x "$BENCH" ]] || { echo "Missing $BENCH. Build with: go build -o bin/kvbench ./cmd/kvbench" >&2; exit 1; }
command -v python3 >/dev/null || { echo 'python3 is required to summarize JSON.' >&2; exit 1; }

ENDPOINT="${TARGET_ENDPOINT:-127.0.0.1:50051}"
WORKERS="${TARGET_WORKERS:-256}"
CONNECTIONS="${TARGET_CONNECTIONS:-16}"
DURATION="${TARGET_DURATION:-15s}"
WARMUP="${TARGET_WARMUP:-3s}"
VALUE_SIZE="${TARGET_VALUE_SIZE:-256}"
KEYSPACE="${TARGET_KEYSPACE:-256}"
CONSISTENCY="${TARGET_CONSISTENCY:-strong}"
LEADER_MAP="${TARGET_LEADER_MAP:-}"
ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
OUT="$ROOT/benchmarks/results/target-$ID"
mkdir -p "$OUT"

for mode in mixed; do
    file="$OUT/${mode}-${CONSISTENCY}.json"
    echo "== $mode endpoint=$ENDPOINT workers=$WORKERS consistency=$CONSISTENCY =="
    leader_flags=()
    if [[ -n "$LEADER_MAP" ]]; then leader_flags=(-leader-map "$LEADER_MAP"); fi
    "$BENCH" -addr "$ENDPOINT" -mode "$mode" "${leader_flags[@]}" \
       -consistency "$CONSISTENCY" -read-percent 50 \
       -duration "$DURATION" -warmup "$WARMUP" \
       -concurrency "$WORKERS" -connections "$CONNECTIONS" \
       -keys "$KEYSPACE" -value-size "$VALUE_SIZE" \
       -timeout 10s -json > "$file"
    python3 - "$file" <<'PY'
import json, sys
with open(sys.argv[1], encoding='utf-8') as f: data = json.load(f)
g, s = data['get'], data['set']
print(f"GET {g['ops_sec']:.0f}/s  SET {s['ops_sec']:.0f}/s  "
      f"total {g['ops_sec']+s['ops_sec']:.0f}/s  "
      f"errors={g['errors']+s['errors']}  "
      f"GET p99={g['latency']['p99_ms']:.2f}ms  "
      f"SET p99={s['latency']['p99_ms']:.2f}ms")
if data['mode'] == 'mixed':
    print('TARGET 10k GET/s + 10k SET/s:',
          'PASS' if g['ops_sec'] >= 10000 and s['ops_sec'] >= 10000 and not (g['errors']+s['errors']) else 'NOT REACHED')
PY
done
echo "Reports: $OUT"
