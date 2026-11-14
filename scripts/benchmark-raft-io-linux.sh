#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C

# Reproducible A/B test: identical code, topology, keyspace and concurrency.
# Both profiles use *durable* Pebble/Raft storage and isolated Docker projects.
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
RUNS="${RUNS:-2}"
DURATION_SECONDS="${DURATION_SECONDS:-12}"
WARMUP_SECONDS="${WARMUP_SECONDS:-2}"
CONCURRENCIES="${CONCURRENCIES:-256}"
KEYSPACE="${KEYSPACE:-16384}"
RUN_ID="raft-io-$(date -u +%Y%m%dT%H%M%SZ)-$$"
OUT="$ROOT/benchmarks/results/live-linux/$RUN_ID"
mkdir -p "$OUT"

benchmark_profile() {
  local label="$1"
  shift
  echo "=== Raft I/O profile: $label ==="
  # Profile variables are injected into both the Compose backends and the
  # JSON report. Benchmark-sharded creates its own project/volumes each time.
  local previous
  previous="$(find "$ROOT/benchmarks/results/live-linux" -maxdepth 1 -type d -name 'sharded-*' -printf '%T@ %p\n' | sort -nr | head -1 | cut -d' ' -f2- || true)"
  env KEEP_RUNNING=0 RUNS="$RUNS" DURATION_SECONDS="$DURATION_SECONDS" \
    WARMUP_SECONDS="$WARMUP_SECONDS" CONCURRENCIES="$CONCURRENCIES" \
    KEYSPACE="$KEYSPACE" "$@" \
    bash "$ROOT/scripts/benchmark-sharded-linux.sh" 2>&1 | tee "$OUT/${label}.log"
  local newest
  newest="$(find "$ROOT/benchmarks/results/live-linux" -maxdepth 1 -type d -name 'sharded-*' -printf '%T@ %p\n' | sort -nr | head -1 | cut -d' ' -f2-)"
  [[ -n "$newest" && "$newest" != "$previous" ]] || { echo "No new benchmark result found for $label" >&2; return 1; }
  cp -a -- "$newest" "$OUT/$label"
  echo "source=$newest" > "$OUT/${label}-source.txt"
}

# Old-style profile: high quorum-RPC parallelism, old Raft append limit,
# and fixed 1.5ms write-batching window. This is an A/B configuration,
# NOT a bit-for-bit reproduction of a previously committed build.
benchmark_profile baseline \
  KVSTORE_READ_VERIFY_PARALLELISM=64 \
  KVSTORE_RAFT_MAX_APPEND_ENTRIES=64 \
  KVSTORE_RAFT_COMMIT_TIMEOUT=50ms \
  KVSTORE_WRITE_BATCH_WINDOW=1500us \
  KVSTORE_WRITE_BATCH_MAX_WINDOW=1500us \
  KVSTORE_WRITE_BATCH_TARGET=256

benchmark_profile tuned \
  KVSTORE_READ_VERIFY_PARALLELISM=2 \
  KVSTORE_RAFT_MAX_APPEND_ENTRIES=128 \
  KVSTORE_RAFT_COMMIT_TIMEOUT=50ms \
  KVSTORE_WRITE_BATCH_WINDOW=1500us \
  KVSTORE_WRITE_BATCH_MAX_WINDOW=5ms \
  KVSTORE_WRITE_BATCH_TARGET=24

printf '%s\n' 'A/B done. Archive both baseline and tuned subdirectories:'
printf '  cd %q && zip -r /root/kvstore-raft-io-bench.zip %q\n' "$ROOT" "benchmarks/results/live-linux/$RUN_ID"
