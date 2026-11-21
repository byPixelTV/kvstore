#!/usr/bin/env bash
set -Eeuo pipefail
# Five minutes of uninterrupted 50/50 traffic against an isolated cluster.
: "${RUNS:=1}" "${DURATION_SECONDS:=300}" "${WARMUP_SECONDS:=3}" \
  "${CONCURRENCIES:=256}" "${KEYSPACE:=16384}" "${BATCH_SIZE:=16}" \
  "${CONNECTIONS:=16}" "${TARGET_OPS:=0}" "${METRICS_INTERVAL:=30}"
export RUNS DURATION_SECONDS WARMUP_SECONDS CONCURRENCIES KEYSPACE BATCH_SIZE CONNECTIONS TARGET_OPS METRICS_INTERVAL
export REQUIRE_SLO=1
exec bash "$(dirname -- "${BASH_SOURCE[0]}")/benchmark-sharded-linux.sh"
