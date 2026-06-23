#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

SCALES="${UNDR9_LARGE_BENCH_SCALES:-100000,1000000}"
ITERATIONS="${UNDR9_LARGE_BENCH_ITERATIONS:-1}"
SCENARIO_PROFILE="${UNDR9_LARGE_BENCH_SCENARIO_PROFILE:-storage-only}"
WORKLOAD_PROFILE="${UNDR9_LARGE_BENCH_WORKLOAD_PROFILE:-compact}"
OUTPUT="${UNDR9_LARGE_BENCH_OUTPUT:-docs/operations/single-node-benchmark-large-scale.json}"

cargo run -q -p undr9-cli --bin undr9-bench -- \
  --scales "$SCALES" \
  --iterations "$ITERATIONS" \
  --scenario-profile "$SCENARIO_PROFILE" \
  --workload-profile "$WORKLOAD_PROFILE" \
  --output "$OUTPUT"
