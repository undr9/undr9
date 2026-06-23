#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

SCALES="${UNDR9_BENCH_SCALES:-1000,5000,10000}"
ITERATIONS="${UNDR9_BENCH_ITERATIONS:-5}"
OUTPUT="${UNDR9_BENCH_OUTPUT:-docs/operations/single-node-benchmark-baseline.json}"
SCENARIO_PROFILE="${UNDR9_BENCH_SCENARIO_PROFILE:-full}"
WORKLOAD_PROFILE="${UNDR9_BENCH_WORKLOAD_PROFILE:-standard}"

cargo run -q -p undr9-cli --bin undr9-bench -- \
  --scales "$SCALES" \
  --iterations "$ITERATIONS" \
  --scenario-profile "$SCENARIO_PROFILE" \
  --workload-profile "$WORKLOAD_PROFILE" \
  --output "$OUTPUT"
