#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

SCALES="${UNDR9_BENCH_SCALES:-1000,5000,10000}"
ITERATIONS="${UNDR9_BENCH_ITERATIONS:-5}"
OUTPUT="${UNDR9_BENCH_OUTPUT:-docs/operations/single-node-benchmark-baseline.json}"
SCENARIO_PROFILE="${UNDR9_BENCH_SCENARIO_PROFILE:-full}"
WORKLOAD_PROFILE="${UNDR9_BENCH_WORKLOAD_PROFILE:-standard}"
HNSW_SEMANTIC_TOP_K="${UNDR9_BENCH_HNSW_SEMANTIC_TOP_K:-}"
HNSW_EF_SEARCH="${UNDR9_BENCH_HNSW_EF_SEARCH:-}"
HNSW_M="${UNDR9_BENCH_HNSW_M:-}"
HNSW_EF_CONSTRUCTION="${UNDR9_BENCH_HNSW_EF_CONSTRUCTION:-}"

ARGS=(
  --scales "$SCALES"
  --iterations "$ITERATIONS"
  --scenario-profile "$SCENARIO_PROFILE"
  --workload-profile "$WORKLOAD_PROFILE"
  --output "$OUTPUT"
)

if [[ -n "$HNSW_SEMANTIC_TOP_K" ]]; then
  ARGS+=(--hnsw-semantic-top-k "$HNSW_SEMANTIC_TOP_K")
fi
if [[ -n "$HNSW_EF_SEARCH" ]]; then
  ARGS+=(--hnsw-ef-search "$HNSW_EF_SEARCH")
fi
if [[ -n "$HNSW_M" ]]; then
  ARGS+=(--hnsw-m "$HNSW_M")
fi
if [[ -n "$HNSW_EF_CONSTRUCTION" ]]; then
  ARGS+=(--hnsw-ef-construction "$HNSW_EF_CONSTRUCTION")
fi

cargo run -q -p undr9-cli --bin undr9-bench -- \
  "${ARGS[@]}"
