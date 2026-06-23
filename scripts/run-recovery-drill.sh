#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

ROOT="${UNDR9_DRILL_ROOT:-./data}"
OUTPUT="${UNDR9_DRILL_OUTPUT:-docs/operations/recovery-drill-report.json}"

cargo run -q -p undr9-cli --bin undr9-cli -- recovery-drill \
  --root "$ROOT" \
  --output "$OUTPUT"
