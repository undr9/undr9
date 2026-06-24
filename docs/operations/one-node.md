# One-Node Operations Guide

## Local Run

```bash
export UNDR9_ADMIN_API_KEY=replace-with-admin-key
export UNDR9_WRITER_API_KEY=replace-with-writer-key
export UNDR9_READER_API_KEY=replace-with-reader-key
cargo run -p undr9-cli -- serve --root ./data --bind 127.0.0.1:8080 --node-id node-1
```

`undr9` does not terminate TLS in-process. Put `Caddy` or `Traefik` in front of the server and terminate TLS at the reverse proxy.

Example deployment flow:

```text
Client -> Caddy/Traefik -> UNDR9
```

Runtime hardening:

- `undr9 serve` now fails fast if the admin, writer, or reader API keys are missing, duplicated, or obviously weak.
- `UNDR9_MAINTENANCE_MAX_NODES` and `UNDR9_MAINTENANCE_MAX_EDGES` define the largest dataset size that maintenance endpoints will process without rejecting the request.
- `SIGTERM` and `Ctrl+C` trigger a graceful shutdown path that marks `/readyz` unavailable before flushing storage state.
- Use `/healthz` for liveness and `/readyz` for load-balancer readiness.

## Observability

UNDR9 exposes Prometheus metrics at:

```text
GET /metrics
```

Tracing and structured logs are configured through environment variables before `undr9 serve` starts:

```bash
export UNDR9_LOG_LEVEL=info
export UNDR9_TRACING_ENABLED=true
export UNDR9_TRACING_JSON=true
export UNDR9_METRICS_ENABLED=true
export UNDR9_OTLP_ENABLED=true
export UNDR9_OTLP_PROTOCOL=grpc
export UNDR9_OTLP_ENDPOINT=http://127.0.0.1:4317
export UNDR9_OTLP_HEADERS=authorization=Bearer%20token
export UNDR9_OTLP_TIMEOUT_MS=10000
```

Behavior:

- `UNDR9_LOG_LEVEL` controls the runtime tracing filter.
- `UNDR9_TRACING_ENABLED` enables or disables tracing bootstrap.
- `UNDR9_TRACING_JSON=true` emits JSON logs suitable for collectors and log pipelines.
- `UNDR9_METRICS_ENABLED=false` disables `/metrics`.
- `UNDR9_OTLP_ENABLED=true` enables in-process OTLP trace export.
- `UNDR9_OTLP_PROTOCOL` supports `grpc` and `http/protobuf`.
- `UNDR9_OTLP_ENDPOINT` points to the collector or backend endpoint.
- `UNDR9_OTLP_HEADERS` passes comma-separated `key=value` headers for auth or tenancy.
- `UNDR9_OTLP_TIMEOUT_MS` controls OTLP exporter timeout.

Trace correlation:

- If a request includes a W3C `traceparent` header, UNDR9 reuses its trace ID.
- Every API response includes `x-undr9-trace-id`.
- Query, transaction, startup, restore, repair, compaction, and index rebuild paths emit tracing events.

Current tracing model:

- UNDR9 can export traces in-process over OTLP using `grpc` or `http/protobuf`.
- UNDR9 keeps `traceparent`, `x-undr9-trace-id`, and JSON structured logs for correlation and debugging.
- OTLP is the primary production tracing path; JSON logs remain the compatibility and debugging layer.

Collector examples:

```bash
# gRPC OTLP to a local collector or Tempo/Jaeger gateway
export UNDR9_OTLP_ENABLED=true
export UNDR9_OTLP_PROTOCOL=grpc
export UNDR9_OTLP_ENDPOINT=http://127.0.0.1:4317
```

```bash
# HTTP/protobuf OTLP to a collector or vendor endpoint
export UNDR9_OTLP_ENABLED=true
export UNDR9_OTLP_PROTOCOL=http/protobuf
export UNDR9_OTLP_ENDPOINT=http://127.0.0.1:4318/v1/traces
```

Examples:

- Grafana Tempo via OpenTelemetry Collector: point `UNDR9_OTLP_ENDPOINT` at the collector `4317` or `4318/v1/traces` receiver.
- Jaeger native OTLP: use its OTLP gRPC or HTTP endpoint directly.
- Datadog / New Relic / other vendors: set `UNDR9_OTLP_ENDPOINT` to the vendor OTLP ingest URL and pass auth or tenancy through `UNDR9_OTLP_HEADERS`.

## Storage Maintenance

```bash
cargo run -p undr9-cli -- verify-storage --root ./data
cargo run -p undr9-cli -- compact-storage --root ./data
cargo run -p undr9-cli -- rebuild-indexes --root ./data
cargo run -p undr9-cli -- backup-storage --root ./data --destination ./backup
cargo run -p undr9-cli -- restore-storage --root ./data --source ./backup
cargo run -p undr9-cli -- restore-storage --root ./data --source ./backup --target-lsn 42
cargo run -p undr9-cli -- repair-storage --root ./data
cargo run -p undr9-cli -- run-transaction --root ./data --plan ./transaction-plan.json
```

Backup and restore behavior:

- Backups now write `backup-manifest.json` with file checksums and an integrity snapshot.
- Restore validates the backup manifest before cutover.
- Restore is staged first and only replaces the live directory after the staged copy verifies cleanly.
- `restore-storage --target-lsn <lsn>` provides point-in-time restore within the retained WAL window stored in the verified backup.
- Restore-to-LSN rejects targets older than the published checkpoint or newer than the retained WAL history.

Suggested recovery objectives for a single-node deployment:

- `RPO`: bounded by the retained WAL history included in the latest verified backup.
- `RTO`: measure with `restore-storage` on your production-sized dataset and keep the result with your runbook.

Recommended recovery drill:

1. Run `backup-storage` against live data.
2. Restore the backup into a separate directory.
3. Validate node and edge counts with `inspect-storage` and `verify-storage`.
4. Repeat once with `--target-lsn` set to a known retained commit to verify PITR.

Automated recovery drill:

```bash
./scripts/run-recovery-drill.sh
```

The drill runs verified backup, full restore, PITR restore to the latest retained LSN, and integrity validation, then writes a JSON report with measured backup and restore timings.
The report also records `latest_restorable_lsn` so operators can see the newest retained PITR target validated by the drill.

Maintenance visibility:

- `GET /v1/admin/maintenance/status` reports the last maintenance operation, outcome, elapsed time, and the configured node/edge budgets.
- Maintenance requests reject with `maintenance_budget_exceeded` when the current dataset is larger than the configured maintenance window.

## Import / Export

Use JSONL for interoperability and keep binary snapshots for on-disk backups.

```bash
cargo run -p undr9-cli -- export --root ./data ./graph.jsonl
cargo run -p undr9-cli -- import --root ./data ./graph.jsonl
```

Each JSONL line is one record:

```json
{"type":"node", ...}
{"type":"edge", ...}
```

## Replication Operations

Milestone 6 stores cluster and replication metadata in `data/meta/cluster-topology.json` and `data/meta/replication-state.json`.

```bash
cargo run -p undr9-cli -- configure-leader --root ./data --bind 127.0.0.1:9100 --node-id leader-1
cargo run -p undr9-cli -- register-replica --root ./data --bind 127.0.0.1:9100 --node-id leader-1 --replica-node-id replica-1 --replica-address 127.0.0.1:9101
cargo run -p undr9-cli -- replication-status --root ./data --bind 127.0.0.1:9100 --node-id leader-1
cargo run -p undr9-cli -- replication-history --root ./data --bind 127.0.0.1:9100 --node-id leader-1 --after-source-lsn 0 --output ./replication-records.json
cargo run -p undr9-cli -- show-cluster-topology --root ./data --bind 127.0.0.1:9100 --node-id leader-1
```

To configure a follower and apply shipped records:

```bash
cargo run -p undr9-cli -- configure-follower --root ./replica-data --bind 127.0.0.1:9101 --node-id replica-1 --leader-node-id leader-1 --leader-address 127.0.0.1:9100
cargo run -p undr9-cli -- apply-replication --root ./replica-data --bind 127.0.0.1:9101 --node-id replica-1 --file ./replication-records.json
cargo run -p undr9-cli -- acknowledge-replica --root ./data --bind 127.0.0.1:9100 --node-id leader-1 --replica-node-id replica-1 --source-lsn 4
```

To promote a replacement leader:

```bash
cargo run -p undr9-cli -- promote-node --root ./data --bind 127.0.0.1:9100 --node-id leader-1 --target-node-id replica-1
```

## Docker

```bash
export UNDR9_ADMIN_API_KEY=replace-with-admin-key
export UNDR9_WRITER_API_KEY=replace-with-writer-key
export UNDR9_READER_API_KEY=replace-with-reader-key
docker compose up --build
```

Container notes:

- The image exposes `/healthz` and `/readyz`; the compose file wires `/readyz` as the healthcheck.
- `docker stop` sends `SIGTERM`, which UNDR9 now handles as a graceful drain-and-flush path.
- Keep the storage volume mounted at `/var/lib/undr9/data` and back it up with `backup-storage`, not raw file copies of a live directory.

## Benchmarks

```bash
./scripts/run-benchmarks.sh
```

Benchmark output now records:

- microsecond samples instead of millisecond-only timings
- `p50`, `p95`, and `p99`
- per-scenario throughput in operations per second
- `peak_rss_bytes` for the benchmark process
- per-scale storage footprint including WAL, snapshots, deltas, index bytes, compaction time, and recovery-open time
- direct `exact_lookup` and `list_neighbors_1_hop` scenarios in addition to traversal and retrieval paths
- separate exact and HNSW query timings for `vector_search` and `ranked_retrieval`
- `vector_index_footprint` with `hnsw_index_bytes`, `hnsw_build_elapsed_us`, and `hnsw_reload_elapsed_us`
- `quality_comparisons` with exact-vs-HNSW `overlap_ratio`, `jaccard_ratio`, `top1_match`, and result-set difference counts

Vector benchmark behavior:

- `vector_search_exact` and `ranked_retrieval_exact` use the exact backend
- `vector_search_hnsw` and `ranked_retrieval_hnsw` use the HNSW backend
- the benchmark runner forces `exact_fallback_threshold=1` for HNSW runs so small baseline scales actually exercise the ANN backend
- the current published benchmark defaults use `semantic_top_k=250` and `ef_search=128`
- this override is benchmark-only and does not change the normal production defaults

Published quality observations from the current `100k` artifact:

- `vector_search` shows `overlap_ratio=1.0`, `jaccard_ratio=1.0`, and `top1_match=true`
- `ranked_retrieval` shows `overlap_ratio=0.56`, `jaccard_ratio=0.3889`, and `top1_match=true`
- treat these as workload-specific measurements, not universal recall guarantees

The runner defaults are:

```text
scales=1000,5000,10000
iterations=5
```

Override them with `UNDR9_BENCH_SCALES`, `UNDR9_BENCH_ITERATIONS`, and `UNDR9_BENCH_OUTPUT` when publishing a different envelope.
The standard runner also accepts `UNDR9_BENCH_SCENARIO_PROFILE`, `UNDR9_BENCH_WORKLOAD_PROFILE`, `UNDR9_BENCH_HNSW_SEMANTIC_TOP_K`, `UNDR9_BENCH_HNSW_EF_SEARCH`, `UNDR9_BENCH_HNSW_M`, and `UNDR9_BENCH_HNSW_EF_CONSTRUCTION`.

Published artifacts:

- `docs/operations/single-node-benchmark-baseline.json` for the repeatable small-scale baseline
- `docs/operations/single-node-benchmark-100k.json` for a target-scale `100k` node single-pass benchmark run with exact-vs-HNSW vector query timings
- `docs/operations/single-node-benchmark-1m-storage.json` for a `1M` node storage-only compact-profile benchmark run

Large-scale storage benchmark runner:

```bash
./scripts/run-large-scale-benchmarks.sh
```

The `storage-only` + `compact` profile now uses chunked batch generation so `1M+` storage benchmarks do not need to materialize the full node and edge workload in memory before execution.

Default large-scale settings:

```text
scales=100000,1000000
iterations=1
scenario_profile=storage-only
workload_profile=compact
output=docs/operations/single-node-benchmark-large-scale.json
```

## Compatibility Checks

```bash
./scripts/run-compatibility.sh
```

## Recovery Drill Report

```bash
./scripts/run-recovery-drill.sh
```

Default output:

```text
docs/operations/recovery-drill-report.json
```

## Audit Log

Sensitive write and maintenance actions append JSON lines to:

```text
data/meta/audit.log
```
