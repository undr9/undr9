# UNDR9 Developer Guide

This guide is the fastest way for a developer to understand the repository, run
UNDR9 locally, validate changes, and ship work without tripping over the current
project conventions.

## What UNDR9 Is

UNDR9 is a Rust workspace for a single-node graph database with:

- durable storage backed by snapshots, deltas, and a write-ahead log
- HTTP APIs for node, edge, query, health, readiness, metrics, and admin flows
- CLI workflows for storage maintenance, backup and restore, replication, and drills
- benchmark and recovery-drill scripts for operational validation
- Docker and GHCR publishing for local and server deployment

The current repository remote and package metadata point at:

- GitHub repo: `https://github.com/undr9/undr9`
- GHCR image: `ghcr.io/undr9/undr9`

## Quick Start

### Prerequisites

Install:

- Rust `1.81` or newer matching `rust-toolchain.toml`
- Cargo
- Docker Desktop or Docker Engine
- Git

Optional but useful:

- `jq` for reading JSON responses
- `curl` for API smoke tests

### Clone And Enter The Repo

```bash
git clone https://github.com/undr9/undr9.git
cd undr9
```

### Build The Workspace

```bash
cargo check --workspace
```

### Start A Local Node

UNDR9 requires three API keys at runtime. Use strong keys even for local work.

```bash
export UNDR9_ADMIN_API_KEY=dev-admin-key-0000000000001
export UNDR9_WRITER_API_KEY=dev-writer-key-000000000001
export UNDR9_READER_API_KEY=dev-reader-key-000000000001

cargo run -p undr9-cli --bin undr9-cli -- \
  serve \
  --root ./data \
  --bind 127.0.0.1:8080 \
  --node-id node-1
```

### Verify The Server

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

Expected readiness response:

```json
{
  "service": "undr9",
  "status": "ready"
}
```

### Query With Per-Request `top_k`

`top_k` is a query-level override for the semantic candidate budget used by `VectorSearch`
and `RankedRetrieval`. It does not change the backend globally, and it does not replace the
response `limit`.

```bash
curl -X POST http://127.0.0.1:8080/v1/query \
  -H 'content-type: application/json' \
  -H "x-api-key: ${UNDR9_READER_API_KEY}" \
  -d '{
    "RankedRetrieval": {
      "query_vector": [1.0, 0.0],
      "reference_node_id": "node_a",
      "edge_type": "relates_to",
      "from_epoch_ms": 1710000000000,
      "to_epoch_ms": 1719999999999,
      "limit": 10,
      "top_k": 50,
      "now_epoch_ms": 1720000000000,
      "retrieval_profile": "v1-default"
    }
  }'
```

### Create A Test Node

```bash
curl -X POST http://127.0.0.1:8080/v1/nodes \
  -H 'content-type: application/json' \
  -H "x-api-key: ${UNDR9_WRITER_API_KEY}" \
  -d '{
    "id": "node_a",
    "node_type": "memory",
    "properties": {
      "unique_key": { "kind": "String", "value": "alpha" },
      "timestamp": { "kind": "Integer", "value": 1710000000000 }
    }
  }'
```

### Query It Back

```bash
curl -X POST http://127.0.0.1:8080/v1/query \
  -H 'content-type: application/json' \
  -H "x-api-key: ${UNDR9_READER_API_KEY}" \
  -d '{
    "GetNodeById": {
      "node_id": "node_a"
    }
  }'
```

## Important First-Time Gotchas

- `cargo run -p undr9-cli -- --help` fails because the package has two binaries.
- Use `--bin undr9-cli` for the main CLI and `--bin undr9-bench` for benchmarks.
- The server fails fast if admin, writer, or reader API keys are missing, duplicated, or obviously weak.
- `/healthz` is liveness; `/readyz` is readiness and returns `503` while the server is draining.
- The default local data root is `./data`; keep it out of commits.
- `README_local.md` remains local-only and gitignored; the tracked root `README.md` is the public GitHub landing page.

## Repository Map

### Top-Level

- `Cargo.toml`: workspace definition and shared dependency versions
- `CONTRIBUTING.md`: contributor expectations and pre-push validation
- `Dockerfile`: production-oriented multi-stage image build
- `docker-compose.yml`: local or server container deployment
- `scripts/`: benchmark and recovery automation
- `docs/`: API and operations documentation
- `.github/workflows/`: CI and GHCR publishing workflows

### Workspace Crates

- `crates/common`: shared utility types and helpers
- `crates/config`: runtime configuration model and defaults
- `crates/core`: core graph types such as nodes, edges, and values
- `crates/storage`: durable storage engine and recovery logic
- `crates/wal`: write-ahead log encoding, replay, and validation
- `crates/index`: index build and lookup support
- `crates/query`: exact, traversal, vector, and ranked retrieval logic
- `crates/memory`: in-memory ranking and retrieval helpers
- `crates/auth`: API key auth and role enforcement
- `crates/api`: Axum HTTP server and endpoint wiring
- `crates/observability`: metrics, tracing, and audit logging integration
- `crates/replication`: replication state and shipped-record workflows
- `crates/cluster`: cluster metadata and node-topology behavior
- `crates/cli`: operational CLI and benchmark binaries

## Local Development Workflow

### 1. Read The Current Defaults

The CLI exposes generated defaults:

```bash
cargo run -p undr9-cli --bin undr9-cli -- show-default-config
```

Current notable defaults:

- bind address: `127.0.0.1:8080`
- request timeout: `5000ms`
- max request body: `1048576` bytes
- storage root: `./data`
- WAL segment size: `67108864` bytes
- WAL replay guardrail: `536870912` bytes
- maintenance limits: `5,000,000` nodes and `10,000,000` edges
- vector backend: `hnsw`
- vector semantic candidate default: `100`
- HNSW `ef_search`: `64`

You can still force the exact backend explicitly:

```bash
export UNDR9_VECTOR_INDEX_BACKEND=exact
```

Or keep HNSW and tune the global backend settings:

```bash
export UNDR9_VECTOR_INDEX_SEMANTIC_TOP_K=250
export UNDR9_HNSW_EF_SEARCH=128
```

### 2. Inspect The On-Disk Layout

```bash
cargo run -p undr9-cli --bin undr9-cli -- print-layout
```

Current layout:

- `data/manifest.json`
- `data/wal/`
- `data/nodes/`
- `data/edges/`
- `data/indexes/`
- `data/vectors/`
- `data/meta/`

### 3. Run The Full Local Validation Suite

Run the same checks as CI before you push:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

### 4. Keep Changes Scoped

Project expectations from `CONTRIBUTING.md`:

- keep module boundaries clean
- keep transport logic out of storage and query crates
- add tests for meaningful behavior changes
- update docs when behavior or architecture changes
- avoid unnecessary dependencies and circular crate coupling

## Running UNDR9

### Main Server

```bash
cargo run -p undr9-cli --bin undr9-cli -- serve --help
```

Current serve options:

- `--root <ROOT>`: storage root, default `./data`
- `--bind <BIND>`: listen address, default `127.0.0.1:8080`
- `--node-id <NODE_ID>`: local node identifier, default `node-1`

### Useful CLI Commands

```bash
cargo run -p undr9-cli --bin undr9-cli -- --help
```

Core commands you will use most often:

- `serve`
- `inspect-storage`
- `verify-storage`
- `compact-storage`
- `rebuild-indexes`
- `backup-storage`
- `restore-storage`
- `repair-storage`
- `export`
- `import`
- `run-transaction`
- `recovery-drill`
- replication and cluster commands for leader and follower flows

### Storage Maintenance

```bash
cargo run -p undr9-cli --bin undr9-cli -- verify-storage --root ./data
cargo run -p undr9-cli --bin undr9-cli -- compact-storage --root ./data
cargo run -p undr9-cli --bin undr9-cli -- rebuild-indexes --root ./data
```

### Backup And Restore

```bash
cargo run -p undr9-cli --bin undr9-cli -- backup-storage --root ./data --destination ./backup
cargo run -p undr9-cli --bin undr9-cli -- restore-storage --root ./data --source ./backup
cargo run -p undr9-cli --bin undr9-cli -- restore-storage --root ./data --source ./backup --target-lsn 42
```

Important behavior:

- backups write a manifest with checksums
- restore validates before cutover
- restore can target a retained LSN for PITR validation
- raw file copies of a live data directory are not the preferred backup strategy

### Import And Export

```bash
cargo run -p undr9-cli --bin undr9-cli -- export --root ./data ./graph.jsonl
cargo run -p undr9-cli --bin undr9-cli -- import --root ./data ./graph.jsonl
```

## HTTP API Developer Notes

Authentication is API-key based:

```text
x-api-key: <api-key>
```

Roles:

- admin
- writer
- reader

Key endpoints:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `POST /v1/nodes`
- `GET /v1/nodes/:id`
- `POST /v1/edges`
- `POST /v1/query`
- `GET /v1/admin/maintenance/status`

Supported query families include:

- exact node lookup
- unique-key lookup
- neighbor listing
- bounded traversal
- label search
- time-range search
- vector search
- ranked retrieval

Primary API doc:

- `docs/api/http.md`

## Observability

UNDR9 exposes metrics at:

```text
GET /metrics
```

Useful environment variables:

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

Developer takeaways:

- use JSON logs locally when debugging request flow
- reuse `traceparent` if you are testing distributed traces
- look for `x-undr9-trace-id` in API responses
- OTLP export is optional and disabled by default

## Benchmarks And Recovery Drills

### Standard Benchmark Run

```bash
./scripts/run-benchmarks.sh
```

Controllable through:

- `UNDR9_BENCH_SCALES`
- `UNDR9_BENCH_ITERATIONS`
- `UNDR9_BENCH_OUTPUT`
- `UNDR9_BENCH_SCENARIO_PROFILE`
- `UNDR9_BENCH_WORKLOAD_PROFILE`

### Large-Scale Storage Benchmark Run

```bash
./scripts/run-large-scale-benchmarks.sh
```

The large-scale storage-only compact path uses chunked generation so `1M+`
benchmarks do not need to materialize the full graph in memory first.

### Recovery Drill

```bash
./scripts/run-recovery-drill.sh
```

The recovery drill writes a JSON report to:

- `docs/operations/recovery-drill-report.json`

Published benchmark examples live under:

- `docs/operations/single-node-benchmark-baseline.json`
- `docs/operations/single-node-benchmark-100k.json`
- `docs/operations/single-node-benchmark-1m-storage.json`
- `docs/operations/single-node-benchmark-10m-storage.json`

## Docker And GHCR

### Local Docker Build

```bash
docker build -t undr9:local .
```

### Local Docker Run

```bash
docker run --rm \
  -p 8080:8080 \
  -v undr9_data:/var/lib/undr9/data \
  -e UNDR9_ADMIN_API_KEY=dev-admin-key-0000000000001 \
  -e UNDR9_WRITER_API_KEY=dev-writer-key-000000000001 \
  -e UNDR9_READER_API_KEY=dev-reader-key-000000000001 \
  undr9:local
```

### Docker Compose

```bash
export UNDR9_ADMIN_API_KEY=dev-admin-key-0000000000001
export UNDR9_WRITER_API_KEY=dev-writer-key-000000000001
export UNDR9_READER_API_KEY=dev-reader-key-000000000001
docker compose up --build
```

### GHCR

UNDR9 publishes container images to:

```text
ghcr.io/undr9/undr9
```

The publish workflow now emits a multi-arch manifest for:

- `linux/amd64`
- `linux/arm64`

That means:

- Apple Silicon developers can pull the standard tag
- Intel Linux servers can use the same tag
- ARM64 Linux servers can use the same tag
- Docker Desktop on macOS and Windows can test the Linux container locally

Pull:

```bash
docker pull ghcr.io/undr9/undr9:latest
```

## Replication And Cluster Work

For replication or cluster-state testing, the CLI currently supports:

- `configure-leader`
- `configure-follower`
- `register-replica`
- `replication-status`
- `replication-history`
- `apply-replication`
- `acknowledge-replica`
- `show-cluster-topology`
- `promote-node`

The relevant metadata currently lives under `data/meta/`.

## Troubleshooting

### `cargo run` complains that it cannot determine which binary to run

Use one of these:

```bash
cargo run -p undr9-cli --bin undr9-cli -- --help
cargo run -p undr9-cli --bin undr9-bench -- --help
```

### Docker volume permissions fail on first start

The current image includes `docker-entrypoint.sh`, which prepares the mounted
data directory and then drops privileges to the `undr9` user. Rebuild the image
if you are testing an older local layer cache.

### `docker pull ghcr.io/undr9/undr9:latest` fails on Apple Silicon

That previously happened when only an `amd64` image was published. The workflow
now publishes both `amd64` and `arm64`; retry after the latest `docker` GitHub
Actions workflow completes successfully.

### Readiness Is Failing During Shutdown

This is expected. During drain or graceful shutdown:

- `/healthz` stays usable for basic liveness
- `/readyz` flips unavailable so load balancers stop sending traffic

### A Maintenance Request Is Rejected

Check:

- `UNDR9_MAINTENANCE_MAX_NODES`
- `UNDR9_MAINTENANCE_MAX_EDGES`
- `GET /v1/admin/maintenance/status`

## Recommended Reading

Start here after this guide:

- `CONTRIBUTING.md`
- `docs/api/http.md`
- `docs/operations/one-node.md`
- `deployments/docker/README.md`
- `.github/workflows/ci.yml`
- `.github/workflows/docker.yml`

## Suggested First Contribution Path

If you are new to the codebase, this sequence works well:

1. Run the server locally and hit `/healthz`, `/readyz`, and one node CRUD request.
2. Read `docs/api/http.md` and `docs/operations/one-node.md`.
3. Trace one request path through `crates/api`, `crates/query`, and `crates/storage`.
4. Run the full validation suite.
5. Make a small docs or API improvement before touching storage or recovery logic.
