<p align="center">
  <img src="assets/undr9-landscape.png" alt="UNDR9 header" />
</p>

# UNDR9

UNDR9 is a graph-native memory database for AI systems that need durable storage, semantic retrieval, and operational discipline in one runtime.

It combines graph relationships, vector search, temporal signals, importance, and confidence into a single database layer so memory does not have to be stitched together across multiple services.

## Why UNDR9

- Built as a database, not a demo: WAL-backed writes, recovery flows, compaction, backup, restore, and verification are first-class concerns.
- Designed for retrieval workloads: graph traversal, exact lookup, vector search, and ranked hybrid retrieval share the same data model.
- Operationally visible: health, readiness, metrics, tracing, and maintenance endpoints are part of the default surface.
- Practical to self-host: Rust workspace, Docker image, GHCR publishing, and one-node operational docs are already in the repo.
- Honest about evidence: published benchmark artifacts and current limitations are documented directly in the repository.

## Core Capabilities

- Durable storage with snapshots, deltas, WAL replay, verification, repair, and compaction.
- HTTP API for node CRUD, edge CRUD, queries, health, readiness, metrics, and admin operations.
- Query support for exact lookup, unique-key lookup, traversal, time-range search, vector search, and ranked retrieval.
- Operational tooling for backup, restore, PITR-oriented restore, recovery drills, and maintenance status reporting.
- Observability through Prometheus-style metrics, structured logs, trace propagation, and OTLP export.
- Container delivery through the checked-in Dockerfile and GHCR multi-arch image publishing.

## Operational Foundations

- Health model: `GET /healthz` for liveness and `GET /readyz` for readiness and drain awareness.
- Storage safety: WAL plus snapshot/delta persistence with rebuild, verify, repair, and compaction workflows.
- Recovery posture: recovery drill automation and backup manifest validation are included in the repository.
- Deployment path: Docker image, Docker Compose, and deployment notes are available for local or server use.
- Runtime controls: configuration is exposed through CLI defaults plus environment-variable overrides.

## Performance Evidence

The current benchmark artifacts were collected on an `Apple Silicon (macos/aarch64)` host under a `compact` workload profile.

### Scale And Footprint

| Scale Tier | Node Count | Edge Count | Peak RSS (RAM) | On-Disk Footprint | Post-Compaction Size | Recovery Open Time |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **100k** | `100,000` | `99,999` | `1.00 GB` | `40.46 MB` | `35.16 MB` | `1.28 s` |
| **1M** | `1,000,000` | `999,999` | `2.70 GB` | `278.54 MB` | `245.47 MB` | `9.41 s` |
| **10M** | `10,000,000` | `9,999,999` | `7.54 GB` | `2.85 GB` | `2.50 GB` | `104.32 s` |
| **100M** | `Not yet published` | `Not yet published` | `Not yet published` | `Not yet published` | `Not yet published` | `Not yet published` |

### Latency Profiles

| Scenario Name | 100k Scale Latency | 1M Scale Latency | 10M Scale Latency | Primary Performance Driver |
| :--- | :--- | :--- | :--- | :--- |
| **`storage_upsert`** | `4.07 s` (single batch) | `33.15 s` (single batch) | `363.42 s` (single batch) | WAL append volume, serialization, and fsync-heavy write throughput |
| **`storage_delete`** | `5.01 s` | `43.18 s` | `513.38 s` | Tombstone-heavy mutation volume plus WAL rewrite cost |
| **`wal_recovery`** | `8.72 s` | `67.10 s` | `730.44 s` | WAL replay loop, manifest validation, and full state rebuild on open |
| **`exact_lookup`** | `56 us` | `Not yet published` | `Not yet published` | Direct id or unique-key index lookup with minimal plan overhead |
| **`list_neighbors_1_hop`** | `56 us` | `Not yet published` | `Not yet published` | Adjacency index bucket scan for a single hop |
| **`traverse_5_hops`** | `346 us` | `Not yet published` | `Not yet published` | Bounded BFS over adjacency and reverse-adjacency indexes |
| **`shortest_path`** | `1,399 us` | `Not yet published` | `Not yet published` | Frontier expansion and visited-set maintenance across graph hops |
| **`temporal_range`** | `3,684 us` | `Not yet published` | `Not yet published` | Timestamp-bucket scan through the temporal index |
| **`vector_search_exact`** | `390,797 us` | `Not yet published` | `Not yet published` | Exact cosine scoring across all eligible vector candidates |
| **`vector_search_hnsw`** | `24,502 us` | `Not yet published` | `Not yet published` | HNSW ANN candidate generation with final vector scoring over the returned set |
| **`ranked_retrieval_exact`** | `410,973 us` | `Not yet published` | `Not yet published` | Full hybrid retrieval with exact semantic candidate generation |
| **`ranked_retrieval_hnsw`** | `2,070 us` | `Not yet published` | `Not yet published` | HNSW semantic prefilter unioned with structural candidates before hybrid reranking |

Published HNSW benchmark artifacts currently use `semantic_top_k=250` and `ef_search=128` at `100k`. The persisted vector-index sidecar footprint at that point is `5,892,367` bytes, with `207.55 s` build time and `140.47 s` warm-load time in the benchmark environment.

For quality alignment at `100k`, the current artifact shows `vector_search` exact-vs-HNSW top-50 overlap of `1.0` with `top1_match=true`, while `ranked_retrieval` shows top-50 overlap of `0.56` with `top1_match=true`. These are workload-specific published measurements, not a general recall guarantee.

## Quick Start

### Prerequisites

- Rust `1.81` or newer
- Cargo
- Docker Desktop or Docker Engine
- Git
- `curl`

### Clone

```bash
git clone https://github.com/undr9/undr9.git
cd undr9
```

### Build

```bash
cargo check --workspace
```

### Run Locally

UNDR9 requires three API keys at startup.

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

### Verify Health

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

### Create A Memory Node

```bash
curl -X POST http://127.0.0.1:8080/v1/nodes \
  -H 'content-type: application/json' \
  -H "x-api-key: ${UNDR9_WRITER_API_KEY}" \
  -d '{
    "id": "node_a",
    "node_type": "memory",
    "properties": {
      "unique_key": { "kind": "String", "value": "alpha" },
      "timestamp": { "kind": "Integer", "value": 1720000000000 },
      "importance": { "kind": "Float", "value": 0.9 },
      "confidence": { "kind": "Float", "value": 0.85 }
    },
    "vectors": {
      "default": [0.12, 0.44, 0.31, 0.78]
    }
  }'
```

### Query By Vector

`limit` controls the number of final results returned. `top_k` optionally overrides the semantic candidate pool for the request when HNSW is active. `vector_name` selects the named vector space and defaults to `default`.

```bash
curl -X POST http://127.0.0.1:8080/v1/query \
  -H 'content-type: application/json' \
  -H "x-api-key: ${UNDR9_READER_API_KEY}" \
  -d '{
    "VectorSearch": {
      "query_vector": [1.0, 0.0],
      "vector_name": "default",
      "node_type": "memory",
      "limit": 10,
      "top_k": 50
    }
  }'
```

### Query By Node Id

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

## Runtime Vector Behavior

- HNSW is the default runtime vector backend.
- Switch back to exact search with `export UNDR9_VECTOR_INDEX_BACKEND=exact`.
- Tune global defaults with `UNDR9_VECTOR_INDEX_SEMANTIC_TOP_K` and `UNDR9_HNSW_EF_SEARCH`.
- Store vectors only in the node `vectors` map; `properties.embedding` is not accepted.
- Use per-query `vector_name` to select the named vector space for `VectorSearch` and `RankedRetrieval`.
- Use per-query `top_k` to override the semantic candidate budget for `VectorSearch` and `RankedRetrieval`.

For the full runtime environment variable reference, including server, storage, WAL, auth, maintenance, vector index, observability, and audit settings, see [DEVELOPERS.md](./DEVELOPERS.md).

## Docker

### Build A Local Image

```bash
docker build -t undr9:local .
```

### Run The Local Image

```bash
docker run --rm \
  -p 8080:8080 \
  -v undr9_data:/var/lib/undr9/data \
  -e UNDR9_ADMIN_API_KEY=dev-admin-key-0000000000001 \
  -e UNDR9_WRITER_API_KEY=dev-writer-key-000000000001 \
  -e UNDR9_READER_API_KEY=dev-reader-key-000000000001 \
  undr9:local
```

### Pull From GHCR

UNDR9 publishes a multi-arch image at `ghcr.io/undr9/undr9` for:

- `linux/amd64`
- `linux/arm64`

```bash
docker pull ghcr.io/undr9/undr9:latest
```

## API Overview

Authentication uses:

```text
x-api-key: <api-key>
```

Key endpoints:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `POST /v1/nodes`
- `GET /v1/nodes/:id`
- `POST /v1/edges`
- `POST /v1/query`
- `GET /v1/admin/maintenance/status`

Supported query families:

- exact node lookup
- unique-key lookup
- neighbor listing
- bounded traversal
- label search
- time-range search
- vector search
- ranked retrieval

See the full API reference in [docs/api/http.md](./docs/api/http.md).

## Observability

Useful runtime settings:

```bash
export UNDR9_LOG_LEVEL=info
export UNDR9_TRACING_ENABLED=true
export UNDR9_TRACING_JSON=true
export UNDR9_METRICS_ENABLED=true
export UNDR9_OTLP_ENABLED=true
export UNDR9_OTLP_PROTOCOL=grpc
export UNDR9_OTLP_ENDPOINT=http://127.0.0.1:4317
```

Operational notes:

- `/metrics` exposes Prometheus-style metrics.
- `/readyz` flips unavailable during drain or graceful shutdown.
- Responses include `x-undr9-trace-id`.
- OTLP export can be enabled behind a collector.

## Common Commands

### Show CLI Commands

```bash
cargo run -p undr9-cli --bin undr9-cli -- --help
```

### Inspect Runtime Defaults

```bash
cargo run -p undr9-cli --bin undr9-cli -- show-default-config
cargo run -p undr9-cli --bin undr9-cli -- print-layout
```

### Storage Operations

```bash
cargo run -p undr9-cli --bin undr9-cli -- verify-storage --root ./data
cargo run -p undr9-cli --bin undr9-cli -- compact-storage --root ./data
cargo run -p undr9-cli --bin undr9-cli -- rebuild-indexes --root ./data
cargo run -p undr9-cli --bin undr9-cli -- backup-storage --root ./data --destination ./backup
cargo run -p undr9-cli --bin undr9-cli -- restore-storage --root ./data --source ./backup
```

### Import And Export

```bash
cargo run -p undr9-cli --bin undr9-cli -- export --root ./data ./graph.jsonl
cargo run -p undr9-cli --bin undr9-cli -- import --root ./data ./graph.jsonl
```

### Recovery Drill

```bash
./scripts/run-recovery-drill.sh
```

### Benchmarks

```bash
./scripts/run-benchmarks.sh
./scripts/run-large-scale-benchmarks.sh
```

## Retrieval Model

UNDR9 is designed for memory retrieval rather than only record storage. Ranked retrieval can combine multiple signals in one query path:

- `graph`: graph distance from a reference node can boost nearby context.
- `semantic`: vector similarity can surface related memories from the selected vector space.
- `timestamp`: temporal decay can keep recent context relevant without requiring hard recency filters.
- `importance`: explicitly stored importance can raise high-value memories.
- `confidence`: explicitly stored confidence can down-rank uncertain information.

This lets applications store memories once and retrieve them with a database-native ranking model instead of layering graph, vector, cache, and custom scoring systems separately.

## Architecture Notes

### Typed Query Surface

UNDR9 uses a typed JSON query interface rather than a string-parsed query language. Requests deserialize into explicit variants such as `GetNodeById`, `Traverse`, `VectorSearch`, and `RankedRetrieval`, which keeps query dispatch predictable and reduces parser ambiguity in application code.

### Storage Layout

Graph indexes use compact adjacency buckets, temporal lookups use timestamp buckets, and vector candidate enumeration is built around a compact node-id set. Properties are stored as typed values instead of reparsed blobs, which keeps retrieval paths more predictable around timestamps, importance, confidence, and vectors.

## Deployment Notes

- Put a reverse proxy such as Caddy or Traefik in front of UNDR9 for TLS termination.
- Use `/healthz` for liveness and `/readyz` for readiness.
- Keep persistent data on a mounted volume at `/var/lib/undr9/data` in Docker.
- Use `backup-storage` and `restore-storage` rather than raw file copies of a live directory.
- Tune `UNDR9_MAINTENANCE_MAX_NODES` and `UNDR9_MAINTENANCE_MAX_EDGES` to match the maintenance window.

## Repository Layout

Top-level areas:

- `crates/` for Rust workspace crates.
- `docs/` for API and operations documentation.
- `scripts/` for benchmark and recovery automation.
- `deployments/docker/` for container deployment notes.
- `.github/workflows/` for CI and container publish workflows.

Important crates:

- `crates/storage` for durable storage and recovery logic.
- `crates/wal` for WAL encoding and replay.
- `crates/query` for query planning and retrieval execution.
- `crates/api` for the Axum HTTP server.
- `crates/observability` for metrics, tracing, and audit surfaces.
- `crates/cli` for operational and benchmarking binaries.

## Developer Workflow

Run the same checks as CI before pushing:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The `undr9-cli` package contains two binaries, so use `--bin undr9-cli` for the main CLI and `--bin undr9-bench` for benchmarks.

## Documentation

- [Developer Guide](./DEVELOPERS.md)
- [Contributing Guide](./CONTRIBUTING.md)
- [HTTP API](./docs/api/http.md)
- [One-Node Operations Guide](./docs/operations/one-node.md)
- [Docker Deployment](./deployments/docker/README.md)

## Status

The repository already includes:

- single-node serving
- storage maintenance and recovery tooling
- benchmark automation and published operational artifacts
- replication and cluster-management command surfaces
- CI and GHCR multi-arch publishing

If you want to adopt or extend the codebase quickly, start with [DEVELOPERS.md](./DEVELOPERS.md).
