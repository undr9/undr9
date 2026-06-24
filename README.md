# UNDR9

Memory is becoming the new database layer for AI.
Today's agents can reason. Tomorrow's agents must remember.

UNDR9 is a graph-native memory database built to make memory a first-class primitive.



## 💡 Philosophy & Core Problem

Modern databases were built for applications.

AI agents are not traditional applications.

A web application asks:

- What record matches this ID?
- What rows satisfy this filter?
- What is connected to this node?

An AI agent asks something fundamentally different:

> What memory is most relevant right now?

Current memory architectures are often assembled from multiple independent systems:

```text
Graph Database
      +
Vector Database
      +
Cache
      +
Custom Ranking Logic
      +
Application Glue
```

Relationships live in one place.

Semantic similarity lives somewhere else.

Recency is handled separately.

Importance and confidence are usually implemented as custom application code.

As agents become more autonomous, memory becomes fragmented, difficult to reason about, and increasingly expensive to maintain.

Human memory works differently.

We recall information through a combination of:

- Associations
- Similar experiences
- Importance
- Confidence
- Time

Memories are connected.

Memories evolve.

Memories compete for relevance.

UNDR9 is inspired by these principles.

Instead of treating memory as an afterthought, UNDR9 treats memory as a first-class data primitive.

A memory can be:

- Connected through relationships
- Retrieved semantically
- Ranked by importance
- Weighted by confidence
- Influenced by time

The goal is not to build another graph database.

The goal is to build infrastructure for systems that need to remember.

---

### Multi-Dimensional Cognitive Schema
UNDR9 bypasses raw, unweighted graph nodes by baking multi-dimensional cognitive primitives directly into the database core:
* **`graph` Topology:** Ranked retrieval can start from a reference node and score candidates by graph distance, so nearby nodes in the actual relationship graph are favored before broad semantic matches pull in unrelated context.
* **`timestamp` (Temporal Decay):** Temporal recency is scored directly from the stored `timestamp` field using a bounded decay curve with a seven-day half-life style envelope, letting recent memories stay prominent while old context naturally fades instead of dominating forever.
* **`importance`:** Importance is normalized into the final retrieval score so critical concepts remain retrievable even when they are not the newest items in the graph.
* **`confidence`:** Confidence is also normalized into ranking, giving the engine a way to down-weight uncertain or weakly trusted memories before they reach the agent loop.

---

## 🛠️ Architectural Choices & Advantages

### Type-Safe JSON Variant RPC Interface
UNDR9 deliberately abandons traditional string-parsed query languages (like SQL or Cypher). 
* **Single Typed Dispatch Path:** Requests deserialize into a `QueryRequest` enum with explicit variants such as `GetNodeById`, `Traverse`, `VectorSearch`, and `RankedRetrieval`, so execution dispatch becomes a direct match on typed variants instead of a second query-language parse and plan phase.
* **Compile-Time Validation:** On the Rust side, query shapes map cleanly to native enums and structs, which eliminates a large class of string-concatenation mistakes, parser ambiguities, and injection-style bugs that show up in text-driven query assembly.

### High-Density Rust Memory Layout
* **Contiguous Storage & Cache Locality:** Graph indexes keep adjacency and reverse-adjacency lists as `Vec<EdgeId>` buckets, temporal lookups as timestamp buckets, and vector candidates as a compact node-id list, which keeps hot traversal and candidate-enumeration paths close to linear memory access patterns.
* **Typed Property Encoding:** Instead of reparsing untyped blobs on every request, node properties are stored as explicit variants such as `String`, `Integer`, `Float`, and `FloatList`, which keeps query execution predictable and reduces incidental runtime type churn around embeddings, timestamps, importance, and confidence.

---

## 📈 Checked-In Performance Evidence

The following data represents empirical, automated testing evidence verified on an `Apple Silicon (macos/aarch64)` host machine under a `compact` workload profile.

### Scale & Footprint Comparison
| Scale Tier | Node Count | Edge Count | Peak RSS (RAM) | On-Disk Footprint | Post-Compaction Size | Recovery Open Time |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **100k** | `100,000` | `99,999` | `1.00 GB` | `40.46 MB` | `35.16 MB` | `1.28 s` |
| **1M** | `1,000,000` | `999,999` | `2.70 GB` | `278.54 MB` | `245.47 MB` | `9.41 s` |
| **10M** | `10,000,000` | `9,999,999` | `7.54 GB` | `2.85 GB` | `2.50 GB` | `104.32 s` |
| **100M** | `Not yet published` | `Not yet published` | `Not yet published` | `Not yet published` | `Not yet published` | `Not yet published` |

### Latency Profiles by Scenario
| Scenario Name | 100k Scale Latency | 1M Scale Latency | 10M Scale Latency | Primary Performance Driver |
| :--- | :--- | :--- | :--- | :--- |
| **`storage_upsert`** | `4.07 s` (single batch) | `33.15 s` (single batch) | `363.42 s` (single batch) | WAL append volume, serialization, and fsync-heavy write throughput |
| **`storage_delete`** | `5.01 s` | `43.18 s` | `513.38 s` | Tombstone-heavy mutation volume plus WAL rewrite cost |
| **`wal_recovery`** | `8.72 s` | `67.10 s` | `730.44 s` | WAL replay loop, manifest validation, and full state rebuild on open |
| **`exact_lookup`** | `56 us` | `Not yet published` | `Not yet published` | Direct id or unique-key index lookup with minimal plan overhead |
| **`list_neighbors_1_hop`**| `56 us` | `Not yet published` | `Not yet published` | Adjacency index bucket scan for a single hop |
| **`traverse_5_hops`** | `346 us` | `Not yet published` | `Not yet published` | Bounded BFS over adjacency and reverse-adjacency indexes |
| **`shortest_path`** | `1,399 us` | `Not yet published` | `Not yet published` | Frontier expansion and visited-set maintenance across graph hops |
| **`temporal_range`** | `3,684 us` | `Not yet published` | `Not yet published` | Timestamp-bucket scan through the temporal index |
| **`vector_search_exact`** | `390,797 us` | `Not yet published` | `Not yet published` | Exact cosine scoring across all eligible vector candidates |
| **`vector_search_hnsw`** | `24,502 us` | `Not yet published` | `Not yet published` | HNSW ANN candidate generation with final vector scoring over the returned set |
| **`ranked_retrieval_exact`** | `410,973 us` | `Not yet published` | `Not yet published` | Full hybrid retrieval with exact semantic candidate generation |
| **`ranked_retrieval_hnsw`** | `2,070 us` | `Not yet published` | `Not yet published` | HNSW semantic prefilter unioned with structural candidates before hybrid reranking |

At `100k`, the published HNSW benchmark artifact records benchmark tuning of `semantic_top_k=250` and `ef_search=128`, plus a persisted vector-index sidecar footprint of `5,892,367` bytes, with `207.55 s` build time and `140.47 s` warm-load time in the current benchmark environment. Larger-scale HNSW latency and footprint evidence is not yet published.

At the same `100k` scale, the current quality artifact shows `vector_search` exact-vs-HNSW top-50 overlap of `1.0` with `top1_match=true`, while `ranked_retrieval` shows top-50 overlap of `0.56` with `top1_match=true`. These are workload-specific measurements from the published benchmark artifact, not a general recall guarantee.

---


## Why UNDR9

UNDR9 is built for developers who want:

- a self-hosted graph database with straightforward operational workflows
- explicit health and readiness behavior for real deployments
- storage verification, compaction, backup, restore, and PITR-oriented tooling
- observability through metrics, logs, and OTLP tracing
- a workspace layout that is easy to develop and extend in Rust

## Quick Start

### Prerequisites

- Rust `1.81` or newer
- Cargo
- Docker Desktop or Docker Engine
- Git
- `curl` for quick API checks

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

Runtime vector index behavior:

- HNSW is the default runtime vector backend
- switch back to exact search with `export UNDR9_VECTOR_INDEX_BACKEND=exact`
- keep HNSW but tune global defaults with `UNDR9_VECTOR_INDEX_SEMANTIC_TOP_K` and `UNDR9_HNSW_EF_SEARCH`
- per-query `top_k` can override the semantic candidate budget for `VectorSearch` and `RankedRetrieval`

### Verify

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

### Create A Node

```bash
curl -X POST http://127.0.0.1:8080/v1/nodes \
  -H 'content-type: application/json' \
  -H "x-api-key: ${UNDR9_WRITER_API_KEY}" \
  -d '{
    "id": "node_a",
    "node_type": "memory",
    "properties": {
      "unique_key": { "kind": "String", "value": "alpha" }
    }
  }'
```

### Query With A `top_k` Override

`limit` still controls how many ranked results are returned. `top_k` overrides the semantic
candidate pool size for this request when the HNSW backend is active.

```bash
curl -X POST http://127.0.0.1:8080/v1/query \
  -H 'content-type: application/json' \
  -H "x-api-key: ${UNDR9_READER_API_KEY}" \
  -d '{
    "VectorSearch": {
      "query_vector": [1.0, 0.0],
      "node_type": "memory",
      "limit": 10,
      "top_k": 50
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

UNDR9 publishes to:

```text
ghcr.io/undr9/undr9
```

The published image is multi-arch for:

- `linux/amd64`
- `linux/arm64`

That supports:

- Linux servers on Intel or ARM64
- Apple Silicon development with Docker Desktop
- Windows development through Linux containers in Docker Desktop

Pull the latest image:

```bash
docker pull ghcr.io/undr9/undr9:latest
```

## Core Capabilities

- **Durable storage**: snapshots, deltas, WAL replay, verification, repair, and compaction
- **HTTP API**: node CRUD, edge CRUD, queries, health, readiness, metrics, and admin endpoints
- **Query support**: exact lookup, unique-key lookup, traversal, time-range search, vector search, and ranked retrieval
- **Operational tooling**: backup, restore, PITR-oriented restore, recovery drill, and maintenance status reporting
- **Observability**: Prometheus metrics, structured logs, trace propagation, and OTLP export
- **Container delivery**: hardened Docker image and GHCR publishing workflow

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

Supported query families include:

- exact node lookup
- unique-key lookup
- neighbor listing
- bounded traversal
- label search
- time-range search
- vector search
- ranked retrieval

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

- `/metrics` exposes Prometheus-style metrics
- `/readyz` flips unavailable during drain or graceful shutdown
- responses include `x-undr9-trace-id`
- OTLP export can be enabled when running behind a collector

## Repository Layout

Top-level areas:

- `crates/`: Rust workspace crates
- `docs/`: API and operations documentation
- `scripts/`: benchmark and recovery automation
- `deployments/docker/`: container deployment notes
- `.github/workflows/`: CI and container publish workflows

Important crates:

- `crates/storage`: durable storage and recovery logic
- `crates/wal`: WAL encoding and replay
- `crates/query`: query planning and retrieval execution
- `crates/api`: Axum HTTP server
- `crates/observability`: metrics, tracing, and audit surfaces
- `crates/cli`: operational and benchmarking binaries

## Developer Workflow

Run the same checks as CI before you push:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Important note:

- the `undr9-cli` package contains two binaries, so use `--bin undr9-cli` for the main CLI and `--bin undr9-bench` for benchmarks

## Documentation

- [Developer Guide](./DEVELOPERS.md)
- [Contributing Guide](./CONTRIBUTING.md)
- [HTTP API](./docs/api/http.md)
- [One-Node Operations Guide](./docs/operations/one-node.md)
- [Docker Deployment](./deployments/docker/README.md)

## Deployment Notes

- Put a reverse proxy such as Caddy or Traefik in front of UNDR9 for TLS termination.
- Use `/healthz` for liveness and `/readyz` for readiness.
- Keep persistent data on a mounted volume at `/var/lib/undr9/data` in Docker.
- Use `backup-storage` and `restore-storage` rather than raw file copies of a live directory.
- Tune `UNDR9_MAINTENANCE_MAX_NODES` and `UNDR9_MAINTENANCE_MAX_EDGES` to match your maintenance window.

## Status

UNDR9 already includes:

- single-node serving
- storage maintenance and recovery tooling
- benchmark automation and published operational artifacts
- replication and cluster-management command surfaces
- CI plus GHCR multi-arch publishing

If you want to contribute or adopt the codebase quickly, start with
[DEVELOPERS.md](./DEVELOPERS.md).
