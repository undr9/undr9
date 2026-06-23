# UNDR9

UNDR9 is an open-source, self-hosted graph-memory database written in Rust. The repository is organized as a multi-crate workspace so storage, WAL, indexing, query planning, retrieval, auth, API transport, and operations can evolve independently without collapsing module boundaries.

## Milestone 0 Status

Milestone 0 establishes the production-facing foundation:

- Rust workspace with explicit crate boundaries
- shared configuration, error, and domain primitives
- storage bootstrap and manifest initialization
- WAL, index, query, ranking, auth, observability, replication, and cluster contracts
- Axum health, readiness, and metrics router
- CLI for inspecting default config and storage layout
- formatting, linting, and CI

## Milestone 1 Status

Milestone 1 adds the first durable single-node engine:

- WAL append and replay
- node and edge snapshot persistence
- single-node CRUD engine with graceful shutdown
- crash recovery tests
- CLI hooks for storage bootstrap and manifest inspection

## Milestone 2 Status

Milestone 2 adds the first queryable and remotely accessible service layer:

- primary in-memory indexes
- query planner and executor
- authenticated Axum CRUD endpoints
- deterministic JSON error responses
- API contract tests and query execution tests

## Milestone 3 Status

Milestone 3 adds retrieval and client access:

- vector similarity search
- temporal range search
- ranked retrieval with score breakdowns
- official Python SDK with sync and async clients
- Rust retrieval tests and Python SDK tests

## Milestone 4 Status

Milestone 4 hardens operations around the single-node service:

- compaction, backup, restore, repair, integrity verification, and index rebuild tooling
- richer Prometheus metrics
- structured logs and audit events
- Docker and `docker-compose` deployment artifacts
- benchmark and compatibility automation

## Milestone 5 Status

Milestone 5 adds explicit transaction sessions:

- snapshot-style transaction reads
- staged node and edge operations
- optimistic write-write conflict detection
- commit and rollback endpoints
- concurrent transaction tests

## Workspace Layout

- `crates/common`: shared ids, checksums, and error types
- `crates/config`: application configuration schema and validation
- `crates/core`: domain records for nodes, edges, properties, and write batches
- `crates/storage`: storage bootstrap, directory layout, and manifest persistence
- `crates/wal`: WAL metadata and durability configuration contracts
- `crates/index`: index catalog definitions
- `crates/query`: query AST and planning contracts
- `crates/memory`: score breakdown and ranking composition
- `crates/auth`: role and authorization policy
- `crates/observability`: metrics snapshot rendering
- `crates/api`: Axum router with health/readiness/metrics endpoints
- `crates/replication`: replication status contracts
- `crates/cluster`: cluster topology contracts
- `crates/cli`: developer CLI

## Getting Started

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Useful Commands

```bash
cargo run -p undr9-cli -- show-default-config
cargo run -p undr9-cli -- print-layout --root ./data
cargo run -p undr9-cli -- bootstrap-storage --root ./data
cargo run -p undr9-cli -- show-manifest --root ./data
cargo run -p undr9-cli -- inspect-storage --root ./data
cargo run -p undr9-cli -- serve --root ./data --bind 127.0.0.1:8080
cargo run -p undr9-cli -- verify-storage --root ./data
cargo run -p undr9-cli -- compact-storage --root ./data
cargo run -p undr9-cli -- rebuild-indexes --root ./data
cargo run -p undr9-cli -- backup-storage --root ./data --destination ./backup
cargo run -p undr9-cli -- restore-storage --root ./data --source ./backup
cargo run -p undr9-cli -- repair-storage --root ./data
cargo run -p undr9-cli -- run-transaction --root ./data --plan ./transaction-plan.json
./scripts/run-benchmarks.sh
./scripts/run-compatibility.sh
```

## Documentation

- `docs/implementation-roadmap.md`
- `docs/repository-structure.md`
- `docs/subsystem-specifications.md`
- `docs/architecture/milestone-1-storage.md`
- `docs/architecture/milestone-2-query-api.md`
- `docs/architecture/milestone-3-retrieval-sdk.md`
- `docs/architecture/milestone-4-operations.md`
- `docs/architecture/milestone-5-transactions.md`
- `docs/api/http.md`
- `docs/operations/one-node.md`
- `docs/adr/`
