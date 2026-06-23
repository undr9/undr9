# UNDR9 Implementation Roadmap

## Purpose

This roadmap translates the UNDR9 requirements document into an implementation sequence that preserves the intended architecture:

- single-node server first
- durable storage before higher-level query features
- modular Rust workspace with clear crate boundaries
- production readiness through testing, observability, and deployment artifacts

The requirements document remains the source of truth. This roadmap exists to stage delivery, identify dependencies, and reduce architectural drift.

## Guiding Constraints

- Language: Rust on stable toolchain
- Runtime: Tokio
- HTTP transport: Axum
- Serialization: Serde
- Product model: self-hosted networked server
- Initial deployment: single-node first
- Durability: WAL-backed recovery is mandatory before the server is considered production-ready
- Repository shape: multi-crate Rust workspace with isolated subsystems
- SDK scope for this repository baseline: Python SDK only, documented in ADR `0001`

## Delivery Principles

- Implement each major subsystem behind a clean crate boundary.
- Prefer a narrow, testable vertical slice over broad but incomplete scaffolding.
- Make on-disk formats explicit, versioned, and checksum-protected from the beginning.
- Keep transport handlers thin and free of business logic.
- Require documentation and tests for every public API.
- Defer distributed behavior until single-node correctness, recoverability, and observability are proven.

## Milestone Plan

## Milestone 0: Foundation and Architecture Baseline

### Objectives

- Establish the Rust workspace and crate dependency hierarchy.
- Write architecture, repository, and subsystem specifications.
- Define the storage layout, manifest format, and WAL record envelope.
- Set up formatting, linting, CI, and workspace-level developer tooling.

### Deliverables

- Cargo workspace
- workspace-level `rustfmt` and `clippy` configuration
- CI pipeline for build, test, lint, docs
- docs for roadmap, repository structure, subsystem specs, and ADRs
- configuration model for server, storage, auth, logging, and performance

### Exit Criteria

- `cargo check`, `cargo test`, and `cargo clippy` run cleanly across the workspace
- architecture documentation exists and is internally consistent

## Milestone 1: Storage Core and Durable Single-Node CRUD

### Objectives

- Implement manifest management and storage directory bootstrap.
- Implement WAL segment writer, fsync policy, checksums, replay, and checkpoint markers.
- Implement persistent node and edge storage segments.
- Implement basic CRUD for nodes and edges.
- Implement startup recovery and graceful shutdown flushing.

### Deliverables

- storage engine with versioned segment files
- WAL append/replay/checkpoint pipeline
- node and edge codecs
- crash recovery integration tests
- CLI/admin hooks for storage inspection

### Performance Focus

- optimize for predictable local reads and safe writes
- avoid loading full datasets into memory at startup
- keep WAL append path simple and sequential

### Exit Criteria

- fresh database starts and persists data
- restart after crash replays committed writes correctly
- node and edge CRUD are covered by unit and integration tests

## Milestone 2: Core Indexes, Query Execution, and HTTP API

### Objectives

- Implement primary indexes:
  - node id index
  - unique-key index
  - adjacency index
  - reverse adjacency index
  - label/type index
- Implement query planner/executor for:
  - exact lookup
  - neighbor listing
  - bounded traversal
- Expose Axum HTTP API.
- Add authentication, authorization, and deterministic error responses.

### Deliverables

- query crate with parser, planner, and executor
- index persistence and rebuild flows
- authenticated CRUD/query HTTP endpoints
- health and readiness endpoints
- API docs and contract tests

### Performance Focus

- keep traversal execution index-first
- bound fan-out and hop expansion costs
- keep per-request allocations controlled

### Exit Criteria

- documented API supports the minimum query capabilities from the requirements
- common traversals meet initial latency targets on representative local data

## Milestone 3: Retrieval Expansion

### Objectives

- Implement temporal indexing and time-range retrieval.
- Implement vector storage and similarity search.
- Implement ranked hybrid retrieval that merges graph, vector, temporal, confidence, and importance signals.
- Introduce the memory layer for ranking and consolidation logic.

### Deliverables

- temporal index
- vector segment layout and vector index abstraction
- ranking engine with score breakdowns
- retrieval endpoints and query operators
- benchmark suite for hybrid retrieval workloads

### Tradeoffs

- choose a vector index implementation that preserves maintainability and operational simplicity over maximal novelty
- begin with deterministic, explainable ranking signals before advanced learned ranking

### Exit Criteria

- ranked retrieval returns stable score breakdowns
- similarity and time-range queries are documented and benchmarked

## Milestone 4: Operational Hardening

### Objectives

- Add compaction, backup, restore, repair, and index rebuild tooling.
- Add observability:
  - metrics
  - structured logs
  - audit events
- Add Docker image, `docker-compose`, and deployment examples.
- Add benchmark automation and compatibility tests for storage evolution.

### Deliverables

- maintenance jobs and admin endpoints/CLI commands
- metrics endpoint
- backup/restore workflow
- deployment artifacts
- onboarding and operations documentation

### Exit Criteria

- a documented one-node deployment can be launched locally and in Docker
- storage maintenance operations are testable and documented

## Milestone 5: Transaction Model Expansion

### Objectives

- Extend from record-level atomicity to explicit multi-operation transactions if not already fully implemented.
- Define isolation guarantees, conflict detection, and rollback semantics.
- Preserve WAL and recovery compatibility.

### Deliverables

- transaction manager
- lock or MVCC strategy as documented in ADR/spec
- transaction API and tests

### Exit Criteria

- transaction guarantees are explicit, documented, and validated under concurrent tests

## Milestone 6: Replication and Clustering

### Objectives

- Implement leader-based replication.
- Add read replicas, failover, and recovery flows.
- Define cluster membership, metadata, and rebalancing strategy.

### Deliverables

- replication log shipping or consensus-backed replication layer
- cluster control-plane design
- operational procedures for failover and node replacement

### Scalability Focus

- keep single-node data structures reusable
- avoid introducing distributed coupling into storage internals until interfaces are stable

### Exit Criteria

- replica lag, failover behavior, and recovery flows are measurable and tested

## Cross-Cutting Streams

## Testing

- unit tests for codecs, indexes, query logic, WAL, ranking, and auth
- integration tests for startup, crash recovery, API behavior, compaction, and backup/restore
- benchmark suite for CRUD, traversal, temporal, vector, and hybrid retrieval
- compatibility tests for storage format upgrades

## Documentation

- architecture docs
- API docs
- developer onboarding
- deployment and operations guides
- changelog and contribution guidance

## Security

- API key/token authentication
- role-based authorization
- audit logs for sensitive operations
- input validation and request limits
- TLS support in production deployments

## Recommended Immediate Next Steps

1. Create the Rust workspace and crate skeleton defined in the repository structure document.
2. Lock the storage manifest, WAL envelope, and segment format interfaces before implementing CRUD.
3. Implement a minimal vertical slice: startup -> manifest -> WAL append -> node write -> recovery -> node read.
4. Add API and query layers only after the storage core and crash recovery tests are stable.
