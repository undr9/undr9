# UNDR9 Subsystem Specifications

## Purpose

This document converts the product requirements into subsystem-level specifications that are implementable, testable, and compatible with a production-grade Rust codebase.

## 1. Storage Engine

## Goals

- durable on-disk persistence
- inspectable and versioned file layout
- bounded startup memory behavior
- atomic file replacement where possible
- compatibility with WAL replay and future compaction

## On-Disk Layout

```text
data/
├── manifest.json
├── wal/
├── nodes/
├── edges/
├── indexes/
├── vectors/
└── meta/
```

## Core Structures

- `manifest.json`
  - storage format version
  - file inventory
  - checksums
  - active settings snapshot
  - last clean shutdown marker
  - checkpoint metadata
- node segments
  - append-oriented immutable segments plus metadata footer
- edge segments
  - append-oriented immutable segments plus metadata footer
- meta records
  - users, roles, namespace metadata, configuration snapshots

## Design

- Use append-oriented immutable data segments with periodic compaction instead of in-place mutation-heavy files.
- Store records with explicit length, version, checksum, and tombstone state.
- Maintain a manifest that points to the active set of data and index files.
- Use temp-file plus atomic rename for manifest and checkpoint publication.

## Tradeoffs

- Append-oriented segments simplify crash recovery and reduce partial-write risk.
- Compaction becomes a necessary background or maintenance concern.
- Immutable segments increase file-count and metadata management pressure over time, which must be addressed by checkpointing and compaction.

## Performance Implications

- sequential appends improve write predictability
- immutable segments improve read consistency and simplify concurrent readers
- startup remains fast because indexes and segments can be opened lazily or partially mapped rather than fully loaded

## Scalability Implications

- segment-based storage supports tens of millions of records more cleanly than monolithic files
- file growth remains manageable through compaction and manifest-driven active set management

## Test Requirements

- segment codec round-trip tests
- crash simulation with partial WAL and checkpoint boundaries
- checksum corruption detection
- manifest version compatibility tests

## 2. WAL and Recovery

## Goals

- guarantee recovery of committed writes
- preserve write order
- provide replay, checkpoint, and truncation semantics

## WAL Record Envelope

Each WAL entry should include:

- log sequence number
- record type
- transaction or write batch id
- payload length
- payload checksum
- format version

## Design

- WAL segments are append-only files with monotonic sequence ranges.
- Commit acknowledgment occurs only after WAL durability policy is satisfied.
- Recovery replays WAL records in order into storage/index rebuild paths.
- Checkpoints publish a durable storage state and allow older WAL segments to be pruned according to retention policy.

## Tradeoffs

- stricter fsync improves safety but increases write latency
- looser fsync batching improves throughput but increases risk window between acknowledged writes and power loss

## Performance Implications

- sequential WAL writes are favorable for disks and SSDs
- batching reduces syscall overhead
- replay cost grows with uncheckpointed log volume, so checkpoint cadence matters operationally

## Scalability Implications

- WAL sequencing becomes the natural backbone for later replication streams
- explicit segment rotation simplifies archival and recovery tooling

## Test Requirements

- replay ordering tests
- torn-record handling tests
- commit visibility after restart
- checkpoint/pruning tests

## 3. Index Layer

## Required Indexes

- node id index
- unique-key index
- adjacency index by source and relation type
- reverse adjacency index
- label/type index
- temporal index
- vector index

## Design

- Keep canonical data in storage segments; indexes are derived acceleration structures.
- Persist index snapshots where beneficial, but allow full rebuild from base storage plus WAL.
- Separate exact-match indexes from ranking-oriented retrieval structures.

## Tradeoffs

- persisted indexes reduce startup time but add write amplification
- rebuildable indexes simplify corruption recovery but increase restart cost after failures

## Performance Implications

- adjacency indexing is critical to meet traversal latency targets
- unique-key and id indexes must be O(1)-like in hot paths
- temporal and vector indexes should support bounded candidate generation before ranking

## Scalability Implications

- partition-friendly index abstractions help future sharding work
- rebuildability provides operational resilience as datasets grow

## Test Requirements

- index correctness under inserts, updates, and deletes
- index rebuild parity tests
- traversal fan-out and filtering behavior tests

## 4. Transaction Model

## Baseline Interpretation

The requirements document clearly mandates atomic and durable writes in V1 and explicitly calls out stronger transaction support in V2. The baseline interpretation is:

- V1 guarantees record-level atomicity, WAL-backed durability, and consistent visibility for committed operations
- V2 expands this into explicit multi-operation transactions with stronger isolation guarantees

Any deviation from this interpretation should be captured in a dedicated ADR before implementation.

## Initial Design

- start with a single-writer commit pipeline to minimize corruption risk
- permit concurrent readers with snapshot-like visibility based on committed state
- batch related record mutations under one write intent so node, edge, and index updates are committed consistently

## Tradeoffs

- a single-writer model is operationally simple and safe but constrains peak write concurrency
- moving too early to complex concurrency control increases implementation risk before correctness is proven

## Performance Implications

- write serialization limits throughput under heavy concurrent writes
- read-heavy workloads still perform well if indexes and snapshot visibility are efficient

## Scalability Implications

- a clear commit protocol becomes reusable for future MVCC or replicated log designs
- stronger isolation can be layered later if boundaries remain explicit

## Test Requirements

- atomic multi-record write tests
- concurrent read/write visibility tests
- rollback and recovery edge cases once explicit transactions are introduced

## 5. Query Layer

## Required Capabilities

- get node by id or unique key
- list neighbors by edge type and direction
- traverse bounded paths with filters
- search by text fields and labels
- search by vector similarity
- search by time range
- ranked hybrid retrieval with score breakdown

## Design

- Represent requests as a typed AST instead of embedding ad hoc execution logic in handlers.
- Introduce a planner that selects execution strategies based on query shape.
- Keep result ranking as a separate composition phase so scoring remains explainable.

## Tradeoffs

- a minimal custom query language is easier to maintain than Cypher compatibility
- planner abstraction adds complexity up front but prevents endpoint-specific query logic from spreading through the codebase

## Performance Implications

- planning overhead must stay small relative to execution for common lookups
- query paths should favor index-driven candidate generation rather than storage scans

## Scalability Implications

- a typed plan/executor architecture makes it easier to route work across shards or replicas in later versions

## Test Requirements

- parser and planner unit tests
- executor correctness tests
- deterministic response tests with score breakdown validation

## 6. Memory and Retrieval Layer

## Goals

- combine graph structure, semantic similarity, recency, confidence, and importance into ranked retrieval
- keep ranking explainable and deterministic

## Initial Scoring Model

The ranking pipeline should compose:

- structural relevance
- vector similarity
- temporal recency
- importance
- confidence

Responses should expose score breakdowns so clients can reason about ranking output.

## Tradeoffs

- transparent heuristic ranking is easier to debug than opaque learned ranking
- highly configurable scoring can increase operator flexibility but risks inconsistency without sane defaults

## Performance Implications

- ranking should operate on bounded candidate sets produced by indexes and planners
- vector search must not feed unbounded result sets into the ranker

## Scalability Implications

- score composition remains portable to replicas or distributed query execution if component signals are explicit

## Test Requirements

- score component unit tests
- ranking stability tests
- hybrid retrieval benchmarks

## 7. API Layer

## Protocol Baseline

- HTTP/JSON is mandatory in the first implementation phase
- gRPC remains a planned extension once the service contracts are stable

## HTTP API Principles

- deterministic versioned responses
- structured error model with code, message, and actionable details
- thin transport handlers delegating to service interfaces

## Endpoint Groups

- health and readiness
- auth/session or API key management
- node CRUD
- edge CRUD
- query execution
- retrieval endpoints
- admin and maintenance endpoints
- metrics endpoint

## Tradeoffs

- HTTP/JSON maximizes accessibility and simplifies early adoption
- gRPC can improve service-to-service efficiency later but should not destabilize the initial API surface

## Performance Implications

- request validation should be cheap and avoid unnecessary cloning
- pagination and bounded result limits are required to keep latency and payload size predictable

## Scalability Implications

- a stable service contract is required before building replicas, SDK maturity, or streaming features

## Test Requirements

- endpoint contract tests
- auth and authorization tests
- deterministic error response tests

## 8. Security and Access Control

## Requirements

- API key, token, or local admin credential authentication
- role-based authorization
- audit logs for sensitive operations
- request validation and rate limiting
- TLS support for production

## Design

- centralize authorization policy in the `auth` crate
- make sensitive admin operations auditable by default
- keep authn/authz logic out of transport handlers beyond extraction and delegation

## 9. Observability

## Requirements

- health endpoint
- readiness endpoint
- metrics endpoint
- structured logs
- diagnostics suitable for self-hosted production operation

## Design

- standardize tracing spans around request handling, storage operations, WAL commits, and recovery
- expose metrics for latency, errors, WAL replay time, checkpoint duration, compaction duration, and replication lag in later phases

## 10. Replication and Clustering

## Phase Placement

Replication is explicitly a later-phase capability and should not distort the single-node core design.

## Initial Constraints

- keep durable ordering explicit through WAL/log sequence numbers
- keep storage and query subsystems interface-driven so replication can consume stable internal contracts
- separate replication data-plane concerns from cluster membership/control-plane concerns

## Tradeoffs

- deferring distributed complexity protects V1 correctness and maintainability
- early interface planning reduces future rewrite cost

## 11. Python SDK

## Scope

This repository implements only the official Python SDK in accordance with repository-level instructions.

## Design

- package under `sdk/python`
- expose a clean developer-facing API with typed models where practical
- support sync and async usage
- implement retries, timeouts, and connection configuration
- target the public HTTP API and versioned response model

## Test Requirements

- client contract tests against a test server
- serialization/deserialization tests
- retry and timeout behavior tests

## 12. Recommended First Vertical Slice

The first implementation slice should be intentionally narrow but production-real:

1. initialize storage directory and manifest
2. append a node-create record to WAL
3. fsync according to configured durability mode
4. materialize node storage
5. build/update node id index
6. restart the server and replay WAL
7. fetch the node through a documented HTTP endpoint

This slice validates the most critical system properties early:

- storage correctness
- durability
- recovery
- internal layering
- API/service integration
