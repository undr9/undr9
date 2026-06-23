# UNDR9 Repository Structure

## Goals

The repository structure must support:

- strong module ownership
- clean dependency boundaries
- independent testing of major subsystems
- storage/query/API separation
- long-term open-source maintainability

This structure follows the requirements document while constraining the implementation to the current repository directive that only the Python SDK is in scope.

## Top-Level Layout

```text
undr9/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── .github/
│   └── workflows/
├── crates/
│   ├── common/
│   ├── config/
│   ├── core/
│   ├── storage/
│   ├── wal/
│   ├── index/
│   ├── query/
│   ├── memory/
│   ├── auth/
│   ├── api/
│   ├── observability/
│   ├── replication/
│   ├── cluster/
│   └── cli/
├── sdk/
│   └── python/
├── docs/
│   ├── adr/
│   ├── architecture/
│   └── api/
├── tests/
│   ├── integration/
│   ├── compatibility/
│   └── fixtures/
├── benches/
├── examples/
├── docker/
├── deployments/
└── scripts/
```

## Crate Responsibilities

## `common`

Shared primitives and utility types that are broadly reusable and domain-neutral.

### Owns

- ids and key wrappers
- error types and error codes
- checksum helpers
- time abstractions
- shared serde models used across internal boundaries

### Must Not Own

- storage policy
- query planning
- transport concerns

## `config`

Configuration schema, defaults, validation, and environment/file loading.

### Owns

- server config
- storage config
- auth config
- WAL and checkpoint config
- observability config

## `core`

Domain model and service interfaces for nodes, edges, properties, memory records, vectors, and transactional write intents.

### Owns

- canonical node/edge/value models
- service traits for repositories and engines
- stable internal contracts between upper and lower layers

### Must Not Own

- filesystem operations
- HTTP DTOs

## `storage`

Persistent file layout, manifests, segment/page management, codecs, data readers/writers, compaction primitives, and repair helpers.

### Owns

- `data/manifest.json`
- node/edge segment formats
- vector segment persistence
- atomic file replacement helpers
- checksummed record encoding

### Dependencies

- may depend on `common`, `config`, and `core`
- must not depend on `api`, `query`, or `auth`

## `wal`

Durable append-only logging, replay, checkpoints, and crash recovery orchestration.

### Owns

- WAL record envelope
- segment rotation
- fsync policy
- replay ordering
- checkpoint metadata

### Dependencies

- may depend on `common`, `config`, and `core`
- may coordinate with `storage`
- must not depend on `api`

## `index`

In-memory and persisted indexes for exact lookup and retrieval acceleration.

### Owns

- node id index
- unique-key index
- adjacency index
- reverse adjacency index
- label/type index
- temporal index
- vector index abstraction

### Dependencies

- may depend on `common`, `config`, `core`, `storage`
- must not depend on `api`

## `query`

Query language, parsing, planning, optimization, and execution orchestration.

### Owns

- request AST
- logical plan
- physical plan
- executor pipeline
- result shaping for internal consumption

### Must Not Own

- authentication
- HTTP request parsing
- file layout details

## `memory`

Higher-level memory semantics, ranking, temporal reasoning, consolidation, and hybrid scoring.

### Owns

- importance scoring
- confidence weighting
- recency weighting
- score breakdown composition

## `auth`

Authentication, authorization, roles, permissions, token or API key validation, and audit policy hooks.

## `api`

Network transport entry points and protocol models.

### Owns

- Axum HTTP routers
- request validation
- response serialization
- error translation
- health/readiness endpoints
- metrics endpoint exposure

### Must Not Own

- storage logic
- ranking logic
- query planning logic

## `observability`

Metrics, tracing, structured logging, and diagnostic helpers.

## `replication`

Replication protocol and state transfer. This crate exists early to hold interfaces, but implementation remains gated until the roadmap reaches distributed features.

## `cluster`

Cluster metadata, membership, and control-plane abstractions. Kept separate from `replication` so data-path replication concerns do not absorb cluster coordination responsibilities.

## `cli`

Administrative CLI for backup, restore, check, repair, vacuum, import/export, and diagnostic operations.

## Dependency Rules

The dependency graph should remain acyclic and roughly layered:

```text
common
  └── config
  └── core
        └── storage
        └── wal
        └── index
              └── query
              └── memory
                    └── auth
                    └── observability
                          └── api
                          └── cli

replication and cluster depend only on lower-layer contracts unless a later ADR explicitly expands that boundary.
```

Additional rules:

- `api` depends inward, never the reverse.
- `storage` and `wal` remain reusable without `api`.
- `query` operates on traits/contracts, not concrete transport models.
- `memory` composes retrieval signals; it does not own persistence.
- `sdk/python` targets the public API only and must not depend on private Rust internals.

## Testing Layout

## Unit Tests

Unit tests live inside each crate for local invariants:

- codecs
- planners
- ranking functions
- auth policy
- WAL replay semantics

## Integration Tests

Cross-crate and system tests live under `tests/integration/`.

Examples:

- startup and shutdown
- crash recovery
- CRUD over HTTP
- traversal queries
- checkpoint and compaction
- backup and restore

## Compatibility Tests

`tests/compatibility/` stores versioned fixtures and migration coverage for storage evolution.

## Benchmark Layout

`benches/` should include:

- node CRUD
- edge CRUD
- adjacency traversal
- bounded path queries
- temporal range retrieval
- vector similarity
- ranked hybrid retrieval

## SDK Structure

Only the Python SDK is in scope for this repository baseline.

```text
sdk/python/
├── pyproject.toml
├── README.md
├── src/undr9/
├── tests/
└── examples/
```

The Python SDK should expose:

- `Client`
- `Node`
- `Edge`
- `Query`
- `Result`
- `Undr9Error`

It should support:

- typed request and response models where practical
- sync and async clients
- retries, timeouts, and connection management

## Documentation Layout

Suggested structure:

```text
docs/
├── adr/
├── architecture/
├── api/
├── operations/
└── onboarding/
```

## Rationale

This layout preserves the requirements document's modular intent while tightening ownership boundaries:

- storage internals stay isolated
- query logic stays transport-agnostic
- memory ranking stays compositional
- distributed features do not destabilize the single-node core

That separation improves testability, reduces circular dependencies, and makes storage-format evolution safer over time.
