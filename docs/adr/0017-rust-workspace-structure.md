# ADR 0017: Rust Workspace Structure

## Status

Accepted

## Context

UNDR9 already uses a multi-crate Rust workspace and the approved architecture explicitly requires strong separation among storage, WAL, indexes, query planning, memory semantics, auth, observability, replication, cluster control, API, and CLI surfaces.

The remaining ADR need is to freeze that workspace structure as an intentional architectural decision rather than an incidental repo layout.

## Decision

UNDR9 will remain a layered Rust workspace with the following first-class crates:

- `common`
- `config`
- `core`
- `storage`
- `wal`
- `index`
- `query`
- `memory`
- `auth`
- `observability`
- `replication`
- `cluster`
- `api`
- `cli`

The dependency model stays inward-facing:

- lower crates do not depend on transport crates
- distributed crates depend on stable lower-layer contracts, not vice versa
- the Python SDK remains outside Rust crate dependency graphs

This workspace structure is part of the architecture, not merely a development convenience.

## Alternatives Considered

### Monolithic application crate

Rejected because it would blur subsystem boundaries, reduce testability, and make storage-format evolution riskier.

### Many tiny micro-crates beyond the current structure

Rejected because it would increase maintenance and dependency overhead without proportionate architectural benefit in V1.

### Put replication and cluster inside storage

Rejected because the roadmap explicitly wants distributed concerns separated from the single-node core.

## Consequences

### Positive

- preserves clean ownership boundaries
- improves independent testing and reasoning
- reduces circular dependency risk
- keeps distributed features from destabilizing storage internals

### Negative

- cross-crate coordination requires discipline
- some refactors involve multiple crates and docs updates

## Future Evolution

- add specialized crates only when there is a stable architectural boundary, such as a future `grpc` or `consolidation` crate
- keep crate explosion under control by preferring modules inside existing crates unless the boundary is product-significant
