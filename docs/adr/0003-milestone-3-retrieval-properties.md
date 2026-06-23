# ADR 0003: Milestone 3 Retrieval Property Conventions

## Status

Accepted

## Context

Milestone 3 requires vector search, temporal search, and ranked retrieval, but the current storage engine does not yet have dedicated persisted vector segments or a separate temporal record format.

We need a stable V1 convention that:

- works with the existing durable node snapshot format
- is easy to test end to end
- does not force a premature redesign of storage

## Decision

For Milestone 3, retrieval signals are stored on nodes using reserved property keys:

- `embedding`: `FloatList`
- `timestamp`: `Integer` epoch milliseconds
- `importance`: `Float` or `Integer`, normalized to `0.0..=1.0`
- `confidence`: `Float` or `Integer`, normalized to `0.0..=1.0`

Vector and temporal indexes are derived from the current in-memory node set after writes and rebuilt from persisted state on startup.

## Consequences

- Milestone 3 can ship vector, temporal, and ranked retrieval without blocking on a larger storage redesign.
- The conventions are explicit for the Python SDK and HTTP API.
- Later milestones can migrate these signals into dedicated vector or temporal storage without changing the high-level retrieval APIs immediately.
- Write amplification remains acceptable for the current milestone, but incremental index maintenance and persisted vector structures are still required later.
