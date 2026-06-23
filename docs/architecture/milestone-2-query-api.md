# Milestone 2 Query And API Architecture

## Scope

Milestone 2 extends the Milestone 1 durable storage engine with:

- primary in-memory indexes
- a query planner and executor
- authenticated HTTP CRUD endpoints
- deterministic JSON error responses

## Index Strategy

The current `GraphIndex` rebuilds from the storage engine's in-memory node and edge state and maintains:

- node id presence
- unique-key lookup via the `unique_key` node property
- adjacency index by source node
- reverse adjacency index by target node
- label/type index by `node_type`

The indexes are rebuilt after each write in the current implementation. This is acceptable for Milestone 2 correctness and testability, but later milestones should move to incremental maintenance and optional persisted index snapshots.

## Query Flow

The HTTP layer delegates all graph query logic to the `query` crate:

1. API authenticates and authorizes the request.
2. API takes a graph snapshot from the storage-backed database wrapper.
3. The query planner selects a plan kind.
4. The executor uses indexes and the snapshot to produce deterministic results.

Implemented query capabilities:

- exact lookup by node id
- exact lookup by unique key
- neighbor listing by relation type and direction
- bounded traversal by hop count
- label/type lookup

## API Layer

The API layer is intentionally thin:

- request parsing
- authn/authz
- transport-specific error shaping
- delegation to storage or query services

It does not own graph business logic, traversal logic, or storage mutation rules.

## Authentication Model

Milestone 2 uses static API keys from configuration:

- admin
- writer
- reader

This satisfies the repository's V1 need for authenticated access while keeping the implementation simple enough to evolve toward more advanced token management later.

## Error Model

All endpoint failures use a deterministic JSON envelope:

```json
{
  "code": "validation_error",
  "message": "path node id does not match request body id",
  "details": []
}
```

The current error codes are:

- `unauthorized`
- `forbidden`
- `validation_error`
- `not_found`
- `lock_error`
- `io_error`
- `serialization_error`
- `corruption`

## Tradeoffs

- **Correctness first**: rebuilding indexes after each write is simpler and less error-prone than incremental mutation during the first queryable milestone.
- **Maintainability**: the API remains thin because query execution lives in its own crate.
- **Scalability cost**: full index rebuilds will not be viable at large graph sizes and must be optimized in later milestones.
- **Operational simplicity**: static API keys are easy to reason about but are not the final auth model.
