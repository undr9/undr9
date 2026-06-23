# Milestone 3 Retrieval And SDK Architecture

## Scope

Milestone 3 extends the Milestone 2 graph service with:

- vector similarity search
- temporal range search
- ranked retrieval with score breakdowns
- the official Python SDK

## Retrieval Signals

The current milestone standardizes the following node property keys:

- `embedding`: vector embedding stored as `FloatList`
- `timestamp`: event or memory timestamp as epoch milliseconds
- `importance`: normalized importance signal
- `confidence`: normalized confidence signal

These conventions are formalized in `docs/adr/0003-milestone-3-retrieval-properties.md`.

## Retrieval Planning

The planner now distinguishes:

- `TemporalRange`
- `VectorSimilarity`
- `RankedHybrid`

These plan kinds are separate from the Milestone 2 lookup and traversal plans so later execution engines can optimize them independently.

## Retrieval Execution

### Vector Search

- uses nodes with `embedding`
- computes cosine similarity against the query vector
- returns ranked results with semantic score breakdowns

### Temporal Search

- uses the temporal index for the reserved `timestamp` field
- supports bounded time windows
- returns deterministic node sets

### Ranked Retrieval

Combines:

- structural relevance from graph distance to a reference node
- semantic relevance from vector similarity
- temporal freshness from recency
- importance
- confidence

The weighted combination uses the existing ranking weights from the `memory` crate and returns per-result score breakdowns.

## Index Strategy

Milestone 3 adds two derived index dimensions:

- temporal index by timestamp
- vector candidate list for nodes with embeddings

These are still rebuilt from in-memory state after writes. This is correct and easy to reason about, but not the final scaling strategy.

## Python SDK

The official SDK lives under `sdk/python/` and provides:

- sync client
- async client
- typed models for nodes, edges, property values, and ranked query results

The SDK intentionally mirrors the HTTP API contract closely to keep client behavior predictable and easy to maintain while the server is still evolving.
