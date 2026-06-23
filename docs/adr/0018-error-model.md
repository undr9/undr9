# ADR 0018: Error Model

## Status

Accepted

## Context

UNDR9 exposes errors across several boundaries:

- internal Rust subsystem APIs
- HTTP/JSON responses
- operator CLI commands
- future SDKs

The system already distinguishes validation, conflict, IO, serialization, not-found, and corruption concerns. The remaining architectural decision is how to formalize that into a stable model that remains understandable to operators and SDK consumers.

## Decision

UNDR9 will use a typed internal error taxonomy with stable public HTTP error envelopes.

The internal canonical categories are:

- `Validation`
- `Conflict`
- `Io`
- `Serialization`
- `NotFound`
- `Corruption`

The public HTTP error envelope remains:

- `code`
- `message`
- `details`

Error translation happens at the API boundary. Storage, WAL, query, and replication internals do not own HTTP semantics directly.

Conflict-style operational failures such as follower write rejection or stale transaction commit attempts are first-class `Conflict` errors, not generic validation failures.

## Alternatives Considered

### String-only errors

Rejected because they are hard to test, classify, and translate consistently across SDKs.

### HTTP status codes as the only error model

Rejected because internal crates must not depend on transport semantics, and status codes alone are too coarse for durable system behavior.

### Exception-style domain hierarchies per subsystem with no shared taxonomy

Rejected because operators and SDKs benefit from a unified cross-system classification.

## Consequences

### Positive

- predictable operator and client behavior
- easier error translation in the Python SDK
- stable audit and observability categorization
- clean separation between domain errors and transport representation

### Negative

- the taxonomy must be curated as new subsystem concerns appear
- some lower-level errors will still need contextual wrapping to remain actionable

## Future Evolution

- add machine-readable subcodes for more granular automation
- introduce retryability hints and remediation metadata where useful
- align future gRPC status mapping with the same canonical internal error categories
