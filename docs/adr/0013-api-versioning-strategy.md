# ADR 0013: API Versioning Strategy

## Status

Accepted

## Context

UNDR9 exposes HTTP/JSON as the V1 public contract and already groups core endpoints under `/v1/`. The approved architecture also anticipates:

- a Python SDK
- future gRPC support
- evolving retrieval and replication features

That requires a stable versioning strategy that can support additive growth without forcing constant client rewrites.

## Decision

UNDR9 will use explicit path-based major versioning for public HTTP APIs:

- `/v1/...` for the initial stable public contract

Within a major version:

- additive fields are allowed
- existing fields must preserve meaning
- error envelopes remain structurally stable
- behavior changes that would break a conforming client require a new major version

Internal retrieval profiles and storage formats version independently from the HTTP major version.

## Alternatives Considered

### Header-only versioning

Rejected because path-based versioning is easier to document, route, test, and debug across SDKs and self-hosted operators.

### Date-based API versions

Rejected because the product roadmap is milestone-based and the client ecosystem is still small enough that semantic major versions are clearer.

### Unversioned API with best-effort compatibility

Rejected because it creates too much ambiguity for the Python SDK and future gRPC contracts.

## Consequences

### Positive

- simple and explicit public compatibility boundary
- easy routing and SDK targeting
- clear major-change policy

### Negative

- duplicate routes may accumulate across future major versions
- path-based versioning alone does not solve finer-grained capability negotiation

## Future Evolution

- add capability headers or feature negotiation for optional extensions
- align gRPC service package versions with the same major-version policy
- publish deprecation windows and compatibility guarantees per major API line
