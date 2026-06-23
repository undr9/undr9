# ADR 0006: Serialization Format

## Status

Accepted

## Context

UNDR9 needs serialization decisions at multiple layers:

- HTTP and SDK contracts
- durable on-disk records
- WAL payloads
- snapshots and operator-visible metadata

The canonical decision set prioritizes inspectability, maintainability, and correctness over aggressive micro-optimization in V1. The repository also already centers Serde across internal and external boundaries.

## Decision

UNDR9 will standardize on a layered serialization strategy:

- `JSON` for HTTP APIs, snapshots, manifests, and operator-visible metadata
- `Serde-backed structured payloads` for internal Rust models
- `Versioned framed payloads` for durable records and WAL entries, with JSON payload encoding in V1

This means V1 storage and WAL payloads remain human-inspectable through JSON, while the outer envelope provides length framing and checksums.

The property model, error model, and API responses all remain explicitly versioned at the schema level rather than inferred from ad hoc field presence.

## Alternatives Considered

### Pure binary everywhere

Rejected for V1 because it reduces inspectability and slows operator debugging, repair tooling, and architecture iteration.

### Protobuf everywhere

Rejected because HTTP/JSON is the V1 contract, and introducing Protobuf at every layer would add complexity before gRPC is a first-class product requirement.

### Ad hoc hand-rolled text formats

Rejected because they provide neither the schema discipline of Serde models nor the interoperability of standard JSON tooling.

## Consequences

### Positive

- keeps API, SDK, storage tooling, and docs aligned on one understandable shape
- simplifies debugging and corruption investigation
- reduces implementation complexity in early milestones
- works naturally with Axum and Serde in Rust and the Python SDK

### Negative

- JSON payloads are larger and slower than compact binary encodings
- durable storage costs more bytes per record in V1
- large snapshots and WAL scans will be less efficient than later binary encodings

## Future Evolution

- preserve the outer record and WAL framing while swapping payload encoding from JSON to a compact binary format if performance warrants it
- add Protobuf or FlatBuffers for gRPC and internal replication streaming
- support dual-read migration periods where old JSON payloads and new binary payloads coexist under explicit `format_version` handling
