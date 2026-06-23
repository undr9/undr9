# ADR 0014: Property Type System

## Status

Accepted

## Context

UNDR9 stores canonical graph and memory content on nodes and edges through a property map. The retrieval engine already depends on typed access to reserved fields like embeddings, timestamps, importance, and confidence.

The property type system must therefore remain:

- explicit
- serializable across HTTP, storage, and SDK layers
- small enough to keep V1 manageable
- extensible for future richer types

## Decision

UNDR9 V1 properties use a tagged scalar and list type system with the following canonical kinds:

- `String`
- `Integer`
- `Float`
- `Boolean`
- `StringList`
- `FloatList`

The property model is closed in V1. Arbitrary nested JSON objects are not first-class property values.

The HTTP API and SDK must expose property kinds explicitly so type interpretation never depends on client-specific heuristics.

## Alternatives Considered

### Untyped JSON values

Rejected because retrieval, indexing, and SDK consistency all benefit from explicit typing.

### Rich nested document types in V1

Rejected because that would move UNDR9 toward document-database scope and complicate indexing and storage semantics.

### Fully schema-bound properties

Rejected because UNDR9 is intended to remain flexible at the record level, with optional indexing and retrieval conventions layered above.

## Consequences

### Positive

- deterministic cross-language behavior
- easy property validation and retrieval signal extraction
- limited and testable storage surface

### Negative

- no first-class nested object support in V1
- some application payloads will need flattening before ingest

## Future Evolution

- add `Bytes`, `IntegerList`, and structured interval/provenance types if justified by workload needs
- support optional schema declarations without making them mandatory
- evolve toward user-defined typed secondary indexes in later milestones
