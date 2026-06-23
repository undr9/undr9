# ADR 0011: Memory Metadata Namespace

## Status

Accepted

## Context

UNDR9 models memory as graph nodes plus reserved metadata rather than introducing a separate persisted memory primitive in V1. That requires a clear namespace policy for fields that the retrieval engine interprets semantically.

Without an explicit namespace, applications risk accidental collisions between ordinary business properties and engine-reserved memory metadata.

## Decision

UNDR9 V1 will reserve a canonical memory metadata namespace on node properties using the following keys:

- `embedding`
- `timestamp`
- `importance`
- `confidence`

These keys are engine-reserved for retrieval and memory semantics. Applications may store additional domain properties freely, but they must not repurpose the reserved keys with incompatible types.

The reserved keys use the following canonical meanings:

- `embedding`: `FloatList`
- `timestamp`: UTC epoch milliseconds as `Integer`
- `importance`: normalized `Float` in `[0,1]`
- `confidence`: normalized `Float` in `[0,1]`

## Alternatives Considered

### Separate nested object such as `memory.*`

Rejected for V1 because the current property model and retrieval APIs already rely on flat reserved keys and changing that now would create unnecessary migration complexity.

### Separate memory table or file family

Rejected because the canonical product definition keeps `Memory = Node + Metadata` in V1.

### No reserved namespace

Rejected because retrieval correctness would become ambiguous and harder to validate.

## Consequences

### Positive

- simple and explicit retrieval contract
- easy SDK and API documentation
- deterministic mapping from stored properties to retrieval signals

### Negative

- reserves a few common property names globally
- future migration to richer nested metadata may require adapters

## Future Evolution

- add a formal namespaced metadata view such as `memory.embedding` while preserving backward compatibility for the current keys
- extend the namespace with interval, provenance, or consolidation metadata if later milestones require them
- validate reserved property usage more strictly at write time
