# ADR 0016: Retrieval Profile Versioning

## Status

Accepted

## Context

UNDR9 retrieval combines multiple signals and explicit ranking weights. The canonical decision set fixes the initial score formula, but retrieval behavior will evolve over time through:

- new signals
- new default weights
- alternate ranking profiles
- future ANN backends or temporal policies

That requires a versioning strategy separate from HTTP API versioning so retrieval behavior can evolve explicitly and reproducibly.

## Decision

UNDR9 will version retrieval behavior through explicit retrieval profiles.

V1 ships with a default retrieval profile identified logically as:

- `retrieval_profile = v1-default`

The V1 default formula is:

`score = 0.30 * structural + 0.30 * semantic + 0.15 * temporal + 0.15 * importance + 0.10 * confidence`

Retrieval profiles version:

- signal set
- signal normalization rules
- ranking weights
- tie-breaking rules where applicable

HTTP API version and retrieval profile version are independent. A stable `/v1/query` endpoint may support more than one retrieval profile over time if explicitly requested.

## Alternatives Considered

### No retrieval profile versioning

Rejected because silent ranking changes would make results harder to trust and benchmark.

### Bind retrieval behavior directly to API major version

Rejected because retrieval tuning should evolve more granularly than the whole public API surface.

### User-supplied arbitrary weights only

Rejected for V1 because it would weaken determinism and make benchmarking and debugging less comparable.

## Consequences

### Positive

- reproducible ranking behavior
- benchmark and regression testing can target named profiles
- easier future experimentation without silent contract drift

### Negative

- adds another versioned dimension to product documentation
- requires policy for default-profile selection and deprecation

## Future Evolution

- allow clients to request alternate named profiles
- support tenant- or workload-specific profile overrides in later editions
- persist retrieval profile identifiers in consolidation or ranking audit records
