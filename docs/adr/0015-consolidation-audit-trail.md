# ADR 0015: Consolidation Audit Trail

## Status

Accepted

## Context

The canonical decision set defines consolidation as a deterministic background process that may merge, relink, strengthen, or deprioritize memories. Because consolidation changes memory state semantically, operators need an auditable trail explaining:

- what consolidation ran
- which memories were examined
- what changes were applied
- whether the run was deterministic and reproducible

This is especially important because UNDR9 is an AI-memory product rather than a generic graph store.

## Decision

UNDR9 will record consolidation audit events as append-only structured entries under the control metadata area, initially through the audit log in `data/meta/`.

Each consolidation audit event must capture at least:

- consolidation run identifier
- rule or profile version used
- timestamp
- affected node ids
- action type such as merge, link, importance update, confidence update, or no-op
- before/after summaries sufficient for operator reasoning

The audit trail is mandatory when consolidation is enabled.

## Alternatives Considered

### No consolidation audit trail

Rejected because consolidation changes are materially important to trust, debugging, and operator review.

### Store audit only in external observability systems

Rejected because local durability and self-hosted operability require first-party audit records.

### Reuse generic write audit entries only

Rejected because generic write logs do not capture consolidation-specific intent or policy context well enough.

## Consequences

### Positive

- improves operator trust and explainability
- supports deterministic replay and incident analysis
- creates a stable foundation for future policy tooling

### Negative

- adds metadata volume and write overhead
- requires careful redaction policy if memory payloads can contain sensitive content

## Future Evolution

- add a dedicated consolidation audit file family if event volume grows
- support diff-friendly structured before/after summaries
- correlate consolidation runs with retrieval profile and scoring versions
