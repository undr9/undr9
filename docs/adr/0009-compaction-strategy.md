# ADR 0009: Compaction Strategy

## Status

Accepted

## Context

The approved architecture uses append-only canonical storage with tombstones and immutable segments. That inevitably creates stale records, obsolete snapshots, and retained WAL history. UNDR9 therefore needs a compaction strategy that is safe, operator-friendly, and consistent with the current single-node implementation posture.

## Decision

UNDR9 V1 compaction is an explicit maintenance operation that:

1. checkpoints the current durable state
2. rewrites active canonical node and edge state into fresh snapshot segments
3. republishes the manifest
4. truncates obsolete WAL segments after the checkpoint boundary
5. rebuilds derived indexes as needed

Compaction is deterministic and offline with respect to the local write pipeline in V1. It is not a continuous background LSM-style merge process.

Tombstones are eliminated during compaction once their effects are fully reflected in the newly published canonical state.

## Alternatives Considered

### No compaction in V1

Rejected because append-only storage without compaction would cause unbounded file growth and poorer restart and maintenance behavior.

### Always-on background compaction

Rejected for V1 because it introduces concurrency complexity, write amplification tuning, and failure modes before correctness and operability are fully established.

### LSM-style leveled compaction

Rejected because the storage engine is not an LSM tree in V1 and should not simulate one prematurely.

## Consequences

### Positive

- simple operational model
- easy-to-test maintenance semantics
- predictable checkpoint and truncation workflow
- reclaim space without changing the core write path

### Negative

- operators must schedule compaction explicitly
- large compactions can create temporary write amplification and IO spikes
- startup cost can still grow between compaction points

## Future Evolution

- add incremental or background compaction modes
- compact vector and temporal structures independently when they become first-class persisted segment families
- add throttling, scheduling, and observability for automated maintenance in larger deployments
