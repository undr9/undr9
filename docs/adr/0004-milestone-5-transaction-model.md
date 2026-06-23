# ADR 0004: Milestone 5 Snapshot Transactions

## Status

Accepted

## Context

Milestone 5 expands UNDR9 from atomic write batches to explicit multi-operation transactions.

The existing system already provides:

- WAL-backed durable commits
- a single-node storage engine
- consistent committed-state reads
- a simple single-writer correctness model

The open design question is how to add explicit transactions without breaking the existing WAL and recovery path.

## Decision

Milestone 5 introduces explicit transaction sessions with:

- snapshot-style reads over the committed state visible at `begin_transaction`
- staged node and edge mutations in memory until commit
- optimistic write-write conflict detection on touched node and edge ids
- atomic commit through the existing WAL `WriteBatch` record path
- rollback by discarding the staged session state

The only supported isolation level in this milestone is:

- `Snapshot`

## Guarantees

- Reads inside a transaction see a stable snapshot plus that transaction's own staged writes.
- Uncommitted writes are never visible outside the transaction.
- Commit is atomic and durable because the final staged batch is committed through the existing WAL-backed write path.
- Conflicts are detected when a touched node or edge changed after the transaction's start revision.

## Non-Goals

Milestone 5 does not promise:

- serializable isolation
- predicate conflict detection
- phantom protection
- distributed transactions
- long-lived persisted transaction sessions across process restarts

## Rationale

- It preserves WAL and recovery compatibility by keeping the final durable unit a `WriteBatch`.
- It keeps the concurrency model explainable and independently testable.
- It allows future evolution toward MVCC or stronger isolation without invalidating the current storage format.

## Consequences

### Positive

- explicit transaction API
- rollback semantics
- concurrent conflict tests with deterministic behavior

### Negative

- write conflicts are still discovered optimistically at commit time
- snapshot state is currently kept in memory per active transaction
- process restarts clear in-flight transactions
