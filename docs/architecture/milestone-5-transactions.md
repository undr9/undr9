# Milestone 5 Transaction Model

## Scope

Milestone 5 extends the single-node engine with:

- explicit transaction sessions
- snapshot-style reads
- optimistic conflict detection
- rollback semantics
- transaction API coverage and concurrent tests

## Transaction Lifecycle

1. `begin_transaction` captures the current committed revision and a snapshot of nodes and edges.
2. Staged operations update only the transaction snapshot.
3. Reads inside the transaction observe the snapshot plus staged writes.
4. `commit_transaction` checks for write-write conflicts on touched records.
5. If conflict-free, the merged staged batch commits through the existing WAL-backed write path.
6. `rollback_transaction` drops the staged state.

## Isolation Model

The currently supported isolation level is:

- `Snapshot`

This means:

- a transaction sees a stable committed snapshot from its start time
- it also sees its own staged writes
- committed writes from other transactions do not become visible mid-transaction

This does not yet provide serializable isolation.

## Conflict Detection

The engine tracks lineage revisions for node and edge ids. Commit fails with a conflict when a touched id changed after the transaction's starting revision.

This catches:

- concurrent updates to the same node
- concurrent updates to the same edge
- deletes versus updates on the same ids
- cascaded edge deletes caused by node deletion

## Durability

Transactions do not introduce a new WAL record format yet. The durable unit remains the final `WriteBatch` written at commit time, which preserves recovery compatibility with earlier milestones.

## Tradeoffs

- Snapshot cloning keeps the first explicit transaction model simple and testable.
- Optimistic conflict detection keeps the single-writer storage path intact.
- In-flight transactions are not recovered across process restarts.

## Future Work

Later milestones can evolve this model toward:

- lighter-weight MVCC snapshots
- serializable isolation
- persistent transaction metadata
- replicated transaction coordination
