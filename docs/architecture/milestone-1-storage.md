# Milestone 1 Storage Architecture

## Scope

Milestone 1 implements the first durable single-node write path for UNDR9:

- manifest-backed storage bootstrap
- WAL append and replay
- persistent node and edge snapshot segments
- in-process CRUD engine
- crash recovery and graceful shutdown

## Commit Flow

Each logical write batch follows this order:

1. validate the batch against the current in-memory state
2. append the batch to the WAL
3. apply the batch to in-memory node and edge maps
4. atomically publish updated node, edge, and vector snapshot files
5. atomically publish the manifest with updated checksums and `last_applied_lsn`

This ordering ensures a committed write is either already reflected in the snapshot segments or can be reconstructed from the WAL after restart.

## Recovery Flow

On startup, the engine:

1. bootstraps the storage layout and loads the manifest
2. loads the current node, edge, and vector snapshot files
3. replays WAL records with LSNs newer than `last_applied_lsn`
4. republishes snapshots and manifest to converge state

The snapshot files are treated as the base state and the WAL is treated as the recovery source for newer committed mutations.

## Current Segment Strategy

Milestone 1 uses one active snapshot segment per entity family:

- `data/nodes/segment-0000000000000001.snapshot.rkyv`
- `data/edges/segment-0000000000000001.snapshot.rkyv`
- `data/vectors/segment-0000000000000001.snapshot.rkyv`

This is intentionally simple and safe for the first durable implementation. Later milestones can evolve this into rolling immutable segments plus compaction without changing the high-level commit protocol.

## Tradeoffs

- **Safety**: snapshot publication uses temp-file plus rename, which minimizes partial-write exposure.
- **Simplicity**: snapshot rewriting is easier to reason about than a full append-only segment compaction system.
- **Cost**: rewriting the active snapshot on every committed batch increases write amplification and will not be the final scaling strategy.
- **Recovery**: WAL replay is deterministic and bounded by `max_replay_bytes`, but long WAL windows should eventually be reduced through more explicit checkpoint and compaction policies.

## Immediate Follow-Up

Milestone 2 should build on this foundation by:

- adding primary indexes
- exposing CRUD through the HTTP API
- keeping transport handlers thin by delegating to the storage/query services
