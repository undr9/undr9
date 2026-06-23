# ADR 0012: Snapshot Format

## Status

Accepted

## Context

UNDR9 publishes durable state through canonical segment snapshots, index snapshots, and checkpoint-aware maintenance operations. Snapshots must be:

- versioned
- checksummed through the manifest
- easy to inspect and repair
- consistent with the append-only canonical data model

They also need to coexist with WAL replay so the engine can recover from either a recent snapshot plus WAL or from canonical files rebuilt through maintenance.

## Decision

UNDR9 V1 snapshot files will use versioned JSON documents containing:

- `format_version`
- ordered record collection
- snapshot-specific metadata as needed

The initial snapshot families are:

- node snapshot
- edge snapshot
- graph index snapshot

Snapshots are published through manifest-driven atomic replacement rather than in-place mutation.

The manifest remains the source of truth for which snapshot files are active and what checksums they must satisfy.

## Alternatives Considered

### Binary-only snapshots

Rejected for V1 because repair, debugging, and compatibility testing benefit from human-readable artifacts.

### Snapshotless recovery from WAL only

Rejected because replay-only recovery causes unbounded restart cost and complicates maintenance.

### In-place rewritten state files

Rejected because atomic publication through new snapshot files is safer and more compatible with the append-oriented design.

## Consequences

### Positive

- deterministic, inspectable storage state
- simpler backup, restore, and compatibility fixtures
- clear separation between canonical published state and replay log history

### Negative

- JSON snapshots cost more space than compact binary encodings
- large snapshots can be slower to write and parse

## Future Evolution

- support compact binary snapshot payloads with the same versioned manifest contract
- add per-snapshot statistics and bloom/filter metadata
- introduce differential snapshots if recovery and compaction costs justify them
