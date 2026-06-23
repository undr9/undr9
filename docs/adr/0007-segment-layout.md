# ADR 0007: Segment Layout

## Status

Accepted

## Context

The subsystem specifications already establish a multi-directory layout under `data/` with separate areas for nodes, edges, indexes, vectors, WAL, and metadata. The remaining question is the segment-level organization inside those directories.

UNDR9 V1 is append-oriented, node-centric, and rebuilds derived indexes from canonical storage plus WAL. Segment layout must therefore keep canonical data separate from derived acceleration structures and make compaction straightforward.

## Decision

UNDR9 V1 segment layout will use multiple immutable segment families:

- `nodes/` for canonical node records
- `edges/` for canonical edge records
- `indexes/` for derived index snapshots
- `vectors/` reserved for future dedicated vector segment evolution
- `meta/` for manifests, audit logs, cluster metadata, and related control files

Within each canonical family, segments are monotonically named and published as immutable snapshots or append-oriented segment files. The manifest identifies the active set.

The logical segment unit uses a `16KB` block orientation for future binary packing, even though V1 payloads remain JSON-framed records.

There is no single monolithic `nodes.dat` or `edges.dat` file in V1.

## Alternatives Considered

### Single-file database layout

Rejected because it increases operational blast radius, complicates compaction, and makes partial repairs less targeted.

### Fully page-based heap files from day one

Rejected because page mutation is not aligned with the chosen append-only durability strategy.

### One directory per label or partition

Rejected for V1 because partitioning and locality-aware placement are future distributed concerns.

## Consequences

### Positive

- clean separation of canonical and derived state
- simpler compaction and backup/restore workflows
- easier corruption isolation and repair by file family
- natural fit for future replication and segment shipping

### Negative

- more filesystem entries and manifest bookkeeping
- compaction must reconcile multiple active files
- future vector specialization will require migration from node-property conventions into dedicated vector files

## Future Evolution

- add generation metadata and segment statistics footers
- introduce partition-aware segment placement for clustered deployments
- move vector payloads from node properties into dedicated vector segments without changing higher-level retrieval contracts
