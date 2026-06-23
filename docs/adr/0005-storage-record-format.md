# ADR 0005: Storage Record Format

## Status

Accepted

## Context

The canonical decision set fixes UNDR9 V1 as an append-oriented graph and memory database with durable WAL-backed recovery, immutable segments, and node-centric memory retrieval. That requires a storage record format that:

- supports append-only writes
- survives partial-write and crash scenarios
- is versioned for forward-compatible storage evolution
- can represent nodes, edges, and tombstones without separate per-record schemas
- remains inspectable enough for repair and operator tooling

The format must work for single-node V1 while remaining compatible with later compaction, replication, and snapshot publication.

## Decision

UNDR9 storage records will use a self-describing framed envelope with the following logical fields:

- `format_version`
- `record_kind`
- `record_length`
- `record_checksum_crc32`
- `flags`
- `payload`

The payload for V1 canonical records is structured data for:

- node upsert
- edge upsert
- node tombstone
- edge tombstone

Each stored record is immutable after append. Deletes are represented as tombstones, not in-place rewrites.

The record format is append-friendly rather than page-update-friendly. Segment readers consume a sequence of framed records and can stop safely at the last valid checksum boundary.

## Alternatives Considered

### Fixed-width binary records

Rejected because nodes, edges, and memory properties are variable-sized and would either waste space or force indirection too early.

### Raw JSON lines without framing

Rejected because JSON lines alone do not provide robust length framing, binary-safe boundaries, or corruption detection strong enough for durable storage internals.

### Page-slotted records with in-place mutation

Rejected for V1 because the canonical architecture favors append-only durability and simpler crash recovery over mutation-heavy page management.

## Consequences

### Positive

- simplifies append-only writing and torn-tail detection
- supports forward schema evolution through explicit format versioning
- allows repair tools to scan records without reconstructing page state
- aligns naturally with WAL replay and immutable segment publication

### Negative

- incurs per-record framing overhead
- increases write amplification relative to in-place updates
- defers some space-efficiency gains until later compaction and encoding improvements

## Future Evolution

- add stronger checksums or optional cryptographic hashes for archival integrity
- introduce binary payload encoding beneath the same outer framing contract
- support optional compression at the segment block level
- add richer flags for provenance, tenant isolation, or replication metadata if later milestones require them
