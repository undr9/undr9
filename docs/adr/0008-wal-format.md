# ADR 0008: WAL Format

## Status

Accepted

## Context

The canonical decision set commits UNDR9 to WAL-backed durability, segment rotation, checkpoint-based pruning, and later reuse of WAL sequencing for replication. The WAL format therefore has to be stable enough for:

- crash recovery
- checkpoint publication
- transaction commit durability
- future replication streams

It must also preserve the repository's emphasis on explicit, versioned, inspectable behavior.

## Decision

UNDR9 WAL entries will use a versioned framed envelope containing:

- `lsn`
- `record_kind`
- `format_version`
- `payload_length`
- `payload_checksum_crc32`
- `payload`

The supported V1 record kinds are:

- `WriteBatch`
- `Checkpoint`
- `ManifestSync`

`WriteBatch` is the canonical durable mutation unit for:

- single-record writes
- multi-record atomic writes
- explicit transaction commits
- follower-applied replicated writes

WAL segments rotate by configured maximum size, defaulting to `64MB`. Replay is strictly ordered by increasing LSN and stops safely at the last valid record boundary.

## Alternatives Considered

### Operation-specific WAL entries for every field mutation

Rejected because it would complicate recovery and diverge from the existing write-batch contract.

### Unframed append logs

Rejected because they make torn-write detection and partial-tail handling significantly weaker.

### Consensus log as the only durable log

Rejected for V1 because distributed coordination is intentionally deferred until later milestones.

## Consequences

### Positive

- keeps the durable unit simple and composable
- allows replay, transactions, and replication to share one commit primitive
- makes checkpointing and segment pruning operationally straightforward
- supports deterministic crash recovery semantics

### Negative

- `WriteBatch` can be larger than finer-grained log events
- later replication may want richer metadata than the V1 WAL currently carries
- JSON payloads cost more space than compact binary encodings

## Future Evolution

- add optional compression per segment
- add term and replica provenance fields when Raft-backed consensus is introduced
- support background archival and WAL shipping to object storage
- evolve payload encoding while preserving the outer framed envelope
