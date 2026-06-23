# Milestone 4 Operational Hardening

## Scope

Milestone 4 extends the queryable single-node service with:

- storage maintenance operations
- richer metrics
- structured logs
- audit events
- Docker and local deployment artifacts
- benchmark and compatibility automation

## Maintenance Operations

The current maintenance surface supports:

- compaction
- integrity verification
- backup
- restore
- repair
- index rebuild

These operations are available through:

- admin HTTP endpoints
- CLI commands

## Compaction

The current storage engine still uses snapshot-style node and edge segments, so compaction is implemented as:

1. publish a fresh checkpoint
2. republish current snapshots
3. truncate WAL segments

This reduces replay volume and keeps the single-node format operationally manageable until more advanced immutable segment compaction lands in later milestones.

## Backup And Restore

Backup and restore currently operate at the storage-root directory level:

- backup recursively copies the active data directory
- restore replaces the target data directory with backup contents

This is simple and robust for the one-node baseline.

## Repair

Repair is currently scoped to recoverable metadata and derived-state drift:

- bootstrap storage if required
- reload raw snapshots
- replay WAL
- republish manifest and snapshots with corrected checksums

This is intentionally conservative and does not attempt speculative recovery from corrupted snapshot payloads.

## Observability

Milestone 4 observability now includes:

- Prometheus metrics for readiness, node count, edge count, query count, write count, maintenance count, and audit count
- structured JSON log events for request and maintenance paths
- audit log append for write and maintenance actions

Audit events are written to:

- `data/meta/audit.log`

## Deployment

The repository now includes:

- a Dockerfile
- `docker-compose.yml`
- deployment notes under `deployments/docker/`

The runtime entrypoint uses the CLI `serve` command to host the Axum API.
