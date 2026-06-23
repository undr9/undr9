# Storage Architecture Audit

## Scope

This document audits the current UNDR9 storage engine implementation with a focus on:

- data structures
- disk layout
- write amplification
- memory consumption
- crash recovery
- production suitability

The analysis is based on the current code in `crates/storage`, `crates/wal`, `crates/api`, `crates/index`, configuration defaults, and related architecture documents.

## Executive Summary

UNDR9 currently behaves like a fully in-memory graph with WAL-backed durability and whole-snapshot persistence. That makes the engine simple and recoverable, but it also creates severe scaling pressure.

The single biggest storage risk is this:

- every committed write rewrites the full node, edge, and vector snapshots
- every committed write also triggers a full index rebuild and index snapshot rewrite
- transaction start and write preview clone entire in-memory maps

This is not an LSM engine, not a B+Tree engine, and not a segmented append-only graph store in the production sense. It is best described as:

- in-memory canonical state
- append-only WAL for durability
- whole-state snapshot publishing
- whole-index rebuilds

That architecture is acceptable for early correctness-first development, but it is not yet suitable for large production graph workloads.

## Key Verdict

- Single-node correctness foundation: credible
- Production-scale storage engine: not ready
- Largest architectural risk: full snapshot rewrite on every write

## Data Structures

### How nodes are stored

Canonical node state is stored in memory as:

- `BTreeMap<NodeId, NodeRecord>`

inside `StorageEngine`.

Implications:

- ordered and deterministic iteration
- exact lookup is efficient
- full graph must fit in memory

### How edges are stored

Canonical edge state is stored in memory as:

- `BTreeMap<EdgeId, EdgeRecord>`

Implications:

- edge lookup by id is efficient
- full edge set is also memory-resident

### How adjacency lists are stored

Adjacency is not canonical storage state. It is derived in `GraphIndex` using:

- forward adjacency map
- reverse adjacency map

That means adjacency is rebuilt from the full node and edge set after writes rather than updated incrementally.

Implications:

- clean separation between canonical state and read indexes
- very high rebuild cost on write

### How vectors are stored

Vectors live inside `NodeRecord.vectors` in memory and are also written into a dedicated vector snapshot on disk.

Implications:

- vector-bearing graphs grow memory use quickly
- persistence cost grows with total vector count and vector dimensionality

## Disk Layout

## Current model

UNDR9 uses a hybrid of:

- append-only WAL segments
- manifest-driven full snapshots

It is not currently:

- LSM
- B+Tree
- page-oriented storage
- multi-segment immutable state store

### On-disk structure

The active storage layout is:

- `manifest.json`
- `wal/`
- `nodes/`
- `edges/`
- `indexes/`
- `vectors/`
- `meta/`

### Actual storage pattern

The engine flow is:

1. append write batch to WAL
2. apply batch to in-memory maps
3. rewrite full node snapshot
4. rewrite full edge snapshot
5. rewrite full vector snapshot
6. rewrite manifest
7. rebuild indexes from all nodes and edges
8. rewrite index snapshot

This is best described as:

- WAL + full-state snapshot publishing + full derived-index rebuild

## Write Amplification

## WAL write size

WAL cost per commit is roughly proportional to the changed records only, which is good in isolation.

However, WAL efficiency is overwhelmed by the rest of the commit path.

## Snapshot amplification

Snapshot amplification is extreme.

For a small write affecting one node or one edge, the engine currently rewrites:

- all nodes
- all edges
- all vectors
- manifest metadata

This means a one-record update behaves like an `O(total dataset)` persistence operation.

## Compaction amplification

Compaction is explicit and offline. It republishes canonical state and then truncates WAL segments.

This keeps the design simple, but it means:

- heavy I/O during maintenance
- no fine-grained steady-state compaction strategy
- operational dependency on manual maintenance discipline

## Total write amplification

Total write amplification per committed write is currently approximately:

- one WAL append
- one full node snapshot rewrite
- one full edge snapshot rewrite
- one full vector snapshot rewrite
- one manifest rewrite
- one full index rebuild
- one full index snapshot rewrite

This is the most important production risk in the entire system.

## Memory Consumption

## Base memory model

The engine keeps canonical graph state fully in memory:

- all nodes
- all edges
- vectors inside nodes

It also keeps derived index structures in memory.

## Transaction overhead

Transaction start clones the full node and edge maps into transaction-local snapshots.

Normal writes also clone whole maps for preview before commit.

Implications:

- memory scales with dataset size
- memory also scales with transaction concurrency
- one active transaction can approach another copy of canonical graph state

## Scaling estimates

These are architectural estimates, not benchmarked measurements.

### 1M nodes

Likely still feasible on a strong single machine if:

- node payloads are small
- vectors are sparse or limited
- transaction concurrency is low

But write amplification will already be visible.

### 10M nodes

High risk.

Expected problems:

- full snapshot rewrite times become operationally painful
- index rebuild on every write becomes expensive
- memory pressure becomes serious, especially with vectors

### 100M nodes

Not realistic with the current architecture.

Main blockers:

- full in-memory canonical state
- full-snapshot rewrite model
- full-map cloning for writes and transactions
- full-index rebuilds

### 1B nodes

Not credible with the current design.

This would require a materially different storage architecture.

## Recovery Cost

## Crash recovery flow

Recovery does:

1. load manifest
2. load full node snapshot
3. load full edge snapshot
4. load vector snapshot
5. replay WAL records newer than `last_applied_lsn`
6. republish corrected state

## Crash recovery time

Recovery time has two main components:

- full snapshot load time
- WAL replay time

Because the snapshots are whole-state files, cold-start cost grows with total graph size even before replay starts.

## WAL replay time

WAL replay is bounded by configuration through `max_replay_bytes`.

This is helpful as a safety mechanism, but it also creates an operational hazard:

- if WAL growth exceeds the allowed replay budget before compaction, recovery can fail

## Recovery assessment

Recovery is functionally implemented and test-covered.

Recovery is not yet optimized for:

- very large datasets
- long WAL tails
- online maintenance windows

## Risks

### High

- Full dataset snapshot rewrite on every committed write
- Full graph index rebuild on every committed write
- Full-map cloning on write preview and transaction start

### Medium

- Recovery depends on manual compaction discipline
- Compaction window may rely on filesystem durability assumptions stronger than the code explicitly enforces

### Low

- Recovery metadata exists but is not fully leveraged for more advanced restart strategies

## What Is Missing

- incremental persistence of changed records
- incremental index maintenance
- segmented immutable storage or page-based storage
- scalable MVCC or delta-based transactional snapshots
- large-scale recovery benchmarking
- measured durability envelope under heavy write load

## What Should Be Improved

## Short term

- stop rebuilding full indexes on every write
- measure full snapshot publish latency at increasing scales
- measure transaction memory overhead
- add dataset-size-aware warnings around compaction and replay budget

## Medium term

- replace full-snapshot-on-write with incremental segment publishing or checkpoint batching
- move toward append-oriented immutable segments or page-oriented storage
- introduce incremental adjacency and label index updates

## Long term

- redesign storage so steady-state writes are proportional to changed records, not full graph size
- redesign transactions to avoid whole-graph cloning

## Production Readiness Verdict

As a storage engine for a serious production graph database, the current design is not ready.

It is:

- simple
- testable
- recoverable
- understandable

But it is also:

- write-amplified
- memory-heavy
- operationally fragile at scale

UNDR9 needs a fundamentally more scalable persistence model before enterprise graph workloads should be expected to trust it.
