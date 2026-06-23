# Milestone 6 Replication And Clustering

## Scope

Milestone 6 extends the single-node engine with:

- leader-based replication metadata
- manual log shipping through replicated `WriteBatch` records
- follower read-only mode for client traffic
- cluster topology and failover metadata
- CLI and HTTP administration surfaces
- tests for lag, apply, and promotion behavior

## Data Plane

The leader reuses the existing durable storage path:

1. A client write commits through the normal WAL-backed `StorageEngine` path.
2. The API composition layer records the committed `WriteBatch` and source LSN in the replication manager.
3. Followers fetch shipped records through the admin history endpoint or the CLI `replication-history` export workflow.
4. Followers apply those records through their own local `StorageEngine`, preserving local WAL durability and recovery semantics.

This keeps replication outside storage internals while still reusing the same durable write contract.

## Control Plane

Cluster topology is tracked separately from replication progress:

- `undr9-replication` owns leader or follower mode, source LSN tracking, ack progress, and shipped history
- `undr9-cluster` owns nodes, addresses, health, leader identity, and failover term progression
- `undr9-api` composes both and persists metadata under `data/meta/`

The current design is intentionally explicit and operator-driven rather than consensus-backed.

## Metadata Persistence

Milestone 6 persists:

- `data/meta/replication-state.json`
- `data/meta/cluster-topology.json`

This allows CLI commands and server restarts to preserve local replication mode, replica lag state, and topology information.

## Follower Semantics

When a node is in follower mode:

- CRUD write endpoints reject client writes with `409 Conflict`
- transaction begin, stage, commit, and rollback endpoints reject client writes with `409 Conflict`
- read endpoints and query endpoints remain available
- replicated records can still be applied through the replication admin surface

This provides a clear read-replica model without changing the storage engine's single-writer assumptions.

## Failover

Failover is modeled as an explicit topology promotion:

1. Promote the replacement node in cluster metadata.
2. If the local node is the promoted node, switch local replication mode to `Leader`.
3. Otherwise switch local replication mode to `Follower` and point it at the new leader.

The resulting term is stored in cluster topology and reused by the replication manager.

## Tradeoffs

- Manual log shipping keeps the implementation small, testable, and aligned with the roadmap's requirement to avoid distributed coupling before interfaces stabilize.
- Persisting metadata in JSON keeps the operator workflow transparent and easy to inspect.
- The design does not yet implement quorum commit, automatic election, or consensus membership changes.

## Future Work

Later milestones can evolve this into:

- automatic leader election
- background streaming replication
- persisted replication history compaction
- node replacement and rebalancing automation
- consensus-backed commit coordination
