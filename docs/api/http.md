# UNDR9 HTTP API

## Authentication

When authentication is enabled, clients must provide:

```text
x-api-key: <api-key>
```

Configured keys currently map to:

- admin
- writer
- reader

## Health Endpoints

### `GET /healthz`

Returns:

```json
{
  "service": "undr9",
  "status": "ok"
}
```

### `GET /readyz`

Returns:

```json
{
  "service": "undr9",
  "status": "ready"
}
```

During shutdown or drain, this endpoint returns `503 Service Unavailable` with:

```json
{
  "service": "undr9",
  "status": "draining"
}
```

### `GET /metrics`

Returns Prometheus-style metrics.

## Node CRUD

### `POST /v1/nodes`

Request body:

```json
{
  "id": "node_a",
  "node_type": "memory",
  "properties": {
    "unique_key": {
      "kind": "String",
      "value": "alpha"
    },
    "timestamp": {
      "kind": "Integer",
      "value": 1720000000000
    },
    "importance": {
      "kind": "Float",
      "value": 0.9
    },
    "confidence": {
      "kind": "Float",
      "value": 0.85
    }
  },
  "vectors": {
    "default": [0.12, 0.44, 0.31, 0.78]
  }
}
```

### `GET /v1/nodes/:id`

Returns the node record if it exists.

### `PUT /v1/nodes/:id`

The path id must match the body `id`.

### `DELETE /v1/nodes/:id`

Returns `204 No Content` on success.

When the local node is configured as a follower, node write endpoints return `409 Conflict` and only replicated writes are accepted.

## Edge CRUD

### `POST /v1/edges`

Request body:

```json
{
  "id": "edge_ab",
  "source": "node_a",
  "target": "node_b",
  "edge_type": "relates_to",
  "properties": {}
}
```

### `GET /v1/edges/:id`

Returns the edge record if it exists.

### `PUT /v1/edges/:id`

The path id must match the body `id`.

### `DELETE /v1/edges/:id`

Returns `204 No Content` on success.

When the local node is configured as a follower, edge write endpoints return `409 Conflict`.

## Query Endpoint

### `POST /v1/query`

Supported query payloads:

Milestone 3 retrieval signals use the following reserved node properties:

- `timestamp`: `Integer` epoch milliseconds
- `importance`: `Float` or `Integer`
- `confidence`: `Float` or `Integer`

Vector data is stored only in the node `vectors` map. `properties.embedding` is no longer
accepted. The default retrieval vector is `vectors.default`, and clients can store additional
named vectors such as `vectors.title`, `vectors.summary`, or `vectors.unique_key`.

#### Exact Lookup By Node Id

```json
{
  "GetNodeById": {
    "node_id": "node_a"
  }
}
```

#### Exact Lookup By Unique Key

```json
{
  "GetNodeByUniqueKey": {
    "unique_key": "alpha"
  }
}
```

#### Neighbor Listing

```json
{
  "ListNeighbors": {
    "node_id": "node_a",
    "edge_type": "relates_to",
    "direction": "Outgoing"
  }
}
```

#### Bounded Traversal

```json
{
  "Traverse": {
    "start_node_id": "node_a",
    "edge_type": "relates_to",
    "direction": "Outgoing",
    "max_hops": 2
  }
}
```

#### Label/Type Lookup

```json
{
  "SearchByLabel": {
    "label": "memory"
  }
}
```

#### Filter Nodes

`FilterNodes` provides database-side property filtering with an optional `label` prefilter. Use
`label` for node type selection, and express predicate logic in the `where` clause with
`eq`, `gt`, `gte`, `lt`, `lte`, `and`, and `or`.

| Operator | Meaning                             | Example                     |
| -------- | ----------------------------------- | --------------------------- |
| `eq`     | Equal to                            | age = 25                    |
| `gt`     | Greater than                        | age > 25                    |
| `gte`    | Greater than or equal to            | age >= 25                   |
| `lt`     | Less than                           | age < 25                    |
| `lte`    | Less than or equal to               | age <= 25                   |
| `and`    | Both conditions must be true        | age > 25 AND city = 'Delhi' |
| `or`     | At least one condition must be true | age > 25 OR city = 'Delhi'  |


```json
{
  "FilterNodes": {
    "label": "user",
    "where": {
      "op": "or",
      "conditions": [
        {
          "op": "gt",
          "field": "score",
          "value": {
            "kind": "Integer",
            "value": 90
          }
        },
        {
          "op": "eq",
          "field": "unique_key",
          "value": {
            "kind": "String",
            "value": "alice"
          }
        }
      ]
    },
    "limit": 50
  }
}
```

Notes:

- `label` maps to the node `node_type` field and is optional.
- `eq` supports exact matching for property values and special fields `id` and `label`.
- `gt`, `gte`, `lt`, and `lte` require numeric property values.
- `and` and `or` accept a `conditions` array of nested predicates.

Additional examples:

Match a normal property exactly:

```json
{
  "FilterNodes": {
    "where": {
      "op": "eq",
      "field": "unique_key",
      "value": {
        "kind": "String",
        "value": "alice"
      }
    },
    "limit": 10
  }
}
```

Match the built-in node `id` exactly:

```json
{
  "FilterNodes": {
    "where": {
      "op": "eq",
      "field": "id",
      "value": {
        "kind": "String",
        "value": "node_123"
      }
    },
    "limit": 1
  }
}
```

Match the built-in node `label` exactly:

```json
{
  "FilterNodes": {
    "where": {
      "op": "eq",
      "field": "label",
      "value": {
        "kind": "String",
        "value": "user"
      }
    },
    "limit": 50
  }
}
```

Use the top-level `label` prefilter with a numeric property predicate:

```json
{
  "FilterNodes": {
    "label": "user",
    "where": {
      "op": "gt",
      "field": "score",
      "value": {
        "kind": "Integer",
        "value": 90
      }
    },
    "limit": 50
  }
}
```

#### Temporal Range Search

```json
{
  "TimeRange": {
    "field": "timestamp",
    "from_epoch_ms": 1000,
    "to_epoch_ms": 2000,
    "limit": 10
  }
}
```

#### Vector Search

```json
{
  "VectorSearch": {
    "query_vector": [1.0, 0.0],
    "vector_name": "default",
    "node_type": "memory",
    "limit": 10,
    "top_k": 50
  }
}
```

`vector_name` selects which named vector space to search. `limit` controls the number of final
results returned. `top_k` optionally overrides the semantic candidate pool size before final
ranking.

#### Ranked Retrieval

```json
{
  "RankedRetrieval": {
    "query_vector": [1.0, 0.0],
    "vector_name": "default",
    "reference_node_id": "node_a",
    "edge_type": "relates_to",
    "from_epoch_ms": 1710000000000,
    "to_epoch_ms": 1719999999999,
    "limit": 10,
    "top_k": 50,
    "now_epoch_ms": 1720000000000,
    "retrieval_profile": "v1-default"
  }
}
```

`RankedRetrieval` uses `vector_name` only for the semantic portion of hybrid ranking. Structural,
temporal, importance, and confidence scoring remain unchanged.

Example response:

```json
{
  "plan_kind": "RankedHybrid",
  "nodes": [
    {
      "id": "node_a",
      "node_type": "memory",
      "properties": {}
    }
  ],
  "edges": [],
  "ranked_results": [
    {
      "node": {
        "id": "node_a",
        "node_type": "memory",
        "properties": {}
      },
      "score": 0.92,
      "breakdown": {
        "structural": 1.0,
        "semantic": 0.95,
        "temporal": 0.80,
        "importance": 0.70,
        "confidence": 0.60
      }
    }
  ]
}
```

## Error Responses

All failures return:

```json
{
  "code": "validation_error",
  "message": "path node id does not match request body id",
  "details": []
}
```

## Admin Endpoints

Admin maintenance endpoints require an admin API key.

### `POST /v1/admin/compact`

Compacts the current single-node storage state.

### `GET /v1/admin/integrity`

Returns an integrity report:

```json
{
  "manifest_present": true,
  "node_snapshot_valid": true,
  "edge_snapshot_valid": true,
  "wal_replay_valid": true,
  "node_count": 2,
  "edge_count": 1,
  "issues": []
}
```

### `POST /v1/admin/rebuild-indexes`

Rebuilds derived indexes and republishes the index snapshot metadata.

### `GET /v1/admin/maintenance/status`

Returns the current or last known maintenance execution state, including:

- `last_operation`
- `last_outcome`
- `elapsed_ms`
- `last_node_count`
- `last_edge_count`
- `max_node_count`
- `max_edge_count`

### `POST /v1/admin/backup`

Request body:

```json
{
  "destination": "/tmp/undr9-backup"
}
```

The backup directory includes `backup-manifest.json` with file checksums and an integrity snapshot for restore validation.
Maintenance endpoints that return `MaintenanceResponse` now include `status`, `operation`, `elapsed_ms`, and `detail` so operators can audit execution duration directly from the API response.
Maintenance endpoints can reject with `maintenance_budget_exceeded` when the current dataset exceeds the configured maintenance budget.

### `POST /v1/admin/restore`

Request body:

```json
{
  "source": "./backup",
  "target_lsn": 42
}
```

Restore validates the backup manifest and verifies a staged copy before replacing the live storage root.
If `target_lsn` is present, UNDR9 performs a point-in-time restore within the retained WAL window of the verified backup.

### `POST /v1/admin/repair`

Runs repair over snapshots, WAL, and manifest metadata.

## Replication And Cluster Endpoints

Milestone 6 introduces manual leader-based log shipping with persisted replication and cluster metadata under `data/meta/`.

### `GET /v1/admin/replication/status`

Returns the local replication state and per-replica lag:

```json
{
  "status": {
    "mode": "Leader",
    "local_node_id": "leader-1",
    "leader_node_id": "leader-1",
    "current_term": 2,
    "last_applied_lsn": 4,
    "last_committed_source_lsn": 4,
    "last_pulled_source_lsn": 0,
    "last_applied_source_lsn": 4,
    "replicas": [
      {
        "replica_node_id": "replica-1",
        "last_acked_source_lsn": 3,
        "last_applied_source_lsn": 3
      }
    ]
  },
  "replica_lag": {
    "replica-1": 1
  }
}
```

### `GET /v1/admin/replication/history?after_source_lsn=0`

Returns committed leader batches available for shipping to followers.

### `POST /v1/admin/replication/leader`

Promotes the local node to leader mode and updates cluster term metadata.

### `POST /v1/admin/replication/follower`

Request body:

```json
{
  "leader_node_id": "leader-1",
  "leader_address": "127.0.0.1:9100"
}
```

Configures the local node as a follower of the named leader.

### `POST /v1/admin/replication/ack`

Request body:

```json
{
  "replica_node_id": "replica-1",
  "source_lsn": 4
}
```

Updates replica acknowledgement progress on the leader.

### `POST /v1/admin/replication/apply`

Request body:

```json
{
  "records": [
    {
      "source_node_id": "leader-1",
      "source_term": 2,
      "source_lsn": 4,
      "batch": {
        "nodes_upserted": [],
        "edges_upserted": [],
        "deleted_node_ids": [],
        "deleted_edge_ids": []
      }
    }
  ]
}
```

Applies shipped leader records to a follower using the normal WAL-backed storage path.

### `GET /v1/admin/cluster/topology`

Returns the persisted cluster topology:

```json
{
  "term": 3,
  "leader_node_id": "leader-1",
  "nodes": [
    {
      "node_id": "leader-1",
      "address": "127.0.0.1:9100",
      "role": "Primary",
      "healthy": true
    },
    {
      "node_id": "replica-1",
      "address": "127.0.0.1:9101",
      "role": "Replica",
      "healthy": true
    }
  ]
}
```

### `POST /v1/admin/cluster/nodes`

Request body:

```json
{
  "node_id": "replica-1",
  "address": "127.0.0.1:9101"
}
```

Registers a readable replica in cluster topology and the leader's replication status.

### `POST /v1/admin/cluster/nodes/:id/health`

Request body:

```json
{
  "healthy": false
}
```

Updates health metadata for a cluster node.

### `POST /v1/admin/cluster/promote`

Request body:

```json
{
  "node_id": "replica-1"
}
```

Promotes the named node to leader in the cluster topology and updates the local node's replication role accordingly.

## Transaction Endpoints

The current transaction API supports explicit snapshot transactions.

### `POST /v1/transactions/begin`

Request body:

```json
{
  "isolation_level": "Snapshot"
}
```

Response example:

```json
{
  "transaction_id": "tx_1",
  "isolation_level": "Snapshot",
  "state": "Active",
  "started_at_revision": 4,
  "staged_operation_count": 0,
  "touched_node_count": 0,
  "touched_edge_count": 0
}
```

When the local node is configured as a follower, transaction lifecycle endpoints return `409 Conflict` because the node is read-only for client writes.

### `GET /v1/transactions`

Lists active transaction summaries.

### `GET /v1/transactions/:id`

Returns the transaction summary for a single active transaction.

### `POST /v1/transactions/:id/operations`

Supported staged operation payloads:

```json
{
  "UpsertNode": {
    "id": "node_a",
    "node_type": "memory",
    "properties": {}
  }
}
```

```json
{
  "DeleteNode": {
    "node_id": "node_a"
  }
}
```

```json
{
  "UpsertEdge": {
    "id": "edge_ab",
    "source": "node_a",
    "target": "node_b",
    "edge_type": "relates_to",
    "properties": {}
  }
}
```

```json
{
  "DeleteEdge": {
    "edge_id": "edge_ab"
  }
}
```

### `POST /v1/transactions/:id/query`

Runs a normal query against the transaction's snapshot plus staged writes.

### `POST /v1/transactions/:id/commit`

Commits the staged transaction atomically and durably.

Response example:

```json
{
  "transaction_id": "tx_1",
  "committed_revision": 5,
  "committed_lsn": 5,
  "staged_operation_count": 3
}
```

### `POST /v1/transactions/:id/rollback`

Rolls back the active transaction and discards its staged changes.
