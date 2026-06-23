# Query Engine Audit

## Scope

This audit reviews the UNDR9 query engine with emphasis on:

- traversal cost
- shortest-path behavior
- planner type
- execution model
- memory usage
- production suitability as a graph query engine

## Executive Summary

UNDR9 has a clean and understandable query engine, but it is not yet the kind of mature execution engine that makes production graph databases competitive.

The biggest strengths are:

- simple typed JSON query model
- deterministic behavior
- adjacency-backed traversal
- bidirectional BFS for shortest path

The biggest weaknesses are:

- no cost-based planner
- no streaming operator pipeline
- whole-graph snapshot cloning per query
- global scans for vector search and ranked retrieval

This engine is appropriate for a correctness-first product phase. It is not yet optimized enough to be considered production-grade query infrastructure for large graphs.

## Key Verdict

- Planner maturity: early
- Execution maturity: early
- Graph traversal model: reasonable for bounded local traversals
- Large-scale query engine maturity: not ready

## Planner Audit

## Rule based or cost based?

The planner is rule based, not cost based.

Each query request variant maps deterministically to a fixed `PlanKind`. There is no evidence of:

- cardinality estimation
- access path comparison
- cost-based plan selection
- adaptive execution
- join reordering

This means planner behavior is straightforward but limited.

## Consequences

Positive:

- predictable
- easy to debug
- stable behavior across runs

Negative:

- cannot optimize based on graph shape
- cannot choose between alternative access paths
- cannot become "smart" on dense or skewed graphs

## Execution Model Audit

## Pull model or push model?

Neither in the mature database sense.

The current engine is best described as:

- snapshot clone
- direct function dispatch
- in-memory execution
- fully materialized results

There is no sign of a classic iterator pipeline, pull-based operator tree, push pipeline, or streaming runtime.

## How execution works

The flow is:

1. API takes a database snapshot
2. planner validates request and returns a fixed plan kind
3. executor pattern-matches the request type
4. specialized functions execute using in-memory maps and indexes
5. results are collected and returned

This design is simple, but it is not an advanced graph query execution engine.

## Traversal Cost Audit

## 1 hop

One-hop traversal is the best-case graph query shape for the current system.

Behavior:

- adjacency index lookup
- iterate connected edges
- apply optional filters

This is likely efficient for sparse and moderately connected nodes.

## 2 hop

Two-hop traversal is still acceptable for bounded local exploration.

Cost depends mostly on frontier fan-out:

- sparse graph: manageable
- hub-heavy graph: can expand quickly

## 5 hop

At five hops, cost becomes highly graph-shape dependent.

Expected behavior:

- acceptable only when fan-out remains controlled
- sensitive to high-degree nodes
- requires strict use of `limit`, `max_hops`, and `timeout_ms`

## 10 hop

Ten-hop traversal is not credible as a generally safe query pattern on large graphs under the current architecture.

Reasons:

- BFS frontier growth dominates
- results remain fully materialized
- snapshots and supporting structures are already expensive
- there is no cost-aware plan adjustment

## Traversal conclusion

The current system is best suited for:

- 1 hop
- 2 hop
- small bounded multi-hop queries

It is not yet suitable for broad deep graph exploration at scale.

## Shortest Path Audit

Shortest path uses unweighted bidirectional BFS.

That is the correct algorithm for:

- shortest path by hop count

It is not the correct algorithm for:

- weighted routing
- cost-sensitive routing
- heuristic route search

## Strengths

- better than one-sided BFS
- appropriate for unweighted graph connectivity
- bounded by direction, depth, limit, and timeout

## Weaknesses

- not weighted
- not cost-aware
- still sensitive to frontier explosion
- may terminate early because `limit` acts as a coarse search budget

## Retrieval Query Audit

## Vector search

Vector search currently behaves like:

- scan all vector candidates
- compute cosine similarity
- sort all scored results
- truncate to limit

This is not ANN.

Missing:

- HNSW
- IVF
- PQ
- graph ANN structures
- vector-specific paging or segment pruning

Result:

- acceptable for small or moderate vector sets
- not appropriate for large semantic retrieval workloads

## Ranked retrieval

Ranked retrieval is functionally strong in concept because it blends:

- semantic similarity
- structural relevance
- temporal freshness
- importance
- confidence

But its execution is still expensive because it effectively performs:

- local structural distance computation
- wide candidate scan
- full result sorting

This is a good product idea implemented on top of an early execution engine.

## Memory Usage

The query path is memory-heavy.

Main reasons:

- query snapshots clone all nodes and edges
- transaction queries clone transaction-local graph state
- traversal allocates visited sets and queues
- vector search and ranked retrieval allocate full scored result lists before sorting

This means query cost is not just about execution time. It is also about:

- temporary memory
- allocation churn
- snapshot duplication

## Large Graph Behavior

## Likely acceptable

- exact lookup
- unique-key lookup
- neighbor listing
- bounded traversal in sparse subgraphs

## Likely problematic

- deep traversal on dense graphs
- large vector search workloads
- large hybrid retrieval workloads
- many concurrent queries with large snapshots

## What Is Missing

- cost-based planning
- operator pipeline execution
- streaming results
- incremental or shared snapshot model
- ANN indexing for vectors
- graph statistics for planning
- realistic large-graph benchmark suite

## What Should Be Improved

## Short term

- stop cloning whole graph state per query where possible
- benchmark 1-hop, 2-hop, 5-hop, and 10-hop traversals on realistic graph shapes
- isolate and measure vector search and ranked retrieval candidate costs

## Medium term

- introduce shared immutable snapshots or copy-on-write structures
- add statistics-aware planning
- distinguish return limit from search budget more cleanly
- add streaming result support for traversal-heavy reads

## Long term

- build a real graph execution engine with operator abstractions
- add ANN for vector retrieval
- introduce cost-based planning informed by graph statistics

## Production Readiness Verdict

UNDR9 is not ready as a production-grade graph query engine for large-scale workloads.

It is:

- correct enough to reason about
- bounded in important places
- architecturally coherent

But it is not yet:

- planner-smart
- memory-efficient
- large-scale optimized
- competitive with mature graph execution engines

For production graph database positioning, query execution needs to become a first-class engineering program, not just a clean implementation of basic query types.
