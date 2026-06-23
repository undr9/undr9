# Scalability Envelope Report

## Scope

This report defines the current scalability envelope of UNDR9 based on:

- repository code
- architecture documents
- available tests
- available benchmark programs

It also states what is still missing before enterprise customers should trust the platform for a serious proof of concept.

## Executive Summary

UNDR9 now has a repeatable in-repository single-node benchmark runner, but it still does not have a credible measured enterprise scalability envelope.

The repository contains:

- a repeatable `undr9-bench` runner for CRUD, traversal, temporal, vector, ranked retrieval, and WAL recovery
- a generated baseline artifact at `docs/operations/single-node-benchmark-baseline.json`
- benchmark strategy documentation
- no evidence of 1M, 10M, 100M, or 1B node tests

As of now, the scalability envelope is mostly an architectural estimate, not a measured performance claim.

## Current Measured Evidence

## Test suite

`cargo test -q` passes across the repository.

This is useful as a correctness baseline, but it does not establish scalability.

## Existing benchmark evidence

The repository now includes a benchmark entrypoint:

- `./scripts/run-benchmarks.sh`
- `cargo run -q -p undr9-cli --bin undr9-bench -- --scales 1000,5000 --iterations 3`

Current checked-in measured baseline includes:

- storage upsert and delete timings
- WAL recovery timings
- label scan timings
- bounded traversal timings
- shortest path timings
- temporal range timings
- vector search timings
- ranked retrieval timings

Important limitation:

- the current committed baseline covers 1,000 and 5,000 node scales only
- it is still not representative of enterprise-scale graph workloads

## Published Benchmark Intent

The repository states that benchmark suites should cover:

- node CRUD
- edge CRUD
- adjacency traversal
- bounded graph traversal
- temporal range retrieval
- vector similarity retrieval
- ranked hybrid retrieval
- WAL replay and recovery
- compaction and maintenance operations

That intent is now implemented for single-node baseline workloads, but not yet at the requested enterprise scales.

## Required Dataset Scales

The requested enterprise envelope should include at least:

- 1M nodes
- 10M nodes
- 100M nodes
- 1B nodes

## Current credibility by scale

### 1M nodes

Possible for a focused lab run on a strong single machine, but not yet proven.

Main questions:

- how large are node properties
- how many edges per node
- how many vectors exist
- what is vector dimensionality

### 10M nodes

Low confidence with current architecture.

Likely bottlenecks:

- full in-memory canonical state
- full index rebuild on each write
- full snapshot rewrite on each write
- per-query snapshot cloning

### 100M nodes

Not credible with the current implementation model.

### 1B nodes

Not credible at all with the current implementation model.

## Benchmark Coverage Audit

## Create

Status:

- no dedicated measured scalability benchmark found

Risk:

- write amplification makes create cost highly dataset-sensitive

## Update

Status:

- no measured update throughput envelope found

Risk:

- single-record update currently rewrites full snapshots and rebuilds indexes

## Delete

Status:

- no measured delete benchmark found

Risk:

- same persistence amplification pattern as update

## Traversal

Status:

- no multi-scale traversal benchmark suite found

Risk:

- fan-out dominates quickly beyond bounded local traversals

## Shortest Path

Status:

- no multi-scale shortest-path benchmark suite found

Risk:

- bidirectional BFS is correct for hop-count paths, but not yet characterized on dense graphs

## Similarity Search

Status:

- no ANN benchmark suite
- no large vector corpus benchmark suite

Risk:

- vector search is currently linear scan over vector candidates

## Memory Recall / Ranked Retrieval

Status:

- one very small benchmark exists

Risk:

- global candidate scoring and sorting will become expensive as graph size grows

## Architectural Envelope

## Where the current design is likely acceptable

- local single-node development
- correctness-focused product iteration
- bounded traversal on modest datasets
- experimental retrieval workloads

## Where the current design is likely unacceptable

- write-heavy graph workloads
- dense graph workloads
- large vector corpora
- many concurrent transaction sessions
- enterprise-scale graph analytics

## Missing Before Enterprise PoC

- measured ingest throughput at 1M and 10M nodes
- measured update and delete throughput
- measured traversal latencies for 1, 2, 5, and 10 hops
- measured shortest-path behavior on sparse and dense graphs
- measured vector search latencies at scale
- measured ranked retrieval latencies at scale
- measured crash recovery time by dataset size
- measured compaction duration by dataset size
- memory footprint reporting by dataset shape

## Required Benchmark Program

## Dataset dimensions

Every benchmark should vary:

- node count
- edge count
- average degree
- hub concentration
- vector coverage
- vector dimensions
- property payload size

## Scale classes

- Small: 100K nodes
- Medium: 1M nodes
- Large: 10M nodes
- Very Large: 100M nodes
- Aspirational: 1B nodes

## Workload matrix

- create nodes
- update nodes
- delete nodes
- create edges
- update edges
- delete edges
- 1-hop neighbor query
- 2-hop traversal
- 5-hop traversal
- 10-hop traversal
- shortest path
- vector search
- ranked retrieval
- crash recovery
- compaction

## Success criteria

The benchmark program should publish:

- p50, p95, p99 latency
- throughput
- peak RSS
- WAL growth
- snapshot growth
- recovery duration
- compaction duration
- hardware profile

## Production Readiness Verdict

UNDR9 currently does not have a trustworthy scalability envelope for enterprise use.

The present state is:

- strategy exists
- one small benchmark exists
- large-scale evidence does not exist

Without a real benchmark suite, enterprise customers should not be asked to believe scalability claims beyond small experimental deployments.
