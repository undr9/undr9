# ADR 0019: Benchmark Strategy

## Status

Accepted

## Context

The canonical decision set commits UNDR9 to explicit performance targets for:

- indexed query latency
- hybrid retrieval latency
- write throughput
- large graph and memory workloads

The repository already includes benchmark hooks, but the missing architectural decision is how benchmarks should be organized so they guide engineering rather than become incidental micro-measurements.

## Decision

UNDR9 benchmark strategy will focus on workload-level benchmarks, not isolated synthetic micro-optimizations only.

The canonical benchmark suites are:

- node CRUD
- edge CRUD
- adjacency traversal
- bounded graph traversal
- temporal range retrieval
- vector similarity retrieval
- ranked hybrid retrieval
- WAL replay and recovery
- compaction and maintenance operations

Benchmark results should be tied to:

- dataset scale
- retrieval profile version
- storage format version where relevant
- hardware notes for reproducibility

The primary goal is regression detection and architectural validation against target workloads.

## Alternatives Considered

### No formal benchmark strategy until late scale stages

Rejected because performance decisions are already shaping storage, retrieval, and replication boundaries.

### Pure microbenchmark focus

Rejected because operator-visible performance depends on end-to-end workload paths, not just isolated functions.

### Ad hoc local benchmarking only

Rejected because results become non-comparable and hard to use in architectural decisions.

## Consequences

### Positive

- performance work stays aligned to product use cases
- regressions in retrieval, recovery, and maintenance can be detected early
- architecture decisions such as profile changes or index evolution become measurable

### Negative

- benchmark maintenance adds engineering overhead
- workload fixtures must be curated carefully to remain representative

## Future Evolution

- add distributed benchmarks once streaming replication and clustering mature
- separate cold-cache and warm-cache benchmark classes
- publish benchmark baselines per major release and retrieval profile
