# UNDR9 ADR Index

This directory contains the architecture decision records that define the durable product and implementation choices for UNDR9.

## ADR Summary

| ADR | Title | Status | Scope |
| --- | --- | --- | --- |
| `0001` | SDK Scope Is Python Only | Accepted | Official SDK policy |
| `0002` | V1 Transaction Boundary | Accepted | Early transaction interpretation |
| `0003` | Milestone 3 Retrieval Property Conventions | Accepted | Retrieval signal field conventions |
| `0004` | Milestone 5 Snapshot Transactions | Accepted | Explicit transaction model |
| `0005` | Storage Record Format | Accepted | Durable record framing |
| `0006` | Serialization Format | Accepted | API, snapshot, and internal encoding policy |
| `0007` | Segment Layout | Accepted | Canonical on-disk family layout |
| `0008` | WAL Format | Accepted | Durable log envelope and record kinds |
| `0009` | Compaction Strategy | Accepted | Canonical maintenance and pruning model |
| `0010` | Vector Similarity Metric | Accepted | Semantic scoring baseline |
| `0011` | Memory Metadata Namespace | Accepted | Reserved memory property keys |
| `0012` | Snapshot Format | Accepted | Snapshot publication and structure |
| `0013` | API Versioning Strategy | Accepted | Public contract versioning |
| `0014` | Property Type System | Accepted | Typed property model |
| `0015` | Consolidation Audit Trail | Accepted | Memory consolidation observability |
| `0016` | Retrieval Profile Versioning | Accepted | Ranking behavior versioning |
| `0017` | Rust Workspace Structure | Accepted | Repository and crate architecture |
| `0018` | Error Model | Accepted | Internal and public error taxonomy |
| `0019` | Benchmark Strategy | Accepted | Performance validation policy |

## Reading Order

For the fastest architectural onboarding, read in this order:

1. `0001` through `0004` for the original scope and milestone-defining constraints
2. `0005` through `0009` for storage, WAL, and maintenance decisions
3. `0010` through `0016` for retrieval, memory, API, and consolidation decisions
4. `0017` through `0019` for workspace, error, and benchmarking policy

## Canonical Themes

Across the ADR set, the stable architectural themes are:

- append-oriented durability over in-place mutation
- explicit, versioned, inspectable system contracts
- graph-native memory modeling with reserved retrieval metadata
- deterministic ranking and explainable retrieval behavior
- strict separation between single-node core and later distributed evolution
- Python as the only official SDK in the current repository baseline

## Related Documents

- [implementation-roadmap.md](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/docs/implementation-roadmap.md)
- [repository-structure.md](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/docs/repository-structure.md)
- [subsystem-specifications.md](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/docs/subsystem-specifications.md)
