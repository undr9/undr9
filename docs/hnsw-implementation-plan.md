# HNSW Implementation Plan

## Status

Current implementation status:

- Phase 0 complete
- Phase 1 complete
- Phase 2 complete
- Phase 3 complete
- Phase 4 complete
- Phase 5 not started

Implemented so far:

- vector index abstraction introduced with exact and HNSW-backed states
- config surface added for backend selection and HNSW tuning
- `vector_search` now uses backend-provided semantic candidate generation
- HNSW runtime build, exact fallback, persistence, and warm-load are implemented
- `ranked_retrieval` now uses semantic prefilter candidates unioned with structural candidates before final hybrid reranking

Still pending before HNSW can be documented as fully rolled out:

- benchmark evidence for exact versus HNSW latency and recall
- operator-facing documentation updates beyond this plan
- public README performance/architecture updates backed by published evidence

## Goal

Add HNSW-based approximate nearest neighbor indexing to UNDR9 so `vector_search`
and the semantic candidate generation step of `ranked_retrieval` can scale
better than the current exact linear scan.

The implementation must preserve:

- current API request shapes
- exact-mode fallback for correctness and debugging
- storage recovery and rebuild correctness
- existing hybrid final scoring for ranked retrieval

## Current Baseline

Today:

- `vector_search` performs a linear scan over vector-bearing candidates and scores
  them with cosine similarity
- `ranked_retrieval` combines structural, semantic, temporal, importance, and
  confidence signals, but its semantic portion is still exact scan based
- `GraphIndex` stores vector-capable node IDs as a flat `Vec<NodeId>`
- persisted storage keeps vector data, but not a durable ANN sidecar index

Relevant code:

- `crates/query/src/lib.rs`
- `crates/index/src/lib.rs`
- `crates/memory/src/lib.rs`
- `crates/storage/src/lib.rs`
- `crates/api/src/lib.rs`
- `crates/cli/src/main.rs`
- `crates/cli/src/bin/undr9-bench.rs`

## Design Principles

1. Keep the public query surface stable.
2. Add HNSW as candidate generation, not final hybrid scoring.
3. Preserve exact mode as a baseline and fallback path.
4. Make ANN persistence a derived artifact from stored vectors, not a new source of truth.
5. Ship `vector_search` acceleration first, then extend to `ranked_retrieval`.

## Rollout Phases

### Phase 0: Abstraction First

Introduce a vector index abstraction without changing current behavior.

Status: Complete.

Deliverables:

- exact search moved behind a trait or enum-backed abstraction
- no query API changes
- no recall changes

### Phase 1: Exact Backend Refactor

Refactor current exact vector search into a reusable backend inside `crates/index`.

Status: Complete.

Deliverables:

- exact implementation parity
- query integration updated to call the abstraction
- tests proving no regression

### Phase 2: HNSW Build And Search

Add HNSW index construction and ANN search support.

Status: Complete.

Deliverables:

- HNSW build path from stored vectors
- candidate retrieval for `vector_search`
- exact fallback on invalid dimensions or missing index

### Phase 3: Persistence And Rebuild

Persist ANN artifacts so startup does not always require rebuild from scratch.

Status: Complete.

Deliverables:

- index sidecar files under storage layout
- load-on-open when valid
- rebuild fallback when missing or corrupt

### Phase 4: Ranked Retrieval Integration

Use HNSW only for the semantic candidate generation portion of hybrid retrieval.

Status: Complete.

Deliverables:

- semantic top-K candidate prefilter
- existing hybrid rerank preserved
- tests proving acceptable quality versus exact scan

### Phase 5: Benchmarks, Docs, And Hardening

Measure speed, quality, load time, and footprint, then document operational usage.

Status: Pending.

Deliverables:

- benchmark coverage for exact vs HNSW
- recall comparisons
- operator docs
- README and deployment docs updated after evidence is checked in

## File-By-File Engineering Checklist

### `Cargo.toml`

- Add one ANN dependency scoped to the index layer.
- Add a workspace feature such as `hnsw`.
- Keep exact mode buildable without the ANN dependency enabled if possible.

### `crates/config/src/lib.rs`

Add vector index configuration to `AppConfig`.

#### Structs To Add

- `VectorIndexConfig`
- `VectorIndexBackendConfig`

Suggested fields:

- `backend`
- `persist_on_rebuild`
- `exact_fallback_threshold`
- `semantic_top_k`
- `hnsw_m`
- `hnsw_ef_construction`
- `hnsw_ef_search`
- `partition_by_node_type`

#### Functions To Update

- `Default for AppConfig`
- `AppConfig::apply_env_overrides()`

#### Environment Variables To Add

- `UNDR9_VECTOR_INDEX_BACKEND`
- `UNDR9_VECTOR_INDEX_PERSIST_ON_REBUILD`
- `UNDR9_VECTOR_INDEX_EXACT_FALLBACK_THRESHOLD`
- `UNDR9_HNSW_M`
- `UNDR9_HNSW_EF_CONSTRUCTION`
- `UNDR9_HNSW_EF_SEARCH`
- `UNDR9_HNSW_SEMANTIC_TOP_K`
- `UNDR9_HNSW_PARTITION_BY_NODE_TYPE`

### `crates/index/src/lib.rs`

This is the main HNSW implementation point.

#### Existing State To Replace

- `GraphIndex.vector_index: Vec<NodeId>`

#### Structs To Add

- `VectorIndexPartitionKey`
- `VectorIndexState`
- `ExactVectorIndex`
- `HnswVectorIndex`
- `HnswBuildParams`
- `VectorSearchCandidate`
- `VectorIndexSnapshot`

Suggested partition key fields:

- `vector_name`
- `dimension`
- `node_type`

Suggested HNSW state fields:

- `dimension`
- `node_count`
- `bytes_on_disk`
- `params`
- backend-specific graph handle

#### Functions To Add

- `GraphIndex::rebuild_with_config(...)`
- `GraphIndex::vector_search_candidates(...)`
- `GraphIndex::ranked_semantic_candidates(...)`
- `GraphIndex::load_vector_indexes(...)`
- `GraphIndex::persist_vector_indexes(...)`
- `GraphIndex::vector_index_snapshot(...)`

#### Functions To Update

- `GraphIndex::upsert_node(...)`
- `GraphIndex::delete_node(...)`
- `GraphIndex::snapshot()`
- `GraphIndex::rebuild(...)`

#### Notes

- Index only `vectors["default"]` first.
- Partition by dimension.
- Optionally partition by `node_type`.
- Keep exact fallback logic inside this crate.

### `crates/core/src/lib.rs`

Keep vector access consistent and validated.

#### Functions To Add

- `NodeRecord::default_vector() -> Option<&[f32]>`
- `NodeRecord::vector_dimensions() -> Vec<(String, usize)>`
- `NodeRecord::validate_vector_dimensions() -> Result<()>`

#### Notes

- Preserve legacy `embedding` compatibility.
- Do not introduce schema churn beyond what indexing needs.

### `crates/query/src/lib.rs`

This is where query execution starts using ANN candidate generation.

#### Traits To Extend

- `GraphView`

#### Methods To Add

- `vector_search_candidates(...)`
- `ranked_semantic_candidates(...)`

#### Functions To Add

- `semantic_top_k_for_ranked_retrieval(...)`
- `validate_query_vector_dimension(...)`

#### Functions To Update

- `vector_search(...)`
- `ranked_retrieval(...)`
- `ranked_retrieval_candidates(...)` if needed for unioning semantic and structural candidates

#### Rules

- `vector_search` should use HNSW directly when enabled and valid.
- `ranked_retrieval` should use HNSW only to prefilter semantic candidates.
- Final `MemoryRanker::rank(...)` behavior must stay intact.

### `crates/memory/src/lib.rs`

Keep scoring logic stable.

#### Structs To Optionally Add

- `SemanticCandidateTuning`

#### Functions To Keep Stable

- `MemoryRanker::rank(...)`
- `MemoryRanker::cosine_similarity(...)`
- `MemoryRanker::temporal_recency_score(...)`

#### Notes

- Do not embed HNSW implementation here.
- This crate remains the home of hybrid scoring, not ANN graph logic.

### `crates/storage/src/lib.rs`

Persist ANN artifacts as derived sidecars near storage state.

#### Layout Functions To Add

- `StorageLayout::vector_index_dir()`
- `StorageLayout::vector_index_manifest_path()`
- `StorageLayout::vector_index_partition_path(...)`

#### Structs To Add

- `VectorIndexManifest`
- `PersistedVectorIndexPartition`

Suggested manifest fields:

- `format_version`
- `backend`
- `partitions`
- `checksums`
- `build_params`

#### Functions To Add

- `load_vector_index_manifest(...)`
- `persist_vector_index_manifest(...)`

#### Rules

- Stored vectors remain the source of truth.
- HNSW files are rebuildable artifacts.
- Startup should load ANN when valid and rebuild when not.

### `crates/api/src/lib.rs`

Thread vector index config and lifecycle through the database layer.

#### Functions To Update

- `Database::rebuild_indexes()`
- `Database::persist_index_snapshot()`
- `Database::refresh_indexes(...)`

#### Functions To Add

- `Database::load_or_rebuild_vector_indexes(...)`

#### Optional Endpoints

- `GET /v1/admin/indexes/status`

#### Notes

- Reuse `POST /v1/admin/rebuild-indexes`.
- Extend operator-visible snapshots with ANN metadata.

### `crates/cli/src/main.rs`

Expose ANN-aware inspection and tuning flows.

#### Commands To Update

- `RebuildIndexes`
- `ShowDefaultConfig`

#### Commands To Add

- `InspectIndexes`
- `VerifyVectorIndex`

#### Optional Flags For `RebuildIndexes`

- `--vector-backend`
- `--hnsw-m`
- `--hnsw-ef-construction`
- `--hnsw-ef-search`

### `crates/cli/src/bin/undr9-bench.rs`

Benchmark both exact and ANN behavior.

#### Scenarios To Add

- `vector_search_exact`
- `vector_search_hnsw`
- `ranked_retrieval_exact`
- `ranked_retrieval_hnsw`

#### Metrics To Add

- `recall_at_10`
- `recall_at_50`
- `vector_index_build_elapsed_us`
- `vector_index_load_elapsed_us`
- real `index_bytes`

#### Notes

- Compare HNSW against exact scan, not just against itself.
- Keep published evidence separate for exact and ANN runs.

## Test Plan

### `crates/index`

Add tests for:

- HNSW build on uniform dimensions
- exact-vs-HNSW candidate overlap
- partitioning by dimension
- optional partitioning by node type
- deletion/update handling
- exact fallback on unsupported cases

### `crates/query`

Add tests for:

- `vector_search` recall against exact baseline
- ranked retrieval with HNSW semantic prefilter
- dimension mismatch rejection
- semantic prefilter fallback behavior
- time-bounded hybrid retrieval still respecting time filters

### `crates/storage/tests/recovery.rs`

Add tests for:

- startup with valid persisted HNSW sidecar
- rebuild when sidecar is missing
- fallback when sidecar is corrupt
- restore and repair flows rebuilding ANN correctly

### `crates/api`

Add tests for:

- rebuild endpoint reporting extended ANN metadata
- optional admin index-status endpoint
- exact and HNSW modes producing stable API semantics on small fixtures

## Operational Behavior

### Initial Mutation Model

Start with a conservative model:

- rebuild ANN on `rebuild-indexes`
- optionally rebuild on startup fallback
- optionally rebuild on compaction/checkpoint boundaries
- use exact fallback or an exact overlay for recent mutations if needed

Do not start with fully incremental HNSW mutation unless the dependency and
correctness model are well understood.

### Fallback Rules

Fallback to exact search when:

- HNSW is disabled
- dataset is below the configured threshold
- query vector dimension is invalid for the target partition
- ANN files are missing or corrupt
- ANN load fails on startup

## Documentation Tasks

### `docs/api/http.md`

- document exact vs HNSW backend behavior at a high level
- document any new admin inspection endpoint

### `docs/operations/one-node.md`

- add vector index rebuild and recovery notes
- document ANN-related environment variables
- explain when exact mode is preferable

### `deployments/docker/README.md`

- document ANN memory overhead expectations
- mention persistent sidecar index files

### `README.md`

- do not claim HNSW until implemented and benchmarked
- update performance and architecture sections after evidence is published

## Success Criteria

The rollout is successful when:

- `vector_search` latency improves materially at `100k+` and especially `1M+`
- `ranked_retrieval` gets lower latency without unacceptable recall loss
- restart, backup, restore, repair, and rebuild workflows remain correct
- exact mode remains available and tested
- benchmark evidence includes latency, recall, build time, load time, and index bytes

## Recommended Delivery Order

1. Config and abstraction layer with exact parity.
2. HNSW-backed `vector_search`.
3. Persistence and rebuild/load integration.
4. HNSW candidate generation for `ranked_retrieval`.
5. Benchmarks, tests, and documentation hardening.

## Out Of Scope For First Cut

- full incremental HNSW maintenance on every write
- multi-vector-field ANN beyond `default`
- public per-query HNSW tuning knobs in the HTTP API
- replacing hybrid reranking with pure ANN output

## Final Recommendation

Implement HNSW in a way that accelerates semantic candidate generation while
keeping UNDR9's current hybrid retrieval semantics and storage correctness model
intact.

Start small:

- exact backend abstraction first
- `vector_search` second
- hybrid ranked retrieval integration third

That sequence gives measurable performance wins without taking unnecessary
correctness risk early in the rollout.
