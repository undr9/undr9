# ADR 0010: Vector Similarity Metric

## Status

Accepted

## Context

The canonical decision set establishes that:

- UNDR9 does not generate embeddings
- V1 vector indexing is flat
- retrieval must be deterministic and explainable
- ranked retrieval combines semantic, structural, temporal, importance, and confidence signals

The remaining decision is which similarity metric anchors semantic scoring in V1.

## Decision

UNDR9 V1 will use normalized cosine similarity as the canonical vector similarity metric.

The semantic score is computed from the cosine similarity of the stored node embedding and the query vector, then normalized into the `[0,1]` range for direct composition with the other ranking signals.

The database computes this semantic score inside the retrieval engine, not the client.

## Alternatives Considered

### Dot product

Rejected because it is sensitive to vector magnitude and would make cross-model and cross-pipeline behavior less predictable unless upstream normalization were strictly enforced.

### Euclidean distance

Rejected because it is less natural for semantic embedding retrieval in the expected AI-memory workloads and less directly composable with the current ranking model.

### Model-specific custom metrics

Rejected for V1 because they reduce portability and make retrieval semantics harder to reason about.

## Consequences

### Positive

- well-understood semantic retrieval behavior
- naturally compatible with normalized hybrid ranking
- easy to test deterministically
- works with flat vector scans and future ANN indexes

### Negative

- flat cosine search is computationally expensive at larger scales
- some embedding models may benefit from different metrics, but V1 does not expose that variability

## Future Evolution

- allow retrieval profiles to specify alternate metrics for later vector index families
- support ANN backends such as HNSW while keeping cosine as the default semantic contract
- add optional vector normalization enforcement on ingest if future operational experience warrants it
