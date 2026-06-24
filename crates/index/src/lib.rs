use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;

use hnsw_stable::{Cosine, Hnsw, HnswConfig as RuntimeHnswConfig, InMemoryVectorStore};
use im::OrdMap;
use serde::{Deserialize, Serialize};
use undr9_common::{EdgeId, NodeId, Result as Undr9Result, Undr9Error};
use undr9_config::{VectorIndexBackendConfig, VectorIndexConfig};
use undr9_core::{EdgeRecord, NodeRecord, PropertyValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexName {
    NodeId,
    UniqueKey,
    Adjacency,
    ReverseAdjacency,
    LabelType,
    Temporal,
    Vector,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDescriptor {
    pub name: IndexName,
    pub persisted: bool,
    pub rebuildable: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCatalog {
    indexes: Vec<IndexDescriptor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphIndex {
    node_ids: BTreeSet<NodeId>,
    unique_key_index: BTreeMap<String, NodeId>,
    adjacency_index: BTreeMap<NodeId, Vec<EdgeId>>,
    reverse_adjacency_index: BTreeMap<NodeId, Vec<EdgeId>>,
    label_type_index: BTreeMap<String, Vec<NodeId>>,
    temporal_index: BTreeMap<i64, Vec<NodeId>>,
    vector_index: VectorIndexState,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExactVectorIndex {
    candidate_ids_by_name: BTreeMap<String, Vec<NodeId>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorIndexState {
    Exact(ExactVectorIndex),
    Hnsw(HnswVectorIndex),
}

#[derive(Serialize, Deserialize)]
pub struct HnswVectorIndex {
    spaces: BTreeMap<String, HnswVectorSpace>,
    max_nodes: usize,
    semantic_top_k: usize,
    exact_fallback_threshold: usize,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
}

#[derive(Clone, Serialize, Deserialize)]
struct HnswVectorSpace {
    candidate_ids: Vec<NodeId>,
    dimension: Option<usize>,
    #[serde(skip, default)]
    runtime: Option<Arc<HnswRuntime>>,
}

struct HnswRuntime {
    graph: Hnsw<NodeId, Cosine>,
    vectors: InMemoryVectorStore<f32>,
}

impl PartialEq for HnswVectorSpace {
    fn eq(&self, other: &Self) -> bool {
        self.candidate_ids == other.candidate_ids && self.dimension == other.dimension
    }
}

impl Eq for HnswVectorSpace {}

impl std::fmt::Debug for HnswVectorSpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswVectorSpace")
            .field("candidate_count", &self.candidate_ids.len())
            .field("dimension", &self.dimension)
            .field("runtime_ready", &self.runtime.is_some())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSnapshot {
    pub format_version: u16,
    pub node_count: usize,
    pub unique_key_count: usize,
    pub adjacency_key_count: usize,
    pub reverse_adjacency_key_count: usize,
    pub label_bucket_count: usize,
    pub temporal_bucket_count: usize,
    pub vector_space_count: usize,
    pub vector_candidate_count: usize,
    pub vector_backend: String,
    pub vector_runtime_ready: bool,
    pub vector_dimensions: BTreeMap<String, usize>,
}

pub struct VectorIndexLoadConfig<'a> {
    pub manifest_path: &'a Path,
    pub graph_path: &'a Path,
    pub vectors_path: &'a Path,
    pub expected_last_applied_lsn: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedVectorIndexManifest {
    format_version: u16,
    backend: String,
    last_applied_lsn: Option<u64>,
    max_nodes: usize,
    semantic_top_k: usize,
    exact_fallback_threshold: usize,
    hnsw_m: usize,
    hnsw_ef_construction: usize,
    hnsw_ef_search: usize,
    vector_spaces: Vec<PersistedVectorSpaceManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedVectorSpaceManifest {
    vector_name: String,
    vector_candidate_count: usize,
    dimension: usize,
}

impl Default for VectorIndexState {
    fn default() -> Self {
        Self::Exact(ExactVectorIndex::default())
    }
}

impl IndexCatalog {
    pub fn v1_defaults() -> Self {
        Self {
            indexes: vec![
                IndexDescriptor {
                    name: IndexName::NodeId,
                    persisted: true,
                    rebuildable: true,
                },
                IndexDescriptor {
                    name: IndexName::UniqueKey,
                    persisted: true,
                    rebuildable: true,
                },
                IndexDescriptor {
                    name: IndexName::Adjacency,
                    persisted: true,
                    rebuildable: true,
                },
                IndexDescriptor {
                    name: IndexName::ReverseAdjacency,
                    persisted: true,
                    rebuildable: true,
                },
                IndexDescriptor {
                    name: IndexName::LabelType,
                    persisted: true,
                    rebuildable: true,
                },
                IndexDescriptor {
                    name: IndexName::Temporal,
                    persisted: false,
                    rebuildable: true,
                },
                IndexDescriptor {
                    name: IndexName::Vector,
                    persisted: false,
                    rebuildable: true,
                },
            ],
        }
    }

    pub fn all(&self) -> &[IndexDescriptor] {
        &self.indexes
    }
}

impl GraphIndex {
    pub fn rebuild(nodes: &[NodeRecord], edges: &[EdgeRecord]) -> Self {
        Self::rebuild_with_config(nodes, edges, &VectorIndexConfig::default())
    }

    pub fn rebuild_with_config(
        nodes: &[NodeRecord],
        edges: &[EdgeRecord],
        config: &VectorIndexConfig,
    ) -> Self {
        Self::rebuild_internal(nodes, edges, config, None)
    }

    pub fn rebuild_with_config_and_vector_index_load(
        nodes: &[NodeRecord],
        edges: &[EdgeRecord],
        config: &VectorIndexConfig,
        load_config: Option<VectorIndexLoadConfig<'_>>,
    ) -> Self {
        Self::rebuild_internal(nodes, edges, config, load_config.as_ref())
    }

    fn rebuild_internal(
        nodes: &[NodeRecord],
        edges: &[EdgeRecord],
        config: &VectorIndexConfig,
        load_config: Option<&VectorIndexLoadConfig<'_>>,
    ) -> Self {
        let mut index = Self::default();
        index.vector_index = VectorIndexState::from_config(config, nodes);

        for node in nodes {
            index.upsert_node(node);
        }

        for edge in edges {
            index.upsert_edge(edge);
        }

        let loaded_runtime = load_config
            .and_then(|load_config| {
                index
                    .vector_index
                    .try_load_runtime(load_config)
                    .map_or(None, Some)
            })
            .unwrap_or(false);
        if !loaded_runtime {
            index.vector_index.initialize_runtime_from_nodes(nodes);
        }

        index
    }

    pub fn upsert_node(&mut self, node: &NodeRecord) {
        self.node_ids.insert(node.id.clone());
        push_unique(
            self.label_type_index
                .entry(node.node_type.clone())
                .or_default(),
            node.id.clone(),
        );
        if let Some(timestamp_ms) = node.timestamp_ms() {
            push_unique(
                self.temporal_index.entry(timestamp_ms).or_default(),
                node.id.clone(),
            );
        }
        self.vector_index.upsert_node(node);

        if let Some(PropertyValue::String(unique_key)) = node.properties.get("unique_key") {
            self.unique_key_index
                .insert(unique_key.clone(), node.id.clone());
        }
    }

    pub fn delete_node(&mut self, node: &NodeRecord) {
        self.node_ids.remove(&node.id);
        remove_from_bucket(&mut self.label_type_index, &node.node_type, &node.id);
        if let Some(timestamp_ms) = node.timestamp_ms() {
            remove_from_bucket(&mut self.temporal_index, &timestamp_ms, &node.id);
        }
        self.vector_index.delete_node(node);
        if let Some(PropertyValue::String(unique_key)) = node.properties.get("unique_key") {
            if self.unique_key_index.get(unique_key) == Some(&node.id) {
                self.unique_key_index.remove(unique_key);
            }
        }
    }

    pub fn upsert_edge(&mut self, edge: &EdgeRecord) {
        push_unique(
            self.adjacency_index.entry(edge.source.clone()).or_default(),
            edge.id.clone(),
        );
        push_unique(
            self.reverse_adjacency_index
                .entry(edge.target.clone())
                .or_default(),
            edge.id.clone(),
        );
    }

    pub fn delete_edge(&mut self, edge: &EdgeRecord) {
        remove_from_bucket(&mut self.adjacency_index, &edge.source, &edge.id);
        remove_from_bucket(&mut self.reverse_adjacency_index, &edge.target, &edge.id);
    }

    pub fn contains_node(&self, node_id: &NodeId) -> bool {
        self.node_ids.contains(node_id)
    }

    pub fn lookup_unique_key(&self, unique_key: &str) -> Option<&NodeId> {
        self.unique_key_index.get(unique_key)
    }

    pub fn node_ids_by_type(&self, node_type: &str) -> &[NodeId] {
        self.label_type_index
            .get(node_type)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn edge_ids_for(
        &self,
        node_id: &NodeId,
        direction: EdgeDirection,
        edges: &impl EdgeRecordLookup,
        edge_type: Option<&str>,
    ) -> Vec<EdgeId> {
        self.edge_ids_for_iter(node_id, direction, edges, edge_type)
            .cloned()
            .collect()
    }

    pub fn edge_ids_for_iter<'a>(
        &'a self,
        node_id: &'a NodeId,
        direction: EdgeDirection,
        edges: &'a impl EdgeRecordLookup,
        edge_type: Option<&'a str>,
    ) -> Box<dyn Iterator<Item = &'a EdgeId> + 'a> {
        let outgoing = if matches!(direction, EdgeDirection::Outgoing | EdgeDirection::Both) {
            self.adjacency_index
                .get(node_id)
                .map(Vec::as_slice)
                .unwrap_or(&[])
        } else {
            &[]
        };
        let incoming = if matches!(direction, EdgeDirection::Incoming | EdgeDirection::Both) {
            self.reverse_adjacency_index
                .get(node_id)
                .map(Vec::as_slice)
                .unwrap_or(&[])
        } else {
            &[]
        };

        Box::new(
            outgoing
                .iter()
                .chain(incoming.iter())
                .filter(move |edge_id| {
                    edge_type
                        .map(|value| {
                            edges
                                .get_edge(edge_id)
                                .map(|edge| edge.edge_type == value)
                                .unwrap_or(false)
                        })
                        .unwrap_or(true)
                }),
        )
    }

    pub fn node_ids_in_time_range(&self, from_epoch_ms: i64, to_epoch_ms: i64) -> Vec<NodeId> {
        self.node_ids_in_time_range_iter(from_epoch_ms, to_epoch_ms)
            .cloned()
            .collect()
    }

    pub fn node_ids_in_time_range_iter(
        &self,
        from_epoch_ms: i64,
        to_epoch_ms: i64,
    ) -> impl Iterator<Item = &NodeId> {
        self.temporal_index
            .range(from_epoch_ms..=to_epoch_ms)
            .flat_map(|(_, node_ids)| node_ids.iter())
    }

    pub fn vector_candidate_ids_iter(
        &self,
        vector_name: &str,
    ) -> Box<dyn Iterator<Item = &NodeId> + '_> {
        self.vector_index.candidate_ids_iter(vector_name)
    }

    pub fn semantic_candidate_ids(
        &self,
        query_vector: &[f32],
        vector_name: &str,
        node_type: Option<&str>,
        limit: usize,
        top_k_override: Option<usize>,
    ) -> Vec<NodeId> {
        if limit == 0 {
            return Vec::new();
        }

        let allowed_ids = node_type.and_then(|node_type| {
            self.label_type_index
                .get(node_type)
                .map(|node_ids| node_ids.iter().cloned().collect::<BTreeSet<_>>())
        });

        self.vector_index
            .semantic_candidate_ids(
                query_vector,
                vector_name,
                limit,
                top_k_override,
                allowed_ids.as_ref(),
            )
            .unwrap_or_else(|| self.exact_semantic_candidate_ids(vector_name, allowed_ids.as_ref()))
    }

    pub fn snapshot(&self) -> IndexSnapshot {
        IndexSnapshot {
            format_version: 1,
            node_count: self.node_ids.len(),
            unique_key_count: self.unique_key_index.len(),
            adjacency_key_count: self.adjacency_index.len(),
            reverse_adjacency_key_count: self.reverse_adjacency_index.len(),
            label_bucket_count: self.label_type_index.len(),
            temporal_bucket_count: self.temporal_index.len(),
            vector_space_count: self.vector_index.space_count(),
            vector_candidate_count: self.vector_index.len(),
            vector_backend: self.vector_index.backend_name().to_owned(),
            vector_runtime_ready: self.vector_index.runtime_ready(),
            vector_dimensions: self.vector_index.dimensions(),
        }
    }

    pub fn persist_vector_index(
        &self,
        manifest_path: &Path,
        graph_path: &Path,
        vectors_path: &Path,
        last_applied_lsn: Option<u64>,
    ) -> Undr9Result<()> {
        self.vector_index
            .persist(manifest_path, graph_path, vectors_path, last_applied_lsn)
    }

    fn exact_semantic_candidate_ids(
        &self,
        vector_name: &str,
        allowed_ids: Option<&BTreeSet<NodeId>>,
    ) -> Vec<NodeId> {
        self.vector_index
            .candidate_ids_iter(vector_name)
            .filter(|node_id| {
                allowed_ids
                    .map(|allowed_ids| allowed_ids.contains(*node_id))
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    }
}

impl ExactVectorIndex {
    fn upsert_node(&mut self, node: &NodeRecord) {
        for (vector_name, candidate_ids) in &mut self.candidate_ids_by_name {
            if node.vector(vector_name).is_some() {
                push_unique(candidate_ids, node.id.clone());
            } else {
                candidate_ids.retain(|node_id| node_id != &node.id);
            }
        }
        self.candidate_ids_by_name
            .retain(|_, candidate_ids| !candidate_ids.is_empty());
        for vector_name in node.vectors.keys() {
            push_unique(
                self.candidate_ids_by_name
                    .entry(vector_name.clone())
                    .or_default(),
                node.id.clone(),
            );
        }
    }

    fn delete_node(&mut self, node: &NodeRecord) {
        for candidate_ids in self.candidate_ids_by_name.values_mut() {
            candidate_ids.retain(|node_id| node_id != &node.id);
        }
        self.candidate_ids_by_name
            .retain(|_, candidate_ids| !candidate_ids.is_empty());
    }

    fn candidate_ids_iter(&self, vector_name: &str) -> Box<dyn Iterator<Item = &NodeId> + '_> {
        match self.candidate_ids_by_name.get(vector_name) {
            Some(candidate_ids) => Box::new(candidate_ids.iter()),
            None => Box::new(std::iter::empty()),
        }
    }

    fn len(&self) -> usize {
        self.candidate_ids_by_name
            .values()
            .map(Vec::len)
            .sum::<usize>()
    }

    fn space_count(&self) -> usize {
        self.candidate_ids_by_name.len()
    }
}

impl Clone for HnswVectorIndex {
    fn clone(&self) -> Self {
        Self {
            spaces: self.spaces.clone(),
            max_nodes: self.max_nodes,
            semantic_top_k: self.semantic_top_k,
            exact_fallback_threshold: self.exact_fallback_threshold,
            m: self.m,
            ef_construction: self.ef_construction,
            ef_search: self.ef_search,
        }
    }
}

impl std::fmt::Debug for HnswVectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswVectorIndex")
            .field("space_count", &self.spaces.len())
            .field(
                "candidate_count",
                &self
                    .spaces
                    .values()
                    .map(|space| space.candidate_ids.len())
                    .sum::<usize>(),
            )
            .field("dimensions", &self.dimensions())
            .field("max_nodes", &self.max_nodes)
            .field("semantic_top_k", &self.semantic_top_k)
            .field("exact_fallback_threshold", &self.exact_fallback_threshold)
            .field("m", &self.m)
            .field("ef_construction", &self.ef_construction)
            .field("ef_search", &self.ef_search)
            .field("runtime_ready", &self.runtime_ready())
            .finish()
    }
}

impl PartialEq for HnswVectorIndex {
    fn eq(&self, other: &Self) -> bool {
        self.spaces == other.spaces
            && self.max_nodes == other.max_nodes
            && self.semantic_top_k == other.semantic_top_k
            && self.exact_fallback_threshold == other.exact_fallback_threshold
            && self.m == other.m
            && self.ef_construction == other.ef_construction
            && self.ef_search == other.ef_search
    }
}

impl Eq for HnswVectorIndex {}

impl HnswVectorIndex {
    fn build(nodes: &[NodeRecord], config: &VectorIndexConfig) -> Self {
        Self {
            spaces: hnsw_spaces_from_nodes(nodes),
            max_nodes: nodes.len().saturating_add(1_024).max(1),
            semantic_top_k: config.semantic_top_k,
            exact_fallback_threshold: config.exact_fallback_threshold,
            m: config.hnsw_m,
            ef_construction: config.hnsw_ef_construction,
            ef_search: config.hnsw_ef_search,
        }
    }

    fn upsert_node(&mut self, node: &NodeRecord) {
        for (vector_name, space) in &mut self.spaces {
            space.upsert_node(vector_name, node);
        }
        for vector_name in node.vectors.keys() {
            self.spaces
                .entry(vector_name.clone())
                .or_insert_with(|| HnswVectorSpace::new(node.vector(vector_name).map(|v| v.len())))
                .upsert_node(vector_name, node);
        }
        self.spaces
            .retain(|_, space| !space.candidate_ids.is_empty());
    }

    fn delete_node(&mut self, node: &NodeRecord) {
        for space in self.spaces.values_mut() {
            space.delete_node(node);
        }
        self.spaces
            .retain(|_, space| !space.candidate_ids.is_empty());
    }

    fn candidate_ids_iter(&self, vector_name: &str) -> Box<dyn Iterator<Item = &NodeId> + '_> {
        match self.spaces.get(vector_name) {
            Some(space) => Box::new(space.candidate_ids.iter()),
            None => Box::new(std::iter::empty()),
        }
    }

    fn len(&self) -> usize {
        self.spaces
            .values()
            .map(|space| space.candidate_ids.len())
            .sum::<usize>()
    }

    fn space_count(&self) -> usize {
        self.spaces.len()
    }

    fn semantic_candidate_ids(
        &self,
        query_vector: &[f32],
        vector_name: &str,
        limit: usize,
        top_k_override: Option<usize>,
        allowed_ids: Option<&BTreeSet<NodeId>>,
    ) -> Option<Vec<NodeId>> {
        let space = self.spaces.get(vector_name)?;
        let runtime = space.runtime.as_ref()?;
        let dimension = space.dimension?;
        if limit == 0
            || space.candidate_ids.len() <= self.exact_fallback_threshold
            || query_vector.len() != dimension
        {
            return None;
        }

        let candidate_limit = limit
            .max(top_k_override.unwrap_or(self.semantic_top_k))
            .min(space.candidate_ids.len());
        let hits = match allowed_ids {
            Some(allowed_ids) => runtime.graph.search(
                &runtime.vectors,
                query_vector,
                candidate_limit,
                Some(&|node_id: &NodeId| allowed_ids.contains(node_id)),
            ),
            None => runtime
                .graph
                .search(&runtime.vectors, query_vector, candidate_limit, None),
        }
        .ok()?;

        Some(hits.into_iter().map(|hit| hit.key).collect())
    }

    fn initialize_runtime_from_nodes(&mut self, nodes: &[NodeRecord]) {
        for (vector_name, space) in &mut self.spaces {
            if space.runtime.is_none() {
                space.runtime = space.build_runtime(
                    vector_name,
                    nodes,
                    self.max_nodes,
                    self.exact_fallback_threshold,
                    self.m,
                    self.ef_construction,
                    self.ef_search,
                );
            }
        }
    }

    fn try_load_runtime(&mut self, load: &VectorIndexLoadConfig<'_>) -> Undr9Result<bool> {
        if !load.manifest_path.exists() {
            return Ok(false);
        }

        let manifest = read_vector_index_manifest(load.manifest_path)?;
        if !self.matches_manifest(&manifest, load.expected_last_applied_lsn) {
            return Ok(false);
        }

        for space_manifest in &manifest.vector_spaces {
            let Some(space) = self.spaces.get_mut(&space_manifest.vector_name) else {
                return Ok(false);
            };
            let graph_path = vector_space_sidecar_path(
                load.graph_path,
                &space_manifest.vector_name,
                ".hnsw.bin",
            );
            let vectors_path = vector_space_sidecar_path(
                load.vectors_path,
                &space_manifest.vector_name,
                ".vectors.bin",
            );
            if !graph_path.exists() || !vectors_path.exists() {
                return Ok(false);
            }
            let graph = {
                let file = File::open(&graph_path).map_err(|error| {
                    Undr9Error::Io(format!(
                        "failed to open vector index graph '{}': {error}",
                        graph_path.display()
                    ))
                })?;
                let mut reader = BufReader::new(file);
                Hnsw::load_from(Cosine::new(), &mut reader).map_err(|error| {
                    Undr9Error::Corruption(format!(
                        "failed to load HNSW graph '{}': {error}",
                        graph_path.display()
                    ))
                })?
            };
            let vectors = {
                let file = File::open(&vectors_path).map_err(|error| {
                    Undr9Error::Io(format!(
                        "failed to open vector index vectors '{}': {error}",
                        vectors_path.display()
                    ))
                })?;
                let mut reader = BufReader::new(file);
                let (vectors, _) =
                    InMemoryVectorStore::<f32>::load_from(&mut reader).map_err(|error| {
                        Undr9Error::Corruption(format!(
                            "failed to load vector store '{}': {error}",
                            vectors_path.display()
                        ))
                    })?;
                vectors
            };
            space.runtime = Some(Arc::new(HnswRuntime { graph, vectors }));
        }
        Ok(true)
    }

    fn persist(
        &self,
        manifest_path: &Path,
        graph_path: &Path,
        vectors_path: &Path,
        last_applied_lsn: Option<u64>,
    ) -> Undr9Result<()> {
        if self.spaces.is_empty() {
            cleanup_vector_index_sidecars(manifest_path, graph_path, vectors_path)?;
            return Ok(());
        }

        for path in [manifest_path, graph_path, vectors_path] {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|error| {
                    Undr9Error::Io(format!(
                        "failed to create vector index directory '{}': {error}",
                        parent.display()
                    ))
                })?;
            }
        }

        cleanup_matching_sidecars(graph_path, ".hnsw.bin")?;
        cleanup_matching_sidecars(vectors_path, ".vectors.bin")?;

        let mut vector_spaces = Vec::new();
        for (vector_name, space) in &self.spaces {
            let Some(runtime) = space.runtime.as_ref() else {
                continue;
            };
            let Some(dimension) = space.dimension else {
                continue;
            };
            let graph_space_path = vector_space_sidecar_path(graph_path, vector_name, ".hnsw.bin");
            let vectors_space_path =
                vector_space_sidecar_path(vectors_path, vector_name, ".vectors.bin");
            {
                let file = File::create(&graph_space_path).map_err(|error| {
                    Undr9Error::Io(format!(
                        "failed to create vector index graph '{}': {error}",
                        graph_space_path.display()
                    ))
                })?;
                let mut writer = BufWriter::new(file);
                runtime.graph.save_to(&mut writer).map_err(|error| {
                    Undr9Error::Serialization(format!(
                        "failed to persist HNSW graph '{}': {error}",
                        graph_space_path.display()
                    ))
                })?;
            }
            {
                let file = File::create(&vectors_space_path).map_err(|error| {
                    Undr9Error::Io(format!(
                        "failed to create vector store '{}': {error}",
                        vectors_space_path.display()
                    ))
                })?;
                let mut writer = BufWriter::new(file);
                runtime
                    .vectors
                    .save_to(&mut writer, runtime.graph.len())
                    .map_err(|error| {
                        Undr9Error::Serialization(format!(
                            "failed to persist vector store '{}': {error}",
                            vectors_space_path.display()
                        ))
                    })?;
            }
            vector_spaces.push(PersistedVectorSpaceManifest {
                vector_name: vector_name.clone(),
                vector_candidate_count: space.candidate_ids.len(),
                dimension,
            });
        }

        if vector_spaces.is_empty() {
            cleanup_vector_index_sidecars(manifest_path, graph_path, vectors_path)?;
            return Ok(());
        }

        write_vector_index_manifest(
            manifest_path,
            &PersistedVectorIndexManifest {
                format_version: 1,
                backend: "hnsw".to_owned(),
                last_applied_lsn,
                max_nodes: self.max_nodes,
                semantic_top_k: self.semantic_top_k,
                exact_fallback_threshold: self.exact_fallback_threshold,
                hnsw_m: self.m,
                hnsw_ef_construction: self.ef_construction,
                hnsw_ef_search: self.ef_search,
                vector_spaces,
            },
        )?;
        Ok(())
    }

    fn matches_manifest(
        &self,
        manifest: &PersistedVectorIndexManifest,
        expected_last_applied_lsn: Option<u64>,
    ) -> bool {
        manifest.format_version == 1
            && manifest.backend == "hnsw"
            && manifest.last_applied_lsn == expected_last_applied_lsn
            && manifest.max_nodes == self.max_nodes
            && manifest.semantic_top_k == self.semantic_top_k
            && manifest.exact_fallback_threshold == self.exact_fallback_threshold
            && manifest.hnsw_m == self.m
            && manifest.hnsw_ef_construction == self.ef_construction
            && manifest.hnsw_ef_search == self.ef_search
            && manifest.vector_spaces.len() == self.spaces.len()
            && manifest.vector_spaces.iter().all(|space_manifest| {
                self.spaces
                    .get(&space_manifest.vector_name)
                    .map(|space| {
                        Some(space_manifest.dimension) == space.dimension
                            && space_manifest.vector_candidate_count == space.candidate_ids.len()
                    })
                    .unwrap_or(false)
            })
    }

    fn runtime_ready(&self) -> bool {
        self.spaces.values().all(|space| {
            space.runtime.is_some() || space.candidate_ids.len() <= self.exact_fallback_threshold
        })
    }

    fn dimensions(&self) -> BTreeMap<String, usize> {
        self.spaces
            .iter()
            .filter_map(|(vector_name, space)| {
                space
                    .dimension
                    .map(|dimension| (vector_name.clone(), dimension))
            })
            .collect()
    }
}

impl VectorIndexState {
    fn from_config(config: &VectorIndexConfig, nodes: &[NodeRecord]) -> Self {
        match config.backend {
            VectorIndexBackendConfig::Exact => Self::Exact(ExactVectorIndex::default()),
            VectorIndexBackendConfig::Hnsw => Self::Hnsw(HnswVectorIndex::build(nodes, config)),
        }
    }

    fn upsert_node(&mut self, node: &NodeRecord) {
        match self {
            Self::Exact(index) => index.upsert_node(node),
            Self::Hnsw(index) => index.upsert_node(node),
        }
    }

    fn delete_node(&mut self, node: &NodeRecord) {
        match self {
            Self::Exact(index) => index.delete_node(node),
            Self::Hnsw(index) => index.delete_node(node),
        }
    }

    fn candidate_ids_iter(&self, vector_name: &str) -> Box<dyn Iterator<Item = &NodeId> + '_> {
        match self {
            Self::Exact(index) => index.candidate_ids_iter(vector_name),
            Self::Hnsw(index) => index.candidate_ids_iter(vector_name),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Exact(index) => index.len(),
            Self::Hnsw(index) => index.len(),
        }
    }

    fn semantic_candidate_ids(
        &self,
        query_vector: &[f32],
        vector_name: &str,
        limit: usize,
        top_k_override: Option<usize>,
        allowed_ids: Option<&BTreeSet<NodeId>>,
    ) -> Option<Vec<NodeId>> {
        match self {
            Self::Exact(_) => None,
            Self::Hnsw(index) => index.semantic_candidate_ids(
                query_vector,
                vector_name,
                limit,
                top_k_override,
                allowed_ids,
            ),
        }
    }

    fn try_load_runtime(&mut self, load: &VectorIndexLoadConfig<'_>) -> Undr9Result<bool> {
        match self {
            Self::Exact(_) => Ok(false),
            Self::Hnsw(index) => index.try_load_runtime(load),
        }
    }

    fn initialize_runtime_from_nodes(&mut self, nodes: &[NodeRecord]) {
        match self {
            Self::Exact(_) => {}
            Self::Hnsw(index) => index.initialize_runtime_from_nodes(nodes),
        }
    }

    fn persist(
        &self,
        manifest_path: &Path,
        graph_path: &Path,
        vectors_path: &Path,
        last_applied_lsn: Option<u64>,
    ) -> Undr9Result<()> {
        match self {
            Self::Exact(_) => {
                cleanup_vector_index_sidecars(manifest_path, graph_path, vectors_path)
            }
            Self::Hnsw(index) => {
                index.persist(manifest_path, graph_path, vectors_path, last_applied_lsn)
            }
        }
    }

    fn backend_name(&self) -> &'static str {
        match self {
            Self::Exact(_) => "exact",
            Self::Hnsw(_) => "hnsw",
        }
    }

    fn runtime_ready(&self) -> bool {
        match self {
            Self::Exact(_) => true,
            Self::Hnsw(index) => index.runtime_ready(),
        }
    }

    fn dimensions(&self) -> BTreeMap<String, usize> {
        match self {
            Self::Exact(_) => BTreeMap::new(),
            Self::Hnsw(index) => index.dimensions(),
        }
    }

    fn space_count(&self) -> usize {
        match self {
            Self::Exact(index) => index.space_count(),
            Self::Hnsw(index) => index.space_count(),
        }
    }
}

impl HnswVectorSpace {
    fn new(dimension: Option<usize>) -> Self {
        Self {
            candidate_ids: Vec::new(),
            dimension,
            runtime: None,
        }
    }

    fn upsert_node(&mut self, vector_name: &str, node: &NodeRecord) {
        match (node.vector(vector_name), self.dimension) {
            (Some(vector), Some(dimension)) if vector.len() == dimension => {
                push_unique(&mut self.candidate_ids, node.id.clone());
                if let Some(runtime) = self.runtime.as_ref() {
                    if runtime
                        .graph
                        .set(&runtime.vectors, node.id.clone(), vector)
                        .is_err()
                    {
                        self.runtime = None;
                    }
                }
            }
            (Some(vector), None) => {
                self.dimension = Some(vector.len());
                push_unique(&mut self.candidate_ids, node.id.clone());
                self.runtime = None;
            }
            (Some(_), Some(_)) => {
                self.candidate_ids.retain(|node_id| node_id != &node.id);
                if let Some(runtime) = self.runtime.as_ref() {
                    let _ = runtime.graph.delete(&node.id);
                }
                self.runtime = None;
            }
            (None, _) => {
                self.delete_node(node);
            }
        }
    }

    fn delete_node(&mut self, node: &NodeRecord) {
        self.candidate_ids.retain(|node_id| node_id != &node.id);
        if let Some(runtime) = self.runtime.as_ref() {
            let _ = runtime.graph.delete(&node.id);
        }
    }

    fn build_runtime(
        &self,
        vector_name: &str,
        nodes: &[NodeRecord],
        max_nodes: usize,
        exact_fallback_threshold: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Option<Arc<HnswRuntime>> {
        let dimension = self.dimension?;
        if dimension == 0 || self.candidate_ids.len() <= exact_fallback_threshold {
            return None;
        }
        if nodes
            .iter()
            .filter_map(|node| node.vector(vector_name))
            .any(|vector| vector.len() != dimension)
        {
            return None;
        }

        let config = RuntimeHnswConfig::new(dimension, max_nodes)
            .m(m)
            .ef_construction(ef_construction)
            .ef_search(ef_search);
        let graph = Hnsw::new(Cosine::new(), config);
        let vectors = InMemoryVectorStore::new(dimension, max_nodes);
        for node in nodes {
            if let Some(vector) = node.vector(vector_name) {
                graph.insert(&vectors, node.id.clone(), vector).ok()?;
            }
        }
        Some(Arc::new(HnswRuntime { graph, vectors }))
    }
}

fn hnsw_spaces_from_nodes(nodes: &[NodeRecord]) -> BTreeMap<String, HnswVectorSpace> {
    let mut vectors_by_name = BTreeMap::<String, Vec<&[f32]>>::new();
    let mut candidate_ids_by_name = BTreeMap::<String, Vec<NodeId>>::new();
    for node in nodes {
        for (vector_name, vector) in &node.vectors {
            vectors_by_name
                .entry(vector_name.clone())
                .or_default()
                .push(vector.as_slice());
            push_unique(
                candidate_ids_by_name
                    .entry(vector_name.clone())
                    .or_default(),
                node.id.clone(),
            );
        }
    }

    candidate_ids_by_name
        .into_iter()
        .map(|(vector_name, candidate_ids)| {
            let dimension = shared_dimension(
                vectors_by_name
                    .get(&vector_name)
                    .into_iter()
                    .flatten()
                    .copied(),
            );
            (
                vector_name,
                HnswVectorSpace {
                    candidate_ids,
                    dimension,
                    runtime: None,
                },
            )
        })
        .collect()
}

fn shared_dimension<'a>(vectors: impl Iterator<Item = &'a [f32]>) -> Option<usize> {
    let mut dimension = None;
    for vector in vectors {
        match dimension {
            Some(existing) if existing != vector.len() => return None,
            Some(_) => {}
            None => dimension = Some(vector.len()),
        }
    }
    dimension
}

fn read_vector_index_manifest(path: &Path) -> Undr9Result<PersistedVectorIndexManifest> {
    let file = File::open(path).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to open vector index manifest '{}': {error}",
            path.display()
        ))
    })?;
    serde_json::from_reader(BufReader::new(file)).map_err(|error| {
        Undr9Error::Corruption(format!(
            "failed to deserialize vector index manifest '{}': {error}",
            path.display()
        ))
    })
}

fn write_vector_index_manifest(
    path: &Path,
    manifest: &PersistedVectorIndexManifest,
) -> Undr9Result<()> {
    let file = File::create(path).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to create vector index manifest '{}': {error}",
            path.display()
        ))
    })?;
    serde_json::to_writer_pretty(BufWriter::new(file), manifest).map_err(|error| {
        Undr9Error::Serialization(format!(
            "failed to serialize vector index manifest '{}': {error}",
            path.display()
        ))
    })
}

fn cleanup_vector_index_sidecars(
    manifest_path: &Path,
    graph_path: &Path,
    vectors_path: &Path,
) -> Undr9Result<()> {
    cleanup_matching_sidecars(graph_path, ".hnsw.bin")?;
    cleanup_matching_sidecars(vectors_path, ".vectors.bin")?;
    for path in [manifest_path, graph_path, vectors_path] {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Undr9Error::Io(format!(
                    "failed to remove vector index sidecar '{}': {error}",
                    path.display()
                )))
            }
        }
    }
    Ok(())
}

fn cleanup_matching_sidecars(base_path: &Path, suffix: &str) -> Undr9Result<()> {
    let Some(parent) = base_path.parent() else {
        return Ok(());
    };
    let Some(file_name) = base_path.file_name().and_then(|name| name.to_str()) else {
        return Ok(());
    };
    let prefix = file_name.strip_suffix(suffix).unwrap_or(file_name);
    let entries = match fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(Undr9Error::Io(format!(
                "failed to read vector index directory '{}': {error}",
                parent.display()
            )))
        }
    };

    for entry in entries {
        let entry = entry.map_err(|error| {
            Undr9Error::Io(format!(
                "failed to inspect vector index directory '{}': {error}",
                parent.display()
            ))
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let matches_named_sidecar =
            name.starts_with(&format!("{prefix}.")) && name.ends_with(suffix);
        if name == file_name || matches_named_sidecar {
            match fs::remove_file(entry.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(Undr9Error::Io(format!(
                        "failed to remove vector index sidecar '{}': {error}",
                        entry.path().display()
                    )))
                }
            }
        }
    }
    Ok(())
}

fn vector_space_sidecar_path(
    base_path: &Path,
    vector_name: &str,
    suffix: &str,
) -> std::path::PathBuf {
    let parent = base_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();
    let file_name = base_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let prefix = file_name.strip_suffix(suffix).unwrap_or(file_name);
    parent.join(format!(
        "{prefix}.{}{suffix}",
        sanitize_vector_name(vector_name)
    ))
}

fn sanitize_vector_name(vector_name: &str) -> String {
    let mut sanitized = String::with_capacity(vector_name.len().max(1));
    for ch in vector_name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() {
        "default".to_owned()
    } else {
        sanitized
    }
}

pub trait EdgeRecordLookup {
    fn get_edge(&self, edge_id: &EdgeId) -> Option<&EdgeRecord>;
}

impl EdgeRecordLookup for BTreeMap<EdgeId, EdgeRecord> {
    fn get_edge(&self, edge_id: &EdgeId) -> Option<&EdgeRecord> {
        self.get(edge_id)
    }
}

impl EdgeRecordLookup for OrdMap<EdgeId, EdgeRecord> {
    fn get_edge(&self, edge_id: &EdgeId) -> Option<&EdgeRecord> {
        self.get(edge_id)
    }
}

fn push_unique<T>(values: &mut Vec<T>, value: T)
where
    T: PartialEq,
{
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn remove_from_bucket<K, V>(index: &mut BTreeMap<K, Vec<V>>, key: &K, value: &V)
where
    K: Ord + Clone,
    V: PartialEq,
{
    let should_remove = if let Some(values) = index.get_mut(key) {
        values.retain(|existing| existing != value);
        values.is_empty()
    } else {
        false
    };

    if should_remove {
        index.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::VectorIndexLoadConfig;
    use super::{EdgeDirection, GraphIndex, IndexCatalog, IndexName, VectorIndexState};
    use tempfile::tempdir;
    use undr9_common::{EdgeId, NodeId};
    use undr9_config::{VectorIndexBackendConfig, VectorIndexConfig};
    use undr9_core::{EdgeRecord, NodeRecord, PropertyValue};

    #[test]
    fn v1_catalog_contains_required_primary_indexes() {
        let catalog = IndexCatalog::v1_defaults();

        assert!(catalog
            .all()
            .iter()
            .any(|index| index.name == IndexName::NodeId));
        assert!(catalog
            .all()
            .iter()
            .any(|index| index.name == IndexName::Adjacency));
        assert!(catalog
            .all()
            .iter()
            .any(|index| index.name == IndexName::Vector));
    }

    #[test]
    fn rebuilds_graph_indexes_for_lookup_and_adjacency() {
        let mut node_a =
            NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory").expect("node");
        node_a.properties.insert(
            "unique_key".to_owned(),
            PropertyValue::String("alpha".to_owned()),
        );
        node_a
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(100));
        node_a.vectors.insert("default".to_owned(), vec![0.1, 0.2]);
        let node_b =
            NodeRecord::new(NodeId::new("node_b").expect("valid id"), "memory").expect("node");
        let edge = EdgeRecord {
            id: EdgeId::new("edge_ab").expect("valid edge id"),
            source: node_a.id.clone(),
            target: node_b.id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        };

        let nodes = vec![node_a.clone(), node_b.clone()];
        let edges = vec![edge.clone()];
        let edge_map = edges
            .iter()
            .cloned()
            .map(|item| (item.id.clone(), item))
            .collect::<BTreeMap<_, _>>();

        let index = GraphIndex::rebuild(&nodes, &edges);
        assert!(index.contains_node(&node_a.id));
        assert_eq!(index.lookup_unique_key("alpha"), Some(&node_a.id));
        assert_eq!(index.node_ids_by_type("memory").len(), 2);
        assert_eq!(
            index
                .edge_ids_for(
                    &node_a.id,
                    EdgeDirection::Outgoing,
                    &edge_map,
                    Some("relates_to")
                )
                .len(),
            1
        );
        assert_eq!(index.node_ids_in_time_range(90, 110).len(), 1);
        assert_eq!(index.vector_candidate_ids_iter("default").count(), 1);
    }

    #[test]
    fn incremental_updates_match_rebuild_for_mixed_changes() {
        let mut node_a =
            NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory").expect("node");
        node_a.properties.insert(
            "unique_key".to_owned(),
            PropertyValue::String("alpha".to_owned()),
        );
        node_a
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(100));
        node_a.vectors.insert("default".to_owned(), vec![0.1, 0.2]);

        let node_b =
            NodeRecord::new(NodeId::new("node_b").expect("valid id"), "memory").expect("node");
        let edge_ab = EdgeRecord {
            id: EdgeId::new("edge_ab").expect("valid edge id"),
            source: node_a.id.clone(),
            target: node_b.id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        };

        let mut incremental = GraphIndex::rebuild(&[node_a.clone(), node_b.clone()], &[edge_ab]);

        let mut updated_node_a =
            NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory_v2").expect("node");
        updated_node_a.properties.insert(
            "unique_key".to_owned(),
            PropertyValue::String("beta".to_owned()),
        );
        updated_node_a
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(250));

        let node_c =
            NodeRecord::new(NodeId::new("node_c").expect("valid id"), "memory").expect("node");
        let edge_ac = EdgeRecord {
            id: EdgeId::new("edge_ac").expect("valid edge id"),
            source: updated_node_a.id.clone(),
            target: node_c.id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        };

        incremental.delete_edge(&EdgeRecord {
            id: EdgeId::new("edge_ab").expect("valid edge id"),
            source: node_a.id.clone(),
            target: node_b.id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        });
        incremental.delete_node(&node_a);
        incremental.upsert_node(&updated_node_a);
        incremental.upsert_node(&node_c);
        incremental.upsert_edge(&edge_ac);
        incremental.delete_node(&node_b);

        let rebuilt = GraphIndex::rebuild(&[updated_node_a.clone(), node_c.clone()], &[edge_ac]);

        assert_eq!(incremental, rebuilt);
        assert_eq!(incremental.lookup_unique_key("alpha"), None);
        assert_eq!(
            incremental.lookup_unique_key("beta"),
            Some(&updated_node_a.id)
        );
        assert_eq!(incremental.node_ids_by_type("memory_v2").len(), 1);
        assert_eq!(incremental.node_ids_in_time_range(200, 300).len(), 1);
        assert_eq!(incremental.vector_candidate_ids_iter("default").count(), 0);
    }

    #[test]
    fn graph_index_defaults_to_exact_vector_backend() {
        let index = GraphIndex::default();
        assert!(matches!(index.vector_index, VectorIndexState::Exact(_)));
    }

    #[test]
    fn hnsw_backend_returns_filtered_semantic_candidates() {
        let mut node_a =
            NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory").expect("node");
        node_a.vectors.insert("default".to_owned(), vec![1.0, 0.0]);
        let mut node_b =
            NodeRecord::new(NodeId::new("node_b").expect("valid id"), "memory").expect("node");
        node_b.vectors.insert("default".to_owned(), vec![0.8, 0.2]);
        let mut node_c =
            NodeRecord::new(NodeId::new("node_c").expect("valid id"), "profile").expect("node");
        node_c.vectors.insert("default".to_owned(), vec![1.0, 0.0]);

        let config = VectorIndexConfig {
            backend: VectorIndexBackendConfig::Hnsw,
            exact_fallback_threshold: 1,
            semantic_top_k: 2,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 64,
        };
        let index = GraphIndex::rebuild_with_config(
            &[node_a.clone(), node_b.clone(), node_c.clone()],
            &[],
            &config,
        );

        let hits = index.semantic_candidate_ids(&[1.0, 0.0], "default", Some("memory"), 2, None);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0], node_a.id);
        assert!(hits.iter().all(|node_id| node_id != &node_c.id));

        let snapshot = index.snapshot();
        assert_eq!(snapshot.vector_backend, "hnsw");
        assert!(snapshot.vector_runtime_ready);
        assert_eq!(snapshot.vector_dimensions.get("default"), Some(&2));
    }

    #[test]
    fn hnsw_vector_index_persists_and_warm_loads() {
        let mut node_a =
            NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory").expect("node");
        node_a.vectors.insert("default".to_owned(), vec![1.0, 0.0]);
        let mut node_b =
            NodeRecord::new(NodeId::new("node_b").expect("valid id"), "memory").expect("node");
        node_b.vectors.insert("default".to_owned(), vec![0.0, 1.0]);
        let config = VectorIndexConfig {
            backend: VectorIndexBackendConfig::Hnsw,
            exact_fallback_threshold: 1,
            semantic_top_k: 2,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 64,
        };
        let tempdir = tempdir().expect("temporary directory should be created");
        let manifest_path = tempdir.path().join("vector-index.manifest.json");
        let graph_path = tempdir.path().join("vector-index.hnsw.bin");
        let vectors_path = tempdir.path().join("vector-index.vectors.bin");

        let index =
            GraphIndex::rebuild_with_config(&[node_a.clone(), node_b.clone()], &[], &config);
        index
            .persist_vector_index(&manifest_path, &graph_path, &vectors_path, Some(42))
            .expect("vector index should persist");

        let loaded = GraphIndex::rebuild_with_config_and_vector_index_load(
            &[node_a.clone(), node_b.clone()],
            &[],
            &config,
            Some(VectorIndexLoadConfig {
                manifest_path: &manifest_path,
                graph_path: &graph_path,
                vectors_path: &vectors_path,
                expected_last_applied_lsn: Some(42),
            }),
        );

        let snapshot = loaded.snapshot();
        assert_eq!(snapshot.vector_backend, "hnsw");
        assert!(snapshot.vector_runtime_ready);
        let hits = loaded.semantic_candidate_ids(&[1.0, 0.0], "default", None, 1, None);
        assert!(!hits.is_empty());
        assert_eq!(hits[0], node_a.id);
        assert!(hits.contains(&node_b.id));
    }
}
