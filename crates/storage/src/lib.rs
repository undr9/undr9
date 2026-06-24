use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use rkyv::{Archive as RkyvArchive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use undr9_common::{crc32, EdgeId, NodeId, Result, TransactionId, Undr9Error};
use undr9_config::{AppConfig, StorageConfig, WalConfig};
use undr9_core::{
    EdgeRecord, IsolationLevel, NodeRecord, TransactionCommitResult, TransactionOperation,
    TransactionState, TransactionSummary, WriteBatch,
};
use undr9_memory::{ConsolidationAction, ConsolidationEvent, MemoryConsolidator};
use undr9_wal::{CheckpointMarker, LogSequenceNumber, Wal, WalRecordKind};

pub const MANIFEST_FILE_NAME: &str = "manifest.json";
pub const DATA_DIRECTORIES: [&str; 7] = [
    "wal", "nodes", "edges", "indexes", "vectors", "deltas", "meta",
];
pub const NODE_SEGMENT_FILE_NAME: &str = "segment-0000000000000001.snapshot.rkyv";
pub const EDGE_SEGMENT_FILE_NAME: &str = "segment-0000000000000001.snapshot.rkyv";
pub const VECTOR_SEGMENT_FILE_NAME: &str = "segment-0000000000000001.snapshot.rkyv";
pub const LEGACY_NODE_SEGMENT_FILE_NAME: &str = "segment-0000000000000001.snapshot.json";
pub const LEGACY_EDGE_SEGMENT_FILE_NAME: &str = "segment-0000000000000001.snapshot.json";
pub const LEGACY_VECTOR_SEGMENT_FILE_NAME: &str = "segment-0000000000000001.snapshot.json";
pub const INDEX_SNAPSHOT_FILE_NAME: &str = "graph-index.snapshot.json";
pub const AUDIT_LOG_FILE_NAME: &str = "audit.log";
pub const CONSOLIDATION_LOG_FILE_NAME: &str = "consolidation.log";
pub const DELTA_SEGMENT_FILE_EXTENSION: &str = "json";
pub const BACKUP_MANIFEST_FILE_NAME: &str = "backup-manifest.json";

#[derive(Debug, Clone, PartialEq, Eq)]
struct StorageIoFailpoint {
    operation: &'static str,
    path_fragment: String,
}

static STORAGE_IO_FAILPOINT: OnceLock<Mutex<Option<StorageIoFailpoint>>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageLayout {
    pub root_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub storage_version: String,
    pub files: BTreeMap<String, ManifestFile>,
    pub settings: ManifestSettings,
    pub last_clean_shutdown: bool,
    pub last_applied_lsn: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestFile {
    pub relative_path: String,
    pub checksum_crc32: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestSettings {
    pub create_if_missing: bool,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, RkyvArchive, RkyvSerialize, RkyvDeserialize,
)]
#[archive(check_bytes)]
struct NodeSegmentSnapshot {
    format_version: u16,
    records: Vec<StoredNodeRecord>,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, RkyvArchive, RkyvSerialize, RkyvDeserialize,
)]
#[archive(check_bytes)]
struct EdgeSegmentSnapshot {
    format_version: u16,
    records: Vec<StoredEdgeRecord>,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, RkyvArchive, RkyvSerialize, RkyvDeserialize,
)]
#[archive(check_bytes)]
struct StoredNodeRecord {
    id: String,
    node_type: String,
    properties: BTreeMap<String, StoredPropertyValue>,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, RkyvArchive, RkyvSerialize, RkyvDeserialize,
)]
#[archive(check_bytes)]
struct StoredEdgeRecord {
    id: String,
    source: String,
    target: String,
    edge_type: String,
    properties: BTreeMap<String, StoredPropertyValue>,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, RkyvArchive, RkyvSerialize, RkyvDeserialize,
)]
#[archive(check_bytes)]
#[serde(tag = "kind", content = "value")]
enum StoredPropertyValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    StringList(Vec<String>),
    FloatList(Vec<f32>),
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, RkyvArchive, RkyvSerialize, RkyvDeserialize,
)]
#[archive(check_bytes)]
struct VectorSegmentSnapshot {
    format_version: u16,
    records: Vec<NodeVectorRecord>,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, RkyvArchive, RkyvSerialize, RkyvDeserialize,
)]
#[archive(check_bytes)]
struct NodeVectorRecord {
    node_id: String,
    vectors: BTreeMap<String, Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct DeltaSegmentEntry {
    lsn: u64,
    batch: WriteBatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct DeltaSegmentSnapshot {
    format_version: u16,
    entries: Vec<DeltaSegmentEntry>,
}

#[derive(Debug, Clone)]
struct VersionedValue<T> {
    revision: u64,
    value: Option<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JsonlRecord {
    Node(NodeRecord),
    Edge(EdgeRecord),
}

#[derive(Debug)]
pub struct StorageEngine {
    layout: StorageLayout,
    manifest: Manifest,
    wal: Wal,
    nodes: BTreeMap<NodeId, NodeRecord>,
    edges: BTreeMap<EdgeId, EdgeRecord>,
    node_lineage: BTreeMap<NodeId, u64>,
    edge_lineage: BTreeMap<EdgeId, u64>,
    node_versions: BTreeMap<NodeId, Vec<VersionedValue<NodeRecord>>>,
    edge_versions: BTreeMap<EdgeId, Vec<VersionedValue<EdgeRecord>>>,
    commit_revision: u64,
    latest_applied_lsn: Option<u64>,
    checkpoint_dirty: bool,
    pending_checkpoint_entries: Vec<DeltaSegmentEntry>,
    next_transaction_ordinal: u64,
    transactions: BTreeMap<TransactionId, TransactionSession>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityReport {
    pub manifest_present: bool,
    pub node_snapshot_valid: bool,
    pub edge_snapshot_valid: bool,
    pub wal_replay_valid: bool,
    pub node_count: usize,
    pub edge_count: usize,
    pub issues: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    pub source_root: String,
    pub file_count: usize,
    pub files: BTreeMap<String, ManifestFile>,
    pub integrity: IntegrityReport,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransactionDelta {
    pub removed_nodes: BTreeMap<NodeId, NodeRecord>,
    pub removed_edges: BTreeMap<EdgeId, EdgeRecord>,
    pub added_nodes: BTreeMap<NodeId, NodeRecord>,
    pub added_edges: BTreeMap<EdgeId, EdgeRecord>,
}

#[derive(Debug, Clone)]
struct TransactionSession {
    transaction_id: TransactionId,
    isolation_level: IsolationLevel,
    state: TransactionState,
    started_at_revision: u64,
    node_overrides: BTreeMap<NodeId, Option<NodeRecord>>,
    edge_overrides: BTreeMap<EdgeId, Option<EdgeRecord>>,
    staged_batch: WriteBatch,
    touched_node_ids: BTreeSet<NodeId>,
    touched_edge_ids: BTreeSet<EdgeId>,
}

impl StorageLayout {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
        }
    }

    pub fn manifest_path(&self) -> PathBuf {
        self.root_dir.join(MANIFEST_FILE_NAME)
    }

    pub fn subdirectory(&self, name: &str) -> PathBuf {
        self.root_dir.join(name)
    }

    pub fn node_segment_path(&self) -> PathBuf {
        self.subdirectory("nodes").join(NODE_SEGMENT_FILE_NAME)
    }

    pub fn legacy_node_segment_path(&self) -> PathBuf {
        self.subdirectory("nodes")
            .join(LEGACY_NODE_SEGMENT_FILE_NAME)
    }

    pub fn edge_segment_path(&self) -> PathBuf {
        self.subdirectory("edges").join(EDGE_SEGMENT_FILE_NAME)
    }

    pub fn legacy_edge_segment_path(&self) -> PathBuf {
        self.subdirectory("edges")
            .join(LEGACY_EDGE_SEGMENT_FILE_NAME)
    }

    pub fn index_snapshot_path(&self) -> PathBuf {
        self.subdirectory("indexes").join(INDEX_SNAPSHOT_FILE_NAME)
    }

    pub fn vector_index_manifest_path(&self) -> PathBuf {
        self.subdirectory("indexes")
            .join("vector-index.manifest.json")
    }

    pub fn vector_index_graph_path(&self) -> PathBuf {
        self.subdirectory("indexes").join("vector-index.hnsw.bin")
    }

    pub fn vector_index_vectors_path(&self) -> PathBuf {
        self.subdirectory("indexes")
            .join("vector-index.vectors.bin")
    }

    pub fn delta_directory(&self) -> PathBuf {
        self.subdirectory("deltas")
    }

    pub fn delta_segment_path(&self, first_lsn: u64, checkpoint_lsn: u64) -> PathBuf {
        self.delta_directory().join(format!(
            "delta-{first_lsn:020}-{checkpoint_lsn:020}.{DELTA_SEGMENT_FILE_EXTENSION}"
        ))
    }

    pub fn vector_segment_path(&self) -> PathBuf {
        self.subdirectory("vectors").join(VECTOR_SEGMENT_FILE_NAME)
    }

    pub fn legacy_vector_segment_path(&self) -> PathBuf {
        self.subdirectory("vectors")
            .join(LEGACY_VECTOR_SEGMENT_FILE_NAME)
    }

    pub fn audit_log_path(&self) -> PathBuf {
        self.subdirectory("meta").join(AUDIT_LOG_FILE_NAME)
    }

    pub fn consolidation_log_path(&self) -> PathBuf {
        self.subdirectory("meta").join(CONSOLIDATION_LOG_FILE_NAME)
    }
}

impl Manifest {
    pub fn for_config(config: &StorageConfig) -> Self {
        let mut files = BTreeMap::new();
        files.insert(
            MANIFEST_FILE_NAME.to_owned(),
            ManifestFile {
                relative_path: MANIFEST_FILE_NAME.to_owned(),
                checksum_crc32: 0,
            },
        );

        Self {
            storage_version: config.storage_version.clone(),
            files,
            settings: ManifestSettings {
                create_if_missing: config.create_if_missing,
            },
            last_clean_shutdown: false,
            last_applied_lsn: None,
        }
    }
}

impl From<undr9_core::PropertyValue> for StoredPropertyValue {
    fn from(value: undr9_core::PropertyValue) -> Self {
        match value {
            undr9_core::PropertyValue::String(value) => Self::String(value),
            undr9_core::PropertyValue::Integer(value) => Self::Integer(value),
            undr9_core::PropertyValue::Float(value) => Self::Float(value),
            undr9_core::PropertyValue::Boolean(value) => Self::Boolean(value),
            undr9_core::PropertyValue::StringList(value) => Self::StringList(value),
            undr9_core::PropertyValue::FloatList(value) => Self::FloatList(value),
        }
    }
}

impl From<StoredPropertyValue> for undr9_core::PropertyValue {
    fn from(value: StoredPropertyValue) -> Self {
        match value {
            StoredPropertyValue::String(value) => Self::String(value),
            StoredPropertyValue::Integer(value) => Self::Integer(value),
            StoredPropertyValue::Float(value) => Self::Float(value),
            StoredPropertyValue::Boolean(value) => Self::Boolean(value),
            StoredPropertyValue::StringList(value) => Self::StringList(value),
            StoredPropertyValue::FloatList(value) => Self::FloatList(value),
        }
    }
}

impl StoredNodeRecord {
    fn from_node(record: &NodeRecord) -> Self {
        Self {
            id: record.id.to_string(),
            node_type: record.node_type.clone(),
            properties: record
                .properties
                .clone()
                .into_iter()
                .map(|(key, value)| (key, StoredPropertyValue::from(value)))
                .collect(),
        }
    }

    fn into_node(self) -> Result<NodeRecord> {
        Ok(NodeRecord {
            id: NodeId::new(self.id).map_err(|error| {
                Undr9Error::Corruption(format!("invalid node id in segment snapshot: {error}"))
            })?,
            node_type: self.node_type,
            properties: self
                .properties
                .into_iter()
                .map(|(key, value)| (key, undr9_core::PropertyValue::from(value)))
                .collect(),
            vectors: BTreeMap::new(),
        })
    }
}

impl StoredEdgeRecord {
    fn from_edge(record: &EdgeRecord) -> Self {
        Self {
            id: record.id.to_string(),
            source: record.source.to_string(),
            target: record.target.to_string(),
            edge_type: record.edge_type.clone(),
            properties: record
                .properties
                .clone()
                .into_iter()
                .map(|(key, value)| (key, StoredPropertyValue::from(value)))
                .collect(),
        }
    }

    fn into_edge(self) -> Result<EdgeRecord> {
        Ok(EdgeRecord {
            id: EdgeId::new(self.id).map_err(|error| {
                Undr9Error::Corruption(format!("invalid edge id in segment snapshot: {error}"))
            })?,
            source: NodeId::new(self.source).map_err(|error| {
                Undr9Error::Corruption(format!(
                    "invalid source node id in edge segment snapshot: {error}"
                ))
            })?,
            target: NodeId::new(self.target).map_err(|error| {
                Undr9Error::Corruption(format!(
                    "invalid target node id in edge segment snapshot: {error}"
                ))
            })?,
            edge_type: self.edge_type,
            properties: self
                .properties
                .into_iter()
                .map(|(key, value)| (key, undr9_core::PropertyValue::from(value)))
                .collect(),
        })
    }
}

impl NodeVectorRecord {
    fn from_node(node: &NodeRecord) -> Self {
        Self {
            node_id: node.id.to_string(),
            vectors: node.vectors.clone(),
        }
    }

    fn into_parts(self) -> Result<(NodeId, BTreeMap<String, Vec<f32>>)> {
        let node_id = NodeId::new(self.node_id).map_err(|error| {
            Undr9Error::Corruption(format!(
                "invalid node id in vector segment snapshot: {error}"
            ))
        })?;
        Ok((node_id, self.vectors))
    }
}

impl StorageEngine {
    pub fn open(config: &AppConfig) -> Result<Self> {
        config.validate()?;

        let (layout, manifest) = bootstrap(&config.storage)?;
        let wal = Wal::open(layout.subdirectory("wal"), &config.wal)?;
        let (nodes, edges) = load_published_state(&layout, &manifest)?;

        let mut engine = Self {
            layout,
            manifest,
            wal,
            nodes,
            edges,
            node_lineage: BTreeMap::new(),
            edge_lineage: BTreeMap::new(),
            node_versions: BTreeMap::new(),
            edge_versions: BTreeMap::new(),
            commit_revision: 0,
            latest_applied_lsn: None,
            checkpoint_dirty: false,
            pending_checkpoint_entries: Vec::new(),
            next_transaction_ordinal: 1,
            transactions: BTreeMap::new(),
        };

        engine.recover()?;
        engine.rebuild_lineage();
        Ok(engine)
    }

    pub fn layout(&self) -> &StorageLayout {
        &self.layout
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn all_nodes(&self) -> Vec<NodeRecord> {
        self.nodes.values().cloned().collect()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn all_edges(&self) -> Vec<EdgeRecord> {
        self.edges.values().cloned().collect()
    }

    pub fn current_revision(&self) -> u64 {
        self.commit_revision
    }

    pub fn latest_applied_lsn(&self) -> Option<u64> {
        self.latest_applied_lsn
    }

    pub fn needs_checkpoint(&self) -> bool {
        self.checkpoint_dirty
    }

    pub fn pending_checkpoint_count(&self) -> usize {
        self.pending_checkpoint_entries.len()
    }

    pub fn upsert_node(&mut self, node: NodeRecord) -> Result<LogSequenceNumber> {
        self.apply_write_batch(WriteBatch {
            nodes_upserted: vec![node],
            ..WriteBatch::default()
        })
    }

    pub fn upsert_nodes(&mut self, nodes: Vec<NodeRecord>) -> Result<LogSequenceNumber> {
        self.apply_write_batch(WriteBatch {
            nodes_upserted: nodes,
            ..WriteBatch::default()
        })
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<&NodeRecord> {
        self.nodes.get(node_id)
    }

    pub fn delete_node(&mut self, node_id: &NodeId) -> Result<Option<NodeRecord>> {
        if !self.nodes.contains_key(node_id) {
            return Ok(None);
        }

        let removed = self.nodes.get(node_id).cloned();
        self.apply_write_batch(WriteBatch {
            deleted_node_ids: vec![node_id.clone()],
            ..WriteBatch::default()
        })?;
        Ok(removed)
    }

    pub fn delete_nodes(&mut self, node_ids: Vec<NodeId>) -> Result<usize> {
        let existing = node_ids
            .into_iter()
            .filter(|node_id| self.nodes.contains_key(node_id))
            .collect::<Vec<_>>();
        if existing.is_empty() {
            return Ok(0);
        }

        self.apply_write_batch(WriteBatch {
            deleted_node_ids: existing.clone(),
            ..WriteBatch::default()
        })?;
        Ok(existing.len())
    }

    pub fn upsert_edge(&mut self, edge: EdgeRecord) -> Result<LogSequenceNumber> {
        self.apply_write_batch(WriteBatch {
            edges_upserted: vec![edge],
            ..WriteBatch::default()
        })
    }

    pub fn upsert_edges(&mut self, edges: Vec<EdgeRecord>) -> Result<LogSequenceNumber> {
        self.apply_write_batch(WriteBatch {
            edges_upserted: edges,
            ..WriteBatch::default()
        })
    }

    pub fn get_edge(&self, edge_id: &EdgeId) -> Option<&EdgeRecord> {
        self.edges.get(edge_id)
    }

    pub fn delete_edge(&mut self, edge_id: &EdgeId) -> Result<Option<EdgeRecord>> {
        if !self.edges.contains_key(edge_id) {
            return Ok(None);
        }

        let removed = self.edges.get(edge_id).cloned();
        self.apply_write_batch(WriteBatch {
            deleted_edge_ids: vec![edge_id.clone()],
            ..WriteBatch::default()
        })?;
        Ok(removed)
    }

    pub fn delete_edges(&mut self, edge_ids: Vec<EdgeId>) -> Result<usize> {
        let existing = edge_ids
            .into_iter()
            .filter(|edge_id| self.edges.contains_key(edge_id))
            .collect::<Vec<_>>();
        if existing.is_empty() {
            return Ok(0);
        }

        self.apply_write_batch(WriteBatch {
            deleted_edge_ids: existing.clone(),
            ..WriteBatch::default()
        })?;
        Ok(existing.len())
    }

    pub fn apply_write_batch(&mut self, batch: WriteBatch) -> Result<LogSequenceNumber> {
        self.apply_write_batch_with_snapshot_readers(batch, self.transactions.len())
    }

    fn apply_write_batch_with_snapshot_readers(
        &mut self,
        batch: WriteBatch,
        snapshot_reader_count: usize,
    ) -> Result<LogSequenceNumber> {
        let batch = normalize_write_batch(batch)?;
        if batch.is_empty() {
            return Err(Undr9Error::Validation(
                "write batch cannot be empty".to_owned(),
            ));
        }

        validate_batch_against_maps(&self.nodes, &self.edges, &batch)?;
        let effects = batch_effects(&self.edges, &batch);
        if snapshot_reader_count > 0 {
            preserve_historical_versions(
                &effects,
                &self.nodes,
                &self.edges,
                &self.node_lineage,
                &self.edge_lineage,
                &mut self.node_versions,
                &mut self.edge_versions,
            );
        }

        let payload = serialize_write_batch(&batch)?;
        let record = self.wal.append(WalRecordKind::WriteBatch, payload)?;
        apply_batch_to_maps_unchecked(&mut self.nodes, &mut self.edges, &batch);
        self.commit_revision = record.header.lsn.0;
        self.latest_applied_lsn = Some(record.header.lsn.0);
        self.checkpoint_dirty = true;
        self.pending_checkpoint_entries.push(DeltaSegmentEntry {
            lsn: record.header.lsn.0,
            batch: batch.clone(),
        });
        update_lineage_for_batch(
            &effects,
            &mut self.node_lineage,
            &mut self.edge_lineage,
            &self.edges,
            self.commit_revision,
        );
        if snapshot_reader_count > 0 {
            record_versions_after_commit(
                &effects,
                &self.nodes,
                &self.edges,
                &mut self.node_versions,
                &mut self.edge_versions,
                self.commit_revision,
            );
        }

        Ok(record.header.lsn)
    }

    pub fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> TransactionSummary {
        let transaction_id = TransactionId::new(format!("tx_{}", self.next_transaction_ordinal))
            .expect("transaction id should be valid");
        self.next_transaction_ordinal += 1;

        let session = TransactionSession {
            transaction_id: transaction_id.clone(),
            isolation_level,
            state: TransactionState::Active,
            started_at_revision: self.commit_revision,
            node_overrides: BTreeMap::new(),
            edge_overrides: BTreeMap::new(),
            staged_batch: WriteBatch::default(),
            touched_node_ids: BTreeSet::new(),
            touched_edge_ids: BTreeSet::new(),
        };
        let summary = summary_for_session(&session);
        self.transactions.insert(transaction_id, session);
        summary
    }

    pub fn transaction_summary(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<TransactionSummary> {
        let session = self.transactions.get(transaction_id).ok_or_else(|| {
            Undr9Error::NotFound(format!("transaction '{}' was not found", transaction_id))
        })?;
        Ok(summary_for_session(session))
    }

    pub fn list_transactions(&self) -> Vec<TransactionSummary> {
        self.transactions
            .values()
            .map(summary_for_session)
            .collect()
    }

    pub fn transaction_node(
        &self,
        transaction_id: &TransactionId,
        node_id: &NodeId,
    ) -> Result<Option<NodeRecord>> {
        let session = self.active_session(transaction_id)?;
        self.transaction_node_visible(session, node_id)
    }

    pub fn transaction_edge(
        &self,
        transaction_id: &TransactionId,
        edge_id: &EdgeId,
    ) -> Result<Option<EdgeRecord>> {
        let session = self.active_session(transaction_id)?;
        self.transaction_edge_visible(session, edge_id)
    }

    pub fn transaction_snapshot(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<(BTreeMap<NodeId, NodeRecord>, BTreeMap<EdgeId, EdgeRecord>)> {
        let session = self.active_session(transaction_id)?;
        let mut nodes = BTreeMap::new();
        let mut node_ids = self.node_lineage.keys().cloned().collect::<BTreeSet<_>>();
        node_ids.extend(self.nodes.keys().cloned());
        node_ids.extend(session.node_overrides.keys().cloned());
        for node_id in node_ids {
            if let Some(node) = self.transaction_node_visible(session, &node_id)? {
                nodes.insert(node_id, node);
            }
        }

        let mut edges = BTreeMap::new();
        let mut edge_ids = self.edge_lineage.keys().cloned().collect::<BTreeSet<_>>();
        edge_ids.extend(self.edges.keys().cloned());
        edge_ids.extend(session.edge_overrides.keys().cloned());
        for edge_id in edge_ids {
            if let Some(edge) = self.transaction_edge_visible(session, &edge_id)? {
                edges.insert(edge_id, edge);
            }
        }

        Ok((nodes, edges))
    }

    pub fn transaction_delta_from_current(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<TransactionDelta> {
        let session = self.active_session(transaction_id)?;
        let mut delta = TransactionDelta::default();

        let mut node_ids = self
            .node_lineage
            .iter()
            .filter_map(|(node_id, revision)| {
                (*revision > session.started_at_revision).then_some(node_id.clone())
            })
            .collect::<BTreeSet<_>>();
        node_ids.extend(self.node_versions.keys().cloned());
        node_ids.extend(session.node_overrides.keys().cloned());

        for node_id in node_ids {
            let current = self.nodes.get(&node_id).cloned();
            let visible = self.transaction_node_visible(session, &node_id)?;
            if current != visible {
                if let Some(current) = current {
                    delta.removed_nodes.insert(node_id.clone(), current);
                }
                if let Some(visible) = visible {
                    delta.added_nodes.insert(node_id, visible);
                }
            }
        }

        let mut edge_ids = self
            .edge_lineage
            .iter()
            .filter_map(|(edge_id, revision)| {
                (*revision > session.started_at_revision).then_some(edge_id.clone())
            })
            .collect::<BTreeSet<_>>();
        edge_ids.extend(self.edge_versions.keys().cloned());
        edge_ids.extend(session.edge_overrides.keys().cloned());

        for edge_id in edge_ids {
            let current = self.edges.get(&edge_id).cloned();
            let visible = self.transaction_edge_visible(session, &edge_id)?;
            if current != visible {
                if let Some(current) = current {
                    delta.removed_edges.insert(edge_id.clone(), current);
                }
                if let Some(visible) = visible {
                    delta.added_edges.insert(edge_id, visible);
                }
            }
        }

        Ok(delta)
    }

    pub fn transaction_staged_batch(&self, transaction_id: &TransactionId) -> Result<WriteBatch> {
        let session = self.active_session(transaction_id)?;
        Ok(session.staged_batch.clone())
    }

    pub fn stage_transaction_operation(
        &mut self,
        transaction_id: &TransactionId,
        operation: TransactionOperation,
    ) -> Result<TransactionSummary> {
        let mut session = self.transactions.remove(transaction_id).ok_or_else(|| {
            Undr9Error::NotFound(format!("transaction '{}' was not found", transaction_id))
        })?;
        if session.state != TransactionState::Active {
            return Err(Undr9Error::Conflict(format!(
                "transaction '{}' is not active",
                transaction_id
            )));
        }

        self.stage_operation_on_session(&mut session, operation)?;
        let summary = summary_for_session(&session);
        self.transactions.insert(transaction_id.clone(), session);

        Ok(summary)
    }

    pub fn commit_transaction(
        &mut self,
        transaction_id: &TransactionId,
    ) -> Result<TransactionCommitResult> {
        let session = self.active_session(transaction_id)?.clone();

        for node_id in &session.touched_node_ids {
            if self.node_lineage.get(node_id).copied().unwrap_or(0) > session.started_at_revision {
                return Err(Undr9Error::Conflict(format!(
                    "node '{}' changed after transaction '{}' began",
                    node_id, transaction_id
                )));
            }
        }
        for edge_id in &session.touched_edge_ids {
            if self.edge_lineage.get(edge_id).copied().unwrap_or(0) > session.started_at_revision {
                return Err(Undr9Error::Conflict(format!(
                    "edge '{}' changed after transaction '{}' began",
                    edge_id, transaction_id
                )));
            }
        }

        if session.staged_batch.is_empty() {
            return Err(Undr9Error::Validation(format!(
                "transaction '{}' has no staged operations",
                transaction_id
            )));
        }

        let lsn = self.apply_write_batch_with_snapshot_readers(
            session.staged_batch.clone(),
            self.transactions.len().saturating_sub(1),
        )?;
        self.transactions.remove(transaction_id);
        self.clear_version_history_if_unused();

        Ok(TransactionCommitResult {
            transaction_id: transaction_id.clone(),
            committed_revision: self.commit_revision,
            committed_lsn: lsn.0,
            staged_operation_count: summary_for_session(&session).staged_operation_count,
        })
    }

    pub fn rollback_transaction(
        &mut self,
        transaction_id: &TransactionId,
    ) -> Result<TransactionSummary> {
        let mut session = self.transactions.remove(transaction_id).ok_or_else(|| {
            Undr9Error::NotFound(format!("transaction '{}' was not found", transaction_id))
        })?;
        session.state = TransactionState::RolledBack;
        self.clear_version_history_if_unused();
        Ok(summary_for_session(&session))
    }

    pub fn graceful_shutdown(&mut self) -> Result<()> {
        self.publish_checkpoint(true)
    }

    pub fn compact(&mut self) -> Result<()> {
        let checkpoint = self.wal.append_checkpoint(CheckpointMarker {
            last_applied_lsn: self
                .latest_applied_lsn
                .or(self.manifest.last_applied_lsn)
                .unwrap_or(0),
        })?;
        self.latest_applied_lsn = Some(checkpoint.header.lsn.0);
        self.persist_state(Some(checkpoint.header.lsn.0), true, true)?;
        self.pending_checkpoint_entries.clear();
        self.checkpoint_dirty = false;
        self.wal.truncate_all_segments()?;
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        self.publish_checkpoint(false)
    }

    pub fn verify_integrity(&self) -> Result<IntegrityReport> {
        verify_storage_layout(&self.layout)
    }

    pub fn backup_to(&self, destination: impl AsRef<Path>) -> Result<()> {
        backup_directory(&self.layout.root_dir, destination)
    }

    pub fn export_jsonl(&self, destination: impl AsRef<Path>) -> Result<()> {
        let path = destination.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                Undr9Error::Io(format!(
                    "failed to create export parent directory '{}': {error}",
                    parent.display()
                ))
            })?;
        }
        let mut file = fs::File::create(path).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to create export file '{}': {error}",
                path.display()
            ))
        })?;

        for node in self.nodes.values() {
            let line =
                serde_json::to_string(&JsonlRecord::Node(node.clone())).map_err(|error| {
                    Undr9Error::Serialization(format!(
                        "failed to serialize node export record: {error}"
                    ))
                })?;
            writeln!(file, "{line}").map_err(|error| {
                Undr9Error::Io(format!("failed to write node export line: {error}"))
            })?;
        }
        for edge in self.edges.values() {
            let line =
                serde_json::to_string(&JsonlRecord::Edge(edge.clone())).map_err(|error| {
                    Undr9Error::Serialization(format!(
                        "failed to serialize edge export record: {error}"
                    ))
                })?;
            writeln!(file, "{line}").map_err(|error| {
                Undr9Error::Io(format!("failed to write edge export line: {error}"))
            })?;
        }

        Ok(())
    }

    pub fn import_jsonl(&mut self, source: impl AsRef<Path>) -> Result<()> {
        let file = fs::File::open(source.as_ref()).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to open import file '{}': {error}",
                source.as_ref().display()
            ))
        })?;
        let reader = BufReader::new(file);
        let mut batch = WriteBatch::default();

        for line in reader.lines() {
            let line = line.map_err(|error| {
                Undr9Error::Io(format!("failed to read import record line: {error}"))
            })?;
            if line.trim().is_empty() {
                continue;
            }
            let record: JsonlRecord = serde_json::from_str(&line).map_err(|error| {
                Undr9Error::Serialization(format!("failed to parse JSONL record: {error}"))
            })?;
            match record {
                JsonlRecord::Node(node) => batch.nodes_upserted.push(node),
                JsonlRecord::Edge(edge) => batch.edges_upserted.push(edge),
            }
        }

        if !batch.is_empty() {
            let _ = self.apply_write_batch(batch)?;
        }
        Ok(())
    }

    pub fn run_consolidation(&mut self, now_epoch_ms: i64) -> Result<Vec<ConsolidationEvent>> {
        let events =
            MemoryConsolidator::analyze(&self.all_nodes(), &self.all_edges(), now_epoch_ms);
        if events.is_empty() {
            return Ok(Vec::new());
        }

        for event in &events {
            let forward_batch = batch_for_consolidation_event(event, &self.nodes);
            if !forward_batch.is_empty() {
                let payload = postcard::to_allocvec(event).map_err(|error| {
                    Undr9Error::Serialization(format!(
                        "failed to serialize consolidation event '{}': {error}",
                        event.event_id
                    ))
                })?;
                let _ = self
                    .wal
                    .append(WalRecordKind::ConsolidationEvent, payload)?;
                let _ = self.apply_write_batch(forward_batch)?;
                append_json_line(&self.layout.consolidation_log_path(), event)?;
            }
        }

        Ok(events)
    }

    fn active_session(&self, transaction_id: &TransactionId) -> Result<&TransactionSession> {
        let session = self.transactions.get(transaction_id).ok_or_else(|| {
            Undr9Error::NotFound(format!("transaction '{}' was not found", transaction_id))
        })?;
        if session.state != TransactionState::Active {
            return Err(Undr9Error::Conflict(format!(
                "transaction '{}' is not active",
                transaction_id
            )));
        }
        Ok(session)
    }

    fn transaction_node_visible(
        &self,
        session: &TransactionSession,
        node_id: &NodeId,
    ) -> Result<Option<NodeRecord>> {
        if let Some(override_value) = session.node_overrides.get(node_id) {
            return Ok(override_value.clone());
        }
        Ok(visible_node_at_revision(
            node_id,
            session.started_at_revision,
            &self.nodes,
            &self.node_lineage,
            &self.node_versions,
        ))
    }

    fn transaction_edge_visible(
        &self,
        session: &TransactionSession,
        edge_id: &EdgeId,
    ) -> Result<Option<EdgeRecord>> {
        if let Some(override_value) = session.edge_overrides.get(edge_id) {
            return Ok(override_value.clone());
        }
        Ok(visible_edge_at_revision(
            edge_id,
            session.started_at_revision,
            &self.edges,
            &self.edge_lineage,
            &self.edge_versions,
        ))
    }

    fn stage_operation_on_session(
        &self,
        session: &mut TransactionSession,
        operation: TransactionOperation,
    ) -> Result<()> {
        let batch = batch_for_operation(&operation);

        match &operation {
            TransactionOperation::UpsertNode(node) => {
                session
                    .node_overrides
                    .insert(node.id.clone(), Some(node.clone()));
            }
            TransactionOperation::UpsertEdge(edge) => {
                if self
                    .transaction_node_visible(session, &edge.source)?
                    .is_none()
                {
                    return Err(Undr9Error::Validation(format!(
                        "edge source node '{}' does not exist",
                        edge.source
                    )));
                }
                if self
                    .transaction_node_visible(session, &edge.target)?
                    .is_none()
                {
                    return Err(Undr9Error::Validation(format!(
                        "edge target node '{}' does not exist",
                        edge.target
                    )));
                }
                session
                    .edge_overrides
                    .insert(edge.id.clone(), Some(edge.clone()));
            }
            TransactionOperation::DeleteNode { node_id } => {
                session.node_overrides.insert(node_id.clone(), None);
                for edge_id in self.transaction_incident_edge_ids(session, node_id)? {
                    session.edge_overrides.insert(edge_id.clone(), None);
                    session.touched_edge_ids.insert(edge_id);
                }
            }
            TransactionOperation::DeleteEdge { edge_id } => {
                session.edge_overrides.insert(edge_id.clone(), None);
            }
        }

        session.staged_batch = merge_write_batches(&session.staged_batch, &batch);
        extend_touched_sets(
            &mut session.touched_node_ids,
            &mut session.touched_edge_ids,
            &batch,
        );
        Ok(())
    }

    fn transaction_incident_edge_ids(
        &self,
        session: &TransactionSession,
        node_id: &NodeId,
    ) -> Result<Vec<EdgeId>> {
        let mut edge_ids = self.edge_lineage.keys().cloned().collect::<BTreeSet<_>>();
        edge_ids.extend(self.edges.keys().cloned());
        edge_ids.extend(session.edge_overrides.keys().cloned());

        let mut incident = Vec::new();
        for edge_id in edge_ids {
            if let Some(edge) = self.transaction_edge_visible(session, &edge_id)? {
                if edge.source == *node_id || edge.target == *node_id {
                    incident.push(edge_id);
                }
            }
        }
        Ok(incident)
    }

    fn rebuild_lineage(&mut self) {
        self.commit_revision = self
            .latest_applied_lsn
            .unwrap_or(self.manifest.last_applied_lsn.unwrap_or(0));
        self.node_lineage = self
            .nodes
            .keys()
            .cloned()
            .map(|node_id| (node_id, self.commit_revision))
            .collect();
        self.edge_lineage = self
            .edges
            .keys()
            .cloned()
            .map(|edge_id| (edge_id, self.commit_revision))
            .collect();
        self.node_versions.clear();
        self.edge_versions.clear();
        self.next_transaction_ordinal = 1;
        self.transactions.clear();
    }

    fn clear_version_history_if_unused(&mut self) {
        if self.transactions.is_empty() {
            self.node_versions.clear();
            self.edge_versions.clear();
        }
    }

    fn recover(&mut self) -> Result<()> {
        let last_applied_lsn = self.manifest.last_applied_lsn.unwrap_or(0);
        let replayed = self.wal.replay()?;
        let mut latest_seen_lsn = None;

        for record in replayed {
            if record.header.lsn.0 <= last_applied_lsn {
                continue;
            }

            latest_seen_lsn = Some(record.header.lsn.0);
            match record.header.kind {
                WalRecordKind::WriteBatch => {
                    let batch = deserialize_write_batch(&record.payload, "recovery")?;
                    apply_batch_to_maps(&mut self.nodes, &mut self.edges, &batch)?;
                    self.pending_checkpoint_entries.push(DeltaSegmentEntry {
                        lsn: record.header.lsn.0,
                        batch,
                    });
                }
                WalRecordKind::Checkpoint => {
                    let _: CheckpointMarker =
                        postcard::from_bytes(&record.payload).map_err(|error| {
                            Undr9Error::Corruption(format!(
                                "failed to deserialize WAL checkpoint during recovery: {error}"
                            ))
                        })?;
                }
                WalRecordKind::ManifestSync => {}
                WalRecordKind::ConsolidationEvent => {}
            }
        }

        self.latest_applied_lsn = latest_seen_lsn.or(self.manifest.last_applied_lsn);
        self.checkpoint_dirty = latest_seen_lsn.is_some();
        Ok(())
    }

    fn publish_checkpoint(&mut self, clean_shutdown: bool) -> Result<()> {
        let durable_lsn = self
            .latest_applied_lsn
            .or(self.manifest.last_applied_lsn)
            .unwrap_or(0);
        let checkpoint = self.wal.append_checkpoint(CheckpointMarker {
            last_applied_lsn: durable_lsn,
        })?;
        self.latest_applied_lsn = Some(checkpoint.header.lsn.0);
        if !self.pending_checkpoint_entries.is_empty() {
            let first_lsn = self
                .pending_checkpoint_entries
                .first()
                .map(|entry| entry.lsn)
                .unwrap_or(durable_lsn);
            let path = self
                .layout
                .delta_segment_path(first_lsn, checkpoint.header.lsn.0);
            let relative = relative_path(&self.layout, &path)?;
            let snapshot = DeltaSegmentSnapshot {
                format_version: 1,
                entries: std::mem::take(&mut self.pending_checkpoint_entries),
            };
            let checksum = persist_delta_segment(&path, &snapshot)?;
            self.manifest.files.insert(
                relative.clone(),
                ManifestFile {
                    relative_path: relative,
                    checksum_crc32: checksum,
                },
            );
        }
        self.manifest.last_applied_lsn = Some(checkpoint.header.lsn.0);
        self.manifest.last_clean_shutdown = clean_shutdown;
        persist_manifest(&self.layout.manifest_path(), &self.manifest)?;
        self.checkpoint_dirty = false;
        Ok(())
    }

    fn persist_state(
        &mut self,
        last_applied_lsn: Option<u64>,
        clean_shutdown: bool,
        clear_delta_segments: bool,
    ) -> Result<()> {
        let node_snapshot = NodeSegmentSnapshot {
            format_version: 1,
            records: self
                .nodes
                .values()
                .map(StoredNodeRecord::from_node)
                .collect(),
        };
        let edge_snapshot = EdgeSegmentSnapshot {
            format_version: 1,
            records: self
                .edges
                .values()
                .map(StoredEdgeRecord::from_edge)
                .collect(),
        };
        let vector_snapshot = VectorSegmentSnapshot {
            format_version: 1,
            records: self
                .nodes
                .values()
                .map(NodeVectorRecord::from_node)
                .collect(),
        };

        let node_relative = relative_path(&self.layout, &self.layout.node_segment_path())?;
        let edge_relative = relative_path(&self.layout, &self.layout.edge_segment_path())?;
        let vector_relative = relative_path(&self.layout, &self.layout.vector_segment_path())?;

        let node_checksum =
            persist_rkyv_snapshot(&self.layout.node_segment_path(), &node_snapshot)?;
        let edge_checksum =
            persist_rkyv_snapshot(&self.layout.edge_segment_path(), &edge_snapshot)?;
        let vector_checksum =
            persist_rkyv_snapshot(&self.layout.vector_segment_path(), &vector_snapshot)?;

        self.manifest
            .files
            .remove("nodes/segment-0000000000000001.snapshot.json");
        self.manifest
            .files
            .remove("edges/segment-0000000000000001.snapshot.json");
        self.manifest
            .files
            .remove("vectors/segment-0000000000000001.snapshot.json");

        let obsolete_delta_paths = if clear_delta_segments {
            remove_manifest_files_with_prefix(&mut self.manifest, "deltas/")
                .into_iter()
                .map(|relative| self.layout.root_dir.join(relative))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        self.manifest.files.insert(
            node_relative.clone(),
            ManifestFile {
                relative_path: node_relative,
                checksum_crc32: node_checksum,
            },
        );
        self.manifest.files.insert(
            edge_relative.clone(),
            ManifestFile {
                relative_path: edge_relative,
                checksum_crc32: edge_checksum,
            },
        );
        self.manifest.files.insert(
            vector_relative.clone(),
            ManifestFile {
                relative_path: vector_relative,
                checksum_crc32: vector_checksum,
            },
        );
        self.manifest.last_applied_lsn = last_applied_lsn;
        self.manifest.last_clean_shutdown = clean_shutdown;

        persist_manifest(&self.layout.manifest_path(), &self.manifest)?;
        for path in obsolete_delta_paths {
            if path.exists() {
                fs::remove_file(&path).map_err(|error| {
                    Undr9Error::Io(format!(
                        "failed to remove obsolete delta segment '{}': {error}",
                        path.display()
                    ))
                })?;
            }
        }
        Ok(())
    }
}

pub fn verify_storage_layout(layout: &StorageLayout) -> Result<IntegrityReport> {
    let manifest_path = layout.manifest_path();
    let manifest_present = manifest_path.exists();
    let mut issues = Vec::new();

    let manifest = if manifest_present {
        match load_manifest(&manifest_path) {
            Ok(manifest) => Some(manifest),
            Err(error) => {
                issues.push(format!("manifest: {error}"));
                None
            }
        }
    } else {
        issues.push("manifest: missing".to_owned());
        None
    };

    let (node_snapshot_valid, edge_snapshot_valid, node_count, edge_count) = match manifest.as_ref()
    {
        Some(manifest) => match load_published_state(layout, manifest) {
            Ok((nodes, edges)) => (true, true, nodes.len(), edges.len()),
            Err(error) => {
                issues.push(format!("published_state: {error}"));
                (false, false, 0, 0)
            }
        },
        None => {
            let nodes = match load_raw_node_state(layout) {
                Ok(nodes) => Some(nodes),
                Err(error) => {
                    issues.push(format!("nodes: {error}"));
                    None
                }
            };
            let edges = match load_raw_edge_state(layout) {
                Ok(edges) => Some(edges),
                Err(error) => {
                    issues.push(format!("edges: {error}"));
                    None
                }
            };
            (
                nodes.is_some(),
                edges.is_some(),
                nodes.as_ref().map(BTreeMap::len).unwrap_or(0),
                edges.as_ref().map(BTreeMap::len).unwrap_or(0),
            )
        }
    };

    let wal_replay_valid = match undr9_wal::replay_from_dir(&layout.subdirectory("wal"), u64::MAX) {
        Ok(_) => true,
        Err(error) => {
            issues.push(format!("wal: {error}"));
            false
        }
    };

    Ok(IntegrityReport {
        manifest_present,
        node_snapshot_valid,
        edge_snapshot_valid,
        wal_replay_valid,
        node_count,
        edge_count,
        issues,
    })
}

pub fn backup_directory(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> Result<()> {
    let source = source.as_ref();
    let destination = destination.as_ref();

    if !source.exists() {
        return Err(Undr9Error::NotFound(format!(
            "backup source '{}' does not exist",
            source.display()
        )));
    }

    if destination.exists() {
        fs::remove_dir_all(destination).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to remove existing backup destination '{}': {error}",
                destination.display()
            ))
        })?;
    }

    copy_directory_recursive(source, destination)?;
    persist_backup_manifest(source, destination)
}

pub fn restore_directory(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> Result<()> {
    restore_directory_internal(source.as_ref(), destination.as_ref(), None, None)
}

pub fn restore_directory_to_lsn(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    target_lsn: u64,
    wal_config: &WalConfig,
) -> Result<()> {
    restore_directory_internal(
        source.as_ref(),
        destination.as_ref(),
        Some(target_lsn),
        Some(wal_config),
    )
}

fn restore_directory_internal(
    source: &Path,
    destination: &Path,
    target_lsn: Option<u64>,
    wal_config: Option<&WalConfig>,
) -> Result<()> {
    if !source.exists() {
        return Err(Undr9Error::NotFound(format!(
            "restore source '{}' does not exist",
            source.display()
        )));
    }

    verify_backup_directory(source)?;

    let staging_dir = destination.with_extension("restore-staging");
    if staging_dir.exists() {
        fs::remove_dir_all(&staging_dir).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to remove existing restore staging directory '{}': {error}",
                staging_dir.display()
            ))
        })?;
    }
    copy_directory_recursive(source, &staging_dir)?;
    if let Some(target_lsn) = target_lsn {
        let wal_config = wal_config.ok_or_else(|| {
            Undr9Error::Validation(
                "restore to lsn requires an explicit wal configuration".to_owned(),
            )
        })?;
        trim_restored_wal_to_lsn(&staging_dir, target_lsn, wal_config)?;
        let integrity = verify_storage_layout(&StorageLayout::new(&staging_dir))?;
        if !integrity.node_snapshot_valid
            || !integrity.edge_snapshot_valid
            || !integrity.wal_replay_valid
        {
            return Err(Undr9Error::Corruption(format!(
                "staged point-in-time restore '{}' failed integrity validation",
                staging_dir.display()
            )));
        }
    } else {
        verify_backup_directory(&staging_dir)?;
    }

    if destination.exists() {
        fs::remove_dir_all(destination).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to remove existing restore destination '{}': {error}",
                destination.display()
            ))
        })?;
    }

    fs::rename(&staging_dir, destination).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to promote staged restore '{}' -> '{}': {error}",
            staging_dir.display(),
            destination.display()
        ))
    })?;
    Ok(())
}

pub fn repair_storage(config: &AppConfig) -> Result<IntegrityReport> {
    config.validate()?;
    let layout = prepare_storage_layout(&config.storage)?;
    let manifest = load_manifest_for_repair(&layout, &config.storage)?;
    let ((mut nodes, mut edges), checkpoint_lsn) = match load_published_state(&layout, &manifest) {
        Ok(state) => (state, manifest.last_applied_lsn.unwrap_or(0)),
        Err(_) => match load_raw_published_state(&layout) {
            Ok(state) => (state, manifest.last_applied_lsn.unwrap_or(0)),
            Err(_) => {
                let mut nodes = load_raw_node_state(&layout).unwrap_or_default();
                let edges = load_raw_edge_state(&layout).unwrap_or_default();
                attach_raw_vectors(&layout, &mut nodes)?;
                ((nodes, edges), 0)
            }
        },
    };
    let wal = Wal::open(layout.subdirectory("wal"), &config.wal)?;
    let replayed = wal.replay()?;
    let mut last_applied_lsn = manifest.last_applied_lsn;

    for record in replayed {
        if record.header.lsn.0 <= checkpoint_lsn {
            continue;
        }
        last_applied_lsn = Some(record.header.lsn.0);
        if record.header.kind == WalRecordKind::WriteBatch {
            let batch = deserialize_write_batch(&record.payload, "repair")?;
            apply_batch_to_maps(&mut nodes, &mut edges, &batch)?;
        }
    }

    let mut repaired_manifest = Manifest::for_config(&config.storage);
    let mut engine = StorageEngine {
        layout,
        manifest: repaired_manifest.clone(),
        wal,
        nodes,
        edges,
        node_lineage: BTreeMap::new(),
        edge_lineage: BTreeMap::new(),
        node_versions: BTreeMap::new(),
        edge_versions: BTreeMap::new(),
        commit_revision: last_applied_lsn.unwrap_or(0),
        latest_applied_lsn: last_applied_lsn,
        checkpoint_dirty: false,
        pending_checkpoint_entries: Vec::new(),
        next_transaction_ordinal: 1,
        transactions: BTreeMap::new(),
    };
    engine.rebuild_lineage();
    engine.persist_state(last_applied_lsn, true, true)?;
    repaired_manifest = engine.manifest.clone();
    let _ = repaired_manifest;

    verify_storage_layout(&engine.layout)
}

fn apply_batch_to_maps(
    nodes: &mut BTreeMap<NodeId, NodeRecord>,
    edges: &mut BTreeMap<EdgeId, EdgeRecord>,
    batch: &WriteBatch,
) -> Result<()> {
    validate_batch_against_maps(nodes, edges, batch)?;
    apply_batch_to_maps_unchecked(nodes, edges, batch);
    Ok(())
}

fn validate_batch_against_maps(
    nodes: &BTreeMap<NodeId, NodeRecord>,
    _edges: &BTreeMap<EdgeId, EdgeRecord>,
    batch: &WriteBatch,
) -> Result<()> {
    if batch.edges_upserted.is_empty() {
        return Ok(());
    }

    let upserted_node_ids = batch
        .nodes_upserted
        .iter()
        .map(|node| node.id.clone())
        .collect::<BTreeSet<_>>();

    for edge in &batch.edges_upserted {
        if !nodes.contains_key(&edge.source) && !upserted_node_ids.contains(&edge.source) {
            return Err(Undr9Error::Validation(format!(
                "edge source node '{}' does not exist",
                edge.source
            )));
        }

        if !nodes.contains_key(&edge.target) && !upserted_node_ids.contains(&edge.target) {
            return Err(Undr9Error::Validation(format!(
                "edge target node '{}' does not exist",
                edge.target
            )));
        }
    }

    Ok(())
}

fn apply_batch_to_maps_unchecked(
    nodes: &mut BTreeMap<NodeId, NodeRecord>,
    edges: &mut BTreeMap<EdgeId, EdgeRecord>,
    batch: &WriteBatch,
) {
    for node in &batch.nodes_upserted {
        nodes.insert(node.id.clone(), node.clone());
    }

    for edge in &batch.edges_upserted {
        edges.insert(edge.id.clone(), edge.clone());
    }

    let deleted_nodes = batch
        .deleted_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for node_id in &deleted_nodes {
        nodes.remove(node_id);
    }
    if !deleted_nodes.is_empty() {
        edges.retain(|_, edge| {
            !deleted_nodes.contains(&edge.source) && !deleted_nodes.contains(&edge.target)
        });
    }

    for edge_id in &batch.deleted_edge_ids {
        edges.remove(edge_id);
    }
}

fn batch_for_operation(operation: &TransactionOperation) -> WriteBatch {
    match operation {
        TransactionOperation::UpsertNode(node) => WriteBatch {
            nodes_upserted: vec![node.clone()],
            ..WriteBatch::default()
        },
        TransactionOperation::UpsertEdge(edge) => WriteBatch {
            edges_upserted: vec![edge.clone()],
            ..WriteBatch::default()
        },
        TransactionOperation::DeleteNode { node_id } => WriteBatch {
            deleted_node_ids: vec![node_id.clone()],
            ..WriteBatch::default()
        },
        TransactionOperation::DeleteEdge { edge_id } => WriteBatch {
            deleted_edge_ids: vec![edge_id.clone()],
            ..WriteBatch::default()
        },
    }
}

fn merge_write_batches(existing: &WriteBatch, incoming: &WriteBatch) -> WriteBatch {
    let mut merged = existing.clone();
    merged
        .nodes_upserted
        .extend(incoming.nodes_upserted.clone());
    merged
        .edges_upserted
        .extend(incoming.edges_upserted.clone());
    merged
        .deleted_node_ids
        .extend(incoming.deleted_node_ids.clone());
    merged
        .deleted_edge_ids
        .extend(incoming.deleted_edge_ids.clone());
    merged
}

fn extend_touched_sets(
    touched_node_ids: &mut BTreeSet<NodeId>,
    touched_edge_ids: &mut BTreeSet<EdgeId>,
    batch: &WriteBatch,
) {
    touched_node_ids.extend(batch.nodes_upserted.iter().map(|node| node.id.clone()));
    touched_node_ids.extend(batch.deleted_node_ids.iter().cloned());
    touched_edge_ids.extend(batch.edges_upserted.iter().map(|edge| edge.id.clone()));
    touched_edge_ids.extend(batch.deleted_edge_ids.iter().cloned());
}

struct BatchEffects {
    touched_node_ids: BTreeSet<NodeId>,
    touched_edge_ids: BTreeSet<EdgeId>,
}

fn batch_effects(edges: &BTreeMap<EdgeId, EdgeRecord>, batch: &WriteBatch) -> BatchEffects {
    let mut touched_node_ids = BTreeSet::new();
    let mut touched_edge_ids = BTreeSet::new();
    extend_touched_sets(&mut touched_node_ids, &mut touched_edge_ids, batch);

    let deleted_nodes = batch
        .deleted_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if !deleted_nodes.is_empty() {
        for edge in edges.values() {
            if deleted_nodes.contains(&edge.source) || deleted_nodes.contains(&edge.target) {
                touched_edge_ids.insert(edge.id.clone());
            }
        }
    }

    BatchEffects {
        touched_node_ids,
        touched_edge_ids,
    }
}

fn preserve_historical_versions(
    effects: &BatchEffects,
    nodes: &BTreeMap<NodeId, NodeRecord>,
    edges: &BTreeMap<EdgeId, EdgeRecord>,
    node_lineage: &BTreeMap<NodeId, u64>,
    edge_lineage: &BTreeMap<EdgeId, u64>,
    node_versions: &mut BTreeMap<NodeId, Vec<VersionedValue<NodeRecord>>>,
    edge_versions: &mut BTreeMap<EdgeId, Vec<VersionedValue<EdgeRecord>>>,
) {
    for node_id in &effects.touched_node_ids {
        if let Some(revision) = node_lineage.get(node_id).copied() {
            append_version_entry(
                node_versions.entry(node_id.clone()).or_default(),
                revision,
                nodes.get(node_id).cloned(),
            );
        }
    }
    for edge_id in &effects.touched_edge_ids {
        if let Some(revision) = edge_lineage.get(edge_id).copied() {
            append_version_entry(
                edge_versions.entry(edge_id.clone()).or_default(),
                revision,
                edges.get(edge_id).cloned(),
            );
        }
    }
}

fn record_versions_after_commit(
    effects: &BatchEffects,
    nodes: &BTreeMap<NodeId, NodeRecord>,
    edges: &BTreeMap<EdgeId, EdgeRecord>,
    node_versions: &mut BTreeMap<NodeId, Vec<VersionedValue<NodeRecord>>>,
    edge_versions: &mut BTreeMap<EdgeId, Vec<VersionedValue<EdgeRecord>>>,
    revision: u64,
) {
    for node_id in &effects.touched_node_ids {
        append_version_entry(
            node_versions.entry(node_id.clone()).or_default(),
            revision,
            nodes.get(node_id).cloned(),
        );
    }
    for edge_id in &effects.touched_edge_ids {
        append_version_entry(
            edge_versions.entry(edge_id.clone()).or_default(),
            revision,
            edges.get(edge_id).cloned(),
        );
    }
}

fn append_version_entry<T: Clone>(
    history: &mut Vec<VersionedValue<T>>,
    revision: u64,
    value: Option<T>,
) {
    if let Some(last) = history.last_mut() {
        if last.revision == revision {
            last.value = value;
            return;
        }
    }
    history.push(VersionedValue { revision, value });
}

fn visible_node_at_revision(
    node_id: &NodeId,
    revision: u64,
    live_nodes: &BTreeMap<NodeId, NodeRecord>,
    node_lineage: &BTreeMap<NodeId, u64>,
    node_versions: &BTreeMap<NodeId, Vec<VersionedValue<NodeRecord>>>,
) -> Option<NodeRecord> {
    visible_value_at_revision(node_id, revision, live_nodes, node_lineage, node_versions)
}

fn visible_edge_at_revision(
    edge_id: &EdgeId,
    revision: u64,
    live_edges: &BTreeMap<EdgeId, EdgeRecord>,
    edge_lineage: &BTreeMap<EdgeId, u64>,
    edge_versions: &BTreeMap<EdgeId, Vec<VersionedValue<EdgeRecord>>>,
) -> Option<EdgeRecord> {
    visible_value_at_revision(edge_id, revision, live_edges, edge_lineage, edge_versions)
}

fn visible_value_at_revision<K, T>(
    key: &K,
    revision: u64,
    live_values: &BTreeMap<K, T>,
    lineage: &BTreeMap<K, u64>,
    history: &BTreeMap<K, Vec<VersionedValue<T>>>,
) -> Option<T>
where
    K: Ord + Clone,
    T: Clone,
{
    match lineage.get(key).copied() {
        Some(current_revision) if current_revision <= revision => live_values.get(key).cloned(),
        Some(_) | None => history.get(key).and_then(|versions| {
            versions
                .iter()
                .rev()
                .find(|entry| entry.revision <= revision)
                .and_then(|entry| entry.value.clone())
        }),
    }
}

fn update_lineage_for_batch(
    effects: &BatchEffects,
    node_lineage: &mut BTreeMap<NodeId, u64>,
    edge_lineage: &mut BTreeMap<EdgeId, u64>,
    edges: &BTreeMap<EdgeId, EdgeRecord>,
    revision: u64,
) {
    for node_id in &effects.touched_node_ids {
        node_lineage.insert(node_id.clone(), revision);
    }
    for edge_id in &effects.touched_edge_ids {
        if edges.contains_key(edge_id) || edge_lineage.contains_key(edge_id) {
            edge_lineage.insert(edge_id.clone(), revision);
        }
    }
}

fn summary_for_session(session: &TransactionSession) -> TransactionSummary {
    TransactionSummary {
        transaction_id: session.transaction_id.clone(),
        isolation_level: session.isolation_level,
        state: session.state,
        started_at_revision: session.started_at_revision,
        staged_operation_count: session.staged_batch.nodes_upserted.len()
            + session.staged_batch.edges_upserted.len()
            + session.staged_batch.deleted_node_ids.len()
            + session.staged_batch.deleted_edge_ids.len(),
        touched_node_count: session.touched_node_ids.len(),
        touched_edge_count: session.touched_edge_ids.len(),
    }
}

pub fn bootstrap(config: &StorageConfig) -> Result<(StorageLayout, Manifest)> {
    let layout = prepare_storage_layout(config)?;
    let manifest_path = layout.manifest_path();
    let manifest = if manifest_path.exists() {
        load_manifest(&manifest_path)?
    } else {
        let manifest = Manifest::for_config(config);
        persist_manifest(&manifest_path, &manifest)?;
        manifest
    };

    Ok((layout, manifest))
}

fn prepare_storage_layout(config: &StorageConfig) -> Result<StorageLayout> {
    let layout = StorageLayout::new(&config.root_dir);

    if !config.create_if_missing && !layout.root_dir.exists() {
        return Err(Undr9Error::Io(format!(
            "storage root does not exist: {}",
            layout.root_dir.display()
        )));
    }

    fs::create_dir_all(&layout.root_dir)
        .map_err(|error| Undr9Error::Io(format!("failed to create storage root: {error}")))?;

    for directory in DATA_DIRECTORIES {
        fs::create_dir_all(layout.subdirectory(directory)).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to create storage directory '{directory}': {error}"
            ))
        })?;
    }
    Ok(layout)
}

fn load_manifest_for_repair(layout: &StorageLayout, config: &StorageConfig) -> Result<Manifest> {
    let manifest_path = layout.manifest_path();
    if !manifest_path.exists() {
        return Ok(Manifest::for_config(config));
    }

    match load_manifest(&manifest_path) {
        Ok(manifest) => Ok(manifest),
        Err(_) => Ok(Manifest::for_config(config)),
    }
}

pub fn load_manifest(path: &Path) -> Result<Manifest> {
    let bytes = fs::read(path).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to read manifest '{}': {error}",
            path.display()
        ))
    })?;

    serde_json::from_slice(&bytes).map_err(|error| {
        Undr9Error::Serialization(format!(
            "failed to deserialize manifest '{}': {error}",
            path.display()
        ))
    })
}

pub fn persist_manifest(path: &Path, manifest: &Manifest) -> Result<()> {
    let payload = serde_json::to_vec_pretty(manifest).map_err(|error| {
        Undr9Error::Serialization(format!(
            "failed to serialize manifest '{}': {error}",
            path.display()
        ))
    })?;
    write_atomically(path, &payload)
}

#[doc(hidden)]
pub fn install_storage_io_failpoint(operation: &'static str, path_fragment: impl Into<String>) {
    let mut guard = storage_io_failpoint_slot()
        .lock()
        .expect("storage failpoint mutex should not be poisoned");
    *guard = Some(StorageIoFailpoint {
        operation,
        path_fragment: path_fragment.into(),
    });
}

#[doc(hidden)]
pub fn clear_storage_io_failpoint() {
    let mut guard = storage_io_failpoint_slot()
        .lock()
        .expect("storage failpoint mutex should not be poisoned");
    *guard = None;
}

pub fn manifest_checksum(manifest: &Manifest) -> Result<u32> {
    let payload = serde_json::to_vec(manifest).map_err(|error| {
        Undr9Error::Serialization(format!("failed to serialize manifest: {error}"))
    })?;
    Ok(crc32(&payload))
}

fn load_node_state(
    layout: &StorageLayout,
    manifest: &Manifest,
) -> Result<BTreeMap<NodeId, NodeRecord>> {
    let Some(path) = resolve_snapshot_path(
        &layout.node_segment_path(),
        &layout.legacy_node_segment_path(),
    ) else {
        return Ok(BTreeMap::new());
    };

    let bytes = read_verified_snapshot(layout, manifest, &path)?;
    let snapshot = load_node_snapshot_from_bytes(&path, &bytes)?;

    if snapshot.format_version != 1 {
        return Err(Undr9Error::Corruption(format!(
            "unsupported node snapshot format version {}",
            snapshot.format_version
        )));
    }

    snapshot
        .records
        .into_iter()
        .map(|record| record.into_node().map(|node| (node.id.clone(), node)))
        .collect::<Result<BTreeMap<_, _>>>()
}

fn load_published_state(
    layout: &StorageLayout,
    manifest: &Manifest,
) -> Result<(BTreeMap<NodeId, NodeRecord>, BTreeMap<EdgeId, EdgeRecord>)> {
    let mut nodes = load_node_state(layout, manifest)?;
    let mut edges = load_edge_state(layout, manifest)?;
    attach_vectors(layout, manifest, &mut nodes)?;
    apply_delta_segments(layout, manifest, &mut nodes, &mut edges)?;
    Ok((nodes, edges))
}

fn load_raw_published_state(
    layout: &StorageLayout,
) -> Result<(BTreeMap<NodeId, NodeRecord>, BTreeMap<EdgeId, EdgeRecord>)> {
    let mut nodes = load_raw_node_state(layout)?;
    let mut edges = load_raw_edge_state(layout)?;
    attach_raw_vectors(layout, &mut nodes)?;
    apply_raw_delta_segments(layout, &mut nodes, &mut edges)?;
    Ok((nodes, edges))
}

fn load_raw_node_state(layout: &StorageLayout) -> Result<BTreeMap<NodeId, NodeRecord>> {
    let Some(path) = resolve_snapshot_path(
        &layout.node_segment_path(),
        &layout.legacy_node_segment_path(),
    ) else {
        return Ok(BTreeMap::new());
    };

    let bytes = fs::read(&path).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to read node snapshot '{}': {error}",
            path.display()
        ))
    })?;
    let snapshot = load_node_snapshot_from_bytes(&path, &bytes)?;

    snapshot
        .records
        .into_iter()
        .map(|record| record.into_node().map(|node| (node.id.clone(), node)))
        .collect::<Result<BTreeMap<_, _>>>()
}

fn load_edge_state(
    layout: &StorageLayout,
    manifest: &Manifest,
) -> Result<BTreeMap<EdgeId, EdgeRecord>> {
    let Some(path) = resolve_snapshot_path(
        &layout.edge_segment_path(),
        &layout.legacy_edge_segment_path(),
    ) else {
        return Ok(BTreeMap::new());
    };

    let bytes = read_verified_snapshot(layout, manifest, &path)?;
    let snapshot = load_edge_snapshot_from_bytes(&path, &bytes)?;

    if snapshot.format_version != 1 {
        return Err(Undr9Error::Corruption(format!(
            "unsupported edge snapshot format version {}",
            snapshot.format_version
        )));
    }

    snapshot
        .records
        .into_iter()
        .map(|record| record.into_edge().map(|edge| (edge.id.clone(), edge)))
        .collect::<Result<BTreeMap<_, _>>>()
}

fn load_raw_edge_state(layout: &StorageLayout) -> Result<BTreeMap<EdgeId, EdgeRecord>> {
    let Some(path) = resolve_snapshot_path(
        &layout.edge_segment_path(),
        &layout.legacy_edge_segment_path(),
    ) else {
        return Ok(BTreeMap::new());
    };

    let bytes = fs::read(&path).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to read edge snapshot '{}': {error}",
            path.display()
        ))
    })?;
    let snapshot = load_edge_snapshot_from_bytes(&path, &bytes)?;

    snapshot
        .records
        .into_iter()
        .map(|record| record.into_edge().map(|edge| (edge.id.clone(), edge)))
        .collect::<Result<BTreeMap<_, _>>>()
}

fn attach_vectors(
    layout: &StorageLayout,
    manifest: &Manifest,
    nodes: &mut BTreeMap<NodeId, NodeRecord>,
) -> Result<()> {
    let Some(path) = resolve_snapshot_path(
        &layout.vector_segment_path(),
        &layout.legacy_vector_segment_path(),
    ) else {
        return Ok(());
    };

    let bytes = read_verified_snapshot(layout, manifest, &path)?;
    let snapshot = load_vector_snapshot_from_bytes(&path, &bytes)?;

    for record in snapshot.records {
        let (node_id, vectors) = record.into_parts()?;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.vectors = vectors;
        }
    }
    Ok(())
}

fn apply_delta_segments(
    layout: &StorageLayout,
    manifest: &Manifest,
    nodes: &mut BTreeMap<NodeId, NodeRecord>,
    edges: &mut BTreeMap<EdgeId, EdgeRecord>,
) -> Result<()> {
    for path in manifest_delta_segment_paths(layout, manifest)? {
        let bytes = read_verified_file(layout, manifest, &path)?;
        let snapshot: DeltaSegmentSnapshot = serde_json::from_slice(&bytes).map_err(|error| {
            Undr9Error::Corruption(format!(
                "failed to deserialize delta segment '{}': {error}",
                path.display()
            ))
        })?;
        if snapshot.format_version != 1 {
            return Err(Undr9Error::Corruption(format!(
                "unsupported delta segment format version {} in '{}'",
                snapshot.format_version,
                path.display()
            )));
        }
        for entry in snapshot.entries {
            apply_batch_to_maps(nodes, edges, &entry.batch)?;
        }
    }
    Ok(())
}

fn apply_raw_delta_segments(
    layout: &StorageLayout,
    nodes: &mut BTreeMap<NodeId, NodeRecord>,
    edges: &mut BTreeMap<EdgeId, EdgeRecord>,
) -> Result<()> {
    for path in raw_delta_segment_paths(layout)? {
        let bytes = fs::read(&path).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to read delta segment '{}': {error}",
                path.display()
            ))
        })?;
        let snapshot: DeltaSegmentSnapshot = serde_json::from_slice(&bytes).map_err(|error| {
            Undr9Error::Corruption(format!(
                "failed to deserialize delta segment '{}': {error}",
                path.display()
            ))
        })?;
        if snapshot.format_version != 1 {
            return Err(Undr9Error::Corruption(format!(
                "unsupported delta segment format version {} in '{}'",
                snapshot.format_version,
                path.display()
            )));
        }
        for entry in snapshot.entries {
            apply_batch_to_maps(nodes, edges, &entry.batch)?;
        }
    }
    Ok(())
}

fn attach_raw_vectors(
    layout: &StorageLayout,
    nodes: &mut BTreeMap<NodeId, NodeRecord>,
) -> Result<()> {
    let Some(path) = resolve_snapshot_path(
        &layout.vector_segment_path(),
        &layout.legacy_vector_segment_path(),
    ) else {
        return Ok(());
    };

    let bytes = fs::read(&path).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to read vector snapshot '{}': {error}",
            path.display()
        ))
    })?;
    let snapshot = load_vector_snapshot_from_bytes(&path, &bytes)?;

    for record in snapshot.records {
        let (node_id, vectors) = record.into_parts()?;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.vectors = vectors;
        }
    }
    Ok(())
}

fn normalize_write_batch(mut batch: WriteBatch) -> Result<WriteBatch> {
    for node in &mut batch.nodes_upserted {
        node.normalize_memory_metadata()?;
    }
    for edge in &batch.edges_upserted {
        validate_namespaced_edge(edge)?;
    }
    Ok(batch)
}

fn serialize_write_batch(batch: &WriteBatch) -> Result<Vec<u8>> {
    serde_json::to_vec(batch).map_err(|error| {
        Undr9Error::Serialization(format!("failed to serialize write batch: {error}"))
    })
}

fn deserialize_write_batch(payload: &[u8], context: &str) -> Result<WriteBatch> {
    serde_json::from_slice(payload)
        .or_else(|_| postcard::from_bytes(payload))
        .map_err(|error| {
            Undr9Error::Corruption(format!(
                "failed to deserialize WAL write batch during {context}: {error}"
            ))
        })
}

fn validate_namespaced_edge(edge: &EdgeRecord) -> Result<()> {
    let source_namespace = edge
        .source
        .as_str()
        .split_once(':')
        .map(|(namespace, _)| namespace);
    let target_namespace = edge
        .target
        .as_str()
        .split_once(':')
        .map(|(namespace, _)| namespace);
    if source_namespace != target_namespace {
        return Err(Undr9Error::Validation(format!(
            "edge '{}' cannot cross namespaces between '{}' and '{}'",
            edge.id, edge.source, edge.target
        )));
    }
    Ok(())
}

fn batch_for_consolidation_event(
    event: &ConsolidationEvent,
    nodes: &BTreeMap<NodeId, NodeRecord>,
) -> WriteBatch {
    match event.action {
        ConsolidationAction::Merge => {
            let mut batch = WriteBatch::default();
            for duplicate_id in &event.related_node_ids {
                if let Some(node) = nodes.get(duplicate_id).cloned() {
                    let mut merged = node;
                    merged.properties.insert(
                        "merged_into".to_owned(),
                        undr9_core::PropertyValue::String(event.target_node_id.to_string()),
                    );
                    merged.properties.insert(
                        "archived".to_owned(),
                        undr9_core::PropertyValue::Boolean(true),
                    );
                    batch.nodes_upserted.push(merged);
                }
            }
            batch
        }
        ConsolidationAction::Demote => {
            let mut batch = WriteBatch::default();
            if let Some(node) = nodes.get(&event.target_node_id).cloned() {
                let mut demoted = node;
                let current = demoted.importance().unwrap_or(0.5);
                demoted.properties.insert(
                    "importance".to_owned(),
                    undr9_core::PropertyValue::Float((current * 0.5) as f64),
                );
                batch.nodes_upserted.push(demoted);
            }
            batch
        }
        ConsolidationAction::Archive => {
            let mut batch = WriteBatch::default();
            if let Some(node) = nodes.get(&event.target_node_id).cloned() {
                let mut archived = node;
                archived.properties.insert(
                    "archived".to_owned(),
                    undr9_core::PropertyValue::Boolean(true),
                );
                batch.nodes_upserted.push(archived);
            }
            batch
        }
        ConsolidationAction::Link => WriteBatch::default(),
    }
}

fn append_json_line<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to create consolidation log directory '{}': {error}",
                parent.display()
            ))
        })?;
    }

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| {
            Undr9Error::Io(format!(
                "failed to open consolidation log '{}': {error}",
                path.display()
            ))
        })?;
    let payload = serde_json::to_string(value).map_err(|error| {
        Undr9Error::Serialization(format!("failed to serialize json line: {error}"))
    })?;
    writeln!(file, "{payload}")
        .map_err(|error| Undr9Error::Io(format!("failed to append json line: {error}")))?;
    Ok(())
}

fn read_verified_snapshot(
    layout: &StorageLayout,
    manifest: &Manifest,
    path: &Path,
) -> Result<Vec<u8>> {
    read_verified_file(layout, manifest, path)
}

fn read_verified_file(layout: &StorageLayout, manifest: &Manifest, path: &Path) -> Result<Vec<u8>> {
    let relative = relative_path(layout, path)?;
    let bytes = fs::read(path).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to read snapshot '{}': {error}",
            path.display()
        ))
    })?;

    if let Some(file) = manifest.files.get(&relative) {
        let actual = crc32(&bytes);
        if file.checksum_crc32 != 0 && file.checksum_crc32 != actual {
            return Err(Undr9Error::Corruption(format!(
                "checksum mismatch for snapshot '{}'",
                path.display()
            )));
        }
    }

    Ok(bytes)
}

fn manifest_delta_segment_paths(
    layout: &StorageLayout,
    manifest: &Manifest,
) -> Result<Vec<PathBuf>> {
    let mut paths = manifest
        .files
        .values()
        .filter(|file| file.relative_path.starts_with("deltas/"))
        .map(|file| layout.root_dir.join(&file.relative_path))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn raw_delta_segment_paths(layout: &StorageLayout) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(layout.delta_directory()).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to enumerate delta directory '{}': {error}",
            layout.delta_directory().display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            Undr9Error::Io(format!(
                "failed to read delta directory entry in '{}': {error}",
                layout.delta_directory().display()
            ))
        })?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some(DELTA_SEGMENT_FILE_EXTENSION) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn resolve_snapshot_path(primary: &Path, legacy: &Path) -> Option<PathBuf> {
    if primary.exists() {
        Some(primary.to_path_buf())
    } else if legacy.exists() {
        Some(legacy.to_path_buf())
    } else {
        None
    }
}

fn load_node_snapshot_from_bytes(path: &Path, bytes: &[u8]) -> Result<NodeSegmentSnapshot> {
    if path.extension().and_then(|value| value.to_str()) == Some("json") {
        serde_json::from_slice(bytes).map_err(|error| {
            Undr9Error::Corruption(format!(
                "failed to deserialize legacy node snapshot '{}': {error}",
                path.display()
            ))
        })
    } else {
        rkyv::from_bytes::<NodeSegmentSnapshot>(bytes).map_err(|error| {
            Undr9Error::Corruption(format!(
                "failed to deserialize node snapshot '{}': {error}",
                path.display()
            ))
        })
    }
}

fn load_edge_snapshot_from_bytes(path: &Path, bytes: &[u8]) -> Result<EdgeSegmentSnapshot> {
    if path.extension().and_then(|value| value.to_str()) == Some("json") {
        serde_json::from_slice(bytes).map_err(|error| {
            Undr9Error::Corruption(format!(
                "failed to deserialize legacy edge snapshot '{}': {error}",
                path.display()
            ))
        })
    } else {
        rkyv::from_bytes::<EdgeSegmentSnapshot>(bytes).map_err(|error| {
            Undr9Error::Corruption(format!(
                "failed to deserialize edge snapshot '{}': {error}",
                path.display()
            ))
        })
    }
}

fn load_vector_snapshot_from_bytes(path: &Path, bytes: &[u8]) -> Result<VectorSegmentSnapshot> {
    if path.extension().and_then(|value| value.to_str()) == Some("json") {
        serde_json::from_slice(bytes).map_err(|error| {
            Undr9Error::Corruption(format!(
                "failed to deserialize legacy vector snapshot '{}': {error}",
                path.display()
            ))
        })
    } else {
        rkyv::from_bytes::<VectorSegmentSnapshot>(bytes).map_err(|error| {
            Undr9Error::Corruption(format!(
                "failed to deserialize vector snapshot '{}': {error}",
                path.display()
            ))
        })
    }
}

fn persist_rkyv_snapshot<T>(path: &Path, payload: &T) -> Result<u32>
where
    T: RkyvArchive + RkyvSerialize<rkyv::ser::serializers::AllocSerializer<256>>,
{
    let bytes = rkyv::to_bytes::<_, 256>(payload).map_err(|error| {
        Undr9Error::Serialization(format!(
            "failed to serialize snapshot '{}': {error}",
            path.display()
        ))
    })?;
    let checksum = crc32(bytes.as_ref());
    write_atomically(path, &bytes)?;
    Ok(checksum)
}

fn persist_delta_segment(path: &Path, payload: &DeltaSegmentSnapshot) -> Result<u32> {
    let bytes = serde_json::to_vec_pretty(payload).map_err(|error| {
        Undr9Error::Serialization(format!(
            "failed to serialize delta segment '{}': {error}",
            path.display()
        ))
    })?;
    let checksum = crc32(&bytes);
    write_atomically(path, &bytes)?;
    Ok(checksum)
}

fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        Undr9Error::Io(format!(
            "path '{}' does not have a parent directory",
            path.display()
        ))
    })?;

    maybe_fail_storage_io("create_dir_all", parent)?;
    fs::create_dir_all(parent).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to create parent directory '{}': {error}",
            parent.display()
        ))
    })?;

    let temp_path = path.with_extension("tmp");
    maybe_fail_storage_io("temp_write", &temp_path)?;
    fs::write(&temp_path, bytes).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to write temp file '{}': {error}",
            temp_path.display()
        ))
    })?;
    maybe_fail_storage_io("rename", path)?;
    fs::rename(&temp_path, path).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to publish file atomically '{}' -> '{}': {error}",
            temp_path.display(),
            path.display()
        ))
    })?;

    Ok(())
}

fn storage_io_failpoint_slot() -> &'static Mutex<Option<StorageIoFailpoint>> {
    STORAGE_IO_FAILPOINT.get_or_init(|| Mutex::new(None))
}

fn maybe_fail_storage_io(operation: &'static str, path: &Path) -> Result<()> {
    let guard = storage_io_failpoint_slot()
        .lock()
        .expect("storage failpoint mutex should not be poisoned");
    if let Some(failpoint) = guard.as_ref() {
        let matches_operation = failpoint.operation == operation;
        let matches_path = path
            .to_string_lossy()
            .contains(failpoint.path_fragment.as_str());
        if matches_operation && matches_path {
            return Err(Undr9Error::Io(format!(
                "injected storage I/O failure for operation '{operation}' on '{}': No space left on device",
                path.display()
            )));
        }
    }
    Ok(())
}

fn relative_path(layout: &StorageLayout, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(&layout.root_dir).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to derive relative path for '{}': {error}",
            path.display()
        ))
    })?;

    Ok(relative.to_string_lossy().into_owned())
}

fn backup_manifest_path(root: &Path) -> PathBuf {
    root.join(BACKUP_MANIFEST_FILE_NAME)
}

fn trim_restored_wal_to_lsn(root: &Path, target_lsn: u64, wal_config: &WalConfig) -> Result<()> {
    let layout = StorageLayout::new(root);
    let checkpoint_lsn = load_manifest(&layout.manifest_path())
        .ok()
        .and_then(|manifest| manifest.last_applied_lsn)
        .unwrap_or(0);
    if target_lsn < checkpoint_lsn {
        return Err(Undr9Error::Validation(format!(
            "requested restore target lsn {target_lsn} is older than checkpoint lsn {checkpoint_lsn}"
        )));
    }

    let wal_root = layout.subdirectory("wal");
    let records = undr9_wal::replay_from_dir(&wal_root, wal_config.max_replay_bytes)?;
    let latest_available_lsn = records
        .last()
        .map(|record| record.header.lsn.0)
        .unwrap_or(checkpoint_lsn);
    if target_lsn > latest_available_lsn {
        return Err(Undr9Error::Validation(format!(
            "requested restore target lsn {target_lsn} exceeds latest available lsn {latest_available_lsn}"
        )));
    }

    let retained = records
        .into_iter()
        .filter(|record| record.header.lsn.0 <= target_lsn)
        .collect::<Vec<_>>();
    undr9_wal::rewrite_dir_from_records(&wal_root, wal_config, &retained)
}

fn persist_backup_manifest(source: &Path, destination: &Path) -> Result<()> {
    let layout = StorageLayout::new(destination);
    let integrity = verify_storage_layout(&layout)?;
    let files = collect_backup_files(destination)?;
    let manifest = BackupManifest {
        source_root: source.display().to_string(),
        file_count: files.len(),
        files,
        integrity,
    };
    let payload = serde_json::to_vec_pretty(&manifest).map_err(|error| {
        Undr9Error::Serialization(format!(
            "failed to serialize backup manifest '{}': {error}",
            backup_manifest_path(destination).display()
        ))
    })?;
    write_atomically(&backup_manifest_path(destination), &payload)
}

fn load_backup_manifest(root: &Path) -> Result<BackupManifest> {
    let path = backup_manifest_path(root);
    let bytes = fs::read(&path).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to read backup manifest '{}': {error}",
            path.display()
        ))
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        Undr9Error::Serialization(format!(
            "failed to deserialize backup manifest '{}': {error}",
            path.display()
        ))
    })
}

fn verify_backup_directory(root: &Path) -> Result<BackupManifest> {
    let manifest = load_backup_manifest(root)?;
    let actual = collect_backup_files(root)?;
    if actual != manifest.files {
        return Err(Undr9Error::Corruption(format!(
            "backup manifest '{}' does not match copied files",
            backup_manifest_path(root).display()
        )));
    }
    let integrity = verify_storage_layout(&StorageLayout::new(root))?;
    if integrity.manifest_present != manifest.integrity.manifest_present
        || integrity.node_snapshot_valid != manifest.integrity.node_snapshot_valid
        || integrity.edge_snapshot_valid != manifest.integrity.edge_snapshot_valid
        || !integrity.wal_replay_valid
        || integrity.node_count != manifest.integrity.node_count
        || integrity.edge_count != manifest.integrity.edge_count
    {
        return Err(Undr9Error::Corruption(format!(
            "backup integrity report for '{}' no longer matches current storage state",
            root.display()
        )));
    }
    Ok(manifest)
}

fn collect_backup_files(root: &Path) -> Result<BTreeMap<String, ManifestFile>> {
    let mut files = BTreeMap::new();
    collect_backup_files_recursive(root, root, &mut files)?;
    Ok(files)
}

fn collect_backup_files_recursive(
    root: &Path,
    current: &Path,
    files: &mut BTreeMap<String, ManifestFile>,
) -> Result<()> {
    for entry in fs::read_dir(current).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to enumerate backup directory '{}': {error}",
            current.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            Undr9Error::Io(format!(
                "failed to read backup directory entry in '{}': {error}",
                current.display()
            ))
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            Undr9Error::Io(format!(
                "failed to inspect backup directory entry '{}': {error}",
                path.display()
            ))
        })?;
        if file_type.is_dir() {
            collect_backup_files_recursive(root, &path, files)?;
            continue;
        }
        let relative = path.strip_prefix(root).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to derive backup relative path for '{}': {error}",
                path.display()
            ))
        })?;
        let relative = relative.to_string_lossy().into_owned();
        if relative == BACKUP_MANIFEST_FILE_NAME {
            continue;
        }
        let bytes = fs::read(&path).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to read backup file '{}': {error}",
                path.display()
            ))
        })?;
        files.insert(
            relative.clone(),
            ManifestFile {
                relative_path: relative,
                checksum_crc32: crc32(&bytes),
            },
        );
    }
    Ok(())
}

fn remove_manifest_files_with_prefix(manifest: &mut Manifest, prefix: &str) -> Vec<String> {
    let keys = manifest
        .files
        .keys()
        .filter(|key| key.starts_with(prefix))
        .cloned()
        .collect::<Vec<_>>();
    for key in &keys {
        manifest.files.remove(key);
    }
    keys
}

fn copy_directory_recursive(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to create directory '{}': {error}",
            destination.display()
        ))
    })?;

    for entry in fs::read_dir(source).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to enumerate directory '{}': {error}",
            source.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            Undr9Error::Io(format!(
                "failed to read directory entry in '{}': {error}",
                source.display()
            ))
        })?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());

        if entry
            .file_type()
            .map_err(|error| {
                Undr9Error::Io(format!(
                    "failed to inspect directory entry '{}': {error}",
                    source_path.display()
                ))
            })?
            .is_dir()
        {
            copy_directory_recursive(&source_path, &destination_path)?;
        } else {
            fs::copy(&source_path, &destination_path).map_err(|error| {
                Undr9Error::Io(format!(
                    "failed to copy '{}' -> '{}': {error}",
                    source_path.display(),
                    destination_path.display()
                ))
            })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use undr9_common::{EdgeId, NodeId};
    use undr9_config::AppConfig;
    use undr9_core::{EdgeRecord, NodeRecord};

    #[test]
    fn bootstraps_storage_layout_and_manifest() {
        let tempdir = tempdir().expect("tempdir should be created");
        let mut config = AppConfig::default();
        config.storage.root_dir = tempdir.path().join("data");

        let (layout, manifest) = super::bootstrap(&config.storage).expect("storage bootstrap");

        assert!(layout.manifest_path().exists());
        assert_eq!(manifest.storage_version, "1");
        assert!(layout.subdirectory("wal").exists());
        assert!(layout.subdirectory("nodes").exists());
    }

    #[test]
    fn persists_and_reads_manifest() {
        let tempdir = tempdir().expect("tempdir should be created");
        let path = tempdir.path().join(super::MANIFEST_FILE_NAME);
        let manifest = super::Manifest {
            storage_version: "1".to_owned(),
            files: Default::default(),
            settings: super::ManifestSettings {
                create_if_missing: true,
            },
            last_clean_shutdown: true,
            last_applied_lsn: Some(7),
        };

        super::persist_manifest(&path, &manifest).expect("manifest should be written");
        let loaded = super::load_manifest(&path).expect("manifest should be read");

        assert_eq!(loaded, manifest);
        assert!(super::manifest_checksum(&loaded).is_ok());
    }

    #[test]
    fn cascades_edges_when_deleting_a_node() {
        let tempdir = tempdir().expect("tempdir should be created");
        let mut config = AppConfig::default();
        config.storage.root_dir = tempdir.path().join("data");

        let mut engine = super::StorageEngine::open(&config).expect("engine should open");
        let node_a = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
            .expect("node should build");
        let node_b = NodeRecord::new(NodeId::new("node_b").expect("valid id"), "memory")
            .expect("node should build");
        let edge = EdgeRecord::new(
            EdgeId::new("edge_a").expect("valid id"),
            node_a.id.clone(),
            node_b.id.clone(),
            "related_to",
        )
        .expect("edge should build");

        engine
            .upsert_node(node_a.clone())
            .expect("node insert should work");
        engine
            .upsert_node(node_b.clone())
            .expect("node insert should work");
        engine.upsert_edge(edge).expect("edge insert should work");

        let removed = engine
            .delete_node(&node_a.id)
            .expect("delete should work")
            .expect("node should exist");
        assert_eq!(removed.id, node_a.id);
        assert_eq!(engine.edge_count(), 0);
    }

    #[test]
    fn verifies_backup_restore_and_integrity() {
        let tempdir = tempdir().expect("tempdir should be created");
        let mut config = AppConfig::default();
        config.storage.root_dir = tempdir.path().join("data");

        let mut engine = super::StorageEngine::open(&config).expect("engine should open");
        let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
            .expect("node should build");
        engine.upsert_node(node).expect("node insert should work");
        engine.compact().expect("compaction should work");

        let report = engine.verify_integrity().expect("integrity should verify");
        assert!(report.node_snapshot_valid);
        assert!(report.wal_replay_valid);

        let backup_dir = tempdir.path().join("backup");
        engine
            .backup_to(&backup_dir)
            .expect("backup should succeed");

        let restore_dir = tempdir.path().join("restore");
        super::restore_directory(&backup_dir, &restore_dir).expect("restore should succeed");

        let restored_layout = super::StorageLayout::new(&restore_dir);
        let restored_report =
            super::verify_storage_layout(&restored_layout).expect("restored data should verify");
        assert_eq!(restored_report.node_count, 1);
    }

    #[test]
    fn repairs_manifest_checksum_drift() {
        let tempdir = tempdir().expect("tempdir should be created");
        let mut config = AppConfig::default();
        config.storage.root_dir = tempdir.path().join("data");

        let mut engine = super::StorageEngine::open(&config).expect("engine should open");
        let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
            .expect("node should build");
        engine.upsert_node(node).expect("node insert should work");

        let mut manifest =
            super::load_manifest(&engine.layout().manifest_path()).expect("manifest should load");
        if let Some(file) = manifest
            .files
            .get_mut("nodes/segment-0000000000000001.snapshot.rkyv")
        {
            file.checksum_crc32 = 1;
        }
        super::persist_manifest(&engine.layout().manifest_path(), &manifest)
            .expect("corrupt manifest should be written");

        let report = super::repair_storage(&config).expect("repair should succeed");
        assert!(report.node_snapshot_valid);
        assert!(report.issues.is_empty());
    }
}
