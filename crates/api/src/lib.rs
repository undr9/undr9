use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, MatchedPath, Path, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use futures_util::stream;
use im::OrdMap;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::Instrument;
use undr9_auth::{Action, ApiKeyAuthenticator, Authorizer, Principal};
use undr9_cluster::{ClusterManager, ClusterTopology, FailoverPlan};
use undr9_common::{EdgeId, NodeId, Result as Undr9Result, TransactionId, Undr9Error};
use undr9_config::{AppConfig, VectorIndexConfig};
use undr9_core::{
    EdgeRecord, IsolationLevel, NodeRecord, TransactionCommitResult, TransactionOperation,
    TransactionSummary, WriteBatch,
};
use undr9_index::{GraphIndex, IndexSnapshot, VectorIndexLoadConfig};
use undr9_observability::{
    annotate_active_span_error, annotate_active_span_success, append_audit_event,
    export_audit_events, now_epoch_ms, prune_audit_log, AuditEvent, EndpointMetricsSnapshot,
    ErrorCounterSnapshot, LatencyHistogramSnapshot, MetricsSnapshot, ServiceCounters,
    StructuredLogEvent,
};
use undr9_query::{
    Executor, GraphMutation, GraphPath, GraphSnapshot, OverlayGraphView, PlanKind, QueryExecution,
    QueryExecutionItem, QueryRequest, QueryResponse, RankedNodeResult,
};
use undr9_replication::{
    ReplicationManager, ReplicationMode, ReplicationRecord, ReplicationStatus,
};
use undr9_storage::{
    repair_storage, restore_directory, restore_directory_to_lsn, IntegrityReport, StorageEngine,
    TransactionDelta,
};

const REPLICATION_METADATA_FILE_NAME: &str = "replication-state.json";
const CLUSTER_METADATA_FILE_NAME: &str = "cluster-topology.json";
const MAX_QUERY_RESPONSE_ITEMS: usize = 1_024;
const MAX_STREAM_QUERY_ITEMS: usize = 2_048;
const MAX_STREAM_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const TRACE_ID_HEADER: &str = "x-undr9-trace-id";

#[derive(Debug)]
pub struct Database {
    engine: StorageEngine,
    indexes: GraphIndex,
    vector_index_config: VectorIndexConfig,
    published_snapshot: Arc<GraphSnapshot>,
    replication: ReplicationManager,
    cluster: ClusterManager,
    wal_replay_latency_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ApiState {
    pub service_name: Arc<str>,
    pub config: AppConfig,
    readiness: Arc<AtomicBool>,
    pub database: Arc<RwLock<Database>>,
    pub counters: Arc<AtomicServiceCounters>,
    pub audit_log_path: Arc<PathBuf>,
    maintenance_status: Arc<RwLock<MaintenanceStatusResponse>>,
    endpoint_metrics: Arc<DashMap<String, AtomicEndpointMetrics>>,
    rate_limits: Arc<DashMap<String, TokenBucket>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub service: String,
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub code: &'static str,
    pub message: String,
    pub details: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MaintenanceResponse {
    pub status: &'static str,
    pub operation: &'static str,
    pub elapsed_ms: u64,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MaintenanceStatusResponse {
    in_progress: bool,
    last_operation: Option<String>,
    last_outcome: Option<String>,
    detail: Option<String>,
    started_at_ms: Option<u128>,
    finished_at_ms: Option<u128>,
    elapsed_ms: Option<u64>,
    last_node_count: usize,
    last_edge_count: usize,
    max_node_count: usize,
    max_edge_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplicationStatusResponse {
    pub status: ReplicationStatus,
    pub replica_lag: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct BackupRequest {
    destination: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RestoreRequest {
    source: String,
    target_lsn: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct BeginTransactionRequest {
    isolation_level: Option<IsolationLevel>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReplicationHistoryQuery {
    after_source_lsn: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConfigureFollowerRequest {
    leader_node_id: String,
    leader_address: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegisterReplicaRequest {
    node_id: String,
    address: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ReplicaAckRequest {
    replica_node_id: String,
    source_lsn: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ApplyReplicationRequest {
    records: Vec<ReplicationRecord>,
}

#[derive(Debug, Clone, Deserialize)]
struct MarkNodeHealthRequest {
    healthy: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct PromoteNodeRequest {
    node_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct AuditExportRequest {
    limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuditExportResponse {
    events: Vec<AuditEvent>,
}

#[derive(Debug, Clone, Default)]
struct StreamRenderSummary {
    ranked_scores: Vec<f32>,
}

#[derive(Debug, Clone)]
struct ApiError {
    status: StatusCode,
    body: ErrorResponse,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum QueryStreamFrame {
    Meta {
        plan_kind: PlanKind,
        retrieval_profile: Option<String>,
    },
    Node {
        node: NodeRecord,
    },
    Edge {
        edge: EdgeRecord,
    },
    RankedResult {
        result: RankedNodeResult,
    },
    Path {
        path: GraphPath,
    },
    End {
        item_count: usize,
    },
}

#[derive(Debug, Clone, Default)]
struct BatchMutation {
    removed_nodes: BTreeMap<NodeId, NodeRecord>,
    removed_edges: BTreeMap<EdgeId, EdgeRecord>,
    added_nodes: BTreeMap<NodeId, NodeRecord>,
    added_edges: BTreeMap<EdgeId, EdgeRecord>,
}

impl From<TransactionDelta> for BatchMutation {
    fn from(delta: TransactionDelta) -> Self {
        Self {
            removed_nodes: delta.removed_nodes,
            removed_edges: delta.removed_edges,
            added_nodes: delta.added_nodes,
            added_edges: delta.added_edges,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TokenBucket {
    tokens: f64,
    last_refill_ms: u128,
}

#[derive(Debug, Default)]
struct AtomicLatencyHistogram {
    observations_total: AtomicU64,
    sum_ms: AtomicU64,
    le_10ms_total: AtomicU64,
    le_50ms_total: AtomicU64,
    le_250ms_total: AtomicU64,
    le_1000ms_total: AtomicU64,
    gt_1000ms_total: AtomicU64,
}

#[derive(Debug, Default)]
pub struct AtomicServiceCounters {
    query_requests_total: AtomicU64,
    write_requests_total: AtomicU64,
    maintenance_operations_total: AtomicU64,
    audit_events_total: AtomicU64,
    query_latency: AtomicLatencyHistogram,
    transaction_latency: AtomicLatencyHistogram,
    traversal_latency: AtomicLatencyHistogram,
    vector_search_latency: AtomicLatencyHistogram,
    ranked_retrieval_latency: AtomicLatencyHistogram,
    compaction_latency: AtomicLatencyHistogram,
    retrieval_score_bucket_low_total: AtomicU64,
    retrieval_score_bucket_medium_total: AtomicU64,
    retrieval_score_bucket_high_total: AtomicU64,
    retrieval_score_bucket_top_total: AtomicU64,
    error_counters: DashMap<String, AtomicU64>,
}

#[derive(Debug, Default)]
struct AtomicEndpointMetrics {
    requests_total: AtomicU64,
    responses_2xx_total: AtomicU64,
    responses_4xx_total: AtomicU64,
    responses_5xx_total: AtomicU64,
    latency_ms_total: AtomicU64,
    latency_le_10ms_total: AtomicU64,
    latency_le_50ms_total: AtomicU64,
    latency_le_250ms_total: AtomicU64,
    latency_gt_250ms_total: AtomicU64,
}

impl AtomicServiceCounters {
    fn snapshot(&self, wal_replay_latency_ms: u64) -> ServiceCounters {
        ServiceCounters {
            query_requests_total: self.query_requests_total.load(Ordering::Relaxed),
            write_requests_total: self.write_requests_total.load(Ordering::Relaxed),
            maintenance_operations_total: self.maintenance_operations_total.load(Ordering::Relaxed),
            audit_events_total: self.audit_events_total.load(Ordering::Relaxed),
            query_latency: self.query_latency.snapshot(),
            transaction_latency: self.transaction_latency.snapshot(),
            traversal_latency: self.traversal_latency.snapshot(),
            vector_search_latency: self.vector_search_latency.snapshot(),
            ranked_retrieval_latency: self.ranked_retrieval_latency.snapshot(),
            compaction_latency: self.compaction_latency.snapshot(),
            recovery_duration: histogram_with_single_gauge(wal_replay_latency_ms),
            wal_lag_records: 0,
            wal_replay_latency_ms,
            memory_pressure_bytes: 0,
            cache_pressure_bytes: 0,
            retrieval_score_bucket_low_total: self
                .retrieval_score_bucket_low_total
                .load(Ordering::Relaxed),
            retrieval_score_bucket_medium_total: self
                .retrieval_score_bucket_medium_total
                .load(Ordering::Relaxed),
            retrieval_score_bucket_high_total: self
                .retrieval_score_bucket_high_total
                .load(Ordering::Relaxed),
            retrieval_score_bucket_top_total: self
                .retrieval_score_bucket_top_total
                .load(Ordering::Relaxed),
            error_counters: snapshot_error_counters(&self.error_counters),
        }
    }
}

impl AtomicLatencyHistogram {
    fn record(&self, elapsed_ms: u64) {
        self.observations_total.fetch_add(1, Ordering::Relaxed);
        self.sum_ms.fetch_add(elapsed_ms, Ordering::Relaxed);
        match elapsed_ms {
            0..=10 => {
                self.le_10ms_total.fetch_add(1, Ordering::Relaxed);
            }
            11..=50 => {
                self.le_50ms_total.fetch_add(1, Ordering::Relaxed);
            }
            51..=250 => {
                self.le_250ms_total.fetch_add(1, Ordering::Relaxed);
            }
            251..=1000 => {
                self.le_1000ms_total.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.gt_1000ms_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> LatencyHistogramSnapshot {
        LatencyHistogramSnapshot {
            observations_total: self.observations_total.load(Ordering::Relaxed),
            sum_ms: self.sum_ms.load(Ordering::Relaxed),
            le_10ms_total: self.le_10ms_total.load(Ordering::Relaxed),
            le_50ms_total: self.le_50ms_total.load(Ordering::Relaxed),
            le_250ms_total: self.le_250ms_total.load(Ordering::Relaxed),
            le_1000ms_total: self.le_1000ms_total.load(Ordering::Relaxed),
            gt_1000ms_total: self.gt_1000ms_total.load(Ordering::Relaxed),
        }
    }
}

fn histogram_with_single_gauge(value_ms: u64) -> LatencyHistogramSnapshot {
    let histogram = AtomicLatencyHistogram::default();
    histogram.record(value_ms);
    histogram.snapshot()
}

fn snapshot_error_counters(counters: &DashMap<String, AtomicU64>) -> Vec<ErrorCounterSnapshot> {
    let mut snapshots = counters
        .iter()
        .filter_map(|entry| {
            let (code, status) = entry.key().split_once('|')?;
            Some(ErrorCounterSnapshot {
                code: code.to_owned(),
                status: status.parse().ok()?,
                count: entry.value().load(Ordering::Relaxed),
            })
        })
        .collect::<Vec<_>>();
    snapshots.sort_by(|left, right| {
        left.code
            .cmp(&right.code)
            .then_with(|| left.status.cmp(&right.status))
    });
    snapshots
}

impl Database {
    pub fn open(config: &AppConfig) -> Undr9Result<Self> {
        Self::open_with_identity(config, "node-1", config.server.bind_address.clone())
    }

    pub fn open_with_identity(
        config: &AppConfig,
        local_node_id: impl Into<String>,
        advertise_address: impl Into<String>,
    ) -> Undr9Result<Self> {
        let started = Instant::now();
        let local_node_id = local_node_id.into();
        let advertise_address = advertise_address.into();
        let startup_span = tracing::info_span!(
            "database.startup",
            bind_address = %config.server.bind_address,
            storage_root = %config.storage.root_dir.display(),
            node_id = %local_node_id
        );
        let _startup_guard = startup_span.enter();
        let engine = StorageEngine::open(config)?;
        let nodes = engine.all_nodes();
        let edges = engine.all_edges();
        let allow_vector_index_load = engine.manifest().last_clean_shutdown
            && engine.latest_applied_lsn() == engine.manifest().last_applied_lsn;
        let vector_index_manifest_path = engine.layout().vector_index_manifest_path();
        let vector_index_graph_path = engine.layout().vector_index_graph_path();
        let vector_index_vectors_path = engine.layout().vector_index_vectors_path();
        let indexes = GraphIndex::rebuild_with_config_and_vector_index_load(
            &nodes,
            &edges,
            &config.vector_index,
            allow_vector_index_load.then_some(VectorIndexLoadConfig {
                manifest_path: &vector_index_manifest_path,
                graph_path: &vector_index_graph_path,
                vectors_path: &vector_index_vectors_path,
                expected_last_applied_lsn: engine.latest_applied_lsn(),
            }),
        );
        let replication = load_json_file(
            engine
                .layout()
                .subdirectory("meta")
                .join(REPLICATION_METADATA_FILE_NAME),
        )?
        .unwrap_or_else(|| ReplicationManager::single_node(local_node_id.clone()));
        let cluster = load_json_file(
            engine
                .layout()
                .subdirectory("meta")
                .join(CLUSTER_METADATA_FILE_NAME),
        )?
        .unwrap_or_else(|| ClusterManager::single_node_with_id(local_node_id, advertise_address));
        let published_snapshot = Arc::new(build_snapshot(&engine, &indexes));
        let database = Self {
            engine,
            indexes,
            vector_index_config: config.vector_index.clone(),
            published_snapshot,
            replication,
            cluster,
            wal_replay_latency_ms: started.elapsed().as_millis() as u64,
        };
        database.persist_index_snapshot()?;
        database.persist_replication_metadata()?;
        database.persist_cluster_topology()?;
        tracing::info!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            node_count = database.engine.node_count(),
            edge_count = database.engine.edge_count(),
            wal_replay_latency_ms = database.wal_replay_latency_ms,
            "database startup completed"
        );
        Ok(database)
    }

    pub fn snapshot(&self) -> Arc<GraphSnapshot> {
        Arc::clone(&self.published_snapshot)
    }

    pub fn graceful_shutdown(&mut self) -> Undr9Result<()> {
        self.engine.graceful_shutdown()?;
        let _ = self.persist_index_snapshot()?;
        Ok(())
    }

    pub fn upsert_node(&mut self, node: NodeRecord) -> Undr9Result<NodeRecord> {
        self.ensure_client_write_allowed()?;
        let batch = WriteBatch {
            nodes_upserted: vec![node.clone()],
            ..WriteBatch::default()
        };
        let mutation = self.plan_batch_mutation(&batch)?;
        let lsn = self.engine.apply_write_batch(batch.clone())?;
        self.record_local_commit(lsn.0, batch)?;
        self.apply_batch_mutation(&mutation);
        Ok(node)
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<NodeRecord> {
        self.engine.get_node(node_id).cloned()
    }

    pub fn delete_node(&mut self, node_id: &NodeId) -> Undr9Result<Option<NodeRecord>> {
        self.ensure_client_write_allowed()?;
        let removed = self.engine.get_node(node_id).cloned();
        if removed.is_none() {
            return Ok(None);
        }
        let batch = WriteBatch {
            deleted_node_ids: vec![node_id.clone()],
            ..WriteBatch::default()
        };
        let mutation = self.plan_batch_mutation(&batch)?;
        let lsn = self.engine.apply_write_batch(batch.clone())?;
        self.record_local_commit(lsn.0, batch)?;
        self.apply_batch_mutation(&mutation);
        Ok(removed)
    }

    pub fn upsert_edge(&mut self, edge: EdgeRecord) -> Undr9Result<EdgeRecord> {
        self.ensure_client_write_allowed()?;
        let batch = WriteBatch {
            edges_upserted: vec![edge.clone()],
            ..WriteBatch::default()
        };
        let mutation = self.plan_batch_mutation(&batch)?;
        let lsn = self.engine.apply_write_batch(batch.clone())?;
        self.record_local_commit(lsn.0, batch)?;
        self.apply_batch_mutation(&mutation);
        Ok(edge)
    }

    pub fn get_edge(&self, edge_id: &EdgeId) -> Option<EdgeRecord> {
        self.engine.get_edge(edge_id).cloned()
    }

    pub fn delete_edge(&mut self, edge_id: &EdgeId) -> Undr9Result<Option<EdgeRecord>> {
        self.ensure_client_write_allowed()?;
        let removed = self.engine.get_edge(edge_id).cloned();
        if removed.is_none() {
            return Ok(None);
        }
        let batch = WriteBatch {
            deleted_edge_ids: vec![edge_id.clone()],
            ..WriteBatch::default()
        };
        let mutation = self.plan_batch_mutation(&batch)?;
        let lsn = self.engine.apply_write_batch(batch.clone())?;
        self.record_local_commit(lsn.0, batch)?;
        self.apply_batch_mutation(&mutation);
        Ok(removed)
    }

    pub fn rebuild_indexes(&mut self) -> Undr9Result<IndexSnapshot> {
        self.refresh_indexes(true)
    }

    pub fn begin_transaction(
        &mut self,
        isolation_level: IsolationLevel,
    ) -> Undr9Result<TransactionSummary> {
        self.ensure_client_write_allowed()?;
        Ok(self.engine.begin_transaction(isolation_level))
    }

    pub fn transaction_summary(
        &self,
        transaction_id: &TransactionId,
    ) -> Undr9Result<TransactionSummary> {
        self.engine.transaction_summary(transaction_id)
    }

    pub fn list_transactions(&self) -> Vec<TransactionSummary> {
        self.engine.list_transactions()
    }

    pub fn stage_transaction_operation(
        &mut self,
        transaction_id: &TransactionId,
        operation: TransactionOperation,
    ) -> Undr9Result<TransactionSummary> {
        self.ensure_client_write_allowed()?;
        self.engine
            .stage_transaction_operation(transaction_id, operation)
    }

    pub fn commit_transaction(
        &mut self,
        transaction_id: &TransactionId,
    ) -> Undr9Result<TransactionCommitResult> {
        self.ensure_client_write_allowed()?;
        let batch = self.engine.transaction_staged_batch(transaction_id)?;
        let mutation = self.plan_batch_mutation(&batch)?;
        let result = self.engine.commit_transaction(transaction_id)?;
        self.record_local_commit(result.committed_lsn, batch)?;
        self.apply_batch_mutation(&mutation);
        Ok(result)
    }

    pub fn rollback_transaction(
        &mut self,
        transaction_id: &TransactionId,
    ) -> Undr9Result<TransactionSummary> {
        self.ensure_client_write_allowed()?;
        self.engine.rollback_transaction(transaction_id)
    }

    pub fn transaction_query_view(
        &self,
        transaction_id: &TransactionId,
    ) -> Undr9Result<(Arc<GraphSnapshot>, GraphMutation)> {
        let base = self.snapshot();
        let mutation =
            BatchMutation::from(self.engine.transaction_delta_from_current(transaction_id)?);
        Ok((base, graph_mutation_from_batch(&mutation)))
    }

    pub fn compact(&mut self) -> Undr9Result<()> {
        self.engine.compact()?;
        let _ = self.refresh_indexes(true)?;
        Ok(())
    }

    pub fn verify_integrity(&self) -> Undr9Result<IntegrityReport> {
        self.engine.verify_integrity()
    }

    pub fn backup_to(&self, destination: impl AsRef<FsPath>) -> Undr9Result<()> {
        let destination = destination.as_ref();
        let started = Instant::now();
        tracing::info!(
            destination = %destination.display(),
            "backup workflow started"
        );
        self.engine.backup_to(destination)?;
        tracing::info!(
            destination = %destination.display(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "backup workflow completed"
        );
        Ok(())
    }

    pub fn restore_from(
        &mut self,
        config: &AppConfig,
        source: impl AsRef<FsPath>,
        target_lsn: Option<u64>,
    ) -> Undr9Result<()> {
        let source = source.as_ref();
        let started = Instant::now();
        tracing::info!(
            source = %source.display(),
            destination = %config.storage.root_dir.display(),
            target_lsn = target_lsn,
            "restore workflow started"
        );
        if let Some(target_lsn) = target_lsn {
            restore_directory_to_lsn(source, &config.storage.root_dir, target_lsn, &config.wal)?;
        } else {
            restore_directory(source, &config.storage.root_dir)?;
        }
        self.engine = StorageEngine::open(config)?;
        self.vector_index_config = config.vector_index.clone();
        let _ = self.rebuild_indexes()?;
        tracing::info!(
            source = %source.display(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            node_count = self.engine.node_count(),
            edge_count = self.engine.edge_count(),
            target_lsn = target_lsn,
            "restore workflow completed"
        );
        Ok(())
    }

    pub fn repair(&mut self, config: &AppConfig) -> Undr9Result<IntegrityReport> {
        let started = Instant::now();
        tracing::info!(
            storage_root = %config.storage.root_dir.display(),
            "repair workflow started"
        );
        let report = repair_storage(config)?;
        self.engine = StorageEngine::open(config)?;
        self.vector_index_config = config.vector_index.clone();
        let _ = self.rebuild_indexes()?;
        tracing::info!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            wal_replay_valid = report.wal_replay_valid,
            issue_count = report.issues.len(),
            "repair workflow completed"
        );
        Ok(report)
    }

    pub fn replication_status(&self) -> ReplicationStatusResponse {
        ReplicationStatusResponse {
            status: self.replication.status().clone(),
            replica_lag: self.replication.replica_lag_map(),
        }
    }

    pub fn replication_history_since(&self, after_source_lsn: u64) -> Vec<ReplicationRecord> {
        self.replication.history_since(after_source_lsn)
    }

    pub fn cluster_topology(&self) -> ClusterTopology {
        self.cluster.topology().clone()
    }

    pub fn configure_as_leader(&mut self) -> Undr9Result<ReplicationStatusResponse> {
        let local_node_id = self.replication.status().local_node_id.clone();
        let plan = self.cluster.ensure_leader(&local_node_id)?;
        self.replication.promote_to_leader(plan.term);
        self.persist_cluster_topology()?;
        self.persist_replication_metadata()?;
        Ok(self.replication_status())
    }

    pub fn configure_as_follower(
        &mut self,
        leader_node_id: impl Into<String>,
        leader_address: impl Into<String>,
    ) -> Undr9Result<ReplicationStatusResponse> {
        let leader_node_id = leader_node_id.into();
        self.cluster.upsert_node(
            leader_node_id.clone(),
            leader_address.into(),
            undr9_cluster::NodeRole::Replica,
            true,
        );
        self.cluster.observe_leader(&leader_node_id)?;
        self.replication
            .failover_to_follower(leader_node_id, self.cluster.topology().term);
        self.persist_cluster_topology()?;
        self.persist_replication_metadata()?;
        Ok(self.replication_status())
    }

    pub fn register_replica(
        &mut self,
        node_id: impl Into<String>,
        address: impl Into<String>,
    ) -> Undr9Result<ClusterTopology> {
        let node_id = node_id.into();
        self.cluster.add_replica(node_id.clone(), address)?;
        self.replication.register_replica(node_id)?;
        self.persist_cluster_topology()?;
        self.persist_replication_metadata()?;
        Ok(self.cluster.topology().clone())
    }

    pub fn acknowledge_replica(
        &mut self,
        replica_node_id: &str,
        source_lsn: u64,
    ) -> Undr9Result<ReplicationStatusResponse> {
        self.replication
            .acknowledge_replica(replica_node_id, source_lsn)?;
        self.persist_replication_metadata()?;
        Ok(self.replication_status())
    }

    pub fn apply_replication_records(
        &mut self,
        records: &[ReplicationRecord],
    ) -> Undr9Result<ReplicationStatusResponse> {
        if self.replication.status().mode != ReplicationMode::Follower {
            return Err(Undr9Error::Conflict(
                "replication apply requires follower mode".to_owned(),
            ));
        }

        let accepted_records = self.validate_replication_records(records)?;
        let mut last_local_lsn = None;
        for record in &accepted_records {
            let mutation = self.plan_batch_mutation(&record.batch)?;
            let lsn = self.engine.apply_write_batch(record.batch.clone())?;
            self.apply_batch_mutation(&mutation);
            last_local_lsn = Some(lsn.0);
        }

        self.replication.apply_follower_records(&accepted_records)?;
        if let Some(local_lsn) = last_local_lsn {
            self.replication.observe_local_apply(local_lsn);
        }
        self.persist_replication_metadata()?;
        Ok(self.replication_status())
    }

    pub fn mark_node_health(
        &mut self,
        node_id: &str,
        healthy: bool,
    ) -> Undr9Result<ClusterTopology> {
        self.cluster.mark_node_health(node_id, healthy)?;
        self.persist_cluster_topology()?;
        Ok(self.cluster.topology().clone())
    }

    pub fn promote_node(&mut self, node_id: &str) -> Undr9Result<FailoverPlan> {
        let plan = self.cluster.promote_node(node_id)?;
        if self.replication.status().local_node_id == node_id {
            self.replication.promote_to_leader(plan.term);
        } else {
            self.replication
                .failover_to_follower(node_id.to_owned(), plan.term);
        }
        self.persist_cluster_topology()?;
        self.persist_replication_metadata()?;
        Ok(plan)
    }

    fn persist_index_snapshot(&self) -> Undr9Result<IndexSnapshot> {
        let snapshot = self.indexes.snapshot();
        let path = self.engine.layout().index_snapshot_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                Undr9Error::Io(format!(
                    "failed to create index snapshot directory '{}': {error}",
                    parent.display()
                ))
            })?;
        }

        let payload = serde_json::to_vec_pretty(&snapshot).map_err(|error| {
            Undr9Error::Serialization(format!("failed to serialize index snapshot: {error}"))
        })?;
        fs::write(&path, payload).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to write index snapshot '{}': {error}",
                path.display()
            ))
        })?;
        self.indexes.persist_vector_index(
            &self.engine.layout().vector_index_manifest_path(),
            &self.engine.layout().vector_index_graph_path(),
            &self.engine.layout().vector_index_vectors_path(),
            self.engine.latest_applied_lsn(),
        )?;
        tracing::debug!(
            snapshot_path = %path.display(),
            node_count = snapshot.node_count,
            unique_key_count = snapshot.unique_key_count,
            "index snapshot persisted"
        );
        Ok(snapshot)
    }

    fn refresh_indexes(&mut self, persist_snapshot: bool) -> Undr9Result<IndexSnapshot> {
        let started = Instant::now();
        self.indexes = GraphIndex::rebuild_with_config(
            &self.engine.all_nodes(),
            &self.engine.all_edges(),
            &self.vector_index_config,
        );
        self.publish_snapshot();
        let snapshot = if persist_snapshot {
            self.persist_index_snapshot()
        } else {
            Ok(self.indexes.snapshot())
        }?;
        tracing::info!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            persist_snapshot = persist_snapshot,
            node_count = snapshot.node_count,
            unique_key_count = snapshot.unique_key_count,
            "index rebuild completed"
        );
        Ok(snapshot)
    }

    fn ensure_client_write_allowed(&self) -> Undr9Result<()> {
        if self.replication.status().mode == ReplicationMode::Follower {
            return Err(Undr9Error::Conflict(
                "node is in follower mode and only accepts replicated writes".to_owned(),
            ));
        }
        Ok(())
    }

    fn record_local_commit(&mut self, committed_lsn: u64, batch: WriteBatch) -> Undr9Result<()> {
        self.replication.observe_local_apply(committed_lsn);
        if self.replication.status().mode == ReplicationMode::Leader {
            self.replication
                .record_leader_commit(committed_lsn, batch)?;
        }
        self.persist_replication_metadata()
    }

    fn persist_replication_metadata(&self) -> Undr9Result<()> {
        persist_json_file(self.replication_metadata_path(), &self.replication)
    }

    fn persist_cluster_topology(&self) -> Undr9Result<()> {
        persist_json_file(self.cluster_metadata_path(), &self.cluster)
    }

    fn replication_metadata_path(&self) -> PathBuf {
        self.engine
            .layout()
            .subdirectory("meta")
            .join(REPLICATION_METADATA_FILE_NAME)
    }

    fn cluster_metadata_path(&self) -> PathBuf {
        self.engine
            .layout()
            .subdirectory("meta")
            .join(CLUSTER_METADATA_FILE_NAME)
    }

    fn publish_snapshot(&mut self) {
        self.published_snapshot = Arc::new(build_snapshot(&self.engine, &self.indexes));
    }

    fn plan_batch_mutation(&self, batch: &WriteBatch) -> Undr9Result<BatchMutation> {
        let mut normalized = batch.clone();
        for node in &mut normalized.nodes_upserted {
            node.normalize_memory_metadata()?;
        }

        let deleted_node_ids = normalized
            .deleted_node_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let deleted_edge_ids = normalized
            .deleted_edge_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut mutation = BatchMutation::default();

        for node in &normalized.nodes_upserted {
            if let Some(previous) = self.engine.get_node(&node.id).cloned() {
                mutation.removed_nodes.insert(node.id.clone(), previous);
            }
            if !deleted_node_ids.contains(&node.id) {
                mutation.added_nodes.insert(node.id.clone(), node.clone());
            }
        }

        for node_id in &normalized.deleted_node_ids {
            if let Some(previous) = self.engine.get_node(node_id).cloned() {
                mutation.removed_nodes.insert(node_id.clone(), previous);
            }
        }

        for edge in &normalized.edges_upserted {
            if let Some(previous) = self.engine.get_edge(&edge.id).cloned() {
                mutation.removed_edges.insert(edge.id.clone(), previous);
            }
            if !deleted_edge_ids.contains(&edge.id)
                && !deleted_node_ids.contains(&edge.source)
                && !deleted_node_ids.contains(&edge.target)
            {
                mutation.added_edges.insert(edge.id.clone(), edge.clone());
            }
        }

        for edge_id in &normalized.deleted_edge_ids {
            if let Some(previous) = self.engine.get_edge(edge_id).cloned() {
                mutation.removed_edges.insert(edge_id.clone(), previous);
            }
        }

        if !deleted_node_ids.is_empty() {
            for edge in self.engine.all_edges() {
                if deleted_node_ids.contains(&edge.source)
                    || deleted_node_ids.contains(&edge.target)
                {
                    mutation.removed_edges.insert(edge.id.clone(), edge.clone());
                    mutation.added_edges.remove(&edge.id);
                }
            }
        }

        Ok(mutation)
    }

    fn apply_batch_mutation(&mut self, mutation: &BatchMutation) {
        self.indexes = indexes_with_mutation(&self.indexes, mutation);
        self.published_snapshot = Arc::new(snapshot_with_mutation(
            self.published_snapshot.as_ref(),
            mutation,
        ));
    }

    fn validate_replication_records(
        &self,
        records: &[ReplicationRecord],
    ) -> Undr9Result<Vec<ReplicationRecord>> {
        let mut current_term = self.replication.status().current_term;
        let mut last_applied_source_lsn = self.replication.status().last_applied_source_lsn;
        let mut accepted_records = Vec::new();

        for record in records {
            if record.source_term < current_term {
                return Err(Undr9Error::Conflict(format!(
                    "replication term regression: follower term {} leader term {}",
                    current_term, record.source_term
                )));
            }
            if record.source_lsn <= last_applied_source_lsn {
                continue;
            }

            current_term = record.source_term;
            last_applied_source_lsn = record.source_lsn;
            accepted_records.push(record.clone());
        }

        Ok(accepted_records)
    }
}

impl ApiState {
    pub fn try_new(config: AppConfig) -> Undr9Result<Self> {
        let advertise_address = config.server.bind_address.clone();
        Self::try_new_with_identity(config, "node-1", advertise_address)
    }

    pub fn try_new_with_identity(
        config: AppConfig,
        local_node_id: impl Into<String>,
        advertise_address: impl Into<String>,
    ) -> Undr9Result<Self> {
        let database = Database::open_with_identity(&config, local_node_id, advertise_address)?;
        Ok(Self {
            service_name: Arc::<str>::from("undr9"),
            audit_log_path: Arc::new(database.engine.layout().audit_log_path()),
            maintenance_status: Arc::new(RwLock::new(MaintenanceStatusResponse {
                in_progress: false,
                last_operation: None,
                last_outcome: None,
                detail: None,
                started_at_ms: None,
                finished_at_ms: None,
                elapsed_ms: None,
                last_node_count: 0,
                last_edge_count: 0,
                max_node_count: config.maintenance.max_node_count,
                max_edge_count: config.maintenance.max_edge_count,
            })),
            config,
            readiness: Arc::new(AtomicBool::new(true)),
            database: Arc::new(RwLock::new(database)),
            counters: Arc::new(AtomicServiceCounters::default()),
            endpoint_metrics: Arc::new(DashMap::new()),
            rate_limits: Arc::new(DashMap::new()),
        })
    }

    pub fn is_ready(&self) -> bool {
        self.readiness.load(Ordering::Relaxed)
    }

    pub fn set_readiness(&self, ready: bool) {
        self.readiness.store(ready, Ordering::Relaxed);
    }

    pub async fn graceful_shutdown(&self) -> Undr9Result<()> {
        self.set_readiness(false);
        let mut database = self.database.write().await;
        database.graceful_shutdown()
    }

    async fn mark_maintenance_started(
        &self,
        operation: &str,
        started_at_ms: u128,
        node_count: usize,
        edge_count: usize,
    ) {
        let mut status = self.maintenance_status.write().await;
        status.in_progress = true;
        status.last_operation = Some(operation.to_owned());
        status.last_outcome = None;
        status.detail = Some("maintenance operation in progress".to_owned());
        status.started_at_ms = Some(started_at_ms);
        status.finished_at_ms = None;
        status.elapsed_ms = None;
        status.last_node_count = node_count;
        status.last_edge_count = edge_count;
        status.max_node_count = self.config.maintenance.max_node_count;
        status.max_edge_count = self.config.maintenance.max_edge_count;
    }

    async fn mark_maintenance_finished(
        &self,
        operation: &str,
        outcome: &str,
        detail: impl Into<String>,
        started_at_ms: u128,
        elapsed_ms: u64,
        node_count: usize,
        edge_count: usize,
    ) {
        let mut status = self.maintenance_status.write().await;
        status.in_progress = false;
        status.last_operation = Some(operation.to_owned());
        status.last_outcome = Some(outcome.to_owned());
        status.detail = Some(detail.into());
        status.started_at_ms = Some(started_at_ms);
        status.finished_at_ms = Some(started_at_ms + u128::from(elapsed_ms));
        status.elapsed_ms = Some(elapsed_ms);
        status.last_node_count = node_count;
        status.last_edge_count = edge_count;
        status.max_node_count = self.config.maintenance.max_node_count;
        status.max_edge_count = self.config.maintenance.max_edge_count;
    }
}

impl Default for ApiState {
    fn default() -> Self {
        Self::try_new(AppConfig::default()).expect("default API state should initialize")
    }
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ErrorResponse {
                code,
                message: message.into(),
                details: Vec::new(),
            },
        }
    }

    fn from_error(error: Undr9Error) -> Self {
        match error {
            Undr9Error::Validation(message) => {
                Self::new(StatusCode::BAD_REQUEST, "validation_error", message)
            }
            Undr9Error::Conflict(message) => Self::new(StatusCode::CONFLICT, "conflict", message),
            Undr9Error::NotFound(message) => Self::new(StatusCode::NOT_FOUND, "not_found", message),
            Undr9Error::Corruption(message) => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, "corruption", message)
            }
            Undr9Error::Io(message) => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, "io_error", message)
            }
            Undr9Error::Serialization(message) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "serialization_error",
                message,
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            [(
                header::HeaderName::from_static("x-undr9-error-code"),
                self.body.code,
            )],
            Json(self.body),
        )
            .into_response()
    }
}

fn load_json_file<T>(path: PathBuf) -> Undr9Result<Option<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(None);
    }

    let payload = fs::read(&path).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to read metadata file '{}': {error}",
            path.display()
        ))
    })?;
    let value = serde_json::from_slice(&payload).map_err(|error| {
        Undr9Error::Serialization(format!(
            "failed to deserialize metadata file '{}': {error}",
            path.display()
        ))
    })?;
    Ok(Some(value))
}

fn persist_json_file<T>(path: PathBuf, value: &T) -> Undr9Result<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to create metadata directory '{}': {error}",
                parent.display()
            ))
        })?;
    }

    let payload = serde_json::to_vec_pretty(value).map_err(|error| {
        Undr9Error::Serialization(format!(
            "failed to serialize metadata file '{}': {error}",
            path.display()
        ))
    })?;
    fs::write(&path, payload).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to write metadata file '{}': {error}",
            path.display()
        ))
    })?;
    Ok(())
}

fn build_snapshot(engine: &StorageEngine, indexes: &GraphIndex) -> GraphSnapshot {
    let nodes = engine
        .all_nodes()
        .into_iter()
        .map(|record| (record.id.clone(), record))
        .collect::<OrdMap<_, _>>();
    let edges = engine
        .all_edges()
        .into_iter()
        .map(|record| (record.id.clone(), record))
        .collect::<OrdMap<_, _>>();

    GraphSnapshot {
        nodes,
        edges,
        indexes: indexes.clone(),
    }
}

pub fn build_router(state: ApiState) -> Router {
    let request_timeout_ms = state.config.server.request_timeout_ms;
    let max_request_body_bytes = state.config.server.max_request_body_bytes;
    Router::new()
        .route("/healthz", get(health_handler))
        .route("/readyz", get(readiness_handler))
        .route("/metrics", get(metrics_handler))
        .route("/v1/nodes", post(create_node_handler))
        .route(
            "/v1/nodes/:id",
            get(get_node_handler)
                .put(update_node_handler)
                .delete(delete_node_handler),
        )
        .route("/v1/edges", post(create_edge_handler))
        .route(
            "/v1/edges/:id",
            get(get_edge_handler)
                .put(update_edge_handler)
                .delete(delete_edge_handler),
        )
        .route("/v1/query", post(query_handler))
        .route("/v1/query/stream", post(query_stream_handler))
        .route("/v1/transactions", get(list_transactions_handler))
        .route("/v1/transactions/begin", post(begin_transaction_handler))
        .route("/v1/transactions/:id", get(transaction_summary_handler))
        .route(
            "/v1/transactions/:id/operations",
            post(stage_transaction_operation_handler),
        )
        .route(
            "/v1/transactions/:id/query",
            post(transaction_query_handler),
        )
        .route(
            "/v1/transactions/:id/query/stream",
            post(transaction_query_stream_handler),
        )
        .route(
            "/v1/transactions/:id/commit",
            post(commit_transaction_handler),
        )
        .route(
            "/v1/transactions/:id/rollback",
            post(rollback_transaction_handler),
        )
        .route("/v1/admin/compact", post(compact_handler))
        .route("/v1/admin/backup", post(backup_handler))
        .route("/v1/admin/restore", post(restore_handler))
        .route("/v1/admin/repair", post(repair_handler))
        .route("/v1/admin/rebuild-indexes", post(rebuild_indexes_handler))
        .route(
            "/v1/admin/maintenance/status",
            get(maintenance_status_handler),
        )
        .route("/v1/admin/integrity", get(integrity_handler))
        .route("/v1/admin/audit", get(audit_export_handler))
        .route(
            "/v1/admin/replication/status",
            get(replication_status_handler),
        )
        .route(
            "/v1/admin/replication/history",
            get(replication_history_handler),
        )
        .route(
            "/v1/admin/replication/leader",
            post(configure_leader_handler),
        )
        .route(
            "/v1/admin/replication/follower",
            post(configure_follower_handler),
        )
        .route("/v1/admin/replication/ack", post(replica_ack_handler))
        .route(
            "/v1/admin/replication/apply",
            post(apply_replication_handler),
        )
        .route("/v1/admin/cluster/topology", get(cluster_topology_handler))
        .route("/v1/admin/cluster/nodes", post(register_replica_handler))
        .route(
            "/v1/admin/cluster/nodes/:id/health",
            post(mark_node_health_handler),
        )
        .route("/v1/admin/cluster/promote", post(promote_node_handler))
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .layer(middleware::from_fn_with_state(
            request_timeout_ms,
            request_timeout_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_metrics_middleware,
        ))
        .with_state(state)
}

async fn request_timeout_middleware(
    State(request_timeout_ms): State<u64>,
    request: Request,
    next: Next,
) -> Response {
    match tokio::time::timeout(Duration::from_millis(request_timeout_ms), next.run(request)).await {
        Ok(response) => response,
        Err(_) => ApiError::new(
            StatusCode::REQUEST_TIMEOUT,
            "request_timeout",
            format!("request exceeded server timeout of {request_timeout_ms}ms"),
        )
        .into_response(),
    }
}

async fn request_metrics_middleware(
    State(state): State<ApiState>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().as_str().to_owned();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_owned())
        .unwrap_or_else(|| request.uri().path().to_owned());
    let trace_id = request_trace_id(request.headers());
    let span = tracing::info_span!(
        "http.request",
        otel_kind = "server",
        http_method = %method,
        http_route = %route,
        trace_id = %trace_id
    );
    async move {
        let started = Instant::now();
        tracing::info!("request started");
        let mut response = next.run(request).await;
        if let Ok(header_value) = HeaderValue::from_str(&trace_id) {
            response.headers_mut().insert(
                header::HeaderName::from_static(TRACE_ID_HEADER),
                header_value,
            );
        }
        if let Some(error_code) = response
            .headers()
            .get("x-undr9-error-code")
            .and_then(|value| value.to_str().ok())
        {
            record_api_error_metric(&state, response.status(), error_code);
            annotate_active_span_error(
                error_code,
                response.status().as_u16(),
                format!("request failed with status {}", response.status()),
            );
            tracing::warn!(
                error_code = error_code,
                http_status = response.status().as_u16(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "request failed"
            );
        } else {
            annotate_active_span_success();
            tracing::info!(
                http_status = response.status().as_u16(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "request completed"
            );
        }
        record_endpoint_metrics(
            &state,
            &method,
            &route,
            response.status(),
            started.elapsed(),
        );
        response
    }
    .instrument(span)
    .await
}

async fn health_handler(State(state): State<ApiState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        service: state.service_name.to_string(),
        status: "ok",
    })
}

async fn readiness_handler(State(state): State<ApiState>) -> Response {
    let status = if state.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let payload = Json(HealthResponse {
        service: state.service_name.to_string(),
        status: if state.is_ready() {
            "ready"
        } else {
            "draining"
        },
    });

    (status, payload).into_response()
}

async fn metrics_handler(State(state): State<ApiState>) -> Response {
    if !state.config.observability.metrics_enabled {
        return ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "metrics endpoint is disabled",
        )
        .into_response();
    }
    let database = lock_database(&state).await;
    let counters = state.counters.snapshot(database.wal_replay_latency_ms);
    let metrics = MetricsSnapshot {
        service_name: state.service_name.to_string(),
        ready: state.is_ready(),
        node_count: database.engine.node_count(),
        edge_count: database.engine.edge_count(),
        active_transactions: database.list_transactions().len(),
        current_revision: database.engine.current_revision(),
        latest_applied_lsn: database.engine.latest_applied_lsn().unwrap_or(0),
        checkpoint_dirty: database.engine.needs_checkpoint(),
        pending_checkpoint_entries: database.engine.pending_checkpoint_count(),
        endpoint_metrics: snapshot_endpoint_metrics(&state),
        counters,
    }
    .render_prometheus();

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics,
    )
        .into_response()
}

async fn create_node_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(node): Json<NodeRecord>,
) -> std::result::Result<Json<NodeRecord>, ApiError> {
    let principal = authorize(&headers, &state, Action::Write)?;
    let node = {
        let mut database = lock_database_mut(&state).await;
        database.upsert_node(node).map_err(ApiError::from_error)?
    };
    increment_counter(&state, |counters| {
        counters
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "create_node",
            "node",
            "success",
            node.id.to_string(),
        ),
    )?;
    emit_log(
        StructuredLogEvent::new("INFO", "undr9_api", "node created")
            .with_field("node_id", node.id.to_string()),
    );
    Ok(Json(node))
}

async fn update_node_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(node): Json<NodeRecord>,
) -> std::result::Result<Json<NodeRecord>, ApiError> {
    let principal = authorize(&headers, &state, Action::Write)?;
    let path_id = NodeId::new(id).map_err(ApiError::from_error)?;
    if path_id != node.id {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "path node id does not match request body id",
        ));
    }

    let node = {
        let mut database = lock_database_mut(&state).await;
        database.upsert_node(node).map_err(ApiError::from_error)?
    };
    increment_counter(&state, |counters| {
        counters
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "update_node",
            "node",
            "success",
            node.id.to_string(),
        ),
    )?;
    Ok(Json(node))
}

async fn get_node_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Json<NodeRecord>, ApiError> {
    authorize(&headers, &state, Action::Read)?;
    let node_id = NodeId::new(id).map_err(ApiError::from_error)?;
    let database = lock_database(&state).await;
    let node = database.get_node(&node_id).ok_or_else(|| {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("node '{}' was not found", node_id),
        )
    })?;
    Ok(Json(node))
}

async fn delete_node_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<StatusCode, ApiError> {
    let principal = authorize(&headers, &state, Action::Write)?;
    let node_id = NodeId::new(id).map_err(ApiError::from_error)?;
    {
        let mut database = lock_database_mut(&state).await;
        database
            .delete_node(&node_id)
            .map_err(ApiError::from_error)?
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    format!("node '{}' was not found", node_id),
                )
            })?;
    }
    increment_counter(&state, |counters| {
        counters
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "delete_node",
            "node",
            "success",
            node_id.to_string(),
        ),
    )?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_edge_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(edge): Json<EdgeRecord>,
) -> std::result::Result<Json<EdgeRecord>, ApiError> {
    let principal = authorize(&headers, &state, Action::Write)?;
    let edge = {
        let mut database = lock_database_mut(&state).await;
        database.upsert_edge(edge).map_err(ApiError::from_error)?
    };
    increment_counter(&state, |counters| {
        counters
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "create_edge",
            "edge",
            "success",
            edge.id.to_string(),
        ),
    )?;
    Ok(Json(edge))
}

async fn update_edge_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(edge): Json<EdgeRecord>,
) -> std::result::Result<Json<EdgeRecord>, ApiError> {
    let principal = authorize(&headers, &state, Action::Write)?;
    let path_id = EdgeId::new(id).map_err(ApiError::from_error)?;
    if path_id != edge.id {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "path edge id does not match request body id",
        ));
    }

    let edge = {
        let mut database = lock_database_mut(&state).await;
        database.upsert_edge(edge).map_err(ApiError::from_error)?
    };
    increment_counter(&state, |counters| {
        counters
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "update_edge",
            "edge",
            "success",
            edge.id.to_string(),
        ),
    )?;
    Ok(Json(edge))
}

async fn get_edge_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Json<EdgeRecord>, ApiError> {
    authorize(&headers, &state, Action::Read)?;
    let edge_id = EdgeId::new(id).map_err(ApiError::from_error)?;
    let database = lock_database(&state).await;
    let edge = database.get_edge(&edge_id).ok_or_else(|| {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            format!("edge '{}' was not found", edge_id),
        )
    })?;
    Ok(Json(edge))
}

async fn delete_edge_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<StatusCode, ApiError> {
    let principal = authorize(&headers, &state, Action::Write)?;
    let edge_id = EdgeId::new(id).map_err(ApiError::from_error)?;
    {
        let mut database = lock_database_mut(&state).await;
        database
            .delete_edge(&edge_id)
            .map_err(ApiError::from_error)?
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    format!("edge '{}' was not found", edge_id),
                )
            })?;
    }
    increment_counter(&state, |counters| {
        counters
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "delete_edge",
            "edge",
            "success",
            edge_id.to_string(),
        ),
    )?;
    Ok(StatusCode::NO_CONTENT)
}

async fn query_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> std::result::Result<Json<QueryResponse>, ApiError> {
    authorize(&headers, &state, Action::Read)?;
    let started = Instant::now();
    let query_kind = query_request_kind(&request);
    let database = lock_database(&state).await;
    let snapshot = database.snapshot();
    drop(database);
    let response = Executor::execute(&request, snapshot.as_ref()).map_err(ApiError::from_error)?;
    enforce_query_response_budget(&response)?;
    record_query_metrics(
        &state,
        &request,
        &response,
        started.elapsed().as_millis() as u64,
    )?;
    emit_log(StructuredLogEvent::new(
        "INFO",
        "undr9_api",
        "query executed",
    ));
    tracing::info!(
        query_kind = query_kind,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "query execution completed"
    );
    Ok(Json(response))
}

async fn query_stream_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> std::result::Result<Response, ApiError> {
    authorize(&headers, &state, Action::Read)?;
    let started = Instant::now();
    let query_kind = query_request_kind(&request);
    let database = lock_database(&state).await;
    let snapshot = database.snapshot();
    drop(database);
    let execution =
        Executor::execute_iter(&request, snapshot.as_ref()).map_err(ApiError::from_error)?;
    let (response, summary) = render_query_stream(execution)?;
    record_query_stream_metrics(
        &state,
        &request,
        &summary,
        started.elapsed().as_millis() as u64,
    )?;
    emit_log(StructuredLogEvent::new(
        "INFO",
        "undr9_api",
        "streaming query executed",
    ));
    tracing::info!(
        query_kind = query_kind,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "streaming query execution completed"
    );
    Ok(response)
}

async fn begin_transaction_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<BeginTransactionRequest>,
) -> std::result::Result<Json<TransactionSummary>, ApiError> {
    let principal = authorize(&headers, &state, Action::Write)?;
    let started = Instant::now();
    let summary = {
        let mut database = lock_database_mut(&state).await;
        database
            .begin_transaction(request.isolation_level.unwrap_or(IsolationLevel::Snapshot))
            .map_err(ApiError::from_error)?
    };
    increment_counter(&state, |counters| {
        counters
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);
        record_latency(
            &counters.transaction_latency,
            started.elapsed().as_millis() as u64,
        );
    });
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "begin_transaction",
            "transaction",
            "success",
            summary.transaction_id.to_string(),
        ),
    )?;
    Ok(Json(summary))
}

async fn list_transactions_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<TransactionSummary>>, ApiError> {
    authorize(&headers, &state, Action::Read)?;
    let database = lock_database(&state).await;
    Ok(Json(database.list_transactions()))
}

async fn transaction_summary_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Json<TransactionSummary>, ApiError> {
    authorize(&headers, &state, Action::Read)?;
    let transaction_id = TransactionId::new(id).map_err(ApiError::from_error)?;
    let database = lock_database(&state).await;
    let summary = database
        .transaction_summary(&transaction_id)
        .map_err(ApiError::from_error)?;
    Ok(Json(summary))
}

async fn stage_transaction_operation_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(operation): Json<TransactionOperation>,
) -> std::result::Result<Json<TransactionSummary>, ApiError> {
    let principal = authorize(&headers, &state, Action::Write)?;
    let transaction_id = TransactionId::new(id).map_err(ApiError::from_error)?;
    let summary = {
        let mut database = lock_database_mut(&state).await;
        database
            .stage_transaction_operation(&transaction_id, operation)
            .map_err(ApiError::from_error)?
    };
    increment_counter(&state, |counters| {
        counters
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "stage_transaction_operation",
            "transaction",
            "success",
            summary.transaction_id.to_string(),
        ),
    )?;
    Ok(Json(summary))
}

async fn transaction_query_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<QueryRequest>,
) -> std::result::Result<Json<QueryResponse>, ApiError> {
    authorize(&headers, &state, Action::Read)?;
    let started = Instant::now();
    let transaction_id = TransactionId::new(id).map_err(ApiError::from_error)?;
    let database = lock_database(&state).await;
    let (snapshot, mutation) = database
        .transaction_query_view(&transaction_id)
        .map_err(ApiError::from_error)?;
    drop(database);
    let overlay = OverlayGraphView::new(snapshot.as_ref(), &mutation);
    let response = Executor::execute(&request, &overlay).map_err(ApiError::from_error)?;
    enforce_query_response_budget(&response)?;
    record_query_metrics(
        &state,
        &request,
        &response,
        started.elapsed().as_millis() as u64,
    )?;
    Ok(Json(response))
}

async fn transaction_query_stream_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<QueryRequest>,
) -> std::result::Result<Response, ApiError> {
    authorize(&headers, &state, Action::Read)?;
    let started = Instant::now();
    let transaction_id = TransactionId::new(id).map_err(ApiError::from_error)?;
    let database = lock_database(&state).await;
    let (snapshot, mutation) = database
        .transaction_query_view(&transaction_id)
        .map_err(ApiError::from_error)?;
    drop(database);
    let overlay = OverlayGraphView::new(snapshot.as_ref(), &mutation);
    let execution = Executor::execute_iter(&request, &overlay).map_err(ApiError::from_error)?;
    let (response, summary) = render_query_stream(execution)?;
    record_query_stream_metrics(
        &state,
        &request,
        &summary,
        started.elapsed().as_millis() as u64,
    )?;
    Ok(response)
}

async fn commit_transaction_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Json<TransactionCommitResult>, ApiError> {
    let principal = authorize(&headers, &state, Action::Write)?;
    let started = Instant::now();
    let transaction_id = TransactionId::new(id).map_err(ApiError::from_error)?;
    let result = {
        let mut database = lock_database_mut(&state).await;
        database
            .commit_transaction(&transaction_id)
            .map_err(ApiError::from_error)?
    };
    increment_counter(&state, |counters| {
        counters
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);
        record_latency(
            &counters.transaction_latency,
            started.elapsed().as_millis() as u64,
        );
    });
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "commit_transaction",
            "transaction",
            "success",
            result.transaction_id.to_string(),
        ),
    )?;
    Ok(Json(result))
}

async fn rollback_transaction_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Json<TransactionSummary>, ApiError> {
    let principal = authorize(&headers, &state, Action::Write)?;
    let started = Instant::now();
    let transaction_id = TransactionId::new(id).map_err(ApiError::from_error)?;
    let summary = {
        let mut database = lock_database_mut(&state).await;
        database
            .rollback_transaction(&transaction_id)
            .map_err(ApiError::from_error)?
    };
    increment_counter(&state, |counters| {
        counters
            .write_requests_total
            .fetch_add(1, Ordering::Relaxed);
        record_latency(
            &counters.transaction_latency,
            started.elapsed().as_millis() as u64,
        );
    });
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "rollback_transaction",
            "transaction",
            "success",
            summary.transaction_id.to_string(),
        ),
    )?;
    Ok(Json(summary))
}

async fn compact_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<MaintenanceResponse>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let actor = principal.name.clone();
    let started = Instant::now();
    let started_at_ms = now_epoch_ms();
    let (node_count, edge_count) = enforce_maintenance_budget(&state, "compact").await?;
    state
        .mark_maintenance_started("compact", started_at_ms, node_count, edge_count)
        .await;
    {
        let mut database = lock_database_mut(&state).await;
        if let Err(error) = database.compact() {
            state
                .mark_maintenance_finished(
                    "compact",
                    "failed",
                    error.to_string(),
                    started_at_ms,
                    started.elapsed().as_millis() as u64,
                    node_count,
                    edge_count,
                )
                .await;
            return Err(ApiError::from_error(error));
        }
    }
    let elapsed_ms = started.elapsed().as_millis() as u64;
    increment_counter(&state, |counters| {
        counters
            .maintenance_operations_total
            .fetch_add(1, Ordering::Relaxed);
        record_latency(&counters.compaction_latency, elapsed_ms);
    });
    record_audit(
        &state,
        AuditEvent::new(
            actor.clone(),
            "compact",
            "storage",
            "success",
            "storage compacted",
        ),
    )?;
    tracing::info!(
        actor = %actor,
        operation = "compact",
        elapsed_ms,
        "maintenance operation completed"
    );
    state
        .mark_maintenance_finished(
            "compact",
            "success",
            "storage compacted",
            started_at_ms,
            elapsed_ms,
            node_count,
            edge_count,
        )
        .await;
    Ok(Json(MaintenanceResponse {
        status: "ok",
        operation: "compact",
        elapsed_ms,
        detail: "storage compacted".to_owned(),
    }))
}

async fn backup_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<BackupRequest>,
) -> std::result::Result<Json<MaintenanceResponse>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let actor = principal.name.clone();
    let started = Instant::now();
    let started_at_ms = now_epoch_ms();
    let (node_count, edge_count) = enforce_maintenance_budget(&state, "backup").await?;
    state
        .mark_maintenance_started("backup", started_at_ms, node_count, edge_count)
        .await;
    let database = lock_database(&state).await;
    if let Err(error) = database.backup_to(&request.destination) {
        drop(database);
        state
            .mark_maintenance_finished(
                "backup",
                "failed",
                error.to_string(),
                started_at_ms,
                started.elapsed().as_millis() as u64,
                node_count,
                edge_count,
            )
            .await;
        return Err(ApiError::from_error(error));
    }
    drop(database);
    let elapsed_ms = started.elapsed().as_millis() as u64;
    increment_counter(&state, |counters| {
        counters
            .maintenance_operations_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            actor.clone(),
            "backup",
            "storage",
            "success",
            request.destination.clone(),
        ),
    )?;
    tracing::info!(
        actor = %actor,
        operation = "backup",
        destination = %request.destination,
        elapsed_ms,
        "maintenance operation completed"
    );
    state
        .mark_maintenance_finished(
            "backup",
            "success",
            format!("backup created at {}", request.destination),
            started_at_ms,
            elapsed_ms,
            node_count,
            edge_count,
        )
        .await;
    Ok(Json(MaintenanceResponse {
        status: "ok",
        operation: "backup",
        elapsed_ms,
        detail: format!("backup created at {}", request.destination),
    }))
}

async fn restore_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<RestoreRequest>,
) -> std::result::Result<Json<MaintenanceResponse>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let actor = principal.name.clone();
    let started = Instant::now();
    let started_at_ms = now_epoch_ms();
    let (node_count, edge_count) = enforce_maintenance_budget(&state, "restore").await?;
    state
        .mark_maintenance_started("restore", started_at_ms, node_count, edge_count)
        .await;
    {
        let mut database = lock_database_mut(&state).await;
        if let Err(error) =
            database.restore_from(&state.config, &request.source, request.target_lsn)
        {
            state
                .mark_maintenance_finished(
                    "restore",
                    "failed",
                    error.to_string(),
                    started_at_ms,
                    started.elapsed().as_millis() as u64,
                    node_count,
                    edge_count,
                )
                .await;
            return Err(ApiError::from_error(error));
        }
    }
    let elapsed_ms = started.elapsed().as_millis() as u64;
    increment_counter(&state, |counters| {
        counters
            .maintenance_operations_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            actor.clone(),
            "restore",
            "storage",
            "success",
            request.source.clone(),
        ),
    )?;
    tracing::info!(
        actor = %actor,
        operation = "restore",
        source = %request.source,
        elapsed_ms,
        "maintenance operation completed"
    );
    state
        .mark_maintenance_finished(
            "restore",
            "success",
            format!("storage restored from {}", request.source),
            started_at_ms,
            elapsed_ms,
            node_count,
            edge_count,
        )
        .await;
    Ok(Json(MaintenanceResponse {
        status: "ok",
        operation: "restore",
        elapsed_ms,
        detail: format!("storage restored from {}", request.source),
    }))
}

async fn repair_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<IntegrityReport>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let actor = principal.name.clone();
    let started = Instant::now();
    let started_at_ms = now_epoch_ms();
    let (node_count, edge_count) = enforce_maintenance_budget(&state, "repair").await?;
    state
        .mark_maintenance_started("repair", started_at_ms, node_count, edge_count)
        .await;
    let report = {
        let mut database = lock_database_mut(&state).await;
        match database.repair(&state.config) {
            Ok(report) => report,
            Err(error) => {
                state
                    .mark_maintenance_finished(
                        "repair",
                        "failed",
                        error.to_string(),
                        started_at_ms,
                        started.elapsed().as_millis() as u64,
                        node_count,
                        edge_count,
                    )
                    .await;
                return Err(ApiError::from_error(error));
            }
        }
    };
    let elapsed_ms = started.elapsed().as_millis() as u64;
    increment_counter(&state, |counters| {
        counters
            .maintenance_operations_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            actor.clone(),
            "repair",
            "storage",
            "success",
            "repair completed",
        ),
    )?;
    tracing::info!(
        actor = %actor,
        operation = "repair",
        elapsed_ms,
        "maintenance operation completed"
    );
    state
        .mark_maintenance_finished(
            "repair",
            "success",
            "repair completed",
            started_at_ms,
            elapsed_ms,
            node_count,
            edge_count,
        )
        .await;
    Ok(Json(report))
}

async fn rebuild_indexes_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<IndexSnapshot>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let actor = principal.name.clone();
    let started = Instant::now();
    let started_at_ms = now_epoch_ms();
    let (node_count, edge_count) = enforce_maintenance_budget(&state, "rebuild_indexes").await?;
    state
        .mark_maintenance_started("rebuild_indexes", started_at_ms, node_count, edge_count)
        .await;
    let snapshot = {
        let mut database = lock_database_mut(&state).await;
        match database.rebuild_indexes() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                state
                    .mark_maintenance_finished(
                        "rebuild_indexes",
                        "failed",
                        error.to_string(),
                        started_at_ms,
                        started.elapsed().as_millis() as u64,
                        node_count,
                        edge_count,
                    )
                    .await;
                return Err(ApiError::from_error(error));
            }
        }
    };
    let elapsed_ms = started.elapsed().as_millis() as u64;
    increment_counter(&state, |counters| {
        counters
            .maintenance_operations_total
            .fetch_add(1, Ordering::Relaxed);
    });
    record_audit(
        &state,
        AuditEvent::new(
            actor.clone(),
            "rebuild_indexes",
            "index",
            "success",
            "index snapshot rebuilt",
        ),
    )?;
    tracing::info!(
        actor = %actor,
        operation = "rebuild_indexes",
        elapsed_ms,
        "maintenance operation completed"
    );
    state
        .mark_maintenance_finished(
            "rebuild_indexes",
            "success",
            "index snapshot rebuilt",
            started_at_ms,
            elapsed_ms,
            node_count,
            edge_count,
        )
        .await;
    Ok(Json(snapshot))
}

async fn maintenance_status_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<MaintenanceStatusResponse>, ApiError> {
    authorize(&headers, &state, Action::Maintain)?;
    let status = state.maintenance_status.read().await.clone();
    Ok(Json(status))
}

async fn integrity_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<IntegrityReport>, ApiError> {
    authorize(&headers, &state, Action::Maintain)?;
    let database = lock_database(&state).await;
    let report = database.verify_integrity().map_err(ApiError::from_error)?;
    Ok(Json(report))
}

async fn audit_export_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(request): Query<AuditExportRequest>,
) -> std::result::Result<Json<AuditExportResponse>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let configured_limit = state.config.observability.audit_log_export_limit;
    let limit = request.limit.unwrap_or(configured_limit);
    if limit == 0 || limit > configured_limit {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_audit_export_limit",
            format!(
                "audit export limit must be between 1 and {}",
                configured_limit
            ),
        ));
    }

    let events = export_audit_events(&state.audit_log_path, limit).map_err(ApiError::from_error)?;
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "audit_export",
            "audit_log",
            "success",
            format!("exported {} audit events", events.len()),
        ),
    )?;
    Ok(Json(AuditExportResponse { events }))
}

async fn replication_status_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<ReplicationStatusResponse>, ApiError> {
    authorize(&headers, &state, Action::Maintain)?;
    let database = lock_database(&state).await;
    Ok(Json(database.replication_status()))
}

async fn replication_history_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<ReplicationHistoryQuery>,
) -> std::result::Result<Json<Vec<ReplicationRecord>>, ApiError> {
    authorize(&headers, &state, Action::Maintain)?;
    let database = lock_database(&state).await;
    Ok(Json(database.replication_history_since(
        query.after_source_lsn.unwrap_or(0),
    )))
}

async fn configure_leader_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<ReplicationStatusResponse>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let response = {
        let mut database = lock_database_mut(&state).await;
        database
            .configure_as_leader()
            .map_err(ApiError::from_error)?
    };
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "configure_leader",
            "replication",
            "success",
            response.status.local_node_id.clone(),
        ),
    )?;
    Ok(Json(response))
}

async fn configure_follower_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<ConfigureFollowerRequest>,
) -> std::result::Result<Json<ReplicationStatusResponse>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let response = {
        let mut database = lock_database_mut(&state).await;
        database
            .configure_as_follower(request.leader_node_id.clone(), request.leader_address)
            .map_err(ApiError::from_error)?
    };
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "configure_follower",
            "replication",
            "success",
            request.leader_node_id,
        ),
    )?;
    Ok(Json(response))
}

async fn register_replica_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<RegisterReplicaRequest>,
) -> std::result::Result<Json<ClusterTopology>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let topology = {
        let mut database = lock_database_mut(&state).await;
        database
            .register_replica(request.node_id.clone(), request.address.clone())
            .map_err(ApiError::from_error)?
    };
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "register_replica",
            "cluster",
            "success",
            request.node_id,
        ),
    )?;
    Ok(Json(topology))
}

async fn replica_ack_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<ReplicaAckRequest>,
) -> std::result::Result<Json<ReplicationStatusResponse>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let response = {
        let mut database = lock_database_mut(&state).await;
        database
            .acknowledge_replica(&request.replica_node_id, request.source_lsn)
            .map_err(ApiError::from_error)?
    };
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "replica_ack",
            "replication",
            "success",
            request.replica_node_id,
        ),
    )?;
    Ok(Json(response))
}

async fn apply_replication_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<ApplyReplicationRequest>,
) -> std::result::Result<Json<ReplicationStatusResponse>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let response = {
        let mut database = lock_database_mut(&state).await;
        database
            .apply_replication_records(&request.records)
            .map_err(ApiError::from_error)?
    };
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "apply_replication",
            "replication",
            "success",
            format!("records={}", request.records.len()),
        ),
    )?;
    Ok(Json(response))
}

async fn cluster_topology_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Json<ClusterTopology>, ApiError> {
    authorize(&headers, &state, Action::Maintain)?;
    let database = lock_database(&state).await;
    Ok(Json(database.cluster_topology()))
}

async fn mark_node_health_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<MarkNodeHealthRequest>,
) -> std::result::Result<Json<ClusterTopology>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let topology = {
        let mut database = lock_database_mut(&state).await;
        database
            .mark_node_health(&id, request.healthy)
            .map_err(ApiError::from_error)?
    };
    record_audit(
        &state,
        AuditEvent::new(principal.name, "mark_node_health", "cluster", "success", id),
    )?;
    Ok(Json(topology))
}

async fn promote_node_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<PromoteNodeRequest>,
) -> std::result::Result<Json<FailoverPlan>, ApiError> {
    let principal = authorize(&headers, &state, Action::Maintain)?;
    let plan = {
        let mut database = lock_database_mut(&state).await;
        database
            .promote_node(&request.node_id)
            .map_err(ApiError::from_error)?
    };
    record_audit(
        &state,
        AuditEvent::new(
            principal.name,
            "promote_node",
            "cluster",
            "success",
            request.node_id,
        ),
    )?;
    Ok(Json(plan))
}

fn indexes_with_mutation(base: &GraphIndex, mutation: &BatchMutation) -> GraphIndex {
    let mut indexes = base.clone();
    for edge in mutation.removed_edges.values() {
        indexes.delete_edge(edge);
    }
    for node in mutation.removed_nodes.values() {
        indexes.delete_node(node);
    }
    for node in mutation.added_nodes.values() {
        indexes.upsert_node(node);
    }
    for edge in mutation.added_edges.values() {
        indexes.upsert_edge(edge);
    }
    indexes
}

fn graph_mutation_from_batch(batch: &BatchMutation) -> GraphMutation {
    GraphMutation {
        removed_node_ids: batch.removed_nodes.keys().cloned().collect(),
        removed_edge_ids: batch.removed_edges.keys().cloned().collect(),
        added_nodes: batch.added_nodes.clone(),
        added_edges: batch.added_edges.clone(),
    }
}

fn snapshot_with_mutation(base: &GraphSnapshot, mutation: &BatchMutation) -> GraphSnapshot {
    let indexes = indexes_with_mutation(&base.indexes, mutation);
    let mut nodes = base.nodes.clone();
    let mut edges = base.edges.clone();
    for edge_id in mutation.removed_edges.keys() {
        edges.remove(edge_id);
    }
    for node_id in mutation.removed_nodes.keys() {
        nodes.remove(node_id);
    }
    for (node_id, node) in &mutation.added_nodes {
        nodes.insert(node_id.clone(), node.clone());
    }
    for (edge_id, edge) in &mutation.added_edges {
        edges.insert(edge_id.clone(), edge.clone());
    }

    GraphSnapshot {
        nodes,
        edges,
        indexes,
    }
}

fn authorize(
    headers: &HeaderMap,
    state: &ApiState,
    action: Action,
) -> std::result::Result<Principal, ApiError> {
    if !state.config.auth.enabled {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth_disabled",
            "authentication is disabled for this deployment; enable auth to serve requests",
        ));
    }

    let Some(api_key) = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
    else {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing x-api-key header",
        ));
    };

    let principal = ApiKeyAuthenticator::authenticate(&state.config.auth, api_key)
        .map_err(|_| ApiError::new(StatusCode::UNAUTHORIZED, "unauthorized", "invalid API key"))?;
    enforce_rate_limit(state, api_key)?;

    if !Authorizer::is_allowed(principal.clone(), action) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "principal is not authorized for this action",
        ));
    }

    Ok(principal)
}

async fn lock_database(state: &ApiState) -> tokio::sync::RwLockReadGuard<'_, Database> {
    state.database.read().await
}

async fn lock_database_mut(state: &ApiState) -> tokio::sync::RwLockWriteGuard<'_, Database> {
    state.database.write().await
}

fn increment_counter(state: &ApiState, mutate: impl FnOnce(&AtomicServiceCounters)) {
    mutate(&state.counters);
}

fn record_latency(histogram: &AtomicLatencyHistogram, elapsed_ms: u64) {
    histogram.record(elapsed_ms);
}

fn record_api_error_metric(state: &ApiState, status: StatusCode, code: &str) {
    let key = format!("{code}|{}", status.as_u16());
    let entry = state.counters.error_counters.entry(key).or_default();
    entry.fetch_add(1, Ordering::Relaxed);
}

fn request_trace_id(headers: &HeaderMap) -> String {
    headers
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_traceparent_trace_id)
        .unwrap_or_else(generate_trace_id)
}

fn parse_traceparent_trace_id(traceparent: &str) -> Option<String> {
    let mut segments = traceparent.split('-');
    let _version = segments.next()?;
    let trace_id = segments.next()?;
    let _parent_id = segments.next()?;
    let _flags = segments.next()?;
    if segments.next().is_some() {
        return None;
    }
    if trace_id.len() == 32
        && trace_id
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        Some(trace_id.to_ascii_lowercase())
    } else {
        None
    }
}

fn generate_trace_id() -> String {
    static TRACE_COUNTER: AtomicU64 = AtomicU64::new(1);
    let now = now_epoch_ms() as u64;
    let counter = TRACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{now:016x}{counter:016x}")
}

fn query_request_kind(request: &QueryRequest) -> &'static str {
    match request {
        QueryRequest::GetNodeById { .. } => "get_node_by_id",
        QueryRequest::GetNodeByUniqueKey { .. } => "get_node_by_unique_key",
        QueryRequest::FilterNodes { .. } => "filter_nodes",
        QueryRequest::ListNeighbors { .. } => "list_neighbors",
        QueryRequest::Traverse { .. } => "traverse",
        QueryRequest::ShortestPath { .. } => "shortest_path",
        QueryRequest::SearchByLabel { .. } => "search_by_label",
        QueryRequest::TimeRange { .. } => "time_range",
        QueryRequest::VectorSearch { .. } => "vector_search",
        QueryRequest::RankedRetrieval { .. } => "ranked_retrieval",
    }
}

fn record_audit(state: &ApiState, event: AuditEvent) -> std::result::Result<(), ApiError> {
    append_audit_event(&state.audit_log_path, &event).map_err(ApiError::from_error)?;
    prune_audit_log(
        &state.audit_log_path,
        state.config.observability.audit_log_retention_entries,
    )
    .map_err(ApiError::from_error)?;
    increment_counter(state, |counters| {
        counters.audit_events_total.fetch_add(1, Ordering::Relaxed);
    });
    Ok(())
}

async fn enforce_maintenance_budget(
    state: &ApiState,
    operation: &str,
) -> std::result::Result<(usize, usize), ApiError> {
    let database = lock_database(state).await;
    let node_count = database.engine.node_count();
    let edge_count = database.engine.edge_count();
    drop(database);

    if node_count > state.config.maintenance.max_node_count
        || edge_count > state.config.maintenance.max_edge_count
    {
        state
            .mark_maintenance_finished(
                operation,
                "rejected",
                format!(
                    "maintenance budget exceeded: nodes={node_count} edges={edge_count} budgets=({},{})",
                    state.config.maintenance.max_node_count, state.config.maintenance.max_edge_count
                ),
                now_epoch_ms(),
                0,
                node_count,
                edge_count,
            )
            .await;
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "maintenance_budget_exceeded",
            format!(
                "operation '{operation}' exceeds maintenance budget: nodes={node_count}/{} edges={edge_count}/{}",
                state.config.maintenance.max_node_count, state.config.maintenance.max_edge_count
            ),
        ));
    }

    Ok((node_count, edge_count))
}

fn emit_log(event: StructuredLogEvent) {
    event.emit_via_tracing();
}

fn enforce_rate_limit(state: &ApiState, api_key: &str) -> std::result::Result<(), ApiError> {
    let now_ms = now_epoch_ms();
    let mut bucket = state
        .rate_limits
        .entry(api_key.to_owned())
        .or_insert(TokenBucket {
            tokens: 1_000.0,
            last_refill_ms: now_ms,
        });
    let elapsed_ms = now_ms.saturating_sub(bucket.last_refill_ms);
    let refill = (elapsed_ms as f64 / 1_000.0) * 100.0;
    bucket.tokens = (bucket.tokens + refill).min(1_000.0);
    bucket.last_refill_ms = now_ms;

    if bucket.tokens < 1.0 {
        return Err(ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "API key token bucket exhausted",
        ));
    }

    bucket.tokens -= 1.0;
    Ok(())
}

fn record_query_metrics(
    state: &ApiState,
    request: &QueryRequest,
    response: &QueryResponse,
    elapsed_ms: u64,
) -> std::result::Result<(), ApiError> {
    increment_counter(state, |counters| {
        counters
            .query_requests_total
            .fetch_add(1, Ordering::Relaxed);
        record_latency(&counters.query_latency, elapsed_ms);
        match request {
            QueryRequest::Traverse { .. } | QueryRequest::ShortestPath { .. } => {
                record_latency(&counters.traversal_latency, elapsed_ms);
            }
            QueryRequest::VectorSearch { .. } => {
                record_latency(&counters.vector_search_latency, elapsed_ms);
            }
            QueryRequest::RankedRetrieval { .. } => {
                record_latency(&counters.ranked_retrieval_latency, elapsed_ms);
            }
            _ => {}
        }
        for result in &response.ranked_results {
            match result.score {
                score if score < 0.25 => {
                    counters
                        .retrieval_score_bucket_low_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                score if score < 0.50 => {
                    counters
                        .retrieval_score_bucket_medium_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                score if score < 0.75 => {
                    counters
                        .retrieval_score_bucket_high_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    counters
                        .retrieval_score_bucket_top_total
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });
    Ok(())
}

fn record_query_stream_metrics(
    state: &ApiState,
    request: &QueryRequest,
    summary: &StreamRenderSummary,
    elapsed_ms: u64,
) -> std::result::Result<(), ApiError> {
    increment_counter(state, |counters| {
        counters
            .query_requests_total
            .fetch_add(1, Ordering::Relaxed);
        record_latency(&counters.query_latency, elapsed_ms);
        match request {
            QueryRequest::Traverse { .. } | QueryRequest::ShortestPath { .. } => {
                record_latency(&counters.traversal_latency, elapsed_ms);
            }
            QueryRequest::VectorSearch { .. } => {
                record_latency(&counters.vector_search_latency, elapsed_ms);
            }
            QueryRequest::RankedRetrieval { .. } => {
                record_latency(&counters.ranked_retrieval_latency, elapsed_ms);
            }
            _ => {}
        }
        for score in &summary.ranked_scores {
            match *score {
                score if score < 0.25 => {
                    counters
                        .retrieval_score_bucket_low_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                score if score < 0.50 => {
                    counters
                        .retrieval_score_bucket_medium_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                score if score < 0.75 => {
                    counters
                        .retrieval_score_bucket_high_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    counters
                        .retrieval_score_bucket_top_total
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });
    Ok(())
}

fn render_query_stream(
    execution: QueryExecution<'_>,
) -> std::result::Result<(Response, StreamRenderSummary), ApiError> {
    let (lines, summary) = serialize_query_execution_stream(execution)?;
    let body = Body::from_stream(stream::iter(
        lines
            .into_iter()
            .map(|line| Ok::<Bytes, Infallible>(Bytes::from(line))),
    ));
    Ok((
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/x-ndjson")],
            body,
        )
            .into_response(),
        summary,
    ))
}

fn serialize_query_execution_stream(
    execution: QueryExecution<'_>,
) -> std::result::Result<(Vec<String>, StreamRenderSummary), ApiError> {
    let plan_kind = execution.plan_kind;
    let retrieval_profile = execution.retrieval_profile.clone();
    let mut lines = Vec::new();
    let mut summary = StreamRenderSummary::default();
    let mut total_bytes = 0_usize;
    let mut item_count = 0_usize;

    push_stream_frame(
        &mut lines,
        &mut total_bytes,
        &mut item_count,
        QueryStreamFrame::Meta {
            plan_kind,
            retrieval_profile,
        },
        0,
    )?;

    for item in execution.into_items() {
        let (frame, counted_items) = match item {
            QueryExecutionItem::Node(node) => (QueryStreamFrame::Node { node }, 1),
            QueryExecutionItem::Edge(edge) => (QueryStreamFrame::Edge { edge }, 1),
            QueryExecutionItem::RankedResult(result) => {
                summary.ranked_scores.push(result.score);
                (QueryStreamFrame::RankedResult { result }, 1)
            }
            QueryExecutionItem::Path(path) => {
                let count = path.node_ids.len() + path.edge_ids.len();
                (QueryStreamFrame::Path { path }, count)
            }
        };
        push_stream_frame(
            &mut lines,
            &mut total_bytes,
            &mut item_count,
            frame,
            counted_items,
        )?;
    }

    let end_item_count = item_count;
    push_stream_frame(
        &mut lines,
        &mut total_bytes,
        &mut item_count,
        QueryStreamFrame::End {
            item_count: end_item_count,
        },
        0,
    )?;
    Ok((lines, summary))
}

fn push_stream_frame(
    lines: &mut Vec<String>,
    total_bytes: &mut usize,
    item_count: &mut usize,
    frame: QueryStreamFrame,
    counted_items: usize,
) -> std::result::Result<(), ApiError> {
    *item_count += counted_items;
    if *item_count > MAX_STREAM_QUERY_ITEMS {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "stream_query_result_too_large",
            format!(
                "stream query response exceeded budget: {} items > {MAX_STREAM_QUERY_ITEMS}",
                *item_count
            ),
        ));
    }
    let mut line = serde_json::to_string(&frame).map_err(|error| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "serialization_error",
            format!("failed to serialize query stream frame: {error}"),
        )
    })?;
    line.push('\n');
    *total_bytes += line.len();
    if *total_bytes > MAX_STREAM_RESPONSE_BYTES {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "stream_query_body_too_large",
            format!(
                "stream query body exceeded budget: {} bytes > {MAX_STREAM_RESPONSE_BYTES}",
                *total_bytes
            ),
        ));
    }
    lines.push(line);
    Ok(())
}

fn record_endpoint_metrics(
    state: &ApiState,
    method: &str,
    route: &str,
    status: StatusCode,
    elapsed: Duration,
) {
    let key = format!("{method} {route}");
    let entry = state.endpoint_metrics.entry(key).or_default();
    let elapsed_ms = elapsed.as_millis() as u64;
    entry.requests_total.fetch_add(1, Ordering::Relaxed);
    entry
        .latency_ms_total
        .fetch_add(elapsed_ms, Ordering::Relaxed);
    match status.as_u16() {
        200..=299 => {
            entry.responses_2xx_total.fetch_add(1, Ordering::Relaxed);
        }
        400..=499 => {
            entry.responses_4xx_total.fetch_add(1, Ordering::Relaxed);
        }
        500..=599 => {
            entry.responses_5xx_total.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
    match elapsed_ms {
        0..=10 => {
            entry.latency_le_10ms_total.fetch_add(1, Ordering::Relaxed);
        }
        11..=50 => {
            entry.latency_le_50ms_total.fetch_add(1, Ordering::Relaxed);
        }
        51..=250 => {
            entry.latency_le_250ms_total.fetch_add(1, Ordering::Relaxed);
        }
        _ => {
            entry.latency_gt_250ms_total.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn snapshot_endpoint_metrics(state: &ApiState) -> Vec<EndpointMetricsSnapshot> {
    let mut metrics = state
        .endpoint_metrics
        .iter()
        .filter_map(|entry| {
            let (method, route) = entry.key().split_once(' ')?;
            Some(EndpointMetricsSnapshot {
                method: method.to_owned(),
                route: route.to_owned(),
                requests_total: entry.requests_total.load(Ordering::Relaxed),
                responses_2xx_total: entry.responses_2xx_total.load(Ordering::Relaxed),
                responses_4xx_total: entry.responses_4xx_total.load(Ordering::Relaxed),
                responses_5xx_total: entry.responses_5xx_total.load(Ordering::Relaxed),
                latency_ms_total: entry.latency_ms_total.load(Ordering::Relaxed),
                latency_le_10ms_total: entry.latency_le_10ms_total.load(Ordering::Relaxed),
                latency_le_50ms_total: entry.latency_le_50ms_total.load(Ordering::Relaxed),
                latency_le_250ms_total: entry.latency_le_250ms_total.load(Ordering::Relaxed),
                latency_gt_250ms_total: entry.latency_gt_250ms_total.load(Ordering::Relaxed),
            })
        })
        .collect::<Vec<_>>();
    metrics.sort_by(|left, right| {
        left.route
            .cmp(&right.route)
            .then_with(|| left.method.cmp(&right.method))
    });
    metrics
}

fn enforce_query_response_budget(response: &QueryResponse) -> std::result::Result<(), ApiError> {
    let total_items = response.nodes.len()
        + response.edges.len()
        + response.ranked_results.len()
        + response
            .path
            .as_ref()
            .map(|path| path.node_ids.len() + path.edge_ids.len())
            .unwrap_or(0);
    if total_items > MAX_QUERY_RESPONSE_ITEMS {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "query_result_too_large",
            format!(
                "query response exceeded budget: {total_items} items > {MAX_QUERY_RESPONSE_ITEMS}"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use axum::{
        body::{to_bytes, Body},
        http::{header, Request, StatusCode},
    };
    use tower::util::ServiceExt;
    use undr9_common::{EdgeId, NodeId};
    use undr9_config::AppConfig;
    use undr9_core::{
        EdgeRecord, IsolationLevel, NodeRecord, PropertyValue, TransactionOperation, WriteBatch,
    };
    use undr9_index::EdgeDirection;
    use undr9_query::{FilterExpression, QueryRequest};
    use undr9_replication::ReplicationRecord;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn test_state() -> super::ApiState {
        let mut config = AppConfig::default();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        config.storage.root_dir =
            std::env::temp_dir().join(format!("undr9-api-test-{nanos}-{unique}"));
        super::ApiState::try_new(config).expect("API state should initialize")
    }

    fn test_state_with_identity(node_id: &str) -> super::ApiState {
        let mut config = AppConfig::default();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        config.storage.root_dir =
            std::env::temp_dir().join(format!("undr9-api-test-{node_id}-{nanos}-{unique}"));
        let bind = format!("127.0.0.1:{}", 9_000 + unique);
        config.server.bind_address = bind.clone();
        super::ApiState::try_new_with_identity(config, node_id.to_owned(), bind)
            .expect("API state should initialize")
    }

    #[tokio::test]
    async fn health_endpoint_reports_ok() {
        let app = super::build_router(test_state());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("request should be built"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let payload = String::from_utf8(body.to_vec()).expect("body should be UTF-8");
        assert!(payload.contains("\"status\":\"ok\""));
    }

    #[tokio::test]
    async fn readiness_endpoint_reflects_startup_state() {
        let state = test_state();
        state.set_readiness(false);
        let app = super::build_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("request should be built"),
            )
            .await
            .expect("router should respond");

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn metrics_endpoint_includes_per_route_request_series() {
        let app = super::build_router(test_state());
        let health_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("request should be built"),
            )
            .await
            .expect("router should respond");
        assert_eq!(health_response.status(), StatusCode::OK);

        let metrics_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request should be built"),
            )
            .await
            .expect("router should respond");
        assert_eq!(metrics_response.status(), StatusCode::OK);

        let body = to_bytes(metrics_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let payload = String::from_utf8(body.to_vec()).expect("body should be UTF-8");
        assert!(payload.contains("undr9_http_requests_total"));
        assert!(payload.contains("route=\"/healthz\""));
        assert!(payload.contains("method=\"GET\""));
        assert!(payload.contains("status_class=\"2xx\""));
        assert!(payload.contains("undr9_http_request_duration_ms_bucket"));
    }

    #[tokio::test]
    async fn unauthorized_errors_emit_stable_header_and_metrics_series() {
        let app = super::build_router(test_state());
        let traceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/nodes")
                    .header("traceparent", traceparent)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "id": "node_1",
                            "node_type": "memory",
                            "properties": {}
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response
                .headers()
                .get("x-undr9-error-code")
                .and_then(|value| value.to_str().ok()),
            Some("unauthorized")
        );
        assert_eq!(
            response
                .headers()
                .get(super::TRACE_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("0123456789abcdef0123456789abcdef")
        );

        let metrics_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request should be built"),
            )
            .await
            .expect("router should respond");
        assert_eq!(metrics_response.status(), StatusCode::OK);
        let body = to_bytes(metrics_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let payload = String::from_utf8(body.to_vec()).expect("body should be UTF-8");
        assert!(payload.contains("undr9_errors_total"));
        assert!(payload.contains("code=\"unauthorized\""));
        assert!(payload.contains("status=\"401\""));
        assert!(payload.contains("undr9_query_latency_ms_bucket"));
    }

    #[tokio::test]
    async fn rejects_missing_api_key() {
        let app = super::build_router(test_state());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/nodes")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "id": "node_1",
                            "node_type": "memory",
                            "properties": {}
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_disabled_deployment_fails_closed() {
        let mut state = test_state();
        state.config.auth.enabled = false;
        let app = super::build_router(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::SearchByLabel {
                            label: "memory".to_owned(),
                            limit: None,
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn rejects_query_response_that_exceeds_budget() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        {
            let mut database = state.database.write().await;
            for index in 0..600 {
                let node = NodeRecord::new(
                    NodeId::new(format!("budget_node_{index}")).expect("valid node id"),
                    "memory",
                )
                .expect("node should build")
                .with_vector("default", vec![1.0])
                .expect("vector should build");
                database.upsert_node(node).expect("node should insert");
            }
        }
        let app = super::build_router(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::SearchByLabel {
                            label: "memory".to_owned(),
                            limit: Some(600),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::VectorSearch {
                            query_vector: vec![1.0],
                            node_type: Some("memory".to_owned()),
                            vector_name: None,
                            limit: 600,
                            top_k: None,
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let stream_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query/stream")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::VectorSearch {
                            query_vector: vec![1.0],
                            node_type: Some("memory".to_owned()),
                            vector_name: None,
                            limit: 600,
                            top_k: None,
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(stream_response.status(), StatusCode::OK);
        assert_eq!(
            stream_response.headers().get(header::CONTENT_TYPE),
            Some(&header::HeaderValue::from_static("application/x-ndjson"))
        );
        let stream_body = to_bytes(stream_response.into_body(), usize::MAX)
            .await
            .expect("stream body should be readable");
        let payload = String::from_utf8(stream_body.to_vec()).expect("stream body should be UTF-8");
        assert!(payload.contains("\"type\":\"meta\""));
        assert!(payload.contains("\"type\":\"ranked_result\""));
        assert!(!payload.contains("\"type\":\"node\""));
    }

    #[tokio::test]
    async fn traversal_stream_endpoint_emits_incremental_ndjson_frames() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        {
            let mut database = state.database.write().await;
            let node_a = NodeRecord::new(NodeId::new("stream_a").expect("valid id"), "memory")
                .expect("node should build");
            let node_b = NodeRecord::new(NodeId::new("stream_b").expect("valid id"), "memory")
                .expect("node should build");
            let edge = EdgeRecord::new(
                EdgeId::new("stream_edge_ab").expect("valid id"),
                node_a.id.clone(),
                node_b.id.clone(),
                "relates_to",
            )
            .expect("edge should build");
            database.upsert_node(node_a).expect("node should insert");
            database.upsert_node(node_b).expect("node should insert");
            database.upsert_edge(edge).expect("edge should insert");
        }
        let app = super::build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query/stream")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::Traverse {
                            start_node_id: NodeId::new("stream_a").expect("valid id"),
                            edge_type: None,
                            max_hops: Some(1),
                            direction: EdgeDirection::Outgoing,
                            limit: Some(10),
                            timeout_ms: None,
                            constraints: None,
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let payload = String::from_utf8(body.to_vec()).expect("body should be UTF-8");
        let frames = payload
            .lines()
            .map(|line| {
                serde_json::from_str::<serde_json::Value>(line).expect("frame should parse")
            })
            .collect::<Vec<_>>();

        assert_eq!(frames[0]["type"], "meta");
        assert_eq!(frames[1]["type"], "node");
        assert_eq!(frames[2]["type"], "edge");
        assert_eq!(frames[3]["type"], "node");
        assert_eq!(frames.last().expect("end frame exists")["type"], "end");
    }

    #[tokio::test]
    async fn performs_crud_and_query_round_trip() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        let node_a = serde_json::json!({
            "id": "node_a",
            "node_type": "memory",
            "properties": {
                "unique_key": {"kind":"String","value":"alpha"},
                "timestamp": {"kind":"Integer","value":1000},
                "importance": {"kind":"Float","value":0.9},
                "confidence": {"kind":"Float","value":0.8}
            },
            "vectors": {
                "default": [1.0, 0.0]
            }
        });
        let node_b = serde_json::json!({
            "id": "node_b",
            "node_type": "memory",
            "properties": {
                "timestamp": {"kind":"Integer","value":1100},
                "importance": {"kind":"Float","value":0.6},
                "confidence": {"kind":"Float","value":0.7}
            },
            "vectors": {
                "default": [0.7, 0.3]
            }
        });
        let edge = serde_json::json!({
            "id": "edge_ab",
            "source": "node_a",
            "target": "node_b",
            "edge_type": "relates_to",
            "properties": {}
        });

        for payload in [&node_a, &node_b] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/nodes")
                        .header("x-api-key", &admin_key)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            serde_json::to_vec(payload).expect("json should serialize"),
                        ))
                        .expect("request should build"),
                )
                .await
                .expect("router should respond");
            assert_eq!(response.status(), StatusCode::OK);
        }

        let edge_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/edges")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&edge).expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(edge_response.status(), StatusCode::OK);

        let get_node = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes/node_a")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(get_node.status(), StatusCode::OK);

        let neighbor_query = serde_json::json!({
            "ListNeighbors": {
                "node_id": "node_a",
                "edge_type": "relates_to",
                "direction": "Outgoing"
            }
        });
        let neighbor_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&neighbor_query).expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(neighbor_response.status(), StatusCode::OK);
        let neighbor_body = to_bytes(neighbor_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let neighbor_json: serde_json::Value =
            serde_json::from_slice(&neighbor_body).expect("response should be json");
        assert_eq!(
            neighbor_json["nodes"]
                .as_array()
                .expect("nodes should be array")
                .len(),
            1
        );

        let traversal_query = serde_json::to_vec(&QueryRequest::Traverse {
            start_node_id: NodeId::new("node_a").expect("node id should build"),
            edge_type: Some("relates_to".to_owned()),
            direction: EdgeDirection::Outgoing,
            max_hops: Some(1),
            limit: Some(100),
            timeout_ms: Some(5_000),
            constraints: None,
        })
        .expect("query should serialize");
        let traversal_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(traversal_query))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(traversal_response.status(), StatusCode::OK);

        let unique_lookup = serde_json::to_vec(&QueryRequest::GetNodeByUniqueKey {
            unique_key: "alpha".to_owned(),
        })
        .expect("query should serialize");
        let unique_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(unique_lookup))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(unique_response.status(), StatusCode::OK);

        let time_range = serde_json::to_vec(&QueryRequest::TimeRange {
            field: "timestamp".to_owned(),
            from_epoch_ms: 900,
            to_epoch_ms: 2_000,
            limit: 10,
        })
        .expect("query should serialize");
        let time_range_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(time_range))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(time_range_response.status(), StatusCode::OK);

        let vector_search = serde_json::to_vec(&QueryRequest::VectorSearch {
            query_vector: vec![1.0, 0.0],
            node_type: Some("memory".to_owned()),
            vector_name: None,
            limit: 2,
            top_k: None,
        })
        .expect("query should serialize");
        let vector_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(vector_search))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(vector_response.status(), StatusCode::OK);
        let vector_body = to_bytes(vector_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let vector_json: serde_json::Value =
            serde_json::from_slice(&vector_body).expect("response should be json");
        assert_eq!(
            vector_json["ranked_results"]
                .as_array()
                .expect("ranked results should be array")
                .len(),
            2
        );

        let ranked = serde_json::to_vec(&QueryRequest::RankedRetrieval {
            query_vector: Some(vec![1.0, 0.0]),
            reference_node_id: Some(NodeId::new("node_a").expect("node id should build")),
            edge_type: Some("relates_to".to_owned()),
            from_epoch_ms: Some(900),
            to_epoch_ms: Some(10_000),
            vector_name: None,
            limit: 2,
            top_k: None,
            now_epoch_ms: 1_200,
            retrieval_profile: Some("v1-default".to_owned()),
        })
        .expect("query should serialize");
        let ranked_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(ranked))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(ranked_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn filter_nodes_query_and_stream_support_multiple_examples() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        for payload in [
            serde_json::json!({
                "id": "user_alice",
                "node_type": "user",
                "properties": {
                    "unique_key": {"kind":"String","value":"alice"},
                    "score": {"kind":"Integer","value":95}
                }
            }),
            serde_json::json!({
                "id": "user_bob",
                "node_type": "user",
                "properties": {
                    "unique_key": {"kind":"String","value":"bob"},
                    "score": {"kind":"Integer","value":88}
                }
            }),
            serde_json::json!({
                "id": "service_runner",
                "node_type": "service",
                "properties": {
                    "unique_key": {"kind":"String","value":"svc-runner"},
                    "score": {"kind":"Integer","value":99}
                }
            }),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/nodes")
                        .header("x-api-key", &admin_key)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&payload).expect("json should serialize"),
                        ))
                        .expect("request should build"),
                )
                .await
                .expect("router should respond");
            assert_eq!(response.status(), StatusCode::OK);
        }

        let label_and_numeric_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::FilterNodes {
                            label: Some("user".to_owned()),
                            filter: FilterExpression::Gt {
                                field: "score".to_owned(),
                                value: PropertyValue::Integer(90),
                            },
                            limit: Some(10),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(label_and_numeric_response.status(), StatusCode::OK);
        let label_and_numeric_body = to_bytes(label_and_numeric_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let label_and_numeric_json: serde_json::Value =
            serde_json::from_slice(&label_and_numeric_body).expect("response should be json");
        assert_eq!(label_and_numeric_json["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(label_and_numeric_json["nodes"][0]["id"], "user_alice");

        let id_match_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::FilterNodes {
                            label: None,
                            filter: FilterExpression::Eq {
                                field: "id".to_owned(),
                                value: PropertyValue::String("user_bob".to_owned()),
                            },
                            limit: Some(5),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(id_match_response.status(), StatusCode::OK);
        let id_match_body = to_bytes(id_match_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let id_match_json: serde_json::Value =
            serde_json::from_slice(&id_match_body).expect("response should be json");
        assert_eq!(id_match_json["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(id_match_json["nodes"][0]["id"], "user_bob");

        let label_field_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::FilterNodes {
                            label: None,
                            filter: FilterExpression::Eq {
                                field: "label".to_owned(),
                                value: PropertyValue::String("service".to_owned()),
                            },
                            limit: Some(5),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(label_field_response.status(), StatusCode::OK);
        let label_field_body = to_bytes(label_field_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let label_field_json: serde_json::Value =
            serde_json::from_slice(&label_field_body).expect("response should be json");
        assert_eq!(label_field_json["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(label_field_json["nodes"][0]["id"], "service_runner");

        let stream_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query/stream")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::FilterNodes {
                            label: Some("user".to_owned()),
                            filter: FilterExpression::Or {
                                conditions: vec![
                                    FilterExpression::Gt {
                                        field: "score".to_owned(),
                                        value: PropertyValue::Integer(90),
                                    },
                                    FilterExpression::Eq {
                                        field: "unique_key".to_owned(),
                                        value: PropertyValue::String("bob".to_owned()),
                                    },
                                ],
                            },
                            limit: Some(10),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(stream_response.status(), StatusCode::OK);
        assert_eq!(
            stream_response.headers().get(header::CONTENT_TYPE),
            Some(&header::HeaderValue::from_static("application/x-ndjson"))
        );
        let stream_body = to_bytes(stream_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let stream_payload = String::from_utf8(stream_body.to_vec()).expect("body should be UTF-8");
        let frames = stream_payload
            .lines()
            .map(|line| {
                serde_json::from_str::<serde_json::Value>(line).expect("frame should parse")
            })
            .collect::<Vec<_>>();
        assert_eq!(frames[0]["type"], "meta");
        let streamed_ids = frames
            .iter()
            .filter(|frame| frame["type"] == "node")
            .map(|frame| {
                frame["node"]["id"]
                    .as_str()
                    .expect("node id should exist")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(streamed_ids, vec!["user_alice", "user_bob"]);
        assert_eq!(frames.last().expect("end frame exists")["type"], "end");
    }

    #[tokio::test]
    async fn node_and_edge_endpoints_cover_read_update_and_delete_flows() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        for payload in [
            serde_json::json!({
                "id": "crud_node_a",
                "node_type": "memory",
                "properties": {
                    "value": {"kind":"Integer","value":1}
                }
            }),
            serde_json::json!({
                "id": "crud_node_b",
                "node_type": "memory",
                "properties": {
                    "value": {"kind":"Integer","value":2}
                }
            }),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/nodes")
                        .header("x-api-key", &admin_key)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&payload).expect("json should serialize"),
                        ))
                        .expect("request should build"),
                )
                .await
                .expect("router should respond");
            assert_eq!(response.status(), StatusCode::OK);
        }

        let update_node_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/nodes/crud_node_a")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "id": "crud_node_a",
                            "node_type": "memory",
                            "properties": {
                                "value": {"kind":"Integer","value":10},
                                "status": {"kind":"String","value":"updated"}
                            }
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(update_node_response.status(), StatusCode::OK);

        let get_node_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes/crud_node_a")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(get_node_response.status(), StatusCode::OK);
        let get_node_body = to_bytes(get_node_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let get_node_json: serde_json::Value =
            serde_json::from_slice(&get_node_body).expect("response should be json");
        assert_eq!(get_node_json["properties"]["status"]["value"], "updated");

        let create_edge_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/edges")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "id": "crud_edge_ab",
                            "source": "crud_node_a",
                            "target": "crud_node_b",
                            "edge_type": "relates_to",
                            "properties": {
                                "strength": {"kind":"Integer","value":1}
                            }
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create_edge_response.status(), StatusCode::OK);

        let get_edge_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/edges/crud_edge_ab")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(get_edge_response.status(), StatusCode::OK);

        let update_edge_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/edges/crud_edge_ab")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "id": "crud_edge_ab",
                            "source": "crud_node_a",
                            "target": "crud_node_b",
                            "edge_type": "relates_to",
                            "properties": {
                                "strength": {"kind":"Integer","value":5},
                                "status": {"kind":"String","value":"verified"}
                            }
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(update_edge_response.status(), StatusCode::OK);
        let update_edge_body = to_bytes(update_edge_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let update_edge_json: serde_json::Value =
            serde_json::from_slice(&update_edge_body).expect("response should be json");
        assert_eq!(update_edge_json["properties"]["strength"]["value"], 5);
        assert_eq!(
            update_edge_json["properties"]["status"]["value"],
            "verified"
        );

        let delete_edge_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/edges/crud_edge_ab")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(delete_edge_response.status(), StatusCode::NO_CONTENT);

        let get_deleted_edge_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/edges/crud_edge_ab")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(get_deleted_edge_response.status(), StatusCode::NOT_FOUND);

        let delete_node_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/nodes/crud_node_b")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(delete_node_response.status(), StatusCode::NO_CONTENT);

        let get_deleted_node_response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes/crud_node_b")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(get_deleted_node_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn rejects_path_id_body_id_mismatch() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/nodes/node_a")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "id": "node_b",
                            "node_type": "memory",
                            "properties": {
                                "flag": {"kind":"Boolean","value": true}
                            }
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn transaction_endpoints_support_snapshot_reads_and_commit() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        let begin_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/transactions/begin")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "isolation_level": "Snapshot"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(begin_response.status(), StatusCode::OK);
        let begin_body = to_bytes(begin_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let begin_json: serde_json::Value =
            serde_json::from_slice(&begin_body).expect("response should be json");
        let transaction_id = begin_json["transaction_id"]
            .as_str()
            .expect("transaction id should exist")
            .to_owned();

        let node_a = NodeRecord::new(NodeId::new("node_tx").expect("valid node id"), "memory")
            .expect("node should build")
            .with_property("value", PropertyValue::Integer(1))
            .expect("property should build");
        let stage_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/transactions/{transaction_id}/operations"))
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&TransactionOperation::UpsertNode(node_a.clone()))
                            .expect("operation should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(stage_response.status(), StatusCode::OK);

        let tx_query_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/transactions/{transaction_id}/query"))
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::GetNodeById {
                            node_id: node_a.id.clone(),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(tx_query_response.status(), StatusCode::OK);
        let tx_query_body = to_bytes(tx_query_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let tx_query_json: serde_json::Value =
            serde_json::from_slice(&tx_query_body).expect("response should be json");
        assert_eq!(
            tx_query_json["nodes"]
                .as_array()
                .expect("nodes should be array")
                .len(),
            1
        );

        let committed_read = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes/node_tx")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(committed_read.status(), StatusCode::NOT_FOUND);

        let commit_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/transactions/{transaction_id}/commit"))
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(commit_response.status(), StatusCode::OK);

        let post_commit_read = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes/node_tx")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(post_commit_read.status(), StatusCode::OK);

        let conflict_begin = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/transactions/begin")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "isolation_level": IsolationLevel::Snapshot
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        let conflict_body = to_bytes(conflict_begin.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let conflict_json: serde_json::Value =
            serde_json::from_slice(&conflict_body).expect("response should be json");
        let conflict_transaction_id = conflict_json["transaction_id"]
            .as_str()
            .expect("transaction id should exist")
            .to_owned();

        let original_update =
            NodeRecord::new(NodeId::new("node_tx").expect("valid node id"), "memory")
                .expect("node should build")
                .with_property("value", PropertyValue::Integer(2))
                .expect("property should build");
        let conflicting_update =
            NodeRecord::new(NodeId::new("node_tx").expect("valid node id"), "memory")
                .expect("node should build")
                .with_property("value", PropertyValue::Integer(3))
                .expect("property should build");

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/v1/transactions/{conflict_transaction_id}/operations"
                    ))
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&TransactionOperation::UpsertNode(original_update))
                            .expect("operation should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        let direct_update = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/nodes/node_tx")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&conflicting_update).expect("node should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(direct_update.status(), StatusCode::OK);

        let conflict_commit = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/transactions/{conflict_transaction_id}/commit"))
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(conflict_commit.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn transaction_query_preserves_stale_unique_key_view() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        let original =
            NodeRecord::new(NodeId::new("node_lookup").expect("valid node id"), "memory")
                .expect("node should build")
                .with_property("unique_key", PropertyValue::String("goal-alpha".to_owned()))
                .expect("property should build")
                .with_property("value", PropertyValue::Integer(1))
                .expect("property should build");
        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/nodes")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&original).expect("node should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create_response.status(), StatusCode::OK);

        let begin_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/transactions/begin")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "isolation_level": "Snapshot"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        let begin_body = to_bytes(begin_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let begin_json: serde_json::Value =
            serde_json::from_slice(&begin_body).expect("response should be json");
        let transaction_id = begin_json["transaction_id"]
            .as_str()
            .expect("transaction id should exist")
            .to_owned();

        let updated = NodeRecord::new(
            NodeId::new("node_lookup").expect("valid node id"),
            "memory_updated",
        )
        .expect("node should build")
        .with_property("unique_key", PropertyValue::String("goal-beta".to_owned()))
        .expect("property should build")
        .with_property("value", PropertyValue::Integer(2))
        .expect("property should build");
        let update_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/nodes/node_lookup")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&updated).expect("node should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(update_response.status(), StatusCode::OK);

        let tx_query_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/transactions/{transaction_id}/query"))
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::GetNodeByUniqueKey {
                            unique_key: "goal-alpha".to_owned(),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(tx_query_response.status(), StatusCode::OK);
        let tx_query_body = to_bytes(tx_query_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let tx_query_json: serde_json::Value =
            serde_json::from_slice(&tx_query_body).expect("response should be json");
        assert_eq!(
            tx_query_json["nodes"][0]["properties"]["unique_key"]["value"],
            "goal-alpha"
        );

        let committed_query_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::GetNodeByUniqueKey {
                            unique_key: "goal-beta".to_owned(),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(committed_query_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn transaction_query_includes_staged_node_overlay() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        let begin_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/transactions/begin")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "isolation_level": "Snapshot"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        let begin_body = to_bytes(begin_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let begin_json: serde_json::Value =
            serde_json::from_slice(&begin_body).expect("response should be json");
        let transaction_id = begin_json["transaction_id"]
            .as_str()
            .expect("transaction id should exist")
            .to_owned();

        let staged = NodeRecord::new(NodeId::new("node_staged").expect("valid node id"), "memory")
            .expect("node should build")
            .with_property(
                "unique_key",
                PropertyValue::String("goal-staged".to_owned()),
            )
            .expect("property should build");
        let stage_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/transactions/{transaction_id}/operations"))
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&TransactionOperation::UpsertNode(staged.clone()))
                            .expect("operation should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(stage_response.status(), StatusCode::OK);

        let tx_query_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/transactions/{transaction_id}/query"))
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::GetNodeByUniqueKey {
                            unique_key: "goal-staged".to_owned(),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(tx_query_response.status(), StatusCode::OK);
        let tx_query_body = to_bytes(tx_query_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let tx_query_json: serde_json::Value =
            serde_json::from_slice(&tx_query_body).expect("response should be json");
        assert_eq!(tx_query_json["nodes"][0]["id"], staged.id.as_str());

        let committed_query_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/query")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::GetNodeByUniqueKey {
                            unique_key: "goal-staged".to_owned(),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(committed_query_response.status(), StatusCode::OK);
        let committed_query_body = to_bytes(committed_query_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let committed_query_json: serde_json::Value =
            serde_json::from_slice(&committed_query_body).expect("response should be json");
        assert!(committed_query_json["nodes"]
            .as_array()
            .expect("nodes should be an array")
            .is_empty());
    }

    #[tokio::test]
    async fn transaction_listing_summary_stream_and_rollback_endpoints_work() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        let begin_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/transactions/begin")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "isolation_level": "Snapshot"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(begin_response.status(), StatusCode::OK);
        let begin_body = to_bytes(begin_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let begin_json: serde_json::Value =
            serde_json::from_slice(&begin_body).expect("response should be json");
        let transaction_id = begin_json["transaction_id"]
            .as_str()
            .expect("transaction id should exist")
            .to_owned();

        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/transactions")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(list_response.status(), StatusCode::OK);
        let list_body = to_bytes(list_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let list_json: serde_json::Value =
            serde_json::from_slice(&list_body).expect("response should be json");
        assert_eq!(list_json.as_array().unwrap().len(), 1);
        assert_eq!(list_json[0]["transaction_id"], transaction_id);

        let summary_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/transactions/{transaction_id}"))
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(summary_response.status(), StatusCode::OK);
        let summary_body = to_bytes(summary_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let summary_json: serde_json::Value =
            serde_json::from_slice(&summary_body).expect("response should be json");
        assert_eq!(summary_json["transaction_id"], transaction_id);
        assert_eq!(summary_json["state"], "Active");

        let staged_node = NodeRecord::new(
            NodeId::new("tx_stream_node").expect("valid node id"),
            "memory",
        )
        .expect("node should build")
        .with_property("unique_key", PropertyValue::String("tx-stream".to_owned()))
        .expect("property should build");
        let stage_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/transactions/{transaction_id}/operations"))
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&TransactionOperation::UpsertNode(staged_node.clone()))
                            .expect("operation should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(stage_response.status(), StatusCode::OK);

        let stream_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/transactions/{transaction_id}/query/stream"))
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&QueryRequest::GetNodeById {
                            node_id: staged_node.id.clone(),
                        })
                        .expect("query should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(stream_response.status(), StatusCode::OK);
        assert_eq!(
            stream_response.headers().get(header::CONTENT_TYPE),
            Some(&header::HeaderValue::from_static("application/x-ndjson"))
        );
        let stream_body = to_bytes(stream_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let stream_payload = String::from_utf8(stream_body.to_vec()).expect("body should be UTF-8");
        assert!(stream_payload.contains("\"type\":\"meta\""));
        assert!(stream_payload.contains("\"type\":\"node\""));
        assert!(stream_payload.contains("\"id\":\"tx_stream_node\""));
        assert!(stream_payload.contains("\"type\":\"end\""));

        let rollback_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/transactions/{transaction_id}/rollback"))
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(rollback_response.status(), StatusCode::OK);
        let rollback_body = to_bytes(rollback_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let rollback_json: serde_json::Value =
            serde_json::from_slice(&rollback_body).expect("response should be json");
        assert_eq!(rollback_json["transaction_id"], transaction_id);
        assert_eq!(rollback_json["state"], "RolledBack");

        let post_rollback_list_response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/transactions")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(post_rollback_list_response.status(), StatusCode::OK);
        let post_rollback_list_body = to_bytes(post_rollback_list_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let post_rollback_list_json: serde_json::Value =
            serde_json::from_slice(&post_rollback_list_body).expect("response should be json");
        assert!(post_rollback_list_json.as_array().unwrap().is_empty());
    }

    #[test]
    fn published_snapshot_is_immutable_after_commit() {
        let state = test_state();
        let mut database = state.database.blocking_write();

        let initial = NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
            .expect("node should build")
            .with_property("value", PropertyValue::Integer(1))
            .expect("property should build");
        database
            .upsert_node(initial)
            .expect("initial node should commit");

        let committed_snapshot = database.snapshot();
        let updated = NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
            .expect("node should build")
            .with_property("value", PropertyValue::Integer(2))
            .expect("property should build");
        database
            .upsert_node(updated)
            .expect("updated node should commit");

        let node_id = NodeId::new("node_a").expect("valid node id");
        let stale_value = committed_snapshot
            .nodes
            .get(&node_id)
            .expect("old snapshot should retain committed node")
            .property("value")
            .and_then(PropertyValue::as_i64);
        assert_eq!(stale_value, Some(1));

        let refreshed_snapshot = database.snapshot();
        let current_value = refreshed_snapshot
            .nodes
            .get(&node_id)
            .expect("new snapshot should reflect latest commit")
            .property("value")
            .and_then(PropertyValue::as_i64);
        assert_eq!(current_value, Some(2));
    }

    #[tokio::test]
    async fn admin_endpoints_manage_storage() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        let storage_root = state.config.storage.root_dir.clone();
        let backup_root = std::env::temp_dir().join(format!(
            "undr9-api-backup-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_nanos()
        ));
        let app = super::build_router(state);

        let create_node = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/nodes")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "id": "node_admin",
                            "node_type": "memory",
                            "properties": {
                                "timestamp": {"kind":"Integer","value":1000}
                            }
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create_node.status(), StatusCode::OK);

        let rebuild_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/rebuild-indexes")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(rebuild_response.status(), StatusCode::OK);

        let integrity_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/integrity")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(integrity_response.status(), StatusCode::OK);

        let compact_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/compact")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(compact_response.status(), StatusCode::OK);

        let backup_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/backup")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "destination": backup_root
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(backup_response.status(), StatusCode::OK);
        assert!(storage_root.exists());
        assert!(backup_root.exists());
        assert!(backup_root
            .join(undr9_storage::BACKUP_MANIFEST_FILE_NAME)
            .exists());

        let create_newer_node = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/nodes")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "id": "node_after_backup",
                            "node_type": "memory",
                            "properties": {
                                "timestamp": {"kind":"Integer","value":2000}
                            }
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(create_newer_node.status(), StatusCode::OK);

        let restore_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/restore")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "source": backup_root
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(restore_response.status(), StatusCode::OK);

        let restored_engine = undr9_storage::StorageEngine::open(&AppConfig {
            storage: undr9_config::StorageConfig {
                root_dir: storage_root.clone(),
                ..AppConfig::default().storage
            },
            ..AppConfig::default()
        })
        .expect("restored storage should open");
        assert!(restored_engine
            .get_node(&NodeId::new("node_admin").expect("valid node id"))
            .is_some());
        assert!(restored_engine
            .get_node(&NodeId::new("node_after_backup").expect("valid node id"))
            .is_none());
    }

    #[tokio::test]
    async fn admin_audit_endpoint_enforces_export_limit_and_retention() {
        let mut config = AppConfig::default();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        config.storage.root_dir =
            std::env::temp_dir().join(format!("undr9-api-audit-test-{nanos}-{unique}"));
        config.observability.audit_log_retention_entries = 2;
        config.observability.audit_log_export_limit = 2;
        let state = super::ApiState::try_new(config).expect("API state should initialize");
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        for _ in 0..3 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/admin/rebuild-indexes")
                        .header("x-api-key", &admin_key)
                        .body(Body::empty())
                        .expect("request should build"),
                )
                .await
                .expect("router should respond");
            assert_eq!(response.status(), StatusCode::OK);
        }

        let export_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/audit?limit=2")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(export_response.status(), StatusCode::OK);
        let export_body = to_bytes(export_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let exported: super::AuditExportResponse =
            serde_json::from_slice(&export_body).expect("response should deserialize");
        assert_eq!(exported.events.len(), 2);
        assert!(exported
            .events
            .iter()
            .all(|event| event.action == "rebuild_indexes"));

        let invalid_response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/audit?limit=3")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(invalid_response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn maintenance_status_endpoint_reports_last_successful_operation() {
        let state = test_state();
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        let rebuild_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/rebuild-indexes")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(rebuild_response.status(), StatusCode::OK);

        let status_response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/maintenance/status")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(status_response.status(), StatusCode::OK);
        let body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let status: super::MaintenanceStatusResponse =
            serde_json::from_slice(&body).expect("response should deserialize");
        assert!(!status.in_progress);
        assert_eq!(status.last_operation.as_deref(), Some("rebuild_indexes"));
        assert_eq!(status.last_outcome.as_deref(), Some("success"));
    }

    #[tokio::test]
    async fn maintenance_budget_rejects_oversized_operation() {
        let mut config = AppConfig::default();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        config.storage.root_dir =
            std::env::temp_dir().join(format!("undr9-api-maint-budget-test-{nanos}-{unique}"));
        config.maintenance.max_node_count = 1;
        config.maintenance.max_edge_count = 10;
        let state = super::ApiState::try_new(config).expect("API state should initialize");
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        for node_id in ["node_a", "node_b"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/nodes")
                        .header("x-api-key", &admin_key)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&serde_json::json!({
                                "id": node_id,
                                "node_type": "memory",
                                "properties": {}
                            }))
                            .expect("json should serialize"),
                        ))
                        .expect("request should build"),
                )
                .await
                .expect("router should respond");
            assert_eq!(response.status(), StatusCode::OK);
        }

        let compact_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/compact")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(compact_response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let status_response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/maintenance/status")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        let body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let status: super::MaintenanceStatusResponse =
            serde_json::from_slice(&body).expect("response should deserialize");
        assert_eq!(status.last_operation.as_deref(), Some("compact"));
        assert_eq!(status.last_outcome.as_deref(), Some("rejected"));
    }

    #[tokio::test]
    async fn replication_endpoints_support_log_shipping_and_acknowledgement() {
        let leader_state = test_state_with_identity("leader-1");
        let leader_key = leader_state.config.auth.admin_api_key.clone();
        let leader_app = super::build_router(leader_state);

        let follower_state = test_state_with_identity("replica-1");
        let follower_key = follower_state.config.auth.admin_api_key.clone();
        let follower_app = super::build_router(follower_state);

        let configure_leader = leader_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replication/leader")
                    .header("x-api-key", &leader_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(configure_leader.status(), StatusCode::OK);

        let register_replica = leader_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/cluster/nodes")
                    .header("x-api-key", &leader_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "node_id": "replica-1",
                            "address": "127.0.0.1:9101"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(register_replica.status(), StatusCode::OK);

        let configure_follower = follower_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replication/follower")
                    .header("x-api-key", &follower_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "leader_node_id": "leader-1",
                            "leader_address": "127.0.0.1:9100"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(configure_follower.status(), StatusCode::OK);

        let configure_follower_again = follower_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replication/follower")
                    .header("x-api-key", &follower_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "leader_node_id": "leader-1",
                            "leader_address": "127.0.0.1:9100"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(configure_follower_again.status(), StatusCode::OK);

        let follower_write = follower_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/nodes")
                    .header("x-api-key", &follower_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "id": "replica_local_write",
                            "node_type": "memory",
                            "properties": {}
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(follower_write.status(), StatusCode::CONFLICT);

        let leader_write = leader_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/nodes")
                    .header("x-api-key", &leader_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "id": "replicated_node",
                            "node_type": "memory",
                            "properties": {
                                "unique_key": {"kind":"String","value":"replicated"}
                            }
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(leader_write.status(), StatusCode::OK);

        let history_response = leader_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/replication/history?after_source_lsn=0")
                    .header("x-api-key", &leader_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(history_response.status(), StatusCode::OK);
        let history_body = to_bytes(history_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let history_records: Vec<serde_json::Value> =
            serde_json::from_slice(&history_body).expect("response should be json");
        assert_eq!(history_records.len(), 1);
        assert_eq!(history_records[0]["source_lsn"], 1);

        let apply_response = follower_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replication/apply")
                    .header("x-api-key", &follower_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "records": history_records
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(apply_response.status(), StatusCode::OK);

        let follower_read = follower_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes/replicated_node")
                    .header("x-api-key", &follower_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(follower_read.status(), StatusCode::OK);

        let leader_status_before_ack = leader_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/replication/status")
                    .header("x-api-key", &leader_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        let status_before_ack_body = to_bytes(leader_status_before_ack.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let status_before_ack: serde_json::Value =
            serde_json::from_slice(&status_before_ack_body).expect("response should be json");
        assert_eq!(status_before_ack["replica_lag"]["replica-1"], 1);

        let ack_response = leader_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replication/ack")
                    .header("x-api-key", &leader_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "replica_node_id": "replica-1",
                            "source_lsn": 1
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(ack_response.status(), StatusCode::OK);
        let ack_body = to_bytes(ack_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let ack_json: serde_json::Value =
            serde_json::from_slice(&ack_body).expect("response should be json");
        assert_eq!(ack_json["replica_lag"]["replica-1"], 0);
    }

    #[tokio::test]
    async fn replication_apply_rejects_invalid_batch_before_persisting_any_writes() {
        let leader_state = test_state_with_identity("leader-1");
        let leader_key = leader_state.config.auth.admin_api_key.clone();
        let leader_app = super::build_router(leader_state);

        let follower_state = test_state_with_identity("replica-1");
        let follower_key = follower_state.config.auth.admin_api_key.clone();
        let follower_app = super::build_router(follower_state);

        let configure_leader = leader_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replication/leader")
                    .header("x-api-key", &leader_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(configure_leader.status(), StatusCode::OK);

        let configure_follower = follower_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replication/follower")
                    .header("x-api-key", &follower_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "leader_node_id": "leader-1",
                            "leader_address": "127.0.0.1:9100"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(configure_follower.status(), StatusCode::OK);

        let valid_record = ReplicationRecord {
            source_node_id: "leader-1".to_owned(),
            source_term: 2,
            source_lsn: 1,
            batch: WriteBatch {
                nodes_upserted: vec![NodeRecord::new(
                    NodeId::new("partial_node").expect("node id should build"),
                    "memory",
                )
                .expect("node should build")],
                ..WriteBatch::default()
            },
        };
        let invalid_record = ReplicationRecord {
            source_node_id: "leader-1".to_owned(),
            source_term: 1,
            source_lsn: 2,
            batch: WriteBatch {
                nodes_upserted: vec![NodeRecord::new(
                    NodeId::new("unexpected_node").expect("node id should build"),
                    "memory",
                )
                .expect("node should build")],
                ..WriteBatch::default()
            },
        };

        let apply_response = follower_app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replication/apply")
                    .header("x-api-key", &follower_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "records": [valid_record, invalid_record]
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(apply_response.status(), StatusCode::CONFLICT);

        let node_read = follower_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes/partial_node")
                    .header("x-api-key", &follower_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(node_read.status(), StatusCode::NOT_FOUND);

        let status_response = follower_app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/replication/status")
                    .header("x-api-key", &follower_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let status_json: serde_json::Value =
            serde_json::from_slice(&status_body).expect("response should be json");
        assert_eq!(status_json["status"]["last_applied_source_lsn"], 0);
    }

    #[tokio::test]
    async fn cluster_promotion_updates_local_replication_mode() {
        let state = test_state_with_identity("leader-1");
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        let configure_leader = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replication/leader")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(configure_leader.status(), StatusCode::OK);

        let register_replica = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/cluster/nodes")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "node_id": "replica-2",
                            "address": "127.0.0.1:9202"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(register_replica.status(), StatusCode::OK);

        let promote_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/cluster/promote")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "node_id": "replica-2"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(promote_response.status(), StatusCode::OK);

        let topology_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/cluster/topology")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(topology_response.status(), StatusCode::OK);
        let topology_body = to_bytes(topology_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let topology_json: serde_json::Value =
            serde_json::from_slice(&topology_body).expect("response should be json");
        assert_eq!(topology_json["leader_node_id"], "replica-2");

        let status_response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/replication/status")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .expect("body should be readable");
        let status_json: serde_json::Value =
            serde_json::from_slice(&status_body).expect("response should be json");
        assert_eq!(status_json["status"]["mode"], "Follower");
        assert_eq!(status_json["status"]["leader_node_id"], "replica-2");
    }

    #[tokio::test]
    async fn repair_and_cluster_health_endpoints_report_updated_state() {
        let state = test_state_with_identity("leader-health");
        let admin_key = state.config.auth.admin_api_key.clone();
        let app = super::build_router(state);

        let configure_leader = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replication/leader")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(configure_leader.status(), StatusCode::OK);

        let register_replica = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/cluster/nodes")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "node_id": "replica-health",
                            "address": "127.0.0.1:9301"
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(register_replica.status(), StatusCode::OK);

        let mark_unhealthy_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/cluster/nodes/replica-health/health")
                    .header("x-api-key", &admin_key)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "healthy": false
                        }))
                        .expect("json should serialize"),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(mark_unhealthy_response.status(), StatusCode::OK);
        let mark_unhealthy_body = to_bytes(mark_unhealthy_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let mark_unhealthy_json: serde_json::Value =
            serde_json::from_slice(&mark_unhealthy_body).expect("response should be json");
        let unhealthy_replica = mark_unhealthy_json["nodes"]
            .as_array()
            .expect("nodes should be array")
            .iter()
            .find(|node| node["node_id"] == "replica-health")
            .expect("replica should exist");
        assert_eq!(unhealthy_replica["healthy"], false);

        let repair_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/repair")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(repair_response.status(), StatusCode::OK);
        let repair_body = to_bytes(repair_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let repair_json: serde_json::Value =
            serde_json::from_slice(&repair_body).expect("response should be json");
        assert_eq!(repair_json["manifest_present"], true);
        assert_eq!(repair_json["issues"].as_array().unwrap().len(), 0);

        let status_response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/maintenance/status")
                    .header("x-api-key", &admin_key)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(status_response.status(), StatusCode::OK);
        let status_body = to_bytes(status_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let status_json: serde_json::Value =
            serde_json::from_slice(&status_body).expect("response should be json");
        assert_eq!(status_json["last_operation"], "repair");
        assert_eq!(status_json["last_outcome"], "success");
    }
}
