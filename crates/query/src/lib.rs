use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::time::Instant;

use im::OrdMap;
use serde::{Deserialize, Serialize};
use undr9_common::{EdgeId, NodeId, Result, Undr9Error};
use undr9_core::{EdgeRecord, NodeRecord, PropertyValue};
use undr9_index::{EdgeDirection, GraphIndex};
use undr9_memory::{MemoryRanker, RetrievalProfile, ScoreBreakdown};

const DEFAULT_MAX_DEPTH: u8 = 5;
const DEFAULT_RESULT_LIMIT: usize = 100;
const DEFAULT_QUERY_TIMEOUT_MS: u64 = 5_000;
const MAX_RESULT_LIMIT: usize = 1_000;
const MAX_QUERY_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryRequest {
    GetNodeById {
        node_id: NodeId,
    },
    GetNodeByUniqueKey {
        unique_key: String,
    },
    ListNeighbors {
        node_id: NodeId,
        edge_type: Option<String>,
        direction: EdgeDirection,
        limit: Option<usize>,
    },
    Traverse {
        start_node_id: NodeId,
        edge_type: Option<String>,
        direction: EdgeDirection,
        max_hops: Option<u8>,
        limit: Option<usize>,
        timeout_ms: Option<u64>,
        constraints: Option<TraversalConstraints>,
    },
    ShortestPath {
        source_node_id: NodeId,
        target_node_id: NodeId,
        direction: EdgeDirection,
        max_depth: Option<u8>,
        limit: Option<usize>,
        timeout_ms: Option<u64>,
        constraints: Option<TraversalConstraints>,
    },
    SearchByLabel {
        label: String,
        limit: Option<usize>,
    },
    TimeRange {
        field: String,
        from_epoch_ms: i64,
        to_epoch_ms: i64,
        limit: usize,
    },
    VectorSearch {
        query_vector: Vec<f32>,
        node_type: Option<String>,
        limit: usize,
        #[serde(default)]
        top_k: Option<usize>,
    },
    RankedRetrieval {
        query_vector: Option<Vec<f32>>,
        reference_node_id: Option<NodeId>,
        edge_type: Option<String>,
        from_epoch_ms: Option<i64>,
        to_epoch_ms: Option<i64>,
        limit: usize,
        #[serde(default)]
        top_k: Option<usize>,
        now_epoch_ms: i64,
        retrieval_profile: Option<String>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraversalConstraints {
    #[serde(default)]
    pub edge_types: Vec<String>,
    #[serde(default)]
    pub node_labels: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanKind {
    ExactLookup,
    Traversal,
    ShortestPath,
    NeighborLookup,
    LabelScan,
    TemporalRange,
    VectorSimilarity,
    RankedHybrid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryPlan {
    pub kind: PlanKind,
    pub filters: Vec<QueryFilter>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryFilter {
    pub field: String,
    pub value: PropertyValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphSnapshot {
    pub nodes: OrdMap<NodeId, NodeRecord>,
    pub edges: OrdMap<EdgeId, EdgeRecord>,
    pub indexes: GraphIndex,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphMutation {
    pub removed_node_ids: BTreeSet<NodeId>,
    pub removed_edge_ids: BTreeSet<EdgeId>,
    pub added_nodes: BTreeMap<NodeId, NodeRecord>,
    pub added_edges: BTreeMap<EdgeId, EdgeRecord>,
}

#[derive(Debug, Clone, Copy)]
pub struct SemanticCandidateQuery<'a> {
    query_vector: &'a [f32],
    node_type: Option<&'a str>,
    limit: usize,
    top_k_override: Option<usize>,
}

pub trait GraphView {
    fn get_node(&self, node_id: &NodeId) -> Option<NodeRecord>;
    fn get_edge(&self, edge_id: &EdgeId) -> Option<EdgeRecord>;
    fn contains_node(&self, node_id: &NodeId) -> bool;
    fn lookup_unique_key(&self, unique_key: &str) -> Option<NodeId>;
    fn node_ids_by_type<'a>(&'a self, node_type: &'a str) -> Box<dyn Iterator<Item = NodeId> + 'a>;
    fn edge_ids_for_iter<'a>(
        &'a self,
        node_id: &'a NodeId,
        direction: EdgeDirection,
        edge_type: Option<&'a str>,
    ) -> Box<dyn Iterator<Item = EdgeId> + 'a>;
    fn node_ids_in_time_range_iter<'a>(
        &'a self,
        from_epoch_ms: i64,
        to_epoch_ms: i64,
    ) -> Box<dyn Iterator<Item = NodeId> + 'a>;
    fn vector_candidate_ids_iter<'a>(&'a self) -> Box<dyn Iterator<Item = NodeId> + 'a>;
    fn semantic_candidate_ids_iter<'a>(
        &'a self,
        query: SemanticCandidateQuery<'a>,
    ) -> Box<dyn Iterator<Item = NodeId> + 'a>;
    fn all_nodes_iter<'a>(&'a self) -> Box<dyn Iterator<Item = NodeRecord> + 'a>;
}

pub struct OverlayGraphView<'a> {
    base: &'a GraphSnapshot,
    mutation: &'a GraphMutation,
}

impl<'a> OverlayGraphView<'a> {
    pub fn new(base: &'a GraphSnapshot, mutation: &'a GraphMutation) -> Self {
        Self { base, mutation }
    }
}

impl GraphView for GraphSnapshot {
    fn get_node(&self, node_id: &NodeId) -> Option<NodeRecord> {
        self.nodes.get(node_id).cloned()
    }

    fn get_edge(&self, edge_id: &EdgeId) -> Option<EdgeRecord> {
        self.edges.get(edge_id).cloned()
    }

    fn contains_node(&self, node_id: &NodeId) -> bool {
        self.indexes.contains_node(node_id)
    }

    fn lookup_unique_key(&self, unique_key: &str) -> Option<NodeId> {
        self.indexes.lookup_unique_key(unique_key).cloned()
    }

    fn node_ids_by_type<'a>(&'a self, node_type: &'a str) -> Box<dyn Iterator<Item = NodeId> + 'a> {
        Box::new(self.indexes.node_ids_by_type(node_type).iter().cloned())
    }

    fn edge_ids_for_iter<'a>(
        &'a self,
        node_id: &'a NodeId,
        direction: EdgeDirection,
        edge_type: Option<&'a str>,
    ) -> Box<dyn Iterator<Item = EdgeId> + 'a> {
        Box::new(
            self.indexes
                .edge_ids_for_iter(node_id, direction, &self.edges, edge_type)
                .cloned(),
        )
    }

    fn node_ids_in_time_range_iter<'a>(
        &'a self,
        from_epoch_ms: i64,
        to_epoch_ms: i64,
    ) -> Box<dyn Iterator<Item = NodeId> + 'a> {
        Box::new(
            self.indexes
                .node_ids_in_time_range_iter(from_epoch_ms, to_epoch_ms)
                .cloned(),
        )
    }

    fn vector_candidate_ids_iter<'a>(&'a self) -> Box<dyn Iterator<Item = NodeId> + 'a> {
        Box::new(self.indexes.vector_candidate_ids_iter().cloned())
    }

    fn semantic_candidate_ids_iter<'a>(
        &'a self,
        query: SemanticCandidateQuery<'a>,
    ) -> Box<dyn Iterator<Item = NodeId> + 'a> {
        Box::new(
            self.indexes
                .semantic_candidate_ids(
                    query.query_vector,
                    query.node_type,
                    query.limit,
                    query.top_k_override,
                )
                .into_iter(),
        )
    }

    fn all_nodes_iter<'a>(&'a self) -> Box<dyn Iterator<Item = NodeRecord> + 'a> {
        Box::new(self.nodes.values().cloned())
    }
}

impl GraphView for OverlayGraphView<'_> {
    fn get_node(&self, node_id: &NodeId) -> Option<NodeRecord> {
        if let Some(node) = self.mutation.added_nodes.get(node_id) {
            return Some(node.clone());
        }
        if self.mutation.removed_node_ids.contains(node_id) {
            return None;
        }
        self.base.get_node(node_id)
    }

    fn get_edge(&self, edge_id: &EdgeId) -> Option<EdgeRecord> {
        if let Some(edge) = self.mutation.added_edges.get(edge_id) {
            return Some(edge.clone());
        }
        if self.mutation.removed_edge_ids.contains(edge_id) {
            return None;
        }
        self.base.get_edge(edge_id)
    }

    fn contains_node(&self, node_id: &NodeId) -> bool {
        self.get_node(node_id).is_some()
    }

    fn lookup_unique_key(&self, unique_key: &str) -> Option<NodeId> {
        if let Some(node_id) = self
            .mutation
            .added_nodes
            .values()
            .find(|node| node_unique_key(node) == Some(unique_key))
            .map(|node| node.id.clone())
        {
            return Some(node_id);
        }
        self.base
            .lookup_unique_key(unique_key)
            .filter(|node_id| self.get_node(node_id).is_some())
            .filter(|node_id| {
                self.get_node(node_id).as_ref().and_then(node_unique_key) == Some(unique_key)
            })
    }

    fn node_ids_by_type<'a>(&'a self, node_type: &'a str) -> Box<dyn Iterator<Item = NodeId> + 'a> {
        let mut node_ids = self
            .base
            .node_ids_by_type(node_type)
            .filter(|node_id| {
                self.get_node(node_id)
                    .map(|node| node.node_type == node_type)
                    .unwrap_or(false)
            })
            .collect::<BTreeSet<_>>();
        node_ids.extend(
            self.mutation
                .added_nodes
                .values()
                .filter(|node| node.node_type == node_type)
                .map(|node| node.id.clone()),
        );
        Box::new(node_ids.into_iter())
    }

    fn edge_ids_for_iter<'a>(
        &'a self,
        node_id: &'a NodeId,
        direction: EdgeDirection,
        edge_type: Option<&'a str>,
    ) -> Box<dyn Iterator<Item = EdgeId> + 'a> {
        let mut edge_ids = self
            .base
            .edge_ids_for_iter(node_id, direction, edge_type)
            .filter(|edge_id| {
                self.get_edge(edge_id)
                    .as_ref()
                    .map(|edge| edge_matches(edge, node_id, direction, edge_type))
                    .unwrap_or(false)
            })
            .collect::<BTreeSet<_>>();
        edge_ids.extend(
            self.mutation
                .added_edges
                .values()
                .filter(|edge| edge_matches(edge, node_id, direction, edge_type))
                .map(|edge| edge.id.clone()),
        );
        Box::new(edge_ids.into_iter())
    }

    fn node_ids_in_time_range_iter<'a>(
        &'a self,
        from_epoch_ms: i64,
        to_epoch_ms: i64,
    ) -> Box<dyn Iterator<Item = NodeId> + 'a> {
        let mut node_ids = self
            .base
            .node_ids_in_time_range_iter(from_epoch_ms, to_epoch_ms)
            .filter(|node_id| {
                self.get_node(node_id)
                    .as_ref()
                    .map(|node| within_required_time_range(node, from_epoch_ms, to_epoch_ms))
                    .unwrap_or(false)
            })
            .collect::<BTreeSet<_>>();
        node_ids.extend(
            self.mutation
                .added_nodes
                .values()
                .filter(|node| within_required_time_range(node, from_epoch_ms, to_epoch_ms))
                .map(|node| node.id.clone()),
        );
        Box::new(node_ids.into_iter())
    }

    fn vector_candidate_ids_iter<'a>(&'a self) -> Box<dyn Iterator<Item = NodeId> + 'a> {
        let mut node_ids = self
            .base
            .vector_candidate_ids_iter()
            .filter(|node_id| {
                self.get_node(node_id)
                    .as_ref()
                    .and_then(NodeRecord::embedding)
                    .is_some()
            })
            .collect::<BTreeSet<_>>();
        node_ids.extend(
            self.mutation
                .added_nodes
                .values()
                .filter(|node| node.embedding().is_some())
                .map(|node| node.id.clone()),
        );
        Box::new(node_ids.into_iter())
    }

    fn semantic_candidate_ids_iter<'a>(
        &'a self,
        query: SemanticCandidateQuery<'a>,
    ) -> Box<dyn Iterator<Item = NodeId> + 'a> {
        let _ = query.query_vector;
        let _ = query.limit;
        let _ = query.top_k_override;
        match query.node_type {
            Some(node_type) => Box::new(self.node_ids_by_type(node_type).filter(move |node_id| {
                self.get_node(node_id)
                    .as_ref()
                    .and_then(NodeRecord::embedding)
                    .is_some()
            })),
            None => self.vector_candidate_ids_iter(),
        }
    }

    fn all_nodes_iter<'a>(&'a self) -> Box<dyn Iterator<Item = NodeRecord> + 'a> {
        let mut node_ids = self
            .base
            .nodes
            .keys()
            .filter(|node_id| !self.mutation.removed_node_ids.contains(*node_id))
            .cloned()
            .collect::<BTreeSet<_>>();
        node_ids.extend(self.mutation.added_nodes.keys().cloned());
        Box::new(
            node_ids
                .into_iter()
                .filter_map(|node_id| self.get_node(&node_id)),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RankedNodeResult {
    pub node: NodeRecord,
    pub score: f32,
    pub breakdown: ScoreBreakdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphPath {
    pub node_ids: Vec<NodeId>,
    pub edge_ids: Vec<EdgeId>,
    pub hop_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResponse {
    pub plan_kind: PlanKind,
    pub nodes: Vec<NodeRecord>,
    pub edges: Vec<EdgeRecord>,
    pub ranked_results: Vec<RankedNodeResult>,
    pub path: Option<GraphPath>,
    pub retrieval_profile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryExecutionItem {
    Node(NodeRecord),
    Edge(EdgeRecord),
    RankedResult(RankedNodeResult),
    Path(GraphPath),
}

pub struct QueryExecution<'a> {
    pub plan_kind: PlanKind,
    pub retrieval_profile: Option<String>,
    items: Box<dyn Iterator<Item = QueryExecutionItem> + 'a>,
}

pub struct Planner;
pub struct Executor;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraversalRuntime {
    max_depth: u8,
    limit: usize,
    timeout_ms: u64,
    constraints: TraversalConstraints,
}

#[derive(Debug, Clone)]
struct PathParent {
    previous: Option<NodeId>,
    via_edge: Option<EdgeId>,
}

impl Planner {
    pub fn plan(request: &QueryRequest) -> Result<QueryPlan> {
        let plan = match request {
            QueryRequest::GetNodeById { .. } => QueryPlan {
                kind: PlanKind::ExactLookup,
                filters: Vec::new(),
            },
            QueryRequest::GetNodeByUniqueKey { unique_key } => QueryPlan {
                kind: PlanKind::ExactLookup,
                filters: vec![QueryFilter {
                    field: "unique_key".to_owned(),
                    value: PropertyValue::String(unique_key.clone()),
                }],
            },
            QueryRequest::ListNeighbors { limit, .. } => QueryPlan {
                kind: PlanKind::NeighborLookup,
                filters: vec![QueryFilter {
                    field: "limit".to_owned(),
                    value: PropertyValue::Integer(
                        i64::try_from(resolve_result_limit(*limit)?).unwrap_or(i64::MAX),
                    ),
                }],
            },
            QueryRequest::Traverse {
                max_hops,
                limit,
                timeout_ms,
                ..
            } => {
                let runtime = resolve_traversal_runtime(*max_hops, *limit, *timeout_ms, None)?;
                QueryPlan {
                    kind: PlanKind::Traversal,
                    filters: vec![
                        QueryFilter {
                            field: "max_hops".to_owned(),
                            value: PropertyValue::Integer(i64::from(runtime.max_depth)),
                        },
                        QueryFilter {
                            field: "limit".to_owned(),
                            value: PropertyValue::Integer(runtime.limit as i64),
                        },
                        QueryFilter {
                            field: "timeout_ms".to_owned(),
                            value: PropertyValue::Integer(runtime.timeout_ms as i64),
                        },
                    ],
                }
            }
            QueryRequest::ShortestPath {
                max_depth,
                limit,
                timeout_ms,
                ..
            } => {
                let runtime = resolve_traversal_runtime(*max_depth, *limit, *timeout_ms, None)?;
                QueryPlan {
                    kind: PlanKind::ShortestPath,
                    filters: vec![
                        QueryFilter {
                            field: "max_depth".to_owned(),
                            value: PropertyValue::Integer(i64::from(runtime.max_depth)),
                        },
                        QueryFilter {
                            field: "limit".to_owned(),
                            value: PropertyValue::Integer(runtime.limit as i64),
                        },
                        QueryFilter {
                            field: "timeout_ms".to_owned(),
                            value: PropertyValue::Integer(runtime.timeout_ms as i64),
                        },
                    ],
                }
            }
            QueryRequest::SearchByLabel { label, limit } => QueryPlan {
                kind: PlanKind::LabelScan,
                filters: vec![
                    QueryFilter {
                        field: "label".to_owned(),
                        value: PropertyValue::String(label.clone()),
                    },
                    QueryFilter {
                        field: "limit".to_owned(),
                        value: PropertyValue::Integer(
                            i64::try_from(resolve_result_limit(*limit)?).unwrap_or(i64::MAX),
                        ),
                    },
                ],
            },
            QueryRequest::TimeRange {
                field,
                from_epoch_ms,
                to_epoch_ms,
                limit,
            } => {
                if from_epoch_ms > to_epoch_ms {
                    return Err(Undr9Error::Validation(
                        "time range start must be before end".to_owned(),
                    ));
                }
                let limit = resolve_required_result_limit(*limit)?;
                QueryPlan {
                    kind: PlanKind::TemporalRange,
                    filters: vec![
                        QueryFilter {
                            field: "field".to_owned(),
                            value: PropertyValue::String(field.clone()),
                        },
                        QueryFilter {
                            field: "from_epoch_ms".to_owned(),
                            value: PropertyValue::Integer(*from_epoch_ms),
                        },
                        QueryFilter {
                            field: "to_epoch_ms".to_owned(),
                            value: PropertyValue::Integer(*to_epoch_ms),
                        },
                        QueryFilter {
                            field: "limit".to_owned(),
                            value: PropertyValue::Integer(i64::try_from(limit).unwrap_or(i64::MAX)),
                        },
                    ],
                }
            }
            QueryRequest::VectorSearch {
                query_vector,
                node_type,
                limit,
                top_k,
            } => {
                validate_query_vector(query_vector)?;
                let limit = resolve_required_result_limit(*limit)?;
                let top_k = top_k.map(resolve_required_result_limit).transpose()?;
                let mut filters = vec![QueryFilter {
                    field: "vector_dimensions".to_owned(),
                    value: PropertyValue::Integer(query_vector.len() as i64),
                }];
                if let Some(node_type) = node_type {
                    filters.push(QueryFilter {
                        field: "node_type".to_owned(),
                        value: PropertyValue::String(node_type.clone()),
                    });
                }
                QueryPlan {
                    kind: PlanKind::VectorSimilarity,
                    filters: {
                        filters.push(QueryFilter {
                            field: "limit".to_owned(),
                            value: PropertyValue::Integer(i64::try_from(limit).unwrap_or(i64::MAX)),
                        });
                        if let Some(top_k) = top_k {
                            filters.push(QueryFilter {
                                field: "top_k".to_owned(),
                                value: PropertyValue::Integer(
                                    i64::try_from(top_k).unwrap_or(i64::MAX),
                                ),
                            });
                        }
                        filters
                    },
                }
            }
            QueryRequest::RankedRetrieval {
                query_vector,
                reference_node_id,
                from_epoch_ms,
                to_epoch_ms,
                limit,
                top_k,
                ..
            } => {
                if let Some(query_vector) = query_vector {
                    validate_query_vector(query_vector)?;
                }
                if matches!((from_epoch_ms, to_epoch_ms), (Some(from), Some(to)) if from > to) {
                    return Err(Undr9Error::Validation(
                        "ranked retrieval time range start must be before end".to_owned(),
                    ));
                }
                if query_vector.is_none() && reference_node_id.is_none() {
                    return Err(Undr9Error::Validation(
                        "ranked retrieval requires at least a query_vector or reference_node_id"
                            .to_owned(),
                    ));
                }
                let limit = resolve_required_result_limit(*limit)?;
                let top_k = top_k.map(resolve_required_result_limit).transpose()?;
                let mut filters = vec![QueryFilter {
                    field: "limit".to_owned(),
                    value: PropertyValue::Integer(i64::try_from(limit).unwrap_or(i64::MAX)),
                }];
                if let Some(top_k) = top_k {
                    filters.push(QueryFilter {
                        field: "top_k".to_owned(),
                        value: PropertyValue::Integer(i64::try_from(top_k).unwrap_or(i64::MAX)),
                    });
                }
                QueryPlan {
                    kind: PlanKind::RankedHybrid,
                    filters,
                }
            }
        };

        Ok(plan)
    }
}

impl Executor {
    pub fn execute(request: &QueryRequest, snapshot: &dyn GraphView) -> Result<QueryResponse> {
        Ok(Self::execute_iter(request, snapshot)?.collect_response())
    }

    pub fn execute_iter<'a>(
        request: &'a QueryRequest,
        snapshot: &'a dyn GraphView,
    ) -> Result<QueryExecution<'a>> {
        let plan = Planner::plan(request)?;

        let execution = match request {
            QueryRequest::GetNodeById { node_id } => QueryExecution::new(
                plan.kind,
                None,
                Box::new(
                    snapshot
                        .get_node(node_id)
                        .into_iter()
                        .map(QueryExecutionItem::Node),
                ),
            ),
            QueryRequest::GetNodeByUniqueKey { unique_key } => QueryExecution::new(
                plan.kind,
                None,
                Box::new(
                    snapshot
                        .lookup_unique_key(unique_key)
                        .and_then(|node_id| snapshot.get_node(&node_id))
                        .into_iter()
                        .map(QueryExecutionItem::Node),
                ),
            ),
            QueryRequest::ListNeighbors {
                node_id,
                edge_type,
                direction,
                limit,
            } => {
                let limit = resolve_result_limit(*limit)?;
                let (nodes, edges) = collect_neighbor_results(
                    snapshot,
                    node_id,
                    *direction,
                    edge_type.as_deref(),
                    limit,
                );
                QueryExecution::from_response(QueryResponse {
                    plan_kind: plan.kind,
                    nodes,
                    edges,
                    ranked_results: Vec::new(),
                    path: None,
                    retrieval_profile: None,
                })
            }
            QueryRequest::Traverse {
                start_node_id,
                edge_type,
                direction,
                max_hops,
                limit,
                timeout_ms,
                constraints,
            } => {
                let runtime =
                    resolve_traversal_runtime(*max_hops, *limit, *timeout_ms, constraints.clone())?
                        .merge_legacy_edge_type(edge_type.clone());
                QueryExecution::new(
                    plan.kind,
                    None,
                    Box::new(TraversalExecutionIter::new(
                        snapshot,
                        start_node_id.clone(),
                        *direction,
                        runtime,
                    )),
                )
            }
            QueryRequest::ShortestPath {
                source_node_id,
                target_node_id,
                direction,
                max_depth,
                limit,
                timeout_ms,
                constraints,
            } => {
                let runtime = resolve_traversal_runtime(
                    *max_depth,
                    *limit,
                    *timeout_ms,
                    constraints.clone(),
                )?;
                let path = shortest_path(
                    snapshot,
                    source_node_id,
                    target_node_id,
                    *direction,
                    &runtime,
                );
                let (nodes, edges) = path
                    .as_ref()
                    .map(|path| {
                        let nodes = path
                            .node_ids
                            .iter()
                            .filter_map(|node_id| snapshot.get_node(node_id))
                            .collect();
                        let edges = path
                            .edge_ids
                            .iter()
                            .filter_map(|edge_id| snapshot.get_edge(edge_id))
                            .collect();
                        (nodes, edges)
                    })
                    .unwrap_or_else(|| (Vec::new(), Vec::new()));
                QueryExecution::from_response(QueryResponse {
                    plan_kind: plan.kind,
                    nodes,
                    edges,
                    ranked_results: Vec::new(),
                    path,
                    retrieval_profile: None,
                })
            }
            QueryRequest::SearchByLabel { label, limit } => QueryExecution::new(
                plan.kind,
                None,
                Box::new(LabelScanExecutionIter::new(
                    snapshot,
                    label.as_str(),
                    resolve_result_limit(*limit)?,
                )),
            ),
            QueryRequest::TimeRange {
                field,
                from_epoch_ms,
                to_epoch_ms,
                limit,
            } => QueryExecution::new(
                plan.kind,
                None,
                time_range_execution_iter(
                    snapshot,
                    field.as_str(),
                    *from_epoch_ms,
                    *to_epoch_ms,
                    *limit,
                ),
            ),
            QueryRequest::VectorSearch {
                query_vector,
                node_type,
                limit,
                top_k,
            } => {
                let ranked_results =
                    vector_search(snapshot, query_vector, node_type.as_deref(), *limit, *top_k);
                QueryExecution::from_response(QueryResponse {
                    plan_kind: plan.kind,
                    nodes: ranked_results
                        .iter()
                        .map(|result| result.node.clone())
                        .collect(),
                    edges: Vec::new(),
                    ranked_results,
                    path: None,
                    retrieval_profile: None,
                })
            }
            QueryRequest::RankedRetrieval {
                query_vector,
                reference_node_id,
                edge_type,
                from_epoch_ms,
                to_epoch_ms,
                limit,
                top_k,
                now_epoch_ms,
                retrieval_profile,
            } => {
                let profile = resolve_retrieval_profile(retrieval_profile.as_deref())?;
                let ranked_results = ranked_retrieval(
                    snapshot,
                    RankedRetrievalParams {
                        query_vector: query_vector.as_deref(),
                        reference_node_id: reference_node_id.as_ref(),
                        edge_type: edge_type.as_deref(),
                        from_epoch_ms: *from_epoch_ms,
                        to_epoch_ms: *to_epoch_ms,
                        limit: *limit,
                        top_k_override: *top_k,
                        now_epoch_ms: *now_epoch_ms,
                        profile: &profile,
                    },
                );
                let nodes = ranked_results
                    .iter()
                    .map(|result| result.node.clone())
                    .collect();
                QueryExecution::from_response(QueryResponse {
                    plan_kind: plan.kind,
                    nodes,
                    edges: Vec::new(),
                    ranked_results,
                    path: None,
                    retrieval_profile: Some(profile.name),
                })
            }
        };

        Ok(execution)
    }
}

impl<'a> QueryExecution<'a> {
    fn new(
        plan_kind: PlanKind,
        retrieval_profile: Option<String>,
        items: Box<dyn Iterator<Item = QueryExecutionItem> + 'a>,
    ) -> Self {
        Self {
            plan_kind,
            retrieval_profile,
            items,
        }
    }

    fn from_response(response: QueryResponse) -> Self {
        let plan_kind = response.plan_kind;
        let retrieval_profile = response.retrieval_profile.clone();
        let mut items = Vec::new();
        if response.ranked_results.is_empty() {
            items.extend(response.nodes.into_iter().map(QueryExecutionItem::Node));
        } else {
            items.extend(
                response
                    .ranked_results
                    .into_iter()
                    .map(QueryExecutionItem::RankedResult),
            );
        }
        items.extend(response.edges.into_iter().map(QueryExecutionItem::Edge));
        if let Some(path) = response.path {
            items.push(QueryExecutionItem::Path(path));
        }
        Self::new(plan_kind, retrieval_profile, Box::new(items.into_iter()))
    }

    pub fn into_items(self) -> Box<dyn Iterator<Item = QueryExecutionItem> + 'a> {
        self.items
    }

    pub fn collect_response(self) -> QueryResponse {
        let QueryExecution {
            plan_kind,
            retrieval_profile,
            items,
        } = self;
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut ranked_results = Vec::new();
        let mut path = None;

        for item in items {
            match item {
                QueryExecutionItem::Node(node) => nodes.push(node),
                QueryExecutionItem::Edge(edge) => edges.push(edge),
                QueryExecutionItem::RankedResult(result) => {
                    nodes.push(result.node.clone());
                    ranked_results.push(result);
                }
                QueryExecutionItem::Path(graph_path) => {
                    path = Some(graph_path);
                }
            }
        }

        QueryResponse {
            plan_kind,
            nodes,
            edges,
            ranked_results,
            path,
            retrieval_profile,
        }
    }
}

fn validate_query_vector(query_vector: &[f32]) -> Result<()> {
    if query_vector.is_empty() {
        return Err(Undr9Error::Validation(
            "query_vector must not be empty".to_owned(),
        ));
    }

    Ok(())
}

fn collect_neighbor_results(
    snapshot: &dyn GraphView,
    node_id: &NodeId,
    direction: EdgeDirection,
    edge_type: Option<&str>,
    limit: usize,
) -> (Vec<NodeRecord>, Vec<EdgeRecord>) {
    let edge_ids = snapshot.edge_ids_for_iter(node_id, direction, edge_type);
    let mut seen_nodes = BTreeSet::new();
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for edge_id in edge_ids.take(limit) {
        if let Some(edge) = snapshot.get_edge(&edge_id) {
            let neighbor_id = match direction {
                EdgeDirection::Outgoing => edge.target.clone(),
                EdgeDirection::Incoming => edge.source.clone(),
                EdgeDirection::Both => {
                    if edge.source == *node_id {
                        edge.target.clone()
                    } else {
                        edge.source.clone()
                    }
                }
            };

            if seen_nodes.insert(neighbor_id.clone()) {
                if let Some(node) = snapshot.get_node(&neighbor_id) {
                    nodes.push(node);
                }
            }
            edges.push(edge);
        }
    }

    (nodes, edges)
}

struct LabelScanExecutionIter<'a> {
    snapshot: &'a dyn GraphView,
    node_ids: Vec<NodeId>,
    offset: usize,
    yielded: usize,
    limit: usize,
}

impl<'a> LabelScanExecutionIter<'a> {
    fn new(snapshot: &'a dyn GraphView, label: &'a str, limit: usize) -> Self {
        Self {
            snapshot,
            node_ids: snapshot.node_ids_by_type(label).collect(),
            offset: 0,
            yielded: 0,
            limit,
        }
    }
}

impl Iterator for LabelScanExecutionIter<'_> {
    type Item = QueryExecutionItem;

    fn next(&mut self) -> Option<Self::Item> {
        while self.yielded < self.limit && self.offset < self.node_ids.len() {
            let node_id = &self.node_ids[self.offset];
            self.offset += 1;
            if let Some(node) = self.snapshot.get_node(node_id) {
                self.yielded += 1;
                return Some(QueryExecutionItem::Node(node));
            }
        }
        None
    }
}

struct TraversalExecutionIter<'a> {
    snapshot: &'a dyn GraphView,
    direction: EdgeDirection,
    runtime: TraversalRuntime,
    started: Instant,
    visited_nodes: BTreeSet<NodeId>,
    visited_edges: BTreeSet<EdgeId>,
    queue: VecDeque<(NodeId, u8)>,
    pending: VecDeque<QueryExecutionItem>,
}

impl<'a> TraversalExecutionIter<'a> {
    fn new(
        snapshot: &'a dyn GraphView,
        start_node_id: NodeId,
        direction: EdgeDirection,
        runtime: TraversalRuntime,
    ) -> Self {
        let mut visited_nodes = BTreeSet::new();
        let mut pending = VecDeque::new();
        let mut queue = VecDeque::new();
        if snapshot.contains_node(&start_node_id) {
            visited_nodes.insert(start_node_id.clone());
            queue.push_back((start_node_id.clone(), 0));
            if let Some(node) = snapshot.get_node(&start_node_id) {
                pending.push_back(QueryExecutionItem::Node(node));
            }
        }
        Self {
            snapshot,
            direction,
            runtime,
            started: Instant::now(),
            visited_nodes,
            visited_edges: BTreeSet::new(),
            queue,
            pending,
        }
    }
}

impl Iterator for TraversalExecutionIter<'_> {
    type Item = QueryExecutionItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Some(item);
            }

            let (node_id, depth) = self.queue.pop_front()?;
            if self.started.elapsed().as_millis() >= u128::from(self.runtime.timeout_ms) {
                return None;
            }
            if depth == self.runtime.max_depth || self.visited_nodes.len() >= self.runtime.limit {
                continue;
            }

            for (edge, neighbor_id) in expand_neighbors_iter(
                self.snapshot,
                &node_id,
                self.direction,
                &self.runtime.constraints,
            ) {
                if self.visited_edges.len() < self.runtime.limit
                    && self.visited_edges.insert(edge.id.clone())
                {
                    self.pending.push_back(QueryExecutionItem::Edge(edge));
                }
                if self.visited_nodes.insert(neighbor_id.clone())
                    && self.visited_nodes.len() <= self.runtime.limit
                {
                    self.queue.push_back((neighbor_id.clone(), depth + 1));
                    if let Some(node) = self.snapshot.get_node(&neighbor_id) {
                        self.pending.push_back(QueryExecutionItem::Node(node));
                    }
                }
            }
        }
    }
}

fn time_range_execution_iter<'a>(
    snapshot: &'a dyn GraphView,
    field: &'a str,
    from_epoch_ms: i64,
    to_epoch_ms: i64,
    limit: usize,
) -> Box<dyn Iterator<Item = QueryExecutionItem> + 'a> {
    if field == "timestamp" {
        Box::new(
            snapshot
                .node_ids_in_time_range_iter(from_epoch_ms, to_epoch_ms)
                .filter_map(|node_id| snapshot.get_node(&node_id))
                .take(limit)
                .map(QueryExecutionItem::Node),
        )
    } else {
        Box::new(
            snapshot
                .all_nodes_iter()
                .filter_map(move |node| {
                    node.property(field)
                        .and_then(PropertyValue::as_i64)
                        .filter(|value| (*value >= from_epoch_ms) && (*value <= to_epoch_ms))
                        .map(|_| node)
                })
                .take(limit)
                .map(QueryExecutionItem::Node),
        )
    }
}

fn shortest_path(
    snapshot: &dyn GraphView,
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    direction: EdgeDirection,
    runtime: &TraversalRuntime,
) -> Option<GraphPath> {
    if !snapshot.contains_node(source_node_id) || !snapshot.contains_node(target_node_id) {
        return None;
    }
    if source_node_id == target_node_id {
        return Some(GraphPath {
            node_ids: vec![source_node_id.clone()],
            edge_ids: Vec::new(),
            hop_count: 0,
        });
    }

    let started = Instant::now();
    let mut forward_queue = VecDeque::from([(source_node_id.clone(), 0_u8)]);
    let mut backward_queue = VecDeque::from([(target_node_id.clone(), 0_u8)]);
    let mut forward_parents = BTreeMap::from([(
        source_node_id.clone(),
        PathParent {
            previous: None,
            via_edge: None,
        },
    )]);
    let mut backward_parents = BTreeMap::from([(
        target_node_id.clone(),
        PathParent {
            previous: None,
            via_edge: None,
        },
    )]);

    while !forward_queue.is_empty() && !backward_queue.is_empty() {
        if started.elapsed().as_millis() >= u128::from(runtime.timeout_ms) {
            return None;
        }
        if (forward_parents.len() + backward_parents.len()) >= runtime.limit {
            return None;
        }

        if let Some(meet) = expand_frontier(
            snapshot,
            &mut forward_queue,
            &mut forward_parents,
            &backward_parents,
            direction,
            runtime,
        ) {
            return reconstruct_path(
                source_node_id,
                target_node_id,
                meet,
                &forward_parents,
                &backward_parents,
            );
        }
        if let Some(meet) = expand_frontier(
            snapshot,
            &mut backward_queue,
            &mut backward_parents,
            &forward_parents,
            reverse_direction(direction),
            runtime,
        ) {
            return reconstruct_path(
                source_node_id,
                target_node_id,
                meet,
                &forward_parents,
                &backward_parents,
            );
        }
    }

    None
}

fn vector_search(
    snapshot: &dyn GraphView,
    query_vector: &[f32],
    node_type: Option<&str>,
    limit: usize,
    top_k_override: Option<usize>,
) -> Vec<RankedNodeResult> {
    let mut heap = BinaryHeap::new();
    let candidate_ids = semantic_candidate_ids(
        snapshot,
        SemanticCandidateQuery {
            query_vector,
            node_type,
            limit,
            top_k_override,
        },
    );
    for result in candidate_ids
        .filter_map(|node_id| snapshot.get_node(&node_id))
        .filter_map(|node| {
            let embedding = node.embedding()?;
            let semantic =
                MemoryRanker::cosine_similarity(query_vector, embedding).unwrap_or_default();
            Some(RankedNodeResult {
                node: node.clone(),
                score: semantic,
                breakdown: ScoreBreakdown {
                    structural: 0.0,
                    semantic,
                    temporal: 0.0,
                    importance: 0.0,
                    confidence: 0.0,
                },
            })
        })
    {
        push_top_ranked_result(&mut heap, limit, result);
    }
    finalize_top_ranked_results(heap)
}

struct RankedRetrievalParams<'a> {
    query_vector: Option<&'a [f32]>,
    reference_node_id: Option<&'a NodeId>,
    edge_type: Option<&'a str>,
    from_epoch_ms: Option<i64>,
    to_epoch_ms: Option<i64>,
    limit: usize,
    top_k_override: Option<usize>,
    now_epoch_ms: i64,
    profile: &'a RetrievalProfile,
}

fn ranked_retrieval(
    snapshot: &dyn GraphView,
    params: RankedRetrievalParams<'_>,
) -> Vec<RankedNodeResult> {
    let structural_distances = params
        .reference_node_id
        .map(|reference_node_id| graph_distances(snapshot, reference_node_id, params.edge_type));

    let mut heap = BinaryHeap::new();
    for result in
        ranked_retrieval_candidates(snapshot, &params, structural_distances.as_ref()).map(|node| {
            let structural = structural_distances
                .as_ref()
                .and_then(|distances| distances.get(&node.id).copied())
                .map(distance_to_score)
                .unwrap_or(0.0);
            let semantic = params
                .query_vector
                .and_then(|query_vector| {
                    node.embedding().and_then(|embedding| {
                        MemoryRanker::cosine_similarity(query_vector, embedding)
                    })
                })
                .unwrap_or(0.0);
            let temporal = node
                .timestamp_ms()
                .map(|timestamp| {
                    MemoryRanker::temporal_recency_score(timestamp, params.now_epoch_ms)
                })
                .unwrap_or(0.0);
            let importance = MemoryRanker::normalize_signal(node.importance());
            let confidence = MemoryRanker::normalize_signal(node.confidence());
            let breakdown = ScoreBreakdown {
                structural,
                semantic,
                temporal,
                importance,
                confidence,
            };

            RankedNodeResult {
                node,
                score: MemoryRanker::rank(breakdown, params.profile.weights),
                breakdown,
            }
        })
    {
        push_top_ranked_result(&mut heap, params.limit, result);
    }
    finalize_top_ranked_results(heap)
}

fn semantic_candidate_ids<'a>(
    snapshot: &'a dyn GraphView,
    query: SemanticCandidateQuery<'a>,
) -> Box<dyn Iterator<Item = NodeId> + 'a> {
    snapshot.semantic_candidate_ids_iter(query)
}

fn ranked_retrieval_candidates<'a>(
    snapshot: &'a dyn GraphView,
    params: &'a RankedRetrievalParams<'a>,
    structural_distances: Option<&'a BTreeMap<NodeId, u8>>,
) -> Box<dyn Iterator<Item = NodeRecord> + 'a> {
    if let Some(query_vector) = params.query_vector {
        let mut candidate_ids = BTreeSet::new();
        candidate_ids.extend(semantic_candidate_ids(
            snapshot,
            SemanticCandidateQuery {
                query_vector,
                node_type: None,
                limit: params.limit,
                top_k_override: params.top_k_override,
            },
        ));
        if let Some(structural_distances) = structural_distances {
            candidate_ids.extend(structural_distances.keys().cloned());
        }

        if !candidate_ids.is_empty() {
            return Box::new(
                candidate_ids
                    .into_iter()
                    .filter_map(|node_id| snapshot.get_node(&node_id))
                    .filter(move |node| {
                        within_optional_time_range(node, params.from_epoch_ms, params.to_epoch_ms)
                    }),
            );
        }
    }

    match (params.from_epoch_ms, params.to_epoch_ms) {
        (Some(from_epoch_ms), Some(to_epoch_ms)) => Box::new(
            snapshot
                .node_ids_in_time_range_iter(from_epoch_ms, to_epoch_ms)
                .filter_map(|node_id| snapshot.get_node(&node_id)),
        ),
        _ => Box::new(snapshot.all_nodes_iter().filter(move |node| {
            within_optional_time_range(node, params.from_epoch_ms, params.to_epoch_ms)
        })),
    }
}

fn resolve_retrieval_profile(name: Option<&str>) -> Result<RetrievalProfile> {
    let profile = RetrievalProfile::v1_default();
    match name {
        None | Some("v1-default") => Ok(profile),
        Some(value) => Err(Undr9Error::Validation(format!(
            "unsupported retrieval profile '{value}'"
        ))),
    }
}

fn within_optional_time_range(
    node: &NodeRecord,
    from_epoch_ms: Option<i64>,
    to_epoch_ms: Option<i64>,
) -> bool {
    match (from_epoch_ms, to_epoch_ms) {
        (None, None) => true,
        (Some(from_epoch_ms), Some(to_epoch_ms)) => node
            .timestamp_ms()
            .map(|timestamp| timestamp >= from_epoch_ms && timestamp <= to_epoch_ms)
            .unwrap_or(false),
        (Some(from_epoch_ms), None) => node
            .timestamp_ms()
            .map(|timestamp| timestamp >= from_epoch_ms)
            .unwrap_or(false),
        (None, Some(to_epoch_ms)) => node
            .timestamp_ms()
            .map(|timestamp| timestamp <= to_epoch_ms)
            .unwrap_or(false),
    }
}

fn graph_distances(
    snapshot: &dyn GraphView,
    start_node_id: &NodeId,
    edge_type: Option<&str>,
) -> BTreeMap<NodeId, u8> {
    if !snapshot.contains_node(start_node_id) {
        return BTreeMap::new();
    }

    let mut distances = BTreeMap::from([(start_node_id.clone(), 0_u8)]);
    let mut queue = VecDeque::from([(start_node_id.clone(), 0_u8)]);

    while let Some((node_id, depth)) = queue.pop_front() {
        if depth >= 3 {
            continue;
        }

        for edge_id in snapshot.edge_ids_for_iter(&node_id, EdgeDirection::Both, edge_type) {
            let Some(edge) = snapshot.get_edge(&edge_id) else {
                continue;
            };

            let neighbor_id = if edge.source == node_id {
                edge.target.clone()
            } else {
                edge.source.clone()
            };
            distances.entry(neighbor_id.clone()).or_insert_with(|| {
                queue.push_back((neighbor_id.clone(), depth + 1));
                depth + 1
            });
        }
    }

    distances
}

fn resolve_traversal_runtime(
    max_depth: Option<u8>,
    limit: Option<usize>,
    timeout_ms: Option<u64>,
    constraints: Option<TraversalConstraints>,
) -> Result<TraversalRuntime> {
    let max_depth = max_depth.unwrap_or(DEFAULT_MAX_DEPTH);
    let limit = limit.unwrap_or(DEFAULT_RESULT_LIMIT);
    let timeout_ms = timeout_ms.unwrap_or(DEFAULT_QUERY_TIMEOUT_MS);

    if max_depth == 0 {
        return Err(Undr9Error::Validation(
            "max_depth must be greater than zero".to_owned(),
        ));
    }
    let limit = resolve_required_result_limit(limit)?;
    let timeout_ms = resolve_timeout_ms(timeout_ms)?;

    Ok(TraversalRuntime {
        max_depth,
        limit,
        timeout_ms,
        constraints: constraints.unwrap_or_default(),
    })
}

fn resolve_result_limit(limit: Option<usize>) -> Result<usize> {
    resolve_required_result_limit(limit.unwrap_or(DEFAULT_RESULT_LIMIT))
}

fn resolve_required_result_limit(limit: usize) -> Result<usize> {
    if limit == 0 {
        return Err(Undr9Error::Validation(
            "limit must be greater than zero".to_owned(),
        ));
    }
    if limit > MAX_RESULT_LIMIT {
        return Err(Undr9Error::Validation(format!(
            "limit must be less than or equal to {MAX_RESULT_LIMIT}"
        )));
    }
    Ok(limit)
}

fn resolve_timeout_ms(timeout_ms: u64) -> Result<u64> {
    if timeout_ms == 0 {
        return Err(Undr9Error::Validation(
            "timeout_ms must be greater than zero".to_owned(),
        ));
    }
    if timeout_ms > MAX_QUERY_TIMEOUT_MS {
        return Err(Undr9Error::Validation(format!(
            "timeout_ms must be less than or equal to {MAX_QUERY_TIMEOUT_MS}"
        )));
    }
    Ok(timeout_ms)
}

impl TraversalRuntime {
    fn merge_legacy_edge_type(mut self, edge_type: Option<String>) -> Self {
        if let Some(edge_type) = edge_type {
            if !self
                .constraints
                .edge_types
                .iter()
                .any(|value| value == &edge_type)
            {
                self.constraints.edge_types.push(edge_type);
            }
        }
        self
    }
}

fn expand_neighbors(
    snapshot: &dyn GraphView,
    node_id: &NodeId,
    direction: EdgeDirection,
    constraints: &TraversalConstraints,
) -> Vec<(EdgeRecord, NodeId)> {
    expand_neighbors_iter(snapshot, node_id, direction, constraints).collect()
}

fn expand_neighbors_iter<'a>(
    snapshot: &'a dyn GraphView,
    node_id: &'a NodeId,
    direction: EdgeDirection,
    constraints: &'a TraversalConstraints,
) -> Box<dyn Iterator<Item = (EdgeRecord, NodeId)> + 'a> {
    Box::new(
        snapshot
            .edge_ids_for_iter(node_id, direction, None)
            .filter_map(move |edge_id| snapshot.get_edge(&edge_id))
            .filter(move |edge| {
                constraints.edge_types.is_empty()
                    || constraints.edge_types.contains(&edge.edge_type)
            })
            .filter_map(move |edge| {
                let neighbor_id = match direction {
                    EdgeDirection::Outgoing => edge.target.clone(),
                    EdgeDirection::Incoming => edge.source.clone(),
                    EdgeDirection::Both => {
                        if edge.source == *node_id {
                            edge.target.clone()
                        } else {
                            edge.source.clone()
                        }
                    }
                };
                let neighbor = snapshot.get_node(&neighbor_id)?;
                let label_matches = constraints.node_labels.is_empty()
                    || constraints.node_labels.contains(&neighbor.node_type);
                label_matches.then_some((edge, neighbor_id))
            }),
    )
}

fn expand_frontier(
    snapshot: &dyn GraphView,
    queue: &mut VecDeque<(NodeId, u8)>,
    parents: &mut BTreeMap<NodeId, PathParent>,
    other_parents: &BTreeMap<NodeId, PathParent>,
    direction: EdgeDirection,
    runtime: &TraversalRuntime,
) -> Option<NodeId> {
    let level_size = queue.len();
    for _ in 0..level_size {
        let Some((node_id, depth)) = queue.pop_front() else {
            break;
        };
        if depth == runtime.max_depth {
            continue;
        }
        for (edge, neighbor_id) in
            expand_neighbors(snapshot, &node_id, direction, &runtime.constraints)
        {
            if parents.contains_key(&neighbor_id) {
                continue;
            }
            parents.insert(
                neighbor_id.clone(),
                PathParent {
                    previous: Some(node_id.clone()),
                    via_edge: Some(edge.id.clone()),
                },
            );
            if other_parents.contains_key(&neighbor_id) {
                return Some(neighbor_id);
            }
            queue.push_back((neighbor_id, depth + 1));
        }
    }
    None
}

fn node_unique_key(node: &NodeRecord) -> Option<&str> {
    node.properties
        .get("unique_key")
        .and_then(|value| match value {
            PropertyValue::String(value) => Some(value.as_str()),
            _ => None,
        })
}

fn within_required_time_range(node: &NodeRecord, from_epoch_ms: i64, to_epoch_ms: i64) -> bool {
    node.timestamp_ms()
        .map(|timestamp| timestamp >= from_epoch_ms && timestamp <= to_epoch_ms)
        .unwrap_or(false)
}

fn edge_matches(
    edge: &EdgeRecord,
    node_id: &NodeId,
    direction: EdgeDirection,
    edge_type: Option<&str>,
) -> bool {
    let direction_matches = match direction {
        EdgeDirection::Outgoing => edge.source == *node_id,
        EdgeDirection::Incoming => edge.target == *node_id,
        EdgeDirection::Both => edge.source == *node_id || edge.target == *node_id,
    };
    direction_matches
        && edge_type
            .map(|expected| edge.edge_type == expected)
            .unwrap_or(true)
}

fn reconstruct_path(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    meet_node_id: NodeId,
    forward_parents: &BTreeMap<NodeId, PathParent>,
    backward_parents: &BTreeMap<NodeId, PathParent>,
) -> Option<GraphPath> {
    let mut left_nodes = Vec::new();
    let mut left_edges = Vec::new();
    let mut cursor = meet_node_id.clone();
    while let Some(parent) = forward_parents.get(&cursor) {
        left_nodes.push(cursor.clone());
        if let Some(edge_id) = &parent.via_edge {
            left_edges.push(edge_id.clone());
        }
        let Some(previous) = &parent.previous else {
            break;
        };
        cursor = previous.clone();
    }
    left_nodes.reverse();
    left_edges.reverse();

    let mut right_nodes = Vec::new();
    let mut right_edges = Vec::new();
    let mut cursor = meet_node_id.clone();
    while let Some(parent) = backward_parents.get(&cursor) {
        let Some(previous) = &parent.previous else {
            break;
        };
        if let Some(edge_id) = &parent.via_edge {
            right_edges.push(edge_id.clone());
        }
        right_nodes.push(previous.clone());
        cursor = previous.clone();
    }

    let mut node_ids = left_nodes;
    node_ids.extend(right_nodes);
    if node_ids.first() != Some(source_node_id) || node_ids.last() != Some(target_node_id) {
        return None;
    }

    let mut edge_ids = left_edges;
    edge_ids.extend(right_edges);
    Some(GraphPath {
        hop_count: edge_ids.len(),
        node_ids,
        edge_ids,
    })
}

fn reverse_direction(direction: EdgeDirection) -> EdgeDirection {
    match direction {
        EdgeDirection::Outgoing => EdgeDirection::Incoming,
        EdgeDirection::Incoming => EdgeDirection::Outgoing,
        EdgeDirection::Both => EdgeDirection::Both,
    }
}

fn distance_to_score(distance: u8) -> f32 {
    1.0 / (1.0 + f32::from(distance))
}

fn sort_ranked_results(ranked_results: &mut [RankedNodeResult]) {
    ranked_results.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.node.id.as_str().cmp(right.node.id.as_str()))
    });
}

#[derive(Debug, Clone)]
struct RankedHeapEntry {
    result: RankedNodeResult,
}

impl PartialEq for RankedHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.result.score.to_bits() == other.result.score.to_bits()
            && self.result.node.id == other.result.node.id
    }
}

impl Eq for RankedHeapEntry {}

impl PartialOrd for RankedHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .result
            .score
            .total_cmp(&self.result.score)
            .then_with(|| {
                self.result
                    .node
                    .id
                    .as_str()
                    .cmp(other.result.node.id.as_str())
            })
    }
}

fn push_top_ranked_result(
    heap: &mut BinaryHeap<RankedHeapEntry>,
    limit: usize,
    result: RankedNodeResult,
) {
    if limit == 0 {
        return;
    }

    let candidate = RankedHeapEntry { result };
    if heap.len() < limit {
        heap.push(candidate);
        return;
    }

    let should_replace = heap.peek().map(|worst| candidate < *worst).unwrap_or(false);
    if should_replace {
        let _ = heap.pop();
        heap.push(candidate);
    }
}

fn finalize_top_ranked_results(heap: BinaryHeap<RankedHeapEntry>) -> Vec<RankedNodeResult> {
    let mut ranked_results = heap
        .into_iter()
        .map(|entry| entry.result)
        .collect::<Vec<_>>();
    sort_ranked_results(&mut ranked_results);
    ranked_results
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Instant;

    use im::OrdMap;
    use undr9_common::{EdgeId, NodeId};
    use undr9_core::{EdgeRecord, NodeRecord, PropertyValue};
    use undr9_index::{EdgeDirection, GraphIndex};

    struct SemanticCandidateGraphView<'a> {
        base: &'a super::GraphSnapshot,
        semantic_candidate_ids: Vec<NodeId>,
    }

    impl super::GraphView for SemanticCandidateGraphView<'_> {
        fn get_node(&self, node_id: &NodeId) -> Option<NodeRecord> {
            self.base.get_node(node_id)
        }

        fn get_edge(&self, edge_id: &EdgeId) -> Option<EdgeRecord> {
            self.base.get_edge(edge_id)
        }

        fn contains_node(&self, node_id: &NodeId) -> bool {
            self.base.contains_node(node_id)
        }

        fn lookup_unique_key(&self, unique_key: &str) -> Option<NodeId> {
            self.base.lookup_unique_key(unique_key)
        }

        fn node_ids_by_type<'a>(
            &'a self,
            node_type: &'a str,
        ) -> Box<dyn Iterator<Item = NodeId> + 'a> {
            self.base.node_ids_by_type(node_type)
        }

        fn edge_ids_for_iter<'a>(
            &'a self,
            node_id: &'a NodeId,
            direction: EdgeDirection,
            edge_type: Option<&'a str>,
        ) -> Box<dyn Iterator<Item = EdgeId> + 'a> {
            self.base.edge_ids_for_iter(node_id, direction, edge_type)
        }

        fn node_ids_in_time_range_iter<'a>(
            &'a self,
            from_epoch_ms: i64,
            to_epoch_ms: i64,
        ) -> Box<dyn Iterator<Item = NodeId> + 'a> {
            self.base
                .node_ids_in_time_range_iter(from_epoch_ms, to_epoch_ms)
        }

        fn vector_candidate_ids_iter<'a>(&'a self) -> Box<dyn Iterator<Item = NodeId> + 'a> {
            self.base.vector_candidate_ids_iter()
        }

        fn semantic_candidate_ids_iter<'a>(
            &'a self,
            _query: super::SemanticCandidateQuery<'a>,
        ) -> Box<dyn Iterator<Item = NodeId> + 'a> {
            Box::new(self.semantic_candidate_ids.clone().into_iter())
        }

        fn all_nodes_iter<'a>(&'a self) -> Box<dyn Iterator<Item = NodeRecord> + 'a> {
            self.base.all_nodes_iter()
        }
    }

    #[test]
    fn planner_selects_exact_lookup_for_id_queries() {
        let request = super::QueryRequest::GetNodeById {
            node_id: NodeId::new("node_1").expect("valid node id"),
        };
        let plan = super::Planner::plan(&request).expect("plan should be built");

        assert_eq!(plan.kind, super::PlanKind::ExactLookup);
    }

    #[test]
    fn planner_rejects_invalid_traversal_bounds() {
        let request = super::QueryRequest::Traverse {
            start_node_id: NodeId::new("node_1").expect("valid node id"),
            edge_type: None,
            direction: EdgeDirection::Outgoing,
            max_hops: Some(0),
            limit: None,
            timeout_ms: None,
            constraints: None,
        };

        let error = super::Planner::plan(&request).expect_err("plan should be rejected");
        assert!(error.to_string().contains("max_depth"));
    }

    #[test]
    fn planner_rejects_invalid_ranked_retrieval_requests() {
        let request = super::QueryRequest::RankedRetrieval {
            query_vector: None,
            reference_node_id: None,
            edge_type: None,
            from_epoch_ms: None,
            to_epoch_ms: None,
            limit: 5,
            top_k: None,
            now_epoch_ms: 100,
            retrieval_profile: None,
        };

        let error = super::Planner::plan(&request).expect_err("request should be rejected");
        assert!(error.to_string().contains("requires at least"));
    }

    #[test]
    fn planner_records_vector_search_top_k_override() {
        let request = super::QueryRequest::VectorSearch {
            query_vector: vec![1.0, 0.0],
            node_type: Some("memory".to_owned()),
            limit: 5,
            top_k: Some(25),
        };

        let plan = super::Planner::plan(&request).expect("plan should be built");
        assert!(plan.filters.iter().any(|filter| {
            filter.field == "top_k" && filter.value == PropertyValue::Integer(25)
        }));
    }

    #[test]
    fn executes_neighbor_and_traversal_queries() {
        let node_a = NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
            .expect("node should build");
        let node_b = NodeRecord::new(NodeId::new("node_b").expect("valid node id"), "memory")
            .expect("node should build");
        let node_c = NodeRecord::new(NodeId::new("node_c").expect("valid node id"), "memory")
            .expect("node should build");
        let edge_ab = EdgeRecord {
            id: undr9_common::EdgeId::new("edge_ab").expect("valid edge id"),
            source: node_a.id.clone(),
            target: node_b.id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        };
        let edge_bc = EdgeRecord {
            id: undr9_common::EdgeId::new("edge_bc").expect("valid edge id"),
            source: node_b.id.clone(),
            target: node_c.id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        };

        let mut node_map = BTreeMap::new();
        node_map.insert(node_a.id.clone(), node_a.clone());
        node_map.insert(node_b.id.clone(), node_b.clone());
        node_map.insert(node_c.id.clone(), node_c.clone());
        let mut edge_map = BTreeMap::new();
        edge_map.insert(edge_ab.id.clone(), edge_ab.clone());
        edge_map.insert(edge_bc.id.clone(), edge_bc.clone());
        let index = GraphIndex::rebuild(
            &[node_a.clone(), node_b.clone(), node_c.clone()],
            &[edge_ab.clone(), edge_bc.clone()],
        );
        let snapshot = super::GraphSnapshot {
            nodes: node_map.into_iter().collect::<OrdMap<_, _>>(),
            edges: edge_map.into_iter().collect::<OrdMap<_, _>>(),
            indexes: index,
        };

        let neighbors = super::Executor::execute(
            &super::QueryRequest::ListNeighbors {
                node_id: node_a.id.clone(),
                edge_type: Some("relates_to".to_owned()),
                direction: EdgeDirection::Outgoing,
                limit: None,
            },
            &snapshot,
        )
        .expect("neighbors should execute");
        assert_eq!(neighbors.nodes.len(), 1);
        assert_eq!(neighbors.edges.len(), 1);
        assert!(neighbors.path.is_none());

        let traversal = super::Executor::execute(
            &super::QueryRequest::Traverse {
                start_node_id: node_a.id.clone(),
                edge_type: Some("relates_to".to_owned()),
                direction: EdgeDirection::Outgoing,
                max_hops: Some(2),
                limit: Some(10),
                timeout_ms: Some(5_000),
                constraints: Some(super::TraversalConstraints {
                    edge_types: vec!["relates_to".to_owned()],
                    node_labels: vec!["memory".to_owned()],
                }),
            },
            &snapshot,
        )
        .expect("traversal should execute");
        assert_eq!(traversal.nodes.len(), 3);
        assert_eq!(traversal.edges.len(), 2);
        assert!(traversal.ranked_results.is_empty());
        assert!(traversal.path.is_none());

        let shortest_path = super::Executor::execute(
            &super::QueryRequest::ShortestPath {
                source_node_id: node_a.id.clone(),
                target_node_id: node_c.id.clone(),
                direction: EdgeDirection::Outgoing,
                max_depth: Some(3),
                limit: Some(10),
                timeout_ms: Some(5_000),
                constraints: Some(super::TraversalConstraints {
                    edge_types: vec!["relates_to".to_owned()],
                    node_labels: vec!["memory".to_owned()],
                }),
            },
            &snapshot,
        )
        .expect("shortest path should execute");
        assert_eq!(shortest_path.path.expect("path should exist").hop_count, 2);
    }

    #[test]
    fn executes_unique_key_lookup() {
        let mut node = NodeRecord::new(NodeId::new("node_1").expect("valid node id"), "memory")
            .expect("node should build");
        node.properties.insert(
            "unique_key".to_owned(),
            PropertyValue::String("alpha".to_owned()),
        );
        let mut nodes = BTreeMap::new();
        nodes.insert(node.id.clone(), node.clone());
        let snapshot = super::GraphSnapshot {
            nodes: nodes.into_iter().collect::<OrdMap<_, _>>(),
            edges: BTreeMap::new().into_iter().collect::<OrdMap<_, _>>(),
            indexes: GraphIndex::rebuild(&[node.clone()], &[]),
        };

        let result = super::Executor::execute(
            &super::QueryRequest::GetNodeByUniqueKey {
                unique_key: "alpha".to_owned(),
            },
            &snapshot,
        )
        .expect("lookup should succeed");

        assert_eq!(result.nodes.len(), 1);
        assert_eq!(result.nodes[0].id, node.id);
    }

    #[test]
    fn label_scan_respects_result_limit() {
        let node_a =
            NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory").expect("node");
        let node_b =
            NodeRecord::new(NodeId::new("node_b").expect("valid node id"), "memory").expect("node");
        let snapshot = super::GraphSnapshot {
            nodes: vec![
                (node_a.id.clone(), node_a.clone()),
                (node_b.id.clone(), node_b.clone()),
            ]
            .into_iter()
            .collect::<OrdMap<_, _>>(),
            edges: BTreeMap::new().into_iter().collect::<OrdMap<_, _>>(),
            indexes: GraphIndex::rebuild(&[node_a.clone(), node_b.clone()], &[]),
        };

        let result = super::Executor::execute(
            &super::QueryRequest::SearchByLabel {
                label: "memory".to_owned(),
                limit: Some(1),
            },
            &snapshot,
        )
        .expect("label scan should execute");
        assert_eq!(result.nodes.len(), 1);
    }

    #[test]
    fn execute_iter_streams_label_and_time_range_nodes() {
        let mut node_a =
            NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory").expect("node");
        node_a
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(1_000));
        let mut node_b =
            NodeRecord::new(NodeId::new("node_b").expect("valid node id"), "memory").expect("node");
        node_b
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(1_100));
        let mut node_c = NodeRecord::new(NodeId::new("node_c").expect("valid node id"), "profile")
            .expect("node");
        node_c
            .properties
            .insert("custom_time".to_owned(), PropertyValue::Integer(1_050));
        let snapshot = super::GraphSnapshot {
            nodes: vec![
                (node_a.id.clone(), node_a.clone()),
                (node_b.id.clone(), node_b.clone()),
                (node_c.id.clone(), node_c.clone()),
            ]
            .into_iter()
            .collect::<OrdMap<_, _>>(),
            edges: BTreeMap::new().into_iter().collect::<OrdMap<_, _>>(),
            indexes: GraphIndex::rebuild(&[node_a.clone(), node_b.clone(), node_c.clone()], &[]),
        };

        let label_items = super::Executor::execute_iter(
            &super::QueryRequest::SearchByLabel {
                label: "memory".to_owned(),
                limit: Some(1),
            },
            &snapshot,
        )
        .expect("label scan iterator should execute")
        .into_items()
        .collect::<Vec<_>>();
        assert_eq!(label_items.len(), 1);
        match &label_items[0] {
            super::QueryExecutionItem::Node(node) => assert_eq!(node.node_type, "memory"),
            other => panic!("unexpected label scan item: {other:?}"),
        }

        let time_range_items = super::Executor::execute_iter(
            &super::QueryRequest::TimeRange {
                field: "custom_time".to_owned(),
                from_epoch_ms: 1_000,
                to_epoch_ms: 1_100,
                limit: 10,
            },
            &snapshot,
        )
        .expect("time range iterator should execute")
        .into_items()
        .collect::<Vec<_>>();
        assert_eq!(time_range_items.len(), 1);
        match &time_range_items[0] {
            super::QueryExecutionItem::Node(node) => assert_eq!(node.id, node_c.id),
            other => panic!("unexpected time range item: {other:?}"),
        }
    }

    #[test]
    fn executes_temporal_vector_and_ranked_retrieval_queries() {
        let mut node_a =
            NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory").expect("node");
        node_a.properties.insert(
            "embedding".to_owned(),
            PropertyValue::FloatList(vec![1.0, 0.0]),
        );
        node_a
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(1_000));
        node_a
            .properties
            .insert("importance".to_owned(), PropertyValue::Float(0.9));
        node_a
            .properties
            .insert("confidence".to_owned(), PropertyValue::Float(0.8));

        let mut node_b =
            NodeRecord::new(NodeId::new("node_b").expect("valid node id"), "memory").expect("node");
        node_b.properties.insert(
            "embedding".to_owned(),
            PropertyValue::FloatList(vec![0.8, 0.2]),
        );
        node_b
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(1_100));
        node_b
            .properties
            .insert("importance".to_owned(), PropertyValue::Float(0.6));
        node_b
            .properties
            .insert("confidence".to_owned(), PropertyValue::Float(0.7));

        let mut node_c =
            NodeRecord::new(NodeId::new("node_c").expect("valid node id"), "memory").expect("node");
        node_c.properties.insert(
            "embedding".to_owned(),
            PropertyValue::FloatList(vec![0.0, 1.0]),
        );
        node_c
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(4_000_000));
        node_c
            .properties
            .insert("importance".to_owned(), PropertyValue::Float(0.2));
        node_c
            .properties
            .insert("confidence".to_owned(), PropertyValue::Float(0.4));

        let edge_ab = EdgeRecord {
            id: EdgeId::new("edge_ab").expect("valid edge id"),
            source: node_a.id.clone(),
            target: node_b.id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        };
        let edge_bc = EdgeRecord {
            id: EdgeId::new("edge_bc").expect("valid edge id"),
            source: node_b.id.clone(),
            target: node_c.id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        };

        let mut node_map = BTreeMap::new();
        node_map.insert(node_a.id.clone(), node_a.clone());
        node_map.insert(node_b.id.clone(), node_b.clone());
        node_map.insert(node_c.id.clone(), node_c.clone());
        let mut edge_map = BTreeMap::new();
        edge_map.insert(edge_ab.id.clone(), edge_ab.clone());
        edge_map.insert(edge_bc.id.clone(), edge_bc.clone());
        let index = GraphIndex::rebuild(
            &[node_a.clone(), node_b.clone(), node_c.clone()],
            &[edge_ab.clone(), edge_bc.clone()],
        );
        let snapshot = super::GraphSnapshot {
            nodes: node_map.into_iter().collect::<OrdMap<_, _>>(),
            edges: edge_map.into_iter().collect::<OrdMap<_, _>>(),
            indexes: index,
        };

        let time_range = super::Executor::execute(
            &super::QueryRequest::TimeRange {
                field: "timestamp".to_owned(),
                from_epoch_ms: 900,
                to_epoch_ms: 2_000,
                limit: 10,
            },
            &snapshot,
        )
        .expect("time range should execute");
        assert_eq!(time_range.nodes.len(), 2);

        let vector = super::Executor::execute(
            &super::QueryRequest::VectorSearch {
                query_vector: vec![1.0, 0.0],
                node_type: Some("memory".to_owned()),
                limit: 2,
                top_k: None,
            },
            &snapshot,
        )
        .expect("vector search should execute");
        assert_eq!(vector.ranked_results.len(), 2);
        assert_eq!(vector.ranked_results[0].node.id, node_a.id);

        let ranked = super::Executor::execute(
            &super::QueryRequest::RankedRetrieval {
                query_vector: Some(vec![1.0, 0.0]),
                reference_node_id: Some(node_a.id.clone()),
                edge_type: Some("relates_to".to_owned()),
                from_epoch_ms: Some(900),
                to_epoch_ms: Some(10_000),
                limit: 3,
                top_k: None,
                now_epoch_ms: 1_200,
                retrieval_profile: Some("v1-default".to_owned()),
            },
            &snapshot,
        )
        .expect("ranked retrieval should execute");
        assert_eq!(ranked.plan_kind, super::PlanKind::RankedHybrid);
        assert_eq!(ranked.ranked_results.len(), 2);
        assert!(ranked.ranked_results[0].score >= ranked.ranked_results[1].score);
        assert_eq!(ranked.ranked_results[0].node.id, node_a.id);
        assert_eq!(ranked.retrieval_profile.as_deref(), Some("v1-default"));
    }

    #[test]
    fn ranked_retrieval_unions_semantic_and_structural_candidates() {
        let mut node_a =
            NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory").expect("node");
        node_a.properties.insert(
            "embedding".to_owned(),
            PropertyValue::FloatList(vec![1.0, 0.0]),
        );
        node_a
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(1_000));
        node_a
            .properties
            .insert("importance".to_owned(), PropertyValue::Float(0.9));
        node_a
            .properties
            .insert("confidence".to_owned(), PropertyValue::Float(0.9));

        let mut node_b =
            NodeRecord::new(NodeId::new("node_b").expect("valid node id"), "memory").expect("node");
        node_b.properties.insert(
            "embedding".to_owned(),
            PropertyValue::FloatList(vec![0.0, 1.0]),
        );
        node_b
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(1_050));
        node_b
            .properties
            .insert("importance".to_owned(), PropertyValue::Float(0.4));
        node_b
            .properties
            .insert("confidence".to_owned(), PropertyValue::Float(0.4));

        let edge_ab = EdgeRecord {
            id: EdgeId::new("edge_ab").expect("valid edge id"),
            source: node_a.id.clone(),
            target: node_b.id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        };

        let snapshot = super::GraphSnapshot {
            nodes: vec![
                (node_a.id.clone(), node_a.clone()),
                (node_b.id.clone(), node_b.clone()),
            ]
            .into_iter()
            .collect::<OrdMap<_, _>>(),
            edges: vec![(edge_ab.id.clone(), edge_ab.clone())]
                .into_iter()
                .collect::<OrdMap<_, _>>(),
            indexes: GraphIndex::rebuild(
                &[node_a.clone(), node_b.clone()],
                std::slice::from_ref(&edge_ab),
            ),
        };
        let graph_view = SemanticCandidateGraphView {
            base: &snapshot,
            semantic_candidate_ids: vec![node_a.id.clone()],
        };

        let ranked = super::ranked_retrieval(
            &graph_view,
            super::RankedRetrievalParams {
                query_vector: Some(&[1.0, 0.0]),
                reference_node_id: Some(&node_a.id),
                edge_type: Some("relates_to"),
                from_epoch_ms: Some(900),
                to_epoch_ms: Some(1_100),
                limit: 5,
                top_k_override: None,
                now_epoch_ms: 1_200,
                profile: &super::RetrievalProfile::v1_default(),
            },
        );

        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].node.id, node_a.id);
        assert!(ranked.iter().any(|result| result.node.id == node_b.id));
        let node_a_result = ranked
            .iter()
            .find(|result| result.node.id == node_a.id)
            .expect("node_a should remain in the candidate set");
        let node_b_result = ranked
            .iter()
            .find(|result| result.node.id == node_b.id)
            .expect("node_b should be retained by structural union");
        assert!(node_b_result.breakdown.structural > 0.0);
        assert!(node_b_result.breakdown.semantic <= 0.5);
        assert!(node_b_result.breakdown.semantic < node_a_result.breakdown.semantic);
    }

    #[test]
    fn execute_iter_streams_traversal_items_incrementally() {
        let node_a = NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
            .expect("node should build");
        let node_b = NodeRecord::new(NodeId::new("node_b").expect("valid node id"), "memory")
            .expect("node should build");
        let edge_ab = EdgeRecord {
            id: EdgeId::new("edge_ab").expect("valid edge id"),
            source: node_a.id.clone(),
            target: node_b.id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        };

        let snapshot = super::GraphSnapshot {
            nodes: vec![
                (node_a.id.clone(), node_a.clone()),
                (node_b.id.clone(), node_b.clone()),
            ]
            .into_iter()
            .collect::<OrdMap<_, _>>(),
            edges: vec![(edge_ab.id.clone(), edge_ab.clone())]
                .into_iter()
                .collect::<OrdMap<_, _>>(),
            indexes: GraphIndex::rebuild(
                &[node_a.clone(), node_b.clone()],
                std::slice::from_ref(&edge_ab),
            ),
        };

        let items = super::Executor::execute_iter(
            &super::QueryRequest::Traverse {
                start_node_id: node_a.id.clone(),
                edge_type: Some("relates_to".to_owned()),
                direction: EdgeDirection::Outgoing,
                max_hops: Some(2),
                limit: Some(10),
                timeout_ms: Some(5_000),
                constraints: None,
            },
            &snapshot,
        )
        .expect("traversal iterator should execute")
        .into_items()
        .collect::<Vec<_>>();

        assert_eq!(items.len(), 3);
        match &items[0] {
            super::QueryExecutionItem::Node(node) => assert_eq!(node.id, node_a.id),
            other => panic!("unexpected first traversal item: {other:?}"),
        }
        assert!(items.iter().any(|item| matches!(
            item,
            super::QueryExecutionItem::Edge(edge) if edge.id == edge_ab.id
        )));
        assert!(items.iter().any(|item| matches!(
            item,
            super::QueryExecutionItem::Node(node) if node.id == node_b.id
        )));
    }

    #[test]
    fn top_k_heap_keeps_highest_scores_with_stable_tie_breaking() {
        fn ranked(node_id: &str, score: f32) -> super::RankedNodeResult {
            super::RankedNodeResult {
                node: NodeRecord::new(NodeId::new(node_id).expect("valid node id"), "memory")
                    .expect("node should build"),
                score,
                breakdown: super::ScoreBreakdown {
                    structural: 0.0,
                    semantic: 0.0,
                    temporal: 0.0,
                    importance: 0.0,
                    confidence: 0.0,
                },
            }
        }

        let mut heap = std::collections::BinaryHeap::new();
        for result in [
            ranked("node_d", 0.40),
            ranked("node_c", 0.80),
            ranked("node_b", 0.95),
            ranked("node_a", 0.80),
        ] {
            super::push_top_ranked_result(&mut heap, 3, result);
        }

        let ranked_results = super::finalize_top_ranked_results(heap);
        let ranked_ids = ranked_results
            .iter()
            .map(|result| result.node.id.as_str().to_owned())
            .collect::<Vec<_>>();
        let ranked_scores = ranked_results
            .iter()
            .map(|result| result.score)
            .collect::<Vec<_>>();

        assert_eq!(ranked_ids, vec!["node_b", "node_a", "node_c"]);
        assert_eq!(ranked_scores, vec![0.95, 0.80, 0.80]);
    }

    #[test]
    #[ignore = "benchmark automation entrypoint"]
    fn benchmark_hybrid_retrieval_workload() {
        let mut nodes = Vec::new();
        let mut node_map = BTreeMap::new();
        let mut edges = Vec::new();
        let mut edge_map = BTreeMap::new();

        for index in 0..1_000 {
            let mut node = NodeRecord::new(
                NodeId::new(format!("node_{index}")).expect("valid node id"),
                "memory",
            )
            .expect("node should build");
            node.properties.insert(
                "embedding".to_owned(),
                PropertyValue::FloatList(vec![
                    1.0 - (index as f32 / 1_000.0),
                    index as f32 / 1_000.0,
                ]),
            );
            node.properties.insert(
                "timestamp".to_owned(),
                PropertyValue::Integer(1_000 + index as i64),
            );
            node.properties
                .insert("importance".to_owned(), PropertyValue::Float(0.5));
            node.properties
                .insert("confidence".to_owned(), PropertyValue::Float(0.5));
            node_map.insert(node.id.clone(), node.clone());
            nodes.push(node);
        }

        for index in 0..999 {
            let edge = EdgeRecord {
                id: EdgeId::new(format!("edge_{index}")).expect("valid edge id"),
                source: NodeId::new(format!("node_{index}")).expect("valid node id"),
                target: NodeId::new(format!("node_{}", index + 1)).expect("valid node id"),
                edge_type: "relates_to".to_owned(),
                properties: BTreeMap::new(),
            };
            edge_map.insert(edge.id.clone(), edge.clone());
            edges.push(edge);
        }

        let snapshot = super::GraphSnapshot {
            nodes: node_map.into_iter().collect::<OrdMap<_, _>>(),
            edges: edge_map.into_iter().collect::<OrdMap<_, _>>(),
            indexes: GraphIndex::rebuild(&nodes, &edges),
        };

        let start = Instant::now();
        let response = super::Executor::execute(
            &super::QueryRequest::RankedRetrieval {
                query_vector: Some(vec![1.0, 0.0]),
                reference_node_id: Some(NodeId::new("node_0").expect("valid node id")),
                edge_type: Some("relates_to".to_owned()),
                from_epoch_ms: Some(1_000),
                to_epoch_ms: Some(2_000),
                limit: 20,
                top_k: None,
                now_epoch_ms: 2_000,
                retrieval_profile: Some("v1-default".to_owned()),
            },
            &snapshot,
        )
        .expect("benchmark query should execute");
        let elapsed = start.elapsed();

        println!(
            "benchmark_hybrid_retrieval_workload elapsed_ms={} results={}",
            elapsed.as_millis(),
            response.ranked_results.len()
        );
        assert_eq!(response.ranked_results.len(), 20);
    }
}
