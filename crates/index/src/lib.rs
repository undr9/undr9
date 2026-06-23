use std::collections::{BTreeMap, BTreeSet};

use im::OrdMap;
use serde::{Deserialize, Serialize};
use undr9_common::{EdgeId, NodeId};
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
    vector_index: Vec<NodeId>,
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
    pub vector_candidate_count: usize,
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
        let mut index = Self::default();

        for node in nodes {
            index.upsert_node(node);
        }

        for edge in edges {
            index.upsert_edge(edge);
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
        if node.embedding().is_some() {
            push_unique(&mut self.vector_index, node.id.clone());
        }

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
        self.vector_index.retain(|node_id| node_id != &node.id);
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

    pub fn vector_candidate_ids(&self) -> &[NodeId] {
        &self.vector_index
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
            vector_candidate_count: self.vector_index.len(),
        }
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

    use super::{EdgeDirection, GraphIndex, IndexCatalog, IndexName};
    use undr9_common::{EdgeId, NodeId};
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
        node_a.properties.insert(
            "embedding".to_owned(),
            PropertyValue::FloatList(vec![0.1, 0.2]),
        );
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
        assert_eq!(index.vector_candidate_ids().len(), 1);
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
        node_a.properties.insert(
            "embedding".to_owned(),
            PropertyValue::FloatList(vec![0.1, 0.2]),
        );

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
        assert_eq!(incremental.vector_candidate_ids().len(), 0);
    }
}
