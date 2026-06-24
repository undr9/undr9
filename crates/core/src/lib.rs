use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use undr9_common::{EdgeId, NodeId, Result, TransactionId, Undr9Error};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
pub enum PropertyValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    StringList(Vec<String>),
    FloatList(Vec<f32>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: NodeId,
    pub node_type: String,
    pub properties: BTreeMap<String, PropertyValue>,
    #[serde(default)]
    pub vectors: BTreeMap<String, Vec<f32>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeRecord {
    pub id: EdgeId,
    pub source: NodeId,
    pub target: NodeId,
    pub edge_type: String,
    pub properties: BTreeMap<String, PropertyValue>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WriteBatch {
    pub nodes_upserted: Vec<NodeRecord>,
    pub edges_upserted: Vec<EdgeRecord>,
    pub deleted_node_ids: Vec<NodeId>,
    pub deleted_edge_ids: Vec<EdgeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransactionOperation {
    UpsertNode(NodeRecord),
    UpsertEdge(EdgeRecord),
    DeleteNode { node_id: NodeId },
    DeleteEdge { edge_id: EdgeId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionSummary {
    pub transaction_id: TransactionId,
    pub isolation_level: IsolationLevel,
    pub state: TransactionState,
    pub started_at_revision: u64,
    pub staged_operation_count: usize,
    pub touched_node_count: usize,
    pub touched_edge_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionCommitResult {
    pub transaction_id: TransactionId,
    pub committed_revision: u64,
    pub committed_lsn: u64,
    pub staged_operation_count: usize,
}

impl NodeRecord {
    pub fn new(id: NodeId, node_type: impl Into<String>) -> Result<Self> {
        let node_type = node_type.into();
        validate_kind("node_type", &node_type)?;

        Ok(Self {
            id,
            node_type,
            properties: BTreeMap::new(),
            vectors: BTreeMap::new(),
        })
    }

    pub fn with_property(mut self, key: impl Into<String>, value: PropertyValue) -> Result<Self> {
        let key = key.into();
        validate_property_key(&key)?;
        self.properties.insert(key, value);
        Ok(self)
    }

    pub fn with_vector(mut self, name: impl Into<String>, values: Vec<f32>) -> Result<Self> {
        let name = name.into();
        validate_property_key(&name)?;
        self.vectors.insert(name, values);
        Ok(self)
    }

    pub fn property(&self, key: &str) -> Option<&PropertyValue> {
        self.properties.get(key)
    }

    pub fn namespace(&self) -> Option<&str> {
        self.id
            .as_str()
            .split_once(':')
            .map(|(namespace, _)| namespace)
    }

    pub fn vector(&self, name: &str) -> Option<&[f32]> {
        self.vectors.get(name).map(Vec::as_slice)
    }

    pub fn embedding(&self) -> Option<&[f32]> {
        self.vector("default")
    }

    pub fn timestamp_ms(&self) -> Option<i64> {
        self.property("timestamp").and_then(PropertyValue::as_i64)
    }

    pub fn importance(&self) -> Option<f32> {
        self.property("importance").and_then(PropertyValue::as_f32)
    }

    pub fn confidence(&self) -> Option<f32> {
        self.property("confidence").and_then(PropertyValue::as_f32)
    }

    pub fn normalize_memory_metadata(&mut self) -> Result<()> {
        if self.properties.contains_key("embedding") {
            return Err(Undr9Error::Validation(
                "embedding is no longer supported in properties; send vectors.default or another named vector in the vectors map".to_owned(),
            ));
        }

        if let Some(timestamp) = self.properties.get("timestamp") {
            if timestamp.as_i64().is_none() {
                return Err(Undr9Error::Validation(
                    "timestamp must be an integer-compatible value".to_owned(),
                ));
            }
        }

        for field in ["importance", "confidence"] {
            if let Some(value) = self.properties.get(field) {
                if value.as_f32().is_none() {
                    return Err(Undr9Error::Validation(format!(
                        "{field} must be a float-compatible value"
                    )));
                }
            }
        }

        Ok(())
    }
}

impl EdgeRecord {
    pub fn new(
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        edge_type: impl Into<String>,
    ) -> Result<Self> {
        let edge_type = edge_type.into();
        validate_kind("edge_type", &edge_type)?;

        Ok(Self {
            id,
            source,
            target,
            edge_type,
            properties: BTreeMap::new(),
        })
    }
}

impl WriteBatch {
    pub fn is_empty(&self) -> bool {
        self.nodes_upserted.is_empty()
            && self.edges_upserted.is_empty()
            && self.deleted_node_ids.is_empty()
            && self.deleted_edge_ids.is_empty()
    }
}

impl PropertyValue {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(value) => Some(*value),
            Self::Float(value) => Some(*value as i64),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::Integer(value) => Some(*value as f32),
            Self::Float(value) => Some(*value as f32),
            _ => None,
        }
    }
}

fn validate_kind(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Undr9Error::Validation(format!("{field} cannot be empty")));
    }

    Ok(())
}

fn validate_property_key(key: &str) -> Result<()> {
    if key.trim().is_empty() {
        return Err(Undr9Error::Validation(
            "property key cannot be empty".to_owned(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use undr9_common::{EdgeId, NodeId};

    #[test]
    fn builds_node_records_with_properties() {
        let node = super::NodeRecord::new(NodeId::new("node_1").expect("valid node id"), "memory")
            .expect("node should be created")
            .with_property("title", super::PropertyValue::String("UNDR9".to_owned()))
            .expect("property should be accepted");

        assert_eq!(node.node_type, "memory");
        assert!(node.properties.contains_key("title"));
    }

    #[test]
    fn write_batch_reports_when_empty() {
        let batch = super::WriteBatch::default();
        assert!(batch.is_empty());
    }

    #[test]
    fn builds_edge_records() {
        let edge = super::EdgeRecord::new(
            EdgeId::new("edge_1").expect("valid edge id"),
            NodeId::new("source_1").expect("valid source id"),
            NodeId::new("target_1").expect("valid target id"),
            "related_to",
        )
        .expect("edge should be created");

        assert_eq!(edge.edge_type, "related_to");
    }

    #[test]
    fn exposes_retrieval_properties_through_helpers() {
        let node = super::NodeRecord::new(NodeId::new("node_1").expect("valid node id"), "memory")
            .expect("node should be created")
            .with_vector("default", vec![0.1, 0.2])
            .expect("vector should be accepted")
            .with_property("timestamp", super::PropertyValue::Integer(123))
            .expect("timestamp should be accepted")
            .with_property("importance", super::PropertyValue::Float(0.8))
            .expect("importance should be accepted")
            .with_property("confidence", super::PropertyValue::Integer(1))
            .expect("confidence should be accepted");

        assert_eq!(node.embedding(), Some([0.1_f32, 0.2_f32].as_slice()));
        assert_eq!(node.timestamp_ms(), Some(123));
        assert_eq!(node.importance(), Some(0.8));
        assert_eq!(node.confidence(), Some(1.0));
    }

    #[test]
    fn rejects_legacy_embedding_property() {
        let mut node = super::NodeRecord::new(
            NodeId::new("tenant_a:node_1").expect("valid node id"),
            "memory",
        )
        .expect("node should be created")
        .with_property("embedding", super::PropertyValue::FloatList(vec![0.4, 0.6]))
        .expect("property should be accepted");

        let error = node
            .normalize_memory_metadata()
            .expect_err("legacy embedding property should be rejected");
        assert!(error.to_string().contains("no longer supported"));
    }

    #[test]
    fn supports_multiple_named_vectors() {
        let node = super::NodeRecord::new(
            NodeId::new("tenant_a:node_1").expect("valid node id"),
            "memory",
        )
        .expect("node should be created")
        .with_vector("default", vec![0.4, 0.6])
        .expect("default vector should be accepted")
        .with_vector("unique_key", vec![0.9, 0.1])
        .expect("named vector should be accepted");

        assert_eq!(node.namespace(), Some("tenant_a"));
        assert_eq!(node.embedding(), Some([0.4_f32, 0.6_f32].as_slice()));
        assert_eq!(
            node.vector("unique_key"),
            Some([0.9_f32, 0.1_f32].as_slice())
        );
    }
}
