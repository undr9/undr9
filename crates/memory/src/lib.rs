use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use undr9_common::NodeId;
use undr9_core::{EdgeRecord, NodeRecord, PropertyValue, WriteBatch};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ScoreBreakdown {
    pub structural: f32,
    pub semantic: f32,
    pub temporal: f32,
    pub importance: f32,
    pub confidence: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RankingWeights {
    pub structural: f32,
    pub semantic: f32,
    pub temporal: f32,
    pub importance: f32,
    pub confidence: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetrievalProfile {
    pub name: String,
    pub weights: RankingWeights,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsolidationAction {
    Merge,
    Link,
    Demote,
    Archive,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsolidationEvent {
    pub event_id: String,
    pub action: ConsolidationAction,
    pub target_node_id: NodeId,
    pub related_node_ids: Vec<NodeId>,
    pub detail: String,
    pub reverse_batch: WriteBatch,
}

pub struct MemoryRanker;
pub struct MemoryConsolidator;

impl Default for RankingWeights {
    fn default() -> Self {
        Self {
            structural: 0.30,
            semantic: 0.30,
            temporal: 0.15,
            importance: 0.15,
            confidence: 0.10,
        }
    }
}

impl RetrievalProfile {
    pub fn v1_default() -> Self {
        Self {
            name: "v1-default".to_owned(),
            weights: RankingWeights::default(),
        }
    }
}

impl ScoreBreakdown {
    pub fn total(&self, weights: RankingWeights) -> f32 {
        (self.structural * weights.structural)
            + (self.semantic * weights.semantic)
            + (self.temporal * weights.temporal)
            + (self.importance * weights.importance)
            + (self.confidence * weights.confidence)
    }
}

impl MemoryRanker {
    pub fn rank(score: ScoreBreakdown, weights: RankingWeights) -> f32 {
        score.total(weights)
    }

    pub fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f32> {
        if left.is_empty() || left.len() != right.len() {
            return None;
        }

        let dot_product = left
            .iter()
            .zip(right.iter())
            .map(|(left, right)| left * right)
            .sum::<f32>();
        let left_norm = left.iter().map(|value| value * value).sum::<f32>().sqrt();
        let right_norm = right.iter().map(|value| value * value).sum::<f32>().sqrt();

        if left_norm == 0.0 || right_norm == 0.0 {
            return None;
        }

        Some(((dot_product / (left_norm * right_norm)) + 1.0) / 2.0)
    }

    pub fn temporal_recency_score(timestamp_ms: i64, now_epoch_ms: i64) -> f32 {
        if timestamp_ms >= now_epoch_ms {
            return 1.0;
        }

        let age_ms = (now_epoch_ms - timestamp_ms) as f32;
        let half_life_ms = 7.0 * 24.0 * 60.0 * 60.0 * 1000.0;
        1.0 / (1.0 + (age_ms / half_life_ms))
    }

    pub fn normalize_signal(value: Option<f32>) -> f32 {
        value.unwrap_or(0.5).clamp(0.0, 1.0)
    }
}

impl MemoryConsolidator {
    pub fn analyze(
        nodes: &[NodeRecord],
        edges: &[EdgeRecord],
        now_epoch_ms: i64,
    ) -> Vec<ConsolidationEvent> {
        let incident_node_ids = edges.iter().fold(BTreeSet::new(), |mut ids, edge| {
            ids.insert(edge.source.clone());
            ids.insert(edge.target.clone());
            ids
        });
        let mut events = Vec::new();
        let mut duplicate_groups = BTreeMap::<(String, String), Vec<&NodeRecord>>::new();

        for node in nodes {
            if let Some(PropertyValue::String(unique_key)) = node.property("unique_key") {
                duplicate_groups
                    .entry((node.node_type.clone(), unique_key.clone()))
                    .or_default()
                    .push(node);
            }
        }

        for ((node_type, unique_key), group) in duplicate_groups {
            if group.len() < 2 {
                continue;
            }

            let primary_id = group
                .iter()
                .min_by_key(|node| node.id.as_str())
                .expect("group is non-empty")
                .id
                .clone();
            let duplicate_ids = group
                .into_iter()
                .filter(|node| node.id != primary_id)
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();

            if duplicate_ids.is_empty() {
                continue;
            }

            let reverse_nodes = duplicate_ids
                .iter()
                .filter_map(|node_id| nodes.iter().find(|node| node.id == *node_id).cloned())
                .collect();
            events.push(ConsolidationEvent {
                event_id: format!("merge:{}", primary_id),
                action: ConsolidationAction::Merge,
                target_node_id: primary_id.clone(),
                related_node_ids: duplicate_ids,
                detail: format!("merged duplicate memory nodes for {node_type}:{unique_key}"),
                reverse_batch: WriteBatch {
                    nodes_upserted: reverse_nodes,
                    ..WriteBatch::default()
                },
            });
        }

        for node in nodes {
            if !incident_node_ids.contains(&node.id) {
                events.push(ConsolidationEvent {
                    event_id: format!("demote:{}", node.id),
                    action: ConsolidationAction::Demote,
                    target_node_id: node.id.clone(),
                    related_node_ids: Vec::new(),
                    detail: "demoted isolated memory".to_owned(),
                    reverse_batch: WriteBatch {
                        nodes_upserted: vec![node.clone()],
                        ..WriteBatch::default()
                    },
                });
            }

            if let Some(timestamp_ms) = node.timestamp_ms() {
                let stale_ms = 30_i64 * 24 * 60 * 60 * 1000;
                if (now_epoch_ms - timestamp_ms) > stale_ms {
                    events.push(ConsolidationEvent {
                        event_id: format!("archive:{}", node.id),
                        action: ConsolidationAction::Archive,
                        target_node_id: node.id.clone(),
                        related_node_ids: Vec::new(),
                        detail: "archived stale memory".to_owned(),
                        reverse_batch: WriteBatch {
                            nodes_upserted: vec![node.clone()],
                            ..WriteBatch::default()
                        },
                    });
                }
            }
        }

        events
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConsolidationAction, MemoryConsolidator, MemoryRanker, RankingWeights, RetrievalProfile,
        ScoreBreakdown,
    };
    use std::collections::BTreeMap;
    use undr9_common::{EdgeId, NodeId};
    use undr9_core::{EdgeRecord, NodeRecord, PropertyValue};

    #[test]
    fn ranker_combines_weighted_scores() {
        let total = MemoryRanker::rank(
            ScoreBreakdown {
                structural: 1.0,
                semantic: 0.5,
                temporal: 0.25,
                importance: 0.75,
                confidence: 1.0,
            },
            RankingWeights::default(),
        );

        assert!(total > 0.0);
        assert!(total <= 1.0);
    }

    #[test]
    fn computes_cosine_similarity() {
        let similarity =
            MemoryRanker::cosine_similarity(&[1.0, 0.0], &[0.5, 0.5]).expect("similarity");
        assert!(similarity > 0.5);
        assert!(similarity <= 1.0);
    }

    #[test]
    fn recency_score_favors_recent_timestamps() {
        let recent = MemoryRanker::temporal_recency_score(1_000, 1_100);
        let stale = MemoryRanker::temporal_recency_score(1_000, 10_000_000);

        assert!(recent > stale);
        assert!(recent <= 1.0);
    }

    #[test]
    fn exposes_default_retrieval_profile() {
        let profile = RetrievalProfile::v1_default();
        assert_eq!(profile.name, "v1-default");
        assert_eq!(profile.weights.semantic, 0.30);
    }

    #[test]
    fn consolidation_finds_duplicate_and_stale_memories() {
        let mut node_a =
            NodeRecord::new(NodeId::new("tenant_a:node_a").expect("valid id"), "Memory")
                .expect("node");
        node_a.properties.insert(
            "unique_key".to_owned(),
            PropertyValue::String("same".to_owned()),
        );
        node_a
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(1_000));

        let mut node_b =
            NodeRecord::new(NodeId::new("tenant_a:node_b").expect("valid id"), "Memory")
                .expect("node");
        node_b.properties.insert(
            "unique_key".to_owned(),
            PropertyValue::String("same".to_owned()),
        );
        node_b
            .properties
            .insert("timestamp".to_owned(), PropertyValue::Integer(1_100));

        let edge = EdgeRecord {
            id: EdgeId::new("edge_ab").expect("valid edge id"),
            source: node_a.id.clone(),
            target: node_b.id.clone(),
            edge_type: "RELATED_TO".to_owned(),
            properties: BTreeMap::new(),
        };

        let events =
            MemoryConsolidator::analyze(&[node_a, node_b], &[edge], 40 * 24 * 60 * 60 * 1000);
        assert!(events
            .iter()
            .any(|event| event.action == ConsolidationAction::Merge));
        assert!(events
            .iter()
            .any(|event| event.action == ConsolidationAction::Archive));
    }
}
