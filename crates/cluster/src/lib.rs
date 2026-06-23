use serde::{Deserialize, Serialize};
use undr9_common::{Result, Undr9Error};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    Primary,
    Replica,
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNode {
    pub node_id: String,
    pub address: String,
    pub role: NodeRole,
    pub healthy: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterTopology {
    pub term: u64,
    pub leader_node_id: Option<String>,
    pub nodes: Vec<ClusterNode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverPlan {
    pub old_leader_node_id: Option<String>,
    pub new_leader_node_id: String,
    pub term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterManager {
    topology: ClusterTopology,
}

impl ClusterTopology {
    pub fn single_node_with_id(node_id: impl Into<String>, address: impl Into<String>) -> Self {
        let node_id = node_id.into();
        Self {
            term: 1,
            leader_node_id: Some(node_id.clone()),
            nodes: vec![ClusterNode {
                node_id,
                address: address.into(),
                role: NodeRole::Primary,
                healthy: true,
            }],
        }
    }

    pub fn single_node(address: impl Into<String>) -> Self {
        Self::single_node_with_id("node-1", address)
    }
}

impl ClusterManager {
    pub fn single_node_with_id(node_id: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            topology: ClusterTopology::single_node_with_id(node_id, address),
        }
    }

    pub fn single_node(address: impl Into<String>) -> Self {
        Self::single_node_with_id("node-1", address)
    }

    pub fn topology(&self) -> &ClusterTopology {
        &self.topology
    }

    pub fn add_replica(
        &mut self,
        node_id: impl Into<String>,
        address: impl Into<String>,
    ) -> Result<()> {
        let node_id = node_id.into();
        if self
            .topology
            .nodes
            .iter()
            .any(|node| node.node_id == node_id)
        {
            return Err(Undr9Error::Conflict(format!(
                "cluster node '{}' already exists",
                node_id
            )));
        }

        self.topology.nodes.push(ClusterNode {
            node_id,
            address: address.into(),
            role: NodeRole::Replica,
            healthy: true,
        });
        self.topology
            .nodes
            .sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(())
    }

    pub fn upsert_node(
        &mut self,
        node_id: impl Into<String>,
        address: impl Into<String>,
        role: NodeRole,
        healthy: bool,
    ) {
        let node_id = node_id.into();
        let address = address.into();
        if let Some(node) = self
            .topology
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
        {
            node.address = address;
            node.role = role;
            node.healthy = healthy;
        } else {
            self.topology.nodes.push(ClusterNode {
                node_id,
                address,
                role,
                healthy,
            });
            self.topology
                .nodes
                .sort_by(|left, right| left.node_id.cmp(&right.node_id));
        }
    }

    pub fn mark_node_health(&mut self, node_id: &str, healthy: bool) -> Result<()> {
        let node = self
            .topology
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| {
                Undr9Error::NotFound(format!("cluster node '{node_id}' was not found"))
            })?;
        node.healthy = healthy;
        Ok(())
    }

    pub fn promote_node(&mut self, node_id: &str) -> Result<FailoverPlan> {
        let old_leader = self.topology.leader_node_id.clone();
        let mut found = false;
        for node in &mut self.topology.nodes {
            if node.node_id == node_id {
                node.role = NodeRole::Primary;
                node.healthy = true;
                found = true;
            } else if node.role == NodeRole::Primary {
                node.role = NodeRole::Replica;
            }
        }
        if !found {
            return Err(Undr9Error::NotFound(format!(
                "cluster node '{node_id}' was not found"
            )));
        }

        self.topology.term += 1;
        self.topology.leader_node_id = Some(node_id.to_owned());
        Ok(FailoverPlan {
            old_leader_node_id: old_leader,
            new_leader_node_id: node_id.to_owned(),
            term: self.topology.term,
        })
    }

    pub fn ensure_leader(&mut self, node_id: &str) -> Result<FailoverPlan> {
        let old_leader = self.topology.leader_node_id.clone();
        let mut found = false;
        for node in &mut self.topology.nodes {
            if node.node_id == node_id {
                node.role = NodeRole::Primary;
                node.healthy = true;
                found = true;
            } else if node.role == NodeRole::Primary {
                node.role = NodeRole::Replica;
            }
        }
        if !found {
            return Err(Undr9Error::NotFound(format!(
                "cluster node '{node_id}' was not found"
            )));
        }
        if old_leader.as_deref() != Some(node_id) {
            self.topology.term += 1;
        }
        self.topology.leader_node_id = Some(node_id.to_owned());
        Ok(FailoverPlan {
            old_leader_node_id: old_leader,
            new_leader_node_id: node_id.to_owned(),
            term: self.topology.term,
        })
    }

    pub fn observe_leader(&mut self, node_id: &str) -> Result<()> {
        let mut found = false;
        for node in &mut self.topology.nodes {
            if node.node_id == node_id {
                node.role = NodeRole::Primary;
                node.healthy = true;
                found = true;
            } else if node.role == NodeRole::Primary {
                node.role = NodeRole::Replica;
            }
        }
        if !found {
            return Err(Undr9Error::NotFound(format!(
                "cluster node '{node_id}' was not found"
            )));
        }
        self.topology.leader_node_id = Some(node_id.to_owned());
        Ok(())
    }

    pub fn leader(&self) -> Option<&ClusterNode> {
        self.topology.leader_node_id.as_ref().and_then(|leader_id| {
            self.topology
                .nodes
                .iter()
                .find(|node| &node.node_id == leader_id)
        })
    }

    pub fn readable_replicas(&self) -> Vec<&ClusterNode> {
        self.topology
            .nodes
            .iter()
            .filter(|node| node.role == NodeRole::Replica && node.healthy)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterManager, NodeRole};

    #[test]
    fn single_node_topology_is_primary_only() {
        let topology = ClusterManager::single_node("127.0.0.1:7000");
        assert_eq!(topology.topology().nodes.len(), 1);
        assert_eq!(topology.topology().nodes[0].role, NodeRole::Primary);
    }

    #[test]
    fn promote_node_updates_leader_and_term() {
        let mut cluster = ClusterManager::single_node("127.0.0.1:7000");
        cluster
            .add_replica("node-2", "127.0.0.1:7001")
            .expect("replica should add");
        let plan = cluster
            .promote_node("node-2")
            .expect("promotion should work");

        assert_eq!(plan.new_leader_node_id, "node-2");
        assert_eq!(cluster.topology().leader_node_id.as_deref(), Some("node-2"));
        assert_eq!(cluster.topology().term, 2);
    }

    #[test]
    fn ensure_leader_is_idempotent_for_same_node() {
        let mut cluster = ClusterManager::single_node("127.0.0.1:7000");

        let plan = cluster
            .ensure_leader("node-1")
            .expect("leader should remain stable");

        assert_eq!(plan.new_leader_node_id, "node-1");
        assert_eq!(cluster.topology().leader_node_id.as_deref(), Some("node-1"));
        assert_eq!(cluster.topology().term, 1);
    }

    #[test]
    fn observe_leader_updates_topology_without_bumping_term() {
        let mut cluster = ClusterManager::single_node("127.0.0.1:7000");
        cluster
            .add_replica("node-2", "127.0.0.1:7001")
            .expect("replica should add");

        cluster
            .observe_leader("node-2")
            .expect("leader observation should work");

        assert_eq!(cluster.topology().leader_node_id.as_deref(), Some("node-2"));
        assert_eq!(cluster.topology().term, 1);
    }
}
