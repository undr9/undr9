use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use undr9_common::Undr9Error;
use undr9_core::WriteBatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationMode {
    Disabled,
    Leader,
    Follower,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaProgress {
    pub replica_node_id: String,
    pub last_acked_source_lsn: u64,
    pub last_applied_source_lsn: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicationRecord {
    pub source_node_id: String,
    pub source_term: u64,
    pub source_lsn: u64,
    pub batch: WriteBatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub mode: ReplicationMode,
    pub local_node_id: String,
    pub leader_node_id: Option<String>,
    pub current_term: u64,
    pub last_applied_lsn: u64,
    pub last_committed_source_lsn: u64,
    pub last_pulled_source_lsn: u64,
    pub last_applied_source_lsn: u64,
    pub replicas: Vec<ReplicaProgress>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicationManager {
    status: ReplicationStatus,
    history: Vec<ReplicationRecord>,
}

impl ReplicationStatus {
    pub fn single_node(local_node_id: impl Into<String>) -> Self {
        Self {
            mode: ReplicationMode::Disabled,
            local_node_id: local_node_id.into(),
            leader_node_id: None,
            current_term: 0,
            last_applied_lsn: 0,
            last_committed_source_lsn: 0,
            last_pulled_source_lsn: 0,
            last_applied_source_lsn: 0,
            replicas: Vec::new(),
        }
    }

    pub fn single_node_default() -> Self {
        Self::single_node("node-1")
    }

    pub fn replica_lag(&self, replica_node_id: &str) -> Option<u64> {
        self.replicas
            .iter()
            .find(|replica| replica.replica_node_id == replica_node_id)
            .map(|replica| {
                self.last_committed_source_lsn
                    .saturating_sub(replica.last_acked_source_lsn)
            })
    }
}

impl ReplicationManager {
    pub fn single_node(local_node_id: impl Into<String>) -> Self {
        Self {
            status: ReplicationStatus::single_node(local_node_id),
            history: Vec::new(),
        }
    }

    pub fn single_node_default() -> Self {
        Self::single_node("node-1")
    }

    pub fn status(&self) -> &ReplicationStatus {
        &self.status
    }

    pub fn history_since(&self, after_source_lsn: u64) -> Vec<ReplicationRecord> {
        self.history
            .iter()
            .filter(|record| record.source_lsn > after_source_lsn)
            .cloned()
            .collect()
    }

    pub fn configure_as_leader(&mut self, local_node_id: impl Into<String>, current_term: u64) {
        let local_node_id = local_node_id.into();
        self.status.mode = ReplicationMode::Leader;
        self.status.local_node_id = local_node_id.clone();
        self.status.leader_node_id = Some(local_node_id);
        self.status.current_term = current_term;
    }

    pub fn configure_as_follower(
        &mut self,
        local_node_id: impl Into<String>,
        leader_node_id: impl Into<String>,
        current_term: u64,
    ) {
        self.status.mode = ReplicationMode::Follower;
        self.status.local_node_id = local_node_id.into();
        self.status.leader_node_id = Some(leader_node_id.into());
        self.status.current_term = current_term;
    }

    pub fn disable(&mut self, local_node_id: impl Into<String>) {
        self.status.mode = ReplicationMode::Disabled;
        self.status.local_node_id = local_node_id.into();
        self.status.leader_node_id = None;
        self.status.replicas.clear();
        self.history.clear();
    }

    pub fn observe_local_apply(&mut self, local_lsn: u64) {
        self.status.last_applied_lsn = local_lsn;
    }

    pub fn register_replica(
        &mut self,
        replica_node_id: impl Into<String>,
    ) -> Result<(), Undr9Error> {
        ensure_leader(&self.status)?;
        let replica_node_id = replica_node_id.into();
        if self
            .status
            .replicas
            .iter()
            .any(|replica| replica.replica_node_id == replica_node_id)
        {
            return Ok(());
        }

        self.status.replicas.push(ReplicaProgress {
            replica_node_id,
            last_acked_source_lsn: 0,
            last_applied_source_lsn: 0,
        });
        self.status
            .replicas
            .sort_by(|left, right| left.replica_node_id.cmp(&right.replica_node_id));
        Ok(())
    }

    pub fn record_leader_commit(
        &mut self,
        source_lsn: u64,
        batch: WriteBatch,
    ) -> Result<(), Undr9Error> {
        ensure_leader(&self.status)?;
        self.status.last_committed_source_lsn = source_lsn;
        self.status.last_applied_lsn = source_lsn;
        self.status.last_applied_source_lsn = source_lsn;
        self.history.push(ReplicationRecord {
            source_node_id: self.status.local_node_id.clone(),
            source_term: self.status.current_term,
            source_lsn,
            batch,
        });
        Ok(())
    }

    pub fn acknowledge_replica(
        &mut self,
        replica_node_id: &str,
        source_lsn: u64,
    ) -> Result<(), Undr9Error> {
        ensure_leader(&self.status)?;
        let replica = self
            .status
            .replicas
            .iter_mut()
            .find(|replica| replica.replica_node_id == replica_node_id)
            .ok_or_else(|| {
                Undr9Error::NotFound(format!("replica '{replica_node_id}' is not registered"))
            })?;
        replica.last_acked_source_lsn = source_lsn;
        replica.last_applied_source_lsn = source_lsn;
        Ok(())
    }

    pub fn apply_follower_records(
        &mut self,
        records: &[ReplicationRecord],
    ) -> Result<Option<u64>, Undr9Error> {
        ensure_follower(&self.status)?;
        let mut last_applied_source_lsn = None;
        for record in records {
            if record.source_term < self.status.current_term {
                return Err(Undr9Error::Conflict(format!(
                    "replication term regression: follower term {} leader term {}",
                    self.status.current_term, record.source_term
                )));
            }
            if record.source_lsn <= self.status.last_applied_source_lsn {
                continue;
            }
            self.status.current_term = record.source_term;
            self.status.last_pulled_source_lsn = record.source_lsn;
            self.status.last_applied_source_lsn = record.source_lsn;
            last_applied_source_lsn = Some(record.source_lsn);
        }
        Ok(last_applied_source_lsn)
    }

    pub fn promote_to_leader(&mut self, new_term: u64) {
        let local_node_id = self.status.local_node_id.clone();
        self.status.mode = ReplicationMode::Leader;
        self.status.leader_node_id = Some(local_node_id);
        self.status.current_term = new_term;
        self.status.last_committed_source_lsn = self.status.last_applied_source_lsn;
    }

    pub fn failover_to_follower(&mut self, leader_node_id: impl Into<String>, new_term: u64) {
        self.status.mode = ReplicationMode::Follower;
        self.status.leader_node_id = Some(leader_node_id.into());
        self.status.current_term = new_term;
    }

    pub fn replica_lag_map(&self) -> BTreeMap<String, u64> {
        self.status
            .replicas
            .iter()
            .map(|replica| {
                (
                    replica.replica_node_id.clone(),
                    self.status
                        .last_committed_source_lsn
                        .saturating_sub(replica.last_acked_source_lsn),
                )
            })
            .collect()
    }
}

fn ensure_leader(status: &ReplicationStatus) -> Result<(), Undr9Error> {
    if status.mode != ReplicationMode::Leader {
        return Err(Undr9Error::Conflict(
            "replication operation requires leader mode".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_follower(status: &ReplicationStatus) -> Result<(), Undr9Error> {
    if status.mode != ReplicationMode::Follower {
        return Err(Undr9Error::Conflict(
            "replication operation requires follower mode".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ReplicationManager, ReplicationMode};
    use undr9_common::NodeId;
    use undr9_core::{NodeRecord, WriteBatch};

    #[test]
    fn single_node_default_disables_replication() {
        let status = super::ReplicationStatus::single_node_default();
        assert_eq!(status.mode, ReplicationMode::Disabled);
    }

    #[test]
    fn leader_records_commits_and_replica_ack_updates_lag() {
        let mut manager = ReplicationManager::single_node_default();
        manager.configure_as_leader("leader-1", 2);
        manager
            .register_replica("replica-1")
            .expect("replica should register");
        manager
            .record_leader_commit(
                7,
                WriteBatch {
                    nodes_upserted: vec![NodeRecord::new(
                        NodeId::new("node_a").expect("valid node id"),
                        "memory",
                    )
                    .expect("node should build")],
                    ..WriteBatch::default()
                },
            )
            .expect("commit should record");
        assert_eq!(manager.history_since(0).len(), 1);
        assert_eq!(manager.replica_lag_map().get("replica-1"), Some(&7));

        manager
            .acknowledge_replica("replica-1", 7)
            .expect("ack should update");
        assert_eq!(manager.replica_lag_map().get("replica-1"), Some(&0));
    }

    #[test]
    fn follower_tracks_last_applied_source_lsn_and_can_promote() {
        let mut manager = ReplicationManager::single_node_default();
        manager.configure_as_follower("replica-1", "leader-1", 1);
        manager
            .apply_follower_records(&[super::ReplicationRecord {
                source_node_id: "leader-1".to_owned(),
                source_term: 1,
                source_lsn: 8,
                batch: WriteBatch::default(),
            }])
            .expect("follower should apply");
        assert_eq!(manager.status().last_applied_source_lsn, 8);

        manager.promote_to_leader(2);
        assert_eq!(manager.status().mode, ReplicationMode::Leader);
        assert_eq!(
            manager.status().leader_node_id.as_deref(),
            Some("replica-1")
        );
    }
}
