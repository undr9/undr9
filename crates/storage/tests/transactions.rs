use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use undr9_common::NodeId;
use undr9_config::AppConfig;
use undr9_core::{IsolationLevel, NodeRecord, PropertyValue, TransactionOperation};
use undr9_storage::StorageEngine;

static TEST_ENGINE_COUNTER: AtomicU64 = AtomicU64::new(1);

fn test_engine() -> StorageEngine {
    let mut config = AppConfig::default();
    let ordinal = TEST_ENGINE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let process_id = std::process::id();
    config.storage.root_dir =
        std::env::temp_dir().join(format!("undr9-storage-tx-{process_id}-{ordinal}"));
    StorageEngine::open(&config).expect("storage engine should open")
}

#[test]
fn commits_multi_operation_transaction_atomically() {
    let mut engine = test_engine();
    let summary = engine.begin_transaction(IsolationLevel::Snapshot);

    let node_a = NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
        .expect("node should build");
    let node_b = NodeRecord::new(NodeId::new("node_b").expect("valid node id"), "memory")
        .expect("node should build");
    let edge = undr9_core::EdgeRecord::new(
        undr9_common::EdgeId::new("edge_ab").expect("valid edge id"),
        node_a.id.clone(),
        node_b.id.clone(),
        "relates_to",
    )
    .expect("edge should build");

    for operation in [
        TransactionOperation::UpsertNode(node_a.clone()),
        TransactionOperation::UpsertNode(node_b.clone()),
        TransactionOperation::UpsertEdge(edge.clone()),
    ] {
        engine
            .stage_transaction_operation(&summary.transaction_id, operation)
            .expect("operation should stage");
    }

    let commit = engine
        .commit_transaction(&summary.transaction_id)
        .expect("transaction should commit");

    assert!(commit.committed_lsn > 0);
    assert!(engine.get_node(&node_a.id).is_some());
    assert!(engine.get_node(&node_b.id).is_some());
    assert!(engine.get_edge(&edge.id).is_some());
}

#[test]
fn rollback_discards_staged_changes() {
    let mut engine = test_engine();
    let summary = engine.begin_transaction(IsolationLevel::Snapshot);
    let node = NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
        .expect("node should build");

    engine
        .stage_transaction_operation(
            &summary.transaction_id,
            TransactionOperation::UpsertNode(node.clone()),
        )
        .expect("operation should stage");
    let rollback = engine
        .rollback_transaction(&summary.transaction_id)
        .expect("rollback should work");

    assert_eq!(rollback.state, undr9_core::TransactionState::RolledBack);
    assert!(engine.get_node(&node.id).is_none());
}

#[test]
fn snapshot_reads_remain_stable_and_conflicts_on_concurrent_write() {
    let engine = Arc::new(Mutex::new(test_engine()));
    {
        let mut engine = engine.lock().expect("mutex should not be poisoned");
        let initial = NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
            .expect("node should build")
            .with_property("value", PropertyValue::Integer(1))
            .expect("property should build");
        engine
            .upsert_node(initial)
            .expect("initial node should insert");
    }

    let transaction_id = {
        let mut engine = engine.lock().expect("mutex should not be poisoned");
        engine
            .begin_transaction(IsolationLevel::Snapshot)
            .transaction_id
    };

    {
        let snapshot_value = engine
            .lock()
            .expect("mutex should not be poisoned")
            .transaction_node(
                &transaction_id,
                &NodeId::new("node_a").expect("valid node id"),
            )
            .expect("transaction read should work")
            .expect("node should exist")
            .property("value")
            .and_then(PropertyValue::as_i64);
        assert_eq!(snapshot_value, Some(1));
    }

    let writer = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            let mut engine = engine.lock().expect("mutex should not be poisoned");
            let summary = engine.begin_transaction(IsolationLevel::Snapshot);
            let updated = NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
                .expect("node should build")
                .with_property("value", PropertyValue::Integer(2))
                .expect("property should build");
            engine
                .stage_transaction_operation(
                    &summary.transaction_id,
                    TransactionOperation::UpsertNode(updated),
                )
                .expect("writer should stage");
            engine
                .commit_transaction(&summary.transaction_id)
                .expect("writer should commit");
        })
    };
    writer.join().expect("writer thread should finish");

    let stale_read_value = engine
        .lock()
        .expect("mutex should not be poisoned")
        .transaction_node(
            &transaction_id,
            &NodeId::new("node_a").expect("valid node id"),
        )
        .expect("transaction read should work")
        .expect("node should exist")
        .property("value")
        .and_then(PropertyValue::as_i64);
    assert_eq!(stale_read_value, Some(1));

    {
        let mut engine = engine.lock().expect("mutex should not be poisoned");
        engine
            .stage_transaction_operation(
                &transaction_id,
                TransactionOperation::UpsertNode(
                    NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
                        .expect("node should build")
                        .with_property("value", PropertyValue::Integer(3))
                        .expect("property should build"),
                ),
            )
            .expect("staging on stale transaction should work");
    }
    let conflict = engine
        .lock()
        .expect("mutex should not be poisoned")
        .commit_transaction(&transaction_id)
        .expect_err("commit should conflict");
    assert!(conflict.to_string().contains("changed after transaction"));
}

#[test]
fn snapshot_reads_handle_delete_and_recreate_of_same_node_id() {
    let mut engine = test_engine();
    let original = NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
        .expect("node should build")
        .with_property("value", PropertyValue::Integer(1))
        .expect("property should build");
    engine
        .upsert_node(original)
        .expect("initial node should insert");

    let tx_before_delete = engine.begin_transaction(IsolationLevel::Snapshot).transaction_id;

    engine
        .delete_node(&NodeId::new("node_a").expect("valid node id"))
        .expect("delete should succeed");

    let tx_after_delete = engine.begin_transaction(IsolationLevel::Snapshot).transaction_id;

    let recreated = NodeRecord::new(NodeId::new("node_a").expect("valid node id"), "memory")
        .expect("node should build")
        .with_property("value", PropertyValue::Integer(2))
        .expect("property should build");
    engine
        .upsert_node(recreated)
        .expect("recreated node should insert");

    let before_delete_value = engine
        .transaction_node(&tx_before_delete, &NodeId::new("node_a").expect("valid node id"))
        .expect("snapshot read should work")
        .expect("node should exist before delete")
        .property("value")
        .and_then(PropertyValue::as_i64);
    assert_eq!(before_delete_value, Some(1));

    let after_delete_value = engine
        .transaction_node(&tx_after_delete, &NodeId::new("node_a").expect("valid node id"))
        .expect("snapshot read should work");
    assert!(after_delete_value.is_none());
}
