use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::sync::{Mutex, OnceLock};

use tempfile::tempdir;
use undr9_common::{EdgeId, NodeId};
use undr9_config::AppConfig;
use undr9_core::{EdgeRecord, NodeRecord, PropertyValue, WriteBatch};
use undr9_storage::{
    backup_directory, bootstrap, clear_storage_io_failpoint, install_storage_io_failpoint,
    load_manifest, persist_manifest, repair_storage, restore_directory,
    restore_directory_to_lsn, StorageEngine, BACKUP_MANIFEST_FILE_NAME,
};
use undr9_wal::{
    clear_wal_io_failpoint, install_wal_io_failpoint, Wal, WalRecordKind,
};

static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn failpoint_test_lock() -> &'static Mutex<()> {
    FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

struct RecoveryTestGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl Drop for RecoveryTestGuard {
    fn drop(&mut self) {
        clear_storage_io_failpoint();
        clear_wal_io_failpoint();
    }
}

fn recovery_test_guard() -> RecoveryTestGuard {
    RecoveryTestGuard {
        _lock: failpoint_test_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    }
}

#[test]
fn persists_crud_state_across_clean_restart() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node_a = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");
    let node_b = NodeRecord::new(NodeId::new("node_b").expect("valid id"), "memory")
        .expect("node should build");
    let edge = EdgeRecord::new(
        EdgeId::new("edge_ab").expect("valid id"),
        node_a.id.clone(),
        node_b.id.clone(),
        "relates_to",
    )
    .expect("edge should build");

    {
        let mut engine = StorageEngine::open(&config).expect("engine should open");
        engine
            .upsert_node(node_a.clone())
            .expect("first node should persist");
        engine
            .upsert_node(node_b.clone())
            .expect("second node should persist");
        engine
            .upsert_edge(edge.clone())
            .expect("edge should persist");
        engine.graceful_shutdown().expect("shutdown should persist");
        assert!(!engine.layout().node_segment_path().exists());
        let delta_files = fs::read_dir(engine.layout().delta_directory())
            .expect("delta directory should exist")
            .filter_map(|entry| entry.ok())
            .count();
        assert!(delta_files > 0);
    }

    {
        let engine = StorageEngine::open(&config).expect("engine should reopen");
        assert_eq!(engine.node_count(), 2);
        assert_eq!(engine.edge_count(), 1);
        assert_eq!(
            engine
                .get_node(&node_a.id)
                .expect("node should exist")
                .node_type,
            "memory"
        );
        assert_eq!(
            engine
                .get_edge(&edge.id)
                .expect("edge should exist")
                .edge_type,
            "relates_to"
        );
    }
}

#[test]
fn recovers_committed_wal_records_without_snapshot_flush() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let mut node_a = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");
    node_a
        .properties
        .insert("timestamp".to_owned(), PropertyValue::Integer(1_000));
    node_a
        .vectors
        .insert("default".to_owned(), vec![1.0, 0.0, 0.5]);
    let node_b = NodeRecord::new(NodeId::new("node_b").expect("valid id"), "memory")
        .expect("node should build");
    let edge = EdgeRecord::new(
        EdgeId::new("edge_ab").expect("valid id"),
        node_a.id.clone(),
        node_b.id.clone(),
        "relates_to",
    )
    .expect("edge should build");

    let (layout, _) = bootstrap(&config.storage).expect("storage should bootstrap");
    let mut wal = Wal::open(layout.subdirectory("wal"), &config.wal).expect("WAL should open");

    let node_batch = WriteBatch {
        nodes_upserted: vec![node_a.clone(), node_b.clone()],
        ..WriteBatch::default()
    };
    wal.append(
        WalRecordKind::WriteBatch,
        serde_json::to_vec(&node_batch).expect("node batch should serialize"),
    )
    .expect("node batch should append");

    let edge_batch = WriteBatch {
        edges_upserted: vec![edge.clone()],
        ..WriteBatch::default()
    };
    wal.append(
        WalRecordKind::WriteBatch,
        postcard::to_allocvec(&edge_batch).expect("edge batch should serialize"),
    )
    .expect("edge batch should append");

    let engine = StorageEngine::open(&config).expect("engine should recover from WAL");
    assert_eq!(engine.node_count(), 2);
    assert_eq!(engine.edge_count(), 1);
    assert_eq!(engine.manifest().last_applied_lsn, None);
    assert_eq!(engine.latest_applied_lsn(), Some(2));
    assert!(engine.needs_checkpoint());
    assert_eq!(engine.pending_checkpoint_count(), 2);
}

#[test]
fn normal_writes_do_not_publish_full_snapshots_before_checkpoint() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    let mut engine = StorageEngine::open(&config).expect("engine should open");
    engine
        .upsert_node(node.clone())
        .expect("write should be accepted");

    assert_eq!(engine.manifest().last_applied_lsn, None);
    assert_eq!(engine.latest_applied_lsn(), Some(1));
    assert!(engine.needs_checkpoint());
    assert!(!engine.layout().node_segment_path().exists());
    assert!(!engine.layout().edge_segment_path().exists());
    assert!(!engine.layout().vector_segment_path().exists());

    let recovered = StorageEngine::open(&config).expect("engine should recover from WAL");
    assert!(recovered.get_node(&node.id).is_some());
}

#[test]
fn invalid_write_batch_does_not_mutate_live_state() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node_a = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");
    let missing = NodeId::new("node_missing").expect("valid id");
    let invalid_edge = EdgeRecord::new(
        EdgeId::new("edge_invalid").expect("valid id"),
        node_a.id.clone(),
        missing,
        "relates_to",
    )
    .expect("edge should build");

    let mut engine = StorageEngine::open(&config).expect("engine should open");
    engine
        .upsert_node(node_a.clone())
        .expect("seed node should persist");

    let revision_before = engine.current_revision();
    let lsn_before = engine.latest_applied_lsn();

    let error = engine
        .upsert_edge(invalid_edge)
        .expect_err("invalid edge write should fail");
    assert!(error.to_string().contains("does not exist"));
    assert_eq!(engine.current_revision(), revision_before);
    assert_eq!(engine.latest_applied_lsn(), lsn_before);
    assert!(engine.get_node(&node_a.id).is_some());
    assert_eq!(engine.edge_count(), 0);
}

#[test]
fn corrupted_wal_is_reported_and_rejected_on_restart() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    {
        let mut engine = StorageEngine::open(&config).expect("engine should open");
        engine
            .upsert_node(node)
            .expect("write should succeed before corruption");
    }

    let (layout, _) = bootstrap(&config.storage).expect("storage should bootstrap");
    let wal_path = layout.subdirectory("wal").join("00000000000000000001.wal");
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&wal_path)
        .expect("wal segment should open");
    let len = file.metadata().expect("metadata should load").len();
    file.seek(SeekFrom::Start(len.saturating_sub(1)))
        .expect("seek should work");
    file.write_all(&[0x7f])
        .expect("corruption byte should write");

    let integrity = StorageEngine::open(&config)
        .err()
        .expect("corrupted WAL should fail reopen");
    assert!(
        integrity.to_string().contains("corruption")
            || integrity.to_string().contains("checksum")
            || integrity.to_string().contains("deserialize")
    );

    let report = repair_storage(&config).expect_err("repair should refuse corrupted wal");
    assert!(
        report.to_string().contains("corruption")
            || report.to_string().contains("checksum")
            || report.to_string().contains("deserialize")
    );
}

#[test]
fn repair_recovers_from_corrupted_delta_segment_using_wal() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    {
        let mut engine = StorageEngine::open(&config).expect("engine should open");
        engine
            .upsert_node(node.clone())
            .expect("write should succeed before recovery");
    }

    {
        let mut reopened = StorageEngine::open(&config).expect("engine should recover from WAL");
        assert!(reopened.get_node(&node.id).is_some());
        reopened
            .graceful_shutdown()
            .expect("explicit checkpoint should publish delta segment");
    }

    let (layout, _) = bootstrap(&config.storage).expect("storage should bootstrap");
    let delta_path = fs::read_dir(layout.delta_directory())
        .expect("delta directory should exist")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .next()
        .expect("recovery should have published a delta segment");
    fs::write(&delta_path, b"{not-valid-json").expect("delta corruption should be written");

    let open_error = StorageEngine::open(&config)
        .err()
        .expect("corrupted delta should fail normal open");
    assert!(!open_error.to_string().trim().is_empty());

    let report = repair_storage(&config).expect("repair should rebuild from wal despite bad delta");
    assert!(report.wal_replay_valid);

    let repaired = StorageEngine::open(&config).expect("engine should reopen after repair");
    assert!(repaired.get_node(&node.id).is_some());
}

#[test]
fn repair_recovers_from_malformed_manifest_using_wal() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    {
        let mut engine = StorageEngine::open(&config).expect("engine should open");
        engine
            .upsert_node(node.clone())
            .expect("write should succeed before corruption");
    }

    let (layout, _) = bootstrap(&config.storage).expect("storage should bootstrap");
    fs::write(layout.manifest_path(), b"{invalid-json").expect("manifest corruption should write");

    let open_error = StorageEngine::open(&config)
        .err()
        .expect("malformed manifest should fail normal open");
    assert!(open_error.to_string().contains("manifest"));

    let report = repair_storage(&config).expect("repair should recover from malformed manifest");
    assert!(report.wal_replay_valid);

    let repaired = StorageEngine::open(&config).expect("engine should reopen after repair");
    assert!(repaired.get_node(&node.id).is_some());
}

#[test]
fn repair_recovers_when_manifest_metadata_is_wrong_but_delta_is_valid() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node_a = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");
    let node_b = NodeRecord::new(NodeId::new("node_b").expect("valid id"), "memory")
        .expect("node should build");

    {
        let mut engine = StorageEngine::open(&config).expect("engine should open");
        engine
            .upsert_node(node_a.clone())
            .expect("first write should succeed");
        engine
            .graceful_shutdown()
            .expect("checkpointed shutdown should persist metadata");
    }

    {
        let mut engine = StorageEngine::open(&config).expect("engine should reopen");
        engine
            .upsert_node(node_b.clone())
            .expect("second write should stay in WAL");
    }

    let (layout, _) = bootstrap(&config.storage).expect("storage should bootstrap");
    let delta_path = fs::read_dir(layout.delta_directory())
        .expect("delta directory should exist")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .next()
        .expect("graceful shutdown should publish a delta segment");
    let delta_relative = delta_path
        .strip_prefix(&layout.root_dir)
        .expect("delta path should be relative to root")
        .to_string_lossy()
        .into_owned();

    let mut manifest = load_manifest(&layout.manifest_path()).expect("manifest should load");
    manifest
        .files
        .get_mut(&delta_relative)
        .expect("manifest should reference delta file")
        .checksum_crc32 ^= 1;
    persist_manifest(&layout.manifest_path(), &manifest).expect("manifest should rewrite");

    let open_error = StorageEngine::open(&config)
        .err()
        .expect("broken manifest metadata should fail normal open");
    assert!(
        open_error.to_string().contains("checksum")
            || open_error.to_string().contains("snapshot")
            || open_error.to_string().contains("delta")
    );

    let report = repair_storage(&config).expect("repair should recover from metadata corruption");
    assert!(report.wal_replay_valid);

    let repaired = StorageEngine::open(&config).expect("engine should reopen after repair");
    assert!(repaired.get_node(&node_a.id).is_some());
    assert!(repaired.get_node(&node_b.id).is_some());
}

#[test]
fn wal_append_failure_does_not_mutate_live_or_reopened_state() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    let mut engine = StorageEngine::open(&config).expect("engine should open");
    install_wal_io_failpoint("append_write", ".wal");
    let error = engine
        .upsert_node(node.clone())
        .expect_err("injected WAL append failure should reject write");
    clear_wal_io_failpoint();

    assert!(error.to_string().contains("No space left on device"));
    assert!(engine.get_node(&node.id).is_none());
    assert_eq!(engine.node_count(), 0);

    let reopened = StorageEngine::open(&config).expect("engine should reopen cleanly");
    assert!(reopened.get_node(&node.id).is_none());
    assert_eq!(reopened.node_count(), 0);
}

#[test]
fn checkpoint_delta_publish_failure_remains_recoverable_from_wal() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    let mut engine = StorageEngine::open(&config).expect("engine should open");
    engine
        .upsert_node(node.clone())
        .expect("seed write should succeed");

    install_storage_io_failpoint("temp_write", "deltas/");
    let error = engine
        .graceful_shutdown()
        .expect_err("delta publish failure should abort checkpoint");
    clear_storage_io_failpoint();

    assert!(error.to_string().contains("No space left on device"));

    let reopened = StorageEngine::open(&config).expect("engine should recover from WAL");
    assert!(reopened.get_node(&node.id).is_some());
}

#[test]
fn checkpoint_delta_rename_failure_remains_recoverable_from_wal() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    let mut engine = StorageEngine::open(&config).expect("engine should open");
    engine
        .upsert_node(node.clone())
        .expect("seed write should succeed");

    install_storage_io_failpoint("rename", "deltas/");
    let error = engine
        .graceful_shutdown()
        .expect_err("delta rename failure should abort checkpoint");
    clear_storage_io_failpoint();

    assert!(error.to_string().contains("No space left on device"));

    let reopened = StorageEngine::open(&config).expect("engine should recover from WAL");
    assert!(reopened.get_node(&node.id).is_some());
}

#[test]
fn manifest_publish_failure_remains_recoverable_on_restart() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    let mut engine = StorageEngine::open(&config).expect("engine should open");
    engine
        .upsert_node(node.clone())
        .expect("seed write should succeed");

    install_storage_io_failpoint("temp_write", "manifest.tmp");
    let error = engine
        .graceful_shutdown()
        .expect_err("manifest publish failure should abort checkpoint");
    clear_storage_io_failpoint();

    assert!(error.to_string().contains("No space left on device"));

    let reopened = StorageEngine::open(&config).expect("engine should recover from WAL");
    assert!(reopened.get_node(&node.id).is_some());
}

#[test]
fn manifest_rename_failure_remains_recoverable_on_restart() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    let mut engine = StorageEngine::open(&config).expect("engine should open");
    engine
        .upsert_node(node.clone())
        .expect("seed write should succeed");

    install_storage_io_failpoint("rename", "manifest.json");
    let error = engine
        .graceful_shutdown()
        .expect_err("manifest rename failure should abort checkpoint");
    clear_storage_io_failpoint();

    assert!(error.to_string().contains("No space left on device"));

    let reopened = StorageEngine::open(&config).expect("engine should recover from WAL");
    assert!(reopened.get_node(&node.id).is_some());
}

#[test]
fn bulk_write_helpers_apply_and_delete_records_in_batches() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("data");

    let node_a = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");
    let node_b = NodeRecord::new(NodeId::new("node_b").expect("valid id"), "memory")
        .expect("node should build");
    let edge = EdgeRecord::new(
        EdgeId::new("edge_ab").expect("valid id"),
        node_a.id.clone(),
        node_b.id.clone(),
        "relates_to",
    )
    .expect("edge should build");

    let mut engine = StorageEngine::open(&config).expect("engine should open");
    let node_lsn = engine
        .upsert_nodes(vec![node_a.clone(), node_b.clone()])
        .expect("node batch should persist");
    let edge_lsn = engine
        .upsert_edges(vec![edge.clone()])
        .expect("edge batch should persist");

    assert!(edge_lsn > node_lsn);
    assert_eq!(engine.node_count(), 2);
    assert_eq!(engine.edge_count(), 1);

    let removed_edges = engine
        .delete_edges(vec![edge.id.clone()])
        .expect("edge batch delete should succeed");
    let removed_nodes = engine
        .delete_nodes(vec![node_a.id.clone(), node_b.id.clone()])
        .expect("node batch delete should succeed");

    assert_eq!(removed_edges, 1);
    assert_eq!(removed_nodes, 2);
    assert_eq!(engine.node_count(), 0);
    assert_eq!(engine.edge_count(), 0);
}

#[test]
fn backup_and_restore_preserve_wal_only_commits() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");
    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("source");

    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    let engine = {
        let mut engine = StorageEngine::open(&config).expect("engine should open");
        engine
            .upsert_node(node.clone())
            .expect("write should succeed before backup");
        engine
    };

    let backup_dir = tempdir.path().join("backup");
    engine
        .backup_to(&backup_dir)
        .expect("backup should succeed with dirty WAL state");

    let restore_dir = tempdir.path().join("restore");
    restore_directory(&backup_dir, &restore_dir).expect("restore should succeed");

    let mut restored_config = AppConfig::default();
    restored_config.storage.root_dir = restore_dir;

    let restored = StorageEngine::open(&restored_config).expect("restored engine should open");
    assert!(restored.get_node(&node.id).is_some());
}

#[test]
fn restore_rejects_tampered_backup_before_cutover() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");

    let mut source_config = AppConfig::default();
    source_config.storage.root_dir = tempdir.path().join("source");
    let protected_node = NodeRecord::new(NodeId::new("protected").expect("valid id"), "memory")
        .expect("node should build");
    let mut source = StorageEngine::open(&source_config).expect("source engine should open");
    source
        .upsert_node(protected_node.clone())
        .expect("source write should succeed");

    let backup_dir = tempdir.path().join("backup");
    source
        .backup_to(&backup_dir)
        .expect("backup should succeed before tampering");
    fs::remove_file(backup_dir.join(BACKUP_MANIFEST_FILE_NAME))
        .expect("backup manifest should exist for tamper test");

    let mut destination_config = AppConfig::default();
    destination_config.storage.root_dir = tempdir.path().join("destination");
    let sentinel = NodeRecord::new(NodeId::new("sentinel").expect("valid id"), "memory")
        .expect("node should build");
    {
        let mut destination =
            StorageEngine::open(&destination_config).expect("destination engine should open");
        destination
            .upsert_node(sentinel.clone())
            .expect("destination write should succeed");
    }

    let error = restore_directory(&backup_dir, &destination_config.storage.root_dir)
        .expect_err("tampered backup should be rejected");
    assert!(error.to_string().contains("backup manifest"));

    let destination =
        StorageEngine::open(&destination_config).expect("destination should remain readable");
    assert!(destination.get_node(&sentinel.id).is_some());
    assert!(destination.get_node(&protected_node.id).is_none());
}

#[test]
fn backup_writes_verification_manifest() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");

    let mut config = AppConfig::default();
    config.storage.root_dir = tempdir.path().join("source");
    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");
    let mut engine = StorageEngine::open(&config).expect("engine should open");
    engine
        .upsert_node(node)
        .expect("write should succeed before backup");

    let backup_dir = tempdir.path().join("backup");
    engine
        .backup_to(&backup_dir)
        .expect("backup should write verification manifest");

    let manifest_path = backup_dir.join(BACKUP_MANIFEST_FILE_NAME);
    assert!(manifest_path.exists());
    let payload = fs::read_to_string(&manifest_path).expect("backup manifest should be readable");
    assert!(payload.contains("\"integrity\""));
    assert!(payload.contains("\"file_count\""));
}

#[test]
fn restore_to_lsn_recovers_retained_wal_prefix() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");

    let mut source_config = AppConfig::default();
    source_config.storage.root_dir = tempdir.path().join("source");
    let node_a = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");
    let node_b = NodeRecord::new(NodeId::new("node_b").expect("valid id"), "memory")
        .expect("node should build");

    let mut engine = StorageEngine::open(&source_config).expect("engine should open");
    let node_a_lsn = engine
        .upsert_node(node_a.clone())
        .expect("first write should succeed");
    let node_b_lsn = engine
        .upsert_node(node_b.clone())
        .expect("second write should succeed");
    assert!(node_b_lsn > node_a_lsn);

    let backup_dir = tempdir.path().join("backup");
    engine
        .backup_to(&backup_dir)
        .expect("backup should succeed before PITR restore");

    let restore_dir = tempdir.path().join("restore");
    restore_directory_to_lsn(
        &backup_dir,
        &restore_dir,
        node_a_lsn.0,
        &source_config.wal,
    )
    .expect("restore to retained lsn should succeed");

    let mut restored_config = AppConfig::default();
    restored_config.storage.root_dir = restore_dir;
    let restored = StorageEngine::open(&restored_config).expect("restored engine should open");
    assert!(restored.get_node(&node_a.id).is_some());
    assert!(restored.get_node(&node_b.id).is_none());
}

#[test]
fn restore_to_lsn_rejects_targets_before_checkpoint() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");

    let mut source_config = AppConfig::default();
    source_config.storage.root_dir = tempdir.path().join("source");
    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    let mut engine = StorageEngine::open(&source_config).expect("engine should open");
    let checkpoint_lsn = engine.upsert_node(node).expect("write should succeed");
    engine
        .graceful_shutdown()
        .expect("shutdown should publish checkpoint");

    let backup_dir = tempdir.path().join("backup");
    backup_directory(&source_config.storage.root_dir, &backup_dir)
        .expect("backup should succeed after checkpoint");

    let restore_dir = tempdir.path().join("restore");
    let error = restore_directory_to_lsn(
        &backup_dir,
        &restore_dir,
        checkpoint_lsn.0.saturating_sub(1),
        &source_config.wal,
    )
    .expect_err("older-than-checkpoint restore target must be rejected");
    assert!(error.to_string().contains("older than checkpoint"));
}

#[test]
fn restore_to_lsn_rejects_targets_beyond_available_wal() {
    let _guard = recovery_test_guard();
    let tempdir = tempdir().expect("tempdir should be created");

    let mut source_config = AppConfig::default();
    source_config.storage.root_dir = tempdir.path().join("source");
    let node = NodeRecord::new(NodeId::new("node_a").expect("valid id"), "memory")
        .expect("node should build");

    let mut engine = StorageEngine::open(&source_config).expect("engine should open");
    let latest_lsn = engine.upsert_node(node).expect("write should succeed");

    let backup_dir = tempdir.path().join("backup");
    engine
        .backup_to(&backup_dir)
        .expect("backup should succeed before PITR bounds check");

    let restore_dir = tempdir.path().join("restore");
    let error = restore_directory_to_lsn(
        &backup_dir,
        &restore_dir,
        latest_lsn.0 + 1,
        &source_config.wal,
    )
    .expect_err("future restore target must be rejected");
    assert!(error.to_string().contains("exceeds latest available lsn"));
}
