use std::fs;

use tempfile::tempdir;
use undr9_common::crc32;
use undr9_config::AppConfig;
use undr9_storage::StorageEngine;

#[test]
fn opens_v1_style_storage_fixture() {
    let tempdir = tempdir().expect("tempdir should be created");
    let root = tempdir.path().join("fixture");
    fs::create_dir_all(root.join("wal")).expect("wal directory should be created");
    fs::create_dir_all(root.join("nodes")).expect("nodes directory should be created");
    fs::create_dir_all(root.join("edges")).expect("edges directory should be created");
    fs::create_dir_all(root.join("indexes")).expect("indexes directory should be created");
    fs::create_dir_all(root.join("vectors")).expect("vectors directory should be created");
    fs::create_dir_all(root.join("meta")).expect("meta directory should be created");

    let node_snapshot = r#"{
  "format_version": 1,
  "records": [
    {
      "id": "node_a",
      "node_type": "memory",
      "properties": {
        "timestamp": {
          "kind": "Integer",
          "value": 1000
        }
      }
    }
  ]
}"#;
    let edge_snapshot = r#"{
  "format_version": 1,
  "records": []
}"#;

    fs::write(
        root.join("nodes/segment-0000000000000001.snapshot.json"),
        node_snapshot,
    )
    .expect("node snapshot should be written");
    fs::write(
        root.join("edges/segment-0000000000000001.snapshot.json"),
        edge_snapshot,
    )
    .expect("edge snapshot should be written");

    let manifest = format!(
        r#"{{
  "storage_version": "1",
  "files": {{
    "manifest.json": {{
      "relative_path": "manifest.json",
      "checksum_crc32": 0
    }},
    "nodes/segment-0000000000000001.snapshot.json": {{
      "relative_path": "nodes/segment-0000000000000001.snapshot.json",
      "checksum_crc32": {}
    }},
    "edges/segment-0000000000000001.snapshot.json": {{
      "relative_path": "edges/segment-0000000000000001.snapshot.json",
      "checksum_crc32": {}
    }}
  }},
  "settings": {{
    "create_if_missing": true
  }},
  "last_clean_shutdown": true,
  "last_applied_lsn": null
}}"#,
        crc32(node_snapshot.as_bytes()),
        crc32(edge_snapshot.as_bytes()),
    );
    fs::write(root.join("manifest.json"), manifest).expect("manifest should be written");

    let mut config = AppConfig::default();
    config.storage.root_dir = root;

    let engine = StorageEngine::open(&config).expect("v1 fixture should open");
    assert_eq!(engine.node_count(), 1);
    assert_eq!(engine.edge_count(), 0);
}
