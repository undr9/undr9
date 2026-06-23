use std::fs;
use std::time::Instant;

use clap::{Parser, Subcommand};
use tempfile::tempdir;
use undr9_api::{build_router, ApiState};
use undr9_config::AppConfig;
use undr9_core::{IsolationLevel, TransactionOperation};
use undr9_index::GraphIndex;
use undr9_observability::{initialize_tracing, shutdown_tracing, RuntimeObservabilityConfig};
use undr9_replication::ReplicationRecord;
use undr9_storage::{
    backup_directory, bootstrap, load_manifest, repair_storage, restore_directory,
    restore_directory_to_lsn, StorageEngine, StorageLayout,
};
use undr9_wal::replay_from_dir;

#[derive(Debug, Parser)]
#[command(name = "undr9")]
#[command(about = "UNDR9 development CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    ShowDefaultConfig,
    PrintLayout {
        #[arg(long, default_value = "./data")]
        root: String,
    },
    BootstrapStorage {
        #[arg(long, default_value = "./data")]
        root: String,
    },
    ShowManifest {
        #[arg(long, default_value = "./data")]
        root: String,
    },
    InspectStorage {
        #[arg(long, default_value = "./data")]
        root: String,
    },
    CompactStorage {
        #[arg(long, default_value = "./data")]
        root: String,
    },
    VerifyStorage {
        #[arg(long, default_value = "./data")]
        root: String,
    },
    BackupStorage {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long)]
        destination: String,
    },
    RestoreStorage {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long)]
        source: String,
        #[arg(long)]
        target_lsn: Option<u64>,
    },
    RepairStorage {
        #[arg(long, default_value = "./data")]
        root: String,
    },
    RebuildIndexes {
        #[arg(long, default_value = "./data")]
        root: String,
    },
    Export {
        #[arg(long, default_value = "./data")]
        root: String,
        file: String,
    },
    Import {
        #[arg(long, default_value = "./data")]
        root: String,
        file: String,
    },
    RunTransaction {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long)]
        plan: String,
    },
    ReplicationStatus {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
    },
    ReplicationHistory {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
        #[arg(long, default_value_t = 0)]
        after_source_lsn: u64,
        #[arg(long)]
        output: Option<String>,
    },
    ConfigureLeader {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
    },
    ConfigureFollower {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
        #[arg(long)]
        leader_node_id: String,
        #[arg(long)]
        leader_address: String,
    },
    RegisterReplica {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
        #[arg(long)]
        replica_node_id: String,
        #[arg(long)]
        replica_address: String,
    },
    AcknowledgeReplica {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
        #[arg(long)]
        replica_node_id: String,
        #[arg(long)]
        source_lsn: u64,
    },
    ApplyReplication {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
        #[arg(long)]
        file: String,
    },
    ShowClusterTopology {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
    },
    MarkNodeHealth {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
        #[arg(long)]
        target_node_id: String,
        #[arg(long, default_value_t = true)]
        healthy: bool,
    },
    PromoteNode {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
        #[arg(long)]
        target_node_id: String,
    },
    RecoveryDrill {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long)]
        output: Option<String>,
    },
    Serve {
        #[arg(long, default_value = "./data")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "node-1")]
        node_id: String,
    },
}

#[derive(Debug, serde::Deserialize)]
struct TransactionPlan {
    isolation_level: Option<IsolationLevel>,
    operations: Vec<TransactionOperation>,
}

#[derive(Debug, serde::Serialize)]
struct RecoveryDrillReport {
    source_root: String,
    source_node_count: usize,
    source_edge_count: usize,
    source_last_applied_lsn: u64,
    latest_restorable_lsn: u64,
    backup_elapsed_ms: u128,
    restore_elapsed_ms: u128,
    pitr_elapsed_ms: Option<u128>,
    backup_manifest_present: bool,
    source_integrity: undr9_storage::IntegrityReport,
    restored_integrity: undr9_storage::IntegrityReport,
    pitr_integrity: Option<undr9_storage::IntegrityReport>,
    verified: bool,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::ShowDefaultConfig => {
            let config = AppConfig::default();
            let payload =
                serde_json::to_string_pretty(&config).expect("default config must be serializable");
            println!("{payload}");
        }
        Command::PrintLayout { root } => {
            let layout = StorageLayout::new(root);
            let lines = [
                format!("root={}", layout.root_dir.display()),
                format!("manifest={}", layout.manifest_path().display()),
                format!("wal={}", layout.subdirectory("wal").display()),
                format!("nodes={}", layout.subdirectory("nodes").display()),
                format!("edges={}", layout.subdirectory("edges").display()),
                format!("indexes={}", layout.subdirectory("indexes").display()),
                format!("vectors={}", layout.subdirectory("vectors").display()),
                format!("meta={}", layout.subdirectory("meta").display()),
            ];

            for line in lines {
                println!("{line}");
            }
        }
        Command::BootstrapStorage { root } => {
            let mut config = AppConfig::default();
            config.storage.root_dir = root.into();

            let (layout, manifest) =
                bootstrap(&config.storage).expect("storage bootstrap must work");
            println!("root={}", layout.root_dir.display());
            println!("manifest={}", layout.manifest_path().display());
            println!("storage_version={}", manifest.storage_version);
        }
        Command::ShowManifest { root } => {
            let layout = StorageLayout::new(root);
            let manifest =
                load_manifest(&layout.manifest_path()).expect("manifest should be readable");
            let payload =
                serde_json::to_string_pretty(&manifest).expect("manifest must serialize to JSON");
            println!("{payload}");
        }
        Command::InspectStorage { root } => {
            let mut config = AppConfig::default();
            config.storage.root_dir = root.into();

            let engine = StorageEngine::open(&config).expect("storage engine must open");
            println!("root={}", engine.layout().root_dir.display());
            println!("nodes={}", engine.node_count());
            println!("edges={}", engine.edge_count());
            println!(
                "last_applied_lsn={}",
                engine.manifest().last_applied_lsn.unwrap_or(0)
            );
            println!(
                "last_clean_shutdown={}",
                engine.manifest().last_clean_shutdown
            );
            println!("audit_log={}", engine.layout().audit_log_path().display());
        }
        Command::CompactStorage { root } => {
            let mut config = AppConfig::default();
            config.storage.root_dir = root.into();
            let mut engine = StorageEngine::open(&config).expect("storage engine must open");
            engine.compact().expect("storage compaction must work");
            println!("status=ok");
            println!("detail=storage compacted");
        }
        Command::VerifyStorage { root } => {
            let mut config = AppConfig::default();
            config.storage.root_dir = root.into();
            let engine = StorageEngine::open(&config).expect("storage engine must open");
            let payload = serde_json::to_string_pretty(
                &engine
                    .verify_integrity()
                    .expect("integrity verification must work"),
            )
            .expect("integrity report must serialize");
            println!("{payload}");
        }
        Command::BackupStorage { root, destination } => {
            backup_directory(root, &destination).expect("backup must work");
            println!("status=ok");
            println!("destination={destination}");
        }
        Command::RestoreStorage {
            root,
            source,
            target_lsn,
        } => {
            if let Some(target_lsn) = target_lsn {
                let config = AppConfig::default();
                restore_directory_to_lsn(&source, root, target_lsn, &config.wal)
                    .expect("point-in-time restore must work");
                println!("target_lsn={target_lsn}");
            } else {
                restore_directory(&source, root).expect("restore must work");
            }
            println!("status=ok");
            println!("source={source}");
        }
        Command::RepairStorage { root } => {
            let mut config = AppConfig::default();
            config.storage.root_dir = root.into();
            let payload =
                serde_json::to_string_pretty(&repair_storage(&config).expect("repair must work"))
                    .expect("repair report must serialize");
            println!("{payload}");
        }
        Command::RebuildIndexes { root } => {
            let mut config = AppConfig::default();
            config.storage.root_dir = root.into();
            let engine = StorageEngine::open(&config).expect("storage engine must open");
            let index = GraphIndex::rebuild(&engine.all_nodes(), &engine.all_edges());
            let snapshot = index.snapshot();
            let path = engine.layout().index_snapshot_path();
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("index snapshot directory must exist");
            }
            fs::write(
                &path,
                serde_json::to_vec_pretty(&snapshot).expect("index snapshot must serialize"),
            )
            .expect("index snapshot must be written");
            println!("status=ok");
            println!("index_snapshot={}", path.display());
        }
        Command::Export { root, file } => {
            let mut config = AppConfig::default();
            config.storage.root_dir = root.into();
            let engine = StorageEngine::open(&config).expect("storage engine must open");
            engine.export_jsonl(&file).expect("jsonl export must work");
            println!("status=ok");
            println!("file={file}");
        }
        Command::Import { root, file } => {
            let mut config = AppConfig::default();
            config.storage.root_dir = root.into();
            let mut engine = StorageEngine::open(&config).expect("storage engine must open");
            engine.import_jsonl(&file).expect("jsonl import must work");
            println!("status=ok");
            println!("file={file}");
        }
        Command::RunTransaction { root, plan } => {
            let mut config = AppConfig::default();
            config.storage.root_dir = root.into();
            let mut engine = StorageEngine::open(&config).expect("storage engine must open");
            let plan_payload =
                fs::read_to_string(&plan).expect("transaction plan must be readable");
            let plan: TransactionPlan =
                serde_json::from_str(&plan_payload).expect("transaction plan must deserialize");
            let summary =
                engine.begin_transaction(plan.isolation_level.unwrap_or(IsolationLevel::Snapshot));
            for operation in plan.operations {
                engine
                    .stage_transaction_operation(&summary.transaction_id, operation)
                    .expect("transaction stage must work");
            }
            let commit = engine
                .commit_transaction(&summary.transaction_id)
                .expect("transaction commit must work");
            let payload =
                serde_json::to_string_pretty(&commit).expect("transaction result must serialize");
            println!("{payload}");
        }
        Command::ReplicationStatus {
            root,
            bind,
            node_id,
        } => {
            let state = api_state(&root, &bind, &node_id);
            let database = state.database.read().await;
            print_json(&database.replication_status());
        }
        Command::ReplicationHistory {
            root,
            bind,
            node_id,
            after_source_lsn,
            output,
        } => {
            let state = api_state(&root, &bind, &node_id);
            let database = state.database.read().await;
            let records = database.replication_history_since(after_source_lsn);
            emit_json(&records, output.as_deref());
        }
        Command::ConfigureLeader {
            root,
            bind,
            node_id,
        } => {
            let state = api_state(&root, &bind, &node_id);
            let mut database = state.database.write().await;
            let response = database
                .configure_as_leader()
                .expect("leader configuration must work");
            print_json(&response);
        }
        Command::ConfigureFollower {
            root,
            bind,
            node_id,
            leader_node_id,
            leader_address,
        } => {
            let state = api_state(&root, &bind, &node_id);
            let mut database = state.database.write().await;
            let response = database
                .configure_as_follower(leader_node_id, leader_address)
                .expect("follower configuration must work");
            print_json(&response);
        }
        Command::RegisterReplica {
            root,
            bind,
            node_id,
            replica_node_id,
            replica_address,
        } => {
            let state = api_state(&root, &bind, &node_id);
            let mut database = state.database.write().await;
            let topology = database
                .register_replica(replica_node_id, replica_address)
                .expect("replica registration must work");
            print_json(&topology);
        }
        Command::AcknowledgeReplica {
            root,
            bind,
            node_id,
            replica_node_id,
            source_lsn,
        } => {
            let state = api_state(&root, &bind, &node_id);
            let mut database = state.database.write().await;
            let response = database
                .acknowledge_replica(&replica_node_id, source_lsn)
                .expect("replica acknowledgement must work");
            print_json(&response);
        }
        Command::ApplyReplication {
            root,
            bind,
            node_id,
            file,
        } => {
            let state = api_state(&root, &bind, &node_id);
            let payload = fs::read_to_string(&file).expect("replication file must be readable");
            let records: Vec<ReplicationRecord> =
                serde_json::from_str(&payload).expect("replication file must deserialize");
            let mut database = state.database.write().await;
            let response = database
                .apply_replication_records(&records)
                .expect("replication apply must work");
            print_json(&response);
        }
        Command::ShowClusterTopology {
            root,
            bind,
            node_id,
        } => {
            let state = api_state(&root, &bind, &node_id);
            let database = state.database.read().await;
            print_json(&database.cluster_topology());
        }
        Command::MarkNodeHealth {
            root,
            bind,
            node_id,
            target_node_id,
            healthy,
        } => {
            let state = api_state(&root, &bind, &node_id);
            let mut database = state.database.write().await;
            let topology = database
                .mark_node_health(&target_node_id, healthy)
                .expect("health update must work");
            print_json(&topology);
        }
        Command::PromoteNode {
            root,
            bind,
            node_id,
            target_node_id,
        } => {
            let state = api_state(&root, &bind, &node_id);
            let mut database = state.database.write().await;
            let plan = database
                .promote_node(&target_node_id)
                .expect("node promotion must work");
            print_json(&plan);
        }
        Command::RecoveryDrill { root, output } => {
            let report = run_recovery_drill(&root).expect("recovery drill must succeed");
            emit_json(&report, output.as_deref());
        }
        Command::Serve {
            root,
            bind,
            node_id,
        } => {
            let config = runtime_server_config(&root, &bind)
                .expect("server configuration must include explicit auth keys");
            initialize_tracing(
                "undr9",
                &RuntimeObservabilityConfig {
                    log_level: config.observability.log_level.clone(),
                    tracing_enabled: config.observability.tracing_enabled,
                    tracing_json: config.observability.tracing_json,
                    otlp_enabled: config.observability.otlp_enabled,
                    otlp_protocol: config.observability.otlp_protocol.clone(),
                    otlp_endpoint: config.observability.otlp_endpoint.clone(),
                    otlp_headers: config.observability.otlp_headers.clone(),
                    otlp_timeout_ms: config.observability.otlp_timeout_ms,
                },
            )
            .expect("runtime observability must initialize");
            tracing::info!(bind_address = %bind, node_id = %node_id, "starting UNDR9 API server");

            let state = ApiState::try_new_with_identity(config, node_id, bind.clone())
                .expect("API state must initialize");
            let app = build_router(state.clone());
            let listener = tokio::net::TcpListener::bind(&bind)
                .await
                .expect("bind address must be available");
            println!("listening={bind}");
            println!("tls=reverse-proxy-required");
            println!("recommended_proxy=caddy_or_traefik");
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal(state))
                .await
                .expect("server must run successfully");
            shutdown_tracing().expect("runtime observability must shutdown cleanly");
        }
    }
}

fn runtime_server_config(root: &str, bind: &str) -> Result<AppConfig, String> {
    let mut config = AppConfig::default();
    config.storage.root_dir = root.into();
    config.server.bind_address = bind.to_owned();
    config.apply_env_overrides();

    let admin_key = std::env::var("UNDR9_ADMIN_API_KEY").ok();
    let writer_key = std::env::var("UNDR9_WRITER_API_KEY").ok();
    let reader_key = std::env::var("UNDR9_READER_API_KEY").ok();

    if !config.auth.enabled {
        return Err(
            "auth must remain enabled for `undr9 serve`; refusing insecure auth-disabled startup"
                .to_owned(),
        );
    }
    if admin_key.as_deref().unwrap_or("").trim().is_empty()
        || writer_key.as_deref().unwrap_or("").trim().is_empty()
        || reader_key.as_deref().unwrap_or("").trim().is_empty()
    {
        return Err(
            "set UNDR9_ADMIN_API_KEY, UNDR9_WRITER_API_KEY, and UNDR9_READER_API_KEY before running `undr9 serve`"
                .to_owned(),
        );
    }

    config
        .validate()
        .map_err(|error| format!("invalid runtime server config: {error}"))?;
    Ok(config)
}

fn run_recovery_drill(root: &str) -> Result<RecoveryDrillReport, String> {
    let mut source_config = AppConfig::default();
    source_config.storage.root_dir = root.into();
    source_config
        .validate()
        .map_err(|error| format!("invalid recovery drill source config: {error}"))?;

    let source_engine = StorageEngine::open(&source_config)
        .map_err(|error| format!("failed to open source storage for recovery drill: {error}"))?;
    let source_node_count = source_engine.node_count();
    let source_edge_count = source_engine.edge_count();
    let source_last_applied_lsn = source_engine.manifest().last_applied_lsn.unwrap_or(0);
    let latest_restorable_lsn = replay_from_dir(
        &source_engine.layout().subdirectory("wal"),
        source_config.wal.max_replay_bytes,
    )
    .map_err(|error| format!("failed to enumerate retained WAL for recovery drill: {error}"))?
    .last()
    .map(|record| record.header.lsn.0)
    .unwrap_or(source_last_applied_lsn);
    let mut source_integrity = source_engine.verify_integrity().map_err(|error| {
        format!("failed to verify source integrity for recovery drill: {error}")
    })?;
    source_integrity.node_count = source_node_count;
    source_integrity.edge_count = source_edge_count;

    let drill_dir =
        tempdir().map_err(|error| format!("failed to create recovery drill tempdir: {error}"))?;
    let backup_dir = drill_dir.path().join("backup");
    let restore_dir = drill_dir.path().join("restore-full");
    let pitr_dir = drill_dir.path().join("restore-pitr");

    let backup_started = Instant::now();
    backup_directory(root, &backup_dir)
        .map_err(|error| format!("recovery drill backup failed: {error}"))?;
    let backup_elapsed_ms = backup_started.elapsed().as_millis();

    let restore_started = Instant::now();
    restore_directory(&backup_dir, &restore_dir)
        .map_err(|error| format!("recovery drill restore failed: {error}"))?;
    let restore_elapsed_ms = restore_started.elapsed().as_millis();

    let mut restored_config = AppConfig::default();
    restored_config.storage.root_dir = restore_dir.clone();
    let restored_engine = StorageEngine::open(&restored_config)
        .map_err(|error| format!("failed to open restored storage for recovery drill: {error}"))?;
    let mut restored_integrity = restored_engine.verify_integrity().map_err(|error| {
        format!("failed to verify restored integrity for recovery drill: {error}")
    })?;
    restored_integrity.node_count = restored_engine.node_count();
    restored_integrity.edge_count = restored_engine.edge_count();

    if restored_engine.node_count() != source_node_count
        || restored_engine.edge_count() != source_edge_count
    {
        return Err(format!(
            "recovery drill restore counts diverged: source=({source_node_count},{source_edge_count}) restored=({},{})",
            restored_engine.node_count(),
            restored_engine.edge_count()
        ));
    }

    let (pitr_elapsed_ms, pitr_integrity) = if latest_restorable_lsn > 0 {
        let pitr_started = Instant::now();
        restore_directory_to_lsn(
            &backup_dir,
            &pitr_dir,
            latest_restorable_lsn,
            &source_config.wal,
        )
        .map_err(|error| format!("recovery drill PITR restore failed: {error}"))?;
        let pitr_elapsed_ms = pitr_started.elapsed().as_millis();

        let mut pitr_config = AppConfig::default();
        pitr_config.storage.root_dir = pitr_dir;
        let pitr_engine = StorageEngine::open(&pitr_config)
            .map_err(|error| format!("failed to open PITR storage for recovery drill: {error}"))?;
        if pitr_engine.node_count() != source_node_count
            || pitr_engine.edge_count() != source_edge_count
        {
            return Err(format!(
                "recovery drill PITR counts diverged: source=({source_node_count},{source_edge_count}) pitr=({},{})",
                pitr_engine.node_count(),
                pitr_engine.edge_count()
            ));
        }
        let mut pitr_integrity = pitr_engine.verify_integrity().map_err(|error| {
            format!("failed to verify PITR integrity for recovery drill: {error}")
        })?;
        pitr_integrity.node_count = pitr_engine.node_count();
        pitr_integrity.edge_count = pitr_engine.edge_count();
        (Some(pitr_elapsed_ms), Some(pitr_integrity))
    } else {
        (None, None)
    };

    Ok(RecoveryDrillReport {
        source_root: source_config.storage.root_dir.display().to_string(),
        source_node_count,
        source_edge_count,
        source_last_applied_lsn,
        latest_restorable_lsn,
        backup_elapsed_ms,
        restore_elapsed_ms,
        pitr_elapsed_ms,
        backup_manifest_present: backup_dir
            .join(undr9_storage::BACKUP_MANIFEST_FILE_NAME)
            .exists(),
        source_integrity,
        restored_integrity,
        pitr_integrity,
        verified: true,
    })
}

async fn shutdown_signal(state: ApiState) {
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler must initialize");
        sigterm.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate => {}
    }

    tracing::info!("shutdown signal received; draining readiness and flushing storage state");
    if let Err(error) = state.graceful_shutdown().await {
        tracing::error!(error = %error, "graceful shutdown failed");
    }
}

fn api_state(root: &str, bind: &str, node_id: &str) -> ApiState {
    let mut config = AppConfig::default();
    config.storage.root_dir = root.into();
    config.server.bind_address = bind.to_owned();
    ApiState::try_new_with_identity(config, node_id.to_owned(), bind.to_owned())
        .expect("API state must initialize")
}

fn print_json<T>(value: &T)
where
    T: serde::Serialize,
{
    emit_json(value, None);
}

fn emit_json<T>(value: &T, output: Option<&str>)
where
    T: serde::Serialize,
{
    let payload = serde_json::to_string_pretty(value).expect("value must serialize");
    if let Some(output) = output {
        fs::write(output, format!("{payload}\n")).expect("output file must be writable");
    } else {
        println!("{payload}");
    }
}
