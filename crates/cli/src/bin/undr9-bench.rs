use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use im::OrdMap;
use serde::Serialize;
use tempfile::tempdir;
use undr9_common::{EdgeId, NodeId};
use undr9_config::{AppConfig, VectorIndexBackendConfig, VectorIndexConfig};
use undr9_core::{EdgeRecord, NodeRecord, PropertyValue};
use undr9_index::{EdgeDirection, GraphIndex};
use undr9_query::{Executor, GraphSnapshot, QueryRequest, QueryResponse};
use undr9_storage::StorageEngine;

type BenchResult<T> = Result<T, Box<dyn Error>>;
const STORAGE_BATCH_SIZE: usize = 50_000;
const BENCHMARK_WAL_REPLAY_BYTES: u64 = 16 * 1024 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "undr9-bench")]
#[command(about = "Run repeatable single-node UNDR9 benchmarks and emit a JSON baseline")]
struct Args {
    #[arg(long, default_value = "1000,5000,10000")]
    scales: String,
    #[arg(long, default_value_t = 5)]
    iterations: usize,
    #[arg(
        long,
        default_value = "docs/operations/single-node-benchmark-baseline.json"
    )]
    output: PathBuf,
    #[arg(long, default_value = "full")]
    scenario_profile: String,
    #[arg(long, default_value = "standard")]
    workload_profile: String,
    #[arg(long)]
    hnsw_semantic_top_k: Option<usize>,
    #[arg(long)]
    hnsw_ef_search: Option<usize>,
    #[arg(long)]
    hnsw_m: Option<usize>,
    #[arg(long)]
    hnsw_ef_construction: Option<usize>,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    generated_at_ms: u128,
    host_os: &'static str,
    host_arch: &'static str,
    timing_unit: &'static str,
    iterations: usize,
    scenario_profile: String,
    workload_profile: String,
    hnsw_tuning: HnswTuningReport,
    scales: Vec<ScaleReport>,
}

#[derive(Debug, Serialize)]
struct ScaleReport {
    node_count: usize,
    edge_count: usize,
    peak_rss_bytes: Option<u64>,
    storage_footprint: StorageFootprintReport,
    vector_index_footprint: Option<VectorIndexFootprintReport>,
    quality_comparisons: Vec<QualityComparisonReport>,
    scenarios: Vec<ScenarioReport>,
}

#[derive(Debug, Serialize)]
struct StorageFootprintReport {
    wal_bytes: u64,
    snapshot_bytes: u64,
    delta_bytes: u64,
    index_bytes: u64,
    total_bytes: u64,
    post_compaction_total_bytes: u64,
    compaction_elapsed_us: u128,
    recovery_elapsed_us: u128,
}

#[derive(Debug, Serialize)]
struct VectorIndexFootprintReport {
    hnsw_index_bytes: u64,
    hnsw_build_elapsed_us: u128,
    hnsw_reload_elapsed_us: u128,
}

#[derive(Debug, Clone, Serialize)]
struct HnswTuningReport {
    semantic_top_k: usize,
    ef_search: usize,
    m: usize,
    ef_construction: usize,
}

#[derive(Debug, Serialize)]
struct QualityComparisonReport {
    name: String,
    limit: usize,
    exact_result_count: usize,
    hnsw_result_count: usize,
    overlap_count: usize,
    overlap_ratio: f64,
    jaccard_ratio: f64,
    top1_match: bool,
    exact_only_count: usize,
    hnsw_only_count: usize,
}

#[derive(Debug, Serialize)]
struct ScenarioReport {
    name: String,
    samples_us: Vec<u128>,
    min_us: u128,
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    max_us: u128,
    mean_us: f64,
    throughput_ops_per_sec: f64,
}

fn main() -> BenchResult<()> {
    let args = Args::parse();
    let scales = parse_scales(&args.scales)?;
    let scenario_profile = parse_scenario_profile(&args.scenario_profile)?;
    let workload_profile = parse_workload_profile(&args.workload_profile)?;
    let hnsw_tuning = benchmark_hnsw_tuning_from_args(&args);
    let mut reports = Vec::new();

    for node_count in scales {
        reports.push(run_scale(
            node_count,
            args.iterations,
            scenario_profile,
            workload_profile,
            &hnsw_tuning,
        )?);
    }

    let report = BenchmarkReport {
        generated_at_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        host_os: std::env::consts::OS,
        host_arch: std::env::consts::ARCH,
        timing_unit: "microseconds",
        iterations: args.iterations,
        scenario_profile: scenario_profile.as_str().to_owned(),
        workload_profile: workload_profile.as_str().to_owned(),
        hnsw_tuning: hnsw_tuning.clone(),
        scales: reports,
    };

    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&args.output, serde_json::to_vec_pretty(&report)?)?;

    println!("wrote benchmark baseline to {}", args.output.display());
    for scale in &report.scales {
        println!(
            "scale nodes={} edges={}",
            scale.node_count, scale.edge_count
        );
        for scenario in &scale.scenarios {
            println!(
                "  {:24} p50={}us p95={}us p99={}us max={}us mean={:.2}us ops/s={:.2}",
                scenario.name,
                scenario.p50_us,
                scenario.p95_us,
                scenario.p99_us,
                scenario.max_us,
                scenario.mean_us,
                scenario.throughput_ops_per_sec
            );
        }
    }

    Ok(())
}

fn parse_scales(raw: &str) -> BenchResult<Vec<usize>> {
    let mut scales = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.parse::<usize>())
        .collect::<Result<Vec<_>, _>>()?;
    scales.sort_unstable();
    scales.dedup();
    if scales.is_empty() {
        return Err("at least one benchmark scale is required".into());
    }
    Ok(scales)
}

fn run_scale(
    node_count: usize,
    iterations: usize,
    scenario_profile: ScenarioProfile,
    workload_profile: WorkloadProfile,
    hnsw_tuning: &HnswTuningReport,
) -> BenchResult<ScaleReport> {
    if matches!(scenario_profile, ScenarioProfile::StorageOnly)
        && matches!(workload_profile, WorkloadProfile::Compact)
    {
        return run_streamed_storage_scale(node_count, iterations);
    }

    let workload = build_workload(node_count, workload_profile)?;
    let exact_snapshot = if scenario_profile.includes_query_scenarios() {
        Some(workload.snapshot_with_vector_index_config(&benchmark_exact_vector_index_config()))
    } else {
        None
    };
    let hnsw_snapshot =
        if scenario_profile.includes_query_scenarios() && workload_profile.includes_vectors() {
            Some(
                workload.snapshot_with_vector_index_config(&benchmark_hnsw_vector_index_config(
                    hnsw_tuning,
                )),
            )
        } else {
            None
        };
    let storage_footprint = measure_storage_footprint(&workload)?;
    let vector_index_footprint = if workload_profile.includes_vectors() {
        Some(measure_vector_index_footprint(&workload, hnsw_tuning)?)
    } else {
        None
    };
    let quality_comparisons = if let (Some(exact_snapshot), Some(hnsw_snapshot)) =
        (exact_snapshot.as_ref(), hnsw_snapshot.as_ref())
    {
        measure_quality_comparisons(exact_snapshot, hnsw_snapshot, &workload)?
    } else {
        Vec::new()
    };
    let mut scenarios = Vec::new();

    if scenario_profile.includes_storage_scenarios() {
        scenarios.push(measure("storage_upsert", iterations, || {
            bench_storage_upsert(&workload)
        })?);
        scenarios.push(measure("storage_delete", iterations, || {
            bench_storage_delete(&workload)
        })?);
        scenarios.push(measure("wal_recovery", iterations, || {
            bench_wal_recovery(&workload)
        })?);
    }

    if let Some(snapshot) = exact_snapshot.as_ref() {
        scenarios.push(measure("exact_lookup", iterations, || {
            bench_exact_lookup(snapshot, &workload)
        })?);
        scenarios.push(measure("list_neighbors_1_hop", iterations, || {
            bench_list_neighbors(snapshot, &workload)
        })?);
        scenarios.push(measure("label_scan", iterations, || {
            bench_label_scan(snapshot)
        })?);
        scenarios.push(measure("traverse_5_hops", iterations, || {
            bench_traverse(snapshot, &workload)
        })?);
        scenarios.push(measure("shortest_path", iterations, || {
            bench_shortest_path(snapshot, &workload)
        })?);
        scenarios.push(measure("temporal_range", iterations, || {
            bench_temporal_range(snapshot)
        })?);
        if workload_profile.includes_vectors() {
            scenarios.push(measure("vector_search_exact", iterations, || {
                bench_vector_search(snapshot)
            })?);
            scenarios.push(measure("ranked_retrieval_exact", iterations, || {
                bench_ranked_retrieval(snapshot, &workload)
            })?);
            if let Some(hnsw_snapshot) = hnsw_snapshot.as_ref() {
                scenarios.push(measure("vector_search_hnsw", iterations, || {
                    bench_vector_search(hnsw_snapshot)
                })?);
                scenarios.push(measure("ranked_retrieval_hnsw", iterations, || {
                    bench_ranked_retrieval(hnsw_snapshot, &workload)
                })?);
            }
        }
    }

    Ok(ScaleReport {
        node_count: workload.nodes.len(),
        edge_count: workload.edges.len(),
        peak_rss_bytes: current_process_peak_rss_bytes(),
        storage_footprint,
        vector_index_footprint,
        quality_comparisons,
        scenarios,
    })
}

fn run_streamed_storage_scale(node_count: usize, iterations: usize) -> BenchResult<ScaleReport> {
    let storage_footprint = measure_storage_footprint_streamed(node_count)?;
    let scenarios = vec![
        measure("storage_upsert", iterations, || {
            bench_storage_upsert_streamed(node_count)
        })?,
        measure("storage_delete", iterations, || {
            bench_storage_delete_streamed(node_count)
        })?,
        measure("wal_recovery", iterations, || {
            bench_wal_recovery_streamed(node_count)
        })?,
    ];

    Ok(ScaleReport {
        node_count,
        edge_count: node_count.saturating_sub(1),
        peak_rss_bytes: current_process_peak_rss_bytes(),
        storage_footprint,
        vector_index_footprint: None,
        quality_comparisons: Vec::new(),
        scenarios,
    })
}

fn measure<F>(name: &str, iterations: usize, mut workload: F) -> BenchResult<ScenarioReport>
where
    F: FnMut() -> BenchResult<()>,
{
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        workload()?;
        samples.push(started.elapsed().as_micros());
    }
    Ok(summarize(name, samples))
}

fn summarize(name: &str, mut samples: Vec<u128>) -> ScenarioReport {
    samples.sort_unstable();
    let min_us = samples[0];
    let max_us = *samples.last().unwrap_or(&min_us);
    let p50_us = percentile(&samples, 0.50);
    let p95_us = percentile(&samples, 0.95);
    let p99_us = percentile(&samples, 0.99);
    let mean_us = samples.iter().sum::<u128>() as f64 / samples.len() as f64;
    ScenarioReport {
        name: name.to_owned(),
        samples_us: samples,
        min_us,
        p50_us,
        p95_us,
        p99_us,
        max_us,
        mean_us,
        throughput_ops_per_sec: if mean_us > 0.0 {
            1_000_000.0 / mean_us
        } else {
            0.0
        },
    }
}

fn percentile(samples: &[u128], percentile: f64) -> u128 {
    let index = ((samples.len().saturating_sub(1)) as f64 * percentile).round() as usize;
    samples[index.min(samples.len().saturating_sub(1))]
}

fn bench_storage_upsert(workload: &Workload) -> BenchResult<()> {
    let tempdir = tempdir()?;
    let config = benchmark_storage_config(tempdir.path().join("data"));

    let mut engine = StorageEngine::open(&config)?;
    engine.upsert_nodes(workload.nodes.clone())?;
    engine.upsert_edges(workload.edges.clone())?;
    if engine.node_count() != workload.nodes.len() || engine.edge_count() != workload.edges.len() {
        return Err("storage_upsert benchmark produced incorrect counts".into());
    }
    Ok(())
}

fn bench_storage_upsert_streamed(node_count: usize) -> BenchResult<()> {
    let tempdir = tempdir()?;
    let config = benchmark_storage_config(tempdir.path().join("data"));

    let mut engine = StorageEngine::open(&config)?;
    populate_compact_storage(&mut engine, node_count)?;
    verify_storage_counts(&engine, node_count)
}

fn bench_storage_delete(workload: &Workload) -> BenchResult<()> {
    let tempdir = tempdir()?;
    let config = benchmark_storage_config(tempdir.path().join("data"));

    let mut engine = StorageEngine::open(&config)?;
    engine.upsert_nodes(workload.nodes.clone())?;
    engine.upsert_edges(workload.edges.clone())?;
    engine.delete_edges(
        workload
            .edges
            .iter()
            .rev()
            .map(|edge| edge.id.clone())
            .collect(),
    )?;
    engine.delete_nodes(
        workload
            .nodes
            .iter()
            .rev()
            .map(|node| node.id.clone())
            .collect(),
    )?;
    if engine.node_count() != 0 || engine.edge_count() != 0 {
        return Err("storage_delete benchmark did not remove all records".into());
    }
    Ok(())
}

fn bench_storage_delete_streamed(node_count: usize) -> BenchResult<()> {
    let tempdir = tempdir()?;
    let config = benchmark_storage_config(tempdir.path().join("data"));

    let mut engine = StorageEngine::open(&config)?;
    populate_compact_storage(&mut engine, node_count)?;

    for range in reverse_batch_ranges(node_count.saturating_sub(1)) {
        engine.delete_edges(edge_ids_for_range(range)?)?;
    }
    for range in reverse_batch_ranges(node_count) {
        engine.delete_nodes(node_ids_for_range(range)?)?;
    }

    if engine.node_count() != 0 || engine.edge_count() != 0 {
        return Err("streamed storage_delete benchmark did not remove all records".into());
    }
    Ok(())
}

fn bench_wal_recovery(workload: &Workload) -> BenchResult<()> {
    let tempdir = tempdir()?;
    let config = benchmark_storage_config(tempdir.path().join("data"));

    {
        let mut engine = StorageEngine::open(&config)?;
        engine.upsert_nodes(workload.nodes.clone())?;
        engine.upsert_edges(workload.edges.clone())?;
    }

    let reopened = StorageEngine::open(&config)?;
    if reopened.node_count() != workload.nodes.len()
        || reopened.edge_count() != workload.edges.len()
    {
        return Err("wal_recovery benchmark reopened with incorrect counts".into());
    }
    Ok(())
}

fn bench_wal_recovery_streamed(node_count: usize) -> BenchResult<()> {
    let tempdir = tempdir()?;
    let config = benchmark_storage_config(tempdir.path().join("data"));

    {
        let mut engine = StorageEngine::open(&config)?;
        populate_compact_storage(&mut engine, node_count)?;
    }

    let reopened = StorageEngine::open(&config)?;
    verify_storage_counts(&reopened, node_count)
}

fn bench_label_scan(snapshot: &GraphSnapshot) -> BenchResult<()> {
    let response = Executor::execute(
        &QueryRequest::SearchByLabel {
            label: "memory".to_owned(),
            limit: Some(1_000),
        },
        snapshot,
    )?;
    if response.nodes.is_empty() {
        return Err("label_scan benchmark returned no nodes".into());
    }
    Ok(())
}

fn bench_exact_lookup(snapshot: &GraphSnapshot, workload: &Workload) -> BenchResult<()> {
    let response = Executor::execute(
        &QueryRequest::GetNodeById {
            node_id: workload.start_node_id.clone(),
        },
        snapshot,
    )?;
    if response.nodes.len() != 1 {
        return Err("exact_lookup benchmark failed to return exactly one node".into());
    }
    Ok(())
}

fn bench_list_neighbors(snapshot: &GraphSnapshot, workload: &Workload) -> BenchResult<()> {
    let response = Executor::execute(
        &QueryRequest::ListNeighbors {
            node_id: workload.start_node_id.clone(),
            edge_type: Some("relates_to".to_owned()),
            direction: EdgeDirection::Outgoing,
            limit: Some(50),
        },
        snapshot,
    )?;
    if response.nodes.is_empty() || response.edges.is_empty() {
        return Err("list_neighbors_1_hop benchmark returned no graph items".into());
    }
    Ok(())
}

fn bench_traverse(snapshot: &GraphSnapshot, workload: &Workload) -> BenchResult<()> {
    let response = Executor::execute(
        &QueryRequest::Traverse {
            start_node_id: workload.start_node_id.clone(),
            edge_type: Some("relates_to".to_owned()),
            direction: EdgeDirection::Outgoing,
            max_hops: Some(5),
            limit: Some(1_000),
            timeout_ms: Some(5_000),
            constraints: None,
        },
        snapshot,
    )?;
    if response.nodes.is_empty() || response.edges.is_empty() {
        return Err("traverse_5_hops benchmark returned no graph items".into());
    }
    Ok(())
}

fn bench_shortest_path(snapshot: &GraphSnapshot, workload: &Workload) -> BenchResult<()> {
    let response = Executor::execute(
        &QueryRequest::ShortestPath {
            source_node_id: workload.start_node_id.clone(),
            target_node_id: workload.end_node_id.clone(),
            direction: EdgeDirection::Outgoing,
            max_depth: Some(32),
            limit: Some(1_000),
            timeout_ms: Some(5_000),
            constraints: None,
        },
        snapshot,
    )?;
    if response.path.is_none() {
        return Err("shortest_path benchmark failed to find a path".into());
    }
    Ok(())
}

fn bench_temporal_range(snapshot: &GraphSnapshot) -> BenchResult<()> {
    let response = Executor::execute(
        &QueryRequest::TimeRange {
            field: "timestamp".to_owned(),
            from_epoch_ms: 1_000,
            to_epoch_ms: 1_000_000,
            limit: 1_000,
        },
        snapshot,
    )?;
    if response.nodes.is_empty() {
        return Err("temporal_range benchmark returned no nodes".into());
    }
    Ok(())
}

fn bench_vector_search(snapshot: &GraphSnapshot) -> BenchResult<()> {
    let response = execute_vector_search(snapshot)?;
    if response.ranked_results.is_empty() {
        return Err("vector_search benchmark returned no results".into());
    }
    Ok(())
}

fn bench_ranked_retrieval(snapshot: &GraphSnapshot, workload: &Workload) -> BenchResult<()> {
    let response = execute_ranked_retrieval(snapshot, workload)?;
    if response.ranked_results.is_empty() {
        return Err("ranked_retrieval benchmark returned no results".into());
    }
    Ok(())
}

fn execute_vector_search(snapshot: &GraphSnapshot) -> BenchResult<QueryResponse> {
    Ok(Executor::execute(
        &QueryRequest::VectorSearch {
            query_vector: vec![1.0, 0.0, 0.5, 0.25],
            node_type: Some("memory".to_owned()),
            vector_name: None,
            limit: 50,
            top_k: None,
        },
        snapshot,
    )?)
}

fn execute_ranked_retrieval(
    snapshot: &GraphSnapshot,
    workload: &Workload,
) -> BenchResult<QueryResponse> {
    Ok(Executor::execute(
        &QueryRequest::RankedRetrieval {
            query_vector: Some(vec![1.0, 0.0, 0.5, 0.25]),
            reference_node_id: Some(workload.start_node_id.clone()),
            edge_type: Some("relates_to".to_owned()),
            from_epoch_ms: Some(1_000),
            to_epoch_ms: Some(1_000_000),
            vector_name: None,
            limit: 50,
            top_k: None,
            now_epoch_ms: 1_000_000,
            retrieval_profile: Some("v1-default".to_owned()),
        },
        snapshot,
    )?)
}

fn measure_quality_comparisons(
    exact_snapshot: &GraphSnapshot,
    hnsw_snapshot: &GraphSnapshot,
    workload: &Workload,
) -> BenchResult<Vec<QualityComparisonReport>> {
    let exact_vector = execute_vector_search(exact_snapshot)?;
    let hnsw_vector = execute_vector_search(hnsw_snapshot)?;
    let exact_ranked = execute_ranked_retrieval(exact_snapshot, workload)?;
    let hnsw_ranked = execute_ranked_retrieval(hnsw_snapshot, workload)?;

    Ok(vec![
        compare_ranked_results(
            "vector_search",
            &exact_vector.ranked_results,
            &hnsw_vector.ranked_results,
        ),
        compare_ranked_results(
            "ranked_retrieval",
            &exact_ranked.ranked_results,
            &hnsw_ranked.ranked_results,
        ),
    ])
}

fn compare_ranked_results(
    name: &str,
    exact: &[undr9_query::RankedNodeResult],
    hnsw: &[undr9_query::RankedNodeResult],
) -> QualityComparisonReport {
    let exact_ids = exact
        .iter()
        .map(|result| result.node.id.clone())
        .collect::<Vec<_>>();
    let hnsw_ids = hnsw
        .iter()
        .map(|result| result.node.id.clone())
        .collect::<Vec<_>>();
    let exact_set = exact_ids.iter().cloned().collect::<BTreeSet<_>>();
    let hnsw_set = hnsw_ids.iter().cloned().collect::<BTreeSet<_>>();
    let overlap_count = exact_set.intersection(&hnsw_set).count();
    let union_count = exact_set.union(&hnsw_set).count();
    let limit = exact.len().max(hnsw.len());

    QualityComparisonReport {
        name: name.to_owned(),
        limit,
        exact_result_count: exact.len(),
        hnsw_result_count: hnsw.len(),
        overlap_count,
        overlap_ratio: if limit > 0 {
            overlap_count as f64 / limit as f64
        } else {
            0.0
        },
        jaccard_ratio: if union_count > 0 {
            overlap_count as f64 / union_count as f64
        } else {
            0.0
        },
        top1_match: exact_ids.first() == hnsw_ids.first(),
        exact_only_count: exact_set.difference(&hnsw_set).count(),
        hnsw_only_count: hnsw_set.difference(&exact_set).count(),
    }
}

fn measure_storage_footprint(workload: &Workload) -> BenchResult<StorageFootprintReport> {
    let tempdir = tempdir()?;
    let config = benchmark_storage_config(tempdir.path().join("data"));

    let mut engine = StorageEngine::open(&config)?;
    engine.upsert_nodes(workload.nodes.clone())?;
    engine.upsert_edges(workload.edges.clone())?;

    let layout = engine.layout().clone();
    let wal_bytes = recursive_path_size(&layout.subdirectory("wal"))?;
    let snapshot_bytes = file_len_if_exists(&layout.node_segment_path())?
        + file_len_if_exists(&layout.edge_segment_path())?
        + file_len_if_exists(&layout.vector_segment_path())?;
    let delta_bytes = recursive_path_size(&layout.delta_directory())?;
    let index_bytes = file_len_if_exists(&layout.index_snapshot_path())?;
    let total_bytes = recursive_path_size(&layout.root_dir)?;

    let compaction_started = Instant::now();
    engine.compact()?;
    let compaction_elapsed_us = compaction_started.elapsed().as_micros();
    let post_compaction_total_bytes = recursive_path_size(&layout.root_dir)?;

    drop(engine);
    let recovery_started = Instant::now();
    let reopened = StorageEngine::open(&config)?;
    let recovery_elapsed_us = recovery_started.elapsed().as_micros();
    if reopened.node_count() != workload.nodes.len()
        || reopened.edge_count() != workload.edges.len()
    {
        return Err("storage footprint recovery check reopened with incorrect counts".into());
    }

    Ok(StorageFootprintReport {
        wal_bytes,
        snapshot_bytes,
        delta_bytes,
        index_bytes,
        total_bytes,
        post_compaction_total_bytes,
        compaction_elapsed_us,
        recovery_elapsed_us,
    })
}

fn measure_storage_footprint_streamed(node_count: usize) -> BenchResult<StorageFootprintReport> {
    let tempdir = tempdir()?;
    let config = benchmark_storage_config(tempdir.path().join("data"));

    let mut engine = StorageEngine::open(&config)?;
    populate_compact_storage(&mut engine, node_count)?;

    let layout = engine.layout().clone();
    let wal_bytes = recursive_path_size(&layout.subdirectory("wal"))?;
    let snapshot_bytes = file_len_if_exists(&layout.node_segment_path())?
        + file_len_if_exists(&layout.edge_segment_path())?
        + file_len_if_exists(&layout.vector_segment_path())?;
    let delta_bytes = recursive_path_size(&layout.delta_directory())?;
    let index_bytes = file_len_if_exists(&layout.index_snapshot_path())?;
    let total_bytes = recursive_path_size(&layout.root_dir)?;

    let compaction_started = Instant::now();
    engine.compact()?;
    let compaction_elapsed_us = compaction_started.elapsed().as_micros();
    let post_compaction_total_bytes = recursive_path_size(&layout.root_dir)?;

    drop(engine);
    let recovery_started = Instant::now();
    let reopened = StorageEngine::open(&config)?;
    let recovery_elapsed_us = recovery_started.elapsed().as_micros();
    verify_storage_counts(&reopened, node_count)?;

    Ok(StorageFootprintReport {
        wal_bytes,
        snapshot_bytes,
        delta_bytes,
        index_bytes,
        total_bytes,
        post_compaction_total_bytes,
        compaction_elapsed_us,
        recovery_elapsed_us,
    })
}

struct Workload {
    nodes: Vec<NodeRecord>,
    edges: Vec<EdgeRecord>,
    start_node_id: NodeId,
    end_node_id: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScenarioProfile {
    Full,
    StorageOnly,
}

impl ScenarioProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::StorageOnly => "storage-only",
        }
    }

    fn includes_storage_scenarios(self) -> bool {
        true
    }

    fn includes_query_scenarios(self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkloadProfile {
    Standard,
    Compact,
}

impl WorkloadProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Compact => "compact",
        }
    }

    fn includes_vectors(self) -> bool {
        matches!(self, Self::Standard)
    }
}

impl Workload {
    fn snapshot_with_vector_index_config(&self, vector_index: &VectorIndexConfig) -> GraphSnapshot {
        GraphSnapshot {
            nodes: self
                .nodes
                .iter()
                .cloned()
                .map(|node| (node.id.clone(), node))
                .collect::<OrdMap<_, _>>(),
            edges: self
                .edges
                .iter()
                .cloned()
                .map(|edge| (edge.id.clone(), edge))
                .collect::<OrdMap<_, _>>(),
            indexes: GraphIndex::rebuild_with_config(&self.nodes, &self.edges, vector_index),
        }
    }
}

fn build_workload(node_count: usize, workload_profile: WorkloadProfile) -> BenchResult<Workload> {
    let mut nodes = Vec::with_capacity(node_count);
    let mut edges = Vec::with_capacity(node_count.saturating_sub(1));

    for index in 0..node_count {
        let mut node = NodeRecord::new(
            NodeId::new(format!("node_{index}"))?,
            if index % 5 == 0 { "profile" } else { "memory" },
        )?;
        node.properties.insert(
            "unique_key".to_owned(),
            PropertyValue::String(format!("unique_{index}")),
        );
        node.properties.insert(
            "timestamp".to_owned(),
            PropertyValue::Integer(1_000 + index as i64),
        );
        if matches!(workload_profile, WorkloadProfile::Standard) {
            node.properties.insert(
                "importance".to_owned(),
                PropertyValue::Float(((index % 10) as f64) / 10.0),
            );
            node.properties.insert(
                "confidence".to_owned(),
                PropertyValue::Float((((index + 3) % 10) as f64) / 10.0),
            );
            node.properties.insert(
                "embedding".to_owned(),
                PropertyValue::FloatList(vec![
                    1.0 - (index as f32 / node_count.max(1) as f32),
                    index as f32 / node_count.max(1) as f32,
                    ((index % 7) as f32) / 7.0,
                    ((index % 11) as f32) / 11.0,
                ]),
            );
        }
        nodes.push(node);
    }

    for index in 0..node_count.saturating_sub(1) {
        edges.push(EdgeRecord {
            id: EdgeId::new(format!("edge_{index}"))?,
            source: nodes[index].id.clone(),
            target: nodes[index + 1].id.clone(),
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        });
    }

    Ok(Workload {
        start_node_id: nodes
            .first()
            .map(|node| node.id.clone())
            .ok_or("workload must contain at least one node")?,
        end_node_id: nodes
            .get(node_count.saturating_sub(1).min(24))
            .map(|node| node.id.clone())
            .ok_or("workload must contain at least one node")?,
        nodes,
        edges,
    })
}

fn populate_compact_storage(engine: &mut StorageEngine, node_count: usize) -> BenchResult<()> {
    for range in batch_ranges(node_count) {
        engine.upsert_nodes(nodes_for_range(range)?)?;
    }
    for range in batch_ranges(node_count.saturating_sub(1)) {
        engine.upsert_edges(edges_for_range(range)?)?;
    }
    Ok(())
}

fn verify_storage_counts(engine: &StorageEngine, node_count: usize) -> BenchResult<()> {
    let expected_edges = node_count.saturating_sub(1);
    if engine.node_count() != node_count || engine.edge_count() != expected_edges {
        return Err(format!(
            "storage benchmark produced incorrect counts: expected=({node_count},{expected_edges}) actual=({},{})",
            engine.node_count(),
            engine.edge_count()
        )
        .into());
    }
    Ok(())
}

fn batch_ranges(total: usize) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < total {
        let end = (start + STORAGE_BATCH_SIZE).min(total);
        ranges.push(start..end);
        start = end;
    }
    ranges
}

fn reverse_batch_ranges(total: usize) -> Vec<std::ops::Range<usize>> {
    let mut ranges = batch_ranges(total);
    ranges.reverse();
    ranges
}

fn nodes_for_range(range: std::ops::Range<usize>) -> BenchResult<Vec<NodeRecord>> {
    let mut nodes = Vec::with_capacity(range.end.saturating_sub(range.start));
    for index in range {
        let mut node = NodeRecord::new(
            NodeId::new(format!("node_{index}"))?,
            if index % 5 == 0 { "profile" } else { "memory" },
        )?;
        node.properties.insert(
            "unique_key".to_owned(),
            PropertyValue::String(format!("unique_{index}")),
        );
        node.properties.insert(
            "timestamp".to_owned(),
            PropertyValue::Integer(1_000 + index as i64),
        );
        nodes.push(node);
    }
    Ok(nodes)
}

fn edges_for_range(range: std::ops::Range<usize>) -> BenchResult<Vec<EdgeRecord>> {
    let mut edges = Vec::with_capacity(range.end.saturating_sub(range.start));
    for index in range {
        edges.push(EdgeRecord {
            id: EdgeId::new(format!("edge_{index}"))?,
            source: NodeId::new(format!("node_{index}"))?,
            target: NodeId::new(format!("node_{}", index + 1))?,
            edge_type: "relates_to".to_owned(),
            properties: BTreeMap::new(),
        });
    }
    Ok(edges)
}

fn node_ids_for_range(range: std::ops::Range<usize>) -> BenchResult<Vec<NodeId>> {
    range
        .map(|index| NodeId::new(format!("node_{index}")).map_err(Into::into))
        .collect()
}

fn edge_ids_for_range(range: std::ops::Range<usize>) -> BenchResult<Vec<EdgeId>> {
    range
        .map(|index| EdgeId::new(format!("edge_{index}")).map_err(Into::into))
        .collect()
}

fn benchmark_storage_config(root_dir: PathBuf) -> AppConfig {
    let mut config = AppConfig::default();
    config.storage.root_dir = root_dir;
    config.wal.max_replay_bytes = config.wal.max_replay_bytes.max(BENCHMARK_WAL_REPLAY_BYTES);
    config
}

fn benchmark_exact_vector_index_config() -> VectorIndexConfig {
    let mut config = VectorIndexConfig::default();
    config.backend = VectorIndexBackendConfig::Exact;
    config
}

fn benchmark_hnsw_vector_index_config(hnsw_tuning: &HnswTuningReport) -> VectorIndexConfig {
    let mut config = VectorIndexConfig::default();
    config.backend = VectorIndexBackendConfig::Hnsw;
    // Force the benchmark to exercise the ANN backend even at small baseline scales.
    config.exact_fallback_threshold = 1;
    config.semantic_top_k = hnsw_tuning.semantic_top_k;
    config.hnsw_ef_search = hnsw_tuning.ef_search;
    config.hnsw_m = hnsw_tuning.m;
    config.hnsw_ef_construction = hnsw_tuning.ef_construction;
    config
}

fn measure_vector_index_footprint(
    workload: &Workload,
    hnsw_tuning: &HnswTuningReport,
) -> BenchResult<VectorIndexFootprintReport> {
    let tempdir = tempdir()?;
    let config = benchmark_storage_config(tempdir.path().join("data"));
    let mut engine = StorageEngine::open(&config)?;
    engine.upsert_nodes(workload.nodes.clone())?;
    engine.upsert_edges(workload.edges.clone())?;

    let layout = engine.layout().clone();
    let vector_index_config = benchmark_hnsw_vector_index_config(hnsw_tuning);

    let build_started = Instant::now();
    let index =
        GraphIndex::rebuild_with_config(&workload.nodes, &workload.edges, &vector_index_config);
    let hnsw_build_elapsed_us = build_started.elapsed().as_micros();
    index.persist_vector_index(
        &layout.vector_index_manifest_path(),
        &layout.vector_index_graph_path(),
        &layout.vector_index_vectors_path(),
        engine.latest_applied_lsn(),
    )?;

    let hnsw_index_bytes = file_len_if_exists(&layout.vector_index_manifest_path())?
        + file_len_if_exists(&layout.vector_index_graph_path())?
        + file_len_if_exists(&layout.vector_index_vectors_path())?;

    let reload_started = Instant::now();
    let reloaded = GraphIndex::rebuild_with_config_and_vector_index_load(
        &workload.nodes,
        &workload.edges,
        &vector_index_config,
        Some(undr9_index::VectorIndexLoadConfig {
            manifest_path: &layout.vector_index_manifest_path(),
            graph_path: &layout.vector_index_graph_path(),
            vectors_path: &layout.vector_index_vectors_path(),
            expected_last_applied_lsn: engine.latest_applied_lsn(),
        }),
    );
    let hnsw_reload_elapsed_us = reload_started.elapsed().as_micros();

    if reloaded.snapshot().vector_backend != "hnsw" || !reloaded.snapshot().vector_runtime_ready {
        return Err("vector index footprint benchmark failed to reload an HNSW runtime".into());
    }

    Ok(VectorIndexFootprintReport {
        hnsw_index_bytes,
        hnsw_build_elapsed_us,
        hnsw_reload_elapsed_us,
    })
}

fn benchmark_hnsw_tuning_from_args(args: &Args) -> HnswTuningReport {
    let defaults = benchmark_default_hnsw_tuning();
    HnswTuningReport {
        semantic_top_k: args.hnsw_semantic_top_k.unwrap_or(defaults.semantic_top_k),
        ef_search: args.hnsw_ef_search.unwrap_or(defaults.ef_search),
        m: args.hnsw_m.unwrap_or(defaults.m),
        ef_construction: args
            .hnsw_ef_construction
            .unwrap_or(defaults.ef_construction),
    }
}

fn benchmark_default_hnsw_tuning() -> HnswTuningReport {
    let defaults = VectorIndexConfig::default();
    HnswTuningReport {
        // Benchmarks use a wider semantic pool than the product defaults so published
        // exact-vs-HNSW claims reflect a better latency/overlap tradeoff.
        semantic_top_k: 250,
        ef_search: 128,
        m: defaults.hnsw_m,
        ef_construction: defaults.hnsw_ef_construction,
    }
}

fn parse_scenario_profile(raw: &str) -> BenchResult<ScenarioProfile> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "full" => Ok(ScenarioProfile::Full),
        "storage-only" | "storage_only" | "storage" => Ok(ScenarioProfile::StorageOnly),
        other => Err(format!("unsupported scenario profile '{other}'").into()),
    }
}

fn parse_workload_profile(raw: &str) -> BenchResult<WorkloadProfile> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "standard" => Ok(WorkloadProfile::Standard),
        "compact" => Ok(WorkloadProfile::Compact),
        other => Err(format!("unsupported workload profile '{other}'").into()),
    }
}

fn recursive_path_size(path: &std::path::Path) -> BenchResult<u64> {
    if !path.exists() {
        return Ok(0);
    }
    if path.is_file() {
        return Ok(path.metadata()?.len());
    }

    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        total += recursive_path_size(&entry.path())?;
    }
    Ok(total)
}

fn file_len_if_exists(path: &std::path::Path) -> BenchResult<u64> {
    if path.exists() {
        Ok(path.metadata()?.len())
    } else {
        Ok(0)
    }
}

fn current_process_peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return None;
    }

    let usage = unsafe { usage.assume_init() };
    #[cfg(target_os = "macos")]
    {
        Some(usage.ru_maxrss as u64)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Some((usage.ru_maxrss as u64) * 1024)
    }
}
