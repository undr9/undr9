use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry::global;
use opentelemetry::trace::Status as OtelStatus;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use undr9_common::{Result, Undr9Error};

static TRACING_INITIALIZED: OnceLock<()> = OnceLock::new();
static OTEL_TRACER_PROVIDER: OnceLock<Mutex<Option<SdkTracerProvider>>> = OnceLock::new();

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyHistogramSnapshot {
    pub observations_total: u64,
    pub sum_ms: u64,
    pub le_10ms_total: u64,
    pub le_50ms_total: u64,
    pub le_250ms_total: u64,
    pub le_1000ms_total: u64,
    pub gt_1000ms_total: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCounterSnapshot {
    pub code: String,
    pub status: u16,
    pub count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceCounters {
    pub query_requests_total: u64,
    pub write_requests_total: u64,
    pub maintenance_operations_total: u64,
    pub audit_events_total: u64,
    pub query_latency: LatencyHistogramSnapshot,
    pub transaction_latency: LatencyHistogramSnapshot,
    pub traversal_latency: LatencyHistogramSnapshot,
    pub vector_search_latency: LatencyHistogramSnapshot,
    pub ranked_retrieval_latency: LatencyHistogramSnapshot,
    pub compaction_latency: LatencyHistogramSnapshot,
    pub recovery_duration: LatencyHistogramSnapshot,
    pub wal_lag_records: u64,
    pub wal_replay_latency_ms: u64,
    pub memory_pressure_bytes: u64,
    pub cache_pressure_bytes: u64,
    pub retrieval_score_bucket_low_total: u64,
    pub retrieval_score_bucket_medium_total: u64,
    pub retrieval_score_bucket_high_total: u64,
    pub retrieval_score_bucket_top_total: u64,
    pub error_counters: Vec<ErrorCounterSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub service_name: String,
    pub ready: bool,
    pub node_count: usize,
    pub edge_count: usize,
    pub active_transactions: usize,
    pub current_revision: u64,
    pub latest_applied_lsn: u64,
    pub checkpoint_dirty: bool,
    pub pending_checkpoint_entries: usize,
    pub endpoint_metrics: Vec<EndpointMetricsSnapshot>,
    pub counters: ServiceCounters,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointMetricsSnapshot {
    pub method: String,
    pub route: String,
    pub requests_total: u64,
    pub responses_2xx_total: u64,
    pub responses_4xx_total: u64,
    pub responses_5xx_total: u64,
    pub latency_ms_total: u64,
    pub latency_le_10ms_total: u64,
    pub latency_le_50ms_total: u64,
    pub latency_le_250ms_total: u64,
    pub latency_gt_250ms_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredLogEvent {
    pub timestamp_ms: u128,
    pub level: String,
    pub target: String,
    pub message: String,
    pub fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub timestamp_ms: u128,
    pub actor: String,
    pub action: String,
    pub resource: String,
    pub outcome: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeObservabilityConfig {
    pub log_level: String,
    pub tracing_enabled: bool,
    pub tracing_json: bool,
    pub otlp_enabled: bool,
    pub otlp_protocol: String,
    pub otlp_endpoint: String,
    pub otlp_headers: String,
    pub otlp_timeout_ms: u64,
}

impl MetricsSnapshot {
    pub fn render_prometheus(&self) -> String {
        let ready_value = u8::from(self.ready);
        let checkpoint_dirty_value = u8::from(self.checkpoint_dirty);

        let mut rendered = format!(
            "# HELP undr9_service_ready Whether the UNDR9 service is ready.\n\
             # TYPE undr9_service_ready gauge\n\
             undr9_service_ready{{service=\"{service}\"}} {ready}\n\
             # HELP undr9_nodes_total Number of nodes currently loaded.\n\
             # TYPE undr9_nodes_total gauge\n\
             undr9_nodes_total{{service=\"{service}\"}} {nodes}\n\
             # HELP undr9_edges_total Number of edges currently loaded.\n\
             # TYPE undr9_edges_total gauge\n\
             undr9_edges_total{{service=\"{service}\"}} {edges}\n\
             # HELP undr9_active_transactions Number of active transactions.\n\
             # TYPE undr9_active_transactions gauge\n\
             undr9_active_transactions{{service=\"{service}\"}} {active_transactions}\n\
             # HELP undr9_current_revision Current committed revision.\n\
             # TYPE undr9_current_revision gauge\n\
             undr9_current_revision{{service=\"{service}\"}} {current_revision}\n\
             # HELP undr9_latest_applied_lsn Latest applied WAL sequence number.\n\
             # TYPE undr9_latest_applied_lsn gauge\n\
             undr9_latest_applied_lsn{{service=\"{service}\"}} {latest_applied_lsn}\n\
             # HELP undr9_checkpoint_dirty Whether checkpoint publication is pending.\n\
             # TYPE undr9_checkpoint_dirty gauge\n\
             undr9_checkpoint_dirty{{service=\"{service}\"}} {checkpoint_dirty}\n\
             # HELP undr9_pending_checkpoint_entries Number of unpublished checkpoint entries.\n\
             # TYPE undr9_pending_checkpoint_entries gauge\n\
             undr9_pending_checkpoint_entries{{service=\"{service}\"}} {pending_checkpoint_entries}\n\
             # HELP undr9_query_requests_total Total query requests handled.\n\
             # TYPE undr9_query_requests_total counter\n\
             undr9_query_requests_total{{service=\"{service}\"}} {queries}\n\
             # HELP undr9_write_requests_total Total write requests handled.\n\
             # TYPE undr9_write_requests_total counter\n\
             undr9_write_requests_total{{service=\"{service}\"}} {writes}\n\
             # HELP undr9_maintenance_operations_total Total maintenance operations handled.\n\
             # TYPE undr9_maintenance_operations_total counter\n\
             undr9_maintenance_operations_total{{service=\"{service}\"}} {maintenance}\n\
             # HELP undr9_audit_events_total Total audit events written.\n\
             # TYPE undr9_audit_events_total counter\n\
             undr9_audit_events_total{{service=\"{service}\"}} {audit}\n\
             # HELP undr9_wal_lag_records WAL records pending durable checkpoint publication.\n\
             # TYPE undr9_wal_lag_records gauge\n\
             undr9_wal_lag_records{{service=\"{service}\"}} {wal_lag_records}\n\
             # HELP undr9_wal_replay_latency_ms Last WAL replay latency in milliseconds.\n\
             # TYPE undr9_wal_replay_latency_ms gauge\n\
             undr9_wal_replay_latency_ms{{service=\"{service}\"}} {wal_replay_latency}\n\
             # HELP undr9_memory_pressure_bytes Estimated in-process memory pressure in bytes.\n\
             # TYPE undr9_memory_pressure_bytes gauge\n\
             undr9_memory_pressure_bytes{{service=\"{service}\"}} {memory_pressure_bytes}\n\
             # HELP undr9_cache_pressure_bytes Estimated cached graph snapshot pressure in bytes.\n\
             # TYPE undr9_cache_pressure_bytes gauge\n\
             undr9_cache_pressure_bytes{{service=\"{service}\"}} {cache_pressure_bytes}\n\
             # HELP undr9_retrieval_score_distribution Retrieval score distribution buckets.\n\
             # TYPE undr9_retrieval_score_distribution counter\n\
             undr9_retrieval_score_distribution{{service=\"{service}\",bucket=\"0.00-0.25\"}} {score_low}\n\
             undr9_retrieval_score_distribution{{service=\"{service}\",bucket=\"0.25-0.50\"}} {score_medium}\n\
             undr9_retrieval_score_distribution{{service=\"{service}\",bucket=\"0.50-0.75\"}} {score_high}\n\
             undr9_retrieval_score_distribution{{service=\"{service}\",bucket=\"0.75-1.00\"}} {score_top}\n",
            service = self.service_name,
            ready = ready_value,
            nodes = self.node_count,
            edges = self.edge_count,
            active_transactions = self.active_transactions,
            current_revision = self.current_revision,
            latest_applied_lsn = self.latest_applied_lsn,
            checkpoint_dirty = checkpoint_dirty_value,
            pending_checkpoint_entries = self.pending_checkpoint_entries,
            queries = self.counters.query_requests_total,
            writes = self.counters.write_requests_total,
            maintenance = self.counters.maintenance_operations_total,
            audit = self.counters.audit_events_total,
            wal_lag_records = self.counters.wal_lag_records,
            wal_replay_latency = self.counters.wal_replay_latency_ms,
            memory_pressure_bytes = self.counters.memory_pressure_bytes,
            cache_pressure_bytes = self.counters.cache_pressure_bytes,
            score_low = self.counters.retrieval_score_bucket_low_total,
            score_medium = self.counters.retrieval_score_bucket_medium_total,
            score_high = self.counters.retrieval_score_bucket_high_total,
            score_top = self.counters.retrieval_score_bucket_top_total
        );

        rendered.push_str(&render_histogram(
            &self.service_name,
            "undr9_query_latency_ms",
            "End-to-end query latency in milliseconds.",
            &self.counters.query_latency,
            &[("subsystem", "query")],
        ));
        rendered.push_str(&render_histogram(
            &self.service_name,
            "undr9_transaction_latency_ms",
            "Transaction lifecycle latency in milliseconds.",
            &self.counters.transaction_latency,
            &[("subsystem", "transaction")],
        ));
        rendered.push_str(&render_histogram(
            &self.service_name,
            "undr9_traversal_latency_ms",
            "Traversal query latency in milliseconds.",
            &self.counters.traversal_latency,
            &[("subsystem", "query")],
        ));
        rendered.push_str(&render_histogram(
            &self.service_name,
            "undr9_vector_search_latency_ms",
            "Vector search latency in milliseconds.",
            &self.counters.vector_search_latency,
            &[("subsystem", "query")],
        ));
        rendered.push_str(&render_histogram(
            &self.service_name,
            "undr9_ranked_retrieval_latency_ms",
            "Ranked retrieval latency in milliseconds.",
            &self.counters.ranked_retrieval_latency,
            &[("subsystem", "query")],
        ));
        rendered.push_str(&render_histogram(
            &self.service_name,
            "undr9_compaction_latency_ms",
            "Compaction latency in milliseconds.",
            &self.counters.compaction_latency,
            &[("subsystem", "maintenance")],
        ));
        rendered.push_str(&render_histogram(
            &self.service_name,
            "undr9_recovery_duration_ms",
            "Recovery duration in milliseconds.",
            &self.counters.recovery_duration,
            &[("subsystem", "recovery")],
        ));

        if !self.endpoint_metrics.is_empty() {
            rendered.push_str(
                "# HELP undr9_http_requests_total Total HTTP requests by route, method, and status class.\n\
                 # TYPE undr9_http_requests_total counter\n\
                 # HELP undr9_http_request_duration_ms Request latency histogram by route and method.\n\
                 # TYPE undr9_http_request_duration_ms histogram\n",
            );

            for endpoint in &self.endpoint_metrics {
                let count = endpoint.requests_total;
                let le_10 = endpoint.latency_le_10ms_total;
                let le_50 = le_10 + endpoint.latency_le_50ms_total;
                let le_250 = le_50 + endpoint.latency_le_250ms_total;
                rendered.push_str(&format!(
                    "undr9_http_requests_total{{service=\"{service}\",method=\"{method}\",route=\"{route}\",status_class=\"2xx\"}} {responses_2xx}\n\
                     undr9_http_requests_total{{service=\"{service}\",method=\"{method}\",route=\"{route}\",status_class=\"4xx\"}} {responses_4xx}\n\
                     undr9_http_requests_total{{service=\"{service}\",method=\"{method}\",route=\"{route}\",status_class=\"5xx\"}} {responses_5xx}\n\
                     undr9_http_request_duration_ms_bucket{{service=\"{service}\",method=\"{method}\",route=\"{route}\",le=\"10\"}} {le_10}\n\
                     undr9_http_request_duration_ms_bucket{{service=\"{service}\",method=\"{method}\",route=\"{route}\",le=\"50\"}} {le_50}\n\
                     undr9_http_request_duration_ms_bucket{{service=\"{service}\",method=\"{method}\",route=\"{route}\",le=\"250\"}} {le_250}\n\
                     undr9_http_request_duration_ms_bucket{{service=\"{service}\",method=\"{method}\",route=\"{route}\",le=\"+Inf\"}} {count}\n\
                     undr9_http_request_duration_ms_sum{{service=\"{service}\",method=\"{method}\",route=\"{route}\"}} {latency_sum}\n\
                     undr9_http_request_duration_ms_count{{service=\"{service}\",method=\"{method}\",route=\"{route}\"}} {count}\n",
                    service = self.service_name,
                    method = endpoint.method,
                    route = endpoint.route,
                    responses_2xx = endpoint.responses_2xx_total,
                    responses_4xx = endpoint.responses_4xx_total,
                    responses_5xx = endpoint.responses_5xx_total,
                    le_10 = le_10,
                    le_50 = le_50,
                    le_250 = le_250,
                    count = count,
                    latency_sum = endpoint.latency_ms_total,
                ));
            }
        }

        if !self.counters.error_counters.is_empty() {
            rendered.push_str(
                "# HELP undr9_errors_total Total API errors by stable error code and HTTP status.\n\
                 # TYPE undr9_errors_total counter\n",
            );
            for counter in &self.counters.error_counters {
                rendered.push_str(&format!(
                    "undr9_errors_total{{service=\"{service}\",code=\"{code}\",status=\"{status}\"}} {count}\n",
                    service = self.service_name,
                    code = counter.code,
                    status = counter.status,
                    count = counter.count,
                ));
            }
        }

        rendered
    }
}

fn render_histogram(
    service: &str,
    metric_name: &str,
    help: &str,
    histogram: &LatencyHistogramSnapshot,
    extra_labels: &[(&str, &str)],
) -> String {
    let le_10 = histogram.le_10ms_total;
    let le_50 = le_10 + histogram.le_50ms_total;
    let le_250 = le_50 + histogram.le_250ms_total;
    let le_1000 = le_250 + histogram.le_1000ms_total;
    let count = histogram.observations_total;
    let labels = render_labels(service, extra_labels);
    format!(
        "# HELP {metric_name} {help}\n\
         # TYPE {metric_name} histogram\n\
         {metric_name}_bucket{{{labels},le=\"10\"}} {le_10}\n\
         {metric_name}_bucket{{{labels},le=\"50\"}} {le_50}\n\
         {metric_name}_bucket{{{labels},le=\"250\"}} {le_250}\n\
         {metric_name}_bucket{{{labels},le=\"1000\"}} {le_1000}\n\
         {metric_name}_bucket{{{labels},le=\"+Inf\"}} {count}\n\
         {metric_name}_sum{{{labels}}} {sum}\n\
         {metric_name}_count{{{labels}}} {count}\n",
        metric_name = metric_name,
        help = help,
        labels = labels,
        le_10 = le_10,
        le_50 = le_50,
        le_250 = le_250,
        le_1000 = le_1000,
        count = count,
        sum = histogram.sum_ms,
    )
}

fn render_labels(service: &str, extra_labels: &[(&str, &str)]) -> String {
    let mut labels = vec![format!("service=\"{service}\"")];
    labels.extend(
        extra_labels
            .iter()
            .map(|(key, value)| format!("{key}=\"{value}\"")),
    );
    labels.join(",")
}

impl StructuredLogEvent {
    pub fn new(
        level: impl Into<String>,
        target: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            timestamp_ms: now_epoch_ms(),
            level: level.into(),
            target: target.into(),
            message: message.into(),
            fields: BTreeMap::new(),
        }
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    pub fn to_json_line(&self) -> Result<String> {
        let mut payload = serde_json::to_string(self).map_err(|error| {
            Undr9Error::Serialization(format!("failed to serialize structured log event: {error}"))
        })?;
        payload.push('\n');
        Ok(payload)
    }

    pub fn emit_via_tracing(&self) {
        match self.level.to_ascii_uppercase().as_str() {
            "TRACE" => tracing::trace!(
                structured_target = %self.target,
                message = %self.message,
                fields = ?self.fields
            ),
            "DEBUG" => tracing::debug!(
                structured_target = %self.target,
                message = %self.message,
                fields = ?self.fields
            ),
            "WARN" => tracing::warn!(
                structured_target = %self.target,
                message = %self.message,
                fields = ?self.fields
            ),
            "ERROR" => tracing::error!(
                structured_target = %self.target,
                message = %self.message,
                fields = ?self.fields
            ),
            _ => tracing::info!(
                structured_target = %self.target,
                message = %self.message,
                fields = ?self.fields
            ),
        }
    }
}

impl AuditEvent {
    pub fn new(
        actor: impl Into<String>,
        action: impl Into<String>,
        resource: impl Into<String>,
        outcome: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            timestamp_ms: now_epoch_ms(),
            actor: actor.into(),
            action: action.into(),
            resource: resource.into(),
            outcome: outcome.into(),
            detail: detail.into(),
        }
    }

    pub fn to_json_line(&self) -> Result<String> {
        let mut payload = serde_json::to_string(self).map_err(|error| {
            Undr9Error::Serialization(format!("failed to serialize audit event: {error}"))
        })?;
        payload.push('\n');
        Ok(payload)
    }
}

pub fn append_audit_event(path: &Path, event: &AuditEvent) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            Undr9Error::Io(format!(
                "failed to create audit log parent directory '{}': {error}",
                parent.display()
            ))
        })?;
    }

    let payload = event.to_json_line()?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| {
            Undr9Error::Io(format!(
                "failed to open audit log '{}': {error}",
                path.display()
            ))
        })?;
    file.write_all(payload.as_bytes())
        .map_err(|error| Undr9Error::Io(format!("failed to append audit log: {error}")))?;
    Ok(())
}

pub fn export_audit_events(path: &Path, limit: usize) -> Result<Vec<AuditEvent>> {
    let payload = match fs::read_to_string(path) {
        Ok(payload) => payload,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(Undr9Error::Io(format!(
                "failed to read audit log '{}': {error}",
                path.display()
            )))
        }
    };

    let mut events = Vec::new();
    for line in payload
        .lines()
        .rev()
        .take(limit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
        if line.trim().is_empty() {
            continue;
        }
        let event = serde_json::from_str::<AuditEvent>(line).map_err(|error| {
            Undr9Error::Serialization(format!(
                "failed to deserialize audit log entry from '{}': {error}",
                path.display()
            ))
        })?;
        events.push(event);
    }
    Ok(events)
}

pub fn prune_audit_log(path: &Path, retain_entries: usize) -> Result<usize> {
    let payload = match fs::read_to_string(path) {
        Ok(payload) => payload,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(Undr9Error::Io(format!(
                "failed to read audit log '{}': {error}",
                path.display()
            )))
        }
    };

    let lines = payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if lines.len() <= retain_entries {
        return Ok(0);
    }

    let kept = &lines[lines.len() - retain_entries..];
    let mut rewritten = kept.join("\n");
    rewritten.push('\n');
    fs::write(path, rewritten).map_err(|error| {
        Undr9Error::Io(format!(
            "failed to rewrite pruned audit log '{}': {error}",
            path.display()
        ))
    })?;
    Ok(lines.len() - retain_entries)
}

pub fn now_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis()
}

pub fn initialize_tracing(service_name: &str, config: &RuntimeObservabilityConfig) -> Result<bool> {
    if !config.tracing_enabled {
        return Ok(false);
    }
    if TRACING_INITIALIZED.get().is_some() {
        return Ok(false);
    }

    let filter = EnvFilter::try_new(config.log_level.clone()).map_err(|error| {
        Undr9Error::Validation(format!(
            "invalid observability.log_level '{}': {error}",
            config.log_level
        ))
    })?;
    global::set_text_map_propagator(TraceContextPropagator::new());

    if config.otlp_enabled {
        let provider = build_otlp_tracer_provider(service_name, config)?;
        if config.tracing_json {
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().json().with_target(true))
                .with(
                    tracing_opentelemetry::layer()
                        .with_tracer(provider.tracer(service_name.to_owned())),
                )
                .try_init()
        } else {
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    tracing_opentelemetry::layer()
                        .with_tracer(provider.tracer(service_name.to_owned())),
                )
                .with(tracing_subscriber::fmt::layer().with_target(true))
                .try_init()
        }
        .map_err(|error| {
            Undr9Error::Conflict(format!("failed to initialize tracing subscriber: {error}"))
        })?;
        global::set_tracer_provider(provider.clone());
        otel_tracer_provider()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .replace(provider);
    } else if config.tracing_json {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json().with_target(true))
            .try_init()
            .map_err(|error| {
                Undr9Error::Conflict(format!("failed to initialize tracing subscriber: {error}"))
            })?;
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_target(true))
            .try_init()
            .map_err(|error| {
                Undr9Error::Conflict(format!("failed to initialize tracing subscriber: {error}"))
            })?;
    }
    let _ = TRACING_INITIALIZED.set(());
    tracing::info!(
        service_name = service_name,
        log_level = config.log_level,
        tracing_json = config.tracing_json,
        otlp_enabled = config.otlp_enabled,
        otlp_protocol = config.otlp_protocol,
        otlp_endpoint = config.otlp_endpoint,
        "observability initialized"
    );
    Ok(true)
}

pub fn shutdown_tracing() -> Result<()> {
    let mut provider = otel_tracer_provider()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(provider) = provider.take() {
        provider.shutdown().map_err(|error| {
            Undr9Error::Conflict(format!("failed to shutdown otlp exporter: {error}"))
        })?;
    }
    Ok(())
}

pub fn annotate_active_span_success() {
    let span = tracing::Span::current();
    span.set_status(OtelStatus::Ok);
}

pub fn annotate_active_span_error(error_code: &str, http_status: u16, message: impl Into<String>) {
    let span = tracing::Span::current();
    span.set_attribute("undr9.error_code", error_code.to_owned());
    span.set_attribute("undr9.http_status", i64::from(http_status));
    span.set_status(OtelStatus::error(message.into()));
}

fn otel_tracer_provider() -> &'static Mutex<Option<SdkTracerProvider>> {
    OTEL_TRACER_PROVIDER.get_or_init(|| Mutex::new(None))
}

fn build_otlp_tracer_provider(
    service_name: &str,
    config: &RuntimeObservabilityConfig,
) -> Result<SdkTracerProvider> {
    let protocol = normalize_otlp_protocol(&config.otlp_protocol)?;
    let headers = parse_otlp_headers(&config.otlp_headers)?;
    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", service_name.to_owned()),
            KeyValue::new("service.namespace", "undr9"),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();
    let exporter = match protocol {
        OtlpProtocol::Grpc => opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.otlp_endpoint.clone())
            .with_timeout(std::time::Duration::from_millis(config.otlp_timeout_ms))
            .with_metadata(build_otlp_metadata(&headers)?)
            .build()
            .map_err(|error| {
                Undr9Error::Conflict(format!("failed to build OTLP gRPC exporter: {error}"))
            })?,
        OtlpProtocol::HttpBinary => opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(config.otlp_endpoint.clone())
            .with_timeout(std::time::Duration::from_millis(config.otlp_timeout_ms))
            .with_headers(headers)
            .build()
            .map_err(|error| {
                Undr9Error::Conflict(format!("failed to build OTLP HTTP exporter: {error}"))
            })?,
    };

    Ok(SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OtlpProtocol {
    Grpc,
    HttpBinary,
}

fn normalize_otlp_protocol(protocol: &str) -> Result<OtlpProtocol> {
    match protocol.trim().to_ascii_lowercase().as_str() {
        "" | "grpc" => Ok(OtlpProtocol::Grpc),
        "http" | "http/protobuf" | "http_binary" | "http-binary" => Ok(OtlpProtocol::HttpBinary),
        other => Err(Undr9Error::Validation(format!(
            "unsupported OTLP protocol '{other}'"
        ))),
    }
}

fn parse_otlp_headers(headers: &str) -> Result<HashMap<String, String>> {
    let mut parsed = HashMap::new();
    if headers.trim().is_empty() {
        return Ok(parsed);
    }

    for entry in headers.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (key, value) = entry.split_once('=').ok_or_else(|| {
            Undr9Error::Validation(format!(
                "invalid observability.otlp_headers entry '{entry}', expected key=value"
            ))
        })?;
        parsed.insert(key.trim().to_owned(), value.trim().to_owned());
    }
    Ok(parsed)
}

fn build_otlp_metadata(
    headers: &HashMap<String, String>,
) -> Result<opentelemetry_otlp::tonic_types::metadata::MetadataMap> {
    let mut http_headers = http::HeaderMap::new();
    for (key, value) in headers {
        let header_name =
            http::header::HeaderName::from_bytes(key.as_bytes()).map_err(|error| {
                Undr9Error::Validation(format!(
                    "invalid observability.otlp_headers key '{key}': {error}"
                ))
            })?;
        let header_value = http::HeaderValue::from_str(value).map_err(|error| {
            Undr9Error::Validation(format!(
                "invalid observability.otlp_headers value for '{key}': {error}"
            ))
        })?;
        http_headers.insert(header_name, header_value);
    }
    Ok(opentelemetry_otlp::tonic_types::metadata::MetadataMap::from_headers(http_headers))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        append_audit_event, build_otlp_metadata, export_audit_events, normalize_otlp_protocol,
        parse_otlp_headers, prune_audit_log, AuditEvent, EndpointMetricsSnapshot,
        ErrorCounterSnapshot, LatencyHistogramSnapshot, MetricsSnapshot, OtlpProtocol,
        ServiceCounters, StructuredLogEvent,
    };

    #[test]
    fn renders_prometheus_metrics() {
        let snapshot = MetricsSnapshot {
            service_name: "undr9".to_owned(),
            ready: true,
            node_count: 2,
            edge_count: 1,
            active_transactions: 3,
            current_revision: 12,
            latest_applied_lsn: 11,
            checkpoint_dirty: true,
            pending_checkpoint_entries: 4,
            endpoint_metrics: vec![EndpointMetricsSnapshot {
                method: "GET".to_owned(),
                route: "/healthz".to_owned(),
                requests_total: 3,
                responses_2xx_total: 3,
                responses_4xx_total: 0,
                responses_5xx_total: 0,
                latency_ms_total: 18,
                latency_le_10ms_total: 2,
                latency_le_50ms_total: 1,
                latency_le_250ms_total: 0,
                latency_gt_250ms_total: 0,
            }],
            counters: ServiceCounters {
                query_requests_total: 3,
                write_requests_total: 4,
                maintenance_operations_total: 5,
                audit_events_total: 6,
                query_latency: LatencyHistogramSnapshot {
                    observations_total: 2,
                    sum_ms: 7,
                    le_10ms_total: 1,
                    le_50ms_total: 1,
                    le_250ms_total: 0,
                    le_1000ms_total: 0,
                    gt_1000ms_total: 0,
                },
                transaction_latency: LatencyHistogramSnapshot::default(),
                traversal_latency: LatencyHistogramSnapshot {
                    observations_total: 1,
                    sum_ms: 8,
                    le_10ms_total: 1,
                    le_50ms_total: 0,
                    le_250ms_total: 0,
                    le_1000ms_total: 0,
                    gt_1000ms_total: 0,
                },
                vector_search_latency: LatencyHistogramSnapshot {
                    observations_total: 1,
                    sum_ms: 9,
                    le_10ms_total: 1,
                    le_50ms_total: 0,
                    le_250ms_total: 0,
                    le_1000ms_total: 0,
                    gt_1000ms_total: 0,
                },
                ranked_retrieval_latency: LatencyHistogramSnapshot::default(),
                compaction_latency: LatencyHistogramSnapshot {
                    observations_total: 1,
                    sum_ms: 10,
                    le_10ms_total: 1,
                    le_50ms_total: 0,
                    le_250ms_total: 0,
                    le_1000ms_total: 0,
                    gt_1000ms_total: 0,
                },
                recovery_duration: LatencyHistogramSnapshot {
                    observations_total: 1,
                    sum_ms: 11,
                    le_10ms_total: 0,
                    le_50ms_total: 1,
                    le_250ms_total: 0,
                    le_1000ms_total: 0,
                    gt_1000ms_total: 0,
                },
                wal_lag_records: 2,
                wal_replay_latency_ms: 11,
                memory_pressure_bytes: 1024,
                cache_pressure_bytes: 2048,
                retrieval_score_bucket_low_total: 1,
                retrieval_score_bucket_medium_total: 2,
                retrieval_score_bucket_high_total: 3,
                retrieval_score_bucket_top_total: 4,
                error_counters: vec![ErrorCounterSnapshot {
                    code: "unauthorized".to_owned(),
                    status: 401,
                    count: 2,
                }],
            },
        };

        let rendered = snapshot.render_prometheus();
        assert!(rendered.contains("undr9_service_ready"));
        assert!(rendered.contains("undr9_active_transactions"));
        assert!(rendered.contains("undr9_current_revision"));
        assert!(rendered.contains("undr9_pending_checkpoint_entries"));
        assert!(rendered.contains("undr9_query_requests_total"));
        assert!(rendered.contains("undr9_query_latency_ms_bucket"));
        assert!(rendered.contains("undr9_errors_total"));
        assert!(rendered.contains("undr9_http_requests_total"));
        assert!(rendered.contains("undr9_http_request_duration_ms_bucket"));
        assert!(rendered.contains("service=\"undr9\""));
    }

    #[test]
    fn serializes_structured_logs() {
        let payload = StructuredLogEvent::new("INFO", "undr9_api", "request complete")
            .with_field("path", "/healthz")
            .to_json_line()
            .expect("log event should serialize");

        assert!(payload.contains("\"level\":\"INFO\""));
        assert!(payload.contains("\"path\":\"/healthz\""));
    }

    #[test]
    fn appends_audit_events_to_file() {
        let tempdir = tempdir().expect("tempdir should be created");
        let path = tempdir.path().join("audit.log");
        let event = AuditEvent::new("admin", "backup", "database", "success", "backup created");

        append_audit_event(&path, &event).expect("audit event should be written");

        let payload = std::fs::read_to_string(path).expect("audit log should exist");
        assert!(payload.contains("\"action\":\"backup\""));
    }

    #[test]
    fn exports_and_prunes_audit_events() {
        let tempdir = tempdir().expect("tempdir should be created");
        let path = tempdir.path().join("audit.log");
        for index in 0..4 {
            append_audit_event(
                &path,
                &AuditEvent::new(
                    "admin",
                    format!("action_{index}"),
                    "storage",
                    "success",
                    "detail",
                ),
            )
            .expect("audit event should be written");
        }

        let exported = export_audit_events(&path, 2).expect("audit export should succeed");
        assert_eq!(exported.len(), 2);
        assert_eq!(exported[0].action, "action_2");
        assert_eq!(exported[1].action, "action_3");

        let removed = prune_audit_log(&path, 2).expect("audit log prune should succeed");
        assert_eq!(removed, 2);

        let remaining = export_audit_events(&path, 10).expect("audit export should succeed");
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].action, "action_2");
        assert_eq!(remaining[1].action, "action_3");
    }

    #[test]
    fn parses_otlp_headers_and_protocol_variants() {
        let headers = parse_otlp_headers("authorization=Bearer token,x-scope-orgid=tenant")
            .expect("headers should parse");
        assert_eq!(
            headers.get("authorization"),
            Some(&"Bearer token".to_owned())
        );
        assert_eq!(headers.get("x-scope-orgid"), Some(&"tenant".to_owned()));

        assert!(matches!(
            normalize_otlp_protocol("grpc").expect("grpc protocol should parse"),
            OtlpProtocol::Grpc
        ));
        assert!(matches!(
            normalize_otlp_protocol("http/protobuf").expect("http protocol should parse"),
            OtlpProtocol::HttpBinary
        ));

        let metadata = build_otlp_metadata(&headers).expect("metadata should build");
        assert_eq!(
            metadata
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token")
        );
    }
}
