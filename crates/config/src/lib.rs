use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use undr9_common::{Result, Undr9Error};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub wal: WalConfig,
    pub auth: AuthConfig,
    pub maintenance: MaintenanceConfig,
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerConfig {
    pub bind_address: String,
    pub request_timeout_ms: u64,
    pub max_request_body_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub root_dir: PathBuf,
    pub create_if_missing: bool,
    pub storage_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalConfig {
    pub segment_size_bytes: u64,
    pub fsync_on_write: bool,
    pub max_replay_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthConfig {
    pub enabled: bool,
    pub bootstrap_admin_username: String,
    pub admin_api_key: String,
    pub writer_api_key: String,
    pub reader_api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MaintenanceConfig {
    pub max_node_count: usize,
    pub max_edge_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObservabilityConfig {
    pub log_level: String,
    pub metrics_enabled: bool,
    pub tracing_enabled: bool,
    pub tracing_json: bool,
    pub otlp_enabled: bool,
    pub otlp_protocol: String,
    pub otlp_endpoint: String,
    pub otlp_headers: String,
    pub otlp_timeout_ms: u64,
    pub audit_log_retention_entries: usize,
    pub audit_log_export_limit: usize,
}

const LEGACY_DEV_ADMIN_KEY: &str = "undr9-dev-admin-key";
const LEGACY_DEV_WRITER_KEY: &str = "undr9-dev-writer-key";
const LEGACY_DEV_READER_KEY: &str = "undr9-dev-reader-key";
const MIN_API_KEY_LENGTH: usize = 16;

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                bind_address: "127.0.0.1:8080".to_owned(),
                request_timeout_ms: 5_000,
                max_request_body_bytes: 1_048_576,
            },
            storage: StorageConfig {
                root_dir: PathBuf::from("./data"),
                create_if_missing: true,
                storage_version: "1".to_owned(),
            },
            wal: WalConfig {
                segment_size_bytes: 64 * 1024 * 1024,
                fsync_on_write: true,
                max_replay_bytes: 512 * 1024 * 1024,
            },
            auth: AuthConfig {
                enabled: true,
                bootstrap_admin_username: "admin".to_owned(),
                admin_api_key: generate_bootstrap_api_key("admin"),
                writer_api_key: generate_bootstrap_api_key("writer"),
                reader_api_key: generate_bootstrap_api_key("reader"),
            },
            maintenance: MaintenanceConfig {
                max_node_count: 5_000_000,
                max_edge_count: 10_000_000,
            },
            observability: ObservabilityConfig {
                log_level: "info".to_owned(),
                metrics_enabled: true,
                tracing_enabled: true,
                tracing_json: true,
                otlp_enabled: false,
                otlp_protocol: "grpc".to_owned(),
                otlp_endpoint: "http://127.0.0.1:4317".to_owned(),
                otlp_headers: String::new(),
                otlp_timeout_ms: 10_000,
                audit_log_retention_entries: 10_000,
                audit_log_export_limit: 1_000,
            },
        }
    }
}

impl AppConfig {
    pub fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("UNDR9_BIND_ADDRESS") {
            self.server.bind_address = value;
        }
        if let Ok(value) = std::env::var("UNDR9_REQUEST_TIMEOUT_MS") {
            if let Ok(parsed) = value.trim().parse::<u64>() {
                self.server.request_timeout_ms = parsed;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_MAX_REQUEST_BODY_BYTES") {
            if let Ok(parsed) = value.trim().parse::<usize>() {
                self.server.max_request_body_bytes = parsed;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_STORAGE_ROOT") {
            self.storage.root_dir = PathBuf::from(value);
        }
        if let Ok(value) = std::env::var("UNDR9_STORAGE_CREATE_IF_MISSING") {
            let normalized = value.trim().to_ascii_lowercase();
            if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
                self.storage.create_if_missing = true;
            } else if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
                self.storage.create_if_missing = false;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_WAL_SEGMENT_SIZE_BYTES") {
            if let Ok(parsed) = value.trim().parse::<u64>() {
                self.wal.segment_size_bytes = parsed;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_WAL_MAX_REPLAY_BYTES") {
            if let Ok(parsed) = value.trim().parse::<u64>() {
                self.wal.max_replay_bytes = parsed;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_WAL_FSYNC_ON_WRITE") {
            let normalized = value.trim().to_ascii_lowercase();
            if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
                self.wal.fsync_on_write = true;
            } else if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
                self.wal.fsync_on_write = false;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_AUTH_ENABLED") {
            let normalized = value.trim().to_ascii_lowercase();
            if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
                self.auth.enabled = true;
            } else if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
                self.auth.enabled = false;
            }
        }

        if let Ok(value) = std::env::var("UNDR9_BOOTSTRAP_ADMIN_USERNAME") {
            self.auth.bootstrap_admin_username = value;
        }
        if let Ok(value) = std::env::var("UNDR9_ADMIN_API_KEY") {
            self.auth.admin_api_key = value;
        }
        if let Ok(value) = std::env::var("UNDR9_WRITER_API_KEY") {
            self.auth.writer_api_key = value;
        }
        if let Ok(value) = std::env::var("UNDR9_READER_API_KEY") {
            self.auth.reader_api_key = value;
        }
        if let Ok(value) = std::env::var("UNDR9_MAINTENANCE_MAX_NODES") {
            if let Ok(parsed) = value.trim().parse::<usize>() {
                self.maintenance.max_node_count = parsed;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_MAINTENANCE_MAX_EDGES") {
            if let Ok(parsed) = value.trim().parse::<usize>() {
                self.maintenance.max_edge_count = parsed;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_LOG_LEVEL") {
            self.observability.log_level = value;
        }
        if let Ok(value) = std::env::var("UNDR9_METRICS_ENABLED") {
            let normalized = value.trim().to_ascii_lowercase();
            if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
                self.observability.metrics_enabled = true;
            } else if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
                self.observability.metrics_enabled = false;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_TRACING_ENABLED") {
            let normalized = value.trim().to_ascii_lowercase();
            if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
                self.observability.tracing_enabled = true;
            } else if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
                self.observability.tracing_enabled = false;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_TRACING_JSON") {
            let normalized = value.trim().to_ascii_lowercase();
            if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
                self.observability.tracing_json = true;
            } else if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
                self.observability.tracing_json = false;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_OTLP_ENABLED") {
            let normalized = value.trim().to_ascii_lowercase();
            if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
                self.observability.otlp_enabled = true;
            } else if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
                self.observability.otlp_enabled = false;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_OTLP_PROTOCOL") {
            self.observability.otlp_protocol = value;
        }
        if let Ok(value) = std::env::var("UNDR9_OTLP_ENDPOINT") {
            self.observability.otlp_endpoint = value;
        }
        if let Ok(value) = std::env::var("UNDR9_OTLP_HEADERS") {
            self.observability.otlp_headers = value;
        }
        if let Ok(value) = std::env::var("UNDR9_OTLP_TIMEOUT_MS") {
            if let Ok(parsed) = value.trim().parse::<u64>() {
                self.observability.otlp_timeout_ms = parsed;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_AUDIT_LOG_RETENTION_ENTRIES") {
            if let Ok(parsed) = value.trim().parse::<usize>() {
                self.observability.audit_log_retention_entries = parsed;
            }
        }
        if let Ok(value) = std::env::var("UNDR9_AUDIT_LOG_EXPORT_LIMIT") {
            if let Ok(parsed) = value.trim().parse::<usize>() {
                self.observability.audit_log_export_limit = parsed;
            }
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.server.bind_address.trim().is_empty() {
            return Err(Undr9Error::Validation(
                "server.bind_address cannot be empty".to_owned(),
            ));
        }

        if self.server.request_timeout_ms == 0 {
            return Err(Undr9Error::Validation(
                "server.request_timeout_ms must be greater than zero".to_owned(),
            ));
        }

        if self.server.max_request_body_bytes == 0 {
            return Err(Undr9Error::Validation(
                "server.max_request_body_bytes must be greater than zero".to_owned(),
            ));
        }

        if self.storage.storage_version.trim().is_empty() {
            return Err(Undr9Error::Validation(
                "storage.storage_version cannot be empty".to_owned(),
            ));
        }

        if self.wal.segment_size_bytes < 4096 {
            return Err(Undr9Error::Validation(
                "wal.segment_size_bytes must be at least 4096".to_owned(),
            ));
        }

        if self.wal.max_replay_bytes < self.wal.segment_size_bytes {
            return Err(Undr9Error::Validation(
                "wal.max_replay_bytes must be greater than or equal to wal.segment_size_bytes"
                    .to_owned(),
            ));
        }

        if self.auth.enabled && self.auth.bootstrap_admin_username.trim().is_empty() {
            return Err(Undr9Error::Validation(
                "auth.bootstrap_admin_username cannot be empty when auth is enabled".to_owned(),
            ));
        }

        if self.auth.enabled && self.auth.admin_api_key.trim().is_empty() {
            return Err(Undr9Error::Validation(
                "auth.admin_api_key cannot be empty when auth is enabled".to_owned(),
            ));
        }

        if self.auth.enabled && self.auth.writer_api_key.trim().is_empty() {
            return Err(Undr9Error::Validation(
                "auth.writer_api_key cannot be empty when auth is enabled".to_owned(),
            ));
        }

        if self.auth.enabled && self.auth.reader_api_key.trim().is_empty() {
            return Err(Undr9Error::Validation(
                "auth.reader_api_key cannot be empty when auth is enabled".to_owned(),
            ));
        }

        if self.auth.enabled
            && (self.auth.admin_api_key.trim().len() < MIN_API_KEY_LENGTH
                || self.auth.writer_api_key.trim().len() < MIN_API_KEY_LENGTH
                || self.auth.reader_api_key.trim().len() < MIN_API_KEY_LENGTH)
        {
            return Err(Undr9Error::Validation(format!(
                "auth API keys must each be at least {MIN_API_KEY_LENGTH} characters when auth is enabled"
            )));
        }

        if self.auth.enabled
            && (self.auth.admin_api_key == self.auth.writer_api_key
                || self.auth.admin_api_key == self.auth.reader_api_key
                || self.auth.writer_api_key == self.auth.reader_api_key)
        {
            return Err(Undr9Error::Validation(
                "auth admin, writer, and reader API keys must be distinct".to_owned(),
            ));
        }

        if self.auth.admin_api_key == LEGACY_DEV_ADMIN_KEY
            || self.auth.writer_api_key == LEGACY_DEV_WRITER_KEY
            || self.auth.reader_api_key == LEGACY_DEV_READER_KEY
        {
            return Err(Undr9Error::Validation(
                "legacy development API keys are forbidden in runtime configuration".to_owned(),
            ));
        }

        if self.maintenance.max_node_count == 0 {
            return Err(Undr9Error::Validation(
                "maintenance.max_node_count must be greater than zero".to_owned(),
            ));
        }
        if self.maintenance.max_edge_count == 0 {
            return Err(Undr9Error::Validation(
                "maintenance.max_edge_count must be greater than zero".to_owned(),
            ));
        }

        if self.observability.log_level.trim().is_empty() {
            return Err(Undr9Error::Validation(
                "observability.log_level cannot be empty".to_owned(),
            ));
        }
        let normalized_otlp_protocol = self.observability.otlp_protocol.trim().to_ascii_lowercase();
        if !normalized_otlp_protocol.is_empty()
            && !matches!(
                normalized_otlp_protocol.as_str(),
                "grpc" | "http" | "http/protobuf" | "http_binary" | "http-binary"
            )
        {
            return Err(Undr9Error::Validation(
                "observability.otlp_protocol must be one of grpc, http, or http/protobuf"
                    .to_owned(),
            ));
        }
        if self.observability.otlp_timeout_ms == 0 {
            return Err(Undr9Error::Validation(
                "observability.otlp_timeout_ms must be greater than zero".to_owned(),
            ));
        }
        if self.observability.otlp_enabled && self.observability.otlp_endpoint.trim().is_empty() {
            return Err(Undr9Error::Validation(
                "observability.otlp_endpoint cannot be empty when otlp export is enabled"
                    .to_owned(),
            ));
        }
        if self.observability.audit_log_retention_entries == 0 {
            return Err(Undr9Error::Validation(
                "observability.audit_log_retention_entries must be greater than zero".to_owned(),
            ));
        }
        if self.observability.audit_log_export_limit == 0 {
            return Err(Undr9Error::Validation(
                "observability.audit_log_export_limit must be greater than zero".to_owned(),
            ));
        }

        Ok(())
    }
}

fn generate_bootstrap_api_key(role: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let process_id = std::process::id();
    format!("undr9-{role}-{process_id:08x}-{nanos:032x}")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::AppConfig;

    #[test]
    fn default_configuration_is_valid() {
        let config = AppConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_invalid_wal_settings() {
        let mut config = AppConfig::default();
        config.wal.segment_size_bytes = 1024;

        let error = config.validate().expect_err("invalid config should fail");
        assert!(error.to_string().contains("segment_size_bytes"));
    }

    #[test]
    fn rejects_legacy_development_keys() {
        let mut config = AppConfig::default();
        config.auth.admin_api_key = "undr9-dev-admin-key".to_owned();

        let error = config
            .validate()
            .expect_err("legacy dev key should fail validation");
        assert!(error.to_string().contains("legacy development API keys"));
    }

    #[test]
    fn applies_observability_env_overrides() {
        let mut config = AppConfig::default();
        std::env::set_var("UNDR9_LOG_LEVEL", "debug");
        std::env::set_var("UNDR9_TRACING_ENABLED", "false");
        std::env::set_var("UNDR9_TRACING_JSON", "false");
        std::env::set_var("UNDR9_OTLP_ENABLED", "true");
        std::env::set_var("UNDR9_OTLP_PROTOCOL", "http/protobuf");
        std::env::set_var("UNDR9_OTLP_ENDPOINT", "http://127.0.0.1:4318/v1/traces");
        std::env::set_var(
            "UNDR9_OTLP_HEADERS",
            "authorization=Bearer token,x-scope-orgid=tenant",
        );
        std::env::set_var("UNDR9_OTLP_TIMEOUT_MS", "2500");

        config.apply_env_overrides();

        assert_eq!(config.observability.log_level, "debug");
        assert!(!config.observability.tracing_enabled);
        assert!(!config.observability.tracing_json);
        assert!(config.observability.otlp_enabled);
        assert_eq!(config.observability.otlp_protocol, "http/protobuf");
        assert_eq!(
            config.observability.otlp_endpoint,
            "http://127.0.0.1:4318/v1/traces"
        );
        assert_eq!(
            config.observability.otlp_headers,
            "authorization=Bearer token,x-scope-orgid=tenant"
        );
        assert_eq!(config.observability.otlp_timeout_ms, 2500);

        std::env::remove_var("UNDR9_LOG_LEVEL");
        std::env::remove_var("UNDR9_TRACING_ENABLED");
        std::env::remove_var("UNDR9_TRACING_JSON");
        std::env::remove_var("UNDR9_OTLP_ENABLED");
        std::env::remove_var("UNDR9_OTLP_PROTOCOL");
        std::env::remove_var("UNDR9_OTLP_ENDPOINT");
        std::env::remove_var("UNDR9_OTLP_HEADERS");
        std::env::remove_var("UNDR9_OTLP_TIMEOUT_MS");
    }

    #[test]
    fn rejects_duplicate_or_short_api_keys() {
        let mut config = AppConfig::default();
        config.auth.admin_api_key = "short".to_owned();
        config.auth.writer_api_key = "short".to_owned();
        config.auth.reader_api_key = "distinct-reader-key".to_owned();

        let error = config
            .validate()
            .expect_err("weak auth keys should fail validation");
        assert!(error.to_string().contains("at least"));

        config.auth.admin_api_key = "distinct-admin-key".to_owned();
        config.auth.writer_api_key = "distinct-admin-key".to_owned();
        config.auth.reader_api_key = "distinct-reader-key".to_owned();

        let error = config
            .validate()
            .expect_err("duplicate auth keys should fail validation");
        assert!(error.to_string().contains("must be distinct"));
    }

    #[test]
    fn applies_runtime_env_overrides() {
        let mut config = AppConfig::default();
        std::env::set_var("UNDR9_BIND_ADDRESS", "0.0.0.0:9090");
        std::env::set_var("UNDR9_REQUEST_TIMEOUT_MS", "9000");
        std::env::set_var("UNDR9_MAX_REQUEST_BODY_BYTES", "2048");
        std::env::set_var("UNDR9_STORAGE_ROOT", "/tmp/undr9-data");
        std::env::set_var("UNDR9_STORAGE_CREATE_IF_MISSING", "false");
        std::env::set_var("UNDR9_WAL_SEGMENT_SIZE_BYTES", "8192");
        std::env::set_var("UNDR9_WAL_MAX_REPLAY_BYTES", "16384");
        std::env::set_var("UNDR9_WAL_FSYNC_ON_WRITE", "false");
        std::env::set_var("UNDR9_MAINTENANCE_MAX_NODES", "123");
        std::env::set_var("UNDR9_MAINTENANCE_MAX_EDGES", "456");

        config.apply_env_overrides();

        assert_eq!(config.server.bind_address, "0.0.0.0:9090");
        assert_eq!(config.server.request_timeout_ms, 9000);
        assert_eq!(config.server.max_request_body_bytes, 2048);
        assert_eq!(config.storage.root_dir, PathBuf::from("/tmp/undr9-data"));
        assert!(!config.storage.create_if_missing);
        assert_eq!(config.wal.segment_size_bytes, 8192);
        assert_eq!(config.wal.max_replay_bytes, 16384);
        assert!(!config.wal.fsync_on_write);
        assert_eq!(config.maintenance.max_node_count, 123);
        assert_eq!(config.maintenance.max_edge_count, 456);

        std::env::remove_var("UNDR9_BIND_ADDRESS");
        std::env::remove_var("UNDR9_REQUEST_TIMEOUT_MS");
        std::env::remove_var("UNDR9_MAX_REQUEST_BODY_BYTES");
        std::env::remove_var("UNDR9_STORAGE_ROOT");
        std::env::remove_var("UNDR9_STORAGE_CREATE_IF_MISSING");
        std::env::remove_var("UNDR9_WAL_SEGMENT_SIZE_BYTES");
        std::env::remove_var("UNDR9_WAL_MAX_REPLAY_BYTES");
        std::env::remove_var("UNDR9_WAL_FSYNC_ON_WRITE");
        std::env::remove_var("UNDR9_MAINTENANCE_MAX_NODES");
        std::env::remove_var("UNDR9_MAINTENANCE_MAX_EDGES");
    }
}
