use std::net::{AddrParseError, SocketAddr};
use std::path::{Path, PathBuf};

use pipe_bolt_domain::ProjectConfig;
use thiserror::Error;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

use crate::runtime::RuntimeSettings;

pub const PROJECT_CONFIG_ENV: &str = "PIPE_BOLT_PROJECT_CONFIG";
pub const REALTIME_BIND_ADDR_ENV: &str = "PIPE_BOLT_REALTIME_BIND_ADDR";
pub const LOG_FILTER_ENV: &str = "PIPE_BOLT_LOG";
pub const DEFAULT_PROJECT_CONFIG_PATH: &str = "pipe-bolt.project.json";
pub const DEFAULT_LOG_FILTER: &str = "pipe_bolt_daemon=info,pipe_bolt_core=info,warn";

const DEFAULT_MAX_PROJECT_CONFIG_BYTES: u64 = 1024 * 1024;
const INITIAL_READ_BUFFER_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DaemonRuntimeConfig {
    pub project_config: ProjectConfigLoadOptions,
    pub runtime: RuntimeSettings,
    pub log_filter: String,
}

impl DaemonRuntimeConfig {
    pub fn from_env_and_args() -> Result<Self, ConfigLoadError> {
        let mut runtime = RuntimeSettings::default();

        if let Some(value) = std::env::var_os(REALTIME_BIND_ADDR_ENV) {
            let value = value.to_string_lossy().into_owned();
            runtime.realtime_bridge_bind_addr = parse_socket_addr(&value)?;
        }

        let log_filter = std::env::var(LOG_FILTER_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_LOG_FILTER.to_owned());

        Ok(Self {
            project_config: ProjectConfigLoadOptions::from_env_and_args()?,
            runtime,
            log_filter,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectConfigLoadOptions {
    pub path: PathBuf,
    pub max_bytes: u64,
}

impl ProjectConfigLoadOptions {
    pub fn from_env_and_args() -> Result<Self, ConfigLoadError> {
        let path = std::env::args_os()
            .nth(1)
            .or_else(|| std::env::var_os(PROJECT_CONFIG_ENV))
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_PROJECT_CONFIG_PATH));

        Self::new(path, DEFAULT_MAX_PROJECT_CONFIG_BYTES)
    }

    pub fn new(path: impl Into<PathBuf>, max_bytes: u64) -> Result<Self, ConfigLoadError> {
        let path = path.into();

        if path.as_os_str().is_empty() {
            return Err(ConfigLoadError::InvalidOptions(
                "project config path must not be empty",
            ));
        }

        if max_bytes == 0 {
            return Err(ConfigLoadError::InvalidOptions(
                "project config max_bytes must be greater than zero",
            ));
        }

        Ok(Self { path, max_bytes })
    }
}

#[derive(Debug, Error)]
pub enum ConfigLoadError {
    #[error("invalid config load options: {0}")]
    InvalidOptions(&'static str),

    #[error("invalid socket address '{value}': {source}")]
    InvalidSocketAddr {
        value: String,
        #[source]
        source: AddrParseError,
    },

    #[error("project config path is not a regular file: {path}")]
    NotAFile { path: String },

    #[error("project config is too large: {actual} bytes exceeds {max} bytes")]
    TooLarge { actual: u64, max: u64 },

    #[error("failed to read project config from {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse project config JSON from {path}: {source}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("project config validation failed: {0}")]
    Validation(#[from] pipe_bolt_domain::DomainError),
}

pub async fn load_project_config(
    options: &ProjectConfigLoadOptions,
) -> Result<ProjectConfig, ConfigLoadError> {
    validate_config_path(&options.path).await?;

    let bytes = read_bounded_file(&options.path, options.max_bytes).await?;
    let config = serde_json::from_slice::<ProjectConfig>(&bytes).map_err(|source| {
        ConfigLoadError::Json {
            path: options.path.display().to_string(),
            source,
        }
    })?;

    config.validate()?;
    Ok(config)
}

async fn validate_config_path(path: &Path) -> Result<(), ConfigLoadError> {
    let path_display = path.display().to_string();
    let metadata = fs::metadata(path)
        .await
        .map_err(|source| ConfigLoadError::Read {
            path: path_display.clone(),
            source,
        })?;

    if !metadata.is_file() {
        return Err(ConfigLoadError::NotAFile { path: path_display });
    }

    Ok(())
}

async fn read_bounded_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>, ConfigLoadError> {
    let path_display = path.display().to_string();
    let file = File::open(path)
        .await
        .map_err(|source| ConfigLoadError::Read {
            path: path_display.clone(),
            source,
        })?;

    let mut reader = BufReader::new(file).take(max_bytes.saturating_add(1));
    let initial_capacity = usize::try_from(max_bytes.min(INITIAL_READ_BUFFER_BYTES))
        .unwrap_or(INITIAL_READ_BUFFER_BYTES as usize);
    let mut bytes = Vec::with_capacity(initial_capacity);

    reader
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| ConfigLoadError::Read {
            path: path_display,
            source,
        })?;

    let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual > max_bytes {
        return Err(ConfigLoadError::TooLarge {
            actual,
            max: max_bytes,
        });
    }

    Ok(bytes)
}

fn parse_socket_addr(value: &str) -> Result<SocketAddr, ConfigLoadError> {
    value
        .parse()
        .map_err(|source| ConfigLoadError::InvalidSocketAddr {
            value: value.to_owned(),
            source,
        })
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[tokio::test]
    async fn config_loader_rejects_oversized_file_without_unbounded_read() {
        let path = unique_temp_path("oversized.json");
        tokio::fs::write(&path, br#"{"id":"too-large"}"#)
            .await
            .expect("write test config");

        let options = ProjectConfigLoadOptions::new(&path, 4).expect("load options");
        let error = load_project_config(&options)
            .await
            .expect_err("oversized error");

        assert!(matches!(error, ConfigLoadError::TooLarge { .. }));
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn config_loader_rejects_invalid_json() {
        let path = unique_temp_path("invalid.json");
        tokio::fs::write(&path, b"not-json")
            .await
            .expect("write test config");

        let options = ProjectConfigLoadOptions::new(&path, 1024).expect("load options");
        let error = load_project_config(&options).await.expect_err("json error");

        assert!(matches!(error, ConfigLoadError::Json { .. }));
        let _ = tokio::fs::remove_file(path).await;
    }

    fn unique_temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();

        std::env::temp_dir().join(format!(
            "pipe-bolt-config-loader-{}-{nanos}-{name}",
            std::process::id()
        ))
    }
}
