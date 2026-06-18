use std::path::PathBuf;

use pipe_bolt_domain::ProjectConfig;
use thiserror::Error;
use tokio::fs;

pub const PROJECT_CONFIG_ENV: &str = "PIPE_BOLT_PROJECT_CONFIG";
pub const DEFAULT_PROJECT_CONFIG_PATH: &str = "pipe-bolt.project.json";
const DEFAULT_MAX_PROJECT_CONFIG_BYTES: u64 = 1024 * 1024;

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
    let path = options.path.display().to_string();
    let metadata = fs::metadata(&options.path)
        .await
        .map_err(|source| ConfigLoadError::Read {
            path: path.clone(),
            source,
        })?;

    if !metadata.is_file() {
        return Err(ConfigLoadError::NotAFile { path });
    }

    if metadata.len() > options.max_bytes {
        return Err(ConfigLoadError::TooLarge {
            actual: metadata.len(),
            max: options.max_bytes,
        });
    }

    let bytes = fs::read(&options.path)
        .await
        .map_err(|source| ConfigLoadError::Read {
            path: path.clone(),
            source,
        })?;

    let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual > options.max_bytes {
        return Err(ConfigLoadError::TooLarge {
            actual,
            max: options.max_bytes,
        });
    }

    let config = serde_json::from_slice::<ProjectConfig>(&bytes).map_err(|source| {
        ConfigLoadError::Json {
            path: path.clone(),
            source,
        }
    })?;

    config.validate()?;
    Ok(config)
}
