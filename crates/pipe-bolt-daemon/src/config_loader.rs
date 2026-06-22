use std::net::{AddrParseError, SocketAddr};
use std::path::{Path, PathBuf};

use pipe_bolt_domain::{ProjectConfig, ProjectId};
use pipe_bolt_storage::error::StorageError;
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

pub const DATABASE_URL_ENV: &str = "PIPE_BOLT_DATABASE_URL";
pub const PROJECT_ID_ENV: &str = "PIPE_BOLT_PROJECT_ID";
pub const STORAGE_KEY_B64_ENV: &str = "PIPE_BOLT_STORAGE_KEY_B64";
pub const STORAGE_KEY_ID_ENV: &str = "PIPE_BOLT_STORAGE_KEY_ID";
pub const STORAGE_MAX_CONNECTIONS_ENV: &str = "PIPE_BOLT_STORAGE_MAX_CONNECTIONS";
pub const STORAGE_RUN_MIGRATIONS_ENV: &str = "PIPE_BOLT_STORAGE_RUN_MIGRATIONS";
pub const PROJECT_CONFIG_BOOTSTRAP_ENV: &str = "PIPE_BOLT_PROJECT_CONFIG_BOOTSTRAP";

pub const DEFAULT_STORAGE_KEY_ID: &str = "default";
pub const DEFAULT_STORAGE_MAX_CONNECTIONS: u32 = 8;
pub const STORAGE_ACTIVE_KEY_ID_ENV: &str = "PIPE_BOLT_STORAGE_ACTIVE_KEY_ID";
pub const STORAGE_KEYS_ENV: &str = "PIPE_BOLT_STORAGE_KEYS";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DaemonRuntimeConfig {
    pub project_config: ProjectConfigSource,
    pub runtime: RuntimeSettings,
    pub storage: Option<StorageRuntimeConfig>,
    pub log_filter: String,
}

impl DaemonRuntimeConfig {
    pub fn from_env_and_args() -> Result<Self, ConfigLoadError> {
        let mut runtime = RuntimeSettings::default();

        if let Some(value) = std::env::var_os(REALTIME_BIND_ADDR_ENV) {
            let value = value.to_string_lossy().into_owned();
            runtime.realtime_bridge_bind_addr = parse_socket_addr(&value)?;
        }

        let storage = StorageRuntimeConfig::from_env()?;
        let project_config = ProjectConfigSource::from_env_and_args(storage.is_some())?;
        let log_filter = std::env::var(LOG_FILTER_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_LOG_FILTER.to_owned());

        Ok(Self {
            project_config,
            runtime,
            storage,
            log_filter,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ProjectConfigSource {
    File(ProjectConfigLoadOptions),
    Postgres {
        project_id: ProjectId,
        bootstrap_file: Option<ProjectConfigLoadOptions>,
    },
}

impl ProjectConfigSource {
    fn from_env_and_args(storage_enabled: bool) -> Result<Self, ConfigLoadError> {
        if !storage_enabled {
            return Ok(Self::File(ProjectConfigLoadOptions::from_env_and_args()?));
        }

        let project_id =
            std::env::var(PROJECT_ID_ENV).map_err(|_| ConfigLoadError::MissingEnv {
                name: PROJECT_ID_ENV,
            })?;
        let project_id = ProjectId::new(project_id)?;
        let bootstrap_file = if env_bool(PROJECT_CONFIG_BOOTSTRAP_ENV, false)? {
            Some(ProjectConfigLoadOptions::from_env_and_args()?)
        } else {
            None
        };

        Ok(Self::Postgres {
            project_id,
            bootstrap_file,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StorageRuntimeConfig {
    pub database_url: String,
    pub active_key_id: String,
    pub keys: Vec<(String, String)>,
    pub max_connections: u32,
    pub run_migrations: bool,
}

impl StorageRuntimeConfig {
    fn from_env() -> Result<Option<Self>, ConfigLoadError> {
        let Some(database_url) = std::env::var(DATABASE_URL_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };

        let max_connections =
            env_u32(STORAGE_MAX_CONNECTIONS_ENV, DEFAULT_STORAGE_MAX_CONNECTIONS)?;
        let run_migrations = env_bool(STORAGE_RUN_MIGRATIONS_ENV, true)?;
        let (active_key_id, keys) = parse_storage_keys()?;

        Ok(Some(Self {
            database_url,
            active_key_id,
            keys,
            max_connections,
            run_migrations,
        }))
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

    #[error("required environment variable {name} is missing")]
    MissingEnv { name: &'static str },

    #[error("environment variable {name} has invalid boolean value '{value}'")]
    InvalidBool { name: &'static str, value: String },

    #[error("environment variable {name} has invalid unsigned integer value '{value}'")]
    InvalidU32 { name: &'static str, value: String },

    #[error("project config '{project_id}' was not found in storage")]
    ProjectConfigNotFound { project_id: String },

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("bootstrap project id mismatch: expected '{expected}', actual '{actual}'")]
    ProjectIdMismatch { expected: String, actual: String },

    #[error("invalid storage keyring config: {reason}")]
    InvalidStorageKeyring { reason: &'static str },
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

fn parse_storage_keys() -> Result<(String, Vec<(String, String)>), ConfigLoadError> {
    if let Ok(keys) = std::env::var(STORAGE_KEYS_ENV)
        && !keys.trim().is_empty()
    {
        let active_key_id =
            std::env::var(STORAGE_ACTIVE_KEY_ID_ENV).map_err(|_| ConfigLoadError::MissingEnv {
                name: STORAGE_ACTIVE_KEY_ID_ENV,
            })?;
        let parsed = keys
            .split(',')
            .map(|entry| {
                let (key_id, key_b64) =
                    entry
                        .split_once('=')
                        .ok_or(ConfigLoadError::InvalidStorageKeyring {
                            reason: "PIPE_BOLT_STORAGE_KEYS entries must use key_id=base64",
                        })?;
                if key_id.trim().is_empty() || key_b64.trim().is_empty() {
                    return Err(ConfigLoadError::InvalidStorageKeyring {
                        reason: "storage key id and key value must not be empty",
                    });
                }
                Ok((key_id.trim().to_owned(), key_b64.trim().to_owned()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        return Ok((active_key_id, parsed));
    }

    let key_b64 = std::env::var(STORAGE_KEY_B64_ENV).map_err(|_| ConfigLoadError::MissingEnv {
        name: STORAGE_KEY_B64_ENV,
    })?;
    let key_id = std::env::var(STORAGE_KEY_ID_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_STORAGE_KEY_ID.to_owned());

    Ok((key_id.clone(), vec![(key_id, key_b64)]))
}

fn parse_socket_addr(value: &str) -> Result<SocketAddr, ConfigLoadError> {
    value
        .parse()
        .map_err(|source| ConfigLoadError::InvalidSocketAddr {
            value: value.to_owned(),
            source,
        })
}

fn env_bool(name: &'static str, default: bool) -> Result<bool, ConfigLoadError> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(default);
    };

    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigLoadError::InvalidBool { name, value }),
    }
}

fn env_u32(name: &'static str, default: u32) -> Result<u32, ConfigLoadError> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(default);
    };

    value
        .parse::<u32>()
        .map_err(|_| ConfigLoadError::InvalidU32 { name, value })
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
