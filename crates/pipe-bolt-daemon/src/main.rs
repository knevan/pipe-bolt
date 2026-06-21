#![deny(
    clippy::panic,
    clippy::panicking_unwrap,
    clippy::redundant_clone,
    clippy::implicit_clone,
    clippy::perf,
    clippy::large_types_passed_by_value,
    clippy::large_futures,
    clippy::trivially_copy_pass_by_ref,
    clippy::clone_on_ref_ptr,
    // clippy::unwrap_used,
    // clippy::missing_const_for_fn,
)]

pub mod config_loader;
pub mod runtime;

use std::error::Error;
use std::sync::Arc;

#[cfg(debug_assertions)]
use dotenvy::dotenv;
use pipe_bolt_domain::ProjectConfig;
use pipe_bolt_storage::model::AuditContext;
use pipe_bolt_storage::postgres::{PostgresStorage, PostgresStorageConfig};
use pipe_bolt_storage::secret::AesGcmSecretCipher;

use crate::config_loader::{
    ConfigLoadError, DaemonRuntimeConfig, ProjectConfigSource, StorageRuntimeConfig,
    load_project_config,
};
use crate::runtime::{ProjectRuntime, RuntimePersistence};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(debug_assertions)]
    {
        if let Err(e) = dotenv() {
            eprintln!("Warning: Failed to load .env file: {}", e);
        }
    }

    let daemon_config = DaemonRuntimeConfig::from_env_and_args()?;
    init_tracing(&daemon_config.log_filter)?;

    let storage = build_storage(daemon_config.storage.as_ref()).await?;
    let project_config = load_config(&daemon_config.project_config, storage.as_ref()).await?;
    let runtime_persistence = storage
        .as_ref()
        .map(|storage| RuntimePersistence::new(project_config.id.clone(), Arc::clone(storage)));
    let runtime =
        ProjectRuntime::start(project_config, daemon_config.runtime, runtime_persistence).await?;

    pipe_bolt_core::web::realtime::router::graceful_signal().await;
    runtime.shutdown().await?;

    Ok(())
}

async fn build_storage(
    config: Option<&StorageRuntimeConfig>,
) -> Result<Option<Arc<PostgresStorage>>, Box<dyn Error + Send + Sync>> {
    let Some(config) = config else {
        return Ok(None);
    };

    let cipher = Arc::new(AesGcmSecretCipher::from_base64_key(
        config.key_id.clone(),
        &config.secret_key_b64,
    )?);
    let mut storage_config = PostgresStorageConfig::new(config.database_url.clone())?;
    storage_config.max_connections = config.max_connections;
    let storage = Arc::new(PostgresStorage::connect(&storage_config, cipher).await?);

    if config.run_migrations {
        storage.migrate().await?;
    }

    Ok(Some(storage))
}

async fn load_config(
    source: &ProjectConfigSource,
    storage: Option<&Arc<PostgresStorage>>,
) -> Result<ProjectConfig, ConfigLoadError> {
    match source {
        ProjectConfigSource::File(options) => load_project_config(options).await,
        ProjectConfigSource::Postgres {
            project_id,
            bootstrap_file,
        } => {
            let storage = storage.ok_or(ConfigLoadError::MissingEnv {
                name: config_loader::DATABASE_URL_ENV,
            })?;

            if let Some(config) = storage.load_project_config(project_id).await? {
                return Ok(config);
            }

            let Some(options) = bootstrap_file else {
                return Err(ConfigLoadError::ProjectConfigNotFound {
                    project_id: project_id.to_string(),
                });
            };

            let config = load_project_config(options).await?;
            storage
                .upsert_project_config(
                    &config,
                    AuditContext::system("bootstrap project config from file"),
                )
                .await?;
            Ok(config)
        }
    }
}

/// Initializes the tracing subscriber with EnvFilter support
fn init_tracing(filter: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let env_filter = tracing_subscriber::EnvFilter::try_new(filter)?;
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init()?;
    Ok(())
}
