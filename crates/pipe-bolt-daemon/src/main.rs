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
pub mod persistence_writer;
pub mod runtime;
pub mod runtime_control;

use std::error::Error;
use std::sync::Arc;

use pipe_bolt_api::{ApiState, ManagementAuth, ManagementStorage, serve_management_api};
use pipe_bolt_domain::ProjectConfig;
use pipe_bolt_storage::model::AuditContext;
use pipe_bolt_storage::postgres::{PostgresStorage, PostgresStorageConfig};
use pipe_bolt_storage::secret::StorageKeyring;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config_loader::{
    ConfigLoadError, DaemonRuntimeConfig, ProjectConfigSource, StorageRuntimeConfig,
    load_project_config,
};
use crate::runtime::{ProjectRuntime, RuntimePersistence};
use crate::runtime_control::RuntimeSupervisor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(feature = "dotenvy")]
    {
        if let Err(error) = dotenvy::dotenv() {
            eprintln!("Warning: Failed to load .env file: {error}");
        }
    }

    let daemon_config = DaemonRuntimeConfig::from_env_and_args()?;
    init_tracing(&daemon_config.log_filter)?;

    let storage = build_storage(daemon_config.storage.as_ref()).await?;
    let project_config = load_config(&daemon_config.project_config, storage.as_ref()).await?;
    let runtime_persistence = storage
        .as_ref()
        .map(|storage| RuntimePersistence::new(project_config.id.clone(), Arc::clone(storage)));
    let runtime = ProjectRuntime::start(
        project_config.clone(),
        daemon_config.runtime.clone(),
        runtime_persistence.clone(),
    )
    .await?;

    let (runtime_owner, api_shutdown_tx, api_worker) = start_management_api_if_configured(
        &daemon_config,
        storage.as_ref(),
        runtime_persistence,
        project_config,
        runtime,
    )
    .await?;

    pipe_bolt_core::web::realtime::router::graceful_signal().await;

    if let Some(shutdown_tx) = api_shutdown_tx {
        let _ = shutdown_tx.send(true);
    }

    runtime_owner.shutdown().await?;

    if let Some(worker) = api_worker {
        join_management_api_worker(worker).await?;
    }

    Ok(())
}

async fn start_management_api_if_configured(
    daemon_config: &DaemonRuntimeConfig,
    storage: Option<&Arc<PostgresStorage>>,
    runtime_persistence: Option<RuntimePersistence>,
    project_config: ProjectConfig,
    runtime: ProjectRuntime,
) -> Result<
    (
        RuntimeOwner,
        Option<watch::Sender<bool>>,
        Option<JoinHandle<()>>,
    ),
    Box<dyn Error + Send + Sync>,
> {
    let Some(api_config) = &daemon_config.management_api else {
        return Ok((RuntimeOwner::Standalone(Some(runtime)), None, None));
    };
    let storage = storage.ok_or(ConfigLoadError::MissingEnv {
        name: config_loader::DATABASE_URL_ENV,
    })?;
    let runtime_persistence = runtime_persistence.ok_or(ConfigLoadError::MissingEnv {
        name: config_loader::DATABASE_URL_ENV,
    })?;

    let supervisor = Arc::new(RuntimeSupervisor::new(
        project_config,
        runtime,
        daemon_config.runtime.clone(),
        runtime_persistence,
        Arc::clone(storage),
    ));

    let auth = ManagementAuth::bearer(api_config.bearer_token.clone())?;
    let api_storage: Arc<dyn ManagementStorage> = Arc::<PostgresStorage>::clone(storage);
    let api_runtime: Arc<dyn pipe_bolt_api::RuntimeControl> =
        Arc::<RuntimeSupervisor>::clone(&supervisor);

    let state = ApiState::new(
        api_storage,
        api_runtime,
        auth,
        api_config.max_config_body_bytes,
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bind_addr = api_config.bind_addr;
    let worker = tokio::spawn(async move {
        serve_management_api(bind_addr, state, shutdown_rx).await;
    });

    Ok((
        RuntimeOwner::Supervisor(supervisor),
        Some(shutdown_tx),
        Some(worker),
    ))
}

async fn build_storage(
    config: Option<&StorageRuntimeConfig>,
) -> Result<Option<Arc<PostgresStorage>>, Box<dyn Error + Send + Sync>> {
    let Some(config) = config else {
        return Ok(None);
    };

    let keyring = Arc::new(StorageKeyring::from_base64_keys(
        config.active_key_id.clone(),
        config.keys.clone(),
    )?);
    let mut storage_config = PostgresStorageConfig::new(config.database_url.clone())?;
    storage_config.max_connections = config.max_connections;
    let storage = Arc::new(PostgresStorage::connect(&storage_config, keyring).await?);

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
            validate_bootstrap_project_id(&config, project_id)?;

            storage
                .create_project_config(
                    &config,
                    AuditContext::system("bootstrap project config from file"),
                )
                .await?;

            Ok(config)
        }
    }
}

fn validate_bootstrap_project_id(
    config: &ProjectConfig,
    expected_project_id: &pipe_bolt_domain::ProjectId,
) -> Result<(), ConfigLoadError> {
    if config.id != *expected_project_id {
        return Err(ConfigLoadError::ProjectIdMismatch {
            expected: expected_project_id.to_string(),
            actual: config.id.to_string(),
        });
    }

    Ok(())
}

fn init_tracing(filter: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let env_filter = tracing_subscriber::EnvFilter::try_new(filter)?;
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init()?;
    Ok(())
}

async fn join_management_api_worker(
    worker: JoinHandle<()>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    worker.await?;
    Ok(())
}

enum RuntimeOwner {
    Standalone(Option<ProjectRuntime>),
    Supervisor(Arc<RuntimeSupervisor>),
}

impl RuntimeOwner {
    async fn shutdown(self) -> Result<(), crate::runtime::RuntimeError> {
        match self {
            Self::Standalone(runtime) => {
                if let Some(runtime) = runtime {
                    runtime.shutdown().await?;
                }
            }
            Self::Supervisor(supervisor) => supervisor.shutdown().await?,
        }

        Ok(())
    }
}
