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

use crate::config_loader::{DaemonRuntimeConfig, load_project_config};
use crate::runtime::ProjectRuntime;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let daemon_config = DaemonRuntimeConfig::from_env_and_args()?;
    init_tracing(&daemon_config.log_filter)?;

    let project_config = load_project_config(&daemon_config.project_config).await?;
    let runtime = ProjectRuntime::start(project_config, daemon_config.runtime).await?;

    pipe_bolt_core::web::realtime::router::graceful_signal().await;

    runtime.shutdown().await?;

    Ok(())
}

/// Initializes the tracing subscriber with EnvFilter support
fn init_tracing(filter: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let env_filter = tracing_subscriber::EnvFilter::try_new(filter)?;
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init()?;
    Ok(())
}
