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

use crate::config_loader::{ProjectConfigLoadOptions, load_project_config};
use crate::runtime::{ProjectRuntime, RuntimeSettings};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let load_options = ProjectConfigLoadOptions::from_env_and_args()?;
    let project_config = load_project_config(&load_options).await?;
    let runtime = ProjectRuntime::start(project_config, RuntimeSettings::default())?;

    pipe_bolt_core::web::realtime::router::graceful_signal().await;
    runtime.shutdown().await?;

    Ok(())
}
