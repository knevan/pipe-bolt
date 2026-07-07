use pipe_bolt_domain::{ProjectConfig, ProjectId};
use pipe_bolt_storage::AuditContext;
use salvo::async_trait;
use thiserror::Error;

use crate::dto::{RuntimeReloadResponse, RuntimeStatusResponse};

#[async_trait]
pub trait RuntimeControl: Send + Sync + 'static {
    async fn status(
        &self,
        project_id: &ProjectId,
    ) -> Result<RuntimeStatusResponse, RuntimeControlError>;

    async fn validate_candidate_config(
        &self,
        project_id: &ProjectId,
        config: &ProjectConfig,
    ) -> Result<(), RuntimeControlError>;

    async fn reload(
        &self,
        project_id: &ProjectId,
        audit: AuditContext,
    ) -> Result<RuntimeReloadResponse, RuntimeControlError>;
}

#[derive(Debug, Error)]
pub enum RuntimeControlError {
    #[error("project '{project_id}' is not managed by this runtime")]
    ProjectNotManaged { project_id: String },

    #[error("runtime lifecycle operation is already in progress")]
    ReloadInProgress,

    #[error("runtime is shutting down: {reason}")]
    ShuttingDown { reason: String },

    #[error("runtime unavailable: {reason}")]
    RuntimeUnavailable { reason: String },

    #[error("runtime config invalid: {reason}")]
    InvalidConfig { reason: String },

    #[error("old runtime shutdown was not proven safe: {reason}")]
    UnsafeOldRuntimeShutdown { reason: String },

    #[error("runtime start failed: {reason}")]
    StartFailed { reason: String },

    #[error("storage error: {reason}")]
    Storage { reason: String },
}
