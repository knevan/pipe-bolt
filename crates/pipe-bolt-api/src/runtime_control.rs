use pipe_bolt_domain::ProjectId;
use salvo::async_trait;
use thiserror::Error;

use crate::dto::{RuntimeReloadResponse, RuntimeStatusResponse};

#[async_trait]
pub trait RuntimeControl: Send + Sync + 'static {
    async fn status(
        &self,
        project_id: &ProjectId,
    ) -> Result<RuntimeStatusResponse, RuntimeControlError>;

    async fn reload(
        &self,
        project_id: &ProjectId,
        reason: Option<String>,
    ) -> Result<RuntimeReloadResponse, RuntimeControlError>;
}

#[derive(Debug, Error)]
pub enum RuntimeControlError {
    #[error("project '{project_id}' is not managed by this runtime")]
    ProjectNotManaged { project_id: String },

    #[error("runtime reload is already in progress")]
    ReloadInProgress,

    #[error("runtime unavailable: {reason}")]
    RuntimeUnavailable { reason: String },

    #[error("runtime config invalid: {reason}")]
    InvalidConfig { reason: String },

    #[error("runtime start failed: {reason}")]
    StartFailed { reason: String },

    #[error("storage error: {reason}")]
    Storage { reason: String },
}
