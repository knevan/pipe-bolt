use std::collections::BTreeMap;

use pipe_bolt_domain::{CommandTemplateId, NormalizedEvent, ProjectConfig, ProjectId};
use pipe_bolt_storage::AuditContext;
use salvo::async_trait;
use thiserror::Error;
use tokio::sync::broadcast;

use crate::dto::{
    ExecuteCommandResponse, RuntimeReadinessResponse, RuntimeReloadResponse, RuntimeStatusResponse,
};

#[async_trait]
pub trait RuntimeControl: Send + Sync + 'static {
    async fn readiness(&self) -> Result<RuntimeReadinessResponse, RuntimeControlError>;

    async fn status(
        &self,
        project_id: &ProjectId,
    ) -> Result<RuntimeStatusResponse, RuntimeControlError>;

    async fn subscribe_realtime_events(
        &self,
        project_id: &ProjectId,
    ) -> Result<broadcast::Receiver<NormalizedEvent>, RuntimeControlError>;

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

    async fn execute_command(
        &self,
        project_id: &ProjectId,
        command_template_id: &CommandTemplateId,
        params: BTreeMap<String, serde_json::Value>,
        audit: AuditContext,
    ) -> Result<ExecuteCommandResponse, RuntimeControlError>;
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

    #[error("command template '{command_template_id}' is not available")]
    CommandTemplateNotFound { command_template_id: String },

    #[error("command rejected: {reason}")]
    CommandRejected { reason: String },

    #[error("storage error: {reason}")]
    Storage { reason: String },
}
