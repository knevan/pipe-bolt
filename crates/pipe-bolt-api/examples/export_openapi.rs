use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use pipe_bolt_api::dto::{
    ExecuteCommandResponse, RuntimeReadinessResponse, RuntimeReloadResponse, RuntimeStatusResponse,
};
use pipe_bolt_api::{
    ApiState, ManagementAuth, ManagementStorage, RuntimeControl, RuntimeControlError,
    management_router,
};
use pipe_bolt_domain::{CommandTemplateId, NormalizedEvent, ProjectConfig, ProjectId};
use pipe_bolt_storage::error::StorageError;
use pipe_bolt_storage::model::{
    AuditContext, AuditEventRecord, FailureEventRecord, FailureListQuery, OperationalListQuery,
    ProjectConfigWriteResult, SinkDeliveryOutcomeRecord,
};
use salvo::async_trait;
use salvo::test::{ResponseExt, TestClient};
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let storage: Arc<dyn ManagementStorage> = Arc::new(OpenApiStorage);
    let runtime: Arc<dyn RuntimeControl> = Arc::new(OpenApiRuntime);
    let auth = ManagementAuth::bearer("openapi-export-token-0123456789abcdef")?;
    let state = ApiState::new(storage, runtime, auth, 1024 * 1024);
    let service = salvo::Service::new(management_router(state));

    let mut response = TestClient::get("http://127.0.0.1:8080/api-doc/openapi.json")
        .send(&service)
        .await;
    let body = response.take_json::<serde_json::Value>().await?;

    serde_json::to_writer_pretty(std::io::stdout(), &body)?;
    println!();

    Ok(())
}

#[derive(Debug)]
struct OpenApiStorage;

#[async_trait]
impl ManagementStorage for OpenApiStorage {
    async fn health_check(&self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn load_project_config(
        &self,
        _project_id: &ProjectId,
    ) -> Result<Option<ProjectConfig>, StorageError> {
        Err(unused_storage_error())
    }

    async fn update_project_config(
        &self,
        _config: &ProjectConfig,
        _expected_version: u64,
        _audit: AuditContext,
    ) -> Result<ProjectConfigWriteResult, StorageError> {
        Err(unused_storage_error())
    }

    async fn list_audit_events(
        &self,
        _project_id: &ProjectId,
        _query: OperationalListQuery,
    ) -> Result<Vec<AuditEventRecord>, StorageError> {
        Err(unused_storage_error())
    }

    async fn list_failures(
        &self,
        _project_id: &ProjectId,
        _query: FailureListQuery,
    ) -> Result<Vec<FailureEventRecord>, StorageError> {
        Err(unused_storage_error())
    }

    async fn resolve_failure(
        &self,
        _project_id: &ProjectId,
        _failure_id: &str,
        _resolution: &str,
        _audit: AuditContext,
    ) -> Result<(), StorageError> {
        Err(unused_storage_error())
    }

    async fn list_delivery_outcomes(
        &self,
        _project_id: &ProjectId,
        _query: OperationalListQuery,
    ) -> Result<Vec<SinkDeliveryOutcomeRecord>, StorageError> {
        Err(unused_storage_error())
    }
}

#[derive(Debug)]
struct OpenApiRuntime;

#[async_trait]
impl RuntimeControl for OpenApiRuntime {
    async fn readiness(&self) -> Result<RuntimeReadinessResponse, RuntimeControlError> {
        Err(unused_runtime_error())
    }

    async fn status(
        &self,
        _project_id: &ProjectId,
    ) -> Result<RuntimeStatusResponse, RuntimeControlError> {
        Err(unused_runtime_error())
    }

    async fn subscribe_realtime_events(
        &self,
        _project_id: &ProjectId,
    ) -> Result<broadcast::Receiver<NormalizedEvent>, RuntimeControlError> {
        Err(unused_runtime_error())
    }

    async fn validate_candidate_config(
        &self,
        _project_id: &ProjectId,
        _config: &ProjectConfig,
    ) -> Result<(), RuntimeControlError> {
        Err(unused_runtime_error())
    }

    async fn reload(
        &self,
        _project_id: &ProjectId,
        _audit: AuditContext,
    ) -> Result<RuntimeReloadResponse, RuntimeControlError> {
        Err(unused_runtime_error())
    }

    async fn execute_command(
        &self,
        _project_id: &ProjectId,
        _command_template_id: &CommandTemplateId,
        _params: BTreeMap<String, serde_json::Value>,
        _audit: AuditContext,
    ) -> Result<ExecuteCommandResponse, RuntimeControlError> {
        Err(unused_runtime_error())
    }
}

fn unused_storage_error() -> StorageError {
    StorageError::InvalidStoredState {
        reason: "OpenAPI export dummy storage must not be called",
    }
}

fn unused_runtime_error() -> RuntimeControlError {
    RuntimeControlError::RuntimeUnavailable {
        reason: "OpenAPI export dummy runtime must not be called".to_owned(),
    }
}
