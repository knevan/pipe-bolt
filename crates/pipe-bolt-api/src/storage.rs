use pipe_bolt_domain::{ProjectConfig, ProjectId};
use pipe_bolt_storage::error::StorageError;
use pipe_bolt_storage::model::{
    AuditContext, AuditEventRecord, FailureEventRecord, FailureListQuery, OperationalListQuery,
    ProjectConfigWriteResult, SinkDeliveryOutcomeRecord,
};
use pipe_bolt_storage::postgres::PostgresStorage;
use salvo::async_trait;

#[async_trait]
pub trait ManagementStorage: Send + Sync + 'static {
    async fn health_check(&self) -> Result<(), StorageError>;

    async fn load_project_config(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<ProjectConfig>, StorageError>;

    async fn update_project_config(
        &self,
        config: &ProjectConfig,
        expected_version: u64,
        audit: AuditContext,
    ) -> Result<ProjectConfigWriteResult, StorageError>;

    async fn list_audit_events(
        &self,
        project_id: &ProjectId,
        query: OperationalListQuery,
    ) -> Result<Vec<AuditEventRecord>, StorageError>;

    async fn list_failures(
        &self,
        project_id: &ProjectId,
        query: FailureListQuery,
    ) -> Result<Vec<FailureEventRecord>, StorageError>;

    async fn resolve_failure(
        &self,
        project_id: &ProjectId,
        failure_id: &str,
        resolution: &str,
        audit: AuditContext,
    ) -> Result<(), StorageError>;

    async fn list_delivery_outcomes(
        &self,
        project_id: &ProjectId,
        query: OperationalListQuery,
    ) -> Result<Vec<SinkDeliveryOutcomeRecord>, StorageError>;
}

#[async_trait]
impl ManagementStorage for PostgresStorage {
    async fn health_check(&self) -> Result<(), StorageError> {
        PostgresStorage::health_check(self).await
    }

    async fn load_project_config(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<ProjectConfig>, StorageError> {
        PostgresStorage::load_project_config(self, project_id).await
    }

    async fn update_project_config(
        &self,
        config: &ProjectConfig,
        expected_version: u64,
        audit: AuditContext,
    ) -> Result<ProjectConfigWriteResult, StorageError> {
        PostgresStorage::update_project_config(self, config, expected_version, audit).await
    }

    async fn list_audit_events(
        &self,
        project_id: &ProjectId,
        query: OperationalListQuery,
    ) -> Result<Vec<AuditEventRecord>, StorageError> {
        PostgresStorage::list_audit_events(self, project_id, query).await
    }

    async fn list_failures(
        &self,
        project_id: &ProjectId,
        query: FailureListQuery,
    ) -> Result<Vec<FailureEventRecord>, StorageError> {
        PostgresStorage::list_failures(self, project_id, query).await
    }

    async fn resolve_failure(
        &self,
        project_id: &ProjectId,
        failure_id: &str,
        resolution: &str,
        audit: AuditContext,
    ) -> Result<(), StorageError> {
        PostgresStorage::resolve_failure(self, project_id, failure_id, resolution, audit).await
    }

    async fn list_delivery_outcomes(
        &self,
        project_id: &ProjectId,
        query: OperationalListQuery,
    ) -> Result<Vec<SinkDeliveryOutcomeRecord>, StorageError> {
        PostgresStorage::list_delivery_outcomes(self, project_id, query).await
    }
}
