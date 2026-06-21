use std::sync::Arc;
use std::time::Duration;

use pipe_bolt_domain::{ProjectConfig, ProjectId};
use serde_json::json;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::error::StorageError;
use crate::model::{
    AuditContext, AuditStatus, FailureSeverity, NewAuditEvent, NewFailureEvent,
    NewSinkDeliveryOutcome, SinkDeliveryStatus,
};
use crate::project_config_codec::{ProjectConfigCodec, StoredProjectConfig};
use crate::secret::SecretCipher;

static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

const DEFAULT_MAX_CONNECTIONS: u32 = 8;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(1800);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PostgresStorageConfig {
    pub database_url: String,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
}

impl PostgresStorageConfig {
    pub fn new(database_url: impl Into<String>) -> Result<Self, StorageError> {
        let database_url = database_url.into();
        if database_url.trim().is_empty() {
            return Err(StorageError::InvalidConfig {
                reason: "database_url must not be empty",
            });
        }

        Ok(Self {
            database_url,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_lifetime: DEFAULT_MAX_LIFETIME,
        })
    }
}

#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
    codec: ProjectConfigCodec,
}

impl PostgresStorage {
    pub async fn connect(
        config: &PostgresStorageConfig,
        cipher: Arc<dyn SecretCipher>,
    ) -> Result<Self, StorageError> {
        validate_config(config)?;

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .idle_timeout(config.idle_timeout)
            .max_lifetime(config.max_lifetime)
            .connect(&config.database_url)
            .await?;

        Ok(Self {
            pool,
            codec: ProjectConfigCodec::new(cipher),
        })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<(), StorageError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub async fn load_project_config(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<ProjectConfig>, StorageError> {
        // Pass arguments directly inside the sqlx::query! macro
        let row = sqlx::query!(
            "SELECT config FROM project_configs WHERE project_id = $1",
            project_id.as_str()
        )
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let stored: StoredProjectConfig = serde_json::from_value(row.config)?;
        let config = self.codec.decode(stored)?;
        Ok(Some(config))
    }
    pub async fn upsert_project_config(
        &self,
        config: &ProjectConfig,
        audit: AuditContext,
    ) -> Result<(), StorageError> {
        config.validate()?;
        let stored = self.codec.encode(config)?;
        let version = version_to_i64(config.version)?;
        let tenant_id = config.tenant_id.as_ref().map(ToString::to_string);
        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            r#"
            INSERT INTO project_configs (project_id, tenant_id, name, enabled, version, config)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (project_id) DO UPDATE SET
                tenant_id = EXCLUDED.tenant_id,
                name = EXCLUDED.name,
                enabled = EXCLUDED.enabled,
                version = EXCLUDED.version,
                config = EXCLUDED.config,
                updated_at = now()
            "#,
            config.id.as_str(),
            tenant_id.as_deref(),
            config.name.as_str(),
            config.enabled,
            version,
            Json(stored) as _
        )
        .execute(&mut *tx)
        .await?;

        let mut metadata = serde_json::Map::new();
        metadata.insert("version".to_owned(), json!(config.version));
        insert_audit_event_tx(
            &mut tx,
            NewAuditEvent {
                project_id: Some(config.id.clone()),
                actor_id: audit.actor_id,
                action: "project_config.upsert".to_owned(),
                target_type: "project_config".to_owned(),
                target_id: config.id.to_string(),
                status: AuditStatus::Succeeded,
                reason: audit.reason,
                metadata,
            },
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn record_sink_delivery_outcome(
        &self,
        outcome: NewSinkDeliveryOutcome,
    ) -> Result<String, StorageError> {
        let delivery_id = generated_id("delivery");
        let status_name = outcome.status.name();
        let http_status = delivery_http_status(&outcome.status).map(i32::from);
        let response_body_bytes = delivery_response_body_bytes(&outcome.status)
            .map(|value| usize_to_i64("response_body_bytes", value))
            .transpose()?;
        let failure_reason = delivery_failure_reason(&outcome.status);
        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            r#"
            INSERT INTO sink_delivery_outcomes (
                delivery_id, project_id, event_id, sink_id, status,
                http_status, response_body_bytes, failure_reason
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
            delivery_id.as_str(),
            outcome.project_id.as_str(),
            outcome.event_id.as_str(),
            outcome.sink_id.as_str(),
            status_name,
            http_status,
            response_body_bytes,
            failure_reason.as_deref()
        )
        .execute(&mut *tx)
        .await?;

        if outcome.status.is_failure() {
            let mut details = serde_json::Map::new();
            details.insert("delivery_id".to_owned(), json!(delivery_id));
            details.insert("status".to_owned(), json!(status_name));
            if let Some(reason) = &failure_reason {
                details.insert("reason".to_owned(), json!(reason));
            }

            insert_failure_event_tx(
                &mut tx,
                NewFailureEvent {
                    project_id: outcome.project_id,
                    event_id: Some(outcome.event_id),
                    sink_id: Some(outcome.sink_id),
                    component: "forwarder".to_owned(),
                    failure_kind: "sink_delivery".to_owned(),
                    severity: delivery_failure_severity(status_name),
                    message: "sink delivery did not complete successfully".to_owned(),
                    details,
                },
            )
            .await?;
        }

        tx.commit().await?;
        Ok(delivery_id)
    }

    pub async fn record_audit_event(&self, event: NewAuditEvent) -> Result<String, StorageError> {
        let mut tx = self.pool.begin().await?;
        let audit_event_id = insert_audit_event_tx(&mut tx, event).await?;
        tx.commit().await?;
        Ok(audit_event_id)
    }

    pub async fn record_failure_event(
        &self,
        event: NewFailureEvent,
    ) -> Result<String, StorageError> {
        let mut tx = self.pool.begin().await?;
        let failure_id = insert_failure_event_tx(&mut tx, event).await?;
        tx.commit().await?;
        Ok(failure_id)
    }

    pub async fn resolve_failure(
        &self,
        failure_id: &str,
        resolution: &str,
        audit: AuditContext,
    ) -> Result<(), StorageError> {
        if failure_id.trim().is_empty() || resolution.trim().is_empty() {
            return Err(StorageError::InvalidConfig {
                reason: "failure_id and resolution must not be empty",
            });
        }

        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            r#"
            UPDATE failure_events
            SET resolved_at = now(), resolution = $2
            WHERE failure_id = $1 AND resolved_at IS NULL
            "#,
        )
        .bind(failure_id)
        .bind(resolution)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            return Err(StorageError::InvalidStoredState {
                reason: "failure was not found or already resolved",
            });
        }

        insert_audit_event_tx(
            &mut tx,
            NewAuditEvent {
                project_id: None,
                actor_id: audit.actor_id,
                action: "failure.resolve".to_owned(),
                target_type: "failure".to_owned(),
                target_id: failure_id.to_owned(),
                status: AuditStatus::Succeeded,
                reason: audit.reason,
                metadata: serde_json::Map::new(),
            },
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }
}

async fn insert_audit_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    event: NewAuditEvent,
) -> Result<String, StorageError> {
    validate_non_empty("action", &event.action)?;
    validate_non_empty("target_type", &event.target_type)?;
    validate_non_empty("target_id", &event.target_id)?;

    let audit_event_id = generated_id("audit");
    let project_id = event.project_id.as_ref().map(ToString::to_string);
    let actor_id = event.actor_id.as_ref().map(ToString::to_string);

    sqlx::query(
        r#"
        INSERT INTO audit_events (
            audit_event_id, project_id, actor_id, action, target_type,
            target_id, status, reason, metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(audit_event_id.as_str())
    .bind(project_id.as_deref())
    .bind(actor_id.as_deref())
    .bind(event.action.as_str())
    .bind(event.target_type.as_str())
    .bind(event.target_id.as_str())
    .bind(event.status.as_str())
    .bind(event.reason.as_deref())
    .bind(Json(event.metadata))
    .execute(&mut **tx)
    .await?;

    Ok(audit_event_id)
}

async fn insert_failure_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    event: NewFailureEvent,
) -> Result<String, StorageError> {
    validate_non_empty("component", &event.component)?;
    validate_non_empty("failure_kind", &event.failure_kind)?;
    validate_non_empty("message", &event.message)?;

    let failure_id = generated_id("failure");

    sqlx::query(
        r#"
        INSERT INTO failure_events (
            failure_id, project_id, event_id, sink_id, component,
            failure_kind, severity, message, details
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(failure_id.as_str())
    .bind(event.project_id.as_str())
    .bind(event.event_id.as_ref().map(|id| id.as_str()))
    .bind(event.sink_id.as_ref().map(|id| id.as_str()))
    .bind(event.component.as_str())
    .bind(event.failure_kind.as_str())
    .bind(event.severity.as_str())
    .bind(event.message.as_str())
    .bind(Json(event.details))
    .execute(&mut **tx)
    .await?;

    Ok(failure_id)
}
fn validate_config(config: &PostgresStorageConfig) -> Result<(), StorageError> {
    if config.database_url.trim().is_empty() {
        return Err(StorageError::InvalidConfig {
            reason: "database_url must not be empty",
        });
    }
    if config.max_connections == 0 {
        return Err(StorageError::InvalidConfig {
            reason: "max_connections must be greater than zero",
        });
    }
    if config.acquire_timeout.is_zero()
        || config.idle_timeout.is_zero()
        || config.max_lifetime.is_zero()
    {
        return Err(StorageError::InvalidConfig {
            reason: "pool timeouts must be greater than zero",
        });
    }
    Ok(())
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), StorageError> {
    if value.trim().is_empty() {
        return Err(StorageError::InvalidConfig { reason: field });
    }
    Ok(())
}

fn generated_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::now_v7())
}

fn version_to_i64(version: u64) -> Result<i64, StorageError> {
    i64::try_from(version).map_err(|_| StorageError::VersionOverflow { version })
}

fn usize_to_i64(field: &'static str, value: usize) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| StorageError::NumericOverflow { field })
}

fn delivery_http_status(status: &SinkDeliveryStatus) -> Option<u16> {
    match status {
        SinkDeliveryStatus::Delivered { http_status, .. }
        | SinkDeliveryStatus::HttpRejected { http_status, .. } => Some(*http_status),
        SinkDeliveryStatus::TimedOut
        | SinkDeliveryStatus::ResponseTooLarge { .. }
        | SinkDeliveryStatus::Failed { .. } => None,
    }
}

fn delivery_response_body_bytes(status: &SinkDeliveryStatus) -> Option<usize> {
    match status {
        SinkDeliveryStatus::Delivered {
            response_body_bytes,
            ..
        }
        | SinkDeliveryStatus::HttpRejected {
            response_body_bytes,
            ..
        } => Some(*response_body_bytes),
        SinkDeliveryStatus::TimedOut
        | SinkDeliveryStatus::ResponseTooLarge { .. }
        | SinkDeliveryStatus::Failed { .. } => None,
    }
}

fn delivery_failure_reason(status: &SinkDeliveryStatus) -> Option<String> {
    match status {
        SinkDeliveryStatus::Delivered { .. } => None,
        SinkDeliveryStatus::HttpRejected { http_status, .. } => Some(format!("http_{http_status}")),
        SinkDeliveryStatus::TimedOut => Some("timed_out".to_owned()),
        SinkDeliveryStatus::ResponseTooLarge { max } => Some(format!("response_too_large:{max}")),
        SinkDeliveryStatus::Failed { reason } => Some(reason.clone()),
    }
}

fn delivery_failure_severity(status_name: &str) -> FailureSeverity {
    match status_name {
        "http_rejected" => FailureSeverity::Warning,
        "timed_out" | "response_too_large" | "failed" => FailureSeverity::Error,
        _ => FailureSeverity::Error,
    }
}
