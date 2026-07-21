use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::prelude::BASE64_STANDARD_NO_PAD;
use pipe_bolt_domain::{
    CommandExecutionId, EventId, MqttQos, ProjectConfig, ProjectId, SinkId, UserId,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::error::StorageError;
use crate::model::{
    AuditContext, AuditEventRecord, AuditStatus, CommandExecutionRecord, CommandExecutionStatus,
    FailureEventRecord, FailureListQuery, FailureSeverity, NewAuditEvent, NewCommandExecution,
    NewFailureEvent, NewSinkDeliveryOutcome, OperationalListQuery, ProjectConfigWriteResult,
    RetentionConfig, SinkDeliveryOutcomeRecord, SinkDeliveryStatus,
};
use crate::project_config_codec::{ProjectConfigCodec, StoredProjectConfig};
use crate::secret::SecretCipher;

static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

const DEFAULT_MAX_CONNECTIONS: u32 = 8;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(1800);

const MAX_IDENTIFIER_BYTES: usize = 96;
const MAX_TARGET_ID_BYTES: usize = 256;
const MAX_REASON_BYTES: usize = 1024;
const MAX_MESSAGE_BYTES: usize = 2048;
const MAX_METADATA_JSON_BYTES: usize = 16 * 1024;
const MAX_DETAILS_JSON_BYTES: usize = 16 * 1024;

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

    pub async fn health_check(&self) -> Result<(), StorageError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn load_project_config(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<ProjectConfig>, StorageError> {
        let row = sqlx::query("SELECT config FROM project_configs WHERE project_id = $1")
            .bind(project_id.as_str())
            .fetch_optional(&self.pool)
            .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let config_json: serde_json::Value = row.try_get("config")?;
        let stored = serde_json::from_value::<StoredProjectConfig>(config_json)?;
        let config = self.codec.decode(stored)?;
        Ok(Some(config))
    }

    pub async fn create_project_config(
        &self,
        config: &ProjectConfig,
        audit: AuditContext,
    ) -> Result<ProjectConfigWriteResult, StorageError> {
        let mut config = config.clone();
        if config.version == 0 {
            config.version = 1;
        }
        config.validate()?;

        let encoded = self.encode_project_config(&config)?;
        let tenant_id = config.tenant_id.as_ref().map(ToString::to_string);
        let mut tx = self.pool.begin().await?;

        let result = sqlx::query(
            r#"
            INSERT INTO project_configs (project_id, tenant_id, name, enabled, version, config_hash, config)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (project_id) DO NOTHING
            "#,
        )
            .bind(config.id.as_str())
            .bind(tenant_id.as_deref())
            .bind(config.name.as_str())
            .bind(config.enabled)
            .bind(version_to_i64(config.version)?)
            .bind(encoded.config_hash.as_str())
            .bind(Json(encoded.config_json.clone()))
            .execute(&mut *tx)
            .await?;

        if result.rows_affected() == 0 {
            let actual_version = current_project_version_tx(&mut tx, &config.id).await?;
            return Err(StorageError::VersionConflict {
                project_id: config.id.to_string(),
                expected_version: None,
                actual_version,
            });
        }

        let write = insert_config_revision_tx(&mut tx, &config, &encoded, &audit).await?;
        insert_config_audit_tx(
            &mut tx,
            &config,
            &write,
            None,
            audit,
            "project_config.create",
        )
        .await?;

        tx.commit().await?;
        Ok(write)
    }

    pub async fn update_project_config(
        &self,
        config: &ProjectConfig,
        expected_version: u64,
        audit: AuditContext,
    ) -> Result<ProjectConfigWriteResult, StorageError> {
        let next_version =
            expected_version
                .checked_add(1)
                .ok_or(StorageError::VersionOverflow {
                    version: expected_version,
                })?;
        let mut config = config.clone();
        config.version = next_version;
        config.validate()?;

        let encoded = self.encode_project_config(&config)?;
        let tenant_id = config.tenant_id.as_ref().map(ToString::to_string);
        let mut tx = self.pool.begin().await?;

        let result = sqlx::query(
            r#"
            UPDATE project_configs
            SET tenant_id = $2,
                name = $3,
                enabled = $4,
                version = $5,
                config_hash = $6,
                config = $7,
                updated_at = now()
            WHERE project_id = $1 AND version = $8
            "#,
        )
        .bind(config.id.as_str())
        .bind(tenant_id.as_deref())
        .bind(config.name.as_str())
        .bind(config.enabled)
        .bind(version_to_i64(next_version)?)
        .bind(encoded.config_hash.as_str())
        .bind(Json(encoded.config_json.clone()))
        .bind(version_to_i64(expected_version)?)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            let actual_version = current_project_version_tx(&mut tx, &config.id).await?;
            return Err(StorageError::VersionConflict {
                project_id: config.id.to_string(),
                expected_version: Some(expected_version),
                actual_version,
            });
        }

        let write = insert_config_revision_tx(&mut tx, &config, &encoded, &audit).await?;
        insert_config_audit_tx(
            &mut tx,
            &config,
            &write,
            Some(expected_version),
            audit,
            "project_config.update",
        )
        .await?;

        tx.commit().await?;
        Ok(write)
    }

    pub async fn record_sink_delivery_outcome(
        &self,
        outcome: NewSinkDeliveryOutcome,
    ) -> Result<String, StorageError> {
        validate_sink_delivery_outcome(&outcome)?;

        let delivery_id = generated_id("delivery");
        let status_name = outcome.status.name();
        let http_status = delivery_http_status(&outcome.status).map(i32::from);
        let response_body_bytes = delivery_response_body_bytes(&outcome.status)
            .map(|value| usize_to_i64("response_body_bytes", value))
            .transpose()?;
        let failure_reason = delivery_failure_reason(&outcome.status);
        let duration_ms = outcome
            .duration_ms
            .map(|value| u64_to_i64("duration_ms", value))
            .transpose()?;
        let attempt = i32::from(outcome.attempt.max(1));
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO sink_delivery_outcomes (
                delivery_id, project_id, event_id, sink_id, status,
                http_status, response_body_bytes, failure_reason,
                correlation_id, duration_ms, attempt
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(delivery_id.as_str())
        .bind(outcome.project_id.as_str())
        .bind(outcome.event_id.as_str())
        .bind(outcome.sink_id.as_str())
        .bind(status_name)
        .bind(http_status)
        .bind(response_body_bytes)
        .bind(failure_reason.as_deref())
        .bind(outcome.correlation_id.as_deref())
        .bind(duration_ms)
        .bind(attempt)
        .execute(&mut *tx)
        .await?;

        if outcome.status.is_failure() {
            let mut details = serde_json::Map::new();
            details.insert("delivery_id".to_owned(), json!(delivery_id));
            details.insert("status".to_owned(), json!(status_name));
            if let Some(reason) = &failure_reason {
                details.insert("reason".to_owned(), json!(reason));
            }
            if let Some(correlation_id) = &outcome.correlation_id {
                details.insert("correlation_id".to_owned(), json!(correlation_id));
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

    pub async fn record_command_execution(
        &self,
        execution: NewCommandExecution,
    ) -> Result<CommandExecutionRecord, StorageError> {
        validate_command_execution(&execution)?;

        let payload_size_bytes = u64_to_i64("payload_size_bytes", execution.payload_size_bytes)?;
        let actor_id = execution.actor_id.as_ref().map(ToString::to_string);
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            INSERT INTO command_executions (
                command_execution_id, project_id, command_template_id, broker_id,
                actor_id, status, topic, qos, retain, payload_size_bytes, failure_reason
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING occurred_at
            "#,
        )
        .bind(execution.command_execution_id.as_str())
        .bind(execution.project_id.as_str())
        .bind(execution.command_template_id.as_str())
        .bind(execution.broker_id.as_str())
        .bind(actor_id.as_deref())
        .bind(execution.status.as_str())
        .bind(execution.topic.as_str())
        .bind(mqtt_qos_name(execution.qos))
        .bind(execution.retain)
        .bind(payload_size_bytes)
        .bind(execution.failure_reason.as_deref())
        .fetch_one(&mut *tx)
        .await?;

        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "command_execution_id".to_owned(),
            json!(execution.command_execution_id.to_string()),
        );
        metadata.insert(
            "command_template_id".to_owned(),
            json!(execution.command_template_id),
        );
        metadata.insert("broker_id".to_owned(), json!(execution.broker_id));
        metadata.insert("topic".to_owned(), json!(execution.topic));
        metadata.insert("status".to_owned(), json!(execution.status.as_str()));
        metadata.insert(
            "payload_size_bytes".to_owned(),
            json!(execution.payload_size_bytes),
        );
        if let Some(failure_reason) = &execution.failure_reason {
            metadata.insert("failure_reason".to_owned(), json!(failure_reason));
        }

        let audit_event_id = insert_audit_event_tx(
            &mut tx,
            NewAuditEvent {
                project_id: Some(execution.project_id.clone()),
                actor_id: execution.actor_id.clone(),
                action: "command.execute".to_owned(),
                target_type: "command_template".to_owned(),
                target_id: execution.command_template_id.to_string(),
                status: if execution.status == CommandExecutionStatus::Failed {
                    AuditStatus::Failed
                } else {
                    AuditStatus::Succeeded
                },
                reason: execution.reason.clone(),
                metadata,
            },
        )
        .await?;

        tx.commit().await?;
        Ok(CommandExecutionRecord {
            command_execution_id: execution.command_execution_id,
            project_id: execution.project_id,
            command_template_id: execution.command_template_id,
            broker_id: execution.broker_id,
            actor_id: execution.actor_id,
            status: execution.status,
            topic: execution.topic,
            qos: execution.qos,
            retain: execution.retain,
            payload_size_bytes: execution.payload_size_bytes,
            failure_reason: execution.failure_reason,
            occurred_at: row.try_get("occurred_at")?,
            audit_event_id,
        })
    }

    pub async fn mark_command_execution_failed(
        &self,
        command_execution_id: &CommandExecutionId,
        failure_reason: &str,
    ) -> Result<(), StorageError> {
        validate_bounded_text("failure_reason", failure_reason, MAX_REASON_BYTES)?;

        sqlx::query(
            r#"
            UPDATE command_executions
            SET status = 'failed', failure_reason = $2
            WHERE command_execution_id = $1
            "#,
        )
        .bind(command_execution_id.as_str())
        .bind(failure_reason)
        .execute(self.pool())
        .await?;

        Ok(())
    }

    pub async fn mark_command_execution_published(
        &self,
        command_execution_id: &CommandExecutionId,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            UPDATE command_executions
            SET status = 'published', failure_reason = NULL
            WHERE command_execution_id = $1
            "#,
        )
        .bind(command_execution_id.as_str())
        .execute(self.pool())
        .await?;

        Ok(())
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
        project_id: &ProjectId,
        failure_id: &str,
        resolution: &str,
        audit: AuditContext,
    ) -> Result<(), StorageError> {
        validate_bounded_text("failure_id", failure_id, MAX_TARGET_ID_BYTES)?;
        validate_bounded_text("resolution", resolution, MAX_MESSAGE_BYTES)?;

        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            UPDATE failure_events
            SET resolved_at = now(), resolution = $3
            WHERE failure_id = $1
              AND project_id = $2
              AND resolved_at IS NULL
            RETURNING project_id
            "#,
        )
        .bind(failure_id)
        .bind(project_id.as_str())
        .bind(resolution)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            let existing = sqlx::query(
                r#"
                SELECT resolved_at
                FROM failure_events
                WHERE failure_id = $1 AND project_id = $2
                "#,
            )
            .bind(failure_id)
            .bind(project_id.as_str())
            .fetch_optional(&mut *tx)
            .await?;

            return match existing {
                None => Err(StorageError::FailureNotFound {
                    project_id: project_id.to_string(),
                    failure_id: failure_id.to_owned(),
                }),
                Some(_) => Err(StorageError::FailureAlreadyResolved {
                    project_id: project_id.to_string(),
                    failure_id: failure_id.to_owned(),
                }),
            };
        };

        let project_id = ProjectId::new(row.try_get::<String, _>("project_id")?)?;
        insert_audit_event_tx(
            &mut tx,
            NewAuditEvent {
                project_id: Some(project_id),
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

    pub async fn list_audit_events(
        &self,
        project_id: &ProjectId,
        query: OperationalListQuery,
    ) -> Result<Vec<AuditEventRecord>, StorageError> {
        let query = query.sanitized();
        let rows = sqlx::query(
            r#"
            SELECT audit_event_id, project_id, actor_id, action, target_type,
                   target_id, status, reason, metadata, occurred_at
            FROM audit_events
            WHERE project_id = $1
              AND ($2::timestamptz IS NULL OR occurred_at < $2)
            ORDER BY occurred_at DESC
            LIMIT $3
            "#,
        )
        .bind(project_id.as_str())
        .bind(query.before)
        .bind(i64::from(query.limit))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(audit_event_from_row).collect()
    }

    pub async fn list_failures(
        &self,
        project_id: &ProjectId,
        query: FailureListQuery,
    ) -> Result<Vec<FailureEventRecord>, StorageError> {
        let query = query.sanitized();
        let rows = sqlx::query(
            r#"
            SELECT failure_id, project_id, event_id, sink_id, component,
                   failure_kind, severity, message, details, occurred_at,
                   resolved_at, resolution
            FROM failure_events
            WHERE project_id = $1
              AND ($2::timestamptz IS NULL OR occurred_at < $2)
              AND ($3::bool = false OR resolved_at IS NULL)
            ORDER BY occurred_at DESC
            LIMIT $4
            "#,
        )
        .bind(project_id.as_str())
        .bind(query.before)
        .bind(query.unresolved_only)
        .bind(i64::from(query.limit))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(failure_event_from_row).collect()
    }

    pub async fn list_delivery_outcomes(
        &self,
        project_id: &ProjectId,
        query: OperationalListQuery,
    ) -> Result<Vec<SinkDeliveryOutcomeRecord>, StorageError> {
        let query = query.sanitized();
        let rows = sqlx::query(
            r#"
            SELECT delivery_id, project_id, event_id, sink_id, status,
                   http_status, response_body_bytes, failure_reason,
                   correlation_id, duration_ms, attempt, occurred_at
            FROM sink_delivery_outcomes
            WHERE project_id = $1
              AND ($2::timestamptz IS NULL OR occurred_at < $2)
            ORDER BY occurred_at DESC
            LIMIT $3
            "#,
        )
        .bind(project_id.as_str())
        .bind(query.before)
        .bind(i64::from(query.limit))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(sink_delivery_outcome_from_row)
            .collect()
    }

    pub async fn delete_expired_operational_rows(
        &self,
        project_id: &ProjectId,
        retention: RetentionConfig,
    ) -> Result<(), StorageError> {
        validate_retention(retention)?;

        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "DELETE FROM audit_events WHERE project_id = $1 AND occurred_at < now() - ($2::int * interval '1 day')",
        )
            .bind(project_id.as_str())
            .bind(i32::from(retention.audit_retention_days))
            .execute(&mut *tx)
            .await?;

        sqlx::query(
            "DELETE FROM sink_delivery_outcomes WHERE project_id = $1 AND occurred_at < now() - ($2::int * interval '1 day')",
        )
            .bind(project_id.as_str())
            .bind(i32::from(retention.delivery_outcome_retention_days))
            .execute(&mut *tx)
            .await?;

        sqlx::query(
            "DELETE FROM failure_events WHERE project_id = $1 AND occurred_at < now() - ($2::int * interval '1 day') AND resolved_at IS NOT NULL",
        )
            .bind(project_id.as_str())
            .bind(i32::from(retention.failure_retention_days))
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    fn encode_project_config(
        &self,
        config: &ProjectConfig,
    ) -> Result<EncodedProjectConfig, StorageError> {
        let stored = self.codec.encode(config)?;
        let config_json = serde_json::to_value(stored)?;
        let config_hash = hash_config_json(&config_json)?;

        Ok(EncodedProjectConfig {
            config_json,
            config_hash,
        })
    }
}

struct EncodedProjectConfig {
    config_json: serde_json::Value,
    config_hash: String,
}

async fn insert_config_revision_tx(
    tx: &mut Transaction<'_, Postgres>,
    config: &ProjectConfig,
    encoded: &EncodedProjectConfig,
    audit: &AuditContext,
) -> Result<ProjectConfigWriteResult, StorageError> {
    let revision_id = generated_id("config_revision");
    let actor_id = audit.actor_id.as_ref().map(ToString::to_string);

    sqlx::query(
        r#"
        INSERT INTO project_config_revisions (
            revision_id, project_id, version, config_hash, config, actor_id, reason
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(revision_id.as_str())
    .bind(config.id.as_str())
    .bind(version_to_i64(config.version)?)
    .bind(encoded.config_hash.as_str())
    .bind(Json(encoded.config_json.clone()))
    .bind(actor_id.as_deref())
    .bind(audit.reason.as_deref())
    .execute(&mut **tx)
    .await?;

    Ok(ProjectConfigWriteResult {
        project_id: config.id.clone(),
        version: config.version,
        revision_id,
        config_hash: encoded.config_hash.clone(),
    })
}

async fn insert_config_audit_tx(
    tx: &mut Transaction<'_, Postgres>,
    config: &ProjectConfig,
    write: &ProjectConfigWriteResult,
    expected_version: Option<u64>,
    audit: AuditContext,
    action: &str,
) -> Result<(), StorageError> {
    let mut metadata = serde_json::Map::new();
    metadata.insert("version".to_owned(), json!(write.version));
    metadata.insert("revision_id".to_owned(), json!(write.revision_id));
    metadata.insert("config_hash".to_owned(), json!(write.config_hash));
    if let Some(expected_version) = expected_version {
        metadata.insert("expected_version".to_owned(), json!(expected_version));
    }

    insert_audit_event_tx(
        tx,
        NewAuditEvent {
            project_id: Some(config.id.clone()),
            actor_id: audit.actor_id,
            action: action.to_owned(),
            target_type: "project_config".to_owned(),
            target_id: config.id.to_string(),
            status: AuditStatus::Succeeded,
            reason: audit.reason,
            metadata,
        },
    )
    .await?;

    Ok(())
}

async fn insert_audit_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    event: NewAuditEvent,
) -> Result<String, StorageError> {
    validate_identifier("action", &event.action)?;
    validate_identifier("target_type", &event.target_type)?;
    validate_bounded_text("target_id", &event.target_id, MAX_TARGET_ID_BYTES)?;
    if let Some(reason) = &event.reason {
        validate_bounded_text("reason", reason, MAX_REASON_BYTES)?;
    }
    validate_json_object_size("metadata", &event.metadata, MAX_METADATA_JSON_BYTES)?;

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
    validate_identifier("component", &event.component)?;
    validate_identifier("failure_kind", &event.failure_kind)?;
    validate_bounded_text("message", &event.message, MAX_MESSAGE_BYTES)?;
    validate_json_object_size("details", &event.details, MAX_DETAILS_JSON_BYTES)?;

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

async fn current_project_version_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &ProjectId,
) -> Result<Option<u64>, StorageError> {
    let row = sqlx::query("SELECT version FROM project_configs WHERE project_id = $1")
        .bind(project_id.as_str())
        .fetch_optional(&mut **tx)
        .await?;

    row.map(|row| i64_to_u64("version", row.try_get::<i64, _>("version")?))
        .transpose()
}

fn audit_event_from_row(row: PgRow) -> Result<AuditEventRecord, StorageError> {
    let project_id = row
        .try_get::<Option<String>, _>("project_id")?
        .map(ProjectId::new)
        .transpose()?;
    let actor_id = row
        .try_get::<Option<String>, _>("actor_id")?
        .map(UserId::new)
        .transpose()?;
    let metadata: serde_json::Value = row.try_get("metadata")?;

    Ok(AuditEventRecord {
        audit_event_id: row.try_get("audit_event_id")?,
        project_id,
        actor_id,
        action: row.try_get("action")?,
        target_type: row.try_get("target_type")?,
        target_id: row.try_get("target_id")?,
        status: parse_audit_status(&row.try_get::<String, _>("status")?)?,
        reason: row.try_get("reason")?,
        metadata: value_to_object("metadata", metadata)?,
        occurred_at: row.try_get("occurred_at")?,
    })
}

fn failure_event_from_row(row: PgRow) -> Result<FailureEventRecord, StorageError> {
    let project_id = ProjectId::new(row.try_get::<String, _>("project_id")?)?;
    let event_id = row
        .try_get::<Option<String>, _>("event_id")?
        .map(EventId::new)
        .transpose()?;
    let sink_id = row
        .try_get::<Option<String>, _>("sink_id")?
        .map(SinkId::new)
        .transpose()?;
    let details: serde_json::Value = row.try_get("details")?;

    Ok(FailureEventRecord {
        failure_id: row.try_get("failure_id")?,
        project_id,
        event_id,
        sink_id,
        component: row.try_get("component")?,
        failure_kind: row.try_get("failure_kind")?,
        severity: parse_failure_severity(&row.try_get::<String, _>("severity")?)?,
        message: row.try_get("message")?,
        details: value_to_object("details", details)?,
        occurred_at: row.try_get("occurred_at")?,
        resolved_at: row.try_get("resolved_at")?,
        resolution: row.try_get("resolution")?,
    })
}

fn sink_delivery_outcome_from_row(row: PgRow) -> Result<SinkDeliveryOutcomeRecord, StorageError> {
    Ok(SinkDeliveryOutcomeRecord {
        delivery_id: row.try_get("delivery_id")?,
        project_id: ProjectId::new(row.try_get::<String, _>("project_id")?)?,
        event_id: EventId::new(row.try_get::<String, _>("event_id")?)?,
        sink_id: SinkId::new(row.try_get::<String, _>("sink_id")?)?,
        status: row.try_get("status")?,
        http_status: optional_i32_to_u16("http_status", row.try_get("http_status")?)?,
        response_body_bytes: optional_i64_to_u64(
            "response_body_bytes",
            row.try_get("response_body_bytes")?,
        )?,
        failure_reason: row.try_get("failure_reason")?,
        correlation_id: row.try_get("correlation_id")?,
        duration_ms: optional_i64_to_u64("duration_ms", row.try_get("duration_ms")?)?,
        attempt: i32_to_u16("attempt", row.try_get("attempt")?)?,
        occurred_at: row.try_get("occurred_at")?,
    })
}

fn parse_audit_status(value: &str) -> Result<AuditStatus, StorageError> {
    match value {
        "succeeded" => Ok(AuditStatus::Succeeded),
        "failed" => Ok(AuditStatus::Failed),
        _ => Err(StorageError::InvalidStoredState {
            reason: "unknown audit status",
        }),
    }
}

fn parse_failure_severity(value: &str) -> Result<FailureSeverity, StorageError> {
    match value {
        "warning" => Ok(FailureSeverity::Warning),
        "error" => Ok(FailureSeverity::Error),
        "critical" => Ok(FailureSeverity::Critical),
        _ => Err(StorageError::InvalidStoredState {
            reason: "unknown failure severity",
        }),
    }
}

fn value_to_object(
    field: &'static str,
    value: serde_json::Value,
) -> Result<serde_json::Map<String, serde_json::Value>, StorageError> {
    match value {
        serde_json::Value::Object(object) => Ok(object),
        _ => Err(StorageError::InvalidField {
            field,
            reason: "stored JSON value must be an object",
        }),
    }
}

fn optional_i64_to_u64(
    field: &'static str,
    value: Option<i64>,
) -> Result<Option<u64>, StorageError> {
    value.map(|value| i64_to_u64(field, value)).transpose()
}

fn optional_i32_to_u16(
    field: &'static str,
    value: Option<i32>,
) -> Result<Option<u16>, StorageError> {
    value.map(|value| i32_to_u16(field, value)).transpose()
}

fn i32_to_u16(field: &'static str, value: i32) -> Result<u16, StorageError> {
    u16::try_from(value).map_err(|_| StorageError::NumericOverflow { field })
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

fn validate_sink_delivery_outcome(outcome: &NewSinkDeliveryOutcome) -> Result<(), StorageError> {
    if let Some(correlation_id) = &outcome.correlation_id {
        validate_bounded_text("correlation_id", correlation_id, MAX_TARGET_ID_BYTES)?;
    }
    if let SinkDeliveryStatus::Failed { reason } = &outcome.status {
        validate_bounded_text("failure_reason", reason, MAX_REASON_BYTES)?;
    }
    Ok(())
}

fn validate_command_execution(execution: &NewCommandExecution) -> Result<(), StorageError> {
    validate_bounded_text("topic", &execution.topic, 1024)?;
    if let Some(reason) = &execution.reason {
        validate_bounded_text("reason", reason, MAX_REASON_BYTES)?;
    }
    if let Some(failure_reason) = &execution.failure_reason {
        validate_bounded_text("failure_reason", failure_reason, MAX_REASON_BYTES)?;
    }
    Ok(())
}

fn validate_retention(retention: RetentionConfig) -> Result<(), StorageError> {
    if retention.audit_retention_days == 0
        || retention.delivery_outcome_retention_days == 0
        || retention.failure_retention_days == 0
    {
        return Err(StorageError::InvalidConfig {
            reason: "retention days must be greater than zero",
        });
    }
    Ok(())
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), StorageError> {
    validate_bounded_text(field, value, MAX_IDENTIFIER_BYTES)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        return Err(StorageError::InvalidField {
            field,
            reason: "identifier contains unsupported characters",
        });
    }
    Ok(())
}

fn validate_bounded_text(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), StorageError> {
    if value.trim().is_empty() {
        return Err(StorageError::InvalidField {
            field,
            reason: "must not be empty",
        });
    }
    let actual_bytes = value.len();
    if actual_bytes > max_bytes {
        return Err(StorageError::FieldTooLarge {
            field,
            actual_bytes,
            max_bytes,
        });
    }
    Ok(())
}

fn validate_json_object_size(
    field: &'static str,
    value: &serde_json::Map<String, serde_json::Value>,
    max_bytes: usize,
) -> Result<(), StorageError> {
    let actual_bytes = serde_json::to_vec(value)?.len();
    if actual_bytes > max_bytes {
        return Err(StorageError::FieldTooLarge {
            field,
            actual_bytes,
            max_bytes,
        });
    }
    Ok(())
}

fn hash_config_json(config_json: &serde_json::Value) -> Result<String, StorageError> {
    let bytes = serde_json::to_vec(config_json)?;
    let digest = Sha256::digest(bytes);
    Ok(format!("sha256:{}", BASE64_STANDARD_NO_PAD.encode(digest)))
}

fn generated_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::now_v7())
}

const fn mqtt_qos_name(qos: MqttQos) -> &'static str {
    match qos {
        MqttQos::AtMostOnce => "at_most_once",
        MqttQos::AtLeastOnce => "at_least_once",
        MqttQos::ExactlyOnce => "exactly_once",
    }
}

fn version_to_i64(version: u64) -> Result<i64, StorageError> {
    i64::try_from(version).map_err(|_| StorageError::VersionOverflow { version })
}

fn i64_to_u64(field: &'static str, value: i64) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::NumericOverflow { field })
}

fn usize_to_i64(field: &'static str, value: usize) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| StorageError::NumericOverflow { field })
}

fn u64_to_i64(field: &'static str, value: u64) -> Result<i64, StorageError> {
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
