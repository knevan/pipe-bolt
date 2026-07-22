use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use pipe_bolt_domain::{
    BackpressurePolicy, BrokerConnectionConfig, BrokerId, CommandExecutionId, CommandTemplateId,
    DeviceIdExtraction, MqttCredentials, MqttQos, PayloadCodecKind, ProjectConfig, ProjectId,
    ReconnectPolicy, RouteId, SecretString, SinkDefinition, SinkId, SinkKind, TlsMode, TopicFilter,
    TopicRouteConfig,
};
use pipe_bolt_storage::error::StorageError;
use pipe_bolt_storage::model::{
    AuditContext, CommandExecutionStatus, FailureSeverity, NewCommandExecution, NewFailureEvent,
    NewSinkDeliveryOutcome, SinkDeliveryStatus,
};
use pipe_bolt_storage::postgres::{PostgresStorage, PostgresStorageConfig};
use pipe_bolt_storage::secret::StorageKeyring;
use sqlx::Row;

#[tokio::test]
async fn storage_migration_applies_on_empty_postgres() {
    let Some(storage) = test_storage().await else {
        return;
    };

    storage.migrate().await.expect("migration applies");
}

#[tokio::test]
async fn project_config_roundtrips_with_encrypted_broker_and_sink_secrets() {
    let Some(storage) = migrated_storage().await else {
        return;
    };
    let config = project_config(&unique_project_id("project-roundtrip"));

    storage
        .create_project_config(&config, AuditContext::system("test create"))
        .await
        .expect("create config");
    let loaded = storage
        .load_project_config(&config.id)
        .await
        .expect("load config")
        .expect("config exists");

    assert_eq!(loaded, config);
}

#[tokio::test]
async fn project_config_upsert_rejects_stale_expected_version() {
    let Some(storage) = migrated_storage().await else {
        return;
    };
    let config = project_config(&unique_project_id("project-conflict"));
    storage
        .create_project_config(&config, AuditContext::system("test create"))
        .await
        .expect("create config");

    let stale = storage
        .update_project_config(&config, 0, AuditContext::system("stale update"))
        .await
        .expect_err("version conflict");

    assert!(matches!(stale, StorageError::VersionConflict { .. }));
}

#[tokio::test]
async fn project_config_revision_is_written_for_each_successful_update() {
    let Some(storage) = migrated_storage().await else {
        return;
    };
    let mut config = project_config(&unique_project_id("project-revision"));
    storage
        .create_project_config(&config, AuditContext::system("test create"))
        .await
        .expect("create config");
    config.name = "Updated Project".to_owned();

    let write = storage
        .update_project_config(&config, 1, AuditContext::system("test update"))
        .await
        .expect("update config");

    assert_eq!(write.version, 2);
}

#[tokio::test]
async fn sink_delivery_failure_writes_outcome_and_failure_in_one_transaction() {
    let Some(storage) = migrated_storage().await else {
        return;
    };
    let config = project_config(&unique_project_id("project-outcome"));
    storage
        .create_project_config(&config, AuditContext::system("test create"))
        .await
        .expect("create config");

    let delivery_id = storage
        .record_sink_delivery_outcome(NewSinkDeliveryOutcome {
            project_id: config.id.clone(),
            event_id: pipe_bolt_domain::EventId::new("event-1").expect("event id"),
            sink_id: SinkId::new("sink-webhook").expect("sink id"),
            status: SinkDeliveryStatus::TimedOut,
            correlation_id: Some("corr-1".to_owned()),
            duration_ms: Some(3000),
            attempt: 1,
        })
        .await
        .expect("record outcome");

    assert!(delivery_id.starts_with("delivery_"));
}

#[tokio::test]
async fn resolve_failure_sets_project_scoped_audit_event() {
    let Some(storage) = migrated_storage().await else {
        return;
    };
    let config = project_config(&unique_project_id("project-resolve"));
    storage
        .create_project_config(&config, AuditContext::system("test create"))
        .await
        .expect("create config");
    let failure_id = storage
        .record_failure_event(NewFailureEvent {
            project_id: config.id.clone(),
            event_id: None,
            sink_id: None,
            component: "storage_test".to_owned(),
            failure_kind: "manual".to_owned(),
            severity: FailureSeverity::Error,
            message: "manual failure".to_owned(),
            details: serde_json::Map::new(),
        })
        .await
        .expect("record failure");

    storage
        .resolve_failure(
            &config.id,
            &failure_id,
            "resolved by test",
            AuditContext::system("test resolve"),
        )
        .await
        .expect("resolve failure");
    let row = sqlx::query(
        "SELECT project_id FROM audit_events WHERE action = 'failure.resolve' AND target_id = $1",
    )
    .bind(&failure_id)
    .fetch_one(storage.pool())
    .await
    .expect("audit row");
    let project_id: String = row.try_get("project_id").expect("project_id");

    assert_eq!(project_id, config.id.to_string());
}

#[tokio::test]
async fn command_execution_publish_success_updates_status() {
    let Some(storage) = migrated_storage().await else {
        return;
    };
    let config = project_config(&unique_project_id("project-command-published"));
    storage
        .create_project_config(&config, AuditContext::system("test create"))
        .await
        .expect("create config");
    let command_execution_id =
        CommandExecutionId::new(unique_id("command-exec-published")).expect("command execution id");
    storage
        .record_command_execution(command_execution(&config.id, &command_execution_id))
        .await
        .expect("record command execution");

    storage
        .mark_command_execution_published(&command_execution_id)
        .await
        .expect("mark command published");
    let row = sqlx::query(
        "SELECT status, failure_reason FROM command_executions WHERE command_execution_id = $1",
    )
    .bind(command_execution_id.as_str())
    .fetch_one(storage.pool())
    .await
    .expect("command execution row");
    let status: String = row.try_get("status").expect("status");
    let failure_reason: Option<String> = row.try_get("failure_reason").expect("failure_reason");

    assert_eq!((status, failure_reason), ("published".to_owned(), None));
}

#[tokio::test]
async fn command_execution_publish_failure_updates_status_and_reason() {
    let Some(storage) = migrated_storage().await else {
        return;
    };
    let config = project_config(&unique_project_id("project-command-failed"));
    storage
        .create_project_config(&config, AuditContext::system("test create"))
        .await
        .expect("create config");
    let command_execution_id =
        CommandExecutionId::new(unique_id("command-exec-failed")).expect("command execution id");
    storage
        .record_command_execution(command_execution(&config.id, &command_execution_id))
        .await
        .expect("record command execution");

    storage
        .mark_command_execution_failed(&command_execution_id, "mqtt client disconnected")
        .await
        .expect("mark command failed");
    let row = sqlx::query(
        "SELECT status, failure_reason FROM command_executions WHERE command_execution_id = $1",
    )
    .bind(command_execution_id.as_str())
    .fetch_one(storage.pool())
    .await
    .expect("command execution row");
    let status: String = row.try_get("status").expect("status");
    let failure_reason: Option<String> = row.try_get("failure_reason").expect("failure_reason");

    assert_eq!(
        (status, failure_reason),
        (
            "failed".to_owned(),
            Some("mqtt client disconnected".to_owned())
        )
    );
}

async fn migrated_storage() -> Option<PostgresStorage> {
    let storage = test_storage().await?;
    storage.migrate().await.expect("migration applies");
    Some(storage)
}

async fn test_storage() -> Option<PostgresStorage> {
    let database_url = std::env::var("PIPE_BOLT_TEST_DATABASE_URL").ok()?;
    let key = STANDARD.encode([7u8; 32]);
    let keyring = Arc::new(StorageKeyring::single("test", &key).expect("keyring"));
    let config = PostgresStorageConfig::new(database_url).expect("storage config");
    Some(
        PostgresStorage::connect(&config, keyring)
            .await
            .expect("storage"),
    )
}

fn unique_project_id(prefix: &str) -> String {
    unique_id(prefix)
}

fn unique_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7())
}

fn command_execution(
    project_id: &ProjectId,
    command_execution_id: &CommandExecutionId,
) -> NewCommandExecution {
    NewCommandExecution {
        command_execution_id: command_execution_id.clone(),
        project_id: project_id.clone(),
        command_template_id: CommandTemplateId::new("command-template-1")
            .expect("command template id"),
        broker_id: BrokerId::new("broker-local").expect("broker id"),
        actor_id: None,
        status: CommandExecutionStatus::Queued,
        topic: "devices/device-1/commands/relay".to_owned(),
        qos: MqttQos::AtLeastOnce,
        retain: false,
        payload_size_bytes: 14,
        failure_reason: None,
        reason: Some("test command".to_owned()),
    }
}

fn project_config(id: &str) -> ProjectConfig {
    let broker_id = BrokerId::new("broker-local").expect("broker id");
    ProjectConfig {
        id: ProjectId::new(id).expect("project id"),
        tenant_id: None,
        name: "Local Project".to_owned(),
        description: None,
        enabled: true,
        version: 1,
        brokers: vec![BrokerConnectionConfig {
            id: broker_id.clone(),
            name: "Local MQTT".to_owned(),
            host: "localhost".to_owned(),
            port: 1883,
            tls: TlsMode::Disabled,
            credentials: Some(MqttCredentials {
                username: "mqtt-user".to_owned(),
                password: SecretString::new("mqtt-password").expect("secret"),
            }),
            keep_alive: Duration::from_secs(30),
            clean_session: false,
            client_id: "pipe-bolt-local".to_owned(),
            reconnect: ReconnectPolicy::default(),
            enabled: true,
        }],
        routes: vec![TopicRouteConfig {
            id: RouteId::new("route-telemetry").expect("route id"),
            broker_id,
            name: "Telemetry".to_owned(),
            topic_filter: TopicFilter::new("devices/+/telemetry").expect("topic filter"),
            codec: PayloadCodecKind::Json,
            schema_mapping_id: None,
            device_id: DeviceIdExtraction::TopicWildcardIndex { index: 0 },
            event_type: "telemetry".to_owned(),
            qos: MqttQos::AtLeastOnce,
            enabled: true,
            backpressure: BackpressurePolicy::Reject,
        }],
        schema_mappings: Vec::new(),
        rules: Vec::new(),
        command_templates: Vec::new(),
        sinks: vec![SinkDefinition {
            id: SinkId::new("sink-webhook").expect("sink id"),
            name: "Webhook".to_owned(),
            enabled: true,
            kind: SinkKind::Webhook {
                url: "https://example.com/events".to_owned(),
                method: pipe_bolt_domain::HttpMethod::Post,
                headers: Vec::new(),
                timeout: Duration::from_secs(5),
            },
        }],
    }
}
