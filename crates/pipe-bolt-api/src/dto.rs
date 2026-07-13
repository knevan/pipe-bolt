use std::collections::BTreeMap;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use pipe_bolt_domain::{
    BrokerConnectionConfig, BrokerId, CommandExecutionId, CommandTemplate, CommandTemplateId,
    DecodedPayload, FieldValue, MqttQos, NormalizedEvent, PayloadSchemaMapping, ProjectConfig,
    ProjectId, RuleDefinition, SinkDefinition, TenantId, TopicRouteConfig,
};
#[cfg(feature = "salvo-oapi")]
use salvo::oapi::ToSchema;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const PROJECT_CONFIG_DOCUMENT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ProjectConfigDocumentV1 {
    pub project_id: ProjectId,
    pub tenant_id: Option<TenantId>,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub brokers: Vec<BrokerConnectionConfig>,
    pub routes: Vec<TopicRouteConfig>,
    pub schema_mappings: Vec<PayloadSchemaMapping>,
    pub rules: Vec<RuleDefinition>,
    pub command_templates: Vec<CommandTemplate>,
    pub sinks: Vec<SinkDefinition>,
}

impl ProjectConfigDocumentV1 {
    pub fn from_domain(config: ProjectConfig) -> Self {
        Self {
            project_id: config.id,
            tenant_id: config.tenant_id,
            name: config.name,
            description: config.description,
            enabled: config.enabled,
            brokers: config.brokers,
            routes: config.routes,
            schema_mappings: config.schema_mappings,
            rules: config.rules,
            command_templates: config.command_templates,
            sinks: config.sinks,
        }
    }

    pub fn into_domain(self, version: u64) -> ProjectConfig {
        ProjectConfig {
            id: self.project_id,
            tenant_id: self.tenant_id,
            name: self.name,
            description: self.description,
            enabled: self.enabled,
            version,
            brokers: self.brokers,
            routes: self.routes,
            schema_mappings: self.schema_mappings,
            rules: self.rules,
            command_templates: self.command_templates,
            sinks: self.sinks,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ProjectConfigResponse {
    pub schema_version: u16,
    pub version: u64,
    pub config: ProjectConfigDocumentV1,
}

impl ProjectConfigResponse {
    pub fn from_domain(config: ProjectConfig) -> Self {
        let version = config.version;
        Self {
            schema_version: PROJECT_CONFIG_DOCUMENT_SCHEMA_VERSION,
            version,
            config: ProjectConfigDocumentV1::from_domain(config),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct UpdateProjectConfigRequest {
    pub expected_version: u64,
    pub config: ProjectConfigDocumentV1,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ProjectConfigWriteResponse {
    pub project_id: ProjectId,
    pub version: u64,
    pub revision_id: String,
    pub config_hash: String,
    pub reload_required: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ResolveFailureRequest {
    pub resolution: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ResolveFailureResponse {
    pub failure_id: String,
    pub resolved: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct RuntimeReloadRequest {
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ExecuteCommandRequest {
    #[serde(default)]
    pub params: BTreeMap<String, serde_json::Value>,
    pub reason: Option<String>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum CommandExecutionStatusResponse {
    Queued,
    Published,
    Failed,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ExecuteCommandResponse {
    pub project_id: ProjectId,
    pub command_template_id: CommandTemplateId,
    pub command_execution_id: CommandExecutionId,
    pub status: CommandExecutionStatusResponse,
    pub broker_id: BrokerId,
    pub topic: String,
    pub qos: MqttQos,
    pub retain: bool,
    pub payload_size_bytes: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub queued_at: OffsetDateTime,
    pub audit_event_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ListResponse<T> {
    pub items: Vec<T>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub next_before: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLifecycleState {
    Running,
    Reloading,
    Stopping,
    Stopped,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ReadinessStatus {
    Ready,
    NotReady,
}

impl ReadinessStatus {
    pub const fn http_status(self) -> salvo::http::StatusCode {
        match self {
            Self::Ready => salvo::http::StatusCode::OK,
            Self::NotReady => salvo::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ReadinessCheckResponse {
    pub status: ReadinessStatus,
    pub message: Option<String>,
}

impl ReadinessCheckResponse {
    pub fn ready() -> Self {
        Self {
            status: ReadinessStatus::Ready,
            message: None,
        }
    }

    pub fn not_ready(message: impl Into<String>) -> Self {
        Self {
            status: ReadinessStatus::NotReady,
            message: Some(message.into()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct RuntimeReadinessResponse {
    pub status: ReadinessStatus,
    pub project_id: String,
    pub lifecycle: RuntimeLifecycleState,
    pub active_version: Option<u64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ReadinessResponse {
    pub status: ReadinessStatus,
    pub service: String,
    pub storage: ReadinessCheckResponse,
    pub runtime: RuntimeReadinessResponse,
}

impl ReadinessResponse {
    pub fn from_checks(
        service: impl Into<String>,
        storage: ReadinessCheckResponse,
        runtime: RuntimeReadinessResponse,
    ) -> Self {
        let status = if storage.status == ReadinessStatus::Ready
            && runtime.status == ReadinessStatus::Ready
        {
            ReadinessStatus::Ready
        } else {
            ReadinessStatus::NotReady
        };

        Self {
            status,
            service: service.into(),
            storage,
            runtime,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct RuntimeStatusResponse {
    pub project_id: ProjectId,
    pub state: RuntimeLifecycleState,
    pub active_version: Option<u64>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_reload_at: Option<OffsetDateTime>,
    pub last_reload_error: Option<String>,
    pub counters: RuntimeCountersResponse,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct RealtimeFilterSnapshot {
    pub device_id: Option<String>,
    pub topic: Option<String>,
    pub topic_prefix: Option<String>,
    pub event_type: Option<String>,
    pub route_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct RealtimeEventResponse {
    pub id: String,
    pub correlation_id: String,
    pub project_id: String,
    pub broker_id: String,
    pub route_id: String,
    pub schema_mapping_id: Option<String>,
    pub topic: String,
    pub device_id: Option<String>,
    pub event_type: String,
    pub received_at: String,
    pub payload_size_bytes: usize,
    pub payload: RealtimePayloadResponse,
    pub fields: BTreeMap<String, serde_json::Value>,
    pub raw: Option<RealtimeRawPayloadResponse>,
    pub normalization_errors: Vec<RealtimeNormalizationDiagnosticResponse>,
    pub metadata: BTreeMap<String, String>,
}

impl RealtimeEventResponse {
    pub fn from_event(event: NormalizedEvent) -> Self {
        Self {
            id: event.id.to_string(),
            correlation_id: event.correlation_id,
            project_id: event.project_id.to_string(),
            broker_id: event.broker_id.to_string(),
            route_id: event.route_id.to_string(),
            schema_mapping_id: event.schema_mapping_id.map(|id| id.to_string()),
            topic: event.topic.as_str().to_owned(),
            device_id: event.device_id,
            event_type: event.event_type,
            received_at: event.received_at.to_string(),
            payload_size_bytes: event.payload_size_bytes,
            payload: RealtimePayloadResponse::from(event.payload),
            fields: event
                .fields
                .into_iter()
                .map(|(name, value)| (name, field_value_to_json(value)))
                .collect(),
            raw: event.raw.map(|raw| RealtimeRawPayloadResponse {
                byte_len: raw.byte_len,
                content_type: raw.content_type,
            }),
            normalization_errors: event
                .normalization_errors
                .into_iter()
                .map(|diagnostic| RealtimeNormalizationDiagnosticResponse {
                    code: diagnostic.code,
                    message: diagnostic.message,
                    field: diagnostic.field,
                })
                .collect(),
            metadata: event.metadata,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum RealtimePayloadResponse {
    Json(serde_json::Value),
    RawBase64(String),
}

impl From<DecodedPayload> for RealtimePayloadResponse {
    fn from(value: DecodedPayload) -> Self {
        match value {
            DecodedPayload::Json(value) => Self::Json(value),
            DecodedPayload::Raw(bytes) => Self::RawBase64(BASE64_STANDARD.encode(bytes)),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct RealtimeRawPayloadResponse {
    pub byte_len: usize,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct RealtimeNormalizationDiagnosticResponse {
    pub code: String,
    pub message: String,
    pub field: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RealtimeServerMessage {
    Ready {
        transport: String,
        filter: RealtimeFilterSnapshot,
    },
    Event {
        data: Box<RealtimeEventResponse>,
    },
    Lagged {
        skipped: u64,
    },
    FilterUpdated {
        filter: RealtimeFilterSnapshot,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RealtimeClientMessage {
    Subscribe {
        device_id: Option<String>,
        topic: Option<String>,
        topic_prefix: Option<String>,
        event_type: Option<String>,
        route_id: Option<String>,
    },
    Ping,
}

fn field_value_to_json(value: FieldValue) -> serde_json::Value {
    match value {
        FieldValue::Null => serde_json::Value::Null,
        FieldValue::Bool(value) => serde_json::Value::Bool(value),
        FieldValue::Number(value) => serde_json::Value::Number(value),
        FieldValue::String(value) => serde_json::Value::String(value),
        FieldValue::Object(value) => serde_json::Value::Object(value),
        FieldValue::Array(value) => serde_json::Value::Array(value),
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct RuntimeReloadResponse {
    pub project_id: ProjectId,
    pub previous_version: u64,
    pub active_version: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub reloaded_at: OffsetDateTime,
    pub old_runtime_shutdown_error: Option<String>,
    pub audit_event_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Default)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct RuntimeCountersResponse {
    pub pipeline: RuntimePipelineCountersResponse,
    pub forwarder: ForwarderCountersResponse,
    pub persistence_writer: Option<PersistenceWriterCountersResponse>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Default)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct RuntimePipelineCountersResponse {
    pub normalized_total: u64,
    pub matched_rule_total: u64,
    pub action_intent_total: u64,
    pub dispatch_failed_total: u64,
    pub realtime_event_published_total: u64,
    pub realtime_event_no_receiver_total: u64,
    pub forward_outcome_total: u64,
    pub delivery_outcome_persist_failed_total: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Default)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ForwarderCountersResponse {
    pub accepted_total: u64,
    pub backpressure_total: u64,
    pub delivered_total: u64,
    pub rejected_total: u64,
    pub timed_out_total: u64,
    pub failed_total: u64,
    pub response_too_large_total: u64,
    pub outcome_dropped_total: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Default)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct PersistenceWriterCountersResponse {
    pub enqueued_total: u64,
    pub queue_full_total: u64,
    pub queue_closed_total: u64,
    pub write_succeeded_total: u64,
    pub write_failed_total: u64,
    pub write_timeout_total: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ErrorResponse {
    pub error: ErrorPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "salvo-oapi", derive(ToSchema))]
pub struct ErrorPayload {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}
