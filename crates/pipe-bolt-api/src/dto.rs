use pipe_bolt_domain::{
    BrokerConnectionConfig, CommandTemplate, PayloadSchemaMapping, ProjectConfig, ProjectId,
    RuleDefinition, SinkDefinition, TenantId, TopicRouteConfig,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const PROJECT_CONFIG_DOCUMENT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
pub struct UpdateProjectConfigRequest {
    pub expected_version: u64,
    pub config: ProjectConfigDocumentV1,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ProjectConfigWriteResponse {
    pub project_id: ProjectId,
    pub version: u64,
    pub revision_id: String,
    pub config_hash: String,
    pub reload_required: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ResolveFailureRequest {
    pub resolution: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ResolveFailureResponse {
    pub failure_id: String,
    pub resolved: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
pub struct RuntimeReloadRequest {
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ListResponse<T> {
    pub items: Vec<T>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub next_before: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLifecycleState {
    Running,
    Reloading,
    Stopping,
    Stopped,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
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
pub struct RuntimeCountersResponse {
    pub pipeline: RuntimePipelineCountersResponse,
    pub forwarder: ForwarderCountersResponse,
    pub persistence_writer: Option<PersistenceWriterCountersResponse>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Default)]
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
pub struct PersistenceWriterCountersResponse {
    pub enqueued_total: u64,
    pub queue_full_total: u64,
    pub queue_closed_total: u64,
    pub write_succeeded_total: u64,
    pub write_failed_total: u64,
    pub write_timeout_total: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorPayload {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}
