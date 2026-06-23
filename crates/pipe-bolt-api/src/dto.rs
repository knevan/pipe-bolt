use pipe_bolt_domain::{ProjectConfig, ProjectId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProjectConfigResponse {
    pub config: ProjectConfig,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UpdateProjectConfigRequest {
    pub expected_version: u64,
    pub config: ProjectConfig,
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

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RuntimeReloadRequest {
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ListResponse<T> {
    pub items: Vec<T>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub next_before: Option<OffsetDateTime>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLifecycleState {
    Running,
    Reloading,
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct RuntimeReloadResponse {
    pub project_id: ProjectId,
    pub previous_version: u64,
    pub active_version: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub reloaded_at: OffsetDateTime,
    pub old_runtime_shutdown_error: Option<String>,
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
