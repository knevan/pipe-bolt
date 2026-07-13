use pipe_bolt_domain::{
    BrokerId, CommandExecutionId, CommandTemplateId, EventId, MqttQos, ProjectId, SinkId, UserId,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

const DEFAULT_OPERATIONAL_LIST_LIMIT: u32 = 100;
const MAX_OPERATIONAL_LIST_LIMIT: u32 = 500;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuditContext {
    pub actor_id: Option<UserId>,
    pub reason: Option<String>,
}

impl AuditContext {
    pub fn system(reason: impl Into<String>) -> Self {
        Self {
            actor_id: None,
            reason: Some(reason.into()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProjectConfigWriteResult {
    pub project_id: ProjectId,
    pub version: u64,
    pub revision_id: String,
    pub config_hash: String,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditStatus {
    Succeeded,
    Failed,
}

impl AuditStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewAuditEvent {
    pub project_id: Option<ProjectId>,
    pub actor_id: Option<UserId>,
    pub action: String,
    pub target_type: String,
    pub target_id: String,
    pub status: AuditStatus,
    pub reason: Option<String>,
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AuditEventRecord {
    pub audit_event_id: String,
    pub project_id: Option<ProjectId>,
    pub actor_id: Option<UserId>,
    pub action: String,
    pub target_type: String,
    pub target_id: String,
    pub status: AuditStatus,
    pub reason: Option<String>,
    pub metadata: serde_json::Map<String, serde_json::Value>,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkDeliveryStatus {
    Delivered {
        http_status: u16,
        response_body_bytes: usize,
    },
    HttpRejected {
        http_status: u16,
        response_body_bytes: usize,
    },
    TimedOut,
    ResponseTooLarge {
        max: usize,
    },
    Failed {
        reason: String,
    },
}

impl SinkDeliveryStatus {
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Delivered { .. } => "delivered",
            Self::HttpRejected { .. } => "http_rejected",
            Self::TimedOut => "timed_out",
            Self::ResponseTooLarge { .. } => "response_too_large",
            Self::Failed { .. } => "failed",
        }
    }

    pub const fn is_failure(&self) -> bool {
        !matches!(self, Self::Delivered { .. })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewSinkDeliveryOutcome {
    pub project_id: ProjectId,
    pub event_id: EventId,
    pub sink_id: SinkId,
    pub status: SinkDeliveryStatus,
    pub correlation_id: Option<String>,
    pub duration_ms: Option<u64>,
    pub attempt: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SinkDeliveryOutcomeRecord {
    pub delivery_id: String,
    pub project_id: ProjectId,
    pub event_id: EventId,
    pub sink_id: SinkId,
    pub status: String,
    pub http_status: Option<u16>,
    pub response_body_bytes: Option<u64>,
    pub failure_reason: Option<String>,
    pub correlation_id: Option<String>,
    pub duration_ms: Option<u64>,
    pub attempt: u16,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandExecutionStatus {
    Queued,
    Published,
    Failed,
}

impl CommandExecutionStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Published => "published",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewCommandExecution {
    pub project_id: ProjectId,
    pub command_template_id: CommandTemplateId,
    pub broker_id: BrokerId,
    pub actor_id: Option<UserId>,
    pub status: CommandExecutionStatus,
    pub topic: String,
    pub qos: MqttQos,
    pub retain: bool,
    pub payload_size_bytes: u64,
    pub failure_reason: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CommandExecutionRecord {
    pub command_execution_id: CommandExecutionId,
    pub project_id: ProjectId,
    pub command_template_id: CommandTemplateId,
    pub broker_id: BrokerId,
    pub actor_id: Option<UserId>,
    pub status: CommandExecutionStatus,
    pub topic: String,
    pub qos: MqttQos,
    pub retain: bool,
    pub payload_size_bytes: u64,
    pub failure_reason: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
    pub audit_event_id: String,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureSeverity {
    Warning,
    Error,
    Critical,
}

impl FailureSeverity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Critical => "critical",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewFailureEvent {
    pub project_id: ProjectId,
    pub event_id: Option<EventId>,
    pub sink_id: Option<SinkId>,
    pub component: String,
    pub failure_kind: String,
    pub severity: FailureSeverity,
    pub message: String,
    pub details: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FailureEventRecord {
    pub failure_id: String,
    pub project_id: ProjectId,
    pub event_id: Option<EventId>,
    pub sink_id: Option<SinkId>,
    pub component: String,
    pub failure_kind: String,
    pub severity: FailureSeverity,
    pub message: String,
    pub details: serde_json::Map<String, serde_json::Value>,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub resolved_at: Option<OffsetDateTime>,
    pub resolution: Option<String>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct OperationalListQuery {
    pub limit: u32,
    pub before: Option<OffsetDateTime>,
}

impl Default for OperationalListQuery {
    fn default() -> Self {
        Self {
            limit: DEFAULT_OPERATIONAL_LIST_LIMIT,
            before: None,
        }
    }
}

impl OperationalListQuery {
    pub fn sanitized(self) -> Self {
        Self {
            limit: self.limit.clamp(1, MAX_OPERATIONAL_LIST_LIMIT),
            before: self.before,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FailureListQuery {
    pub limit: u32,
    pub before: Option<OffsetDateTime>,
    pub unresolved_only: bool,
}

impl Default for FailureListQuery {
    fn default() -> Self {
        Self {
            limit: DEFAULT_OPERATIONAL_LIST_LIMIT,
            before: None,
            unresolved_only: false,
        }
    }
}

impl FailureListQuery {
    pub fn sanitized(self) -> Self {
        Self {
            limit: self.limit.clamp(1, MAX_OPERATIONAL_LIST_LIMIT),
            before: self.before,
            unresolved_only: self.unresolved_only,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RetentionConfig {
    pub audit_retention_days: u16,
    pub delivery_outcome_retention_days: u16,
    pub failure_retention_days: u16,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            audit_retention_days: 365,
            delivery_outcome_retention_days: 90,
            failure_retention_days: 365,
        }
    }
}
