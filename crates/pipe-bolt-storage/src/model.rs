use pipe_bolt_domain::{EventId, ProjectId, SinkId, UserId};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
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
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
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
