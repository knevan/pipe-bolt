use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::SchemaMappingId;
use crate::config::TopicName;
use crate::id::{BrokerId, EventId, ProjectId, RouteId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedEvent {
    pub id: EventId,
    pub correlation_id: String,
    pub project_id: ProjectId,
    pub broker_id: BrokerId,
    pub route_id: RouteId,
    pub schema_mapping_id: Option<SchemaMappingId>,
    pub topic: TopicName,
    pub device_id: Option<String>,
    pub event_type: String,
    #[serde(with = "time::serde::rfc3339")]
    pub received_at: OffsetDateTime,
    pub payload_size_bytes: usize,
    pub payload: DecodedPayload,
    pub fields: BTreeMap<String, FieldValue>,
    pub raw: Option<RawPayloadRef>,
    pub normalization_errors: Vec<NormalizationDiagnostic>,
    pub metadata: BTreeMap<String, String>,
}

impl NormalizedEvent {
    pub fn field(&self, name: &str) -> Option<&FieldValue> {
        self.fields.get(name)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum DecodedPayload {
    Json(serde_json::Value),
    Raw(#[serde(with = "serde_bytes")] Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Object(serde_json::Map<String, serde_json::Value>),
    Array(Vec<serde_json::Value>),
}

impl FieldValue {
    pub fn from_json(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(value) => Self::Bool(value),
            serde_json::Value::Number(value) => Self::Number(value),
            serde_json::Value::String(value) => Self::String(value),
            serde_json::Value::Array(value) => Self::Array(value),
            serde_json::Value::Object(value) => Self::Object(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPayloadRef {
    pub byte_len: usize,
    pub content_type: Option<String>,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NormalizationDiagnostic {
    pub code: String,
    pub message: String,
    pub field: Option<String>,
}
