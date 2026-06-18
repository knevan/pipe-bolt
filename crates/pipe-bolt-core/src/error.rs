use std::error::Error;
use std::fmt::{Display, Formatter, Result};

use pipe_bolt_domain::{DomainError, SinkId};
use thiserror::Error;

/// Error type used by the MQTT core engine public API.
#[derive(Debug)]
pub enum MqttEngineError {
    InvalidConfig(&'static str),
    InvalidTopicFilter(String),
    InvalidTopicName(String),
    Client(String),
    RouterHandler(String),
    WorkerJoin(String),
    Decode(String),
    Pipeline(PipelineError),
    Domain(String),
    Rule(RuleError),
    Dispatch(DispatchError),
    IngressClosed,
    IngressQueueFull,
    TelemetryClosed,
    CommandQueueFull,
    CommandQueueClosed,
}

impl Display for MqttEngineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid MQTT config: {}", message),
            Self::InvalidTopicFilter(message) => {
                write!(f, "invalid MQTT topic filter: {}", message)
            }
            Self::InvalidTopicName(message) => write!(f, "invalid MQTT topic name: {message}"),
            Self::Client(message) => write!(f, "client error: {message}"),
            Self::RouterHandler(message) => write!(f, "router handler error: {message}"),
            Self::WorkerJoin(message) => write!(f, "MQTT worker thread join error: {message}"),
            Self::Decode(message) => write!(f, "payload decode error: {message}"),
            Self::Pipeline(error) => write!(f, "pipeline error: {error}"),
            Self::Domain(message) => write!(f, "domain error: {message}"),
            Self::Rule(error) => write!(f, "rule error: {error}"),
            Self::Dispatch(error) => write!(f, "dispatch error: {error}"),
            Self::IngressClosed => write!(f, "ingress queue is closed"),
            Self::IngressQueueFull => write!(f, "ingress queue is full"),
            Self::TelemetryClosed => write!(f, "telemetry broadcast channel is closed"),
            Self::CommandQueueFull => write!(f, "command queue is full"),
            Self::CommandQueueClosed => write!(f, "command queue is closed"),
        }
    }
}

impl Error for MqttEngineError {}

impl From<PipelineError> for MqttEngineError {
    fn from(error: PipelineError) -> Self {
        Self::Pipeline(error)
    }
}

impl From<DispatchError> for MqttEngineError {
    fn from(error: DispatchError) -> Self {
        Self::Dispatch(error)
    }
}

impl From<RuleError> for MqttEngineError {
    fn from(error: RuleError) -> Self {
        Self::Rule(error)
    }
}

/// Impl DomainError from pipe-bolt-domain module
impl From<DomainError> for MqttEngineError {
    fn from(error: DomainError) -> Self {
        Self::Domain(error.to_string())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PipelineError {
    #[error("payload is too large: {actual} bytes exceeds {max} bytes")]
    PayloadTooLarge { actual: usize, max: usize },

    #[error("raw payload retention is too large: {actual} bytes exceeds {max} bytes")]
    RawPayloadTooLarge { actual: usize, max: usize },

    #[error("invalid JSON payload: {message}")]
    InvalidJson { message: String },

    #[error("JSON payload is too deep: depth {actual} exceeds {max}")]
    JsonTooDeep { actual: usize, max: usize },

    #[error("schema mapping requires JSON object payload")]
    MappingRequiresJsonObject,

    #[error("schema mapping requires JSON payload")]
    MappingRequiresJson,

    #[error("required field '{target}' is missing at path '{source_path}'")]
    MissingRequiredField { target: String, source_path: String },

    #[error("field '{target}' type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        target: String,
        expected: &'static str,
        actual: &'static str,
    },

    #[error("too many extracted fields: {actual} exceeds {max}")]
    TooManyExtractedFields { actual: usize, max: usize },

    #[error("device_id payload field must resolve to string, number, or boolean")]
    InvalidDeviceIdFieldType,

    #[error("payload field device_id extraction requires JSON payload")]
    DeviceIdRequiresJson,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DispatchError {
    #[error("invalid dispatch config: {reason}")]
    InvalidConfig { reason: &'static str },

    #[error("too many action intents for one event: {actual} exceeds {max}")]
    TooManyIntents { actual: usize, max: usize },

    #[error("too many metadata entries for one event: {actual} exceeds {max}")]
    TooManyMetadataEntries { actual: usize, max: usize },

    #[error("realtime stream is unavailable")]
    RealtimeUnavailable,

    #[error("realtime stream is full")]
    RealtimeBackpressure,

    #[error("metadata key is invalid: {reason}")]
    InvalidMetadataKey { reason: &'static str },

    #[error("metadata value is too large: {actual} exceeds {max} bytes")]
    MetadataValueTooLarge { actual: usize, max: usize },

    #[error("sink '{sink_id}' was not found")]
    SinkNotFound { sink_id: SinkId },

    #[error("sink '{sink_id}' is disabled")]
    SinkDisabled { sink_id: SinkId },

    #[error("sink '{sink_id}' uses an unsupported sink kind")]
    UnsupportedSinkKind { sink_id: SinkId },

    #[error("forwarder queue is unavailable")]
    ForwarderUnavailable,

    #[error("forwarder queue is full")]
    ForwarderBackpressure,

    #[error("forward payload serialization failed: {reason}")]
    ForwardPayloadEncode { reason: &'static str },

    #[error("forward payload is too large: {actual} exceeds {max} bytes")]
    ForwardPayloadTooLarge { actual: usize, max: usize },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum RuleError {
    #[error("rule '{rule_id}' is invalid: {reason}")]
    InvalidRule { rule_id: String, reason: String },

    #[error("rule '{rule_id}' uses unsupported trigger '{trigger}'")]
    UnsupportedTrigger {
        rule_id: String,
        trigger: &'static str,
    },

    #[error("rule '{rule_id}' uses unsupported action '{action}'")]
    UnsupportedAction {
        rule_id: String,
        action: &'static str,
    },

    #[error("rule '{rule_id}' condition is too deep: depth {actual} exceeds {max}")]
    ConditionTooDeep {
        rule_id: String,
        actual: usize,
        max: usize,
    },

    #[error("rule '{rule_id}' condition has too many nodes: {actual} exceeds {max}")]
    ConditionTooLarge {
        rule_id: String,
        actual: usize,
        max: usize,
    },

    #[error("rule '{rule_id}' condition group '{operator}' must not be empty")]
    EmptyConditionGroup {
        rule_id: String,
        operator: &'static str,
    },

    #[error("rule '{rule_id}' comparison requires numeric values")]
    NonNumericComparison { rule_id: String },

    #[error("rule '{rule_id}' value source is not available")]
    MissingValue { rule_id: String },

    #[error("rule '{rule_id}' has too many actions: {actual} exceeds {max}")]
    TooManyActions {
        rule_id: String,
        actual: usize,
        max: usize,
    },

    #[error("rule '{rule_id}' metadata key is invalid: {reason}")]
    InvalidMetadataKey {
        rule_id: String,
        reason: &'static str,
    },

    #[error("rule '{rule_id}' metadata value is too large: {actual} exceeds {max} bytes")]
    MetadataValueTooLarge {
        rule_id: String,
        actual: usize,
        max: usize,
    },
}
