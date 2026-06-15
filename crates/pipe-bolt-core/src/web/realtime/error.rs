use salvo::http::StatusError;
use thiserror::Error;

use crate::command::CommandValidationError;
use crate::error::MqttEngineError;

#[derive(Debug)]
pub enum CommandEndpointError {
    Validation(CommandValidationError),
    Queue(MqttEngineError),
}

impl CommandEndpointError {
    pub fn message(&self) -> String {
        match self {
            Self::Validation(err) => err.message(),
            Self::Queue(err) => err.to_string(),
        }
    }

    pub fn into_status_error(self) -> StatusError {
        match self {
            Self::Validation(err) => StatusError::bad_request().brief(err.message()),
            Self::Queue(MqttEngineError::CommandQueueFull) => {
                StatusError::service_unavailable().brief("command queue is full")
            }
            Self::Queue(MqttEngineError::CommandQueueClosed) => {
                StatusError::service_unavailable().brief("command queue is closed")
            }
            Self::Queue(err) => StatusError::internal_server_error().brief(err.to_string()),
        }
    }
}

impl From<CommandValidationError> for CommandEndpointError {
    fn from(err: CommandValidationError) -> Self {
        Self::Validation(err)
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
