use thiserror::Error;

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DomainError {
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },

    #[error("{field} is too long: max {max} bytes")]
    FieldTooLong { field: &'static str, max: usize },

    #[error("{field} contains invalid control characters")]
    InvalidControlCharacters { field: &'static str },

    #[error("MQTT topic filter is invalid: {reason}")]
    InvalidTopicFilter { reason: &'static str },

    #[error("MQTT topic name is invalid: {reason}")]
    InvalidTopicName { reason: &'static str },

    #[error("field path is invalid: {reason}")]
    InvalidFieldPath { reason: &'static str },

    #[error("broker port must not be zero")]
    InvalidBrokerPort,

    #[error("keep alive must be at least 5 seconds")]
    InvalidKeepAlive,
}
