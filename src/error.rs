use std::error::Error;
use std::fmt::{Display, Formatter, Result};

/// Error type used by the MQTT core engine public API
#[derive(Debug)]
pub enum MqttEngineError {
    InvalidConfig(&'static str),
    InvalidTopicFilter(String),
    InvalidTopicName(String),
    Client(String),
    RouterHandler(String),
    WorkerJoin(String),
    Decode(String),
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
            Self::InvalidTopicName(message) => write!(f, "invalid MQTT topic name: {}", message),
            Self::Client(message) => write!(f, "client error: {}", message),
            Self::RouterHandler(message) => write!(f, "router handler error: {}", message),
            Self::WorkerJoin(message) => write!(f, "MQTT worker thread join error: {}", message),
            Self::Decode(message) => write!(f, "payload decode error: {}", message),
            Self::IngressClosed => write!(f, "ingress queue is closed"),
            Self::IngressQueueFull => write!(f, "ingress queue is full"),
            Self::TelemetryClosed => write!(f, "telemetry broadcast channel is closed"),
            Self::CommandQueueFull => write!(f, "command queue is full"),
            Self::CommandQueueClosed => write!(f, "command queue is closed"),
        }
    }
}

impl Error for MqttEngineError {}
