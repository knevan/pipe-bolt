use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use rumqttc::QoS;
use serde::{Deserialize, Serialize};

use crate::error::MqttEngineError;
use crate::message::envelope::validate_topic_name;
use crate::mqtt::engine::MqttHandle;

const DEFAULT_COMMAND_NAMESPACE: &str = "devices";
const DEFAULT_COMMAND_KIND: &str = "command";
const MAX_DEVICE_ID_LEN: usize = 128;
const MAX_COMMAND_NAME_LEN: usize = 128;
const MAX_COMMAND_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Deserialize)]
pub struct CommandRequest {
    pub device: String,
    pub command: String,
    #[serde(default)]
    pub payload: CommandPayload,
    #[serde(default = "default_qos")]
    pub qos: CommandQos,
    #[serde(default)]
    pub retain: bool,
}

impl CommandRequest {
    /// Validates user-provided command input and converts it into a safe MQTT publish request.
    ///
    /// This rejects wildcard characters and multi-level topic segments so clients cannot escape the
    /// configured command namespace.
    pub fn validate(self) -> Result<ValidatedCommand, CommandValidationError> {
        let device = normalize_segment("device", self.device, MAX_DEVICE_ID_LEN)?;
        let command = normalize_segment("command", self.command, MAX_COMMAND_NAME_LEN)?;
        let payload = self.payload.into_bytes()?;

        if payload.len() > MAX_COMMAND_PAYLOAD_BYTES {
            return Err(CommandValidationError::PayloadTooLarge {
                max: MAX_COMMAND_PAYLOAD_BYTES,
                actual: payload.len(),
            });
        }

        let topic = format!(
            "{}/{}/{}/{}",
            DEFAULT_COMMAND_NAMESPACE, device, DEFAULT_COMMAND_KIND, command
        );
        validate_topic_name(&topic)
            .map_err(|err| CommandValidationError::InvalidTopic(err.to_string()))?;

        Ok(ValidatedCommand {
            device,
            command,
            topic,
            qos: self.qos.into(),
            retain: self.retain,
            payload,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedCommand {
    pub device: String,
    pub command: String,
    pub topic: String,
    pub qos: QoS,
    pub retain: bool,
    pub payload: Vec<u8>,
}

impl ValidatedCommand {
    pub fn enqueue(self, mqtt: &MqttHandle) -> Result<CommandQueueReceipt, MqttEngineError> {
        // A queued command only means the local bounded queue accepted it,
        // broker/device acknowledgement is not guaranteed here.
        mqtt.try_enqueue_command(self.topic.clone(), self.qos, self.retain, self.payload)?;

        Ok(CommandQueueReceipt {
            status: CommandQueueStatus::Queued,
            device: self.device,
            command: self.command,
            topic: self.topic,
            queued_at_ms: unix_time_ms(SystemTime::now()),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandQueueReceipt {
    pub status: CommandQueueStatus,
    pub device: String,
    pub command: String,
    pub topic: String,
    pub queued_at_ms: u128,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandQueueStatus {
    Queued,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CommandPayload {
    Json(serde_json::Value),
    Text(String),
    Base64(String),
    #[default]
    Empty,
}

impl CommandPayload {
    fn into_bytes(self) -> Result<Vec<u8>, CommandValidationError> {
        match self {
            Self::Json(value) => serde_json::to_vec(&value)
                .map_err(|err| CommandValidationError::InvalidPayload(err.to_string())),
            Self::Text(value) => Ok(value.into_bytes()),
            Self::Base64(value) => BASE64_STANDARD.decode(value.trim()).map_err(|err| {
                CommandValidationError::InvalidPayload(format!("invalid base64 payload: {}", err))
            }),
            Self::Empty => Ok(Vec::new()),
        }
    }
}

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
pub enum CommandQos {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

impl From<CommandQos> for QoS {
    fn from(value: CommandQos) -> Self {
        match value {
            CommandQos::AtMostOnce => QoS::AtMostOnce,
            CommandQos::AtLeastOnce => QoS::AtLeastOnce,
            CommandQos::ExactlyOnce => QoS::ExactlyOnce,
        }
    }
}

fn default_qos() -> CommandQos {
    CommandQos::AtLeastOnce
}

#[derive(Debug, Clone)]
pub enum CommandValidationError {
    InvalidSegment {
        name: &'static str,
        reason: &'static str,
    },
    InvalidTopic(String),
    InvalidPayload(String),
    PayloadTooLarge {
        max: usize,
        actual: usize,
    },
}

impl CommandValidationError {
    pub fn message(&self) -> String {
        match self {
            Self::InvalidSegment { name, reason } => format!("invalid {}: {}", name, reason),
            Self::InvalidTopic(reason) => format!("invalid command topic: {}", reason),
            Self::InvalidPayload(reason) => format!("invalid command payload: {}", reason),
            Self::PayloadTooLarge { max, actual } => {
                format!(
                    "command payload is too large: max {} bytes, got {} bytes",
                    max, actual
                )
            }
        }
    }
}

fn normalize_segment(
    name: &'static str,
    value: String,
    max_len: usize,
) -> Result<String, CommandValidationError> {
    let value = value.trim();

    if value.is_empty() {
        return Err(CommandValidationError::InvalidSegment {
            name,
            reason: "must not be empty",
        });
    }

    if value.len() > max_len {
        return Err(CommandValidationError::InvalidSegment {
            name,
            reason: "is too long",
        });
    }

    if value.contains('/') || value.contains('+') || value.contains('#') || value.contains('\0') {
        return Err(CommandValidationError::InvalidSegment {
            name,
            reason: "must be a single MQTT topic segment without wildcards",
        });
    }

    Ok(value.to_owned())
}

fn unix_time_ms(value: SystemTime) -> u128 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
