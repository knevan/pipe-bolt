use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::config::{MqttQos, TopicName};
use crate::id::{BrokerId, CommandExecutionId, CommandTemplateId, EventId, ProjectId, UserId};

/// Source that requested command execution.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub enum CommandSource {
    Api,
    Rule,
}

/// Runtime command request accepted from API or rule evaluation before rendering.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandExecutionRequest {
    pub project_id: ProjectId,
    pub command_execution_id: CommandExecutionId,
    pub command_template_id: CommandTemplateId,
    #[serde(default)]
    pub params: BTreeMap<String, serde_json::Value>,
    pub source: CommandSource,
    pub actor_id: Option<UserId>,
    pub source_event_id: Option<EventId>,
    pub correlation_id: String,
    pub reason: Option<String>,
}

impl fmt::Debug for CommandExecutionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandExecutionRequest")
            .field("project_id", &self.project_id)
            .field("command_execution_id", &self.command_execution_id)
            .field("command_template_id", &self.command_template_id)
            .field(
                "params",
                &RedactedMap {
                    len: self.params.len(),
                },
            )
            .field("source", &self.source)
            .field("actor_id", &self.actor_id)
            .field("source_event_id", &self.source_event_id)
            .field("correlation_id", &self.correlation_id)
            .field("reason", &self.reason)
            .finish()
    }
}

/// Transport-ready command produced by command template rendering.
#[derive(Clone, PartialEq, Eq)]
pub struct RenderedCommand {
    project_id: ProjectId,
    broker_id: BrokerId,
    command_template_id: CommandTemplateId,
    command_execution_id: CommandExecutionId,
    topic: TopicName,
    payload: Vec<u8>,
    qos: MqttQos,
    retain: bool,
    payload_size_bytes: u64,
    correlation_id: String,
}

/// Input parts used to construct a rendered command.
#[derive(Clone, PartialEq, Eq)]
pub struct RenderedCommandParts {
    pub project_id: ProjectId,
    pub broker_id: BrokerId,
    pub command_template_id: CommandTemplateId,
    pub command_execution_id: CommandExecutionId,
    pub topic: TopicName,
    pub payload: Vec<u8>,
    pub qos: MqttQos,
    pub retain: bool,
    pub correlation_id: String,
}

impl RenderedCommand {
    /// Creates a rendered command and derives payload size from the owned payload without cloning.
    pub fn from_parts(parts: RenderedCommandParts) -> Self {
        let payload_size_bytes = saturating_usize_to_u64(parts.payload.len());

        Self {
            project_id: parts.project_id,
            broker_id: parts.broker_id,
            command_template_id: parts.command_template_id,
            command_execution_id: parts.command_execution_id,
            topic: parts.topic,
            payload: parts.payload,
            qos: parts.qos,
            retain: parts.retain,
            payload_size_bytes,
            correlation_id: parts.correlation_id,
        }
    }

    pub const fn project_id(&self) -> &ProjectId {
        &self.project_id
    }

    pub const fn broker_id(&self) -> &BrokerId {
        &self.broker_id
    }

    pub const fn command_template_id(&self) -> &CommandTemplateId {
        &self.command_template_id
    }

    pub const fn command_execution_id(&self) -> &CommandExecutionId {
        &self.command_execution_id
    }

    pub const fn topic(&self) -> &TopicName {
        &self.topic
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub const fn qos(&self) -> MqttQos {
        self.qos
    }

    pub const fn retain(&self) -> bool {
        self.retain
    }

    pub const fn payload_size_bytes(&self) -> u64 {
        self.payload_size_bytes
    }

    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

impl fmt::Debug for RenderedCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RenderedCommand")
            .field("project_id", &self.project_id)
            .field("broker_id", &self.broker_id)
            .field("command_template_id", &self.command_template_id)
            .field("command_execution_id", &self.command_execution_id)
            .field("topic", &self.topic)
            .field(
                "payload",
                &RedactedBytes {
                    len: self.payload.len(),
                },
            )
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .field("payload_size_bytes", &self.payload_size_bytes)
            .field("correlation_id", &self.correlation_id)
            .finish()
    }
}

#[derive(Copy, Clone)]
struct RedactedBytes {
    len: usize,
}

impl fmt::Debug for RedactedBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedactedBytes")
            .field("len", &self.len)
            .finish()
    }
}

#[derive(Copy, Clone)]
struct RedactedMap {
    len: usize,
}

impl fmt::Debug for RedactedMap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedactedMap")
            .field("len", &self.len)
            .finish()
    }
}

const fn saturating_usize_to_u64(value: usize) -> u64 {
    if value > u64::MAX as usize {
        u64::MAX
    } else {
        value as u64
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn source_should_serialize_snake_case_when_api_source() {
        let value = serde_json::to_value(CommandSource::Api).expect("serialize command source");

        assert_eq!(value, json!("api"));
    }

    #[test]
    fn request_debug_should_redact_params_when_params_are_present() {
        let request = CommandExecutionRequest {
            project_id: ProjectId::new("project-1").expect("project id"),
            command_execution_id: CommandExecutionId::new("command-exec-1")
                .expect("command execution id"),
            command_template_id: CommandTemplateId::new("relay-on").expect("command template id"),
            params: BTreeMap::from([("token".to_owned(), json!("secret-value"))]),
            source: CommandSource::Api,
            actor_id: Some(UserId::new("user-1").expect("user id")),
            source_event_id: None,
            correlation_id: "corr-1".to_owned(),
            reason: Some("operator request".to_owned()),
        };

        let debug = format!("{request:?}");

        assert!(!debug.contains("secret-value"));
    }

    #[test]
    fn rendered_command_should_compute_payload_size_when_created() {
        let command = rendered_command(vec![1, 2, 3, 4]);

        assert_eq!(command.payload_size_bytes(), 4);
    }

    #[test]
    fn rendered_command_debug_should_redact_payload_when_payload_is_present() {
        let command = rendered_command(b"secret-payload".to_vec());

        let debug = format!("{command:?}");

        assert!(!debug.contains("secret-payload"));
    }

    fn rendered_command(payload: Vec<u8>) -> RenderedCommand {
        RenderedCommand::from_parts(RenderedCommandParts {
            project_id: ProjectId::new("project-1").expect("project id"),
            broker_id: BrokerId::new("broker-1").expect("broker id"),
            command_template_id: CommandTemplateId::new("relay-on").expect("command template id"),
            command_execution_id: CommandExecutionId::new("command-exec-1")
                .expect("command execution id"),
            topic: TopicName::new("devices/device-1/cmd").expect("topic name"),
            payload,
            qos: MqttQos::AtLeastOnce,
            retain: false,
            correlation_id: "corr-1".to_owned(),
        })
    }
}
