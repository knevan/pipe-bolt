use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use pipe_bolt_domain::{
    CommandExecutionId, CommandTemplate, CommandTemplateId, ProjectId, RenderedCommand,
    RenderedCommandParts, TopicName,
};
use rumqttc::QoS;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::error::MqttEngineError;
use crate::message::envelope::validate_topic_name;
use crate::mqtt::engine::MqttHandle;

const DEFAULT_COMMAND_NAMESPACE: &str = "devices";
const DEFAULT_COMMAND_KIND: &str = "command";
const MAX_DEVICE_ID_LEN: usize = 128;
const MAX_COMMAND_NAME_LEN: usize = 128;
pub const DEFAULT_COMMAND_MAX_PAYLOAD_BYTES: usize = 64 * 1024;

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

        if payload.len() > DEFAULT_COMMAND_MAX_PAYLOAD_BYTES {
            return Err(CommandValidationError::PayloadTooLarge {
                max: DEFAULT_COMMAND_MAX_PAYLOAD_BYTES,
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandRenderContext {
    pub project_id: ProjectId,
    pub command_execution_id: CommandExecutionId,
    pub correlation_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandRenderDraft {
    pub topic: TopicName,
    pub payload: Vec<u8>,
    pub payload_size_bytes: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CommandTemplateRenderer {
    max_payload_bytes: usize,
}

impl Default for CommandTemplateRenderer {
    fn default() -> Self {
        Self {
            max_payload_bytes: DEFAULT_COMMAND_MAX_PAYLOAD_BYTES,
        }
    }
}

impl CommandTemplateRenderer {
    pub const fn new(max_payload_bytes: usize) -> Self {
        Self { max_payload_bytes }
    }

    pub fn render(
        &self,
        context: CommandRenderContext,
        template: &CommandTemplate,
        params: &BTreeMap<String, serde_json::Value>,
    ) -> Result<RenderedCommand, CommandRenderError> {
        let draft = self.render_draft(template, params)?;

        Ok(RenderedCommand::from_parts(RenderedCommandParts {
            project_id: context.project_id,
            broker_id: template.broker_id.clone(),
            command_template_id: template.id.clone(),
            command_execution_id: context.command_execution_id,
            topic: draft.topic,
            payload: draft.payload,
            qos: template.qos,
            retain: template.retain,
            correlation_id: context.correlation_id,
        }))
    }

    pub fn render_draft(
        &self,
        template: &CommandTemplate,
        params: &BTreeMap<String, serde_json::Value>,
    ) -> Result<CommandRenderDraft, CommandRenderError> {
        let topic_text = render_topic_template(template, params)?;
        let topic =
            TopicName::new(topic_text).map_err(|error| CommandRenderError::InvalidTopic {
                template_id: template.id.clone(),
                reason: error.to_string(),
            })?;

        let payload_value = render_payload_value(template, &template.payload_template, params)?;
        let payload = serde_json::to_vec(&payload_value).map_err(|error| {
            CommandRenderError::InvalidPayload {
                template_id: template.id.clone(),
                reason: error.to_string(),
            }
        })?;

        if payload.len() > self.max_payload_bytes {
            return Err(CommandRenderError::PayloadTooLarge {
                template_id: template.id.clone(),
                max: self.max_payload_bytes,
                actual: payload.len(),
            });
        }

        Ok(CommandRenderDraft {
            topic,
            payload_size_bytes: saturating_usize_to_u64(payload.len()),
            payload,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum CommandRenderError {
    #[error("command template '{template_id}' has empty {surface} placeholder")]
    EmptyPlaceholder {
        template_id: CommandTemplateId,
        surface: TemplateSurface,
    },

    #[error("command template '{template_id}' has unclosed {surface} placeholder")]
    UnclosedPlaceholder {
        template_id: CommandTemplateId,
        surface: TemplateSurface,
    },

    #[error("command template '{template_id}' has unopened {surface} placeholder")]
    UnopenedPlaceholder {
        template_id: CommandTemplateId,
        surface: TemplateSurface,
    },

    #[error("command template '{template_id}' is missing parameter '{name}'")]
    MissingParameter {
        template_id: CommandTemplateId,
        name: String,
    },

    #[error("command template '{template_id}' parameter '{name}' must be scalar")]
    NonScalarParameter {
        template_id: CommandTemplateId,
        name: String,
    },

    #[error(
        "command template '{template_id}' parameter '{name}' is invalid topic segment: {reason}"
    )]
    InvalidTopicSegment {
        template_id: CommandTemplateId,
        name: String,
        reason: &'static str,
    },

    #[error("command template '{template_id}' rendered invalid MQTT topic: {reason}")]
    InvalidTopic {
        template_id: CommandTemplateId,
        reason: String,
    },

    #[error("command template '{template_id}' payload render failed: {reason}")]
    InvalidPayload {
        template_id: CommandTemplateId,
        reason: String,
    },

    #[error(
        "command template '{template_id}' payload is too large: max {max} bytes, got {actual} bytes"
    )]
    PayloadTooLarge {
        template_id: CommandTemplateId,
        max: usize,
        actual: usize,
    },
}

impl CommandRenderError {
    pub const fn template_id(&self) -> &CommandTemplateId {
        match self {
            Self::EmptyPlaceholder { template_id, .. }
            | Self::UnclosedPlaceholder { template_id, .. }
            | Self::UnopenedPlaceholder { template_id, .. }
            | Self::MissingParameter { template_id, .. }
            | Self::NonScalarParameter { template_id, .. }
            | Self::InvalidTopicSegment { template_id, .. }
            | Self::InvalidTopic { template_id, .. }
            | Self::InvalidPayload { template_id, .. }
            | Self::PayloadTooLarge { template_id, .. } => template_id,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TemplateSurface {
    Topic,
    Payload,
}

type PlaceholderRenderer = fn(
    &CommandTemplate,
    &str,
    &BTreeMap<String, serde_json::Value>,
) -> Result<String, CommandRenderError>;

impl std::fmt::Display for TemplateSurface {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Topic => "topic",
            Self::Payload => "payload",
        })
    }
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

fn render_topic_template(
    template: &CommandTemplate,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<String, CommandRenderError> {
    render_text_template(
        template,
        TemplateSurface::Topic,
        template.topic_template.as_str(),
        params,
        topic_param,
    )
}

fn render_payload_value(
    template: &CommandTemplate,
    value: &serde_json::Value,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<serde_json::Value, CommandRenderError> {
    match value {
        serde_json::Value::String(text) => render_payload_string(template, text, params),
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| render_payload_value(template, value, params))
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
        serde_json::Value::Object(object) => object
            .iter()
            .map(|(key, value)| Ok((key.clone(), render_payload_value(template, value, params)?)))
            .collect::<Result<serde_json::Map<_, _>, CommandRenderError>>()
            .map(serde_json::Value::Object),
        _ => Ok(value.clone()),
    }
}

fn render_payload_string(
    template: &CommandTemplate,
    text: &str,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<serde_json::Value, CommandRenderError> {
    if let Some(key) = exact_placeholder_key(text) {
        validate_placeholder_key(template, TemplateSurface::Payload, key)?;
        return params
            .get(key)
            .cloned()
            .ok_or_else(|| missing_param_error(template, key));
    }

    render_text_template(
        template,
        TemplateSurface::Payload,
        text,
        params,
        scalar_param,
    )
    .map(serde_json::Value::String)
}

fn render_text_template(
    template: &CommandTemplate,
    surface: TemplateSurface,
    text: &str,
    params: &BTreeMap<String, serde_json::Value>,
    render_param: PlaceholderRenderer,
) -> Result<String, CommandRenderError> {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;

    while let Some(start) = rest.find('{') {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 1..];
        let Some(end) = after_start.find('}') else {
            return Err(CommandRenderError::UnclosedPlaceholder {
                template_id: template.id.clone(),
                surface,
            });
        };
        let key = &after_start[..end];
        validate_placeholder_key(template, surface, key)?;
        output.push_str(&render_param(template, key, params)?);
        rest = &after_start[end + 1..];
    }
    output.push_str(rest);

    if output.contains('}') {
        return Err(CommandRenderError::UnopenedPlaceholder {
            template_id: template.id.clone(),
            surface,
        });
    }

    Ok(output)
}

fn exact_placeholder_key(text: &str) -> Option<&str> {
    text.strip_prefix('{')?.strip_suffix('}')
}

fn topic_param(
    template: &CommandTemplate,
    key: &str,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<String, CommandRenderError> {
    let value = scalar_param(template, key, params)?;
    if value.is_empty() {
        return Err(invalid_topic_segment_error(
            template,
            key,
            "must not be empty",
        ));
    }

    if value.contains('/') || value.contains('+') || value.contains('#') {
        return Err(invalid_topic_segment_error(
            template,
            key,
            "must not contain '/', '+', or '#'",
        ));
    }

    if value.contains('\0') || value.chars().any(char::is_control) {
        return Err(invalid_topic_segment_error(
            template,
            key,
            "must not contain null byte or control characters",
        ));
    }

    Ok(value)
}

fn scalar_param(
    template: &CommandTemplate,
    key: &str,
    params: &BTreeMap<String, serde_json::Value>,
) -> Result<String, CommandRenderError> {
    let value = params
        .get(key)
        .ok_or_else(|| missing_param_error(template, key))?;

    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(CommandRenderError::NonScalarParameter {
                template_id: template.id.clone(),
                name: key.to_owned(),
            })
        }
    }
}

fn validate_placeholder_key(
    template: &CommandTemplate,
    surface: TemplateSurface,
    key: &str,
) -> Result<(), CommandRenderError> {
    if key.trim().is_empty() {
        return Err(CommandRenderError::EmptyPlaceholder {
            template_id: template.id.clone(),
            surface,
        });
    }

    Ok(())
}

fn missing_param_error(template: &CommandTemplate, key: &str) -> CommandRenderError {
    CommandRenderError::MissingParameter {
        template_id: template.id.clone(),
        name: key.to_owned(),
    }
}

fn invalid_topic_segment_error(
    template: &CommandTemplate,
    key: &str,
    reason: &'static str,
) -> CommandRenderError {
    CommandRenderError::InvalidTopicSegment {
        template_id: template.id.clone(),
        name: key.to_owned(),
        reason,
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

fn saturating_usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use pipe_bolt_domain::{BrokerId, CommandTemplate, MqttQos};
    use serde_json::json;

    use super::*;

    #[test]
    fn renderer_should_render_topic_when_topic_param_is_valid() {
        let rendered = render_draft(BTreeMap::from([
            ("device_id".to_owned(), json!("device-1")),
            ("state".to_owned(), json!("ON")),
        ]))
        .expect("rendered command");

        assert_eq!(rendered.topic.as_str(), "devices/device-1/commands/relay");
    }

    #[test]
    fn renderer_should_reject_missing_parameter_when_topic_param_absent() {
        let error = render_draft(BTreeMap::from([("state".to_owned(), json!("ON"))]))
            .expect_err("missing param");

        assert!(matches!(error, CommandRenderError::MissingParameter { .. }));
    }

    #[test]
    fn renderer_should_reject_topic_segment_when_param_contains_slash() {
        let error = render_draft(BTreeMap::from([
            ("device_id".to_owned(), json!("factory/a")),
            ("state".to_owned(), json!("ON")),
        ]))
        .expect_err("invalid topic segment");

        assert!(matches!(
            error,
            CommandRenderError::InvalidTopicSegment { .. }
        ));
    }

    #[test]
    fn renderer_should_render_scalar_embedded_placeholder_when_param_is_scalar() {
        let rendered = render_draft(BTreeMap::from([
            ("device_id".to_owned(), json!("device-1")),
            ("state".to_owned(), json!(true)),
        ]))
        .expect("rendered command");
        let payload: serde_json::Value =
            serde_json::from_slice(&rendered.payload).expect("json payload");

        assert_eq!(payload["state"], json!("relay true"));
    }

    #[test]
    fn renderer_should_preserve_json_type_when_payload_is_exact_placeholder() {
        let template = command_template_with_payload(json!("{state}"));
        let rendered = CommandTemplateRenderer::default()
            .render_draft(
                &template,
                &BTreeMap::from([
                    ("device_id".to_owned(), json!("device-1")),
                    ("state".to_owned(), json!(true)),
                ]),
            )
            .expect("rendered command");
        let payload: serde_json::Value =
            serde_json::from_slice(&rendered.payload).expect("json payload");

        assert_eq!(payload, json!(true));
    }

    #[test]
    fn renderer_should_reject_payload_when_rendered_payload_too_large() {
        let template = command_template_with_payload(json!("{state}"));
        let error = CommandTemplateRenderer::new(4)
            .render_draft(
                &template,
                &BTreeMap::from([
                    ("device_id".to_owned(), json!("device-1")),
                    ("state".to_owned(), json!("too-large")),
                ]),
            )
            .expect_err("payload too large");

        assert!(matches!(error, CommandRenderError::PayloadTooLarge { .. }));
    }

    #[test]
    fn renderer_should_reject_unclosed_placeholder_when_topic_has_open_brace_only() {
        let mut template = command_template();
        template.topic_template = "devices/{device_id/commands/relay".to_owned();
        let error = CommandTemplateRenderer::default()
            .render_draft(
                &template,
                &BTreeMap::from([
                    ("device_id".to_owned(), json!("device-1")),
                    ("state".to_owned(), json!("ON")),
                ]),
            )
            .expect_err("unclosed placeholder");

        assert!(matches!(
            error,
            CommandRenderError::UnclosedPlaceholder {
                surface: TemplateSurface::Topic,
                ..
            }
        ));
    }

    #[test]
    fn renderer_should_reject_unopened_placeholder_when_payload_has_close_brace_only() {
        let template = command_template_with_payload(json!("relay }"));
        let error = CommandTemplateRenderer::default()
            .render_draft(
                &template,
                &BTreeMap::from([
                    ("device_id".to_owned(), json!("device-1")),
                    ("state".to_owned(), json!("ON")),
                ]),
            )
            .expect_err("unopened placeholder");

        assert!(matches!(
            error,
            CommandRenderError::UnopenedPlaceholder {
                surface: TemplateSurface::Payload,
                ..
            }
        ));
    }

    #[test]
    fn renderer_should_produce_rendered_command_when_context_is_present() {
        let template = command_template();
        let command = CommandTemplateRenderer::default()
            .render(
                CommandRenderContext {
                    project_id: ProjectId::new("project-1").expect("project id"),
                    command_execution_id: CommandExecutionId::new("command-exec-1")
                        .expect("command execution id"),
                    correlation_id: "corr-1".to_owned(),
                },
                &template,
                &BTreeMap::from([
                    ("device_id".to_owned(), json!("device-1")),
                    ("state".to_owned(), json!("ON")),
                ]),
            )
            .expect("rendered command");

        assert_eq!(command.command_template_id(), &template.id);
    }

    fn render_draft(
        params: BTreeMap<String, serde_json::Value>,
    ) -> Result<CommandRenderDraft, CommandRenderError> {
        CommandTemplateRenderer::default().render_draft(&command_template(), &params)
    }

    fn command_template() -> CommandTemplate {
        command_template_with_payload(json!({ "relay": 1, "state": "relay {state}" }))
    }

    fn command_template_with_payload(payload_template: serde_json::Value) -> CommandTemplate {
        CommandTemplate {
            id: CommandTemplateId::new("relay-on").expect("command template id"),
            name: "Turn Relay On".to_owned(),
            broker_id: BrokerId::new("broker-local").expect("broker id"),
            topic_template: "devices/{device_id}/commands/relay".to_owned(),
            payload_template,
            qos: MqttQos::AtLeastOnce,
            retain: false,
            enabled: true,
        }
    }
}
