use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize, Serializer};

use crate::error::DomainError;
use crate::id::{
    BrokerId, CommandTemplateId, FieldPath, ProjectId, RouteId, SchemaMappingId, SinkId, TenantId,
    validate_text,
};
use crate::rule::RuleDefinition;

const MAX_NAME_BYTES: usize = 160;
const MAX_DESCRIPTION_BYTES: usize = 2_048;
const MAX_TOPIC_BYTES: usize = 1_024;
const MAX_HOST_BYTES: usize = 255;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub id: ProjectId,
    pub tenant_id: Option<TenantId>,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub version: u64,
    pub brokers: Vec<BrokerConnectionConfig>,
    pub routes: Vec<TopicRouteConfig>,
    pub schema_mappings: Vec<PayloadSchemaMapping>,
    pub rules: Vec<RuleDefinition>,
    pub command_templates: Vec<CommandTemplate>,
    pub sinks: Vec<SinkDefinition>,
}

impl ProjectConfig {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_text("project_name", &self.name, MAX_NAME_BYTES)?;

        if let Some(description) = &self.description {
            validate_text("project_description", description, MAX_DESCRIPTION_BYTES)?;
        }

        for broker in &self.brokers {
            broker.validate()?;
        }

        for route in &self.routes {
            route.validate()?;
        }

        for mapping in &self.schema_mappings {
            mapping.validate()?;
        }

        for rule in &self.rules {
            rule.validate()?;
        }

        for command_template in &self.command_templates {
            command_template.validate()?;
        }

        for sink in &self.sinks {
            sink.validate()?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct BrokerConnectionConfig {
    pub id: BrokerId,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub tls: TlsMode,
    pub credentials: Option<MqttCredentials>,
    #[serde(with = "duration_seconds")]
    #[cfg_attr(feature = "salvo-oapi", salvo(schema(value_type = u64)))]
    pub keep_alive: Duration,
    pub clean_session: bool,
    pub client_id: String,
    pub reconnect: ReconnectPolicy,
    pub enabled: bool,
}

impl BrokerConnectionConfig {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_text("broker_name", &self.name, MAX_NAME_BYTES)?;
        validate_text("broker_host", &self.host, MAX_HOST_BYTES)?;
        validate_text("broker_client_id", &self.client_id, MAX_NAME_BYTES)?;

        if self.port == 0 {
            return Err(DomainError::InvalidBrokerPort);
        }

        if self.keep_alive < Duration::from_secs(5) {
            return Err(DomainError::InvalidKeepAlive);
        }

        if let Some(credentials) = &self.credentials {
            credentials.validate()?;
        }

        self.reconnect.validate()?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub enum TlsMode {
    Disabled,
    NativeRoots,
}

#[derive(Clone, Eq, PartialEq, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
#[cfg_attr(feature = "salvo-oapi", salvo(schema(value_type = String)))]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        validate_text("secret", &value, 4_096)?;
        Ok(Self(value))
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString(<redacted>)")
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("<redacted>")
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct MqttCredentials {
    pub username: String,
    pub password: SecretString,
}

impl MqttCredentials {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_text("mqtt_username", &self.username, MAX_NAME_BYTES)
    }
}

impl fmt::Debug for MqttCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MqttCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct ReconnectPolicy {
    #[serde(with = "duration_millis")]
    #[cfg_attr(feature = "salvo-oapi", salvo(schema(value_type = u64)))]
    pub min_delay: Duration,
    #[serde(with = "duration_millis")]
    #[cfg_attr(feature = "salvo-oapi", salvo(schema(value_type = u64)))]
    pub max_delay: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
        }
    }
}

impl ReconnectPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.min_delay.is_zero() {
            return Err(DomainError::EmptyField {
                field: "reconnect_min_delay",
            });
        }

        if self.max_delay < self.min_delay {
            return Err(DomainError::InvalidFieldPath {
                reason: "reconnect max_delay must be greater than or equal to min_delay",
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct TopicRouteConfig {
    pub id: RouteId,
    pub broker_id: BrokerId,
    pub name: String,
    pub topic_filter: TopicFilter,
    pub codec: PayloadCodecKind,
    pub schema_mapping_id: Option<SchemaMappingId>,
    pub device_id: DeviceIdExtraction,
    pub event_type: String,
    pub qos: MqttQos,
    pub enabled: bool,
    pub backpressure: BackpressurePolicy,
}

impl TopicRouteConfig {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_text("route_name", &self.name, MAX_NAME_BYTES)?;
        validate_text("event_type", &self.event_type, MAX_NAME_BYTES)?;
        self.topic_filter.validate()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct TopicFilter(String);

impl TopicFilter {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        let filter = Self(value);
        filter.validate()?;
        Ok(filter)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        validate_text("topic_filter", &self.0, MAX_TOPIC_BYTES)?;

        let parts: Vec<&str> = self.0.split('/').collect();
        for (index, part) in parts.iter().enumerate() {
            if part.contains('#') && *part != "#" {
                return Err(DomainError::InvalidTopicFilter {
                    reason: "multi-level wildcard must occupy the whole segment",
                });
            }

            if *part == "#" && index + 1 != parts.len() {
                return Err(DomainError::InvalidTopicFilter {
                    reason: "multi-level wildcard must be the last segment",
                });
            }

            if part.contains('+') && *part != "+" {
                return Err(DomainError::InvalidTopicFilter {
                    reason: "single-level wildcard must occupy the whole segment",
                });
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct TopicName(String);

impl TopicName {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        validate_text("topic", &value, MAX_TOPIC_BYTES)?;

        if value.contains('+') || value.contains('#') {
            return Err(DomainError::InvalidTopicName {
                reason: "topic name must not contain wildcards",
            });
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub enum PayloadCodecKind {
    Json,
    Raw,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub enum MqttQos {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub enum DeviceIdExtraction {
    None,
    Static { value: String },
    TopicWildcardIndex { index: usize },
    PayloadField { path: FieldPath },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub enum BackpressurePolicy {
    DropNewest,
    DropOldest,
    Reject,
    BlockProducer,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct PayloadSchemaMapping {
    pub id: SchemaMappingId,
    pub name: String,
    pub fields: Vec<FieldMapping>,
}

impl PayloadSchemaMapping {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_text("schema_mapping_name", &self.name, MAX_NAME_BYTES)?;

        for field in &self.fields {
            field.validate()?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct FieldMapping {
    pub source: FieldPath,
    pub target: String,
    pub value_type: FieldValueType,
    pub required: bool,
    pub default: Option<serde_json::Value>,
}

impl FieldMapping {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_text("field_mapping_target", &self.target, MAX_NAME_BYTES)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub enum FieldValueType {
    String,
    Number,
    Boolean,
    Object,
    Array,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct CommandTemplate {
    pub id: CommandTemplateId,
    pub name: String,
    pub broker_id: BrokerId,
    pub topic_template: String,
    pub payload_template: serde_json::Value,
    pub qos: MqttQos,
    pub retain: bool,
    pub enabled: bool,
}

impl CommandTemplate {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_text("command_template_name", &self.name, MAX_NAME_BYTES)?;
        validate_text(
            "command_topic_template",
            &self.topic_template,
            MAX_TOPIC_BYTES,
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct SinkDefinition {
    pub id: SinkId,
    pub name: String,
    pub enabled: bool,
    pub kind: SinkKind,
}

impl SinkDefinition {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_text("sink_name", &self.name, MAX_NAME_BYTES)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub enum SinkKind {
    Webhook {
        url: String,
        method: HttpMethod,
        headers: Vec<HttpHeaderTemplate>,
        #[serde(with = "duration_millis")]
        #[cfg_attr(feature = "salvo-oapi", salvo(schema(value_type = u64)))]
        timeout: Duration,
    },
    Database {
        connection_ref: String,
        table: String,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub enum HttpMethod {
    Post,
    Put,
    Patch,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub struct HttpHeaderTemplate {
    pub name: String,
    pub value: SecretString,
}

mod duration_seconds {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(value.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let seconds = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(seconds))
    }
}

mod duration_millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = value.as_millis().min(u128::from(u64::MAX)) as u64;
        serializer.serialize_u64(millis)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}
