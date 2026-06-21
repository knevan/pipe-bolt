use std::sync::Arc;
use std::time::Duration;

use pipe_bolt_domain::{
    BrokerConnectionConfig, BrokerId, CommandTemplate, HttpHeaderTemplate, HttpMethod,
    MqttCredentials, PayloadSchemaMapping, ProjectConfig, ProjectId, ReconnectPolicy,
    RuleDefinition, SecretString, SinkDefinition, SinkId, SinkKind, TlsMode, TopicRouteConfig,
};
use serde::{Deserialize, Serialize};

use crate::error::StorageError;
use crate::secret::{EncryptedSecret, SecretCipher};

#[derive(Clone)]
pub(crate) struct ProjectConfigCodec {
    cipher: Arc<dyn SecretCipher>,
}

impl ProjectConfigCodec {
    pub(crate) fn new(cipher: Arc<dyn SecretCipher>) -> Self {
        Self { cipher }
    }

    pub(crate) fn encode(
        &self,
        config: &ProjectConfig,
    ) -> Result<StoredProjectConfig, StorageError> {
        StoredProjectConfig::from_domain(config, self.cipher.as_ref())
    }

    pub(crate) fn decode(
        &self,
        stored: StoredProjectConfig,
    ) -> Result<ProjectConfig, StorageError> {
        stored.into_domain(self.cipher.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredProjectConfig {
    pub id: ProjectId,
    pub tenant_id: Option<pipe_bolt_domain::TenantId>,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub version: u64,
    pub brokers: Vec<StoredBrokerConnectionConfig>,
    pub routes: Vec<TopicRouteConfig>,
    pub schema_mappings: Vec<PayloadSchemaMapping>,
    pub rules: Vec<RuleDefinition>,
    pub command_templates: Vec<CommandTemplate>,
    pub sinks: Vec<StoredSinkDefinition>,
}

impl StoredProjectConfig {
    fn from_domain(
        config: &ProjectConfig,
        cipher: &dyn SecretCipher,
    ) -> Result<Self, StorageError> {
        let brokers = config
            .brokers
            .iter()
            .map(|broker| StoredBrokerConnectionConfig::from_domain(&config.id, broker, cipher))
            .collect::<Result<Vec<_>, _>>()?;
        let sinks = config
            .sinks
            .iter()
            .map(|sink| StoredSinkDefinition::from_domain(&config.id, sink, cipher))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            id: config.id.clone(),
            tenant_id: config.tenant_id.clone(),
            name: config.name.clone(),
            description: config.description.clone(),
            enabled: config.enabled,
            version: config.version,
            brokers,
            routes: config.routes.clone(),
            schema_mappings: config.schema_mappings.clone(),
            rules: config.rules.clone(),
            command_templates: config.command_templates.clone(),
            sinks,
        })
    }

    fn into_domain(self, cipher: &dyn SecretCipher) -> Result<ProjectConfig, StorageError> {
        let brokers = self
            .brokers
            .into_iter()
            .map(|broker| broker.into_domain(&self.id, cipher))
            .collect::<Result<Vec<_>, _>>()?;
        let sinks = self
            .sinks
            .into_iter()
            .map(|sink| sink.into_domain(&self.id, cipher))
            .collect::<Result<Vec<_>, _>>()?;

        let config = ProjectConfig {
            id: self.id,
            tenant_id: self.tenant_id,
            name: self.name,
            description: self.description,
            enabled: self.enabled,
            version: self.version,
            brokers,
            routes: self.routes,
            schema_mappings: self.schema_mappings,
            rules: self.rules,
            command_templates: self.command_templates,
            sinks,
        };
        config.validate()?;
        Ok(config)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredBrokerConnectionConfig {
    pub id: BrokerId,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub tls: TlsMode,
    pub credentials: Option<StoredMqttCredentials>,
    #[serde(with = "duration_seconds")]
    pub keep_alive: Duration,
    pub clean_session: bool,
    pub client_id: String,
    pub reconnect: ReconnectPolicy,
    pub enabled: bool,
}

impl StoredBrokerConnectionConfig {
    fn from_domain(
        project_id: &ProjectId,
        broker: &BrokerConnectionConfig,
        cipher: &dyn SecretCipher,
    ) -> Result<Self, StorageError> {
        let credentials = broker
            .credentials
            .as_ref()
            .map(|credentials| {
                StoredMqttCredentials::from_domain(project_id, &broker.id, credentials, cipher)
            })
            .transpose()?;

        Ok(Self {
            id: broker.id.clone(),
            name: broker.name.clone(),
            host: broker.host.clone(),
            port: broker.port,
            tls: broker.tls,
            credentials,
            keep_alive: broker.keep_alive,
            clean_session: broker.clean_session,
            client_id: broker.client_id.clone(),
            reconnect: broker.reconnect.clone(),
            enabled: broker.enabled,
        })
    }

    fn into_domain(
        self,
        project_id: &ProjectId,
        cipher: &dyn SecretCipher,
    ) -> Result<BrokerConnectionConfig, StorageError> {
        let credentials = self
            .credentials
            .map(|credentials| credentials.into_domain(project_id, &self.id, cipher))
            .transpose()?;

        Ok(BrokerConnectionConfig {
            id: self.id,
            name: self.name,
            host: self.host,
            port: self.port,
            tls: self.tls,
            credentials,
            keep_alive: self.keep_alive,
            clean_session: self.clean_session,
            client_id: self.client_id,
            reconnect: self.reconnect,
            enabled: self.enabled,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredMqttCredentials {
    pub username: String,
    pub password: EncryptedSecret,
}

impl StoredMqttCredentials {
    fn from_domain(
        project_id: &ProjectId,
        broker_id: &BrokerId,
        credentials: &MqttCredentials,
        cipher: &dyn SecretCipher,
    ) -> Result<Self, StorageError> {
        let aad = broker_password_aad(project_id, broker_id);
        Ok(Self {
            username: credentials.username.clone(),
            password: cipher.encrypt(credentials.password.expose_secret(), aad.as_bytes())?,
        })
    }

    fn into_domain(
        self,
        project_id: &ProjectId,
        broker_id: &BrokerId,
        cipher: &dyn SecretCipher,
    ) -> Result<MqttCredentials, StorageError> {
        let aad = broker_password_aad(project_id, broker_id);
        let password = cipher.decrypt(&self.password, aad.as_bytes())?;
        Ok(MqttCredentials {
            username: self.username,
            password: SecretString::new(password)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredSinkDefinition {
    pub id: SinkId,
    pub name: String,
    pub enabled: bool,
    pub kind: StoredSinkKind,
}

impl StoredSinkDefinition {
    fn from_domain(
        project_id: &ProjectId,
        sink: &SinkDefinition,
        cipher: &dyn SecretCipher,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            id: sink.id.clone(),
            name: sink.name.clone(),
            enabled: sink.enabled,
            kind: StoredSinkKind::from_domain(project_id, &sink.id, &sink.kind, cipher)?,
        })
    }

    fn into_domain(
        self,
        project_id: &ProjectId,
        cipher: &dyn SecretCipher,
    ) -> Result<SinkDefinition, StorageError> {
        let kind = self.kind.into_domain(project_id, &self.id, cipher)?;
        Ok(SinkDefinition {
            id: self.id,
            name: self.name,
            enabled: self.enabled,
            kind,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum StoredSinkKind {
    Webhook {
        url: String,
        method: HttpMethod,
        headers: Vec<StoredHttpHeaderTemplate>,
        #[serde(with = "duration_millis")]
        timeout: Duration,
    },
    Database {
        connection_ref: String,
        table: String,
    },
}

impl StoredSinkKind {
    fn from_domain(
        project_id: &ProjectId,
        sink_id: &SinkId,
        kind: &SinkKind,
        cipher: &dyn SecretCipher,
    ) -> Result<Self, StorageError> {
        match kind {
            SinkKind::Webhook {
                url,
                method,
                headers,
                timeout,
            } => {
                let headers = headers
                    .iter()
                    .map(|header| {
                        StoredHttpHeaderTemplate::from_domain(project_id, sink_id, header, cipher)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::Webhook {
                    url: url.clone(),
                    method: *method,
                    headers,
                    timeout: *timeout,
                })
            }
            SinkKind::Database {
                connection_ref,
                table,
            } => Ok(Self::Database {
                connection_ref: connection_ref.clone(),
                table: table.clone(),
            }),
        }
    }

    fn into_domain(
        self,
        project_id: &ProjectId,
        sink_id: &SinkId,
        cipher: &dyn SecretCipher,
    ) -> Result<SinkKind, StorageError> {
        match self {
            Self::Webhook {
                url,
                method,
                headers,
                timeout,
            } => {
                let headers = headers
                    .into_iter()
                    .map(|header| header.into_domain(project_id, sink_id, cipher))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(SinkKind::Webhook {
                    url,
                    method,
                    headers,
                    timeout,
                })
            }
            Self::Database {
                connection_ref,
                table,
            } => Ok(SinkKind::Database {
                connection_ref,
                table,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredHttpHeaderTemplate {
    pub name: String,
    pub value: EncryptedSecret,
}

impl StoredHttpHeaderTemplate {
    fn from_domain(
        project_id: &ProjectId,
        sink_id: &SinkId,
        header: &HttpHeaderTemplate,
        cipher: &dyn SecretCipher,
    ) -> Result<Self, StorageError> {
        let aad = sink_header_aad(project_id, sink_id, &header.name);
        Ok(Self {
            name: header.name.clone(),
            value: cipher.encrypt(header.value.expose_secret(), aad.as_bytes())?,
        })
    }

    fn into_domain(
        self,
        project_id: &ProjectId,
        sink_id: &SinkId,
        cipher: &dyn SecretCipher,
    ) -> Result<HttpHeaderTemplate, StorageError> {
        let aad = sink_header_aad(project_id, sink_id, &self.name);
        let value = cipher.decrypt(&self.value, aad.as_bytes())?;
        Ok(HttpHeaderTemplate {
            name: self.name,
            value: SecretString::new(value)?,
        })
    }
}

fn broker_password_aad(project_id: &ProjectId, broker_id: &BrokerId) -> String {
    format!("project:{project_id}:broker:{broker_id}:mqtt_password")
}

fn sink_header_aad(project_id: &ProjectId, sink_id: &SinkId, header_name: &str) -> String {
    format!("project:{project_id}:sink:{sink_id}:header:{header_name}")
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use pipe_bolt_domain::{
        BackpressurePolicy, BrokerConnectionConfig, BrokerId, DeviceIdExtraction,
        HttpHeaderTemplate, HttpMethod, MqttCredentials, MqttQos, PayloadCodecKind,
        PayloadSchemaMapping, ProjectConfig, ProjectId, ReconnectPolicy, RouteId, RuleDefinition,
        SecretString, SinkDefinition, SinkId, SinkKind, TlsMode, TopicFilter, TopicRouteConfig,
    };

    use super::*;
    use crate::secret::AesGcmSecretCipher;

    #[test]
    fn codec_roundtrips_project_config_without_serializing_plaintext_secrets() {
        let key = STANDARD.encode([7u8; 32]);
        let cipher = Arc::new(AesGcmSecretCipher::from_base64_key("test", &key).expect("cipher"));
        let codec = ProjectConfigCodec::new(cipher);
        let config = project_config_with_secrets();

        let stored = codec.encode(&config).expect("encode");
        let json = serde_json::to_string(&stored).expect("json");
        let decoded = codec.decode(stored).expect("decode");

        assert!(!json.contains("mqtt-password"));
        assert!(!json.contains("sink-token"));
        assert_eq!(decoded, config);
    }

    fn project_config_with_secrets() -> ProjectConfig {
        let broker_id = BrokerId::new("broker-local").expect("broker id");
        ProjectConfig {
            id: ProjectId::new("project-local").expect("project id"),
            tenant_id: None,
            name: "Local Project".to_owned(),
            description: None,
            enabled: true,
            version: 1,
            brokers: vec![BrokerConnectionConfig {
                id: broker_id.clone(),
                name: "Local MQTT".to_owned(),
                host: "localhost".to_owned(),
                port: 1883,
                tls: TlsMode::Disabled,
                credentials: Some(MqttCredentials {
                    username: "mqtt-user".to_owned(),
                    password: SecretString::new("mqtt-password").expect("secret"),
                }),
                keep_alive: Duration::from_secs(30),
                clean_session: false,
                client_id: "pipe-bolt-local".to_owned(),
                reconnect: ReconnectPolicy::default(),
                enabled: true,
            }],
            routes: vec![TopicRouteConfig {
                id: RouteId::new("route-telemetry").expect("route id"),
                broker_id,
                name: "Telemetry".to_owned(),
                topic_filter: TopicFilter::new("devices/+/telemetry").expect("topic filter"),
                codec: PayloadCodecKind::Json,
                schema_mapping_id: None,
                device_id: DeviceIdExtraction::TopicWildcardIndex { index: 0 },
                event_type: "telemetry".to_owned(),
                qos: MqttQos::AtLeastOnce,
                enabled: true,
                backpressure: BackpressurePolicy::Reject,
            }],
            schema_mappings: Vec::<PayloadSchemaMapping>::new(),
            rules: Vec::<RuleDefinition>::new(),
            command_templates: Vec::new(),
            sinks: vec![SinkDefinition {
                id: SinkId::new("sink-webhook").expect("sink id"),
                name: "Webhook".to_owned(),
                enabled: true,
                kind: SinkKind::Webhook {
                    url: "https://example.com/events".to_owned(),
                    method: HttpMethod::Post,
                    headers: vec![HttpHeaderTemplate {
                        name: "Authorization".to_owned(),
                        value: SecretString::new("sink-token").expect("secret"),
                    }],
                    timeout: Duration::from_secs(5),
                },
            }],
        }
    }
}
