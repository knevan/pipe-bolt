use std::time::Duration;

use rumqttc::{MqttOptions, QoS, Transport};

use crate::error::MqttEngineError;

/// TLS mode used by the MQTT client
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MqttTlsMode {
    Disabled,
    EnabledWithNativeRoot,
}

/// Subscription registered by the engine after each successful connection
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MqttSubscription {
    pub topic: String,
    pub qos: QoS,
}

impl MqttSubscription {
    pub fn new(topic: impl Into<String>, qos: QoS) -> Self {
        Self {
            topic: topic.into(),
            qos,
        }
    }
}

/// Runtime configuration for reconnect throttling.
#[derive(Debug, Clone)]
pub struct MqttReconnectConfig {
    pub min_delay: Duration,
    pub max_delay: Duration,
}

impl Default for MqttReconnectConfig {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
        }
    }
}

impl MqttReconnectConfig {
    pub fn validate(&self) -> Result<(), MqttEngineError> {
        if self.min_delay.is_zero() {
            return Err(MqttEngineError::InvalidConfig(
                "Reconnect min_delay must be greater than is_zero",
            ));
        }

        if self.max_delay < self.min_delay {
            return Err(MqttEngineError::InvalidConfig(
                "Reconnect max_delay must be greater than or equal to min_delay",
            ));
        }

        Ok(())
    }
}

/// Runtime configuration for the MQTT engine.
#[derive(Debug, Clone)]
pub struct MqttClientConfig {
    pub client_id: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: Duration,
    pub clean_session: bool,
    pub tls_mode: MqttTlsMode,
    pub request_channel_capacity: usize,
    pub subscriptions: Vec<MqttSubscription>,
    pub reconnect: MqttReconnectConfig,
}

impl MqttClientConfig {
    pub fn new(client_id: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            client_id: client_id.into(),
            host: host.into(),
            port,
            username: None,
            password: None,
            keep_alive: Duration::from_secs(30),
            clean_session: false,
            tls_mode: MqttTlsMode::Disabled,
            request_channel_capacity: 64,
            subscriptions: Vec::new(),
            reconnect: MqttReconnectConfig::default(),
        }
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    pub fn with_tls(mut self, tls_mode: MqttTlsMode) -> Self {
        self.tls_mode = tls_mode;
        self
    }

    pub fn with_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    pub fn with_clean_session(mut self, clean_session: bool) -> Self {
        self.clean_session = clean_session;
        self
    }

    pub fn with_reconnect(mut self, reconnect: MqttReconnectConfig) -> Self {
        self.reconnect = reconnect;
        self
    }

    pub fn with_subscription(mut self, topic: impl Into<String>, qos: QoS) -> Self {
        self.subscriptions.push(MqttSubscription::new(topic, qos));
        self
    }

    pub fn build_option(&self) -> Result<MqttOptions, MqttEngineError> {
        self.validate()?;

        let mut options = MqttOptions::new(&self.client_id, &self.host, self.port);
        options.set_keep_alive(self.keep_alive);
        options.set_clean_session(self.clean_session);

        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            options.set_credentials(username, password);
        }

        match self.tls_mode {
            MqttTlsMode::Disabled => {}
            MqttTlsMode::EnabledWithNativeRoot => {
                options.set_transport(Transport::tls_with_default_config());
            }
        }

        Ok(options)
    }

    pub fn validate(&self) -> Result<(), MqttEngineError> {
        if self.client_id.trim().is_empty() {
            return Err(MqttEngineError::InvalidConfig(
                "client_id must not be empty",
            ));
        }

        if self.host.trim().is_empty() {
            return Err(MqttEngineError::InvalidConfig("host must not be empty"));
        }

        if self.port == 0 {
            return Err(MqttEngineError::InvalidConfig("port must not be zero"));
        }

        if self.keep_alive < Duration::from_secs(5) {
            return Err(MqttEngineError::InvalidConfig(
                "keep_alive must be greater than or equal to 5 seconds",
            ));
        }

        if self.request_channel_capacity == 0 {
            return Err(MqttEngineError::InvalidConfig(
                "request_channel_capacity must be greater than zero",
            ));
        }

        if self.username.is_some() != self.password.is_some() {
            return Err(MqttEngineError::InvalidConfig(
                "username and password must be configured together",
            ));
        }

        for subscription in &self.subscriptions {
            if subscription.topic.trim().is_empty() {
                return Err(MqttEngineError::InvalidConfig(
                    "subscription topic must not be empty",
                ));
            }
        }

        self.reconnect.validate()?;

        Ok(())
    }
}
