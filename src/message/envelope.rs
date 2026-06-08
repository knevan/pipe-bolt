use std::time::SystemTime;

use rumqttc::{Publish, QoS};

use crate::error::MqttEngineError;

#[derive(Debug, Clone)]
pub struct MqttMessage {
    topic: String,
    qos: QoS,
    retain: bool,
    payload: Vec<u8>,
    received_at: SystemTime,
}

impl MqttMessage {
    pub fn new(
        topic: impl Into<String>,
        qos: QoS,
        retain: bool,
        payload: impl Into<Vec<u8>>,
        received_at: SystemTime,
    ) -> Result<Self, MqttEngineError> {
        let topic = topic.into();
        validate_topic_name(&topic)?;

        Ok(Self {
            topic,
            qos,
            retain,
            payload: payload.into(),
            received_at,
        })
    }

    pub fn from_publish(publish: Publish) -> Result<Self, MqttEngineError> {
        Self::new(
            publish.topic,
            publish.qos,
            publish.retain,
            publish.payload.to_vec(),
            SystemTime::now(),
        )
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn qos(&self) -> QoS {
        self.qos
    }

    pub fn retain(&self) -> bool {
        self.retain
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }

    pub fn received_at(&self) -> SystemTime {
        self.received_at
    }
}

fn validate_topic_name(topic: &str) -> Result<(), MqttEngineError> {
    if topic.is_empty() {
        return Err(MqttEngineError::InvalidTopicName(
            "topic name must not be empty".to_owned(),
        ));
    }

    if topic.contains('+') || topic.contains('#') {
        return Err(MqttEngineError::InvalidTopicName(
            "topic name must not contain MQTT wildcards".to_owned(),
        ));
    }

    Ok(())
}
