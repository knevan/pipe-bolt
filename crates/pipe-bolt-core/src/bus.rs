use std::time::SystemTime;

use pipe_bolt_domain::{BrokerId, CommandExecutionId, CommandTemplateId, ProjectId};
use rumqttc::QoS;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc};

use crate::error::MqttEngineError;
use crate::message::envelope::MqttMessage;

#[derive(Debug, Clone)]
pub struct BusConfig {
    pub ingress_capacity: usize,
    pub telemetry_capacity: usize,
    pub command_capacity: usize,
}

impl Default for BusConfig {
    fn default() -> Self {
        Self {
            ingress_capacity: 1024,
            telemetry_capacity: 1024,
            command_capacity: 256,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TelemetryEvent {
    pub topic: String,
    pub payload: Vec<u8>,
    pub received_at: SystemTime,
}

impl TelemetryEvent {
    pub fn from_message(message: &MqttMessage) -> Self {
        Self {
            topic: message.topic().to_owned(),
            payload: message.payload().to_vec(),
            received_at: message.received_at(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MqttCommand {
    pub topic: String,
    pub qos: QoS,
    pub retain: bool,
    pub payload: Vec<u8>,
    pub context: Option<MqttCommandContext>,
}

impl MqttCommand {
    pub fn publish(
        topic: impl Into<String>,
        qos: QoS,
        retain: bool,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            topic: topic.into(),
            qos,
            retain,
            payload: payload.into(),
            context: None,
        }
    }

    pub fn publish_with_context(
        context: MqttCommandContext,
        topic: impl Into<String>,
        qos: QoS,
        retain: bool,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            topic: topic.into(),
            qos,
            retain,
            payload: payload.into(),
            context: Some(context),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MqttCommandContext {
    pub project_id: ProjectId,
    pub broker_id: BrokerId,
    pub command_template_id: CommandTemplateId,
    pub command_execution_id: CommandExecutionId,
    pub correlation_id: String,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MqttCommandPublishStatus {
    Published,
    Failed,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MqttCommandPublishOutcome {
    pub context: MqttCommandContext,
    pub topic: String,
    pub status: MqttCommandPublishStatus,
    pub failure_reason: Option<String>,
}

#[derive(Clone)]
pub struct InternalBusHandle {
    ingress_tx: mpsc::Sender<MqttMessage>,
    telemetry_tx: broadcast::Sender<TelemetryEvent>,
    command_tx: mpsc::Sender<MqttCommand>,
}

/// Bounded in-process channels between MQTT polling, routing, telemetry fan-out, and command publishing.
pub struct InternalBus {
    handle: InternalBusHandle,
    ingress_rx: mpsc::Receiver<MqttMessage>,
    command_rx: mpsc::Receiver<MqttCommand>,
}

impl InternalBus {
    pub fn new(config: BusConfig) -> Self {
        let (ingress_tx, ingress_rx) = mpsc::channel(config.ingress_capacity);
        let (telemetry_tx, _) = broadcast::channel(config.telemetry_capacity);
        let (command_tx, command_rx) = mpsc::channel(config.command_capacity);

        Self {
            handle: InternalBusHandle {
                ingress_tx,
                telemetry_tx,
                command_tx,
            },
            ingress_rx,
            command_rx,
        }
    }

    pub fn handle(&self) -> InternalBusHandle {
        self.handle.clone()
    }

    pub fn split(
        self,
    ) -> (
        InternalBusHandle,
        mpsc::Receiver<MqttMessage>,
        mpsc::Receiver<MqttCommand>,
    ) {
        (self.handle, self.ingress_rx, self.command_rx)
    }
}

impl InternalBusHandle {
    pub async fn enqueue_ingress(&self, message: MqttMessage) -> Result<(), MqttEngineError> {
        self.ingress_tx
            .send(message)
            .await
            .map_err(|_| MqttEngineError::IngressClosed)
    }

    pub async fn try_enqueue_ingress(&self, message: MqttMessage) -> Result<(), MqttEngineError> {
        self.ingress_tx.try_send(message).map_err(|err| match err {
            TrySendError::Full(_) => MqttEngineError::IngressQueueFull,
            TrySendError::Closed(_) => MqttEngineError::IngressClosed,
        })
    }

    pub fn publish_telemetry(&self, event: TelemetryEvent) -> Result<usize, MqttEngineError> {
        // Broadcast telemetry is lossy by design,
        // slow consumers receive lag notifications instead of backpressure MQTT ingestion
        match self.telemetry_tx.send(event) {
            Ok(receiver_count) => Ok(receiver_count),
            // Returning Ok(0) because having 0 active receivers is a normal state for lossy telemetry
            Err(_) => Ok(0),
        }
    }

    pub fn subscribe_telemetry(&self) -> broadcast::Receiver<TelemetryEvent> {
        self.telemetry_tx.subscribe()
    }

    pub fn try_enqueue_command(&self, command: MqttCommand) -> Result<(), MqttEngineError> {
        self.command_tx.try_send(command).map_err(|err| match err {
            TrySendError::Full(_) => MqttEngineError::CommandQueueFull,
            TrySendError::Closed(_) => MqttEngineError::CommandQueueClosed,
        })
    }
}
