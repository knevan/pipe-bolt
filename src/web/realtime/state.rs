use std::time::Duration;

use crate::bus::TelemetryEvent;
use crate::command::{CommandQueueReceipt, CommandRequest};
use crate::mqtt::engine::MqttHandle;
use crate::web::realtime::error::CommandEndpointError;

const DEFAULT_WEBSOCKET_CLIENT_BUFFER: usize = 63;
const DEFAULT_WEBSOCKET_PING_INTERVAL: Duration = Duration::from_secs(29);
const DEFAULT_WEBSOCKET_SEND_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_SSE_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(14);

/// Shared immutable runtime settings for realtime HTTP, SSE, and WebSocket handlers.
#[derive(Clone)]
pub struct RealtimeBridgeState {
    mqtt: MqttHandle,
    websocket_client_buffer: usize,
    websocket_ping_interval: Duration,
    websocket_send_timeout: Duration,
    sse_keep_alive_interval: Duration,
}

impl RealtimeBridgeState {
    pub fn new(mqtt: MqttHandle) -> Self {
        Self {
            mqtt,
            websocket_client_buffer: DEFAULT_WEBSOCKET_CLIENT_BUFFER,
            websocket_ping_interval: DEFAULT_WEBSOCKET_PING_INTERVAL,
            websocket_send_timeout: DEFAULT_WEBSOCKET_SEND_TIMEOUT,
            sse_keep_alive_interval: DEFAULT_SSE_KEEP_ALIVE_INTERVAL,
        }
    }

    pub fn with_websocket_client_buffer(mut self, capacity: usize) -> Self {
        self.websocket_client_buffer = capacity;
        self
    }

    pub fn with_websocket_ping_interval(mut self, interval: Duration) -> Self {
        if !interval.is_zero() {
            self.websocket_ping_interval = interval;
        }
        self
    }

    pub fn with_websocket_send_timeout(mut self, timeout: Duration) -> Self {
        if !timeout.is_zero() {
            self.websocket_send_timeout = timeout;
        }
        self
    }

    pub fn with_sse_keep_alive_interval(mut self, interval: Duration) -> Self {
        if !interval.is_zero() {
            self.sse_keep_alive_interval = interval;
        }
        self
    }

    pub(crate) fn websocket_client_buffer(&self) -> usize {
        self.websocket_client_buffer
    }

    pub(crate) fn websocket_ping_interval(&self) -> Duration {
        self.websocket_ping_interval
    }

    pub(crate) fn websocket_send_timeout(&self) -> Duration {
        self.websocket_send_timeout
    }

    pub(crate) fn sse_keep_alive_interval(&self) -> Duration {
        self.sse_keep_alive_interval
    }

    pub(crate) fn subscribe_telemetry(&self) -> tokio::sync::broadcast::Receiver<TelemetryEvent> {
        self.mqtt.subscribe_telemetry()
    }

    pub(crate) fn enqueue_command(
        &self,
        request: CommandRequest,
    ) -> Result<CommandQueueReceipt, CommandEndpointError> {
        let command = request.validate()?;
        command
            .enqueue(&self.mqtt)
            .map_err(CommandEndpointError::Queue)
    }
}
