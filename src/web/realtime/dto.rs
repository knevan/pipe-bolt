use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use serde::{Deserialize, Serialize};

use crate::bus::TelemetryEvent;
use crate::command::{CommandQueueReceipt, CommandRequest};
use crate::web::realtime::filter::{TelemetryFilter, telemetry_device, telemetry_event_type};
use crate::web::realtime::utils::system_time_ms;

#[derive(Debug, Serialize)]
pub struct TelemetryPayload {
    pub topic: String,
    pub device: Option<String>,
    pub event_type: Option<String>,
    pub payload_base64: String,
    pub payload_utf8: Option<String>,
    pub received_at_ms: u128,
}

impl TelemetryPayload {
    pub(crate) fn from_event(event: TelemetryEvent) -> Self {
        let device = telemetry_device(&event.topic).map(str::to_owned);
        let event_type = telemetry_event_type(&event.topic).map(str::to_owned);
        let payload_utf8 = String::from_utf8(event.payload.clone()).ok();

        Self {
            topic: event.topic,
            device,
            event_type,
            payload_base64: BASE64_STANDARD.encode(event.payload),
            payload_utf8,
            received_at_ms: system_time_ms(event.received_at),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TelemetryFilterSnapshot {
    pub device: Option<String>,
    pub topic: Option<String>,
    pub topic_prefix: Option<String>,
    pub event_type: Option<String>,
}

impl From<&TelemetryFilter> for TelemetryFilterSnapshot {
    fn from(value: &TelemetryFilter) -> Self {
        Self {
            device: value.device.clone(),
            topic: value.topic.clone(),
            topic_prefix: value.topic_prefix.clone(),
            event_type: value.event_type.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BridgeServerMessage {
    Ready {
        transport: &'static str,
        filter: TelemetryFilterSnapshot,
    },
    Telemetry {
        data: TelemetryPayload,
    },
    CommandQueue {
        data: CommandQueueReceipt,
    },
    Lagged {
        skipped: u64,
    },
    FilterUpdated {
        filter: TelemetryFilterSnapshot,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BridgeClientMessage {
    Subscribe {
        device: Option<String>,
        topic: Option<String>,
        topic_prefix: Option<String>,
        event_type: Option<String>,
    },
    Command {
        #[serde(flatten)]
        request: CommandRequest,
    },
    Ping,
}
