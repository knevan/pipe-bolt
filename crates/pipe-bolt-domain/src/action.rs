use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::{MqttQos, TopicName};
use crate::id::{BrokerId, CommandExecutionId, CommandTemplateId, EventId, SinkId};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActionIntentTemplate {
    StreamToUi,
    ForwardToSink { sink_id: SinkId },
    PublishCommand { template_id: CommandTemplateId },
    DropEvent,
    AddMetadata { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActionIntent {
    StreamToUi {
        event_id: EventId,
    },
    ForwardToSink {
        event_id: EventId,
        sink_id: SinkId,
        projection: Option<BTreeMap<String, serde_json::Value>>,
    },
    PublishMqttCommand {
        execution_id: CommandExecutionId,
        broker_id: BrokerId,
        template_id: CommandTemplateId,
        topic: TopicName,
        payload: serde_json::Value,
        qos: MqttQos,
        retain: bool,
    },
    DropEvent {
        event_id: EventId,
        reason: Option<String>,
    },
    AddMetadata {
        event_id: EventId,
        key: String,
        value: String,
    },
}
