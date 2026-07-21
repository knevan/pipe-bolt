use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::id::{CommandTemplateId, EventId, SinkId};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[cfg_attr(feature = "salvo-oapi", derive(salvo::oapi::ToSchema))]
pub enum ActionIntentTemplate {
    StreamToUi,
    ForwardToSink {
        sink_id: SinkId,
    },
    ExecuteCommand {
        command_template_id: CommandTemplateId,
        #[serde(default)]
        params: BTreeMap<String, serde_json::Value>,
    },
    DropEvent,
    AddMetadata {
        key: String,
        value: String,
    },
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
    ExecuteCommand {
        event_id: EventId,
        command_template_id: CommandTemplateId,
        params: BTreeMap<String, serde_json::Value>,
        correlation_id: String,
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn action_template_should_serialize_execute_command_when_rule_uses_command_action() {
        let action = ActionIntentTemplate::ExecuteCommand {
            command_template_id: CommandTemplateId::new("relay-on").expect("command template id"),
            params: BTreeMap::from([("device_id".to_owned(), json!("device-1"))]),
        };

        let serialized = serde_json::to_value(action).expect("serialize action template");

        assert_eq!(
            serialized,
            json!({
                "type": "execute_command",
                "command_template_id": "relay-on",
                "params": { "device_id": "device-1" }
            })
        );
    }

    #[test]
    fn action_template_should_reject_publish_command_when_deserializing_legacy_name() {
        let result = serde_json::from_value::<ActionIntentTemplate>(json!({
            "type": "publish_command",
            "template_id": "relay-on"
        }));

        assert!(result.is_err());
    }
}
