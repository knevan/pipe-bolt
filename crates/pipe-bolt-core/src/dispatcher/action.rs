use std::collections::BTreeMap;

use pipe_bolt_domain::{ActionIntent, EventId, NormalizedEvent};
use tokio::sync::mpsc;

use crate::error::DispatchError;

const DEFAULT_MAX_METADATA_KEY_BYTES: usize = 128;
const DEFAULT_MAX_METADATA_VALUE_BYTES: usize = 1024;
const RESERVED_METADATA_PREFIXES: [&str; 2] = ["pipe_bolt.", "_pipe_bolt."];

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DispatchLimits {
    pub max_metadata_key_bytes: usize,
    pub max_metadata_value_bytes: usize,
}

impl Default for DispatchLimits {
    fn default() -> Self {
        Self {
            max_metadata_key_bytes: DEFAULT_MAX_METADATA_KEY_BYTES,
            max_metadata_value_bytes: DEFAULT_MAX_METADATA_VALUE_BYTES,
        }
    }
}

/// Result of dispatching action intents for a single normalized event.
#[derive(Debug, Clone, PartialEq)]
pub struct DispatchOutcome {
    pub event_id: EventId,
    pub executed: Vec<ExecutedAction>,
    pub skipped: Vec<SkippedAction>,
    pub failed: Vec<FailedAction>,
    pub dropped: bool,
}

/// Action that was accepted and executed by the dispatcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutedAction {
    StreamToUi { receiver_count: usize },
    AddMetadata { key: String },
    DropEvent,
}

impl DispatchOutcome {
    fn new(event_id: EventId) -> Self {
        Self {
            event_id,
            executed: Vec::new(),
            skipped: Vec::new(),
            failed: Vec::new(),
            dropped: false,
        }
    }
}

/// Action that was not executed because dispatcher semantics skipped it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkippedAction {
    AfterDrop {
        action_type: &'static str,
    },
    EventMismatch {
        action_type: &'static str,
        action_event_id: EventId,
    },
    Unsupported {
        action_type: &'static str,
    },
}

/// Action that was attempted but failed at the dispatch boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedAction {
    pub action_type: &'static str,
    pub error: DispatchError,
}

/// Bounded realtime event sink used by StreamToUi dispatch.
#[derive(Clone)]
pub struct BoundedRealtimeEventSink {
    tx: mpsc::Sender<NormalizedEvent>,
}

impl BoundedRealtimeEventSink {
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<NormalizedEvent>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (Self { tx }, rx)
    }
}

/// Minimal side effect boundary required by the dispatcher.
pub trait RealtimeEventSink: Clone + Send + Sync + 'static {
    fn try_publish(&self, event: NormalizedEvent) -> Result<usize, DispatchError>;
}

impl RealtimeEventSink for BoundedRealtimeEventSink {
    fn try_publish(&self, event: NormalizedEvent) -> Result<usize, DispatchError> {
        self.tx.try_send(event).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => DispatchError::RealtimeBackpressure,
            mpsc::error::TrySendError::Closed(_) => DispatchError::RealtimeUnavailable,
        })?;

        Ok(1)
    }
}

/// Deterministic dispatcher for rule action intents.
#[derive(Clone)]
pub struct ActionDispatcher<S> {
    realtime: S,
    limits: DispatchLimits,
}

impl<S> ActionDispatcher<S>
where
    S: RealtimeEventSink,
{
    pub fn new(realtime: S) -> Self {
        Self::with_limits(realtime, DispatchLimits::default())
    }

    pub fn with_limits(realtime: S, limits: DispatchLimits) -> Self {
        Self { realtime, limits }
    }

    pub fn dispatch(&self, event: &NormalizedEvent, intents: &[ActionIntent]) -> DispatchOutcome {
        let mut outcome = DispatchOutcome::new(event.id.clone());
        let mut context = EventDispatchContext::new(event);

        for intent in intents {
            let action_type = action_type(intent);

            if outcome.dropped {
                outcome
                    .skipped
                    .push(SkippedAction::AfterDrop { action_type });
                continue;
            }

            if let Some(action_event_id) = intent_event_id(intent)
                && action_event_id != &event.id
            {
                outcome.skipped.push(SkippedAction::EventMismatch {
                    action_type,
                    action_event_id: action_event_id.clone(),
                });
                continue;
            }

            match intent {
                ActionIntent::StreamToUi { .. } => {
                    let enriched = context.enriched_event();
                    match self.realtime.try_publish(enriched) {
                        Ok(receiver_count) => outcome
                            .executed
                            .push(ExecutedAction::StreamToUi { receiver_count }),
                        Err(error) => outcome.failed.push(FailedAction { action_type, error }),
                    }
                }
                ActionIntent::DropEvent { .. } => {
                    outcome.dropped = true;
                    outcome.executed.push(ExecutedAction::DropEvent);
                }
                ActionIntent::AddMetadata { key, value, .. } => {
                    match validate_metadata(key, value, self.limits) {
                        Ok(()) => {
                            context.insert_metadata(key.clone(), value.clone());
                            outcome
                                .executed
                                .push(ExecutedAction::AddMetadata { key: key.clone() });
                        }
                        Err(error) => outcome.failed.push(FailedAction { action_type, error }),
                    }
                }
                ActionIntent::ForwardToSink { .. } => {
                    outcome.skipped.push(SkippedAction::Unsupported {
                        action_type: "forward_to_sink",
                    })
                }
                ActionIntent::PublishMqttCommand { .. } => {
                    outcome.skipped.push(SkippedAction::Unsupported {
                        action_type: "publish_mqtt_command",
                    });
                }
            }
        }

        outcome
    }
}

struct EventDispatchContext<'a> {
    original: &'a NormalizedEvent,
    metadata_overlay: BTreeMap<String, String>,
}

impl<'a> EventDispatchContext<'a> {
    fn new(original: &'a NormalizedEvent) -> Self {
        Self {
            original,
            metadata_overlay: BTreeMap::new(),
        }
    }

    fn insert_metadata(&mut self, key: String, value: String) {
        self.metadata_overlay.insert(key, value);
    }

    fn enriched_event(&self) -> NormalizedEvent {
        if self.metadata_overlay.is_empty() {
            return self.original.clone();
        }

        let mut event = self.original.clone();
        event.metadata.extend(self.metadata_overlay.clone());
        event
    }
}

pub(crate) fn validate_metadata(
    key: &str,
    value: &str,
    limits: DispatchLimits,
) -> Result<(), DispatchError> {
    if key.is_empty() {
        return Err(DispatchError::InvalidMetadataKey {
            reason: "key must not be empty",
        });
    }

    if key.len() > limits.max_metadata_key_bytes {
        return Err(DispatchError::InvalidMetadataKey {
            reason: "key exceeds maximum length",
        });
    }

    if key.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(DispatchError::InvalidMetadataKey {
            reason: "key must not contain control characters",
        });
    }

    if RESERVED_METADATA_PREFIXES
        .iter()
        .any(|prefix| key.starts_with(prefix))
    {
        return Err(DispatchError::InvalidMetadataKey {
            reason: "key uses reserved prefix",
        });
    }

    if value.len() > limits.max_metadata_value_bytes {
        return Err(DispatchError::MetadataValueTooLarge {
            actual: value.len(),
            max: limits.max_metadata_value_bytes,
        });
    }

    Ok(())
}

fn intent_event_id(intent: &ActionIntent) -> Option<&EventId> {
    match intent {
        ActionIntent::StreamToUi { event_id }
        | ActionIntent::ForwardToSink { event_id, .. }
        | ActionIntent::DropEvent { event_id, .. }
        | ActionIntent::AddMetadata { event_id, .. } => Some(event_id),
        ActionIntent::PublishMqttCommand { .. } => None,
    }
}

fn action_type(intent: &ActionIntent) -> &'static str {
    match intent {
        ActionIntent::StreamToUi { .. } => "stream_to_ui",
        ActionIntent::ForwardToSink { .. } => "forward_to_sink",
        ActionIntent::PublishMqttCommand { .. } => "publish_mqtt_command",
        ActionIntent::DropEvent { .. } => "drop_event",
        ActionIntent::AddMetadata { .. } => "add_metadata",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pipe_bolt_domain::{
        ActionIntent, BrokerId, DecodedPayload, EventId, ProjectId, RouteId, TopicName,
    };
    use time::OffsetDateTime;

    use super::*;

    fn event() -> NormalizedEvent {
        NormalizedEvent {
            id: EventId::new("evt-test").unwrap(),
            correlation_id: "evt-test".to_owned(),
            project_id: ProjectId::new("project-1").unwrap(),
            broker_id: BrokerId::new("broker-1").unwrap(),
            route_id: RouteId::new("route-1").unwrap(),
            schema_mapping_id: None,
            topic: TopicName::new("devices/device-1/telemetry").unwrap(),
            device_id: Some("device-1".to_owned()),
            event_type: "telemetry".to_owned(),
            received_at: OffsetDateTime::UNIX_EPOCH,
            payload_size_bytes: 16,
            payload: DecodedPayload::Json(serde_json::json!({ "temperature": 42 })),
            fields: BTreeMap::new(),
            raw: None,
            normalization_errors: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn stream_to_ui_sends_enriched_snapshot_without_mutating_original_event() {
        let (sink, mut rx) = BoundedRealtimeEventSink::channel(1);
        let dispatcher = ActionDispatcher::new(sink);
        let event = event();

        let outcome = dispatcher.dispatch(
            &event,
            &[
                ActionIntent::AddMetadata {
                    event_id: event.id.clone(),
                    key: "severity".to_owned(),
                    value: "hot".to_owned(),
                },
                ActionIntent::StreamToUi {
                    event_id: event.id.clone(),
                },
            ],
        );

        assert!(outcome.failed.is_empty());
        assert_eq!(outcome.executed.len(), 2);
        assert!(event.metadata.is_empty());

        let streamed = rx.recv().await.unwrap();
        assert_eq!(
            streamed.metadata.get("severity").map(String::as_str),
            Some("hot")
        );
    }

    #[test]
    fn drop_event_stops_remaining_actions() {
        let (sink, _rx) = BoundedRealtimeEventSink::channel(1);
        let dispatcher = ActionDispatcher::new(sink);
        let event = event();

        let outcome = dispatcher.dispatch(
            &event,
            &[
                ActionIntent::DropEvent {
                    event_id: event.id.clone(),
                    reason: Some("filtered".to_owned()),
                },
                ActionIntent::StreamToUi {
                    event_id: event.id.clone(),
                },
            ],
        );

        assert!(outcome.dropped);
        assert_eq!(outcome.executed, vec![ExecutedAction::DropEvent]);
        assert_eq!(
            outcome.skipped,
            vec![SkippedAction::AfterDrop {
                action_type: "stream_to_ui"
            }]
        );
    }

    #[test]
    fn stream_to_ui_reports_backpressure() {
        let (sink, _rx) = BoundedRealtimeEventSink::channel(1);
        let dispatcher = ActionDispatcher::new(sink);
        let event = event();

        let first = dispatcher.dispatch(
            &event,
            &[ActionIntent::StreamToUi {
                event_id: event.id.clone(),
            }],
        );
        let second = dispatcher.dispatch(
            &event,
            &[ActionIntent::StreamToUi {
                event_id: event.id.clone(),
            }],
        );

        assert!(first.failed.is_empty());
        assert_eq!(second.failed.len(), 1);
        assert_eq!(second.failed[0].error, DispatchError::RealtimeBackpressure);
    }

    #[test]
    fn invalid_metadata_is_reported_per_action() {
        let (sink, _rx) = BoundedRealtimeEventSink::channel(1);
        let dispatcher = ActionDispatcher::new(sink);
        let event = event();

        let outcome = dispatcher.dispatch(
            &event,
            &[ActionIntent::AddMetadata {
                event_id: event.id.clone(),
                key: "pipe_bolt.internal".to_owned(),
                value: "blocked".to_owned(),
            }],
        );

        assert!(outcome.executed.is_empty());
        assert_eq!(outcome.failed.len(), 1);
        assert!(matches!(
            outcome.failed[0].error,
            DispatchError::InvalidMetadataKey { .. }
        ));
    }
}
