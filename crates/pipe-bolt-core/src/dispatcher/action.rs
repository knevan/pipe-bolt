use std::collections::BTreeMap;

use pipe_bolt_domain::{ActionIntent, EventId, NormalizedEvent};
use tokio::sync::mpsc;

use crate::action_metadata::{
    ActionMetadataLimits, MetadataValidationError, validate_action_metadata,
};
use crate::error::DispatchError;
use crate::forwarder::{EventForwarder, ForwardReceipt, ForwardRequest};

const DEFAULT_MAX_INTENTS_PER_EVENT: usize = 128;
const DEFAULT_MAX_METADATA_ENTRIES_PER_EVENT: usize = 32;
const DEFAULT_MAX_METADATA_KEY_BYTES: usize = 128;
const DEFAULT_MAX_METADATA_VALUE_BYTES: usize = 1024;

/// Runtime limits for dispatch work performed for one event.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DispatchLimits {
    pub max_intents_per_event: usize,
    pub max_metadata_entries_per_event: usize,
    pub max_metadata_key_bytes: usize,
    pub max_metadata_value_bytes: usize,
}

impl DispatchLimits {
    fn metadata_limits(self) -> ActionMetadataLimits {
        ActionMetadataLimits::new(self.max_metadata_key_bytes, self.max_metadata_value_bytes)
    }
}

impl Default for DispatchLimits {
    fn default() -> Self {
        Self {
            max_intents_per_event: DEFAULT_MAX_INTENTS_PER_EVENT,
            max_metadata_entries_per_event: DEFAULT_MAX_METADATA_ENTRIES_PER_EVENT,
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
    pub metadata_overlay: BTreeMap<String, String>,
}

impl DispatchOutcome {
    fn new(event_id: EventId) -> Self {
        Self {
            event_id,
            executed: Vec::new(),
            skipped: Vec::new(),
            failed: Vec::new(),
            dropped: false,
            metadata_overlay: BTreeMap::new(),
        }
    }

    /// Builds an enriched event snapshot by applying the dispatch metadata overlay.
    pub fn enriched_event(&self, original: &NormalizedEvent) -> NormalizedEvent {
        if self.metadata_overlay.is_empty() {
            return original.clone();
        }

        let mut event = original.clone();
        event.metadata.extend(self.metadata_overlay.clone());
        event
    }
}

/// Action that was accepted and executed by the dispatcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutedAction {
    StreamToUi { receipt: RealtimePublishReceipt },
    ForwardToSink { receipt: ForwardReceipt },
    AddMetadata { key: String },
    DropEvent,
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

/// Result returned by a bounded realtime publish boundary.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RealtimePublishReceipt {
    pub accepted: bool,
}

/// Bounded realtime event sink used by StreamToUi dispatch.
#[derive(Clone)]
pub struct BoundedRealtimeEventSink {
    tx: mpsc::Sender<NormalizedEvent>,
}

impl BoundedRealtimeEventSink {
    /// Creates a bounded realtime event sink with explicit capacity validation.
    pub fn try_channel(
        capacity: usize,
    ) -> Result<(Self, mpsc::Receiver<NormalizedEvent>), DispatchError> {
        if capacity == 0 {
            return Err(DispatchError::InvalidConfig {
                reason: "realtime sink capacity must be greater than zero",
            });
        }

        let (tx, rx) = mpsc::channel(capacity);
        Ok((Self { tx }, rx))
    }
}

/// Minimal side-effect boundary required by the dispatcher.
pub trait RealtimeEventSink {
    fn try_publish(&self, event: NormalizedEvent) -> Result<RealtimePublishReceipt, DispatchError>;
}

impl RealtimeEventSink for BoundedRealtimeEventSink {
    fn try_publish(&self, event: NormalizedEvent) -> Result<RealtimePublishReceipt, DispatchError> {
        self.tx.try_send(event).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => DispatchError::RealtimeBackpressure,
            mpsc::error::TrySendError::Closed(_) => DispatchError::RealtimeUnavailable,
        })?;

        Ok(RealtimePublishReceipt { accepted: true })
    }
}

/// Deterministic dispatcher for rule action intents.
#[derive(Clone)]
pub struct ActionDispatcher<R, F> {
    realtime: R,
    forwarder: F,
    limits: DispatchLimits,
}

impl<R, F> ActionDispatcher<R, F>
where
    R: RealtimeEventSink,
    F: EventForwarder,
{
    pub fn new(realtime: R, forwarder: F) -> Self {
        Self::with_limits(realtime, forwarder, DispatchLimits::default())
    }

    pub fn with_limits(realtime: R, forwarder: F, limits: DispatchLimits) -> Self {
        Self {
            realtime,
            forwarder,
            limits,
        }
    }

    pub fn dispatch(
        &self,
        event: &NormalizedEvent,
        intents: &[ActionIntent],
    ) -> Result<DispatchOutcome, DispatchError> {
        if intents.len() > self.limits.max_intents_per_event {
            return Err(DispatchError::TooManyIntents {
                actual: intents.len(),
                max: self.limits.max_intents_per_event,
            });
        }

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
                        Ok(receipt) => outcome
                            .executed
                            .push(ExecutedAction::StreamToUi { receipt }),
                        Err(error) => outcome.failed.push(FailedAction { action_type, error }),
                    }
                }
                ActionIntent::ForwardToSink {
                    sink_id,
                    projection,
                    ..
                } => {
                    let enriched = context.enriched_event();
                    let projection = projection
                        .as_ref()
                        .map(|projection| projection.clone().into_iter().collect());

                    match self.forwarder.try_forward(ForwardRequest {
                        event: enriched,
                        sink_id: sink_id.clone(),
                        projection,
                    }) {
                        Ok(receipt) => outcome
                            .executed
                            .push(ExecutedAction::ForwardToSink { receipt }),
                        Err(error) => outcome.failed.push(FailedAction { action_type, error }),
                    }
                }
                ActionIntent::DropEvent { .. } => {
                    outcome.dropped = true;
                    outcome.executed.push(ExecutedAction::DropEvent);
                }
                ActionIntent::AddMetadata { key, value, .. } => {
                    match context.insert_metadata(key, value, self.limits) {
                        Ok(()) => outcome
                            .executed
                            .push(ExecutedAction::AddMetadata { key: key.clone() }),
                        Err(error) => outcome.failed.push(FailedAction { action_type, error }),
                    }
                }
                ActionIntent::PublishMqttCommand { .. } => {
                    outcome.skipped.push(SkippedAction::Unsupported {
                        action_type: "publish_mqtt_command",
                    });
                }
            }
        }

        outcome.metadata_overlay = context.into_metadata_overlay();
        Ok(outcome)
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

    fn insert_metadata(
        &mut self,
        key: &str,
        value: &str,
        limits: DispatchLimits,
    ) -> Result<(), DispatchError> {
        validate_metadata(key, value, limits)?;

        if !self.metadata_overlay.contains_key(key)
            && self.metadata_overlay.len() >= limits.max_metadata_entries_per_event
        {
            return Err(DispatchError::TooManyMetadataEntries {
                actual: self.metadata_overlay.len() + 1,
                max: limits.max_metadata_entries_per_event,
            });
        }

        self.metadata_overlay
            .insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    fn enriched_event(&self) -> NormalizedEvent {
        if self.metadata_overlay.is_empty() {
            return self.original.clone();
        }

        let mut event = self.original.clone();
        event.metadata.extend(self.metadata_overlay.clone());
        event
    }

    fn into_metadata_overlay(self) -> BTreeMap<String, String> {
        self.metadata_overlay
    }
}

pub(crate) fn validate_metadata(
    key: &str,
    value: &str,
    limits: DispatchLimits,
) -> Result<(), DispatchError> {
    validate_action_metadata(key, value, limits.metadata_limits()).map_err(|error| match error {
        MetadataValidationError::InvalidKey { reason } => {
            DispatchError::InvalidMetadataKey { reason }
        }
        MetadataValidationError::ValueTooLarge { actual, max } => {
            DispatchError::MetadataValueTooLarge { actual, max }
        }
    })
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
        ActionIntent, ActionIntentTemplate, BrokerId, DecodedPayload, EventId, FieldValue,
        ProjectId, RouteId, RuleDefinition, RuleId, RuleTrigger, TopicName,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::*;
    use crate::forwarder::DisabledForwarder;
    use crate::rule::rules::RuleEngine;

    fn event() -> NormalizedEvent {
        let mut fields = BTreeMap::new();
        fields.insert(
            "temperature".to_owned(),
            FieldValue::Number(serde_json::Number::from(42)),
        );

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
            payload: DecodedPayload::Json(json!({ "temperature": 42 })),
            fields,
            raw: None,
            normalization_errors: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    fn dispatcher() -> (
        ActionDispatcher<BoundedRealtimeEventSink, DisabledForwarder>,
        mpsc::Receiver<NormalizedEvent>,
    ) {
        let (sink, rx) = BoundedRealtimeEventSink::try_channel(1).unwrap();
        (ActionDispatcher::new(sink, DisabledForwarder), rx)
    }

    #[tokio::test]
    async fn stream_to_ui_sends_enriched_snapshot_without_mutating_original_event() {
        let (dispatcher, mut rx) = dispatcher();
        let event = event();

        let outcome = dispatcher
            .dispatch(
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
            )
            .unwrap();

        assert!(outcome.failed.is_empty());
        assert_eq!(outcome.executed.len(), 2);
        assert_eq!(
            outcome.metadata_overlay.get("severity").map(String::as_str),
            Some("hot")
        );
        assert!(event.metadata.is_empty());

        let streamed = rx.recv().await.unwrap();
        assert_eq!(
            streamed.metadata.get("severity").map(String::as_str),
            Some("hot")
        );
    }

    #[test]
    fn outcome_builds_enriched_event_from_overlay() {
        let (dispatcher, _rx) = dispatcher();
        let event = event();

        let outcome = dispatcher
            .dispatch(
                &event,
                &[ActionIntent::AddMetadata {
                    event_id: event.id.clone(),
                    key: "severity".to_owned(),
                    value: "hot".to_owned(),
                }],
            )
            .unwrap();

        let enriched = outcome.enriched_event(&event);

        assert!(event.metadata.is_empty());
        assert_eq!(
            enriched.metadata.get("severity").map(String::as_str),
            Some("hot")
        );
    }

    #[test]
    fn drop_event_stops_remaining_actions() {
        let (dispatcher, _rx) = dispatcher();
        let event = event();

        let outcome = dispatcher
            .dispatch(
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
            )
            .unwrap();

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
    fn rejects_too_many_intents_before_side_effects() {
        let (sink, _rx) = BoundedRealtimeEventSink::try_channel(1).unwrap();
        let dispatcher = ActionDispatcher::with_limits(
            sink,
            DisabledForwarder,
            DispatchLimits {
                max_intents_per_event: 1,
                max_metadata_entries_per_event: 32,
                max_metadata_key_bytes: 128,
                max_metadata_value_bytes: 1024,
            },
        );
        let event = event();

        let error = dispatcher
            .dispatch(
                &event,
                &[
                    ActionIntent::StreamToUi {
                        event_id: event.id.clone(),
                    },
                    ActionIntent::DropEvent {
                        event_id: event.id.clone(),
                        reason: None,
                    },
                ],
            )
            .unwrap_err();

        assert_eq!(error, DispatchError::TooManyIntents { actual: 2, max: 1 });
    }

    #[test]
    fn reports_metadata_entry_limit_per_event() {
        let (sink, _rx) = BoundedRealtimeEventSink::try_channel(1).unwrap();
        let dispatcher = ActionDispatcher::with_limits(
            sink,
            DisabledForwarder,
            DispatchLimits {
                max_intents_per_event: 128,
                max_metadata_entries_per_event: 1,
                max_metadata_key_bytes: 128,
                max_metadata_value_bytes: 1024,
            },
        );
        let event = event();

        let outcome = dispatcher
            .dispatch(
                &event,
                &[
                    ActionIntent::AddMetadata {
                        event_id: event.id.clone(),
                        key: "severity".to_owned(),
                        value: "hot".to_owned(),
                    },
                    ActionIntent::AddMetadata {
                        event_id: event.id.clone(),
                        key: "priority".to_owned(),
                        value: "high".to_owned(),
                    },
                ],
            )
            .unwrap();

        assert_eq!(outcome.executed.len(), 1);
        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(
            outcome.failed[0].error,
            DispatchError::TooManyMetadataEntries { actual: 2, max: 1 }
        );
    }

    #[test]
    fn stream_to_ui_reports_backpressure() {
        let (dispatcher, _rx) = dispatcher();
        let event = event();

        let first = dispatcher
            .dispatch(
                &event,
                &[ActionIntent::StreamToUi {
                    event_id: event.id.clone(),
                }],
            )
            .unwrap();
        let second = dispatcher
            .dispatch(
                &event,
                &[ActionIntent::StreamToUi {
                    event_id: event.id.clone(),
                }],
            )
            .unwrap();

        assert_eq!(
            first.executed,
            vec![ExecutedAction::StreamToUi {
                receipt: RealtimePublishReceipt { accepted: true }
            }]
        );
        assert_eq!(second.failed.len(), 1);
        assert_eq!(second.failed[0].error, DispatchError::RealtimeBackpressure);
    }

    #[test]
    fn invalid_sink_capacity_is_rejected() {
        let result = BoundedRealtimeEventSink::try_channel(0);
        assert!(result.is_err());

        if let Err(error) = result {
            assert_eq!(
                error,
                DispatchError::InvalidConfig {
                    reason: "realtime sink capacity must be greater than zero"
                }
            );
        }
    }

    #[test]
    fn invalid_metadata_is_reported_per_action() {
        let (dispatcher, _rx) = dispatcher();
        let event = event();

        let outcome = dispatcher
            .dispatch(
                &event,
                &[ActionIntent::AddMetadata {
                    event_id: event.id.clone(),
                    key: "pipe_bolt.internal".to_owned(),
                    value: "blocked".to_owned(),
                }],
            )
            .unwrap();

        assert!(outcome.executed.is_empty());
        assert_eq!(outcome.failed.len(), 1);
        assert!(matches!(
            outcome.failed[0].error,
            DispatchError::InvalidMetadataKey { .. }
        ));
    }

    #[tokio::test]
    async fn rule_engine_intents_dispatch_to_ui_with_metadata_overlay() {
        let rule = RuleDefinition {
            id: RuleId::new("rule-1").unwrap(),
            name: "Stream hot telemetry".to_owned(),
            enabled: true,
            trigger: RuleTrigger::EventReceived,
            condition: None,
            actions: vec![
                ActionIntentTemplate::AddMetadata {
                    key: "severity".to_owned(),
                    value: "hot".to_owned(),
                },
                ActionIntentTemplate::StreamToUi,
            ],
        };
        let engine = RuleEngine::new(vec![rule]).unwrap();
        let event = event();
        let evaluation = engine.evaluate(&event).unwrap();
        let (dispatcher, mut rx) = dispatcher();

        let outcome = dispatcher.dispatch(&event, &evaluation.intents).unwrap();

        assert_eq!(evaluation.intents.len(), 2);
        assert!(outcome.failed.is_empty());
        assert_eq!(
            outcome.metadata_overlay.get("severity").map(String::as_str),
            Some("hot")
        );
        assert!(event.metadata.is_empty());

        let streamed = rx.recv().await.unwrap();
        assert_eq!(
            streamed.metadata.get("severity").map(String::as_str),
            Some("hot")
        );
    }

    #[test]
    fn forward_to_sink_uses_enriched_snapshot_and_reports_local_acceptance() {
        let event = event();
        let sink_id = pipe_bolt_domain::SinkId::new("sink-1").unwrap();
        let (realtime, _rx) = BoundedRealtimeEventSink::try_channel(1).unwrap();
        let forwarder = RecordingForwarder::default();
        let dispatcher = ActionDispatcher::new(realtime, forwarder.clone());

        let outcome = dispatcher
            .dispatch(
                &event,
                &[
                    ActionIntent::AddMetadata {
                        event_id: event.id.clone(),
                        key: "severity".to_owned(),
                        value: "hot".to_owned(),
                    },
                    ActionIntent::ForwardToSink {
                        event_id: event.id.clone(),
                        sink_id: sink_id.clone(),
                        projection: None,
                    },
                ],
            )
            .unwrap();

        assert!(outcome.failed.is_empty());
        assert_eq!(outcome.executed.len(), 2);
        assert_eq!(
            forwarder.recorded()[0]
                .event
                .metadata
                .get("severity")
                .map(String::as_str),
            Some("hot")
        );
        assert_eq!(
            outcome.executed[1],
            ExecutedAction::ForwardToSink {
                receipt: ForwardReceipt {
                    sink_id,
                    accepted: true
                }
            }
        );
        assert!(event.metadata.is_empty());
    }

    #[derive(Clone, Default)]
    struct RecordingForwarder {
        requests: std::sync::Arc<std::sync::Mutex<Vec<ForwardRequest>>>,
    }

    impl RecordingForwarder {
        fn recorded(&self) -> Vec<ForwardRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl EventForwarder for RecordingForwarder {
        fn try_forward(&self, request: ForwardRequest) -> Result<ForwardReceipt, DispatchError> {
            let sink_id = request.sink_id.clone();
            self.requests.lock().unwrap().push(request);

            Ok(ForwardReceipt {
                sink_id,
                accepted: true,
            })
        }
    }
}
