use std::sync::Arc;

use pipe_bolt_domain::{
    ActionIntent, ActionIntentTemplate, ConditionExpr, DecodedPayload, FieldRef, FieldValue,
    NormalizedEvent, RuleDefinition, RuleTrigger, ValueExpr,
};
use serde_json::{Number, Value};

use crate::action_metadata::{
    ActionMetadataLimits, MetadataValidationError, validate_action_metadata,
};
use crate::error::{MqttEngineError, RuleError};

const DEFAULT_MAX_CONDITION_DEPTH: usize = 16;
const DEFAULT_MAX_CONDITION_NODES: usize = 256;
const DEFAULT_MAX_ACTIONS_PER_RULE: usize = 16;
const DEFAULT_MAX_METADATA_KEY_BYTES: usize = 128;
const DEFAULT_MAX_METADATA_VALUE_BYTES: usize = 1024;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RuleEngineLimits {
    pub max_condition_depth: usize,
    pub max_condition_nodes: usize,
    pub max_actions_per_rule: usize,
    pub max_metadata_key_bytes: usize,
    pub max_metadata_value_bytes: usize,
}

impl RuleEngineLimits {
    fn metadata_limits(self) -> ActionMetadataLimits {
        ActionMetadataLimits::new(self.max_metadata_key_bytes, self.max_metadata_value_bytes)
    }
}

impl Default for RuleEngineLimits {
    fn default() -> Self {
        Self {
            max_condition_depth: DEFAULT_MAX_CONDITION_DEPTH,
            max_condition_nodes: DEFAULT_MAX_CONDITION_NODES,
            max_actions_per_rule: DEFAULT_MAX_ACTIONS_PER_RULE,
            max_metadata_key_bytes: DEFAULT_MAX_METADATA_KEY_BYTES,
            max_metadata_value_bytes: DEFAULT_MAX_METADATA_VALUE_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuleEngine {
    rules: Arc<Vec<CompiledRule>>,
}

impl RuleEngine {
    pub fn new(rules: Vec<RuleDefinition>) -> Result<Self, MqttEngineError> {
        Self::with_limits(rules, RuleEngineLimits::default())
    }

    pub fn with_limits(
        rules: Vec<RuleDefinition>,
        limits: RuleEngineLimits,
    ) -> Result<Self, MqttEngineError> {
        let mut compiled = Vec::with_capacity(rules.len());

        for rule in rules {
            if !rule.enabled {
                continue;
            }

            validate_rule(&rule, limits)?;
            compiled.push(CompiledRule { rule });
        }

        Ok(Self {
            rules: Arc::new(compiled),
        })
    }

    pub fn evaluate(&self, event: &NormalizedEvent) -> Result<RuleEvaluation, MqttEngineError> {
        let mut intents = Vec::new();
        let mut matched_rules = Vec::new();

        for compiled in self.rules.iter() {
            if !trigger_matches(&compiled.rule.trigger, event) {
                continue;
            }

            let matched = match &compiled.rule.condition {
                Some(condition) => evaluate_condition(&compiled.rule, condition, event)?,
                None => true,
            };

            if !matched {
                continue;
            }

            matched_rules.push(compiled.rule.id.clone());

            for action in &compiled.rule.actions {
                intents.push(action_to_intent(&compiled.rule, action, event)?);
            }
        }

        Ok(RuleEvaluation {
            matched_rules,
            intents,
        })
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleEvaluation {
    pub matched_rules: Vec<pipe_bolt_domain::RuleId>,
    pub intents: Vec<ActionIntent>,
}

#[derive(Debug, Clone)]
struct CompiledRule {
    rule: RuleDefinition,
}

fn validate_rule(rule: &RuleDefinition, limits: RuleEngineLimits) -> Result<(), MqttEngineError> {
    if matches!(rule.trigger, RuleTrigger::CommandRequested { .. }) {
        return Err(RuleError::UnsupportedTrigger {
            rule_id: rule.id.to_string(),
            trigger: "command_requested",
        }
        .into());
    }

    if rule.actions.is_empty() {
        return Err(RuleError::InvalidRule {
            rule_id: rule.id.to_string(),
            reason: "rule must define at least one action".to_owned(),
        }
        .into());
    }

    if rule.actions.len() > limits.max_actions_per_rule {
        return Err(RuleError::TooManyActions {
            rule_id: rule.id.to_string(),
            actual: rule.actions.len(),
            max: limits.max_actions_per_rule,
        }
        .into());
    }

    for action in &rule.actions {
        validate_action(rule, action, limits)?;
    }

    if let Some(condition) = &rule.condition {
        validate_condition_groups(rule, condition)?;

        let depth = condition_depth(condition);
        if depth > limits.max_condition_depth {
            return Err(RuleError::ConditionTooDeep {
                rule_id: rule.id.to_string(),
                actual: depth,
                max: limits.max_condition_depth,
            }
            .into());
        }

        let nodes = condition_nodes(condition);
        if nodes > limits.max_condition_nodes {
            return Err(RuleError::ConditionTooLarge {
                rule_id: rule.id.to_string(),
                actual: nodes,
                max: limits.max_condition_nodes,
            }
            .into());
        }
    }

    rule.validate()?;

    Ok(())
}

fn validate_action(
    rule: &RuleDefinition,
    action: &ActionIntentTemplate,
    limits: RuleEngineLimits,
) -> Result<(), MqttEngineError> {
    match action {
        ActionIntentTemplate::StreamToUi
        | ActionIntentTemplate::DropEvent
        | ActionIntentTemplate::ExecuteCommand { .. }
        | ActionIntentTemplate::ForwardToSink { .. } => Ok(()),
        ActionIntentTemplate::AddMetadata { key, value } => {
            validate_metadata_action(rule, key, value, limits)
        }
    }
}

fn validate_metadata_action(
    rule: &RuleDefinition,
    key: &str,
    value: &str,
    limits: RuleEngineLimits,
) -> Result<(), MqttEngineError> {
    validate_action_metadata(key, value, limits.metadata_limits()).map_err(
        |error| match error {
            MetadataValidationError::InvalidKey { reason } => RuleError::InvalidMetadataKey {
                rule_id: rule.id.to_string(),
                reason,
            },
            MetadataValidationError::ValueTooLarge { actual, max } => {
                RuleError::MetadataValueTooLarge {
                    rule_id: rule.id.to_string(),
                    actual,
                    max,
                }
            }
        },
    )?;

    Ok(())
}

fn validate_condition_groups(
    rule: &RuleDefinition,
    condition: &ConditionExpr,
) -> Result<(), MqttEngineError> {
    match condition {
        ConditionExpr::And { conditions } => {
            if conditions.is_empty() {
                return Err(RuleError::EmptyConditionGroup {
                    rule_id: rule.id.to_string(),
                    operator: "and",
                }
                .into());
            }

            for condition in conditions {
                validate_condition_groups(rule, condition)?;
            }
        }
        ConditionExpr::Or { conditions } => {
            if conditions.is_empty() {
                return Err(RuleError::EmptyConditionGroup {
                    rule_id: rule.id.to_string(),
                    operator: "or",
                }
                .into());
            }

            for condition in conditions {
                validate_condition_groups(rule, condition)?;
            }
        }
        ConditionExpr::Not { condition } => validate_condition_groups(rule, condition)?,
        _ => {}
    }

    Ok(())
}

fn trigger_matches(trigger: &RuleTrigger, event: &NormalizedEvent) -> bool {
    match trigger {
        RuleTrigger::EventReceived => true,
        RuleTrigger::RouteMatched { route_id } => route_id == &event.route_id,
        RuleTrigger::CommandRequested { .. } => false,
    }
}

fn evaluate_condition(
    rule: &RuleDefinition,
    condition: &ConditionExpr,
    event: &NormalizedEvent,
) -> Result<bool, MqttEngineError> {
    match condition {
        ConditionExpr::Exists { field } => Ok(resolve_field(field, event).is_some()),
        ConditionExpr::Equals { left, right } => {
            Ok(resolve_value(left, event) == resolve_value(right, event))
        }
        ConditionExpr::NotEquals { left, right } => {
            Ok(resolve_value(left, event) != resolve_value(right, event))
        }
        ConditionExpr::GreaterThan { left, right } => {
            compare_numbers(rule, left, right, event, |left, right| left > right)
        }
        ConditionExpr::GreaterThanOrEqual { left, right } => {
            compare_numbers(rule, left, right, event, |left, right| left >= right)
        }
        ConditionExpr::LessThan { left, right } => {
            compare_numbers(rule, left, right, event, |left, right| left < right)
        }
        ConditionExpr::LessThanOrEqual { left, right } => {
            compare_numbers(rule, left, right, event, |left, right| left <= right)
        }
        ConditionExpr::Contains { left, right } => {
            let left = resolve_value(left, event);
            let right = resolve_value(right, event);
            Ok(value_contains(&left, &right))
        }
        ConditionExpr::And { conditions } => {
            for condition in conditions {
                if !evaluate_condition(rule, condition, event)? {
                    return Ok(false);
                }
            }

            Ok(true)
        }
        ConditionExpr::Or { conditions } => {
            for condition in conditions {
                if evaluate_condition(rule, condition, event)? {
                    return Ok(true);
                }
            }

            Ok(false)
        }
        ConditionExpr::Not { condition } => Ok(!evaluate_condition(rule, condition, event)?),
    }
}

fn compare_numbers(
    rule: &RuleDefinition,
    left: &ValueExpr,
    right: &ValueExpr,
    event: &NormalizedEvent,
    compare: impl FnOnce(f64, f64) -> bool,
) -> Result<bool, MqttEngineError> {
    let left = resolve_optional_value(left, event);
    let right = resolve_optional_value(right, event);

    let (Some(left), Some(right)) = (left, right) else {
        return Ok(false);
    };

    let Some(left) = left.as_f64() else {
        return Err(RuleError::NonNumericComparison {
            rule_id: rule.id.to_string(),
        }
        .into());
    };

    let Some(right) = right.as_f64() else {
        return Err(RuleError::NonNumericComparison {
            rule_id: rule.id.to_string(),
        }
        .into());
    };

    Ok(compare(left, right))
}

// Keep `resolve_value` for the equality and contains operators.
fn resolve_value(value: &ValueExpr, event: &NormalizedEvent) -> Value {
    resolve_optional_value(value, event).unwrap_or(Value::Null)
}

fn resolve_optional_value(value: &ValueExpr, event: &NormalizedEvent) -> Option<Value> {
    match value {
        ValueExpr::Literal { value } => Some(value.clone()),
        ValueExpr::Field { field } => resolve_field(field, event),
    }
}

fn resolve_field(field: &FieldRef, event: &NormalizedEvent) -> Option<Value> {
    match field {
        FieldRef::Extracted { name } => event.field(name).map(field_value_to_json),
        FieldRef::DeviceId => event
            .device_id
            .as_ref()
            .map(|value| Value::String(value.clone())),
        FieldRef::EventType => Some(Value::String(event.event_type.clone())),
        FieldRef::Topic => Some(Value::String(event.topic.as_str().to_owned())),
        FieldRef::Payload { path } => match &event.payload {
            DecodedPayload::Json(value) => get_json_path(value, path.segments()).cloned(),
            DecodedPayload::Raw(_) => None,
        },
        FieldRef::Event { path } => resolve_event_path(event, path.segments()),
    }
}

fn resolve_event_path<'a, I>(event: &NormalizedEvent, segments: I) -> Option<Value>
where
    I: IntoIterator<Item = &'a str>,
{
    let segments = segments.into_iter().collect::<Vec<_>>();

    match segments.as_slice() {
        ["id"] => Some(Value::String(event.id.as_str().to_owned())),
        ["correlation_id"] => Some(Value::String(event.correlation_id.clone())),
        ["project_id"] => Some(Value::String(event.project_id.as_str().to_owned())),
        ["broker_id"] => Some(Value::String(event.broker_id.as_str().to_owned())),
        ["route_id"] => Some(Value::String(event.route_id.as_str().to_owned())),
        ["schema_mapping_id"] => event
            .schema_mapping_id
            .as_ref()
            .map(|id| Value::String(id.as_str().to_owned())),
        ["topic"] => Some(Value::String(event.topic.as_str().to_owned())),
        ["device_id"] => event
            .device_id
            .as_ref()
            .map(|value| Value::String(value.clone())),
        ["event_type"] => Some(Value::String(event.event_type.clone())),
        ["payload_size_bytes"] => {
            Some(Value::Number(Number::from(event.payload_size_bytes as u64)))
        }
        ["metadata", key] => event
            .metadata
            .get(*key)
            .map(|value| Value::String(value.clone())),
        ["fields", key] => event.field(key).map(field_value_to_json),
        _ => None,
    }
}

fn get_json_path<I>(value: &Value, segments: I) -> Option<&Value>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut current = value;

    for segment in segments {
        current = current.get(segment.as_ref())?;
    }

    Some(current)
}

fn field_value_to_json(value: &FieldValue) -> Value {
    match value {
        FieldValue::Null => Value::Null,
        FieldValue::Bool(value) => Value::Bool(*value),
        FieldValue::Number(value) => Value::Number(value.clone()),
        FieldValue::String(value) => Value::String(value.clone()),
        FieldValue::Object(value) => Value::Object(value.clone()),
        FieldValue::Array(value) => Value::Array(value.clone()),
    }
}

fn value_contains(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::String(left), Value::String(right)) => left.contains(right),
        (Value::Array(left), right) => left.iter().any(|value| value == right),
        _ => false,
    }
}

fn action_to_intent(
    rule: &RuleDefinition,
    action: &ActionIntentTemplate,
    event: &NormalizedEvent,
) -> Result<ActionIntent, MqttEngineError> {
    match action {
        ActionIntentTemplate::StreamToUi => Ok(ActionIntent::StreamToUi {
            event_id: event.id.clone(),
        }),
        ActionIntentTemplate::DropEvent => Ok(ActionIntent::DropEvent {
            event_id: event.id.clone(),
            reason: Some(format!("matched rule {}", rule.id)),
        }),
        ActionIntentTemplate::AddMetadata { key, value } => Ok(ActionIntent::AddMetadata {
            event_id: event.id.clone(),
            key: key.clone(),
            value: value.clone(),
        }),
        ActionIntentTemplate::ForwardToSink { sink_id } => Ok(ActionIntent::ForwardToSink {
            event_id: event.id.clone(),
            sink_id: sink_id.clone(),
            projection: None,
        }),
        ActionIntentTemplate::ExecuteCommand {
            command_template_id,
            params,
        } => Ok(ActionIntent::ExecuteCommand {
            event_id: event.id.clone(),
            command_template_id: command_template_id.clone(),
            params: params.clone(),
            correlation_id: event.correlation_id.clone(),
        }),
    }
}

fn condition_depth(condition: &ConditionExpr) -> usize {
    match condition {
        ConditionExpr::And { conditions } | ConditionExpr::Or { conditions } => {
            conditions.iter().map(condition_depth).max().unwrap_or(0) + 1
        }
        ConditionExpr::Not { condition } => condition_depth(condition) + 1,
        _ => 1,
    }
}

fn condition_nodes(condition: &ConditionExpr) -> usize {
    match condition {
        ConditionExpr::And { conditions } | ConditionExpr::Or { conditions } => {
            conditions.iter().map(condition_nodes).sum::<usize>() + 1
        }
        ConditionExpr::Not { condition } => condition_nodes(condition) + 1,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pipe_bolt_domain::{
        ActionIntentTemplate, BrokerId, CommandTemplateId, ConditionExpr, EventId, FieldPath,
        FieldRef, FieldValue, ProjectId, RouteId, RuleId, RuleTrigger, TopicName, ValueExpr,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::*;

    fn event() -> NormalizedEvent {
        let mut fields = BTreeMap::new();
        fields.insert(
            "temperature".to_owned(),
            FieldValue::Number(Number::from(42)),
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

    fn rule(
        condition: Option<ConditionExpr>,
        actions: Vec<ActionIntentTemplate>,
    ) -> RuleDefinition {
        RuleDefinition {
            id: RuleId::new("rule-1").unwrap(),
            name: "High temperature".to_owned(),
            enabled: true,
            trigger: RuleTrigger::EventReceived,
            condition,
            actions,
        }
    }

    #[test]
    fn rejects_empty_and_group() {
        let error = RuleEngine::new(vec![rule(
            Some(ConditionExpr::And {
                conditions: Vec::new(),
            }),
            vec![ActionIntentTemplate::StreamToUi],
        )])
        .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Rule(RuleError::EmptyConditionGroup { .. })
        ));
    }

    #[test]
    fn rejects_empty_or_group() {
        let error = RuleEngine::new(vec![rule(
            Some(ConditionExpr::Or {
                conditions: Vec::new(),
            }),
            vec![ActionIntentTemplate::StreamToUi],
        )])
        .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Rule(RuleError::EmptyConditionGroup { .. })
        ));
    }

    #[test]
    fn missing_numeric_field_evaluates_false_without_stopping_engine() {
        let engine = RuleEngine::new(vec![
            rule(
                Some(ConditionExpr::GreaterThan {
                    left: ValueExpr::Field {
                        field: FieldRef::Extracted {
                            name: "missing_temperature".to_owned(),
                        },
                    },
                    right: ValueExpr::Literal { value: json!(40) },
                }),
                vec![ActionIntentTemplate::DropEvent],
            ),
            rule(None, vec![ActionIntentTemplate::StreamToUi]),
        ])
        .unwrap();

        let evaluation = engine.evaluate(&event()).unwrap();

        assert_eq!(evaluation.intents.len(), 1);
        assert!(matches!(
            evaluation.intents[0],
            ActionIntent::StreamToUi { .. }
        ));
    }

    #[test]
    fn existing_non_numeric_field_still_returns_error_for_numeric_comparison() {
        let mut event = event();
        event.fields.insert(
            "temperature".to_owned(),
            FieldValue::String("hot".to_owned()),
        );

        let engine = RuleEngine::new(vec![rule(
            Some(ConditionExpr::GreaterThan {
                left: ValueExpr::Field {
                    field: FieldRef::Extracted {
                        name: "temperature".to_owned(),
                    },
                },
                right: ValueExpr::Literal { value: json!(40) },
            }),
            vec![ActionIntentTemplate::DropEvent],
        )])
        .unwrap();

        let error = engine.evaluate(&event).unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Rule(RuleError::NonNumericComparison { .. })
        ));
    }

    #[test]
    fn disabled_rule_is_ignored() {
        let mut disabled = rule(None, vec![ActionIntentTemplate::DropEvent]);
        disabled.enabled = false;

        let engine = RuleEngine::new(vec![disabled]).unwrap();
        let evaluation = engine.evaluate(&event()).unwrap();

        assert!(evaluation.intents.is_empty());
        assert!(evaluation.matched_rules.is_empty());
    }

    #[test]
    fn evaluates_and_or_not() {
        let engine = RuleEngine::new(vec![rule(
            Some(ConditionExpr::And {
                conditions: vec![
                    ConditionExpr::Exists {
                        field: FieldRef::DeviceId,
                    },
                    ConditionExpr::Or {
                        conditions: vec![
                            ConditionExpr::Equals {
                                left: ValueExpr::Field {
                                    field: FieldRef::EventType,
                                },
                                right: ValueExpr::Literal {
                                    value: json!("status"),
                                },
                            },
                            ConditionExpr::Not {
                                condition: Box::new(ConditionExpr::Equals {
                                    left: ValueExpr::Field {
                                        field: FieldRef::Topic,
                                    },
                                    right: ValueExpr::Literal {
                                        value: json!("blocked"),
                                    },
                                }),
                            },
                        ],
                    },
                ],
            }),
            vec![ActionIntentTemplate::StreamToUi],
        )])
        .unwrap();

        let evaluation = engine.evaluate(&event()).unwrap();

        assert_eq!(evaluation.intents.len(), 1);
    }

    #[test]
    fn condition_depth_limit_is_enforced() {
        let condition = ConditionExpr::Not {
            condition: Box::new(ConditionExpr::Not {
                condition: Box::new(ConditionExpr::Exists {
                    field: FieldRef::DeviceId,
                }),
            }),
        };

        let error = RuleEngine::with_limits(
            vec![rule(
                Some(condition),
                vec![ActionIntentTemplate::StreamToUi],
            )],
            RuleEngineLimits {
                max_condition_depth: 1,
                max_condition_nodes: 2,
                max_actions_per_rule: 16,
                max_metadata_key_bytes: 128,
                max_metadata_value_bytes: 1024,
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Rule(RuleError::ConditionTooDeep { .. })
        ));
    }

    #[test]
    fn condition_node_limit_is_enforced() {
        let condition = ConditionExpr::And {
            conditions: vec![
                ConditionExpr::Exists {
                    field: FieldRef::DeviceId,
                },
                ConditionExpr::Exists {
                    field: FieldRef::EventType,
                },
            ],
        };

        let error = RuleEngine::with_limits(
            vec![rule(
                Some(condition),
                vec![ActionIntentTemplate::StreamToUi],
            )],
            RuleEngineLimits {
                max_condition_depth: 16,
                max_condition_nodes: 2,
                max_actions_per_rule: 16,
                max_metadata_key_bytes: 128,
                max_metadata_value_bytes: 2,
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Rule(RuleError::ConditionTooLarge { .. })
        ));
    }

    #[test]
    fn evaluates_exists_condition() {
        let engine = RuleEngine::new(vec![rule(
            Some(ConditionExpr::Exists {
                field: FieldRef::Extracted {
                    name: "temperature".to_owned(),
                },
            }),
            vec![ActionIntentTemplate::StreamToUi],
        )])
        .unwrap();

        let evaluation = engine.evaluate(&event()).unwrap();

        assert_eq!(evaluation.intents.len(), 1);
        assert!(matches!(
            evaluation.intents[0],
            ActionIntent::StreamToUi { .. }
        ));
    }

    #[test]
    fn evaluates_numeric_comparison() {
        let engine = RuleEngine::new(vec![rule(
            Some(ConditionExpr::GreaterThan {
                left: ValueExpr::Field {
                    field: FieldRef::Extracted {
                        name: "temperature".to_owned(),
                    },
                },
                right: ValueExpr::Literal { value: json!(40) },
            }),
            vec![ActionIntentTemplate::AddMetadata {
                key: "severity".to_owned(),
                value: "hot".to_owned(),
            }],
        )])
        .unwrap();

        let evaluation = engine.evaluate(&event()).unwrap();

        assert_eq!(evaluation.intents.len(), 1);
        assert!(matches!(
            evaluation.intents[0],
            ActionIntent::AddMetadata { .. }
        ));
    }

    #[test]
    fn route_matched_trigger_filters_by_route_id() {
        let mut route_rule = rule(None, vec![ActionIntentTemplate::StreamToUi]);
        route_rule.trigger = RuleTrigger::RouteMatched {
            route_id: RouteId::new("other-route").unwrap(),
        };

        let engine = RuleEngine::new(vec![route_rule]).unwrap();
        let evaluation = engine.evaluate(&event()).unwrap();

        assert!(evaluation.intents.is_empty());
        assert!(evaluation.matched_rules.is_empty());
    }

    #[test]
    fn emits_forward_to_sink_intent() {
        let sink_id = pipe_bolt_domain::SinkId::new("sink-1").unwrap();
        let engine = RuleEngine::new(vec![rule(
            None,
            vec![ActionIntentTemplate::ForwardToSink {
                sink_id: sink_id.clone(),
            }],
        )])
        .unwrap();

        let evaluation = engine.evaluate(&event()).unwrap();

        assert_eq!(evaluation.intents.len(), 1);
        assert!(matches!(
            &evaluation.intents[0],
            ActionIntent::ForwardToSink { sink_id: actual, .. } if actual == &sink_id
        ));
    }

    #[test]
    fn emits_execute_command_intent_without_transport_fields() {
        let command_template_id = CommandTemplateId::new("relay-on").unwrap();
        let engine = RuleEngine::new(vec![rule(
            None,
            vec![ActionIntentTemplate::ExecuteCommand {
                command_template_id: command_template_id.clone(),
                params: BTreeMap::from([("device_id".to_owned(), json!("device-1"))]),
            }],
        )])
        .unwrap();

        let evaluation = engine.evaluate(&event()).unwrap();

        assert!(matches!(
            &evaluation.intents[0],
            ActionIntent::ExecuteCommand {
                command_template_id: actual,
                params,
                correlation_id,
                ..
            } if actual == &command_template_id
                && params.get("device_id") == Some(&json!("device-1"))
                && correlation_id == "evt-test"
        ));
    }

    #[test]
    fn resolves_payload_path() {
        let engine = RuleEngine::new(vec![rule(
            Some(ConditionExpr::Equals {
                left: ValueExpr::Field {
                    field: FieldRef::Payload {
                        path: FieldPath::new("temperature").unwrap(),
                    },
                },
                right: ValueExpr::Literal { value: json!(42) },
            }),
            vec![ActionIntentTemplate::DropEvent],
        )])
        .unwrap();

        let evaluation = engine.evaluate(&event()).unwrap();

        assert_eq!(evaluation.intents.len(), 1);
        assert!(matches!(
            evaluation.intents[0],
            ActionIntent::DropEvent { .. }
        ));
    }

    #[test]
    fn route_matched_trigger_matches_same_route_id() {
        let mut route_rule = rule(None, vec![ActionIntentTemplate::StreamToUi]);
        route_rule.trigger = RuleTrigger::RouteMatched {
            route_id: RouteId::new("route-1").unwrap(),
        };

        let engine = RuleEngine::new(vec![route_rule]).unwrap();
        let evaluation = engine.evaluate(&event()).unwrap();

        assert_eq!(evaluation.intents.len(), 1);
        assert_eq!(evaluation.matched_rules.len(), 1);
    }

    #[test]
    fn rejects_too_many_actions_per_rule() {
        let error = RuleEngine::with_limits(
            vec![rule(
                None,
                vec![
                    ActionIntentTemplate::StreamToUi,
                    ActionIntentTemplate::DropEvent,
                ],
            )],
            RuleEngineLimits {
                max_condition_depth: 16,
                max_condition_nodes: 2,
                max_actions_per_rule: 1,
                max_metadata_key_bytes: 128,
                max_metadata_value_bytes: 1024,
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Rule(RuleError::TooManyActions { .. })
        ));
    }

    #[test]
    fn rejects_invalid_add_metadata_key() {
        let error = RuleEngine::new(vec![rule(
            None,
            vec![ActionIntentTemplate::AddMetadata {
                key: "pipe_bolt.internal".to_owned(),
                value: "blocked".to_owned(),
            }],
        )])
        .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Rule(RuleError::InvalidMetadataKey { .. })
        ));
    }

    #[test]
    fn rejects_add_metadata_value_that_exceeds_limit() {
        let error = RuleEngine::with_limits(
            vec![rule(
                None,
                vec![ActionIntentTemplate::AddMetadata {
                    key: "severity".to_owned(),
                    value: "hot".to_owned(),
                }],
            )],
            RuleEngineLimits {
                max_condition_depth: 16,
                max_condition_nodes: 2,
                max_actions_per_rule: 16,
                max_metadata_key_bytes: 128,
                max_metadata_value_bytes: 2,
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Rule(RuleError::MetadataValueTooLarge { .. })
        ));
    }
}
