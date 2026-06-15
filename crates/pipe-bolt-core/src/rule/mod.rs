use std::sync::Arc;

use pipe_bolt_domain::{
    ActionIntent, ActionIntentTemplate, ConditionExpr, DecodedPayload, FieldRef, FieldValue,
    NormalizedEvent, RuleDefinition, RuleTrigger, ValueExpr,
};
use serde_json::{Number, Value};

use crate::error::{MqttEngineError, RuleError};

const DEFAULT_MAX_CONDITION_DEPTH: usize = 16;
const DEFAULT_MAX_CONDITION_NODES: usize = 256;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RuleEngineLimits {
    pub max_condition_depth: usize,
    pub max_condition_nodes: usize,
}

impl Default for RuleEngineLimits {
    fn default() -> Self {
        Self {
            max_condition_depth: DEFAULT_MAX_CONDITION_DEPTH,
            max_condition_nodes: DEFAULT_MAX_CONDITION_NODES,
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
    rule.validate()?;

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

    for action in &rule.actions {
        validate_action(rule, action)?;
    }

    if let Some(condition) = &rule.condition {
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

    Ok(())
}

fn validate_action(
    rule: &RuleDefinition,
    action: &ActionIntentTemplate,
) -> Result<(), MqttEngineError> {
    match action {
        ActionIntentTemplate::StreamToUi
        | ActionIntentTemplate::DropEvent
        | ActionIntentTemplate::AddMetadata { .. } => Ok(()),
        ActionIntentTemplate::ForwardToSink { .. } => Err(RuleError::UnsupportedAction {
            rule_id: rule.id.to_string(),
            action: "forward_to_sink",
        }
        .into()),
        ActionIntentTemplate::PublishCommand { .. } => Err(RuleError::UnsupportedAction {
            rule_id: rule.id.to_string(),
            action: "publish_command",
        }
        .into()),
    }
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
    let Some(left) = resolve_value(left, event).as_f64() else {
        return Err(RuleError::NonNumericComparison {
            rule_id: rule.id.to_string(),
        }
        .into());
    };

    let Some(right) = resolve_value(right, event).as_f64() else {
        return Err(RuleError::NonNumericComparison {
            rule_id: rule.id.to_string(),
        }
        .into());
    };

    Ok(compare(left, right))
}

fn resolve_value(value: &ValueExpr, event: &NormalizedEvent) -> Value {
    match value {
        ValueExpr::Literal { value } => value.clone(),
        ValueExpr::Field { field } => resolve_field(field, event).unwrap_or(Value::Null),
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
        ActionIntentTemplate::ForwardToSink { .. } => Err(RuleError::UnsupportedAction {
            rule_id: rule.id.to_string(),
            action: "forward_to_sink",
        }
        .into()),
        ActionIntentTemplate::PublishCommand { .. } => Err(RuleError::UnsupportedAction {
            rule_id: rule.id.to_string(),
            action: "publish_command",
        }
        .into()),
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
        ActionIntentTemplate, BrokerId, ConditionExpr, EventId, FieldPath, FieldRef, FieldValue,
        ProjectId, RouteId, RuleId, RuleTrigger, TopicName, ValueExpr,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::*;

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
    fn rejects_forward_to_sink_before_dispatcher_exists() {
        let error = RuleEngine::new(vec![rule(
            None,
            vec![ActionIntentTemplate::ForwardToSink {
                sink_id: pipe_bolt_domain::SinkId::new("sink-1").unwrap(),
            }],
        )])
        .unwrap_err();

        assert!(matches!(
            error,
            MqttEngineError::Rule(RuleError::UnsupportedAction { .. })
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
}
