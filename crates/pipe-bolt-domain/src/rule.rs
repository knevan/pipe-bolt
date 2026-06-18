use serde::{Deserialize, Serialize};

use crate::action::ActionIntentTemplate;
use crate::error::DomainError;
use crate::id::{CommandTemplateId, FieldPath, RouteId, RuleId, validate_text};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuleDefinition {
    pub id: RuleId,
    pub name: String,
    pub enabled: bool,
    pub trigger: RuleTrigger,
    pub condition: Option<ConditionExpr>,
    pub actions: Vec<ActionIntentTemplate>,
}

impl RuleDefinition {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_text("rule_name", &self.name, 160)?;

        if self.actions.is_empty() {
            return Err(DomainError::InvalidFieldPath {
                reason: "rule must define at least one action",
            });
        }

        if let Some(condition) = &self.condition {
            validate_condition(condition)?;
        }

        for action in &self.actions {
            validate_action_template(action)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleTrigger {
    EventReceived,
    RouteMatched { route_id: RouteId },
    CommandRequested { template_id: CommandTemplateId },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ConditionExpr {
    Exists { field: FieldRef },
    Equals { left: ValueExpr, right: ValueExpr },
    NotEquals { left: ValueExpr, right: ValueExpr },
    GreaterThan { left: ValueExpr, right: ValueExpr },
    GreaterThanOrEqual { left: ValueExpr, right: ValueExpr },
    LessThan { left: ValueExpr, right: ValueExpr },
    LessThanOrEqual { left: ValueExpr, right: ValueExpr },
    Contains { left: ValueExpr, right: ValueExpr },
    And { conditions: Vec<ConditionExpr> },
    Or { conditions: Vec<ConditionExpr> },
    Not { condition: Box<ConditionExpr> },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ValueExpr {
    Field { field: FieldRef },
    Literal { value: serde_json::Value },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum FieldRef {
    Event { path: FieldPath },
    Payload { path: FieldPath },
    Extracted { name: String },
    DeviceId,
    EventType,
    Topic,
}

fn validate_condition(condition: &ConditionExpr) -> Result<(), DomainError> {
    match condition {
        ConditionExpr::And { conditions } | ConditionExpr::Or { conditions } => {
            if conditions.is_empty() {
                return Err(DomainError::InvalidFieldPath {
                    reason: "condition group must not be empty",
                });
            }

            for condition in conditions {
                validate_condition(condition)?;
            }
        }
        ConditionExpr::Not { condition } => validate_condition(condition)?,
        ConditionExpr::Exists { field } => validate_field_ref(field)?,
        ConditionExpr::Equals { left, right }
        | ConditionExpr::NotEquals { left, right }
        | ConditionExpr::GreaterThan { left, right }
        | ConditionExpr::GreaterThanOrEqual { left, right }
        | ConditionExpr::LessThan { left, right }
        | ConditionExpr::LessThanOrEqual { left, right }
        | ConditionExpr::Contains { left, right } => {
            validate_value_expr(left)?;
            validate_value_expr(right)?;
        }
    }

    Ok(())
}

fn validate_value_expr(value: &ValueExpr) -> Result<(), DomainError> {
    match value {
        ValueExpr::Field { field } => validate_field_ref(field),
        ValueExpr::Literal { .. } => Ok(()),
    }
}

fn validate_field_ref(field: &FieldRef) -> Result<(), DomainError> {
    if let FieldRef::Extracted { name } = field {
        validate_text("extracted_field_name", name, 160)?;
    }

    Ok(())
}

fn validate_action_template(action: &ActionIntentTemplate) -> Result<(), DomainError> {
    match action {
        ActionIntentTemplate::AddMetadata { key, value } => {
            validate_text("metadata_key", key, 128)?;
            validate_text("metadata_value", value, 1024)
        }
        ActionIntentTemplate::StreamToUi
        | ActionIntentTemplate::ForwardToSink { .. }
        | ActionIntentTemplate::PublishCommand { .. }
        | ActionIntentTemplate::DropEvent => Ok(()),
    }
}
