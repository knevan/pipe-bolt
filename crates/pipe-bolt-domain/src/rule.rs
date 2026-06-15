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
        validate_text("rule_name", &self.name, 160)
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
