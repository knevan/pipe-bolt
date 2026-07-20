import type {
  PipeBoltDomainConfigCommandTemplate,
  PipeBoltDomainConfigSinkDefinition,
  PipeBoltDomainConfigTopicRouteConfig,
  PipeBoltDomainRuleRuleDefinition,
} from '@/api/generated'

export type RuleTriggerType = 'event_received' | 'route_matched' | 'command_requested'
export type RuleActionType =
  | 'stream_to_ui'
  | 'forward_to_sink'
  | 'publish_command'
  | 'drop_event'
  | 'add_metadata'
export type RuleConditionOperator =
  | 'exists'
  | 'equals'
  | 'not_equals'
  | 'greater_than'
  | 'greater_than_or_equal'
  | 'less_than'
  | 'less_than_or_equal'
  | 'contains'
  | 'and'
  | 'or'
  | 'not'
export type RuleFieldSource =
  | 'event'
  | 'payload'
  | 'extracted'
  | 'device_id'
  | 'event_type'
  | 'topic'
export type RuleLiteralKind = 'string' | 'number' | 'boolean' | 'null' | 'json'

export interface RuleFieldDraft {
  source: RuleFieldSource
  value: string
}

export interface RuleValueDraft {
  field: RuleFieldDraft
  literalKind: RuleLiteralKind
  literalValue: string
  type: 'field' | 'literal'
}

export interface RuleConditionDraft {
  children: RuleConditionDraft[]
  field: RuleFieldDraft
  key: string
  left: RuleValueDraft
  op: RuleConditionOperator
  right: RuleValueDraft
}

export interface RuleActionDraft {
  key: string
  metadataKey: string
  metadataValue: string
  targetId: string
  type: RuleActionType
}

export interface RuleFormDraft {
  actions: RuleActionDraft[]
  condition: RuleConditionDraft
  conditionEnabled: boolean
  enabled: boolean
  id: string
  name: string
  triggerTargetId: string
  triggerType: RuleTriggerType
}

export interface RuleDraftResources {
  commandTemplates: ReadonlyArray<PipeBoltDomainConfigCommandTemplate>
  routes: ReadonlyArray<PipeBoltDomainConfigTopicRouteConfig>
  rules: ReadonlyArray<PipeBoltDomainRuleRuleDefinition>
  sinks: ReadonlyArray<PipeBoltDomainConfigSinkDefinition>
}

export interface RuleValidationIssue {
  message: string
  path: string
}

export interface RuleCompileResult {
  issues: ReadonlyArray<RuleValidationIssue>
  rule?: PipeBoltDomainRuleRuleDefinition
}
