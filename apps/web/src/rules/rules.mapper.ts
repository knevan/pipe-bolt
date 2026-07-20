import type {
  PipeBoltDomainActionActionIntentTemplate,
  PipeBoltDomainRuleConditionExpr,
  PipeBoltDomainRuleFieldRef,
  PipeBoltDomainRuleRuleDefinition,
  PipeBoltDomainRuleRuleTrigger,
  PipeBoltDomainRuleValueExpr,
} from '@/api/generated'
import type {
  RuleActionDraft,
  RuleActionType,
  RuleConditionDraft,
  RuleConditionOperator,
  RuleFieldDraft,
  RuleFormDraft,
  RuleValueDraft,
  RuleValidationIssue,
} from './rules.types'

const MAX_IMPORTED_CONDITION_DEPTH = 16
const MAX_IMPORTED_CONDITION_NODES = 256
const MAX_IMPORTED_ACTIONS = 16
const MAX_IMPORTED_JSON_LITERAL_BYTES = 8_192
const textEncoder = new TextEncoder()

function draftKey(): string {
  return crypto.randomUUID()
}

export function createFieldDraft(): RuleFieldDraft {
  return { source: 'extracted', value: '' }
}

export function createValueDraft(): RuleValueDraft {
  return {
    field: createFieldDraft(),
    literalKind: 'string',
    literalValue: '',
    type: 'field',
  }
}

export function createConditionDraft(
  op: RuleConditionOperator = 'equals',
  key = draftKey(),
): RuleConditionDraft {
  const children = op === 'not' ? [createConditionDraft()] : []
  return {
    children,
    field: createFieldDraft(),
    key,
    left: createValueDraft(),
    op,
    right: { ...createValueDraft(), type: 'literal' },
  }
}

export function createActionDraft(type: RuleActionType = 'stream_to_ui'): RuleActionDraft {
  return {
    key: draftKey(),
    metadataKey: '',
    metadataValue: '',
    targetId: '',
    type,
  }
}

export function createRuleDraft(): RuleFormDraft {
  return {
    actions: [createActionDraft()],
    condition: createConditionDraft(),
    conditionEnabled: false,
    enabled: false,
    id: `rule-${crypto.randomUUID()}`,
    name: 'New rule',
    triggerTargetId: '',
    triggerType: 'event_received',
  }
}

function fieldToDraft(field: PipeBoltDomainRuleFieldRef): RuleFieldDraft {
  switch (field.source) {
    case 'event':
    case 'payload':
      return { source: field.source, value: field.path }
    case 'extracted':
      return { source: field.source, value: field.name }
    default:
      return { source: field.source, value: '' }
  }
}

function literalToDraft(value: unknown): Pick<RuleValueDraft, 'literalKind' | 'literalValue'> {
  if (value === null) return { literalKind: 'null', literalValue: '' }
  if (typeof value === 'string') return { literalKind: 'string', literalValue: value }
  if (typeof value === 'number') return { literalKind: 'number', literalValue: String(value) }
  if (typeof value === 'boolean') {
    return { literalKind: 'boolean', literalValue: String(value) }
  }
  const literalValue = JSON.stringify(value, null, 2) ?? 'null'
  if (textEncoder.encode(literalValue).byteLength > MAX_IMPORTED_JSON_LITERAL_BYTES) {
    throw new Error('Stored JSON literal exceeds the 8192-byte UI limit.')
  }
  return { literalKind: 'json', literalValue }
}

function valueToDraft(value: PipeBoltDomainRuleValueExpr): RuleValueDraft {
  if (value.type === 'field') return { ...createValueDraft(), field: fieldToDraft(value.field) }
  return { ...createValueDraft(), ...literalToDraft(value.value), type: 'literal' }
}

function conditionToDraft(
  condition: PipeBoltDomainRuleConditionExpr,
  depth: number,
  nodeCount: { value: number },
): RuleConditionDraft {
  nodeCount.value += 1
  if (depth > MAX_IMPORTED_CONDITION_DEPTH || nodeCount.value > MAX_IMPORTED_CONDITION_NODES) {
    throw new Error('Stored condition exceeds supported rule-engine limits.')
  }

  const draft = createConditionDraft(condition.op)
  switch (condition.op) {
    case 'exists':
      draft.field = fieldToDraft(condition.field)
      break
    case 'and':
    case 'or':
      draft.children = condition.conditions.map((child) =>
        conditionToDraft(child, depth + 1, nodeCount),
      )
      break
    case 'not':
      draft.children = [conditionToDraft(condition.condition, depth + 1, nodeCount)]
      break
    default:
      draft.left = valueToDraft(condition.left)
      draft.right = valueToDraft(condition.right)
  }
  return draft
}

function actionToDraft(action: PipeBoltDomainActionActionIntentTemplate): RuleActionDraft {
  const draft = createActionDraft(action.type)
  switch (action.type) {
    case 'forward_to_sink':
      draft.targetId = action.sink_id
      break
    case 'publish_command':
      draft.targetId = action.template_id
      break
    case 'add_metadata':
      draft.metadataKey = action.key
      draft.metadataValue = action.value
      break
  }
  return draft
}

export function ruleToDraft(rule: PipeBoltDomainRuleRuleDefinition): RuleFormDraft {
  if (rule.actions.length > MAX_IMPORTED_ACTIONS) {
    throw new Error(`Stored rule exceeds the ${MAX_IMPORTED_ACTIONS}-action runtime limit.`)
  }
  let triggerTargetId = ''
  if (rule.trigger.type === 'route_matched') triggerTargetId = rule.trigger.route_id
  if (rule.trigger.type === 'command_requested') triggerTargetId = rule.trigger.template_id
  return {
    actions: rule.actions.map(actionToDraft),
    condition: rule.condition
      ? conditionToDraft(rule.condition, 1, { value: 0 })
      : createConditionDraft(),
    conditionEnabled: Boolean(rule.condition),
    enabled: rule.enabled,
    id: rule.id,
    name: rule.name,
    triggerTargetId,
    triggerType: rule.trigger.type,
  }
}

function fieldFromDraft(field: RuleFieldDraft): PipeBoltDomainRuleFieldRef {
  switch (field.source) {
    case 'event':
    case 'payload':
      return { path: field.value.trim(), source: field.source }
    case 'extracted':
      return { name: field.value.trim(), source: field.source }
    default:
      return { source: field.source }
  }
}

function literalFromDraft(value: RuleValueDraft): unknown {
  switch (value.literalKind) {
    case 'number': {
      const parsed = Number(value.literalValue)
      if (!value.literalValue.trim() || !Number.isFinite(parsed)) {
        throw new Error('Literal number must be finite.')
      }
      return parsed
    }
    case 'boolean':
      return value.literalValue === 'true'
    case 'null':
      return null
    case 'json':
      return JSON.parse(value.literalValue) as unknown
    default:
      return value.literalValue
  }
}

function valueFromDraft(value: RuleValueDraft): PipeBoltDomainRuleValueExpr {
  return value.type === 'field'
    ? { field: fieldFromDraft(value.field), type: 'field' }
    : { type: 'literal', value: literalFromDraft(value) }
}

function conditionFromDraft(condition: RuleConditionDraft): PipeBoltDomainRuleConditionExpr {
  switch (condition.op) {
    case 'exists':
      return { field: fieldFromDraft(condition.field), op: condition.op }
    case 'and':
    case 'or':
      return { conditions: condition.children.map(conditionFromDraft), op: condition.op }
    case 'not':
      if (!condition.children[0]) throw new Error('Not condition requires one child.')
      return {
        condition: conditionFromDraft(condition.children[0]),
        op: condition.op,
      }
    default:
      return {
        left: valueFromDraft(condition.left),
        op: condition.op,
        right: valueFromDraft(condition.right),
      }
  }
}

function actionFromDraft(action: RuleActionDraft): PipeBoltDomainActionActionIntentTemplate {
  switch (action.type) {
    case 'forward_to_sink':
      return { sink_id: action.targetId, type: action.type }
    case 'publish_command':
      return { template_id: action.targetId, type: action.type }
    case 'add_metadata':
      return { key: action.metadataKey, type: action.type, value: action.metadataValue }
    default:
      return { type: action.type }
  }
}

export function mapRuleDraft(draft: RuleFormDraft): {
  issue?: RuleValidationIssue
  rule?: PipeBoltDomainRuleRuleDefinition
} {
  try {
    const trigger: PipeBoltDomainRuleRuleTrigger =
      draft.triggerType === 'route_matched'
        ? { route_id: draft.triggerTargetId, type: 'route_matched' }
        : draft.triggerType === 'command_requested'
          ? { template_id: draft.triggerTargetId, type: 'command_requested' }
          : { type: 'event_received' }
    return {
      rule: {
        actions: draft.actions.map(actionFromDraft),
        condition: draft.conditionEnabled ? conditionFromDraft(draft.condition) : null,
        enabled: draft.enabled,
        id: draft.id.trim(),
        name: draft.name.trim(),
        trigger,
      },
    }
  } catch (error) {
    return {
      issue: {
        message: error instanceof Error ? error.message : 'Rule draft could not be serialized.',
        path: 'condition',
      },
    }
  }
}
