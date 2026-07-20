import type { PipeBoltDomainRuleRuleDefinition } from '@/api/generated'
import { mapRuleDraft } from './rules.mapper'
import type {
  RuleCompileResult,
  RuleConditionDraft,
  RuleDraftResources,
  RuleFieldDraft,
  RuleFormDraft,
  RuleValidationIssue,
  RuleValueDraft,
} from './rules.types'

const ID_PATTERN = /^[A-Za-z0-9_.:-]+$/u
const FIELD_PATH_PATTERN = /^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$/u
const MAX_ISSUES = 100
const MAX_CONDITION_DEPTH = 3
const MAX_CONDITION_NODES = 64
const MAX_ACTIONS = 16
const MAX_JSON_LITERAL_BYTES = 8_192
const textEncoder = new TextEncoder()

function addIssue(issues: RuleValidationIssue[], path: string, message: string): void {
  if (issues.length < MAX_ISSUES) issues.push({ message, path })
}

function validateText(
  issues: RuleValidationIssue[],
  path: string,
  value: string,
  maxBytes: number,
): void {
  if (!value.trim()) addIssue(issues, path, 'Value is required.')
  if (textEncoder.encode(value).byteLength > maxBytes) {
    addIssue(issues, path, `Value must not exceed ${maxBytes} UTF-8 bytes.`)
  }
  if (
    [...value].some((character) => {
      const codePoint = character.codePointAt(0) ?? 0
      return codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f)
    })
  ) {
    addIssue(issues, path, 'Value must not contain control characters.')
  }
}

function validateId(issues: RuleValidationIssue[], path: string, value: string): void {
  validateText(issues, path, value, 128)
  if (value && !ID_PATTERN.test(value)) {
    addIssue(issues, path, 'Use only letters, numbers, underscore, dash, dot, or colon.')
  }
}

function validateField(field: RuleFieldDraft, path: string, issues: RuleValidationIssue[]): void {
  if (field.source === 'event' || field.source === 'payload') {
    validateText(issues, `${path}.path`, field.value, 256)
    if (field.value && !FIELD_PATH_PATTERN.test(field.value)) {
      addIssue(issues, `${path}.path`, 'Use non-empty dot-separated field path segments.')
    }
  } else if (field.source === 'extracted') {
    validateText(issues, `${path}.name`, field.value, 160)
  }
}

function validateValue(value: RuleValueDraft, path: string, issues: RuleValidationIssue[]): void {
  if (value.type === 'field') {
    validateField(value.field, `${path}.field`, issues)
    return
  }
  if (value.literalKind === 'number') {
    const number = Number(value.literalValue)
    if (!value.literalValue.trim() || !Number.isFinite(number)) {
      addIssue(issues, path, 'Literal number must be finite.')
    } else if (Number.isInteger(number) && !Number.isSafeInteger(number)) {
      addIssue(issues, path, 'Integer literal must be safely representable.')
    }
  }
  if (
    value.literalKind === 'json' &&
    textEncoder.encode(value.literalValue).byteLength > MAX_JSON_LITERAL_BYTES
  ) {
    addIssue(issues, path, `JSON literal must not exceed ${MAX_JSON_LITERAL_BYTES} UTF-8 bytes.`)
  } else if (value.literalKind === 'json') {
    try {
      const parsed = JSON.parse(value.literalValue) as unknown
      const stack: unknown[] = [parsed]
      let nodes = 0
      while (stack.length > 0) {
        const current = stack.pop()
        nodes += 1
        if (nodes > 256) {
          addIssue(issues, path, 'JSON literal must not exceed 256 nodes.')
          break
        }
        if (typeof current === 'number') {
          if (
            !Number.isFinite(current) ||
            (Number.isInteger(current) && !Number.isSafeInteger(current))
          ) {
            addIssue(issues, path, 'JSON numbers must be finite and safely representable.')
            break
          }
        } else if (Array.isArray(current)) {
          if (nodes + stack.length + current.length > 256) {
            addIssue(issues, path, 'JSON literal must not exceed 256 nodes.')
            break
          }
          for (const item of current) stack.push(item)
        } else if (typeof current === 'object' && current !== null) {
          const values = Object.values(current)
          if (nodes + stack.length + values.length > 256) {
            addIssue(issues, path, 'JSON literal must not exceed 256 nodes.')
            break
          }
          for (const item of values) stack.push(item)
        }
      }
    } catch {
      addIssue(issues, path, 'Literal must contain valid JSON.')
    }
  }
}

function validateCondition(root: RuleConditionDraft, issues: RuleValidationIssue[]): void {
  const stack = [{ condition: root, depth: 1, path: 'condition' }]
  let nodes = 0
  while (stack.length > 0) {
    const current = stack.pop()
    if (!current) continue
    nodes += 1
    if (nodes > MAX_CONDITION_NODES) {
      addIssue(issues, 'condition', `Condition must not exceed ${MAX_CONDITION_NODES} nodes.`)
      return
    }
    if (current.depth > MAX_CONDITION_DEPTH) {
      addIssue(
        issues,
        current.path,
        `Condition nesting must not exceed depth ${MAX_CONDITION_DEPTH}.`,
      )
    }

    const { condition, path } = current
    if (condition.op === 'exists') {
      validateField(condition.field, `${path}.field`, issues)
    } else if (condition.op === 'and' || condition.op === 'or') {
      if (!condition.children.length) addIssue(issues, path, 'Condition group must not be empty.')
      for (const [index, child] of condition.children.entries()) {
        stack.push({ condition: child, depth: current.depth + 1, path: `${path}.${index}` })
      }
    } else if (condition.op === 'not') {
      if (condition.children.length !== 1) {
        addIssue(issues, path, 'Not condition must contain exactly one child.')
      }
      const child = condition.children[0]
      if (child) stack.push({ condition: child, depth: current.depth + 1, path: `${path}.not` })
    } else {
      validateValue(condition.left, `${path}.left`, issues)
      validateValue(condition.right, `${path}.right`, issues)
      if (
        ['greater_than', 'greater_than_or_equal', 'less_than', 'less_than_or_equal'].includes(
          condition.op,
        )
      ) {
        for (const [side, value] of [
          ['left', condition.left],
          ['right', condition.right],
        ] as const) {
          if (value.type === 'literal' && value.literalKind !== 'number') {
            addIssue(issues, `${path}.${side}`, 'Numeric comparison literals must be numbers.')
          }
        }
      }
      if (
        condition.op === 'contains' &&
        condition.right.type === 'literal' &&
        condition.right.literalKind !== 'string'
      ) {
        addIssue(issues, `${path}.right`, 'Contains operand must use a string literal.')
      }
    }
  }
}

function validateReferences(
  rule: PipeBoltDomainRuleRuleDefinition,
  resources: RuleDraftResources,
  issues: RuleValidationIssue[],
): void {
  if (rule.trigger.type === 'route_matched') {
    const routeId = rule.trigger.route_id
    if (!resources.routes.some((route) => route.id === routeId)) {
      addIssue(issues, 'trigger.route_id', 'Referenced route does not exist.')
    }
  }
  if (rule.trigger.type === 'command_requested') {
    const templateId = rule.trigger.template_id
    if (!resources.commandTemplates.some((template) => template.id === templateId)) {
      addIssue(issues, 'trigger.template_id', 'Referenced command template does not exist.')
    }
  }

  for (const [index, action] of rule.actions.entries()) {
    if (action.type === 'forward_to_sink') {
      const sink = resources.sinks.find((candidate) => candidate.id === action.sink_id)
      if (!sink || !sink.enabled || sink.kind.type !== 'webhook') {
        addIssue(
          issues,
          `actions.${index}.sink_id`,
          'Forward action requires an enabled webhook sink.',
        )
      }
    } else if (action.type === 'publish_command') {
      const template = resources.commandTemplates.find(
        (candidate) => candidate.id === action.template_id,
      )
      if (!template || !template.enabled) {
        addIssue(
          issues,
          `actions.${index}.template_id`,
          'Publish action requires an enabled command template.',
        )
      }
    }
  }
}

export function compileRuleDraft(
  draft: RuleFormDraft,
  resources: RuleDraftResources,
  originalRuleId?: string,
): RuleCompileResult {
  const issues: RuleValidationIssue[] = []
  validateId(issues, 'id', draft.id)
  validateText(issues, 'name', draft.name, 160)
  if (resources.rules.some((rule) => rule.id === draft.id.trim() && rule.id !== originalRuleId)) {
    addIssue(issues, 'id', 'Rule ID must be unique.')
  }
  if (draft.triggerType !== 'event_received' && !draft.triggerTargetId) {
    addIssue(issues, 'trigger', 'Trigger target is required.')
  }
  if (!draft.actions.length) addIssue(issues, 'actions', 'Rule must define at least one action.')
  if (draft.actions.length > MAX_ACTIONS) {
    addIssue(issues, 'actions', `Rule must not exceed ${MAX_ACTIONS} actions.`)
  }
  for (const [index, action] of draft.actions.entries()) {
    if (action.type === 'forward_to_sink' || action.type === 'publish_command') {
      if (!action.targetId) addIssue(issues, `actions.${index}`, 'Action target is required.')
    } else if (action.type === 'add_metadata') {
      validateText(issues, `actions.${index}.key`, action.metadataKey, 128)
      if (
        action.metadataKey.startsWith('pipe_bolt.') ||
        action.metadataKey.startsWith('_pipe_bolt.')
      ) {
        addIssue(issues, `actions.${index}.key`, 'Metadata key uses a reserved prefix.')
      }
      validateText(issues, `actions.${index}.value`, action.metadataValue, 1_024)
    }
  }
  if (draft.conditionEnabled) validateCondition(draft.condition, issues)

  const mapped = mapRuleDraft(draft)
  if (mapped.issue) addIssue(issues, mapped.issue.path, mapped.issue.message)
  if (mapped.rule) validateReferences(mapped.rule, resources, issues)
  return { issues, rule: mapped.rule }
}
