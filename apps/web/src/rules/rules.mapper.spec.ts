import { describe, expect, it } from 'vitest'

import type { PipeBoltDomainRuleRuleDefinition } from '@/api/generated'
import {
  createActionDraft,
  createConditionDraft,
  createRuleDraft,
  mapRuleDraft,
  ruleToDraft,
} from './rules.mapper'
import type { RuleDraftResources, RuleFormDraft } from './rules.types'
import { compileRuleDraft } from './rules.validation'

const emptyResources: RuleDraftResources = {
  commandTemplates: [],
  routes: [],
  rules: [],
  sinks: [],
}

function validDraft(): RuleFormDraft {
  const draft = createRuleDraft()
  draft.id = 'rule-temperature'
  draft.name = 'Temperature alert'
  draft.enabled = true
  return draft
}

describe('rule draft mapping', () => {
  it('maps a normalized form draft to the backend AST', () => {
    const draft = validDraft()
    draft.id = '  rule-temperature  '
    draft.name = '  Temperature alert  '
    draft.triggerType = 'route_matched'
    draft.triggerTargetId = 'route-main'
    draft.actions = [
      createActionDraft('stream_to_ui'),
      {
        ...createActionDraft('add_metadata'),
        metadataKey: 'severity',
        metadataValue: 'high',
      },
    ]
    draft.conditionEnabled = true
    draft.condition = createConditionDraft('greater_than', 'condition-root')
    draft.condition.left = {
      ...draft.condition.left,
      field: { source: 'payload', value: 'sensor.temperature' },
    }
    draft.condition.right = {
      ...draft.condition.right,
      literalKind: 'number',
      literalValue: '40',
      type: 'literal',
    }

    expect(mapRuleDraft(draft)).toEqual({
      rule: {
        actions: [
          { type: 'stream_to_ui' },
          { key: 'severity', type: 'add_metadata', value: 'high' },
        ],
        condition: {
          left: { field: { path: 'sensor.temperature', source: 'payload' }, type: 'field' },
          op: 'greater_than',
          right: { type: 'literal', value: 40 },
        },
        enabled: true,
        id: 'rule-temperature',
        name: 'Temperature alert',
        trigger: { route_id: 'route-main', type: 'route_matched' },
      },
    })
  })

  it('round-trips supported backend rule variants without semantic drift', () => {
    const rule: PipeBoltDomainRuleRuleDefinition = {
      actions: [
        { type: 'stream_to_ui' },
        { sink_id: 'sink-alerts', type: 'forward_to_sink' },
        { template_id: 'command-cooldown', type: 'publish_command' },
        { key: 'alert', type: 'add_metadata', value: 'temperature' },
        { type: 'drop_event' },
      ],
      condition: {
        conditions: [
          { field: { name: 'temperature', source: 'extracted' }, op: 'exists' },
          {
            left: { field: { source: 'event_type' }, type: 'field' },
            op: 'equals',
            right: { type: 'literal', value: 'telemetry' },
          },
        ],
        op: 'and',
      },
      enabled: true,
      id: 'rule-round-trip',
      name: 'Round trip',
      trigger: { template_id: 'command-cooldown', type: 'command_requested' },
    }

    expect(mapRuleDraft(ruleToDraft(rule))).toEqual({ rule })
  })

  it('returns a structured mapping issue for invalid JSON literals', () => {
    const draft = validDraft()
    draft.conditionEnabled = true
    draft.condition.right.literalKind = 'json'
    draft.condition.right.literalValue = '{invalid'

    expect(mapRuleDraft(draft)).toEqual({
      issue: {
        message: expect.any(String),
        path: 'condition',
      },
    })
  })

  it('omits inactive conditions from serialized rules', () => {
    const draft = validDraft()
    draft.conditionEnabled = false

    expect(mapRuleDraft(draft).rule?.condition).toBeNull()
  })
})

describe('rule draft validation', () => {
  it('accepts a valid event rule', () => {
    const draft = validDraft()

    expect(compileRuleDraft(draft, emptyResources)).toEqual({
      issues: [],
      rule: {
        actions: [{ type: 'stream_to_ui' }],
        condition: null,
        enabled: true,
        id: 'rule-temperature',
        name: 'Temperature alert',
        trigger: { type: 'event_received' },
      },
    })
  })

  it('rejects duplicate rule IDs', () => {
    const draft = validDraft()
    const existing = mapRuleDraft(draft).rule
    const resources = { ...emptyResources, rules: existing ? [existing] : [] }

    expect(compileRuleDraft(draft, resources).issues).toContainEqual({
      message: 'Rule ID must be unique.',
      path: 'id',
    })
  })

  it('rejects condition trees deeper than the authoring limit', () => {
    const draft = validDraft()
    draft.conditionEnabled = true
    let condition = createConditionDraft('equals')
    for (let depth = 0; depth < 3; depth += 1) {
      const parent = createConditionDraft('not')
      parent.children = [condition]
      condition = parent
    }
    draft.condition = condition

    expect(compileRuleDraft(draft, emptyResources).issues).toContainEqual({
      message: 'Condition nesting must not exceed depth 3.',
      path: 'condition.not.not.not',
    })
  })

  it('rejects rules above the bounded action limit', () => {
    const draft = validDraft()
    draft.actions = Array.from({ length: 17 }, () => createActionDraft())

    expect(compileRuleDraft(draft, emptyResources).issues).toContainEqual({
      message: 'Rule must not exceed 16 actions.',
      path: 'actions',
    })
  })

  it('requires enabled command targets for publish actions', () => {
    const draft = validDraft()
    draft.actions = [{ ...createActionDraft('publish_command'), targetId: 'command-missing' }]

    expect(compileRuleDraft(draft, emptyResources).issues).toContainEqual({
      message: 'Publish action requires an enabled command template.',
      path: 'actions.0.template_id',
    })
  })
})
