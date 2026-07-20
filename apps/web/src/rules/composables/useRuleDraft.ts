import { computed, shallowRef, toValue, watch, type MaybeRefOrGetter } from 'vue'

import type { PipeBoltDomainRuleRuleDefinition } from '@/api/generated'
import { compileRuleDraft } from '../rules.validation'
import { createRuleDraft, ruleToDraft } from '../rules.mapper'
import type { RuleDraftResources, RuleFormDraft } from '../rules.types'

interface UseRuleDraftOptions {
  identity: MaybeRefOrGetter<string>
  isNew: MaybeRefOrGetter<boolean>
  originalRule: MaybeRefOrGetter<PipeBoltDomainRuleRuleDefinition | undefined>
  resources: MaybeRefOrGetter<RuleDraftResources>
}

export function useRuleDraft({ identity, isNew, originalRule, resources }: UseRuleDraftOptions) {
  const draft = shallowRef<RuleFormDraft>()
  const baseline = shallowRef('')
  const initializationError = shallowRef<string>()
  let hydratedIdentity: string | undefined

  watch(
    [() => toValue(identity), () => toValue(originalRule)],
    ([currentIdentity, source]) => {
      if (hydratedIdentity === currentIdentity) return
      const creating = toValue(isNew)
      if (!creating && !source) return
      try {
        draft.value = source ? ruleToDraft(source) : createRuleDraft()
        baseline.value = source ? JSON.stringify(source) : ''
        initializationError.value = undefined
        hydratedIdentity = currentIdentity
      } catch (error) {
        initializationError.value =
          error instanceof Error ? error.message : 'Stored rule could not be opened.'
      }
    },
    { immediate: true },
  )

  const compiled = computed(() => {
    if (!draft.value) return { issues: [] }
    const originalId = toValue(originalRule)?.id
    return compileRuleDraft(draft.value, toValue(resources), originalId)
  })
  const serializedRule = computed(() =>
    compiled.value.rule ? JSON.stringify(compiled.value.rule, null, 2) : undefined,
  )
  const isDirty = computed(() => {
    if (!draft.value) return false
    if (!compiled.value.rule) return true
    return JSON.stringify(compiled.value.rule) !== baseline.value
  })

  function update(value: RuleFormDraft): void {
    draft.value = value
  }

  function acceptSaved(rule: PipeBoltDomainRuleRuleDefinition): void {
    baseline.value = JSON.stringify(rule)
  }

  function reset(): void {
    const source = toValue(originalRule)
    draft.value = source ? ruleToDraft(source) : createRuleDraft()
    baseline.value = source ? JSON.stringify(source) : ''
  }

  function clear(): void {
    draft.value = undefined
    baseline.value = ''
    hydratedIdentity = undefined
  }

  return {
    acceptSaved,
    clear,
    compiled,
    draft,
    initializationError,
    isDirty,
    reset,
    serializedRule,
    update,
  }
}
