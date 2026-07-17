import { computed, shallowRef } from 'vue'
import { defineStore } from 'pinia'

import type {
  PipeBoltApiDtoProjectConfigDocumentV1,
  PipeBoltApiDtoProjectConfigResponse,
  PipeBoltApiDtoProjectConfigWriteResponse,
} from '@/api/generated'
import type { ConfigValidationIssue } from './config.validation'
import { redactConfigSecrets } from './composables/useSecretMasking'

export type ConfigSection =
  | keyof Pick<
      PipeBoltApiDtoProjectConfigDocumentV1,
      'brokers' | 'command_templates' | 'routes' | 'schema_mappings' | 'sinks'
    >
  | 'general'
  | 'raw'

function cloneConfig(
  config: PipeBoltApiDtoProjectConfigDocumentV1,
): PipeBoltApiDtoProjectConfigDocumentV1 {
  return structuredClone(config)
}

export const useConfigStore = defineStore('config', () => {
  const projectId = shallowRef<string>()
  const version = shallowRef<number>()
  const schemaVersion = shallowRef<number>()
  const baseline = shallowRef<PipeBoltApiDtoProjectConfigDocumentV1>()
  const draft = shallowRef<PipeBoltApiDtoProjectConfigDocumentV1>()
  const dirtySections = shallowRef<ReadonlySet<ConfigSection>>(new Set())
  const validationIssues = shallowRef<ReadonlyArray<ConfigValidationIssue>>([])
  const validationCurrent = shallowRef(false)
  const reloadRequired = shallowRef(false)

  const isDirty = computed(() => dirtySections.value.size > 0)
  const isLoaded = computed(() => Boolean(draft.value && version.value !== undefined))

  function hydrate(response: PipeBoltApiDtoProjectConfigResponse, force = false): boolean {
    const incomingProjectId = response.config.project_id
    if (!force && projectId.value === incomingProjectId && isDirty.value) return false
    if (
      !force &&
      projectId.value === incomingProjectId &&
      version.value !== undefined &&
      response.version < version.value
    )
      return false

    projectId.value = incomingProjectId
    version.value = response.version
    schemaVersion.value = response.schema_version
    baseline.value = cloneConfig(response.config)
    draft.value = cloneConfig(response.config)
    dirtySections.value = new Set()
    validationIssues.value = []
    validationCurrent.value = false
    return true
  }

  function markDirty(section: ConfigSection): void {
    dirtySections.value = new Set([...dirtySections.value, section])
    validationCurrent.value = false
    validationIssues.value = []
  }

  function updateGeneral(
    values: Pick<PipeBoltApiDtoProjectConfigDocumentV1, 'description' | 'enabled' | 'name'>,
  ): void {
    if (!draft.value) return
    draft.value = { ...draft.value, ...values }
    markDirty('general')
  }

  function updateSection<K extends Exclude<ConfigSection, 'general' | 'raw'>>(
    section: K,
    value: PipeBoltApiDtoProjectConfigDocumentV1[K],
  ): void {
    if (!draft.value) return
    draft.value = { ...draft.value, [section]: structuredClone(value) }
    markDirty(section)
  }

  function replaceDraft(value: PipeBoltApiDtoProjectConfigDocumentV1): void {
    draft.value = cloneConfig(value)
    markDirty('raw')
  }

  function discardChanges(): void {
    if (!baseline.value) return
    draft.value = cloneConfig(baseline.value)
    dirtySections.value = new Set()
    validationIssues.value = []
    validationCurrent.value = false
  }

  function setValidation(issues: ReadonlyArray<ConfigValidationIssue>): void {
    validationIssues.value = issues
    validationCurrent.value = true
  }

  function markSaved(
    response: PipeBoltApiDtoProjectConfigWriteResponse,
    submittedConfig: PipeBoltApiDtoProjectConfigDocumentV1,
  ): void {
    const sanitized = redactConfigSecrets(submittedConfig)
    baseline.value = cloneConfig(sanitized)
    draft.value = cloneConfig(sanitized)
    version.value = response.version
    dirtySections.value = new Set()
    validationIssues.value = []
    validationCurrent.value = false
    reloadRequired.value = response.reload_required
  }

  function markReloaded(): void {
    reloadRequired.value = false
  }

  function clear(): void {
    projectId.value = undefined
    version.value = undefined
    schemaVersion.value = undefined
    baseline.value = undefined
    draft.value = undefined
    dirtySections.value = new Set()
    validationIssues.value = []
    validationCurrent.value = false
    reloadRequired.value = false
  }

  return {
    baseline,
    clear,
    dirtySections,
    discardChanges,
    draft,
    hydrate,
    isDirty,
    isLoaded,
    markReloaded,
    markSaved,
    projectId,
    reloadRequired,
    replaceDraft,
    schemaVersion,
    setValidation,
    updateGeneral,
    updateSection,
    validationCurrent,
    validationIssues,
    version,
  }
})
