import { computed, shallowRef, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useQuery, useQueryCache } from '@pinia/colada'

import { ApiError, getApiErrorMessage, toApiError } from '@/api/errors'
import type { PipeBoltApiDtoProjectConfigDocumentV1 } from '@/api/generated'
import { useProjectStore } from '@/projects'
import { CONFIG_QUERY_KEYS, fetchProjectConfig, saveProjectConfig } from '../config.api'
import { useConfigStore } from '../config.store'
import { validateConfigDocument } from '../config.validation'
import { restoreConfigSecrets } from './useSecretMasking'

export function useConfigDraft() {
  const projects = useProjectStore()
  const config = useConfigStore()
  const queryCache = useQueryCache()
  const { activeProjectId } = storeToRefs(projects)
  const configState = storeToRefs(config)
  const saveReason = shallowRef('')
  const successMessage = shallowRef<string>()
  const reloadLatestError = shallowRef<string>()
  const isSaving = shallowRef(false)
  const saveError = shallowRef<ApiError>()

  const query = useQuery({
    key: () => CONFIG_QUERY_KEYS.byProject(activeProjectId.value ?? ''),
    enabled: () => Boolean(activeProjectId.value),
    query: ({ signal }) => fetchProjectConfig(activeProjectId.value ?? '', signal),
    staleTime: 10_000,
  })
  watch(activeProjectId, (current, previous) => {
    if (current !== previous) {
      config.clear()
      saveError.value = undefined
      successMessage.value = undefined
    }
  })
  watch(
    query.data,
    (response) => {
      if (response && response.config.project_id === activeProjectId.value) config.hydrate(response)
    },
    { immediate: true },
  )

  const loadError = computed(() =>
    query.error.value ? getApiErrorMessage(query.error.value) : undefined,
  )
  const isConflict = computed(() => saveError.value?.kind === 'conflict')

  function validate(): boolean {
    if (!config.draft || !activeProjectId.value) return false
    let candidate: PipeBoltApiDtoProjectConfigDocumentV1
    try {
      candidate = restoreConfigSecrets(config.draft, config.baseline)
    } catch (error) {
      config.setValidation([
        {
          message: error instanceof Error ? error.message : 'Masked secret could not be retained.',
          path: 'secrets',
        },
      ])
      return false
    }
    const issues = validateConfigDocument(candidate, activeProjectId.value, config.baseline)
    config.setValidation(issues)
    return issues.length === 0
  }

  async function save(): Promise<boolean> {
    if (isSaving.value) return false
    successMessage.value = undefined
    saveError.value = undefined
    if (!config.draft || config.version === undefined || !activeProjectId.value || !validate())
      return false

    const projectId = activeProjectId.value
    const expectedVersion = config.version
    const candidate = restoreConfigSecrets(config.draft, config.baseline)
    isSaving.value = true
    try {
      const response = await saveProjectConfig(projectId, {
        config: candidate,
        expected_version: expectedVersion,
        reason: saveReason.value.trim() || undefined,
      })
      if (
        activeProjectId.value === projectId &&
        config.projectId === projectId &&
        config.version === expectedVersion
      ) {
        config.markSaved(response, candidate)
        saveReason.value = ''
        successMessage.value = `Configuration version ${response.version} saved.`
      }
      await queryCache.invalidateQueries({
        key: CONFIG_QUERY_KEYS.byProject(projectId),
        exact: true,
      })
      return true
    } catch (error) {
      if (activeProjectId.value === projectId) saveError.value = toApiError(error)
      return false
    } finally {
      isSaving.value = false
    }
  }

  async function reloadLatest(): Promise<void> {
    if (!activeProjectId.value) return
    saveError.value = undefined
    reloadLatestError.value = undefined
    const projectId = activeProjectId.value
    try {
      const response = await fetchProjectConfig(projectId)
      queryCache.setQueryData(CONFIG_QUERY_KEYS.byProject(projectId), response)
      if (activeProjectId.value === projectId) {
        config.hydrate(response, true)
        successMessage.value = `Loaded latest configuration version ${response.version}.`
      }
    } catch (error) {
      if (activeProjectId.value === projectId) reloadLatestError.value = getApiErrorMessage(error)
    }
  }

  function dismissMessages(): void {
    saveError.value = undefined
    successMessage.value = undefined
  }

  return {
    ...configState,
    dismissMessages,
    discardChanges: config.discardChanges,
    clear: config.clear,
    isConflict,
    isLoading: query.isLoading,
    isSaving,
    loadError,
    refetch: query.refetch,
    reloadLatest,
    reloadLatestError,
    replaceDraft: config.replaceDraft,
    save,
    saveError,
    saveReason,
    markReloaded: config.markReloaded,
    successMessage,
    updateGeneral: config.updateGeneral,
    updateSection: config.updateSection,
    validate,
  }
}
