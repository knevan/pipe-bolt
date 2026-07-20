import {
  computed,
  onScopeDispose,
  readonly,
  shallowRef,
  toValue,
  watch,
  type MaybeRefOrGetter,
} from 'vue'
import { useQuery, useQueryCache } from '@pinia/colada'

import type {
  PipeBoltApiDtoProjectConfigDocumentV1,
  PipeBoltApiDtoProjectConfigWriteResponse,
  PipeBoltDomainRuleRuleDefinition,
} from '@/api/generated'
import { ApiError, getApiErrorMessage, toApiError } from '@/api/errors'
import { fetchRuleConfig, RULE_CONFIG_QUERY_KEYS, saveRuleConfig } from '../rules.api'

const textEncoder = new TextEncoder()

export function useRulesConfig(projectId: MaybeRefOrGetter<string | undefined>) {
  const queryCache = useQueryCache()
  const saveError = shallowRef<ApiError>()
  const isSaving = shallowRef(false)
  const reloadRequired = shallowRef(false)
  const successMessage = shallowRef<string>()
  let saveController: AbortController | undefined

  const query = useQuery({
    key: () => RULE_CONFIG_QUERY_KEYS.byProject(toValue(projectId) ?? ''),
    enabled: () => Boolean(toValue(projectId)),
    query: ({ signal }) => fetchRuleConfig(toValue(projectId) ?? '', signal),
    staleTime: 5_000,
  })
  const loadError = computed(() =>
    query.error.value ? getApiErrorMessage(query.error.value) : undefined,
  )
  const isConflict = computed(() => saveError.value?.kind === 'conflict')

  watch(
    () => toValue(projectId),
    () => {
      saveController?.abort()
      saveError.value = undefined
      successMessage.value = undefined
      reloadRequired.value = false
    },
  )

  async function saveRules(
    rules: ReadonlyArray<PipeBoltDomainRuleRuleDefinition>,
    reason: string,
  ): Promise<PipeBoltApiDtoProjectConfigWriteResponse | undefined> {
    const project = toValue(projectId)
    const current = query.data.value
    if (!project || !current || isSaving.value) return undefined
    if (current.config.project_id !== project) {
      saveError.value = new ApiError({
        code: 'project_context_mismatch',
        kind: 'conflict',
        message: 'Loaded configuration does not match the active project.',
      })
      return undefined
    }
    const auditReason = reason.trim()
    if (!auditReason || textEncoder.encode(auditReason).byteLength > 1_024) {
      saveError.value = new ApiError({
        code: 'invalid_audit_reason',
        kind: 'validation',
        message: 'Audit reason must contain 1 to 1024 UTF-8 bytes.',
      })
      return undefined
    }

    saveController?.abort()
    const controller = new AbortController()
    saveController = controller
    const expectedVersion = current.version
    const nextConfig: PipeBoltApiDtoProjectConfigDocumentV1 = {
      ...structuredClone(current.config),
      rules: rules.map((rule) => structuredClone(rule)),
    }
    saveError.value = undefined
    successMessage.value = undefined
    isSaving.value = true
    try {
      const response = await saveRuleConfig(
        project,
        nextConfig,
        expectedVersion,
        auditReason,
        controller.signal,
      )
      if (controller.signal.aborted || toValue(projectId) !== project) return undefined

      queryCache.setQueryData(RULE_CONFIG_QUERY_KEYS.byProject(project), {
        config: nextConfig,
        schema_version: current.schema_version,
        version: response.version,
      })
      await queryCache.invalidateQueries({
        exact: true,
        key: ['project', project, 'command-catalog'],
      })
      reloadRequired.value = response.reload_required
      successMessage.value = `Configuration version ${response.version} saved.`
      return response
    } catch (error) {
      if (!controller.signal.aborted && toValue(projectId) === project) {
        saveError.value = toApiError(error)
      }
      return undefined
    } finally {
      if (saveController === controller) isSaving.value = false
    }
  }

  function dismissMessages(): void {
    saveError.value = undefined
    successMessage.value = undefined
  }

  function markReloaded(): void {
    reloadRequired.value = false
  }

  onScopeDispose(() => saveController?.abort())

  return {
    data: query.data,
    dismissMessages,
    isConflict,
    isLoading: query.isLoading,
    isSaving: readonly(isSaving),
    loadError,
    markReloaded,
    refetch: query.refetch,
    reloadRequired: readonly(reloadRequired),
    saveError: readonly(saveError),
    saveRules,
    successMessage: readonly(successMessage),
  }
}
