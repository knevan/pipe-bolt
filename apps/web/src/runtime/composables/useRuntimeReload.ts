import { computed, shallowRef, watch } from 'vue'
import { useMutation, useQuery, useQueryCache } from '@pinia/colada'

import { getApiErrorMessage } from '@/api/errors'
import { fetchRuntimeStatus, reloadProjectRuntime } from '../runtime.api'

export function useRuntimeReload(projectId: () => string) {
  const queryCache = useQueryCache()
  const reason = shallowRef('')
  const status = useQuery({
    key: () => ['project', projectId(), 'runtime-status'],
    enabled: () => Boolean(projectId()),
    query: ({ signal }) => fetchRuntimeStatus(projectId(), signal),
    staleTime: 5_000,
  })
  const mutation = useMutation({
    mutation: ({ id, reloadReason }: { id: string; reloadReason?: string }) =>
      reloadProjectRuntime(id, reloadReason),
  })
  const errorMessage = computed(() =>
    mutation.error.value ? getApiErrorMessage(mutation.error.value) : undefined,
  )
  const activeVersion = computed(
    () =>
      (mutation.data.value?.project_id === projectId()
        ? mutation.data.value.active_version
        : undefined) ?? status.data.value?.active_version,
  )

  watch(
    () => projectId(),
    () => mutation.reset(),
  )

  async function reload() {
    mutation.reset()
    const id = projectId()
    const response = await mutation.mutateAsync({
      id,
      reloadReason: reason.value.trim() || undefined,
    })
    reason.value = ''
    await Promise.all([
      queryCache.invalidateQueries({ key: ['project', id, 'runtime-status'], exact: true }),
      queryCache.invalidateQueries({ key: ['runtime', 'readiness'], exact: true }),
    ])
    return response
  }

  return {
    activeVersion,
    errorMessage,
    isLoading: mutation.isLoading,
    reason,
    reload,
    reset: mutation.reset,
  }
}
