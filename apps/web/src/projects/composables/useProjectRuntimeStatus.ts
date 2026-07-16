import { computed, onMounted, onUnmounted, toValue, type MaybeRefOrGetter } from 'vue'
import { useQuery } from '@pinia/colada'

import { getApiErrorMessage } from '@/api/errors'
import { fetchProjectRuntimeStatus } from '../projects.api'

const REFRESH_INTERVAL_MS = 15_000

export function useProjectRuntimeStatus(projectId: MaybeRefOrGetter<string | undefined>) {
  const query = useQuery({
    key: () => ['project', toValue(projectId) ?? '', 'runtime-status'],
    enabled: () => Boolean(toValue(projectId)),
    query: ({ signal }) => fetchProjectRuntimeStatus(toValue(projectId) ?? '', signal),
    staleTime: 5_000,
  })
  const errorMessage = computed(() =>
    query.error.value ? getApiErrorMessage(query.error.value) : undefined,
  )

  let timer: number | undefined
  onMounted(() => {
    timer = window.setInterval(() => void query.refetch(), REFRESH_INTERVAL_MS)
  })
  onUnmounted(() => window.clearInterval(timer))

  return { ...query, errorMessage }
}
