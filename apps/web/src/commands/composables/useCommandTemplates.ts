import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import { useQuery } from '@pinia/colada'

import { getApiErrorMessage } from '@/api/errors'
import { fetchCommandCatalog } from '../commands.api'

export function useCommandTemplates(projectId: MaybeRefOrGetter<string | undefined>) {
  const query = useQuery({
    key: () => ['project', toValue(projectId) ?? '', 'command-catalog'],
    enabled: () => Boolean(toValue(projectId)),
    query: ({ signal }) => fetchCommandCatalog(toValue(projectId) ?? '', signal),
    staleTime: 10_000,
  })
  const errorMessage = computed(() =>
    query.error.value ? getApiErrorMessage(query.error.value) : undefined,
  )

  return { ...query, errorMessage }
}
