import { computed, onMounted, onUnmounted } from 'vue'
import { useQuery } from '@pinia/colada'

import { getApiErrorMessage } from '@/api/errors'
import { fetchLiveness, fetchReadiness } from '../runtime.api'

const REFRESH_INTERVAL_MS = 30_000

export function useSystemStatus() {
  const health = useQuery({
    key: ['runtime', 'liveness'],
    query: ({ signal }) => fetchLiveness(signal),
    staleTime: 10_000,
  })
  const readiness = useQuery({
    key: ['runtime', 'readiness'],
    query: ({ signal }) => fetchReadiness(signal),
    staleTime: 10_000,
  })

  const healthError = computed(() =>
    health.error.value ? getApiErrorMessage(health.error.value) : undefined,
  )
  const readinessError = computed(() =>
    readiness.error.value ? getApiErrorMessage(readiness.error.value) : undefined,
  )

  async function refresh(): Promise<void> {
    await Promise.all([health.refetch(), readiness.refetch()])
  }

  let timer: number | undefined
  onMounted(() => {
    timer = window.setInterval(() => void refresh(), REFRESH_INTERVAL_MS)
  })
  onUnmounted(() => window.clearInterval(timer))

  return { health, healthError, readiness, readinessError, refresh }
}
