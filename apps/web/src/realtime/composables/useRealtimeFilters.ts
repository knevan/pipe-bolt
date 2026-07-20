import { onUnmounted, shallowRef } from 'vue'
import type { RealtimeFilterErrors, RealtimeFilterInput, RealtimeFilters } from '../realtime.types'
import { parseRealtimeFilters } from '../realtime.filters'

const FILTER_DEBOUNCE_MS = 350

const EMPTY_FILTERS: RealtimeFilterInput = {
  deviceId: '',
  eventType: '',
  routeId: '',
  topic: '',
  topicPrefix: '',
}

export function useRealtimeFilters() {
  const draft = shallowRef<RealtimeFilterInput>({ ...EMPTY_FILTERS })
  const active = shallowRef<RealtimeFilters>({})
  const errors = shallowRef<RealtimeFilterErrors>({})
  let timer: number | undefined

  function commit(value: RealtimeFilterInput): void {
    const parsed = parseRealtimeFilters(value)
    errors.value = parsed.errors
    if (!Object.keys(parsed.errors).length) active.value = parsed.filters
  }

  function update(value: RealtimeFilterInput): void {
    draft.value = value
    window.clearTimeout(timer)
    timer = window.setTimeout(() => commit(value), FILTER_DEBOUNCE_MS)
  }

  function clear(): void {
    window.clearTimeout(timer)
    draft.value = { ...EMPTY_FILTERS }
    errors.value = {}
    active.value = {}
  }

  onUnmounted(() => window.clearTimeout(timer))
  return { active, clear, draft, errors, update }
}
