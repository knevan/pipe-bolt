import { onUnmounted, shallowRef } from 'vue'
import type {
  RealtimeFilterErrors,
  RealtimeFilterField,
  RealtimeFilterInput,
  RealtimeFilters,
} from '../realtime.types'

const FILTER_DEBOUNCE_MS = 350
const SEGMENT_MAX_LENGTH = 256
const TOPIC_MAX_LENGTH = 1024

const EMPTY_FILTERS: RealtimeFilterInput = {
  deviceId: '',
  eventType: '',
  routeId: '',
  topic: '',
  topicPrefix: '',
}

function validateField(field: RealtimeFilterField, value: string): string | undefined {
  const normalized = value.trim()
  if (!normalized) return
  const isTopic = field === 'topic' || field === 'topicPrefix'
  if (normalized.includes('+') || normalized.includes('#')) {
    return 'MQTT wildcard characters + and # are not allowed.'
  }
  if (!isTopic && normalized.includes('/')) return 'Value must be a single segment without /.'
  if (normalized.length > (isTopic ? TOPIC_MAX_LENGTH : SEGMENT_MAX_LENGTH)) {
    return `Value exceeds ${isTopic ? TOPIC_MAX_LENGTH : SEGMENT_MAX_LENGTH} characters.`
  }
}

function normalize(input: RealtimeFilterInput): RealtimeFilters {
  const filters: RealtimeFilters = {}
  const deviceId = input.deviceId.trim()
  const eventType = input.eventType.trim()
  const routeId = input.routeId.trim()
  const topic = input.topic.trim()
  const topicPrefix = input.topicPrefix.trim()
  if (deviceId) filters.device_id = deviceId
  if (eventType) filters.event_type = eventType
  if (routeId) filters.route_id = routeId
  if (topic) filters.topic = topic
  if (topicPrefix) filters.topic_prefix = topicPrefix
  return filters
}

export function useRealtimeFilters() {
  const draft = shallowRef<RealtimeFilterInput>({ ...EMPTY_FILTERS })
  const active = shallowRef<RealtimeFilters>({})
  const errors = shallowRef<RealtimeFilterErrors>({})
  let timer: number | undefined

  function commit(value: RealtimeFilterInput): void {
    const nextErrors: RealtimeFilterErrors = {}
    for (const field of Object.keys(value) as RealtimeFilterField[]) {
      const error = validateField(field, value[field])
      if (error) nextErrors[field] = error
    }
    errors.value = nextErrors
    if (!Object.keys(nextErrors).length) active.value = normalize(value)
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
