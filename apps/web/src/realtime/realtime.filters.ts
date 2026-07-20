import type {
  RealtimeFilterErrors,
  RealtimeFilterField,
  RealtimeFilterInput,
  RealtimeFilters,
} from './realtime.types'

const SEGMENT_MAX_LENGTH = 256
const TOPIC_MAX_LENGTH = 1024
const FILTER_FIELDS: ReadonlyArray<RealtimeFilterField> = [
  'deviceId',
  'eventType',
  'routeId',
  'topic',
  'topicPrefix',
]

export interface RealtimeFilterParseResult {
  errors: RealtimeFilterErrors
  filters: RealtimeFilters
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

export function parseRealtimeFilters(input: RealtimeFilterInput): RealtimeFilterParseResult {
  const errors: RealtimeFilterErrors = {}
  for (const field of FILTER_FIELDS) {
    const error = validateField(field, input[field])
    if (error) errors[field] = error
  }

  const filters: RealtimeFilters = {}
  if (Object.keys(errors).length > 0) return { errors, filters }
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
  return { errors, filters }
}
