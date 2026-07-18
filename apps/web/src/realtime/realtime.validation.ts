import type {
  PipeBoltApiDtoRealtimeEventResponse,
  PipeBoltApiDtoRealtimeServerMessage,
} from '@/api/generated'

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isNullableString(value: unknown): boolean {
  return value === undefined || value === null || typeof value === 'string'
}

function isStringRecord(value: unknown): boolean {
  return isRecord(value) && Object.values(value).every((item) => typeof item === 'string')
}

function isPayload(value: unknown): boolean {
  if (!isRecord(value)) return false
  return (
    (value.type === 'json' && 'value' in value) ||
    (value.type === 'raw_base64' && typeof value.value === 'string')
  )
}

function isDiagnostic(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.code === 'string' &&
    typeof value.message === 'string' &&
    isNullableString(value.field)
  )
}

function isRealtimeEvent(
  value: unknown,
  projectId: string,
): value is PipeBoltApiDtoRealtimeEventResponse {
  if (!isRecord(value)) return false
  return (
    value.project_id === projectId &&
    typeof value.id === 'string' &&
    typeof value.correlation_id === 'string' &&
    typeof value.broker_id === 'string' &&
    typeof value.route_id === 'string' &&
    typeof value.topic === 'string' &&
    typeof value.event_type === 'string' &&
    typeof value.received_at === 'string' &&
    isNullableString(value.device_id) &&
    isNullableString(value.schema_mapping_id) &&
    Number.isSafeInteger(value.payload_size_bytes) &&
    (value.payload_size_bytes as number) >= 0 &&
    isPayload(value.payload) &&
    isRecord(value.fields) &&
    isStringRecord(value.metadata) &&
    Array.isArray(value.normalization_errors) &&
    value.normalization_errors.every(isDiagnostic) &&
    (value.raw === undefined ||
      value.raw === null ||
      (isRecord(value.raw) &&
        Number.isSafeInteger(value.raw.byte_len) &&
        (value.raw.byte_len as number) >= 0 &&
        isNullableString(value.raw.content_type)))
  )
}

function isFilter(value: unknown): boolean {
  if (!isRecord(value)) return false
  return ['device_id', 'topic', 'topic_prefix', 'event_type', 'route_id'].every((key) =>
    isNullableString(value[key]),
  )
}

export class RealtimeProtocolError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'RealtimeProtocolError'
  }
}

export function parseRealtimeMessage(
  data: string,
  eventName: string | undefined,
  projectId: string,
): PipeBoltApiDtoRealtimeServerMessage {
  let input: unknown
  try {
    input = JSON.parse(data) as unknown
  } catch {
    if (eventName === 'error') return { message: data, type: 'error' }
    throw new RealtimeProtocolError('Realtime server emitted invalid JSON.')
  }
  if (!isRecord(input) || typeof input.type !== 'string') {
    throw new RealtimeProtocolError('Realtime server emitted an invalid message envelope.')
  }

  if (input.type === 'ready' && input.transport === 'sse' && isFilter(input.filter)) {
    return input as PipeBoltApiDtoRealtimeServerMessage
  }
  if (input.type === 'event' && isRealtimeEvent(input.data, projectId)) {
    return input as PipeBoltApiDtoRealtimeServerMessage
  }
  if (
    input.type === 'lagged' &&
    Number.isSafeInteger(input.skipped) &&
    (input.skipped as number) >= 0
  ) {
    return input as PipeBoltApiDtoRealtimeServerMessage
  }
  if (input.type === 'filter_updated' && isFilter(input.filter)) {
    return input as PipeBoltApiDtoRealtimeServerMessage
  }
  if (input.type === 'error' && typeof input.message === 'string') {
    return input as PipeBoltApiDtoRealtimeServerMessage
  }

  throw new RealtimeProtocolError('Realtime server emitted an unsupported message envelope.')
}
