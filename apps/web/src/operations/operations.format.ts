import type {
  PipeBoltApiDtoFailureEventResponse,
  PipeBoltApiDtoSinkDeliveryOutcomeResponse,
} from '@/api/generated'

export type OperationTone = 'danger' | 'neutral' | 'safe' | 'warning'

const dateTime = new Intl.DateTimeFormat(undefined, {
  dateStyle: 'medium',
  timeStyle: 'medium',
})

export function formatOperationTime(value: string): string {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? 'Invalid timestamp' : dateTime.format(date)
}

export function formatRecord(value: Record<string, unknown>): string {
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return '[Unserializable details]'
  }
}

export function auditStatusTone(status: string): OperationTone {
  const normalized = status.toLowerCase()
  if (['success', 'succeeded', 'published', 'resolved'].includes(normalized)) return 'safe'
  if (['failed', 'error', 'rejected'].includes(normalized)) return 'danger'
  if (['queued', 'pending', 'warning'].includes(normalized)) return 'warning'
  return 'neutral'
}

export function failureTone(failure: PipeBoltApiDtoFailureEventResponse): OperationTone {
  if (failure.resolved_at) return 'safe'
  const severity = failure.severity.toLowerCase()
  if (severity === 'critical' || severity === 'error') return 'danger'
  if (severity === 'warning' || severity === 'warn') return 'warning'
  return 'neutral'
}

export function deliveryTone(outcome: PipeBoltApiDtoSinkDeliveryOutcomeResponse): OperationTone {
  if (outcome.status === 'delivered') return 'safe'
  if (outcome.status === 'http_rejected') return 'warning'
  return 'danger'
}
