import type { PipeBoltApiDtoRealtimeEventResponse } from '@/api/generated'

export interface RealtimeFilterInput {
  deviceId: string
  eventType: string
  routeId: string
  topic: string
  topicPrefix: string
}

export interface RealtimeFilters {
  device_id?: string
  event_type?: string
  route_id?: string
  topic?: string
  topic_prefix?: string
}

export type RealtimeFilterField = keyof RealtimeFilterInput
export type RealtimeFilterErrors = Partial<Record<RealtimeFilterField, string>>

export type RealtimeConnectionState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'paused'
  | 'error'

export type RealtimeEvent = PipeBoltApiDtoRealtimeEventResponse
