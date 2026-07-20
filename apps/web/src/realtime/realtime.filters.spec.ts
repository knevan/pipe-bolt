import { describe, expect, it } from 'vitest'

import type { RealtimeFilterInput } from './realtime.types'
import { parseRealtimeFilters } from './realtime.filters'

function filters(patch: Partial<RealtimeFilterInput> = {}): RealtimeFilterInput {
  return {
    deviceId: '',
    eventType: '',
    routeId: '',
    topic: '',
    topicPrefix: '',
    ...patch,
  }
}

describe('realtime filter parsing', () => {
  it('trims values, omits blanks, and maps API field names', () => {
    expect(
      parseRealtimeFilters(
        filters({
          deviceId: ' device-1 ',
          eventType: ' telemetry ',
          routeId: ' route-main ',
          topic: ' devices/1/telemetry ',
          topicPrefix: ' devices/1 ',
        }),
      ),
    ).toEqual({
      errors: {},
      filters: {
        device_id: 'device-1',
        event_type: 'telemetry',
        route_id: 'route-main',
        topic: 'devices/1/telemetry',
        topic_prefix: 'devices/1',
      },
    })
  })

  it('allows topic separators while rejecting separators in ID fields', () => {
    expect(parseRealtimeFilters(filters({ deviceId: 'devices/1', topic: 'devices/1' }))).toEqual({
      errors: { deviceId: 'Value must be a single segment without /.' },
      filters: {},
    })
  })

  it('rejects MQTT wildcards in every filter field', () => {
    expect(
      parseRealtimeFilters(
        filters({
          deviceId: '+',
          eventType: '#',
          routeId: 'route+',
          topic: 'devices/+',
          topicPrefix: 'devices/#',
        }),
      ).errors,
    ).toEqual({
      deviceId: 'MQTT wildcard characters + and # are not allowed.',
      eventType: 'MQTT wildcard characters + and # are not allowed.',
      routeId: 'MQTT wildcard characters + and # are not allowed.',
      topic: 'MQTT wildcard characters + and # are not allowed.',
      topicPrefix: 'MQTT wildcard characters + and # are not allowed.',
    })
  })

  it('accepts values exactly at client length limits', () => {
    expect(
      parseRealtimeFilters(filters({ deviceId: 'd'.repeat(256), topic: 't'.repeat(1024) })).errors,
    ).toEqual({})
  })

  it('rejects values above client length limits', () => {
    expect(
      parseRealtimeFilters(filters({ deviceId: 'd'.repeat(257), topic: 't'.repeat(1025) })).errors,
    ).toEqual({
      deviceId: 'Value exceeds 256 characters.',
      topic: 'Value exceeds 1024 characters.',
    })
  })
})
