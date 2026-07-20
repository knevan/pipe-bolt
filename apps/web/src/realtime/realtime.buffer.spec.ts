import { describe, expect, it } from 'vitest'

import type { RealtimeEvent } from './realtime.types'
import { REALTIME_EVENT_LIMIT, RealtimeEventBuffer } from './realtime.buffer'

function event(id: string): RealtimeEvent {
  return {
    broker_id: 'broker-main',
    correlation_id: `correlation-${id}`,
    device_id: 'device-1',
    event_type: 'telemetry',
    fields: {},
    id,
    metadata: {},
    normalization_errors: [],
    payload: { type: 'json', value: { temperature: 21 } },
    payload_size_bytes: 18,
    project_id: 'project-test',
    raw: null,
    received_at: '2026-07-20T10:00:00Z',
    route_id: 'route-main',
    schema_mapping_id: null,
    topic: 'devices/device-1/telemetry',
  }
}

describe('realtime event buffer', () => {
  it('retains arrival order below capacity', () => {
    const buffer = new RealtimeEventBuffer({ maxEvents: 3, maxWeight: 100 })
    buffer.push(event('event-1'), 2)
    buffer.push(event('event-2'), 2)

    expect(buffer.events.map(({ id }) => id)).toEqual(['event-1', 'event-2'])
  })

  it('evicts oldest events when count capacity is reached', () => {
    const buffer = new RealtimeEventBuffer({ maxEvents: 2, maxWeight: 100 })
    buffer.push(event('event-1'), 2)
    buffer.push(event('event-2'), 2)

    expect(buffer.push(event('event-3'), 2)).toEqual({ accepted: true, dropped: 1 })
    expect(buffer.events.map(({ id }) => id)).toEqual(['event-2', 'event-3'])
  })

  it('evicts only enough events to satisfy the weight limit', () => {
    const buffer = new RealtimeEventBuffer({ maxEvents: 10, maxWeight: 8 })
    buffer.push(event('event-1'), 2)
    buffer.push(event('event-2'), 2)

    expect(buffer.push(event('event-3'), 2)).toEqual({ accepted: true, dropped: 1 })
    expect({ ids: buffer.events.map(({ id }) => id), weight: buffer.weight }).toEqual({
      ids: ['event-2', 'event-3'],
      weight: 8,
    })
  })

  it('rejects a single overweight event without discarding retained events', () => {
    const buffer = new RealtimeEventBuffer({ maxEvents: 3, maxWeight: 10 })
    buffer.push(event('event-1'), 2)

    expect(buffer.push(event('event-2'), 6)).toEqual({ accepted: false, dropped: 1 })
    expect(buffer.events.map(({ id }) => id)).toEqual(['event-1'])
  })

  it('clears retained events and aggregate weight', () => {
    const buffer = new RealtimeEventBuffer()
    buffer.push(event('event-1'), 2)
    buffer.clear()

    expect({ events: buffer.events, weight: buffer.weight }).toEqual({ events: [], weight: 0 })
  })

  it('keeps the product event count limit explicit', () => {
    expect(REALTIME_EVENT_LIMIT).toBe(200)
  })

  it('rejects invalid constructor bounds', () => {
    expect(() => new RealtimeEventBuffer({ maxEvents: 0 })).toThrow(RangeError)
  })
})
