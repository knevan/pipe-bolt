<script setup lang="ts">
import type { RealtimeEvent } from '../realtime.types'

defineProps<{
  events: ReadonlyArray<RealtimeEvent>
  selectedId?: string
}>()
const emit = defineEmits<{ select: [event: RealtimeEvent] }>()
const dateTime = new Intl.DateTimeFormat(undefined, {
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
})

function formatTime(value: string): string {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? 'invalid time' : dateTime.format(date)
}

function payloadKind(event: RealtimeEvent): string {
  return event.payload.type === 'json' ? 'JSON' : 'RAW'
}
</script>

<template>
  <section class="event-table-panel panel">
    <div class="event-table-heading">
      <div>
        <p class="kicker">NORMALIZED EVENT BUS</p>
        <h2>Recent events</h2>
      </div>
      <span>{{ events.length }} / 200 buffered</span>
    </div>
    <div v-if="events.length" class="event-table-scroll">
      <table class="event-table">
        <thead>
          <tr>
            <th><span class="visually-hidden">Inspect</span></th>
            <th>Received</th>
            <th>Device</th>
            <th>Event type</th>
            <th>Topic</th>
            <th>Route</th>
            <th>Payload</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="event in events"
            :key="event.id"
            v-memo="[event.id === selectedId]"
            :class="{ selected: event.id === selectedId }"
          >
            <td>
              <button
                class="inspect-button"
                type="button"
                :aria-label="`Inspect ${event.event_type} event from ${event.device_id ?? 'unknown device'}`"
                @click="emit('select', event)"
              >
                →
              </button>
            </td>
            <td>
              <time :datetime="event.received_at">{{ formatTime(event.received_at) }}</time>
            </td>
            <td>{{ event.device_id ?? '—' }}</td>
            <td>
              <span class="event-type">{{ event.event_type }}</span>
            </td>
            <td class="topic-cell" :title="event.topic">{{ event.topic }}</td>
            <td>{{ event.route_id }}</td>
            <td>
              <span class="payload-kind">{{ payloadKind(event) }}</span
              ><small>{{ event.payload_size_bytes.toLocaleString() }} B</small>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-else class="event-empty">
      <span class="event-empty-pulse"></span>
      <strong>Waiting for normalized events</strong>
      <p>Only events emitted by a matching <code>stream_to_ui</code> action appear here.</p>
    </div>
  </section>
</template>

<style scoped>
.event-table-panel {
  overflow: hidden;
}

.event-table-heading {
  display: flex;
  padding: 1.15rem 1.25rem;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--line-soft);
}

.event-table-heading h2,
.event-table-heading p {
  margin-bottom: 0;
}

.event-table-heading > span {
  color: var(--muted);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.62rem;
}

.event-table-scroll {
  max-height: 38rem;
  overflow: auto;
}

.event-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.74rem;
}

.event-table th {
  position: sticky;
  z-index: 2;
  top: 0;
  padding: 0.7rem 0.85rem;
  color: #71868b;
  background: #111e24;
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.56rem;
  letter-spacing: 0.08em;
  text-align: left;
  text-transform: uppercase;
}

.event-table td {
  max-width: 18rem;
  padding: 0.75rem 0.85rem;
  color: #afbec1;
  border-top: 1px solid var(--line-soft);
  white-space: nowrap;
}

.event-table tbody tr:hover,
.event-table tbody tr.selected {
  background: rgba(85, 201, 195, 0.07);
}

.event-table time,
.topic-cell,
.event-table td:nth-child(6) {
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.65rem;
}

.inspect-button {
  width: 1.8rem;
  height: 1.8rem;
  color: var(--cyan);
  border: 1px solid var(--line);
  background: transparent;
  cursor: pointer;
}

.inspect-button:hover {
  border-color: var(--cyan);
}

.visually-hidden {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}

.topic-cell {
  overflow: hidden;
  text-overflow: ellipsis;
}

.event-type,
.payload-kind {
  display: inline-block;
  padding: 0.18rem 0.35rem;
  color: var(--cyan);
  border: 1px solid rgba(85, 201, 195, 0.25);
  border-radius: 0.18rem;
  font-size: 0.62rem;
}

.payload-kind {
  margin-right: 0.4rem;
  color: var(--accent);
  border-color: rgba(240, 184, 76, 0.25);
}

.event-table td small {
  color: var(--muted);
}

.event-empty {
  display: grid;
  min-height: 21rem;
  padding: 2rem;
  place-items: center;
  align-content: center;
  color: var(--muted);
  text-align: center;
}

.event-empty-pulse {
  width: 1rem;
  height: 1rem;
  margin-bottom: 1rem;
  border: 2px solid var(--cyan);
  border-radius: 50%;
  box-shadow: 0 0 1.2rem rgba(85, 201, 195, 0.65);
}

.event-empty strong {
  color: var(--text);
}

.event-empty p {
  margin: 0.55rem 0 0;
  font-size: 0.74rem;
}

@media (max-width: 800px) {
  .event-table-scroll {
    max-height: 30rem;
  }

  .event-table {
    min-width: 58rem;
  }
}
</style>
