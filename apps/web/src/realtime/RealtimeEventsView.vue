<script setup lang="ts">
import { computed, shallowRef, watch } from 'vue'
import { storeToRefs } from 'pinia'

import { useProjectStore } from '@/projects'
import RealtimeEventDetailDrawer from './components/RealtimeEventDetailDrawer.vue'
import RealtimeEventTable from './components/RealtimeEventTable.vue'
import RealtimeFilterPanel from './components/RealtimeFilterPanel.vue'
import { useRealtimeFilters } from './composables/useRealtimeFilters'
import { useRealtimeStream } from './composables/useRealtimeStream'
import type { RealtimeEvent, RealtimeFilterInput } from './realtime.types'

const projects = useProjectStore()
const { activeProjectId } = storeToRefs(projects)
const filters = useRealtimeFilters()
const stream = useRealtimeStream({ filters: filters.active, projectId: activeProjectId })
const selectedEvent = shallowRef<RealtimeEvent>()

const stateLabel = computed(() => stream.connectionState.value.replace('_', ' '))
const statusClass = computed(() => ({
  connected: stream.connectionState.value === 'connected',
  error: stream.connectionState.value === 'error',
  paused: stream.connectionState.value === 'paused',
  pending: ['connecting', 'reconnecting'].includes(stream.connectionState.value),
}))

watch([filters.active, activeProjectId], () => {
  selectedEvent.value = undefined
})

function updateFilters(value: RealtimeFilterInput): void {
  filters.update(value)
}
</script>

<template>
  <div class="page realtime-page">
    <header class="page-header realtime-header">
      <div>
        <p class="eyebrow">PROJECT / {{ activeProjectId }}</p>
        <h1>Realtime events</h1>
        <p class="page-summary">
          Inspect normalized events emitted through governed <code>stream_to_ui</code> actions.
        </p>
      </div>
      <div class="stream-state" :class="statusClass">
        <span></span>
        <div>
          <small>STREAM STATE</small><strong>{{ stateLabel }}</strong>
        </div>
      </div>
    </header>

    <div v-if="stream.lagVisible.value" class="alert alert-warning lag-alert" role="alert">
      <div>
        <strong>Stream lagging</strong><span>{{ stream.lagMessage.value }}</span>
      </div>
      <button type="button" @click="stream.dismissLag">Dismiss</button>
    </div>
    <div v-if="stream.streamNotice.value" class="alert alert-danger" role="alert">
      <div>
        <strong>Server stream error</strong><span>{{ stream.streamNotice.value }}</span>
      </div>
    </div>
    <div v-if="stream.errorMessage.value" class="stream-error" role="status">
      <strong>{{
        stream.connectionState.value === 'reconnecting'
          ? `Reconnect attempt ${stream.reconnectAttempt.value}`
          : 'Stream unavailable'
      }}</strong>
      <span>{{ stream.errorMessage.value }}</span>
    </div>

    <RealtimeFilterPanel
      :errors="filters.errors.value"
      :model-value="filters.draft.value"
      @clear="filters.clear"
      @update:model-value="updateFilters"
    />

    <section class="stream-toolbar">
      <div class="stream-metrics">
        <div>
          <span>BUFFER</span><strong>{{ stream.events.value.length }} / 200</strong>
        </div>
        <div>
          <span>BACKEND SKIPPED</span
          ><strong>{{ stream.backendSkipped.value.toLocaleString() }}</strong>
        </div>
        <div>
          <span>BROWSER DROPPED</span
          ><strong>{{ stream.browserDropped.value.toLocaleString() }}</strong>
        </div>
      </div>
      <div class="stream-actions">
        <button
          class="button button-secondary"
          type="button"
          :disabled="!stream.events.value.length"
          @click="stream.clearEvents"
        >
          Clear buffer
        </button>
        <button
          v-if="stream.isStreaming.value"
          class="button button-secondary"
          type="button"
          @click="stream.pause"
        >
          Pause stream
        </button>
        <button
          v-else-if="stream.isPaused.value"
          class="button button-primary"
          type="button"
          @click="stream.resume"
        >
          Resume stream
        </button>
        <button
          v-if="stream.isStreaming.value || stream.isPaused.value"
          class="button button-secondary"
          type="button"
          @click="stream.disconnect"
        >
          Disconnect
        </button>
        <button v-else class="button button-primary" type="button" @click="stream.connect">
          Connect
        </button>
      </div>
    </section>

    <RealtimeEventTable
      :events="stream.events.value"
      :selected-id="selectedEvent?.id"
      @select="selectedEvent = $event"
    />

    <RealtimeEventDetailDrawer
      v-if="selectedEvent"
      :event="selectedEvent"
      @close="selectedEvent = undefined"
    />
  </div>
</template>

<style scoped>
.realtime-page {
  width: min(100%, 105rem);
}

.realtime-header code,
.page-summary code {
  color: var(--cyan);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.78em;
}

.stream-state {
  display: flex;
  min-width: 10rem;
  padding: 0.75rem 1rem;
  align-items: center;
  gap: 0.7rem;
  border: 1px solid var(--line);
  background: var(--surface);
}

.stream-state > span {
  width: 0.65rem;
  height: 0.65rem;
  border-radius: 50%;
  background: var(--muted);
}

.stream-state.connected > span {
  background: var(--safe);
  box-shadow: 0 0 0.8rem rgba(98, 200, 149, 0.6);
}

.stream-state.pending > span {
  background: var(--warning);
}

.stream-state.paused > span {
  background: var(--accent);
}

.stream-state.error > span {
  background: var(--danger);
}

.stream-state small,
.stream-state strong {
  display: block;
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
}

.stream-state small {
  color: var(--muted);
  font-size: 0.52rem;
  letter-spacing: 0.08em;
}

.stream-state strong {
  margin-top: 0.2rem;
  font-size: 0.72rem;
  text-transform: uppercase;
}

.stream-error {
  display: flex;
  margin-bottom: 0.8rem;
  gap: 0.6rem;
  color: #e3b06a;
  font-size: 0.72rem;
}

.stream-error span {
  color: var(--muted);
}

.stream-toolbar {
  display: flex;
  margin: 1rem 0;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
}

.stream-metrics,
.stream-actions {
  display: flex;
  align-items: center;
  gap: 0.55rem;
}

.stream-metrics {
  gap: 1.5rem;
}

.stream-metrics span,
.stream-metrics strong {
  display: block;
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
}

.stream-metrics span {
  color: var(--muted);
  font-size: 0.5rem;
  letter-spacing: 0.08em;
}

.stream-metrics strong {
  margin-top: 0.2rem;
  font-size: 0.7rem;
}

.stream-actions .button {
  min-height: 2.35rem;
  padding: 0.5rem 0.75rem;
  font-size: 0.7rem;
}

@media (max-width: 800px) {
  .stream-toolbar {
    align-items: stretch;
    flex-direction: column;
  }

  .stream-actions {
    flex-wrap: wrap;
  }
}

@media (max-width: 560px) {
  .stream-metrics {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 0.6rem;
  }
}
</style>
