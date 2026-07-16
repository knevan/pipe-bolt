<script setup lang="ts">
import { computed } from 'vue'
import { storeToRefs } from 'pinia'

import { useProjectStore } from './project.store'
import { useProjectRuntimeStatus } from './composables/useProjectRuntimeStatus'

const projects = useProjectStore()
const { activeProjectId } = storeToRefs(projects)
const runtime = useProjectRuntimeStatus(activeProjectId)

const dateTime = new Intl.DateTimeFormat(undefined, {
  dateStyle: 'medium',
  timeStyle: 'medium',
})

const status = computed(() => runtime.data.value)
const pipelineCounters = computed(() => {
  const counters = status.value?.counters.pipeline
  if (!counters) return []
  return [
    ['Normalized', counters.normalized_total],
    ['Rules matched', counters.matched_rule_total],
    ['Action intents', counters.action_intent_total],
    ['Dispatch failed', counters.dispatch_failed_total],
    ['UI published', counters.realtime_event_published_total],
    ['UI no receiver', counters.realtime_event_no_receiver_total],
    ['Forward outcomes', counters.forward_outcome_total],
    ['Outcome persist failed', counters.delivery_outcome_persist_failed_total],
  ] as const
})
const forwarderCounters = computed(() => {
  const counters = status.value?.counters.forwarder
  if (!counters) return []
  return [
    ['Accepted', counters.accepted_total],
    ['Delivered', counters.delivered_total],
    ['Failed', counters.failed_total],
    ['Rejected', counters.rejected_total],
    ['Backpressure', counters.backpressure_total],
    ['Timed out', counters.timed_out_total],
  ] as const
})
const persistenceCounters = computed(() => {
  const counters = status.value?.counters.persistence_writer
  if (!counters) return []
  return [
    ['Enqueued', counters.enqueued_total],
    ['Written', counters.write_succeeded_total],
    ['Write failed', counters.write_failed_total],
    ['Queue full', counters.queue_full_total],
    ['Timed out', counters.write_timeout_total],
  ] as const
})

function formatDate(value?: string | null): string {
  if (!value) return 'Not reported'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? 'Invalid timestamp' : dateTime.format(date)
}
</script>

<template>
  <div class="page">
    <header class="page-header overview-header">
      <div>
        <p class="eyebrow">PROJECT / {{ activeProjectId }}</p>
        <h1>Runtime overview</h1>
        <p class="page-summary">
          Current lifecycle, active configuration, and bounded pipeline health.
        </p>
      </div>
      <button
        class="button button-secondary"
        type="button"
        :disabled="runtime.isLoading.value"
        @click="runtime.refetch()"
      >
        {{ runtime.isLoading.value ? 'Refreshing...' : 'Refresh status' }}
      </button>
    </header>

    <div v-if="runtime.errorMessage.value" class="alert alert-danger" role="alert">
      <div>
        <strong>Runtime status unavailable</strong><span>{{ runtime.errorMessage.value }}</span>
      </div>
      <button type="button" @click="runtime.refetch()">Retry</button>
    </div>

    <section v-if="status" class="status-hero" :data-state="status.state">
      <div class="status-hero-main">
        <p class="kicker">LIFECYCLE</p>
        <div class="lifecycle-row">
          <span class="lifecycle-pulse"></span>
          <h2>{{ status.state }}</h2>
        </div>
        <p>
          Active config version <strong>{{ status.active_version ?? 'none' }}</strong>
        </p>
      </div>
      <dl class="status-facts">
        <div>
          <dt>Started</dt>
          <dd>{{ formatDate(status.started_at) }}</dd>
        </div>
        <div>
          <dt>Last reload</dt>
          <dd>{{ formatDate(status.last_reload_at) }}</dd>
        </div>
      </dl>
    </section>

    <div
      v-else-if="runtime.status.value === 'pending'"
      class="skeleton-grid"
      aria-label="Loading runtime status"
    >
      <div v-for="index in 4" :key="index" class="skeleton-block"></div>
    </div>

    <section v-if="status?.last_reload_error" class="alert alert-warning" role="status">
      <div>
        <strong>Last reload failed</strong>
        <span>{{ status.last_reload_error }}</span>
      </div>
    </section>

    <div v-if="status" class="counter-layout">
      <section class="panel counter-panel counter-panel-wide">
        <div class="panel-heading">
          <div>
            <p class="kicker">DATA PLANE</p>
            <h2>Pipeline</h2>
          </div>
          <span>8 signals</span>
        </div>
        <dl class="counter-grid">
          <div v-for="[label, value] in pipelineCounters" :key="label">
            <dt>{{ label }}</dt>
            <dd>{{ value.toLocaleString() }}</dd>
          </div>
        </dl>
      </section>

      <section class="panel counter-panel">
        <div class="panel-heading">
          <div>
            <p class="kicker">EXTERNAL I/O</p>
            <h2>Forwarder</h2>
          </div>
        </div>
        <dl class="counter-list">
          <div v-for="[label, value] in forwarderCounters" :key="label">
            <dt>{{ label }}</dt>
            <dd>{{ value.toLocaleString() }}</dd>
          </div>
        </dl>
      </section>

      <section class="panel counter-panel">
        <div class="panel-heading">
          <div>
            <p class="kicker">DURABILITY</p>
            <h2>Persistence</h2>
          </div>
        </div>
        <dl v-if="persistenceCounters.length" class="counter-list">
          <div v-for="[label, value] in persistenceCounters" :key="label">
            <dt>{{ label }}</dt>
            <dd>{{ value.toLocaleString() }}</dd>
          </div>
        </dl>
        <p v-else class="empty-state">Persistence writer metrics are not configured.</p>
      </section>
    </div>
  </div>
</template>
