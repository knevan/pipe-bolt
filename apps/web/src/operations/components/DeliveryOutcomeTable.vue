<script setup lang="ts">
import type { PipeBoltApiDtoSinkDeliveryOutcomeResponse } from '@/api/generated'
import { deliveryTone, formatOperationTime } from '../operations.format'
import OperationStatusBadge from './OperationStatusBadge.vue'

defineProps<{ items: ReadonlyArray<PipeBoltApiDtoSinkDeliveryOutcomeResponse> }>()
</script>

<template>
  <div
    v-if="items.length"
    class="operation-table-scroll"
    role="region"
    aria-label="Sink delivery outcome table"
    tabindex="0"
  >
    <table class="operation-table">
      <thead>
        <tr>
          <th>Occurred</th>
          <th>Status</th>
          <th>Delivery / event</th>
          <th>Sink</th>
          <th>HTTP</th>
          <th>Attempt</th>
          <th>Duration</th>
          <th>Response</th>
          <th>Correlation</th>
          <th>Failure reason</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="outcome in items" :key="outcome.delivery_id">
          <td>
            <time :datetime="outcome.occurred_at">{{
              formatOperationTime(outcome.occurred_at)
            }}</time>
          </td>
          <td><OperationStatusBadge :label="outcome.status" :tone="deliveryTone(outcome)" /></td>
          <td>
            <code :title="outcome.delivery_id">delivery: {{ outcome.delivery_id }}</code>
            <code :title="outcome.event_id">event: {{ outcome.event_id }}</code>
          </td>
          <td>
            <code :title="outcome.sink_id">{{ outcome.sink_id }}</code>
          </td>
          <td>
            <OperationStatusBadge
              v-if="outcome.http_status"
              :label="String(outcome.http_status)"
              :tone="outcome.http_status >= 200 && outcome.http_status < 300 ? 'safe' : 'warning'"
            />
            <span v-else>—</span>
          </td>
          <td>{{ outcome.attempt }}</td>
          <td>
            {{ outcome.duration_ms == null ? '—' : `${outcome.duration_ms.toLocaleString()} ms` }}
          </td>
          <td>
            {{
              outcome.response_body_bytes == null
                ? '—'
                : `${outcome.response_body_bytes.toLocaleString()} B`
            }}
          </td>
          <td>
            <code :title="outcome.correlation_id ?? undefined">{{
              outcome.correlation_id ?? '—'
            }}</code>
          </td>
          <td class="reason-cell" :title="outcome.failure_reason ?? undefined">
            {{ outcome.failure_reason ?? '—' }}
          </td>
        </tr>
      </tbody>
    </table>
  </div>
  <div v-else class="operation-empty">
    <strong>No delivery outcomes in this page</strong>
    <p>Webhook delivery attempts and terminal outcomes appear here.</p>
  </div>
</template>

<style scoped>
.operation-table-scroll {
  overflow: auto;
}

.operation-table {
  width: 100%;
  min-width: 76rem;
  border-collapse: collapse;
  font-size: 0.72rem;
}

.operation-table th {
  padding: 0.7rem 0.8rem;
  color: #71868b;
  background: #111e24;
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.54rem;
  letter-spacing: 0.08em;
  text-align: left;
  text-transform: uppercase;
}

.operation-table td {
  max-width: 22rem;
  padding: 0.75rem 0.8rem;
  border-top: 1px solid var(--line-soft);
  color: #afbec1;
  vertical-align: top;
}

.operation-table tbody tr:hover {
  background: rgba(85, 201, 195, 0.05);
}

.operation-table code,
.operation-table time {
  display: block;
  overflow: hidden;
  color: var(--cyan);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.62rem;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.operation-table code + code {
  margin-top: 0.25rem;
}

.reason-cell {
  overflow: hidden;
  color: #ffad9f;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.operation-empty {
  display: grid;
  min-height: 16rem;
  padding: 2rem;
  place-content: center;
  color: var(--muted);
  text-align: center;
}

.operation-empty strong {
  color: var(--text);
}

.operation-empty p {
  margin: 0.5rem 0 0;
  font-size: 0.74rem;
}
</style>
