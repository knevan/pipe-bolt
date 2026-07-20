<script setup lang="ts">
import { shallowRef } from 'vue'

import type { PipeBoltApiDtoFailureEventResponse } from '@/api/generated'
import { failureTone, formatOperationTime, formatRecord } from '../operations.format'
import OperationStatusBadge from './OperationStatusBadge.vue'

defineProps<{ items: ReadonlyArray<PipeBoltApiDtoFailureEventResponse> }>()
const emit = defineEmits<{ resolve: [failure: PipeBoltApiDtoFailureEventResponse] }>()
const expandedIds = shallowRef<ReadonlySet<string>>(new Set())

function toggleDetails(id: string): void {
  const next = new Set(expandedIds.value)
  if (next.has(id)) next.delete(id)
  else next.add(id)
  expandedIds.value = next
}
</script>

<template>
  <div
    v-if="items.length"
    class="operation-table-scroll"
    role="region"
    aria-label="Failure event table"
    tabindex="0"
  >
    <table class="operation-table">
      <thead>
        <tr>
          <th>Occurred</th>
          <th>Component / kind</th>
          <th>Severity</th>
          <th>Message</th>
          <th>References</th>
          <th>Resolution</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        <template v-for="(failure, index) in items" :key="failure.failure_id">
          <tr>
            <td>
              <time :datetime="failure.occurred_at">{{
                formatOperationTime(failure.occurred_at)
              }}</time>
            </td>
            <td>
              <strong>{{ failure.component }}</strong
              ><code>{{ failure.failure_kind }}</code>
            </td>
            <td>
              <OperationStatusBadge
                :label="failure.resolved_at ? 'resolved' : failure.severity"
                :tone="failureTone(failure)"
              />
            </td>
            <td class="message-cell" :title="failure.message">{{ failure.message }}</td>
            <td>
              <code :title="failure.event_id ?? undefined"
                >event: {{ failure.event_id ?? '—' }}</code
              >
              <code :title="failure.sink_id ?? undefined">sink: {{ failure.sink_id ?? '—' }}</code>
            </td>
            <td>
              <template v-if="failure.resolved_at">
                <time :datetime="failure.resolved_at">{{
                  formatOperationTime(failure.resolved_at)
                }}</time>
                <span class="resolution-note" :title="failure.resolution ?? undefined">{{
                  failure.resolution ?? '—'
                }}</span>
              </template>
              <span v-else>Open</span>
            </td>
            <td class="action-cell">
              <button
                class="details-button"
                type="button"
                :aria-controls="`failure-details-${index}`"
                :aria-expanded="expandedIds.has(failure.failure_id)"
                @click="toggleDetails(failure.failure_id)"
              >
                {{ expandedIds.has(failure.failure_id) ? 'Hide' : 'Inspect' }}
              </button>
              <button
                v-if="!failure.resolved_at"
                class="resolve-button"
                type="button"
                @click="emit('resolve', failure)"
              >
                Resolve
              </button>
            </td>
          </tr>
          <tr
            v-if="expandedIds.has(failure.failure_id)"
            :id="`failure-details-${index}`"
            class="detail-row"
          >
            <td colspan="7">
              <pre>{{ formatRecord(failure.details) }}</pre>
            </td>
          </tr>
        </template>
      </tbody>
    </table>
  </div>
  <div v-else class="operation-empty">
    <strong>No failures match this filter</strong>
    <p>Unresolved ingestion, processing, and sink failures appear here.</p>
  </div>
</template>

<style scoped>
.operation-table-scroll {
  overflow: auto;
}

.operation-table {
  width: 100%;
  min-width: 82rem;
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
  max-width: 21rem;
  padding: 0.75rem 0.8rem;
  border-top: 1px solid var(--line-soft);
  color: #afbec1;
  vertical-align: top;
}

.operation-table tbody tr:not(.detail-row):hover {
  background: rgba(85, 201, 195, 0.05);
}

.operation-table td > strong,
.operation-table td > code,
.operation-table td > time,
.resolution-note {
  display: block;
}

.operation-table code,
.operation-table time {
  overflow: hidden;
  color: var(--cyan);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.62rem;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.operation-table td > code + code,
.resolution-note {
  margin-top: 0.25rem;
}

.message-cell,
.resolution-note {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.action-cell {
  white-space: nowrap;
}

.details-button,
.resolve-button {
  margin-right: 0.7rem;
  color: var(--cyan);
  border: 0;
  border-bottom: 1px solid currentColor;
  background: none;
  cursor: pointer;
  font-size: 0.65rem;
}

.resolve-button {
  color: var(--accent);
}

.detail-row pre {
  max-height: 18rem;
  margin: 0;
  padding: 0.85rem;
  overflow: auto;
  color: #9fb4b8;
  background: #081216;
  font-size: 0.65rem;
  white-space: pre-wrap;
  word-break: break-word;
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
