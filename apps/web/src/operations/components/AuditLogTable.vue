<script setup lang="ts">
import { shallowRef } from 'vue'

import type { PipeBoltApiDtoAuditEventResponse } from '@/api/generated'
import { auditStatusTone, formatOperationTime, formatRecord } from '../operations.format'
import OperationStatusBadge from './OperationStatusBadge.vue'

defineProps<{ items: ReadonlyArray<PipeBoltApiDtoAuditEventResponse> }>()
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
    aria-label="Audit event table"
    tabindex="0"
  >
    <table class="operation-table">
      <thead>
        <tr>
          <th>Occurred</th>
          <th>Action</th>
          <th>Status</th>
          <th>Target</th>
          <th>Actor</th>
          <th>Reason</th>
          <th><span class="visually-hidden">Details</span></th>
        </tr>
      </thead>
      <tbody>
        <template v-for="event in items" :key="event.audit_event_id">
          <tr>
            <td>
              <time :datetime="event.occurred_at">{{
                formatOperationTime(event.occurred_at)
              }}</time>
            </td>
            <td>
              <code>{{ event.action }}</code>
            </td>
            <td>
              <OperationStatusBadge :label="event.status" :tone="auditStatusTone(event.status)" />
            </td>
            <td>
              <span>{{ event.target_type }}</span
              ><code :title="event.target_id">{{ event.target_id }}</code>
            </td>
            <td>
              <code>{{ event.actor_id ?? 'system' }}</code>
            </td>
            <td class="message-cell" :title="event.reason ?? undefined">
              {{ event.reason ?? '—' }}
            </td>
            <td>
              <button
                class="details-button"
                type="button"
                :aria-expanded="expandedIds.has(event.audit_event_id)"
                @click="toggleDetails(event.audit_event_id)"
              >
                {{ expandedIds.has(event.audit_event_id) ? 'Hide' : 'Inspect' }}
              </button>
            </td>
          </tr>
          <tr v-if="expandedIds.has(event.audit_event_id)" class="detail-row">
            <td colspan="7">
              <pre>{{ formatRecord(event.metadata) }}</pre>
            </td>
          </tr>
        </template>
      </tbody>
    </table>
  </div>
  <div v-else class="operation-empty">
    <strong>No audit events in this page</strong>
    <p>Config mutations, command execution, and failure resolution appear here.</p>
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
  max-width: 18rem;
  padding: 0.75rem 0.8rem;
  border-top: 1px solid var(--line-soft);
  color: #afbec1;
  vertical-align: top;
}

.operation-table tbody tr:not(.detail-row):hover {
  background: rgba(85, 201, 195, 0.05);
}

.operation-table td > span,
.operation-table td > code {
  display: block;
}

.operation-table code,
.operation-table time {
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.64rem;
}

.operation-table td > code {
  overflow: hidden;
  margin-top: 0.2rem;
  color: var(--cyan);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.message-cell {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.details-button {
  color: var(--cyan);
  border: 0;
  border-bottom: 1px solid currentColor;
  background: none;
  cursor: pointer;
  font-size: 0.65rem;
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

.visually-hidden {
  position: absolute;
  width: 1px;
  height: 1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
}
</style>
