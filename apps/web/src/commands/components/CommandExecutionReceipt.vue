<script setup lang="ts">
import { computed } from 'vue'

import type {
  PipeBoltApiDtoCommandExecutionStatusResponse,
  PipeBoltApiDtoExecuteCommandResponse,
} from '@/api/generated'
import type { CommandTrackingState } from '../commands.types'

const props = defineProps<{
  receipt: PipeBoltApiDtoExecuteCommandResponse
  status: PipeBoltApiDtoCommandExecutionStatusResponse
  trackerError?: string
  trackingState: CommandTrackingState
}>()

const dateTime = new Intl.DateTimeFormat(undefined, {
  dateStyle: 'medium',
  timeStyle: 'medium',
})
const queuedAt = computed(() => {
  const date = new Date(props.receipt.queued_at)
  return Number.isNaN(date.getTime()) ? props.receipt.queued_at : dateTime.format(date)
})
const trackerLabel = computed(() => {
  switch (props.trackingState) {
    case 'polling':
      return 'Polling audit event'
    case 'settled':
      return 'Publish outcome recorded'
    case 'timed_out':
      return 'Monitoring window expired'
    case 'error':
      return 'Audit tracking unavailable'
    default:
      return 'Tracker idle'
  }
})
</script>

<template>
  <section class="receipt" aria-live="polite">
    <div class="receipt-status" :data-status="status">
      <span class="status-signal"></span>
      <div>
        <small>EXECUTION STATUS</small>
        <strong>{{ status }}</strong>
      </div>
      <span class="tracker-label">{{ trackerLabel }}</span>
    </div>

    <ol class="progress-track" aria-label="Command execution progress">
      <li class="complete">
        <span>1</span>
        <div><strong>Queued</strong><small>Accepted into local bounded queue</small></div>
      </li>
      <li :class="{ complete: status === 'published', failed: status === 'failed' }">
        <span>2</span>
        <div>
          <strong>{{ status === 'failed' ? 'Failed' : 'Published' }}</strong>
          <small>{{
            status === 'queued' ? 'Waiting for publish outcome' : 'MQTT publish outcome'
          }}</small>
        </div>
      </li>
    </ol>

    <p v-if="trackingState === 'timed_out'" class="tracker-notice">
      Audit event did not report a terminal publish status within 60 seconds. Receipt remains valid;
      verify operations logs before retrying.
    </p>
    <p v-else-if="trackerError" class="tracker-notice tracker-error">
      {{ trackerError }} Do not retry blindly; command may already be queued.
    </p>

    <dl class="receipt-facts">
      <div>
        <dt>Execution ID</dt>
        <dd>{{ receipt.command_execution_id }}</dd>
      </div>
      <div>
        <dt>Audit event ID</dt>
        <dd>{{ receipt.audit_event_id }}</dd>
      </div>
      <div class="fact-wide">
        <dt>Rendered topic</dt>
        <dd>{{ receipt.topic }}</dd>
      </div>
      <div>
        <dt>Broker</dt>
        <dd>{{ receipt.broker_id }}</dd>
      </div>
      <div>
        <dt>Queued at</dt>
        <dd>{{ queuedAt }}</dd>
      </div>
      <div>
        <dt>QoS / retain</dt>
        <dd>{{ receipt.qos.replaceAll('_', ' ') }} / {{ receipt.retain ? 'yes' : 'no' }}</dd>
      </div>
      <div>
        <dt>Payload</dt>
        <dd>{{ receipt.payload_size_bytes.toLocaleString() }} bytes</dd>
      </div>
    </dl>

    <p class="queue-warning">
      Queued means Pipe Bolt accepted the command into the local bounded queue. It does not mean the
      device processed it.
    </p>
  </section>
</template>

<style scoped>
.receipt {
  display: grid;
  gap: 1rem;
}

.receipt-status {
  display: flex;
  padding: 1rem;
  align-items: center;
  gap: 0.75rem;
  border: 1px solid var(--line);
  background: rgba(229, 167, 70, 0.07);
}

.receipt-status[data-status='published'] {
  border-color: rgba(98, 200, 149, 0.35);
  background: rgba(98, 200, 149, 0.08);
}

.receipt-status[data-status='failed'] {
  border-color: rgba(239, 128, 110, 0.35);
  background: rgba(239, 128, 110, 0.08);
}

.status-signal {
  width: 0.7rem;
  height: 0.7rem;
  flex: none;
  border-radius: 50%;
  background: var(--warning);
  box-shadow: 0 0 0.8rem rgba(229, 167, 70, 0.55);
}

[data-status='published'] .status-signal {
  background: var(--safe);
}

[data-status='failed'] .status-signal {
  background: var(--danger);
}

.receipt-status small,
.receipt-status strong {
  display: block;
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
}

.receipt-status small {
  color: var(--muted);
  font-size: 0.55rem;
}

.receipt-status strong {
  margin-top: 0.2rem;
  text-transform: uppercase;
}

.tracker-label {
  margin-left: auto;
  color: var(--muted);
  font-size: 0.65rem;
}

.progress-track {
  display: grid;
  margin: 0;
  padding: 0;
  grid-template-columns: 1fr 1fr;
  list-style: none;
}

.progress-track li {
  display: flex;
  padding: 0.8rem;
  align-items: center;
  gap: 0.65rem;
  color: var(--muted);
  border-bottom: 2px solid var(--line);
}

.progress-track li > span {
  display: grid;
  width: 1.6rem;
  height: 1.6rem;
  flex: none;
  place-items: center;
  border: 1px solid var(--line);
  font-size: 0.62rem;
}

.progress-track strong,
.progress-track small {
  display: block;
}

.progress-track strong {
  color: var(--text);
  font-size: 0.72rem;
}

.progress-track small {
  margin-top: 0.2rem;
  font-size: 0.6rem;
}

.progress-track .complete {
  border-color: var(--safe);
}

.progress-track .failed {
  border-color: var(--danger);
}

.tracker-notice,
.queue-warning {
  margin: 0;
  padding: 0.8rem;
  color: #e7bd76;
  border: 1px solid rgba(229, 167, 70, 0.25);
  background: rgba(75, 52, 18, 0.2);
  font-size: 0.7rem;
  line-height: 1.55;
}

.tracker-error {
  color: #ff9b8b;
  border-color: rgba(239, 128, 110, 0.3);
  background: rgba(80, 27, 24, 0.22);
}

.receipt-facts {
  display: grid;
  margin: 0;
  grid-template-columns: 1fr 1fr;
  border-top: 1px solid var(--line-soft);
  border-left: 1px solid var(--line-soft);
}

.receipt-facts div {
  min-width: 0;
  padding: 0.72rem;
  border-right: 1px solid var(--line-soft);
  border-bottom: 1px solid var(--line-soft);
}

.receipt-facts .fact-wide {
  grid-column: 1 / -1;
}

.receipt-facts dt {
  color: var(--muted);
  font-size: 0.58rem;
}

.receipt-facts dd {
  overflow-wrap: anywhere;
  margin: 0.3rem 0 0;
  color: #c8d5d7;
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.65rem;
}

.queue-warning {
  color: #b9c8ca;
  border-color: var(--line);
  background: #0b161b;
}

@media (max-width: 520px) {
  .tracker-label {
    display: none;
  }

  .progress-track,
  .receipt-facts {
    grid-template-columns: 1fr;
  }

  .receipt-facts .fact-wide {
    grid-column: auto;
  }
}
</style>
