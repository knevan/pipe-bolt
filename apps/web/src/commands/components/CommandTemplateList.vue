<script setup lang="ts">
import { computed } from 'vue'

import type { PipeBoltDomainConfigCommandTemplate } from '@/api/generated'
import type { CommandBrokerSummary } from '../commands.types'

const props = defineProps<{
  brokers: ReadonlyArray<CommandBrokerSummary>
  templates: ReadonlyArray<PipeBoltDomainConfigCommandTemplate>
}>()
const emit = defineEmits<{ execute: [template: PipeBoltDomainConfigCommandTemplate] }>()
const MAX_PAYLOAD_PREVIEW_CHARS = 12_000

const brokerNames = computed(
  () => new Map(props.brokers.map((broker) => [broker.id, broker.name] as const)),
)

function formatPayload(value: unknown): string {
  let output: string
  try {
    output = JSON.stringify(value, null, 2) ?? 'null'
  } catch {
    return 'Unable to serialize payload template.'
  }
  return output.length > MAX_PAYLOAD_PREVIEW_CHARS
    ? `${output.slice(0, MAX_PAYLOAD_PREVIEW_CHARS)}\n... truncated ...`
    : output
}
</script>

<template>
  <section v-if="templates.length" class="template-grid" aria-label="Command templates">
    <article v-for="template in templates" :key="template.id" class="template-card panel">
      <header class="template-heading">
        <div>
          <p class="kicker">{{ template.id }}</p>
          <h2>{{ template.name }}</h2>
        </div>
        <span class="template-state" :class="{ enabled: template.enabled }">
          {{ template.enabled ? 'ENABLED' : 'DISABLED' }}
        </span>
      </header>

      <dl class="template-facts">
        <div>
          <dt>Broker target</dt>
          <dd>{{ brokerNames.get(template.broker_id) ?? 'Unknown broker' }}</dd>
          <small>{{ template.broker_id }}</small>
        </div>
        <div>
          <dt>QoS</dt>
          <dd>{{ template.qos.replaceAll('_', ' ') }}</dd>
        </div>
        <div>
          <dt>Retain</dt>
          <dd>{{ template.retain ? 'yes' : 'no' }}</dd>
        </div>
      </dl>

      <section class="template-code">
        <span>TOPIC TEMPLATE</span>
        <code>{{ template.topic_template }}</code>
      </section>
      <details class="payload-details">
        <summary>Payload template</summary>
        <pre>{{ formatPayload(template.payload_template) }}</pre>
      </details>

      <footer class="template-actions">
        <span v-if="!template.enabled"
          >Enable this template in configuration before execution.</span
        >
        <span v-else>Execution requires parameters, audit reason, and confirmation.</span>
        <button
          class="button button-primary"
          type="button"
          :disabled="!template.enabled"
          @click="emit('execute', template)"
        >
          Execute command
        </button>
      </footer>
    </article>
  </section>

  <section v-else class="panel empty-catalog">
    <p class="kicker">NO TEMPLATES</p>
    <h2>Command catalog is empty</h2>
    <p>Add a governed command template in project configuration, save it, then reload runtime.</p>
  </section>
</template>

<style scoped>
.template-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(min(100%, 28rem), 1fr));
  gap: 1rem;
}

.template-card {
  display: grid;
  min-width: 0;
  overflow: hidden;
  grid-template-rows: auto auto auto 1fr auto;
}

.template-heading,
.template-actions {
  display: flex;
  padding: 1.15rem 1.25rem;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
}

.template-heading {
  border-bottom: 1px solid var(--line-soft);
}

.template-heading h2,
.template-heading p {
  margin-bottom: 0;
}

.template-heading .kicker {
  overflow-wrap: anywhere;
}

.template-state {
  flex: none;
  padding: 0.28rem 0.42rem;
  color: var(--muted);
  border: 1px solid var(--line);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.55rem;
  letter-spacing: 0.08em;
}

.template-state.enabled {
  color: var(--safe);
  border-color: rgba(98, 200, 149, 0.38);
  background: rgba(98, 200, 149, 0.08);
}

.template-facts {
  display: grid;
  margin: 0;
  grid-template-columns: 1.5fr 1fr 0.65fr;
  border-bottom: 1px solid var(--line-soft);
}

.template-facts div {
  min-width: 0;
  padding: 0.85rem 1rem;
  border-right: 1px solid var(--line-soft);
}

.template-facts div:last-child {
  border-right: 0;
}

.template-facts dt,
.template-code span {
  color: var(--muted);
  font-size: 0.58rem;
  letter-spacing: 0.06em;
}

.template-facts dd {
  overflow: hidden;
  margin: 0.3rem 0 0;
  font-size: 0.76rem;
  text-overflow: ellipsis;
  text-transform: capitalize;
  white-space: nowrap;
}

.template-facts small {
  display: block;
  overflow: hidden;
  margin-top: 0.2rem;
  color: var(--muted);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.55rem;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.template-code {
  display: grid;
  padding: 0.9rem 1rem;
  gap: 0.35rem;
  border-bottom: 1px solid var(--line-soft);
  background: #0b161b;
}

.template-code code {
  overflow-wrap: anywhere;
  color: var(--cyan);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.7rem;
}

.payload-details {
  min-height: 3.3rem;
  padding: 0.9rem 1rem;
  color: var(--muted);
  border-bottom: 1px solid var(--line-soft);
  font-size: 0.7rem;
}

.payload-details summary {
  cursor: pointer;
  font-weight: 700;
}

.payload-details pre {
  max-height: 22rem;
  overflow: auto;
  margin: 0.8rem 0 0;
  padding: 0.75rem;
  color: #c8d5d7;
  border: 1px solid var(--line-soft);
  background: #081216;
  font-size: 0.65rem;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.template-actions {
  align-items: end;
}

.template-actions span {
  max-width: 25rem;
  color: var(--muted);
  font-size: 0.68rem;
  line-height: 1.5;
}

.template-actions .button {
  flex: none;
}

.empty-catalog {
  padding: clamp(1.5rem, 4vw, 3rem);
  text-align: center;
}

.empty-catalog p:last-child {
  margin-bottom: 0;
  color: var(--muted);
}

@media (max-width: 560px) {
  .template-facts {
    grid-template-columns: 1fr 1fr;
  }

  .template-facts div:first-child {
    grid-column: 1 / -1;
    border-bottom: 1px solid var(--line-soft);
  }

  .template-actions {
    align-items: stretch;
    flex-direction: column;
  }
}
</style>
