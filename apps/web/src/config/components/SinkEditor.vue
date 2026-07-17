<script setup lang="ts">
import type {
  PipeBoltDomainConfigHttpHeaderTemplate,
  PipeBoltDomainConfigSinkDefinition,
  PipeBoltDomainConfigSinkKind,
} from '@/api/generated'
import SecretInput from './SecretInput.vue'

const props = defineProps<{ modelValue: PipeBoltDomainConfigSinkDefinition[] }>()
const emit = defineEmits<{ 'update:modelValue': [value: PipeBoltDomainConfigSinkDefinition[]] }>()

function update(index: number, patch: Partial<PipeBoltDomainConfigSinkDefinition>): void {
  const next = structuredClone(props.modelValue)
  const current = next[index]
  if (!current) return
  next[index] = { ...current, ...patch }
  emit('update:modelValue', next)
}

function updateKind(index: number, patch: Partial<PipeBoltDomainConfigSinkKind>): void {
  const sink = props.modelValue[index]
  if (!sink) return
  update(index, { kind: { ...sink.kind, ...patch } as PipeBoltDomainConfigSinkKind })
}

function add(): void {
  emit('update:modelValue', [
    ...props.modelValue,
    {
      enabled: false,
      id: `sink-${crypto.randomUUID()}`,
      kind: {
        headers: [],
        method: 'POST',
        timeout: 5_000,
        type: 'webhook',
        url: 'https://example.com/events',
      },
      name: 'New webhook',
    },
  ])
}

function changeType(index: number, type: PipeBoltDomainConfigSinkKind['type']): void {
  update(index, {
    kind:
      type === 'webhook'
        ? { headers: [], method: 'POST', timeout: 5_000, type, url: 'https://example.com/events' }
        : { connection_ref: '', table: '', type },
  })
}

function updateHeader(
  index: number,
  headerIndex: number,
  patch: Partial<PipeBoltDomainConfigHttpHeaderTemplate>,
): void {
  const sink = props.modelValue[index]
  if (!sink || sink.kind.type !== 'webhook') return
  const headers = structuredClone(sink.kind.headers)
  const current = headers[headerIndex]
  if (!current) return
  headers[headerIndex] = { ...current, ...patch }
  updateKind(index, { headers })
}
</script>

<template>
  <section class="config-section">
    <div class="config-section-heading">
      <div>
        <p class="kicker">DELIVERY</p>
        <h2>Sinks</h2>
      </div>
      <button class="button button-secondary" type="button" @click="add">Add sink</button>
    </div>
    <p v-if="!modelValue.length" class="config-empty">No sinks configured.</p>
    <article v-for="(sink, index) in modelValue" :key="sink.id" class="config-item">
      <div class="config-item-heading">
        <div>
          <span class="config-index">{{ index + 1 }}</span
          ><strong>{{ sink.name }}</strong>
        </div>
        <div class="config-item-actions">
          <label class="toggle-label"
            ><input
              :checked="sink.enabled"
              type="checkbox"
              @change="
                update(index, { enabled: ($event.currentTarget as HTMLInputElement).checked })
              "
            />Enabled</label
          ><button
            class="danger-link"
            type="button"
            @click="
              emit(
                'update:modelValue',
                modelValue.filter((_, itemIndex) => itemIndex !== index),
              )
            "
          >
            Remove
          </button>
        </div>
      </div>
      <div class="config-form-grid config-form-grid-dense">
        <label class="field"
          ><span>ID</span
          ><input
            :value="sink.id"
            @input="update(index, { id: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
        <label class="field"
          ><span>Name</span
          ><input
            :value="sink.name"
            maxlength="160"
            @input="update(index, { name: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
        <label class="field"
          ><span>Sink type</span
          ><select
            :value="sink.kind.type"
            @change="
              changeType(
                index,
                ($event.currentTarget as HTMLSelectElement)
                  .value as PipeBoltDomainConfigSinkKind['type'],
              )
            "
          >
            <option value="webhook">Webhook</option>
            <option value="database">Database</option>
          </select></label
        >
      </div>

      <template v-if="sink.kind.type === 'webhook'">
        <div class="config-form-grid config-form-grid-dense">
          <label class="field field-wide"
            ><span>Webhook URL</span
            ><input
              :value="sink.kind.url"
              type="url"
              @input="updateKind(index, { url: ($event.currentTarget as HTMLInputElement).value })"
          /></label>
          <label class="field"
            ><span>HTTP method</span
            ><select
              :value="sink.kind.method"
              @change="
                updateKind(index, {
                  method: ($event.currentTarget as HTMLSelectElement)
                    .value as typeof sink.kind.method,
                })
              "
            >
              <option value="POST">POST</option>
              <option value="PUT">PUT</option>
              <option value="PATCH">PATCH</option>
            </select></label
          >
          <label class="field"
            ><span>Timeout (milliseconds)</span
            ><input
              :value="sink.kind.timeout"
              min="1"
              step="100"
              type="number"
              @input="
                updateKind(index, {
                  timeout: ($event.currentTarget as HTMLInputElement).valueAsNumber,
                })
              "
          /></label>
        </div>
        <div class="nested-list-heading">
          <strong>Secret headers</strong
          ><button
            type="button"
            @click="updateKind(index, { headers: [...sink.kind.headers, { name: '', value: '' }] })"
          >
            Add header
          </button>
        </div>
        <div
          v-for="(header, headerIndex) in sink.kind.headers"
          :key="`${sink.id}-${headerIndex}`"
          class="nested-row nested-row-header"
        >
          <label class="field"
            ><span>Header name</span
            ><input
              :value="header.name"
              autocomplete="off"
              @input="
                updateHeader(index, headerIndex, {
                  name: ($event.currentTarget as HTMLInputElement).value,
                })
              "
          /></label>
          <label class="field"
            ><span>Secret value</span
            ><SecretInput
              :model-value="header.value"
              @update:model-value="updateHeader(index, headerIndex, { value: $event })"
          /></label>
          <button
            class="icon-remove"
            type="button"
            aria-label="Remove header"
            @click="
              updateKind(index, {
                headers: sink.kind.headers.filter((_, itemIndex) => itemIndex !== headerIndex),
              })
            "
          >
            ×
          </button>
        </div>
      </template>
      <div v-else class="config-form-grid config-form-grid-dense">
        <label class="field"
          ><span>Connection reference</span
          ><input
            :value="sink.kind.connection_ref"
            @input="
              updateKind(index, {
                connection_ref: ($event.currentTarget as HTMLInputElement).value,
              })
            "
        /></label>
        <label class="field"
          ><span>Table</span
          ><input
            :value="sink.kind.table"
            @input="updateKind(index, { table: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
      </div>
    </article>
  </section>
</template>
