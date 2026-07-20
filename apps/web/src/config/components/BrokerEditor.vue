<script setup lang="ts">
import type { PipeBoltDomainConfigBrokerConnectionConfig } from '@/api/generated'
import SecretInput from './SecretInput.vue'

const props = defineProps<{ modelValue: PipeBoltDomainConfigBrokerConnectionConfig[] }>()
const emit = defineEmits<{
  'update:modelValue': [value: PipeBoltDomainConfigBrokerConnectionConfig[]]
}>()

function update(index: number, patch: Partial<PipeBoltDomainConfigBrokerConnectionConfig>): void {
  const next = structuredClone(props.modelValue)
  const current = next[index]
  if (!current) return
  next[index] = { ...current, ...patch }
  emit('update:modelValue', next)
}

function add(): void {
  emit('update:modelValue', [
    ...props.modelValue,
    {
      clean_session: true,
      client_id: `pipe-bolt-${crypto.randomUUID()}`,
      credentials: null,
      enabled: false,
      host: 'localhost',
      id: `broker-${crypto.randomUUID()}`,
      keep_alive: 30,
      name: 'New broker',
      port: 1883,
      reconnect: { max_delay: 30_000, min_delay: 500 },
      tls: 'disabled',
    },
  ])
}

function remove(index: number): void {
  emit(
    'update:modelValue',
    props.modelValue.filter((_, itemIndex) => itemIndex !== index),
  )
}

function toggleCredentials(index: number, enabled: boolean): void {
  update(index, { credentials: enabled ? { password: '', username: '' } : null })
}
</script>

<template>
  <section class="config-section">
    <div class="config-section-heading">
      <div>
        <p class="kicker">INGRESS</p>
        <h2>Broker connections</h2>
      </div>
      <button class="button button-secondary" type="button" @click="add">Add broker</button>
    </div>

    <p v-if="!modelValue.length" class="config-empty">No brokers configured.</p>
    <article
      v-for="(broker, index) in modelValue"
      :key="broker.id"
      class="config-item"
      :aria-label="`Broker ${index + 1}: ${broker.name || 'Unnamed broker'}`"
    >
      <div class="config-item-heading">
        <div>
          <span class="config-index">{{ index + 1 }}</span
          ><strong>{{ broker.name || 'Unnamed broker' }}</strong>
        </div>
        <div class="config-item-actions">
          <label class="toggle-label"
            ><input
              :checked="broker.enabled"
              type="checkbox"
              @change="
                update(index, { enabled: ($event.currentTarget as HTMLInputElement).checked })
              "
            />Enabled</label
          >
          <button class="danger-link" type="button" @click="remove(index)">Remove</button>
        </div>
      </div>

      <div class="config-form-grid config-form-grid-dense">
        <label class="field"
          ><span>ID</span
          ><input
            :value="broker.id"
            @input="update(index, { id: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
        <label class="field"
          ><span>Name</span
          ><input
            :value="broker.name"
            maxlength="160"
            @input="update(index, { name: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
        <label class="field"
          ><span>Host</span
          ><input
            :value="broker.host"
            maxlength="255"
            @input="update(index, { host: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
        <label class="field"
          ><span>Port</span
          ><input
            :value="broker.port"
            max="65535"
            min="1"
            type="number"
            @input="
              update(index, { port: ($event.currentTarget as HTMLInputElement).valueAsNumber })
            "
        /></label>
        <label class="field"
          ><span>Client ID</span
          ><input
            :value="broker.client_id"
            maxlength="160"
            @input="update(index, { client_id: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
        <label class="field"
          ><span>Keep alive (seconds)</span
          ><input
            :value="broker.keep_alive"
            min="5"
            type="number"
            @input="
              update(index, {
                keep_alive: ($event.currentTarget as HTMLInputElement).valueAsNumber,
              })
            "
        /></label>
        <label class="field"
          ><span>TLS mode</span
          ><select
            :value="broker.tls"
            @change="
              update(index, {
                tls: ($event.currentTarget as HTMLSelectElement).value as typeof broker.tls,
              })
            "
          >
            <option value="disabled">Disabled</option>
            <option value="native_roots">Native roots</option>
          </select></label
        >
        <label class="toggle-field"
          ><input
            :checked="broker.clean_session"
            type="checkbox"
            @change="
              update(index, { clean_session: ($event.currentTarget as HTMLInputElement).checked })
            "
          /><span>Clean session</span></label
        >
        <label class="field"
          ><span>Reconnect min (ms)</span
          ><input
            :value="broker.reconnect.min_delay"
            min="1"
            type="number"
            @input="
              update(index, {
                reconnect: {
                  ...broker.reconnect,
                  min_delay: ($event.currentTarget as HTMLInputElement).valueAsNumber,
                },
              })
            "
        /></label>
        <label class="field"
          ><span>Reconnect max (ms)</span
          ><input
            :value="broker.reconnect.max_delay"
            min="1"
            type="number"
            @input="
              update(index, {
                reconnect: {
                  ...broker.reconnect,
                  max_delay: ($event.currentTarget as HTMLInputElement).valueAsNumber,
                },
              })
            "
        /></label>
      </div>

      <div class="secret-group">
        <label class="toggle-label"
          ><input
            :checked="Boolean(broker.credentials)"
            type="checkbox"
            @change="toggleCredentials(index, ($event.currentTarget as HTMLInputElement).checked)"
          />Username and password</label
        >
        <div v-if="broker.credentials" class="config-form-grid config-form-grid-dense">
          <label class="field"
            ><span>Username</span
            ><input
              :value="broker.credentials.username"
              maxlength="160"
              autocomplete="off"
              @input="
                update(index, {
                  credentials: {
                    ...broker.credentials!,
                    username: ($event.currentTarget as HTMLInputElement).value,
                  },
                })
              "
          /></label>
          <label class="field"
            ><span>Password</span
            ><SecretInput
              :model-value="broker.credentials.password"
              @update:model-value="
                update(index, { credentials: { ...broker.credentials!, password: $event } })
              "
          /></label>
        </div>
      </div>
    </article>
  </section>
</template>
