<script setup lang="ts">
import { shallowRef } from 'vue'
import type { PipeBoltDomainConfigCommandTemplate } from '@/api/generated'

const props = defineProps<{
  brokers: ReadonlyArray<{ id: string; name: string }>
  modelValue: PipeBoltDomainConfigCommandTemplate[]
}>()
const emit = defineEmits<{ 'update:modelValue': [value: PipeBoltDomainConfigCommandTemplate[]] }>()
const payloadErrors = shallowRef<Record<number, string>>({})

function update(index: number, patch: Partial<PipeBoltDomainConfigCommandTemplate>): void {
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
      broker_id: props.brokers[0]?.id ?? '',
      enabled: false,
      id: `command-${crypto.randomUUID()}`,
      name: 'New command',
      payload_template: {},
      qos: 'at_least_once',
      retain: false,
      topic_template: 'devices/{device_id}/commands',
    },
  ])
}

function updatePayload(index: number, event: Event): void {
  const value = (event.currentTarget as HTMLTextAreaElement).value
  try {
    update(index, { payload_template: JSON.parse(value) as unknown })
    const next = { ...payloadErrors.value }
    delete next[index]
    payloadErrors.value = next
  } catch {
    payloadErrors.value = {
      ...payloadErrors.value,
      [index]: 'Payload template must be valid JSON.',
    }
  }
}
</script>

<template>
  <section class="config-section">
    <div class="config-section-heading">
      <div>
        <p class="kicker">CONTROL</p>
        <h2>Command templates</h2>
      </div>
      <button class="button button-secondary" type="button" @click="add">Add template</button>
    </div>
    <p v-if="!modelValue.length" class="config-empty">No command templates configured.</p>
    <article v-for="(template, index) in modelValue" :key="template.id" class="config-item">
      <div class="config-item-heading">
        <div>
          <span class="config-index">{{ index + 1 }}</span
          ><strong>{{ template.name }}</strong>
        </div>
        <div class="config-item-actions">
          <label class="toggle-label"
            ><input
              :checked="template.enabled"
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
            :value="template.id"
            @input="update(index, { id: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
        <label class="field"
          ><span>Name</span
          ><input
            :value="template.name"
            maxlength="160"
            @input="update(index, { name: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
        <label class="field"
          ><span>Broker</span
          ><select
            :value="template.broker_id"
            @change="
              update(index, { broker_id: ($event.currentTarget as HTMLSelectElement).value })
            "
          >
            <option value="">Select broker</option>
            <option v-for="broker in brokers" :key="broker.id" :value="broker.id">
              {{ broker.name }} · {{ broker.id }}
            </option>
          </select></label
        >
        <label class="field"
          ><span>QoS</span
          ><select
            :value="template.qos"
            @change="
              update(index, {
                qos: ($event.currentTarget as HTMLSelectElement).value as typeof template.qos,
              })
            "
          >
            <option value="at_most_once">At most once</option>
            <option value="at_least_once">At least once</option>
            <option value="exactly_once">Exactly once</option>
          </select></label
        >
        <label class="field field-wide"
          ><span>Topic template</span
          ><input
            :value="template.topic_template"
            maxlength="1024"
            @input="
              update(index, { topic_template: ($event.currentTarget as HTMLInputElement).value })
            "
        /></label>
        <label class="toggle-field"
          ><input
            :checked="template.retain"
            type="checkbox"
            @change="update(index, { retain: ($event.currentTarget as HTMLInputElement).checked })"
          /><span>Retain message</span></label
        >
        <label class="field field-wide"
          ><span>Payload template (JSON)</span
          ><textarea
            :value="JSON.stringify(template.payload_template, null, 2)"
            rows="6"
            spellcheck="false"
            @change="updatePayload(index, $event)"
          ></textarea
          ><small v-if="payloadErrors[index]" class="field-error">{{
            payloadErrors[index]
          }}</small></label
        >
      </div>
    </article>
  </section>
</template>
