<script setup lang="ts">
import type { RuleFieldDraft, RuleFieldSource } from '../rules.types'

const props = defineProps<{
  modelValue: RuleFieldDraft
}>()
const emit = defineEmits<{ 'update:modelValue': [value: RuleFieldDraft] }>()

function updateSource(event: Event): void {
  emit('update:modelValue', {
    source: (event.currentTarget as HTMLSelectElement).value as RuleFieldSource,
    value: '',
  })
}

function updateValue(event: Event): void {
  emit('update:modelValue', {
    ...props.modelValue,
    value: (event.currentTarget as HTMLInputElement).value,
  })
}
</script>

<template>
  <div class="field-editor">
    <label class="field">
      <span>Field source</span>
      <select :value="modelValue.source" @change="updateSource">
        <option value="event">Event</option>
        <option value="payload">Payload</option>
        <option value="extracted">Extracted</option>
        <option value="device_id">Device ID</option>
        <option value="event_type">Event type</option>
        <option value="topic">Topic</option>
      </select>
    </label>
    <label v-if="['event', 'payload', 'extracted'].includes(modelValue.source)" class="field">
      <span>{{ modelValue.source === 'extracted' ? 'Field name' : 'Dot path' }}</span>
      <input
        :value="modelValue.value"
        autocomplete="off"
        maxlength="256"
        :placeholder="modelValue.source === 'event' ? 'fields.temperature' : 'temperature.value'"
        @input="updateValue"
      />
    </label>
  </div>
</template>

<style scoped>
.field-editor {
  display: grid;
  grid-template-columns: minmax(7rem, 0.65fr) minmax(10rem, 1.35fr);
  gap: 0.55rem;
}

@media (max-width: 560px) {
  .field-editor {
    grid-template-columns: 1fr;
  }
}
</style>
