<script setup lang="ts">
import type { RealtimeFilterErrors, RealtimeFilterInput } from '../realtime.types'

const props = defineProps<{
  errors: RealtimeFilterErrors
  modelValue: RealtimeFilterInput
}>()
const emit = defineEmits<{
  clear: []
  'update:modelValue': [value: RealtimeFilterInput]
}>()

function update(field: keyof RealtimeFilterInput, value: string): void {
  emit('update:modelValue', { ...props.modelValue, [field]: value })
}
</script>

<template>
  <section class="filter-panel panel">
    <div class="filter-heading">
      <div>
        <p class="kicker">SERVER FILTERS</p>
        <h2>Event scope</h2>
      </div>
      <button type="button" @click="emit('clear')">Clear filters</button>
    </div>
    <div class="filter-grid">
      <label class="field">
        <span>Device ID</span>
        <input
          :value="modelValue.deviceId"
          placeholder="device-01"
          @input="update('deviceId', ($event.currentTarget as HTMLInputElement).value)"
        />
        <small v-if="errors.deviceId">{{ errors.deviceId }}</small>
      </label>
      <label class="field">
        <span>Event type</span>
        <input
          :value="modelValue.eventType"
          placeholder="telemetry"
          @input="update('eventType', ($event.currentTarget as HTMLInputElement).value)"
        />
        <small v-if="errors.eventType">{{ errors.eventType }}</small>
      </label>
      <label class="field">
        <span>Route ID</span>
        <input
          :value="modelValue.routeId"
          placeholder="route-main"
          @input="update('routeId', ($event.currentTarget as HTMLInputElement).value)"
        />
        <small v-if="errors.routeId">{{ errors.routeId }}</small>
      </label>
      <label class="field">
        <span>Exact topic</span>
        <input
          :value="modelValue.topic"
          placeholder="devices/01/telemetry"
          @input="update('topic', ($event.currentTarget as HTMLInputElement).value)"
        />
        <small v-if="errors.topic">{{ errors.topic }}</small>
      </label>
      <label class="field">
        <span>Topic prefix</span>
        <input
          :value="modelValue.topicPrefix"
          placeholder="devices/01"
          @input="update('topicPrefix', ($event.currentTarget as HTMLInputElement).value)"
        />
        <small v-if="errors.topicPrefix">{{ errors.topicPrefix }}</small>
      </label>
    </div>
    <p class="filter-note">
      Valid changes reconnect automatically after a short debounce. Wildcards are blocked.
    </p>
  </section>
</template>

<style scoped>
.filter-panel {
  padding: 1.2rem;
}

.filter-heading {
  display: flex;
  margin-bottom: 1rem;
  align-items: center;
  justify-content: space-between;
}

.filter-heading h2,
.filter-heading p {
  margin-bottom: 0;
}

.filter-heading button {
  padding: 0;
  color: var(--muted);
  border: 0;
  border-bottom: 1px solid currentColor;
  background: transparent;
  cursor: pointer;
  font-size: 0.68rem;
}

.filter-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(8rem, 1fr));
  gap: 0.75rem;
}

.field small {
  color: var(--danger);
  font-size: 0.62rem;
}

.filter-note {
  margin: 0.8rem 0 0;
  color: var(--muted);
  font-size: 0.66rem;
}

@media (max-width: 1100px) {
  .filter-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 600px) {
  .filter-grid {
    grid-template-columns: 1fr;
  }
}
</style>
