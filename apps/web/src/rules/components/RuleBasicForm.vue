<script setup lang="ts">
import type {
  PipeBoltDomainConfigCommandTemplate,
  PipeBoltDomainConfigTopicRouteConfig,
} from '@/api/generated'
import type { RuleFormDraft, RuleTriggerType } from '../rules.types'

const props = defineProps<{
  isNew: boolean
  modelValue: RuleFormDraft
  routes: ReadonlyArray<PipeBoltDomainConfigTopicRouteConfig>
  templates: ReadonlyArray<PipeBoltDomainConfigCommandTemplate>
}>()
const emit = defineEmits<{ 'update:modelValue': [value: RuleFormDraft] }>()

function patch(value: Partial<RuleFormDraft>): void {
  emit('update:modelValue', { ...props.modelValue, ...value })
}

function updateTrigger(event: Event): void {
  const triggerType = (event.currentTarget as HTMLSelectElement).value as RuleTriggerType
  const triggerTargetId =
    triggerType === 'route_matched'
      ? (props.routes[0]?.id ?? '')
      : triggerType === 'command_requested'
        ? (props.templates[0]?.id ?? '')
        : ''
  patch({ triggerTargetId, triggerType })
}
</script>

<template>
  <section class="builder-section">
    <header class="builder-section-heading">
      <div>
        <p class="kicker">IDENTITY</p>
        <h2>Rule definition</h2>
      </div>
      <label class="rule-enabled">
        <input
          :checked="modelValue.enabled"
          type="checkbox"
          @change="patch({ enabled: ($event.currentTarget as HTMLInputElement).checked })"
        />
        Enabled
      </label>
    </header>
    <div class="basic-grid">
      <label class="field">
        <span>Rule ID</span>
        <input
          :disabled="!isNew"
          :value="modelValue.id"
          autocomplete="off"
          maxlength="128"
          @input="patch({ id: ($event.currentTarget as HTMLInputElement).value })"
        />
      </label>
      <label class="field">
        <span>Name</span>
        <input
          :value="modelValue.name"
          autocomplete="off"
          maxlength="160"
          @input="patch({ name: ($event.currentTarget as HTMLInputElement).value })"
        />
      </label>
      <label class="field">
        <span>Trigger</span>
        <select :value="modelValue.triggerType" @change="updateTrigger">
          <option value="event_received">Event received</option>
          <option value="route_matched">Route matched</option>
          <option value="command_requested">Command requested</option>
        </select>
      </label>
      <label v-if="modelValue.triggerType === 'route_matched'" class="field">
        <span>Route</span>
        <select
          :value="modelValue.triggerTargetId"
          @change="patch({ triggerTargetId: ($event.currentTarget as HTMLSelectElement).value })"
        >
          <option value="">Select route</option>
          <option v-for="route in routes" :key="route.id" :value="route.id">
            {{ route.name }} · {{ route.id }}{{ route.enabled ? '' : ' · disabled' }}
          </option>
        </select>
      </label>
      <label v-else-if="modelValue.triggerType === 'command_requested'" class="field">
        <span>Command template</span>
        <select
          :value="modelValue.triggerTargetId"
          @change="patch({ triggerTargetId: ($event.currentTarget as HTMLSelectElement).value })"
        >
          <option value="">Select command template</option>
          <option v-for="template in templates" :key="template.id" :value="template.id">
            {{ template.name }} · {{ template.id }}{{ template.enabled ? '' : ' · disabled' }}
          </option>
        </select>
      </label>
    </div>
    <p v-if="modelValue.triggerType === 'command_requested'" class="runtime-compatibility-note">
      Contract supports this trigger, but current runtime compiler may reject it during reload.
    </p>
  </section>
</template>

<style scoped>
.basic-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0.75rem;
}

.rule-enabled {
  display: flex;
  align-items: center;
  gap: 0.45rem;
  color: var(--muted);
  font-size: 0.72rem;
}

.rule-enabled input {
  accent-color: var(--accent);
}

.runtime-compatibility-note {
  margin: 0.8rem 0 0;
  padding: 0.65rem;
  color: #e8b967;
  border: 1px solid rgba(229, 167, 70, 0.25);
  background: rgba(75, 52, 18, 0.18);
  font-size: 0.68rem;
}

@media (max-width: 620px) {
  .basic-grid {
    grid-template-columns: 1fr;
  }
}
</style>
