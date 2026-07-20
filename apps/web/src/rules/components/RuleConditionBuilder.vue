<script setup lang="ts">
import type { RuleConditionDraft } from '../rules.types'
import RuleConditionNode from './RuleConditionNode.vue'

defineProps<{
  condition: RuleConditionDraft
  enabled: boolean
}>()
const emit = defineEmits<{
  'update:condition': [value: RuleConditionDraft]
  'update:enabled': [value: boolean]
}>()
</script>

<template>
  <section class="builder-section condition-builder">
    <header class="builder-section-heading">
      <div>
        <p class="kicker">MATCH LOGIC</p>
        <h2>Condition</h2>
      </div>
      <label class="condition-toggle">
        <input
          :checked="enabled"
          type="checkbox"
          @change="emit('update:enabled', ($event.currentTarget as HTMLInputElement).checked)"
        />
        Apply condition
      </label>
    </header>
    <p v-if="!enabled" class="condition-disabled">
      No condition. Rule matches every event accepted by its trigger.
    </p>
    <RuleConditionNode
      v-else
      :depth="1"
      :model-value="condition"
      @update:model-value="emit('update:condition', $event)"
    />
  </section>
</template>

<style scoped>
.condition-toggle {
  display: flex;
  align-items: center;
  gap: 0.45rem;
  color: var(--muted);
  font-size: 0.7rem;
}

.condition-toggle input {
  accent-color: var(--accent);
}

.condition-disabled {
  margin: 0;
  padding: 1rem;
  color: var(--muted);
  border: 1px dashed var(--line);
  font-size: 0.75rem;
}
</style>
