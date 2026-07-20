<script setup lang="ts">
import { computed } from 'vue'

import { createConditionDraft } from '../rules.mapper'
import type { RuleConditionDraft, RuleConditionOperator } from '../rules.types'
import RuleFieldEditor from './RuleFieldEditor.vue'
import RuleValueEditor from './RuleValueEditor.vue'

defineOptions({ name: 'RuleConditionNode' })

const props = defineProps<{
  depth: number
  modelValue: RuleConditionDraft
}>()
const emit = defineEmits<{ 'update:modelValue': [value: RuleConditionDraft] }>()
const MAX_DEPTH = 3
const MAX_GROUP_CHILDREN = 16
const groupOperators = new Set<RuleConditionOperator>(['and', 'or', 'not'])

const operatorOptions = computed(() => {
  const options: Array<{ label: string; value: RuleConditionOperator }> = [
    { label: 'Exists', value: 'exists' },
    { label: 'Equals', value: 'equals' },
    { label: 'Not equals', value: 'not_equals' },
    { label: 'Greater than', value: 'greater_than' },
    { label: 'Greater or equal', value: 'greater_than_or_equal' },
    { label: 'Less than', value: 'less_than' },
    { label: 'Less or equal', value: 'less_than_or_equal' },
    { label: 'Contains', value: 'contains' },
  ]
  if (props.depth < MAX_DEPTH || groupOperators.has(props.modelValue.op)) {
    options.push(
      { label: 'All (AND)', value: 'and' },
      { label: 'Any (OR)', value: 'or' },
      { label: 'Not', value: 'not' },
    )
  }
  return options
})

function changeOperator(event: Event): void {
  const op = (event.currentTarget as HTMLSelectElement).value as RuleConditionOperator
  const comparisons = new Set<RuleConditionOperator>([
    'equals',
    'not_equals',
    'greater_than',
    'greater_than_or_equal',
    'less_than',
    'less_than_or_equal',
    'contains',
  ])
  if (comparisons.has(op) && comparisons.has(props.modelValue.op)) {
    emit('update:modelValue', { ...props.modelValue, op })
  } else {
    emit('update:modelValue', createConditionDraft(op, props.modelValue.key))
  }
}

function patch(value: Partial<RuleConditionDraft>): void {
  emit('update:modelValue', { ...props.modelValue, ...value })
}

function addChild(): void {
  if (props.modelValue.children.length >= MAX_GROUP_CHILDREN || props.depth >= MAX_DEPTH) return
  patch({ children: [...props.modelValue.children, createConditionDraft()] })
}

function updateChild(index: number, value: RuleConditionDraft): void {
  patch({
    children: props.modelValue.children.map((child, childIndex) =>
      childIndex === index ? value : child,
    ),
  })
}

function removeChild(index: number): void {
  patch({ children: props.modelValue.children.filter((_, childIndex) => childIndex !== index) })
}
</script>

<template>
  <article class="condition-node" :data-depth="depth">
    <header class="condition-node-header">
      <span>DEPTH {{ depth }}</span>
      <label>
        <span>Operator</span>
        <select :value="modelValue.op" @change="changeOperator">
          <option v-for="option in operatorOptions" :key="option.value" :value="option.value">
            {{ option.label }}
          </option>
        </select>
      </label>
    </header>

    <RuleFieldEditor
      v-if="modelValue.op === 'exists'"
      :model-value="modelValue.field"
      @update:model-value="patch({ field: $event })"
    />
    <div v-else-if="!['and', 'or', 'not'].includes(modelValue.op)" class="comparison-grid">
      <RuleValueEditor
        label="Left operand"
        :model-value="modelValue.left"
        @update:model-value="patch({ left: $event })"
      />
      <RuleValueEditor
        label="Right operand"
        :model-value="modelValue.right"
        @update:model-value="patch({ right: $event })"
      />
    </div>
    <div v-else class="condition-children">
      <div v-for="(child, index) in modelValue.children" :key="child.key" class="child-row">
        <RuleConditionNode
          :depth="depth + 1"
          :model-value="child"
          @update:model-value="updateChild(index, $event)"
        />
        <button
          v-if="modelValue.op !== 'not'"
          class="remove-condition"
          type="button"
          aria-label="Remove nested condition"
          @click="removeChild(index)"
        >
          Remove
        </button>
      </div>
      <button
        v-if="modelValue.op !== 'not' && depth < MAX_DEPTH"
        class="add-condition"
        type="button"
        :disabled="modelValue.children.length >= MAX_GROUP_CHILDREN"
        @click="addChild"
      >
        + Add nested condition
      </button>
    </div>
  </article>
</template>

<style scoped>
.condition-node {
  min-width: 0;
  padding: 0.8rem;
  border: 1px solid var(--line);
  background: rgba(8, 18, 22, 0.68);
}

.condition-node[data-depth='2'] {
  border-left-color: var(--cyan);
}

.condition-node[data-depth='3'] {
  border-left-color: var(--accent);
}

.condition-node-header {
  display: flex;
  margin-bottom: 0.7rem;
  align-items: end;
  justify-content: space-between;
  gap: 1rem;
}

.condition-node-header > span,
.condition-node-header label span {
  color: var(--muted);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.52rem;
  letter-spacing: 0.07em;
}

.condition-node-header label {
  display: grid;
  min-width: 11rem;
  gap: 0.25rem;
}

.condition-node-header select {
  height: 2.3rem;
  padding: 0 0.55rem;
  color: var(--text);
  border: 1px solid var(--line);
  background: #0b161b;
}

.comparison-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0.65rem;
}

.condition-children {
  display: grid;
  gap: 0.65rem;
}

.child-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: start;
  gap: 0.45rem;
}

.remove-condition,
.add-condition {
  padding: 0.45rem;
  color: var(--muted);
  border: 1px solid var(--line);
  background: transparent;
  cursor: pointer;
  font-size: 0.62rem;
}

.remove-condition:hover {
  color: var(--danger);
}

.add-condition {
  justify-self: start;
  color: var(--cyan);
}

@media (max-width: 760px) {
  .comparison-grid,
  .child-row {
    grid-template-columns: 1fr;
  }

  .remove-condition {
    justify-self: start;
  }
}
</style>
