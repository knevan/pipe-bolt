<script setup lang="ts">
import type { RuleLiteralKind, RuleValueDraft } from '../rules.types'
import RuleFieldEditor from './RuleFieldEditor.vue'

const props = defineProps<{
  label: string
  modelValue: RuleValueDraft
}>()
const emit = defineEmits<{ 'update:modelValue': [value: RuleValueDraft] }>()

function patch(value: Partial<RuleValueDraft>): void {
  emit('update:modelValue', { ...props.modelValue, ...value })
}

function updateType(event: Event): void {
  patch({ type: (event.currentTarget as HTMLSelectElement).value as RuleValueDraft['type'] })
}

function updateLiteralKind(event: Event): void {
  const literalKind = (event.currentTarget as HTMLSelectElement).value as RuleLiteralKind
  const literalValue =
    literalKind === 'number'
      ? '0'
      : literalKind === 'boolean'
        ? 'false'
        : literalKind === 'json'
          ? '{}'
          : ''
  patch({ literalKind, literalValue })
}
</script>

<template>
  <fieldset class="value-editor">
    <legend>{{ label }}</legend>
    <label class="field expression-type">
      <span>Expression</span>
      <select :value="modelValue.type" @change="updateType">
        <option value="field">Field</option>
        <option value="literal">Literal</option>
      </select>
    </label>
    <RuleFieldEditor
      v-if="modelValue.type === 'field'"
      :model-value="modelValue.field"
      @update:model-value="patch({ field: $event })"
    />
    <div v-else class="literal-editor">
      <label class="field">
        <span>Literal type</span>
        <select :value="modelValue.literalKind" @change="updateLiteralKind">
          <option value="string">String</option>
          <option value="number">Number</option>
          <option value="boolean">Boolean</option>
          <option value="null">Null</option>
          <option value="json">JSON</option>
        </select>
      </label>
      <label v-if="modelValue.literalKind === 'boolean'" class="field">
        <span>Value</span>
        <select
          :value="modelValue.literalValue"
          @change="patch({ literalValue: ($event.currentTarget as HTMLSelectElement).value })"
        >
          <option value="false">false</option>
          <option value="true">true</option>
        </select>
      </label>
      <label v-else-if="modelValue.literalKind === 'json'" class="field literal-wide">
        <span>JSON value</span>
        <textarea
          :value="modelValue.literalValue"
          maxlength="8192"
          rows="4"
          spellcheck="false"
          @input="patch({ literalValue: ($event.currentTarget as HTMLTextAreaElement).value })"
        ></textarea>
      </label>
      <label v-else-if="modelValue.literalKind !== 'null'" class="field">
        <span>Value</span>
        <input
          :value="modelValue.literalValue"
          autocomplete="off"
          :inputmode="modelValue.literalKind === 'number' ? 'decimal' : 'text'"
          maxlength="8192"
          :type="modelValue.literalKind === 'number' ? 'number' : 'text'"
          step="any"
          @input="patch({ literalValue: ($event.currentTarget as HTMLInputElement).value })"
        />
      </label>
      <p v-else class="null-value">Literal value is <code>null</code>.</p>
    </div>
  </fieldset>
</template>

<style scoped>
.value-editor {
  min-width: 0;
  margin: 0;
  padding: 0.7rem;
  border: 1px solid var(--line-soft);
}

.value-editor legend {
  padding: 0 0.35rem;
  color: var(--muted);
  font-size: 0.6rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.expression-type {
  margin-bottom: 0.55rem;
}

.literal-editor {
  display: grid;
  grid-template-columns: 8rem minmax(0, 1fr);
  gap: 0.55rem;
}

.literal-wide {
  grid-column: auto;
}

.null-value {
  align-self: end;
  margin: 0 0 0.7rem;
  color: var(--muted);
  font-size: 0.68rem;
}

.null-value code {
  color: var(--cyan);
}

@media (max-width: 560px) {
  .literal-editor {
    grid-template-columns: 1fr;
  }
}
</style>
