<script setup lang="ts">
import type {
  PipeBoltDomainConfigFieldMapping,
  PipeBoltDomainConfigPayloadSchemaMapping,
} from '@/api/generated'

const props = defineProps<{ modelValue: PipeBoltDomainConfigPayloadSchemaMapping[] }>()
const emit = defineEmits<{
  'update:modelValue': [value: PipeBoltDomainConfigPayloadSchemaMapping[]]
}>()

function update(index: number, patch: Partial<PipeBoltDomainConfigPayloadSchemaMapping>): void {
  const next = structuredClone(props.modelValue)
  const current = next[index]
  if (!current) return
  next[index] = { ...current, ...patch }
  emit('update:modelValue', next)
}

function updateField(
  mappingIndex: number,
  fieldIndex: number,
  patch: Partial<PipeBoltDomainConfigFieldMapping>,
): void {
  const mapping = props.modelValue[mappingIndex]
  if (!mapping) return
  const fields = structuredClone(mapping.fields)
  const field = fields[fieldIndex]
  if (!field) return
  fields[fieldIndex] = { ...field, ...patch }
  update(mappingIndex, { fields })
}

function add(): void {
  emit('update:modelValue', [
    ...props.modelValue,
    { fields: [], id: `mapping-${crypto.randomUUID()}`, name: 'New mapping' },
  ])
}

function addField(index: number): void {
  const mapping = props.modelValue[index]
  if (!mapping) return
  update(index, {
    fields: [
      ...mapping.fields,
      { required: false, source: 'value', target: 'value', value_type: 'string' },
    ],
  })
}

function updateDefault(mappingIndex: number, fieldIndex: number, event: Event): void {
  const value = (event.currentTarget as HTMLInputElement).value
  if (!value) return updateField(mappingIndex, fieldIndex, { default: undefined })
  try {
    updateField(mappingIndex, fieldIndex, { default: JSON.parse(value) as unknown })
  } catch {
    updateField(mappingIndex, fieldIndex, { default: value })
  }
}
</script>

<template>
  <section class="config-section">
    <div class="config-section-heading">
      <div>
        <p class="kicker">NORMALIZATION</p>
        <h2>Schema mappings</h2>
      </div>
      <button class="button button-secondary" type="button" @click="add">Add mapping</button>
    </div>
    <p v-if="!modelValue.length" class="config-empty">No schema mappings configured.</p>
    <article v-for="(mapping, index) in modelValue" :key="mapping.id" class="config-item">
      <div class="config-item-heading">
        <div>
          <span class="config-index">{{ index + 1 }}</span
          ><strong>{{ mapping.name }}</strong>
        </div>
        <button
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
      <div class="config-form-grid config-form-grid-dense">
        <label class="field"
          ><span>ID</span
          ><input
            :value="mapping.id"
            @input="
              update(index, { id: ($event.currentTarget as HTMLInputElement).value })
            " /></label
        ><label class="field"
          ><span>Name</span
          ><input
            :value="mapping.name"
            maxlength="160"
            @input="update(index, { name: ($event.currentTarget as HTMLInputElement).value })"
        /></label>
      </div>
      <div class="nested-list-heading">
        <strong>Field mappings</strong
        ><button type="button" @click="addField(index)">Add field</button>
      </div>
      <div
        v-for="(field, fieldIndex) in mapping.fields"
        :key="`${mapping.id}-${fieldIndex}`"
        class="nested-row nested-row-fields"
      >
        <label class="field"
          ><span>Source path</span
          ><input
            :value="field.source"
            @input="
              updateField(index, fieldIndex, {
                source: ($event.currentTarget as HTMLInputElement).value,
              })
            "
        /></label>
        <label class="field"
          ><span>Target</span
          ><input
            :value="field.target"
            @input="
              updateField(index, fieldIndex, {
                target: ($event.currentTarget as HTMLInputElement).value,
              })
            "
        /></label>
        <label class="field"
          ><span>Value type</span
          ><select
            :value="field.value_type"
            @change="
              updateField(index, fieldIndex, {
                value_type: ($event.currentTarget as HTMLSelectElement)
                  .value as typeof field.value_type,
              })
            "
          >
            <option value="string">String</option>
            <option value="number">Number</option>
            <option value="boolean">Boolean</option>
            <option value="object">Object</option>
            <option value="array">Array</option>
          </select></label
        >
        <label class="field"
          ><span>Default (JSON)</span
          ><input
            :value="field.default === undefined ? '' : JSON.stringify(field.default)"
            @change="updateDefault(index, fieldIndex, $event)"
        /></label>
        <label class="toggle-field"
          ><input
            :checked="field.required"
            type="checkbox"
            @change="
              updateField(index, fieldIndex, {
                required: ($event.currentTarget as HTMLInputElement).checked,
              })
            "
          /><span>Required</span></label
        >
        <button
          class="icon-remove"
          type="button"
          aria-label="Remove field"
          @click="
            update(index, {
              fields: mapping.fields.filter((_, itemIndex) => itemIndex !== fieldIndex),
            })
          "
        >
          ×
        </button>
      </div>
    </article>
  </section>
</template>
