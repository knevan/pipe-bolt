<script setup lang="ts">
import type { PipeBoltApiDtoProjectConfigDocumentV1 } from '@/api/generated'

type GeneralConfig = Pick<PipeBoltApiDtoProjectConfigDocumentV1, 'description' | 'enabled' | 'name'>

const props = defineProps<{ modelValue: GeneralConfig; projectId: string }>()
const emit = defineEmits<{ 'update:modelValue': [value: GeneralConfig] }>()

function update(patch: Partial<GeneralConfig>): void {
  emit('update:modelValue', { ...props.modelValue, ...patch })
}
</script>

<template>
  <section class="config-section">
    <div class="config-section-heading">
      <div>
        <p class="kicker">PROJECT DOCUMENT</p>
        <h2>General settings</h2>
      </div>
      <label class="toggle-label">
        <input
          :checked="modelValue.enabled"
          type="checkbox"
          @change="update({ enabled: ($event.currentTarget as HTMLInputElement).checked })"
        />
        Enabled
      </label>
    </div>
    <div class="config-form-grid">
      <label class="field"><span>Project ID</span><input :value="projectId" disabled /></label>
      <label class="field">
        <span>Name</span>
        <input
          :value="modelValue.name"
          maxlength="160"
          required
          @input="update({ name: ($event.currentTarget as HTMLInputElement).value })"
        />
      </label>
      <label class="field field-wide">
        <span>Description</span>
        <textarea
          :value="modelValue.description ?? ''"
          maxlength="2048"
          rows="4"
          @input="
            update({ description: ($event.currentTarget as HTMLTextAreaElement).value || null })
          "
        ></textarea>
      </label>
    </div>
  </section>
</template>
