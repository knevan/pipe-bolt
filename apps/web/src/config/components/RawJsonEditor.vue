<script setup lang="ts">
import { shallowRef, watch } from 'vue'
import type { PipeBoltApiDtoProjectConfigDocumentV1 } from '@/api/generated'
import {
  isConfigDocument,
  validateConfigDocument,
  type ConfigValidationIssue,
} from '../config.validation'
import {
  MASKED_SECRET,
  maskConfigSecrets,
  restoreConfigSecrets,
} from '../composables/useSecretMasking'

const props = defineProps<{
  config: PipeBoltApiDtoProjectConfigDocumentV1
  projectId: string
}>()
const emit = defineEmits<{ apply: [value: PipeBoltApiDtoProjectConfigDocumentV1] }>()
const text = shallowRef('')
const issues = shallowRef<ReadonlyArray<ConfigValidationIssue>>([])
const parseError = shallowRef<string>()

function findUnmaskedSecret(config: PipeBoltApiDtoProjectConfigDocumentV1): string | undefined {
  for (const broker of config.brokers) {
    if (broker.credentials && broker.credentials.password !== MASKED_SECRET) {
      return `brokers.${broker.id}.credentials.password`
    }
  }
  for (const sink of config.sinks) {
    if (sink.kind.type !== 'webhook') continue
    const header = sink.kind.headers.find((item) => item.value !== MASKED_SECRET)
    if (header) return `sinks.${sink.id}.headers.${header.name}`
  }
}

function reset(): void {
  text.value = JSON.stringify(maskConfigSecrets(props.config), null, 2)
  issues.value = []
  parseError.value = undefined
}

watch(() => props.config, reset, { immediate: true })

function apply(): void {
  parseError.value = undefined
  issues.value = []
  let input: unknown
  try {
    input = JSON.parse(text.value) as unknown
  } catch (error) {
    parseError.value = error instanceof Error ? error.message : 'Invalid JSON document.'
    return
  }

  if (!isConfigDocument(input)) {
    issues.value = validateConfigDocument(input, props.projectId)
    return
  }
  const unmaskedSecret = findUnmaskedSecret(input)
  if (unmaskedSecret) {
    parseError.value = `Keep ${unmaskedSecret} masked. Change secrets in the dedicated section editor.`
    return
  }
  let restored: PipeBoltApiDtoProjectConfigDocumentV1
  try {
    restored = restoreConfigSecrets(input, props.config)
  } catch (error) {
    parseError.value =
      error instanceof Error ? error.message : 'Masked secret could not be retained.'
    return
  }
  const validationIssues = validateConfigDocument(restored, props.projectId, props.config)
  if (validationIssues.length) {
    issues.value = validationIssues
    return
  }
  emit('apply', restored)
}
</script>

<template>
  <section class="config-section">
    <div class="config-section-heading">
      <div>
        <p class="kicker">ADVANCED</p>
        <h2>Raw config document</h2>
      </div>
      <div class="config-item-actions">
        <button class="button button-secondary" type="button" @click="reset">Reset text</button
        ><button class="button button-primary" type="button" @click="apply">Apply JSON</button>
      </div>
    </div>
    <div class="alert alert-warning">
      <div>
        <strong>Masked secret contract</strong
        ><span
          >Credential values render as {{ '••••••••' }}. Keep the mask unchanged to retain existing
          encrypted values.</span
        >
      </div>
    </div>
    <textarea v-model="text" class="raw-editor" rows="30" spellcheck="false"></textarea>
    <p v-if="parseError" class="form-error" role="alert">{{ parseError }}</p>
    <ol v-if="issues.length" class="validation-list" aria-label="Raw JSON validation errors">
      <li v-for="issue in issues" :key="`${issue.path}-${issue.message}`">
        <code>{{ issue.path }}</code
        ><span>{{ issue.message }}</span>
      </li>
    </ol>
  </section>
</template>
