<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, shallowRef, useTemplateRef } from 'vue'

import type {
  PipeBoltApiDtoFailureEventResponse,
  PipeBoltApiDtoResolveFailureRequest,
} from '@/api/generated'
import { MAX_RESOLUTION_BYTES, MAX_RESOLUTION_REASON_BYTES } from '../operations.api'

const props = defineProps<{
  error?: string
  failure: PipeBoltApiDtoFailureEventResponse
  loading: boolean
}>()
const emit = defineEmits<{
  close: []
  submit: [body: PipeBoltApiDtoResolveFailureRequest]
}>()
const resolution = shallowRef('')
const reason = shallowRef('')
const validationError = shallowRef<string>()
const dialog = useTemplateRef<HTMLElement>('dialog')
const initialFocus = useTemplateRef<HTMLTextAreaElement>('initialFocus')
const encoder = new TextEncoder()
const resolutionBytes = computed(() => encoder.encode(resolution.value).byteLength)
const reasonBytes = computed(() => encoder.encode(reason.value).byteLength)

function requestClose(): void {
  if (!props.loading) emit('close')
}

function submit(): void {
  validationError.value = undefined
  const normalizedResolution = resolution.value.trim()
  const normalizedReason = reason.value.trim()
  if (!normalizedResolution) {
    validationError.value = 'Resolution note is required.'
    return
  }
  if (encoder.encode(normalizedResolution).byteLength > MAX_RESOLUTION_BYTES) {
    validationError.value = `Resolution note exceeds ${MAX_RESOLUTION_BYTES} UTF-8 bytes.`
    return
  }
  if (
    normalizedReason &&
    encoder.encode(normalizedReason).byteLength > MAX_RESOLUTION_REASON_BYTES
  ) {
    validationError.value = `Resolution reason exceeds ${MAX_RESOLUTION_REASON_BYTES} UTF-8 bytes.`
    return
  }
  emit('submit', {
    reason: normalizedReason || undefined,
    resolution: normalizedResolution,
  })
}

function handleKeydown(event: KeyboardEvent): void {
  if (event.key === 'Escape') {
    requestClose()
    return
  }
  if (event.key !== 'Tab') return
  const focusable = dialog.value?.querySelectorAll<HTMLElement>(
    'button:not([disabled]), input:not([disabled]), textarea:not([disabled]), [href], [tabindex]:not([tabindex="-1"])',
  )
  if (!focusable?.length) return
  const first = focusable[0]
  const last = focusable[focusable.length - 1]
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault()
    last?.focus()
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault()
    first?.focus()
  }
}

let previousOverflow = ''
let previousFocus: HTMLElement | null = null
let appRoot: HTMLElement | null = null
let previousInert = false
onMounted(() => {
  previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null
  previousOverflow = document.body.style.overflow
  document.body.style.overflow = 'hidden'
  appRoot = document.querySelector<HTMLElement>('#app')
  previousInert = appRoot?.inert ?? false
  if (appRoot) appRoot.inert = true
  document.addEventListener('keydown', handleKeydown)
  void nextTick(() => initialFocus.value?.focus())
})
onUnmounted(() => {
  document.body.style.overflow = previousOverflow
  if (appRoot) appRoot.inert = previousInert
  document.removeEventListener('keydown', handleKeydown)
  previousFocus?.focus()
})
</script>

<template>
  <Teleport to="body">
    <div class="dialog-layer" role="presentation" @mousedown.self="requestClose">
      <section
        ref="dialog"
        class="resolve-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="resolve-dialog-title"
        aria-describedby="resolve-dialog-description"
      >
        <header class="dialog-header">
          <div>
            <p class="kicker">FAILURE RESOLUTION</p>
            <h2 id="resolve-dialog-title">Close operational failure</h2>
          </div>
          <button type="button" :disabled="loading" aria-label="Close dialog" @click="requestClose">
            ×
          </button>
        </header>

        <form class="dialog-body" @submit.prevent="submit">
          <div class="failure-context">
            <span>{{ failure.component }} / {{ failure.failure_kind }}</span>
            <strong>{{ failure.message }}</strong>
            <code>{{ failure.failure_id }}</code>
          </div>
          <p id="resolve-dialog-description" class="dialog-copy">
            Resolution closes this recorded failure and creates an audit event. It does not replay
            the failed operation.
          </p>

          <label class="field">
            <span>Resolution note <strong>*</strong></span>
            <textarea
              ref="initialFocus"
              v-model="resolution"
              rows="5"
              :maxlength="MAX_RESOLUTION_BYTES"
              :disabled="loading"
              placeholder="Describe remediation and verified outcome"
            ></textarea>
            <small :class="{ 'limit-danger': resolutionBytes > MAX_RESOLUTION_BYTES }">
              {{ resolutionBytes }} / {{ MAX_RESOLUTION_BYTES }} UTF-8 bytes
            </small>
          </label>

          <label class="field">
            <span>Audit reason <small>OPTIONAL</small></span>
            <input
              v-model="reason"
              type="text"
              :maxlength="MAX_RESOLUTION_REASON_BYTES"
              :disabled="loading"
              autocomplete="off"
              placeholder="Why this failure can be closed"
            />
            <small :class="{ 'limit-danger': reasonBytes > MAX_RESOLUTION_REASON_BYTES }">
              {{ reasonBytes }} / {{ MAX_RESOLUTION_REASON_BYTES }} UTF-8 bytes
            </small>
          </label>

          <div v-if="validationError || error" class="inline-danger" role="alert">
            {{ validationError ?? error }}
          </div>

          <footer class="dialog-actions">
            <button
              class="button button-secondary"
              type="button"
              :disabled="loading"
              @click="requestClose"
            >
              Cancel
            </button>
            <button class="button button-primary" type="submit" :disabled="loading">
              {{ loading ? 'Resolving…' : 'Resolve failure' }}
            </button>
          </footer>
        </form>
      </section>
    </div>
  </Teleport>
</template>

<style scoped>
.dialog-layer {
  position: fixed;
  z-index: 90;
  inset: 0;
  display: grid;
  padding: 1rem;
  overflow: auto;
  place-items: center;
  background: rgba(2, 8, 10, 0.78);
  backdrop-filter: blur(4px);
}

.resolve-dialog {
  width: min(100%, 42rem);
  max-height: calc(100vh - 2rem);
  overflow: auto;
  border: 1px solid var(--line);
  background: #0d191e;
  box-shadow: 0 2rem 7rem rgba(0, 0, 0, 0.48);
}

.dialog-header {
  position: sticky;
  z-index: 2;
  top: 0;
  display: flex;
  padding: 1.1rem 1.3rem;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--line);
  background: rgba(13, 25, 30, 0.97);
}

.dialog-header h2,
.dialog-header p {
  margin-bottom: 0;
}

.dialog-header button {
  width: 2.2rem;
  height: 2.2rem;
  color: var(--muted);
  border: 1px solid var(--line);
  background: transparent;
  cursor: pointer;
  font-size: 1.4rem;
}

.dialog-body {
  display: grid;
  padding: 1.3rem;
  gap: 1rem;
}

.failure-context {
  display: grid;
  padding: 0.85rem;
  gap: 0.25rem;
  border: 1px solid rgba(239, 128, 110, 0.25);
  background: rgba(80, 27, 24, 0.18);
}

.failure-context span,
.failure-context code {
  color: var(--muted);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.62rem;
}

.failure-context code {
  overflow-wrap: anywhere;
  color: var(--cyan);
}

.dialog-copy {
  margin: 0;
  color: #9babad;
  font-size: 0.75rem;
}

.field > span {
  display: flex;
  justify-content: space-between;
}

.field > span strong,
.limit-danger {
  color: #ff9b8b;
}

.field > span small,
.field > small {
  color: var(--muted);
  font-size: 0.56rem;
}

.field > small {
  text-align: right;
}

.field textarea {
  resize: vertical;
}

.inline-danger {
  padding: 0.8rem;
  color: #ff9b8b;
  border: 1px solid rgba(239, 128, 110, 0.3);
  background: rgba(80, 27, 24, 0.22);
  font-size: 0.75rem;
}

.dialog-actions {
  display: flex;
  padding-top: 0.4rem;
  justify-content: flex-end;
  gap: 0.6rem;
}
</style>
