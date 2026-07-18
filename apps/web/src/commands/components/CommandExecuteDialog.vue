<script setup lang="ts">
import {
  computed,
  nextTick,
  onMounted,
  onUnmounted,
  reactive,
  shallowRef,
  useTemplateRef,
} from 'vue'

import type {
  PipeBoltApiDtoExecuteCommandRequest,
  PipeBoltDomainConfigCommandTemplate,
} from '@/api/generated'
import { extractCommandParameters } from '../commands.template'
import type { CommandParameterKind } from '../commands.types'
import { useCommandExecution } from '../composables/useCommandExecution'
import CommandExecutionReceipt from './CommandExecutionReceipt.vue'

const props = defineProps<{
  projectId: string
  template: PipeBoltDomainConfigCommandTemplate
}>()
const emit = defineEmits<{ close: [] }>()
const phase = shallowRef<'form' | 'confirm' | 'receipt'>('form')
const reason = shallowRef('')
const parameterKinds = reactive(new Map<string, CommandParameterKind>())
const parameterValues = reactive(new Map<string, string>())
const fieldErrors = shallowRef<ReadonlyMap<string, string>>(new Map())
const formError = shallowRef<string>()
const confirmedRequest = shallowRef<PipeBoltApiDtoExecuteCommandRequest>()
const dialog = useTemplateRef<HTMLElement>('dialog')
const initialFocus = useTemplateRef<HTMLInputElement>('initialFocus')
const execution = useCommandExecution(props.projectId, props.template.id)
const extraction = extractCommandParameters(props.template)
const textEncoder = new TextEncoder()
const MAX_REASON_BYTES = 1_024
const MAX_REQUEST_BYTES = 64 * 1_024

for (const parameter of extraction.parameters) {
  parameterKinds.set(parameter.name, 'text')
  parameterValues.set(parameter.name, '')
}

const executionError = computed(() => execution.executeError.value?.message)
const trackerError = computed(() => execution.trackerError.value?.message)
const confirmationParams = computed(() => confirmedRequest.value?.params ?? {})

function updateKind(name: string, event: Event): void {
  const kind = (event.currentTarget as HTMLSelectElement).value as CommandParameterKind
  parameterKinds.set(name, kind)
  parameterValues.set(name, kind === 'boolean' ? 'false' : '')
  const nextErrors = new Map(fieldErrors.value)
  nextErrors.delete(name)
  fieldErrors.value = nextErrors
}

function scalarValue(name: string): string | number | boolean | undefined {
  const value = parameterValues.get(name) ?? ''
  switch (parameterKinds.get(name)) {
    case 'boolean':
      return value === 'true'
    case 'number': {
      if (!value.trim()) return undefined
      const number = Number(value)
      return Number.isFinite(number) ? number : undefined
    }
    default:
      return value
  }
}

function isInvalidTopicParameter(value: string): boolean {
  if (!value || value.includes('/') || value.includes('+') || value.includes('#')) return true
  return [...value].some((character) => {
    const codePoint = character.codePointAt(0) ?? 0
    return codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f)
  })
}

function validateRequest(): PipeBoltApiDtoExecuteCommandRequest | undefined {
  const errors = new Map<string, string>()
  formError.value = undefined
  const params: Record<string, string | number | boolean> = Object.create(null)
  for (const parameter of extraction.parameters) {
    const value = scalarValue(parameter.name)
    if (value === undefined) {
      errors.set(parameter.name, 'Enter a finite JSON number.')
      continue
    }
    const rendered = String(value)
    if (parameter.topic && isInvalidTopicParameter(rendered)) {
      errors.set(parameter.name, 'Topic parameters must be non-empty single MQTT segments.')
      continue
    }
    params[parameter.name] = value
  }
  fieldErrors.value = errors
  if (errors.size > 0) return undefined

  const auditReason = reason.value.trim()
  if (!auditReason) {
    formError.value = 'Audit reason is required.'
    return undefined
  }
  if (textEncoder.encode(auditReason).byteLength > MAX_REASON_BYTES) {
    formError.value = `Audit reason exceeds ${MAX_REASON_BYTES} UTF-8 bytes.`
    return undefined
  }

  const request = { params, reason: auditReason }
  if (textEncoder.encode(JSON.stringify(request)).byteLength > MAX_REQUEST_BYTES) {
    formError.value = `Execution request exceeds ${MAX_REQUEST_BYTES} bytes.`
    return undefined
  }
  return request
}

function prepareConfirmation(): void {
  if (extraction.error) {
    formError.value = extraction.error
    return
  }
  const request = validateRequest()
  if (!request) return
  confirmedRequest.value = request
  phase.value = 'confirm'
}

async function confirmExecution(): Promise<void> {
  if (!confirmedRequest.value) return
  if (await execution.execute(confirmedRequest.value)) phase.value = 'receipt'
}

function requestClose(): void {
  if (!execution.isExecuting.value) emit('close')
}

function handleKeydown(event: KeyboardEvent): void {
  if (event.key === 'Escape') {
    requestClose()
    return
  }
  if (event.key !== 'Tab') return
  const focusable = dialog.value?.querySelectorAll<HTMLElement>(
    'button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [href], [tabindex]:not([tabindex="-1"])',
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
        class="command-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="command-dialog-title"
      >
        <header class="dialog-header">
          <div>
            <p class="kicker">COMMAND GATEWAY / {{ phase.toUpperCase() }}</p>
            <h2 id="command-dialog-title">{{ template.name }}</h2>
          </div>
          <button
            type="button"
            aria-label="Close command dialog"
            :disabled="execution.isExecuting.value"
            @click="requestClose"
          >
            ×
          </button>
        </header>

        <div class="dialog-body">
          <form
            v-if="phase === 'form'"
            class="execution-form"
            @submit.prevent="prepareConfirmation"
          >
            <div class="target-banner">
              <span>TOPIC TEMPLATE</span><code>{{ template.topic_template }}</code>
            </div>

            <div v-if="extraction.error" class="inline-danger" role="alert">
              <strong>Template cannot be executed safely</strong><span>{{ extraction.error }}</span>
            </div>

            <section v-else class="parameter-section">
              <div class="section-heading">
                <div>
                  <p class="kicker">DYNAMIC INPUT</p>
                  <h3>Parameters</h3>
                </div>
                <span>{{ extraction.parameters.length }} detected</span>
              </div>
              <p v-if="!extraction.parameters.length" class="no-parameters">
                This template has no dynamic placeholders.
              </p>
              <div v-else class="parameter-grid">
                <div
                  v-for="(parameter, index) in extraction.parameters"
                  :key="parameter.name"
                  class="parameter-row"
                >
                  <label class="field parameter-value">
                    <span>
                      {{ parameter.name }}
                      <small>{{ parameter.topic ? 'topic' : 'payload' }}</small>
                    </span>
                    <select
                      v-if="parameterKinds.get(parameter.name) === 'boolean'"
                      :id="`command-param-${index}`"
                      :value="parameterValues.get(parameter.name)"
                      :ref="index === 0 ? 'initialFocus' : undefined"
                      @change="
                        parameterValues.set(
                          parameter.name,
                          ($event.currentTarget as HTMLSelectElement).value,
                        )
                      "
                    >
                      <option value="false">false</option>
                      <option value="true">true</option>
                    </select>
                    <input
                      v-else
                      :id="`command-param-${index}`"
                      :value="parameterValues.get(parameter.name)"
                      :ref="index === 0 ? 'initialFocus' : undefined"
                      :inputmode="
                        parameterKinds.get(parameter.name) === 'number' ? 'decimal' : 'text'
                      "
                      :type="parameterKinds.get(parameter.name) === 'number' ? 'number' : 'text'"
                      autocomplete="off"
                      maxlength="60000"
                      step="any"
                      @input="
                        parameterValues.set(
                          parameter.name,
                          ($event.currentTarget as HTMLInputElement).value,
                        )
                      "
                    />
                    <small v-if="fieldErrors.get(parameter.name)" class="field-error">
                      {{ fieldErrors.get(parameter.name) }}
                    </small>
                  </label>
                  <label class="field parameter-type">
                    <span>Scalar type</span>
                    <select
                      :value="parameterKinds.get(parameter.name)"
                      @change="updateKind(parameter.name, $event)"
                    >
                      <option value="text">Text</option>
                      <option value="number">Number</option>
                      <option value="boolean">Boolean</option>
                    </select>
                  </label>
                </div>
              </div>
            </section>

            <label class="field reason-field">
              <span>Audit reason <strong>required</strong></span>
              <input
                v-model="reason"
                :ref="extraction.parameters.length ? undefined : 'initialFocus'"
                autocomplete="off"
                maxlength="1024"
                placeholder="Why is this command necessary?"
                required
              />
              <small>Stored with command audit record. Maximum 1,024 UTF-8 bytes.</small>
            </label>
            <p v-if="formError" class="form-error" role="alert">{{ formError }}</p>

            <footer class="dialog-actions">
              <button class="button button-secondary" type="button" @click="requestClose">
                Cancel
              </button>
              <button
                class="button button-primary"
                type="submit"
                :disabled="Boolean(extraction.error)"
              >
                Review command
              </button>
            </footer>
          </form>

          <section v-else-if="phase === 'confirm'" class="confirmation-step">
            <div class="risk-banner">
              <strong>Explicit confirmation required</strong>
              <span>This action queues an MQTT command with external side effects.</span>
            </div>
            <dl class="confirmation-facts">
              <div>
                <dt>Project</dt>
                <dd>{{ projectId }}</dd>
              </div>
              <div>
                <dt>Template</dt>
                <dd>{{ template.id }}</dd>
              </div>
              <div class="fact-wide">
                <dt>Topic template</dt>
                <dd>{{ template.topic_template }}</dd>
              </div>
              <div class="fact-wide">
                <dt>Audit reason</dt>
                <dd>{{ confirmedRequest?.reason }}</dd>
              </div>
            </dl>
            <section class="confirmation-params">
              <span>PARAMETERS</span>
              <pre>{{ JSON.stringify(confirmationParams, null, 2) }}</pre>
            </section>
            <div v-if="executionError" class="inline-danger" role="alert">
              <strong>Command rejected</strong><span>{{ executionError }}</span>
              <small>Do not retry unless backend response confirms command was not accepted.</small>
            </div>
            <footer class="dialog-actions">
              <button
                class="button button-secondary"
                type="button"
                :disabled="execution.isExecuting.value"
                @click="phase = 'form'"
              >
                Back
              </button>
              <button
                class="button danger-button"
                type="button"
                :disabled="execution.isExecuting.value"
                @click="confirmExecution"
              >
                {{ execution.isExecuting.value ? 'Queueing...' : 'Confirm and queue' }}
              </button>
            </footer>
          </section>

          <section
            v-else-if="execution.receipt.value && execution.currentStatus.value"
            class="receipt-step"
          >
            <CommandExecutionReceipt
              :receipt="execution.receipt.value"
              :status="execution.currentStatus.value"
              :tracker-error="trackerError"
              :tracking-state="execution.trackingState.value"
            />
            <footer class="dialog-actions">
              <button class="button button-primary" type="button" @click="requestClose">
                Close receipt
              </button>
            </footer>
          </section>
        </div>
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

.command-dialog {
  width: min(100%, 46rem);
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
  backdrop-filter: blur(10px);
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

.dialog-header button:disabled {
  cursor: wait;
  opacity: 0.5;
}

.dialog-body {
  padding: 1.3rem;
}

.execution-form,
.confirmation-step,
.receipt-step {
  display: grid;
  gap: 1rem;
}

.target-banner,
.confirmation-params {
  display: grid;
  padding: 0.8rem;
  gap: 0.35rem;
  border: 1px solid var(--line-soft);
  background: #081216;
}

.target-banner span,
.confirmation-params > span {
  color: var(--muted);
  font-size: 0.55rem;
  letter-spacing: 0.08em;
}

.target-banner code {
  overflow-wrap: anywhere;
  color: var(--cyan);
  font-size: 0.7rem;
}

.section-heading {
  display: flex;
  margin-bottom: 0.75rem;
  align-items: end;
  justify-content: space-between;
}

.section-heading h3,
.section-heading p {
  margin: 0;
}

.section-heading span,
.no-parameters {
  color: var(--muted);
  font-size: 0.65rem;
}

.parameter-grid {
  display: grid;
  gap: 0.7rem;
}

.parameter-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 8rem;
  gap: 0.65rem;
}

.parameter-value > span {
  display: flex;
  justify-content: space-between;
  overflow-wrap: anywhere;
}

.parameter-value > span small {
  color: var(--cyan);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.52rem;
  text-transform: uppercase;
}

.reason-field {
  padding-top: 1rem;
  border-top: 1px solid var(--line-soft);
}

.reason-field > span strong {
  color: var(--accent);
}

.reason-field > small {
  color: var(--muted);
  font-size: 0.6rem;
}

.inline-danger,
.risk-banner {
  display: grid;
  padding: 0.85rem;
  gap: 0.25rem;
  color: #ff9b8b;
  border: 1px solid rgba(239, 128, 110, 0.3);
  background: rgba(80, 27, 24, 0.22);
  font-size: 0.75rem;
}

.inline-danger span,
.inline-danger small,
.risk-banner span {
  color: #c9afb0;
}

.risk-banner {
  padding: 1rem;
  color: #f6c472;
  border-color: rgba(229, 167, 70, 0.35);
  background: rgba(75, 52, 18, 0.25);
}

.confirmation-facts {
  display: grid;
  margin: 0;
  grid-template-columns: 1fr 1fr;
  border-top: 1px solid var(--line-soft);
  border-left: 1px solid var(--line-soft);
}

.confirmation-facts div {
  min-width: 0;
  padding: 0.72rem;
  border-right: 1px solid var(--line-soft);
  border-bottom: 1px solid var(--line-soft);
}

.confirmation-facts .fact-wide {
  grid-column: 1 / -1;
}

.confirmation-facts dt {
  color: var(--muted);
  font-size: 0.58rem;
}

.confirmation-facts dd {
  overflow-wrap: anywhere;
  margin: 0.3rem 0 0;
  font-size: 0.7rem;
}

.confirmation-params pre {
  max-height: 14rem;
  overflow: auto;
  margin: 0;
  color: #c8d5d7;
  font-size: 0.65rem;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.dialog-actions {
  display: flex;
  padding-top: 1rem;
  justify-content: flex-end;
  gap: 0.6rem;
  border-top: 1px solid var(--line-soft);
}

.danger-button {
  color: #1d1513;
  background: var(--danger);
}

.danger-button:hover:not(:disabled) {
  background: #ff9b8b;
}

@media (max-width: 520px) {
  .dialog-layer {
    padding: 0;
    place-items: stretch;
  }

  .command-dialog {
    width: 100%;
    min-height: 100vh;
    max-height: none;
    border: 0;
  }

  .parameter-row,
  .confirmation-facts {
    grid-template-columns: 1fr;
  }

  .confirmation-facts .fact-wide {
    grid-column: auto;
  }
}
</style>
