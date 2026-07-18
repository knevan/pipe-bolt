<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, useTemplateRef } from 'vue'
import type { RealtimeEvent } from '../realtime.types'

const props = defineProps<{ event: RealtimeEvent }>()
const emit = defineEmits<{ close: [] }>()
const closeButton = useTemplateRef<HTMLButtonElement>('closeButton')
const drawer = useTemplateRef<HTMLElement>('drawer')
const MAX_RENDERED_JSON_CHARS = 100_000
const MAX_BASE64_CHARS = 512 * 1024
const HEX_PREVIEW_BYTES = 512

function formatJson(value: unknown): string {
  let output: string
  try {
    output = JSON.stringify(value, null, 2) ?? 'null'
  } catch {
    return 'Unable to serialize value.'
  }
  return output.length > MAX_RENDERED_JSON_CHARS
    ? `${output.slice(0, MAX_RENDERED_JSON_CHARS)}\n… truncated …`
    : output
}

const payloadJson = computed(() =>
  props.event.payload.type === 'json' ? formatJson(props.event.payload.value) : undefined,
)
const fieldsJson = computed(() => formatJson(props.event.fields))
const metadataJson = computed(() => formatJson(props.event.metadata))
const rawPayload = computed(() => {
  if (props.event.payload.type !== 'raw_base64') return undefined
  const encoded = props.event.payload.value
  if (encoded.length > MAX_BASE64_CHARS) {
    return { error: 'Base64 payload exceeds browser decode limit.' }
  }

  try {
    const binary = atob(encoded)
    const bytes = Uint8Array.from(binary, (character) => character.codePointAt(0) ?? 0)
    const preview = bytes.subarray(0, HEX_PREVIEW_BYTES)
    const hex = [...preview].map((byte) => byte.toString(16).padStart(2, '0')).join(' ')
    let utf8: string | undefined
    try {
      utf8 = new TextDecoder('utf-8', { fatal: true }).decode(bytes)
    } catch {
      utf8 = undefined
    }
    return {
      byteLength: bytes.byteLength,
      hex: bytes.length > HEX_PREVIEW_BYTES ? `${hex} …` : hex,
      utf8,
    }
  } catch {
    return { error: 'Payload contains invalid Base64 data.' }
  }
})

function keydown(event: KeyboardEvent): void {
  if (event.key === 'Escape') {
    emit('close')
    return
  }
  if (event.key !== 'Tab') return
  const focusable = drawer.value?.querySelectorAll<HTMLElement>(
    'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
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
  document.addEventListener('keydown', keydown)
  void nextTick(() => closeButton.value?.focus())
})
onUnmounted(() => {
  document.body.style.overflow = previousOverflow
  if (appRoot) appRoot.inert = previousInert
  document.removeEventListener('keydown', keydown)
  previousFocus?.focus()
})
</script>

<template>
  <Teleport to="body">
    <div class="drawer-layer" role="presentation" @mousedown.self="emit('close')">
      <aside
        ref="drawer"
        class="event-drawer"
        role="dialog"
        aria-modal="true"
        aria-labelledby="event-detail-title"
      >
        <header class="drawer-header">
          <div>
            <p class="kicker">EVENT INSPECTOR</p>
            <h2 id="event-detail-title">{{ event.event_type }}</h2>
          </div>
          <button
            ref="closeButton"
            type="button"
            aria-label="Close event details"
            @click="emit('close')"
          >
            ×
          </button>
        </header>

        <div class="drawer-body">
          <dl class="event-facts">
            <div>
              <dt>Event ID</dt>
              <dd>{{ event.id }}</dd>
            </div>
            <div>
              <dt>Correlation ID</dt>
              <dd>{{ event.correlation_id }}</dd>
            </div>
            <div>
              <dt>Project</dt>
              <dd>{{ event.project_id }}</dd>
            </div>
            <div>
              <dt>Broker</dt>
              <dd>{{ event.broker_id }}</dd>
            </div>
            <div>
              <dt>Route</dt>
              <dd>{{ event.route_id }}</dd>
            </div>
            <div>
              <dt>Schema</dt>
              <dd>{{ event.schema_mapping_id ?? 'none' }}</dd>
            </div>
            <div>
              <dt>Device</dt>
              <dd>{{ event.device_id ?? 'none' }}</dd>
            </div>
            <div>
              <dt>Received</dt>
              <dd>{{ event.received_at }}</dd>
            </div>
            <div class="fact-wide">
              <dt>Topic</dt>
              <dd>{{ event.topic }}</dd>
            </div>
          </dl>

          <section class="drawer-section">
            <div class="drawer-section-heading">
              <h3>Normalized fields</h3>
              <span>{{ Object.keys(event.fields).length }} fields</span>
            </div>
            <pre>{{ fieldsJson }}</pre>
          </section>

          <section class="drawer-section">
            <div class="drawer-section-heading">
              <h3>Payload</h3>
              <span>{{ event.payload_size_bytes.toLocaleString() }} bytes</span>
            </div>
            <pre v-if="payloadJson !== undefined">{{ payloadJson }}</pre>
            <template v-else-if="rawPayload">
              <p v-if="rawPayload.error" class="drawer-error">{{ rawPayload.error }}</p>
              <template v-else>
                <p class="raw-label">
                  Decoded bytes · {{ rawPayload.byteLength?.toLocaleString() }}
                </p>
                <pre v-if="rawPayload.utf8 !== undefined">{{ rawPayload.utf8 }}</pre>
                <p class="raw-label">Hex preview · first {{ HEX_PREVIEW_BYTES }} bytes</p>
                <pre>{{ rawPayload.hex }}</pre>
              </template>
            </template>
          </section>

          <section v-if="event.raw" class="drawer-section">
            <div class="drawer-section-heading">
              <h3>Raw payload reference</h3>
              <span>metadata only</span>
            </div>
            <dl class="raw-facts">
              <div>
                <dt>Byte length</dt>
                <dd>{{ event.raw.byte_len.toLocaleString() }}</dd>
              </div>
              <div>
                <dt>Content type</dt>
                <dd>{{ event.raw.content_type ?? 'unknown' }}</dd>
              </div>
            </dl>
          </section>

          <section class="drawer-section">
            <div class="drawer-section-heading">
              <h3>Metadata</h3>
              <span>{{ Object.keys(event.metadata).length }} entries</span>
            </div>
            <pre>{{ metadataJson }}</pre>
          </section>

          <section class="drawer-section">
            <div class="drawer-section-heading">
              <h3>Normalization diagnostics</h3>
              <span>{{ event.normalization_errors.length }}</span>
            </div>
            <p v-if="!event.normalization_errors.length" class="drawer-empty">
              No normalization diagnostics.
            </p>
            <ul v-else class="diagnostic-list">
              <li
                v-for="diagnostic in event.normalization_errors"
                :key="`${diagnostic.code}-${diagnostic.field ?? ''}-${diagnostic.message}`"
              >
                <strong>{{ diagnostic.code }}</strong
                ><span>{{ diagnostic.field ?? 'event' }} · {{ diagnostic.message }}</span>
              </li>
            </ul>
          </section>
        </div>
      </aside>
    </div>
  </Teleport>
</template>

<style scoped>
.drawer-layer {
  position: fixed;
  z-index: 80;
  inset: 0;
  display: flex;
  justify-content: flex-end;
  background: rgba(2, 8, 10, 0.7);
  backdrop-filter: blur(3px);
}

.event-drawer {
  width: min(46rem, 92vw);
  height: 100%;
  overflow: auto;
  border-left: 1px solid var(--line);
  background: #0d191e;
  box-shadow: -2rem 0 5rem rgba(0, 0, 0, 0.35);
}

.drawer-header {
  position: sticky;
  z-index: 2;
  top: 0;
  display: flex;
  padding: 1.2rem 1.5rem;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--line);
  background: rgba(13, 25, 30, 0.96);
  backdrop-filter: blur(10px);
}

.drawer-header h2,
.drawer-header p {
  margin-bottom: 0;
}

.drawer-header button {
  width: 2.2rem;
  height: 2.2rem;
  color: var(--muted);
  border: 1px solid var(--line);
  background: transparent;
  cursor: pointer;
  font-size: 1.4rem;
}

.drawer-body {
  padding: 1.5rem;
}

.event-facts {
  display: grid;
  margin: 0;
  grid-template-columns: 1fr 1fr;
  border-top: 1px solid var(--line-soft);
}

.event-facts div {
  min-width: 0;
  padding: 0.7rem;
  border-right: 1px solid var(--line-soft);
  border-bottom: 1px solid var(--line-soft);
}

.event-facts .fact-wide {
  grid-column: 1 / -1;
}

.event-facts dt,
.raw-facts dt {
  color: var(--muted);
  font-size: 0.6rem;
}

.event-facts dd,
.raw-facts dd {
  margin: 0.3rem 0 0;
  overflow-wrap: anywhere;
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.66rem;
}

.drawer-section {
  margin-top: 1.5rem;
}

.drawer-section-heading {
  display: flex;
  margin-bottom: 0.6rem;
  align-items: center;
  justify-content: space-between;
}

.drawer-section-heading h3 {
  margin: 0;
  font-size: 0.82rem;
}

.drawer-section-heading span,
.raw-label {
  color: var(--muted);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.56rem;
}

.drawer-section pre {
  max-height: 26rem;
  margin: 0;
  padding: 0.9rem;
  overflow: auto;
  color: #bcd0d1;
  border: 1px solid var(--line-soft);
  background: #091317;
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.66rem;
  line-height: 1.55;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.raw-label {
  margin: 0.7rem 0 0.35rem;
}

.raw-facts {
  display: grid;
  margin: 0;
  grid-template-columns: 1fr 1fr;
  gap: 0.8rem;
}

.drawer-error,
.drawer-empty {
  color: var(--muted);
  font-size: 0.74rem;
}

.drawer-error {
  color: var(--danger);
}

.diagnostic-list {
  display: grid;
  margin: 0;
  padding: 0;
  gap: 0.45rem;
  list-style: none;
}

.diagnostic-list li {
  display: grid;
  padding: 0.7rem;
  gap: 0.25rem;
  border: 1px solid rgba(239, 128, 110, 0.22);
  background: rgba(80, 27, 24, 0.18);
}

.diagnostic-list strong {
  color: var(--danger);
  font-size: 0.68rem;
}

.diagnostic-list span {
  color: #b9c6c8;
  font-size: 0.7rem;
}

@media (max-width: 600px) {
  .event-drawer {
    width: 100%;
  }

  .event-facts,
  .raw-facts {
    grid-template-columns: 1fr;
  }

  .event-facts .fact-wide {
    grid-column: auto;
  }
}
</style>
