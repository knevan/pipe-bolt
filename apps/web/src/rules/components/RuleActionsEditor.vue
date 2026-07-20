<script setup lang="ts">
import { computed } from 'vue'

import type {
  PipeBoltDomainConfigCommandTemplate,
  PipeBoltDomainConfigSinkDefinition,
} from '@/api/generated'
import { createActionDraft } from '../rules.mapper'
import type { RuleActionDraft, RuleActionType } from '../rules.types'

const props = defineProps<{
  modelValue: ReadonlyArray<RuleActionDraft>
  sinks: ReadonlyArray<PipeBoltDomainConfigSinkDefinition>
  templates: ReadonlyArray<PipeBoltDomainConfigCommandTemplate>
}>()
const emit = defineEmits<{ 'update:modelValue': [value: RuleActionDraft[]] }>()
const MAX_ACTIONS = 16

const webhookSinks = computed(() =>
  props.sinks.filter((sink) => sink.enabled && sink.kind.type === 'webhook'),
)
const commandTemplates = computed(() => props.templates.filter((template) => template.enabled))

function update(index: number, value: RuleActionDraft): void {
  emit(
    'update:modelValue',
    props.modelValue.map((action, actionIndex) => (actionIndex === index ? value : action)),
  )
}

function patch(index: number, value: Partial<RuleActionDraft>): void {
  const current = props.modelValue[index]
  if (current) update(index, { ...current, ...value })
}

function changeType(index: number, event: Event): void {
  update(
    index,
    createActionDraft((event.currentTarget as HTMLSelectElement).value as RuleActionType),
  )
}

function addAction(): void {
  if (props.modelValue.length >= MAX_ACTIONS) return
  emit('update:modelValue', [...props.modelValue, createActionDraft()])
}

function removeAction(index: number): void {
  emit(
    'update:modelValue',
    props.modelValue.filter((_, actionIndex) => actionIndex !== index),
  )
}
</script>

<template>
  <section class="builder-section">
    <header class="builder-section-heading">
      <div>
        <p class="kicker">INTENTS</p>
        <h2>Actions</h2>
      </div>
      <button
        class="button button-secondary"
        type="button"
        :disabled="modelValue.length >= MAX_ACTIONS"
        @click="addAction"
      >
        Add action
      </button>
    </header>
    <div class="action-list">
      <article v-for="(action, index) in modelValue" :key="action.key" class="action-row">
        <span class="action-index">{{ index + 1 }}</span>
        <label class="field">
          <span>Action</span>
          <select :value="action.type" @change="changeType(index, $event)">
            <option value="stream_to_ui">Stream to UI</option>
            <option value="forward_to_sink">Forward to sink</option>
            <option value="publish_command">Publish command</option>
            <option value="drop_event">Drop event</option>
            <option value="add_metadata">Add metadata</option>
          </select>
        </label>
        <label v-if="action.type === 'forward_to_sink'" class="field action-target">
          <span>Enabled webhook sink</span>
          <select
            :value="action.targetId"
            @change="patch(index, { targetId: ($event.currentTarget as HTMLSelectElement).value })"
          >
            <option value="">Select sink</option>
            <option v-for="sink in webhookSinks" :key="sink.id" :value="sink.id">
              {{ sink.name }} · {{ sink.id }}
            </option>
          </select>
        </label>
        <label v-else-if="action.type === 'publish_command'" class="field action-target">
          <span>Enabled command template</span>
          <select
            :value="action.targetId"
            @change="patch(index, { targetId: ($event.currentTarget as HTMLSelectElement).value })"
          >
            <option value="">Select command template</option>
            <option v-for="template in commandTemplates" :key="template.id" :value="template.id">
              {{ template.name }} · {{ template.id }}
            </option>
          </select>
          <small class="compatibility-warning"
            >Current runtime may reject this action on reload.</small
          >
        </label>
        <div v-else-if="action.type === 'add_metadata'" class="metadata-fields">
          <label class="field">
            <span>Metadata key</span>
            <input
              :value="action.metadataKey"
              autocomplete="off"
              maxlength="128"
              @input="
                patch(index, { metadataKey: ($event.currentTarget as HTMLInputElement).value })
              "
            />
          </label>
          <label class="field">
            <span>Metadata value</span>
            <input
              :value="action.metadataValue"
              autocomplete="off"
              maxlength="1024"
              @input="
                patch(index, { metadataValue: ($event.currentTarget as HTMLInputElement).value })
              "
            />
          </label>
        </div>
        <p v-else class="action-description">
          {{
            action.type === 'stream_to_ui'
              ? 'Emit normalized event to realtime subscribers.'
              : 'Stop downstream processing for the matched event.'
          }}
        </p>
        <button
          class="remove-action"
          type="button"
          :aria-label="`Remove action ${index + 1}`"
          @click="removeAction(index)"
        >
          ×
        </button>
      </article>
      <p v-if="!modelValue.length" class="empty-actions">Add at least one action.</p>
    </div>
  </section>
</template>

<style scoped>
.action-list {
  display: grid;
  gap: 0.65rem;
}

.action-row {
  display: grid;
  padding: 0.75rem;
  grid-template-columns: auto minmax(10rem, 0.7fr) minmax(12rem, 1.3fr) auto;
  align-items: start;
  gap: 0.65rem;
  border: 1px solid var(--line-soft);
  background: #0b161b;
}

.action-index {
  display: grid;
  width: 1.6rem;
  height: 2.55rem;
  place-items: center;
  color: var(--accent);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.62rem;
}

.metadata-fields {
  display: grid;
  grid-template-columns: minmax(8rem, 0.7fr) minmax(10rem, 1.3fr);
  gap: 0.55rem;
}

.action-description {
  align-self: center;
  margin: 0;
  color: var(--muted);
  font-size: 0.68rem;
}

.compatibility-warning {
  color: #e8b967;
  font-size: 0.58rem;
}

.remove-action {
  width: 2.2rem;
  height: 2.55rem;
  color: var(--muted);
  border: 0;
  background: transparent;
  cursor: pointer;
  font-size: 1.3rem;
}

.remove-action:hover {
  color: var(--danger);
}

.empty-actions {
  margin: 0;
  padding: 1rem;
  color: var(--danger);
  border: 1px dashed rgba(239, 128, 110, 0.35);
  font-size: 0.72rem;
}

@media (max-width: 820px) {
  .action-row {
    grid-template-columns: auto minmax(0, 1fr) auto;
  }

  .action-target,
  .metadata-fields,
  .action-description {
    grid-column: 2;
  }
}

@media (max-width: 520px) {
  .metadata-fields {
    grid-template-columns: 1fr;
  }
}
</style>
