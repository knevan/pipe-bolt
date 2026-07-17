<script setup lang="ts">
import { computed, shallowRef } from 'vue'
import type {
  PipeBoltApiDtoProjectConfigDocumentV1,
  PipeBoltDomainConfigBrokerConnectionConfig,
  PipeBoltDomainConfigCommandTemplate,
  PipeBoltDomainConfigPayloadSchemaMapping,
  PipeBoltDomainConfigSinkDefinition,
  PipeBoltDomainConfigTopicRouteConfig,
} from '@/api/generated'
import { ReloadRuntimeButton } from '@/runtime'
import BrokerEditor from './components/BrokerEditor.vue'
import CommandTemplateEditor from './components/CommandTemplateEditor.vue'
import ConfigGeneralEditor from './components/ConfigGeneralEditor.vue'
import RawJsonEditor from './components/RawJsonEditor.vue'
import RouteEditor from './components/RouteEditor.vue'
import SchemaMappingEditor from './components/SchemaMappingEditor.vue'
import SinkEditor from './components/SinkEditor.vue'
import { useConfigDraft } from './composables/useConfigDraft'
import { useUnsavedChangesGuard } from './composables/useUnsavedChangesGuard'

type TabId = 'general' | 'brokers' | 'routes' | 'schemas' | 'sinks' | 'commands' | 'raw'

const editor = useConfigDraft()
const activeTab = shallowRef<TabId>('general')
useUnsavedChangesGuard(editor.isDirty, editor.clear)

const tabs = computed(() => {
  const config = editor.draft.value
  return [
    { count: undefined, id: 'general' as const, label: 'General' },
    { count: config?.brokers.length, id: 'brokers' as const, label: 'Brokers' },
    { count: config?.routes.length, id: 'routes' as const, label: 'Routes' },
    { count: config?.schema_mappings.length, id: 'schemas' as const, label: 'Schemas' },
    { count: config?.sinks.length, id: 'sinks' as const, label: 'Sinks' },
    { count: config?.command_templates.length, id: 'commands' as const, label: 'Commands' },
    { count: undefined, id: 'raw' as const, label: 'Raw JSON' },
  ]
})

function updateGeneral(
  value: Pick<PipeBoltApiDtoProjectConfigDocumentV1, 'description' | 'enabled' | 'name'>,
): void {
  editor.updateGeneral(value)
}

function updateBrokers(value: PipeBoltDomainConfigBrokerConnectionConfig[]): void {
  editor.updateSection('brokers', value)
}

function updateRoutes(value: PipeBoltDomainConfigTopicRouteConfig[]): void {
  editor.updateSection('routes', value)
}

function updateSchemas(value: PipeBoltDomainConfigPayloadSchemaMapping[]): void {
  editor.updateSection('schema_mappings', value)
}

function updateSinks(value: PipeBoltDomainConfigSinkDefinition[]): void {
  editor.updateSection('sinks', value)
}

function updateCommands(value: PipeBoltDomainConfigCommandTemplate[]): void {
  editor.updateSection('command_templates', value)
}
</script>

<template>
  <div class="page config-page">
    <header class="page-header">
      <div>
        <p class="eyebrow">PROJECT / {{ editor.projectId.value ?? 'LOADING' }}</p>
        <h1>Configuration</h1>
        <p class="page-summary">
          Edit persisted control-plane configuration with optimistic concurrency and explicit
          runtime activation.
        </p>
      </div>
      <div v-if="editor.isLoaded.value" class="version-stack">
        <span>CONFIG VERSION</span><strong>{{ editor.version.value }}</strong>
        <small>schema v{{ editor.schemaVersion.value }}</small>
      </div>
    </header>

    <div v-if="editor.loadError.value" class="alert alert-danger" role="alert">
      <div>
        <strong>Configuration unavailable</strong><span>{{ editor.loadError.value }}</span>
      </div>
      <button type="button" @click="editor.refetch()">Retry</button>
    </div>
    <div
      v-else-if="editor.isLoading.value && !editor.isLoaded.value"
      class="skeleton-grid"
      aria-label="Loading configuration"
    >
      <div v-for="index in 4" :key="index" class="skeleton-block"></div>
    </div>

    <template v-if="editor.draft.value">
      <div v-if="editor.successMessage.value" class="alert alert-success" role="status">
        <div>
          <strong>Configuration updated</strong><span>{{ editor.successMessage.value }}</span>
        </div>
        <button type="button" @click="editor.dismissMessages">Dismiss</button>
      </div>
      <div v-if="editor.isConflict.value" class="alert alert-warning" role="alert">
        <div>
          <strong>Version conflict</strong
          ><span
            >Another operator saved a newer config version. Reload latest before applying your
            changes.</span
          >
        </div>
        <button type="button" @click="editor.reloadLatest">Reload latest</button>
      </div>
      <div v-else-if="editor.saveError.value" class="alert alert-danger" role="alert">
        <div>
          <strong>Save rejected · {{ editor.saveError.value.code }}</strong
          ><span>{{ editor.saveError.value.message }}</span>
        </div>
        <button type="button" @click="editor.dismissMessages">Dismiss</button>
      </div>
      <div v-if="editor.reloadLatestError.value" class="alert alert-danger" role="alert">
        <div>
          <strong>Latest version unavailable</strong
          ><span>{{ editor.reloadLatestError.value }}</span>
        </div>
      </div>

      <ReloadRuntimeButton
        v-if="editor.projectId.value && editor.version.value !== undefined"
        class="config-reload"
        :config-version="editor.version.value"
        :project-id="editor.projectId.value"
        :reload-required="editor.reloadRequired.value"
        @reloaded="editor.markReloaded"
      />

      <div class="config-workspace">
        <nav class="config-tabs" aria-label="Configuration sections">
          <button
            v-for="tab in tabs"
            :key="tab.id"
            :class="{ active: activeTab === tab.id }"
            type="button"
            @click="activeTab = tab.id"
          >
            <span>{{ tab.label }}</span
            ><small v-if="tab.count !== undefined">{{ tab.count }}</small>
          </button>
        </nav>

        <fieldset
          class="panel config-editor-panel config-editor-fieldset"
          :disabled="editor.isSaving.value"
        >
          <ConfigGeneralEditor
            v-if="activeTab === 'general'"
            :model-value="editor.draft.value"
            :project-id="editor.draft.value.project_id"
            @update:model-value="updateGeneral"
          />
          <BrokerEditor
            v-else-if="activeTab === 'brokers'"
            :model-value="editor.draft.value.brokers"
            @update:model-value="updateBrokers"
          />
          <RouteEditor
            v-else-if="activeTab === 'routes'"
            :brokers="editor.draft.value.brokers"
            :model-value="editor.draft.value.routes"
            :schema-mappings="editor.draft.value.schema_mappings"
            @update:model-value="updateRoutes"
          />
          <SchemaMappingEditor
            v-else-if="activeTab === 'schemas'"
            :model-value="editor.draft.value.schema_mappings"
            @update:model-value="updateSchemas"
          />
          <SinkEditor
            v-else-if="activeTab === 'sinks'"
            :model-value="editor.draft.value.sinks"
            @update:model-value="updateSinks"
          />
          <CommandTemplateEditor
            v-else-if="activeTab === 'commands'"
            :brokers="editor.draft.value.brokers"
            :model-value="editor.draft.value.command_templates"
            @update:model-value="updateCommands"
          />
          <RawJsonEditor
            v-else
            :config="editor.draft.value"
            :project-id="editor.draft.value.project_id"
            @apply="editor.replaceDraft"
          />
        </fieldset>
      </div>

      <section
        v-if="editor.validationCurrent.value"
        class="validation-panel"
        :class="{ valid: !editor.validationIssues.value.length }"
      >
        <strong>{{
          editor.validationIssues.value.length
            ? `${editor.validationIssues.value.length} validation issue(s)`
            : 'Validation preview passed'
        }}</strong>
        <ol v-if="editor.validationIssues.value.length" class="validation-list">
          <li
            v-for="issue in editor.validationIssues.value"
            :key="`${issue.path}-${issue.message}`"
          >
            <code>{{ issue.path }}</code
            ><span>{{ issue.message }}</span>
          </li>
        </ol>
        <p v-else>Backend remains the final validator during save.</p>
      </section>

      <footer class="config-save-bar">
        <div class="dirty-indicator" :class="{ dirty: editor.isDirty.value }">
          <span></span>
          <div>
            <strong>{{ editor.isDirty.value ? 'Unsaved changes' : 'Draft synchronized' }}</strong
            ><small>Version {{ editor.version.value }}</small>
          </div>
        </div>
        <label class="save-reason"
          ><span>Audit reason (optional)</span
          ><input
            v-model="editor.saveReason.value"
            maxlength="1024"
            placeholder="Describe this config change"
        /></label>
        <div class="save-actions">
          <button
            class="button button-secondary"
            type="button"
            :disabled="!editor.isDirty.value || editor.isSaving.value"
            @click="editor.discardChanges"
          >
            Discard
          </button>
          <button
            class="button button-secondary"
            type="button"
            :disabled="editor.isSaving.value"
            @click="editor.validate"
          >
            Validate
          </button>
          <button
            class="button button-primary"
            type="button"
            :disabled="!editor.isDirty.value || editor.isSaving.value"
            @click="editor.save"
          >
            {{ editor.isSaving.value ? 'Saving...' : 'Save config' }}
          </button>
        </div>
      </footer>
    </template>
  </div>
</template>
