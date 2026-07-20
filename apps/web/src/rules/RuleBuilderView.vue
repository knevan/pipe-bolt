<script setup lang="ts">
import { computed, nextTick, shallowRef } from 'vue'
import { storeToRefs } from 'pinia'
import { useRoute, useRouter } from 'vue-router'

import { useProjectStore } from '@/projects'
import { ReloadRuntimeButton } from '@/runtime'
import RuleActionsEditor from './components/RuleActionsEditor.vue'
import RuleBasicForm from './components/RuleBasicForm.vue'
import RuleConditionBuilder from './components/RuleConditionBuilder.vue'
import RuleJsonPreview from './components/RuleJsonPreview.vue'
import { useRuleDraft } from './composables/useRuleDraft'
import { useRulesConfig } from './composables/useRulesConfig'
import { useRuleUnsavedGuard } from './composables/useRuleUnsavedGuard'
import type { RuleDraftResources, RuleFormDraft } from './rules.types'

const route = useRoute()
const router = useRouter()
const projects = useProjectStore()
const { activeProjectId } = storeToRefs(projects)
const config = useRulesConfig(activeProjectId)
const saveReason = shallowRef('')
const activeConfig = computed(() =>
  config.data.value?.config.project_id === activeProjectId.value ? config.data.value : undefined,
)
const isNew = computed(() => route.name === 'project-rule-new')
const routeRuleId = computed(() => {
  const value = route.params.ruleId
  return Array.isArray(value) ? value[0] : value
})
const originalRule = computed(() =>
  isNew.value
    ? undefined
    : activeConfig.value?.config.rules.find((rule) => rule.id === routeRuleId.value),
)
const resources = computed<RuleDraftResources>(() => ({
  commandTemplates: activeConfig.value?.config.command_templates ?? [],
  routes: activeConfig.value?.config.routes ?? [],
  rules: activeConfig.value?.config.rules ?? [],
  sinks: activeConfig.value?.config.sinks ?? [],
}))
const identity = computed(
  () => `${activeProjectId.value ?? ''}|${isNew.value ? 'new' : (routeRuleId.value ?? '')}`,
)
const ruleDraft = useRuleDraft({
  identity,
  isNew,
  originalRule,
  resources,
})
const missingRule = computed(
  () => !isNew.value && Boolean(activeConfig.value) && !originalRule.value,
)
useRuleUnsavedGuard(ruleDraft.isDirty)

function updateDraft(value: RuleFormDraft): void {
  ruleDraft.update(value)
}

function patchDraft(value: Partial<RuleFormDraft>): void {
  if (ruleDraft.draft.value) updateDraft({ ...ruleDraft.draft.value, ...value })
}

async function save(): Promise<void> {
  const compiled = ruleDraft.compiled.value
  const current = activeConfig.value
  if (!compiled.rule || compiled.issues.length || !current) return
  const nextRules = isNew.value
    ? [...current.config.rules, compiled.rule]
    : current.config.rules.map((rule) =>
        rule.id === originalRule.value?.id ? compiled.rule! : rule,
      )
  const reason =
    saveReason.value.trim() || `${isNew.value ? 'Create' : 'Update'} rule '${compiled.rule.id}'.`
  const response = await config.saveRules(nextRules, reason)
  if (!response) return

  ruleDraft.acceptSaved(compiled.rule)
  saveReason.value = ''
  if (isNew.value) {
    await router.replace({
      name: 'project-rule-edit',
      params: { projectId: activeProjectId.value, ruleId: compiled.rule.id },
    })
  }
}

async function reloadLatest(): Promise<void> {
  await config.refetch()
  await nextTick()
  ruleDraft.reset()
  config.dismissMessages()
}

async function cancel(): Promise<void> {
  await router.push({ name: 'project-rules', params: { projectId: activeProjectId.value } })
}
</script>

<template>
  <div class="page rule-builder-page">
    <header class="page-header builder-header">
      <div>
        <p class="eyebrow">PROJECT / {{ activeProjectId }} / RULES</p>
        <h1>{{ isNew ? 'Create rule' : 'Edit rule' }}</h1>
        <p class="page-summary">
          Compose backend-owned typed Rule AST with bounded condition depth and explicit action
          intents.
        </p>
      </div>
      <span v-if="activeConfig" class="config-version">CONFIG v{{ activeConfig.version }}</span>
    </header>

    <div v-if="config.loadError.value" class="alert alert-danger" role="alert">
      <div>
        <strong>Rule config unavailable</strong><span>{{ config.loadError.value }}</span>
      </div>
      <button type="button" @click="config.refetch()">Retry</button>
    </div>
    <div v-if="ruleDraft.initializationError.value" class="alert alert-danger" role="alert">
      <div>
        <strong>Rule cannot be opened</strong><span>{{ ruleDraft.initializationError.value }}</span>
      </div>
    </div>
    <div v-if="missingRule" class="alert alert-danger" role="alert">
      <div>
        <strong>Rule not found</strong><span>Requested rule is absent from current config.</span>
      </div>
      <button type="button" @click="cancel">Back to rules</button>
    </div>
    <div v-if="config.isConflict.value" class="alert alert-warning" role="alert">
      <div>
        <strong>Version conflict</strong>
        <span
          >Configuration changed elsewhere. Reload latest; current draft will be discarded.</span
        >
      </div>
      <button type="button" @click="reloadLatest">Reload latest</button>
    </div>
    <div v-else-if="config.saveError.value" class="alert alert-danger" role="alert">
      <div>
        <strong>Rule save rejected · {{ config.saveError.value.code }}</strong>
        <span>{{ config.saveError.value.message }}</span>
      </div>
      <button type="button" @click="config.dismissMessages">Dismiss</button>
    </div>
    <div v-if="config.successMessage.value" class="alert alert-success" role="status">
      <div>
        <strong>Rule saved</strong><span>{{ config.successMessage.value }}</span>
      </div>
      <button type="button" @click="config.dismissMessages">Dismiss</button>
    </div>

    <ReloadRuntimeButton
      v-if="activeProjectId && activeConfig"
      class="builder-reload"
      :config-version="activeConfig.version"
      :project-id="activeProjectId"
      :reload-required="config.reloadRequired.value"
      @reloaded="config.markReloaded"
    />

    <div
      v-if="config.isLoading.value && !activeConfig"
      class="skeleton-grid"
      aria-label="Loading rule builder"
    >
      <div v-for="index in 4" :key="index" class="skeleton-block"></div>
    </div>

    <template v-if="ruleDraft.draft.value && !missingRule">
      <div class="builder-layout">
        <form class="builder-form" @submit.prevent="save">
          <RuleBasicForm
            :is-new="isNew"
            :model-value="ruleDraft.draft.value"
            :routes="resources.routes"
            :templates="resources.commandTemplates"
            @update:model-value="updateDraft"
          />
          <RuleConditionBuilder
            :condition="ruleDraft.draft.value.condition"
            :enabled="ruleDraft.draft.value.conditionEnabled"
            @update:condition="patchDraft({ condition: $event })"
            @update:enabled="patchDraft({ conditionEnabled: $event })"
          />
          <RuleActionsEditor
            :model-value="ruleDraft.draft.value.actions"
            :sinks="resources.sinks"
            :templates="resources.commandTemplates"
            @update:model-value="patchDraft({ actions: $event })"
          />

          <footer class="builder-save-bar">
            <div class="dirty-state" :class="{ dirty: ruleDraft.isDirty.value }">
              <span></span>
              <div>
                <strong>{{
                  ruleDraft.isDirty.value ? 'Unsaved draft' : 'Rule synchronized'
                }}</strong>
                <small>Backend validates final config and runtime compatibility.</small>
              </div>
            </div>
            <label class="save-reason">
              <span>Audit reason (optional)</span>
              <input
                v-model="saveReason"
                maxlength="1024"
                placeholder="Describe this rule change"
              />
            </label>
            <div class="save-actions">
              <button class="button button-secondary" type="button" @click="cancel">Cancel</button>
              <button
                class="button button-secondary"
                type="button"
                :disabled="!ruleDraft.isDirty.value || config.isSaving.value"
                @click="ruleDraft.reset"
              >
                Reset
              </button>
              <button
                class="button button-primary"
                type="submit"
                :disabled="
                  !ruleDraft.isDirty.value ||
                  Boolean(ruleDraft.compiled.value.issues.length) ||
                  config.isSaving.value
                "
              >
                {{ config.isSaving.value ? 'Saving...' : 'Save rule' }}
              </button>
            </div>
          </footer>
        </form>

        <RuleJsonPreview
          :issues="ruleDraft.compiled.value.issues"
          :serialized="ruleDraft.serializedRule.value"
        />
      </div>
    </template>
  </div>
</template>

<style scoped>
.rule-builder-page {
  width: min(100%, 110rem);
}

.config-version {
  padding: 0.6rem 0.75rem;
  color: var(--cyan);
  border: 1px solid var(--line);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.6rem;
}

.builder-reload {
  margin-bottom: 1rem;
}

.builder-layout {
  display: grid;
  grid-template-columns: minmax(0, 1.65fr) minmax(19rem, 0.75fr);
  align-items: start;
  gap: 1rem;
}

.builder-form {
  display: grid;
  min-width: 0;
  gap: 0.8rem;
}

.builder-form :deep(.builder-section) {
  padding: 1rem;
  border: 1px solid var(--line-soft);
  background: linear-gradient(145deg, rgba(23, 39, 46, 0.82), rgba(15, 27, 32, 0.9));
}

.builder-form :deep(.builder-section-heading) {
  display: flex;
  margin-bottom: 0.9rem;
  align-items: end;
  justify-content: space-between;
  gap: 1rem;
}

.builder-form :deep(.builder-section-heading h2),
.builder-form :deep(.builder-section-heading p) {
  margin-bottom: 0;
}

.builder-save-bar {
  position: sticky;
  z-index: 5;
  bottom: 0;
  display: grid;
  padding: 0.8rem;
  grid-template-columns: auto minmax(12rem, 1fr) auto;
  align-items: end;
  gap: 0.8rem;
  border: 1px solid var(--line);
  background: rgba(13, 25, 30, 0.97);
  backdrop-filter: blur(10px);
}

.dirty-state {
  display: flex;
  align-items: center;
  gap: 0.55rem;
}

.dirty-state > span {
  width: 0.55rem;
  height: 0.55rem;
  border-radius: 50%;
  background: var(--safe);
}

.dirty-state.dirty > span {
  background: var(--accent);
}

.dirty-state strong,
.dirty-state small {
  display: block;
}

.dirty-state strong {
  font-size: 0.7rem;
}

.dirty-state small {
  margin-top: 0.15rem;
  color: var(--muted);
  font-size: 0.56rem;
}

.save-actions {
  display: flex;
  gap: 0.5rem;
}

@media (max-width: 1100px) {
  .builder-layout {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 780px) {
  .builder-save-bar {
    position: static;
    grid-template-columns: 1fr;
  }

  .save-actions {
    flex-wrap: wrap;
  }
}
</style>
