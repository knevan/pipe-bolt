<script setup lang="ts">
import { computed, shallowRef } from 'vue'
import { storeToRefs } from 'pinia'

import type { PipeBoltDomainRuleRuleDefinition } from '@/api/generated'
import { useProjectStore } from '@/projects'
import { ReloadRuntimeButton } from '@/runtime'
import RuleList from './components/RuleList.vue'
import { useRulesConfig } from './composables/useRulesConfig'
import { ruleToDraft } from './rules.mapper'
import type { RuleDraftResources } from './rules.types'
import { compileRuleDraft } from './rules.validation'

const projects = useProjectStore()
const { activeProjectId } = storeToRefs(projects)
const config = useRulesConfig(activeProjectId)
const activeConfig = computed(() =>
  config.data.value?.config.project_id === activeProjectId.value ? config.data.value : undefined,
)
const rules = computed(() => activeConfig.value?.config.rules ?? [])
const toggleError = shallowRef<string>()
const resources = computed<RuleDraftResources>(() => ({
  commandTemplates: activeConfig.value?.config.command_templates ?? [],
  routes: activeConfig.value?.config.routes ?? [],
  rules: rules.value,
  sinks: activeConfig.value?.config.sinks ?? [],
}))

async function toggleRule(rule: PipeBoltDomainRuleRuleDefinition, enabled: boolean): Promise<void> {
  toggleError.value = undefined
  if (enabled) {
    try {
      const draft = ruleToDraft({ ...rule, enabled })
      const validation = compileRuleDraft(draft, resources.value, rule.id)
      if (validation.issues.length) {
        const issue = validation.issues[0]
        toggleError.value = `Cannot enable rule: ${issue?.path ?? 'rule'}: ${issue?.message ?? 'invalid rule'}`
        return
      }
    } catch (error) {
      toggleError.value =
        error instanceof Error
          ? `Cannot enable rule: ${error.message}`
          : 'Cannot enable invalid rule.'
      return
    }
  }
  const next = rules.value.map((candidate) =>
    candidate.id === rule.id ? { ...candidate, enabled } : candidate,
  )
  await config.saveRules(
    next,
    `${enabled ? 'Enable' : 'Disable'} rule '${rule.id}' from rule list.`,
  )
}

async function reloadLatest(): Promise<void> {
  await config.refetch()
  config.dismissMessages()
}
</script>

<template>
  <div class="page rules-page">
    <header class="page-header rules-header">
      <div>
        <p class="eyebrow">PROJECT / {{ activeProjectId }}</p>
        <h1>Rules</h1>
        <p class="page-summary">
          Manage typed, auditable Rule AST definitions that produce action intents without scripts.
        </p>
      </div>
      <div class="header-actions">
        <span v-if="activeConfig" class="config-version">
          CONFIG v{{ activeConfig.version }} · {{ rules.length }} RULES
        </span>
        <RouterLink
          class="button button-primary"
          :to="{ name: 'project-rule-new', params: { projectId: activeProjectId } }"
        >
          Create rule
        </RouterLink>
      </div>
    </header>

    <div v-if="config.loadError.value" class="alert alert-danger" role="alert">
      <div>
        <strong>Rules unavailable</strong><span>{{ config.loadError.value }}</span>
      </div>
      <button type="button" @click="config.refetch()">Retry</button>
    </div>
    <div v-if="toggleError" class="alert alert-warning" role="alert">
      <div>
        <strong>Rule remains disabled</strong><span>{{ toggleError }}</span>
      </div>
      <button type="button" @click="toggleError = undefined">Dismiss</button>
    </div>
    <div v-if="config.isConflict.value" class="alert alert-warning" role="alert">
      <div>
        <strong>Version conflict</strong>
        <span>Configuration changed elsewhere. Reload latest before retrying the toggle.</span>
      </div>
      <button type="button" @click="reloadLatest">Reload latest</button>
    </div>
    <div v-else-if="config.saveError.value" class="alert alert-danger" role="alert">
      <div>
        <strong>Rule update rejected · {{ config.saveError.value.code }}</strong>
        <span>{{ config.saveError.value.message }}</span>
      </div>
      <button type="button" @click="config.dismissMessages">Dismiss</button>
    </div>
    <div v-if="config.successMessage.value" class="alert alert-success" role="status">
      <div>
        <strong>Rule configuration saved</strong><span>{{ config.successMessage.value }}</span>
      </div>
      <button type="button" @click="config.dismissMessages">Dismiss</button>
    </div>

    <ReloadRuntimeButton
      v-if="activeProjectId && activeConfig"
      class="rules-reload"
      :config-version="activeConfig.version"
      :project-id="activeProjectId"
      :reload-required="config.reloadRequired.value"
      @reloaded="config.markReloaded"
    />

    <div
      v-if="config.isLoading.value && !activeConfig"
      class="skeleton-grid"
      aria-label="Loading rules"
    >
      <div v-for="index in 4" :key="index" class="skeleton-block"></div>
    </div>
    <RuleList
      v-else-if="activeConfig"
      :is-saving="config.isSaving.value"
      :project-id="activeProjectId ?? ''"
      :rules="rules"
      @toggle="toggleRule"
    />
  </div>
</template>

<style scoped>
.rules-page {
  width: min(100%, 105rem);
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 0.8rem;
}

.config-version {
  color: var(--muted);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.58rem;
}

.header-actions .button {
  display: inline-flex;
  align-items: center;
  text-decoration: none;
}

.rules-reload {
  margin-bottom: 1rem;
}

@media (max-width: 700px) {
  .rules-header,
  .header-actions {
    align-items: stretch;
    flex-direction: column;
  }
}
</style>
