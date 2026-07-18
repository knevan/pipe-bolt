<script setup lang="ts">
import { computed, shallowRef, watch } from 'vue'
import { storeToRefs } from 'pinia'

import type { PipeBoltDomainConfigCommandTemplate } from '@/api/generated'
import { useProjectStore } from '@/projects'
import CommandExecuteDialog from './components/CommandExecuteDialog.vue'
import CommandTemplateList from './components/CommandTemplateList.vue'
import { useCommandTemplates } from './composables/useCommandTemplates'

const projects = useProjectStore()
const { activeProjectId } = storeToRefs(projects)
const catalog = useCommandTemplates(activeProjectId)
const selectedTemplate = shallowRef<PipeBoltDomainConfigCommandTemplate>()
const templates = computed(() => catalog.data.value?.templates ?? [])
const brokers = computed(() => catalog.data.value?.brokers ?? [])
const enabledCount = computed(() => templates.value.filter((template) => template.enabled).length)

watch(activeProjectId, () => {
  selectedTemplate.value = undefined
})
</script>

<template>
  <div class="page commands-page">
    <header class="page-header commands-header">
      <div>
        <p class="eyebrow">PROJECT / {{ activeProjectId }}</p>
        <h1>Command gateway</h1>
        <p class="page-summary">
          Execute governed MQTT command templates with typed scalar parameters, explicit intent,
          audit visibility, and bounded status tracking.
        </p>
      </div>
      <div v-if="catalog.data.value" class="catalog-summary">
        <div>
          <span>CONFIG VERSION</span><strong>{{ catalog.data.value.version }}</strong>
        </div>
        <div>
          <span>EXECUTABLE</span><strong>{{ enabledCount }} / {{ templates.length }}</strong>
        </div>
      </div>
    </header>

    <div v-if="catalog.errorMessage.value" class="alert alert-danger" role="alert">
      <div>
        <strong>Command catalog unavailable</strong><span>{{ catalog.errorMessage.value }}</span>
      </div>
      <button type="button" @click="catalog.refetch()">Retry</button>
    </div>

    <div
      v-if="catalog.isLoading.value && !catalog.data.value"
      class="skeleton-grid"
      aria-label="Loading command templates"
    >
      <div v-for="index in 4" :key="index" class="skeleton-block"></div>
    </div>
    <CommandTemplateList
      v-else-if="catalog.data.value"
      :brokers="brokers"
      :templates="templates"
      @execute="selectedTemplate = $event"
    />

    <CommandExecuteDialog
      v-if="selectedTemplate && activeProjectId"
      :key="selectedTemplate.id"
      :project-id="activeProjectId"
      :template="selectedTemplate"
      @close="selectedTemplate = undefined"
    />
  </div>
</template>

<style scoped>
.commands-page {
  width: min(100%, 105rem);
}

.catalog-summary {
  display: flex;
  flex: none;
  border: 1px solid var(--line);
  background: var(--surface);
}

.catalog-summary div {
  min-width: 7rem;
  padding: 0.75rem 1rem;
  border-right: 1px solid var(--line-soft);
}

.catalog-summary div:last-child {
  border-right: 0;
}

.catalog-summary span,
.catalog-summary strong {
  display: block;
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
}

.catalog-summary span {
  color: var(--muted);
  font-size: 0.52rem;
  letter-spacing: 0.07em;
}

.catalog-summary strong {
  margin-top: 0.25rem;
  font-size: 0.8rem;
}

@media (max-width: 700px) {
  .commands-header {
    align-items: stretch;
    flex-direction: column;
  }

  .catalog-summary div {
    flex: 1;
  }
}
</style>
