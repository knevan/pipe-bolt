<script setup lang="ts">
import { storeToRefs } from 'pinia'

import { useProjectStore } from '@/projects'
import AuditLogTable from './components/AuditLogTable.vue'
import OperationalPager from './components/OperationalPager.vue'
import { useAuditLog } from './composables/useOperations'

const projects = useProjectStore()
const { activeProjectId } = storeToRefs(projects)
const audit = useAuditLog(activeProjectId)
</script>

<template>
  <section class="operation-panel panel" aria-labelledby="audit-heading">
    <header class="operation-panel-heading">
      <div>
        <p class="kicker">IMMUTABLE ACTIVITY TRAIL</p>
        <h2 id="audit-heading">Audit log</h2>
      </div>
      <span>Newest first</span>
    </header>

    <div v-if="audit.errorMessage.value" class="operation-alert alert alert-danger" role="alert">
      <div>
        <strong>Audit log unavailable</strong><span>{{ audit.errorMessage.value }}</span>
      </div>
      <button type="button" @click="audit.refetch()">Retry</button>
    </div>
    <div
      v-if="audit.isLoading.value && !audit.data.value"
      class="operation-loading"
      aria-label="Loading audit events"
    >
      <span></span><span></span><span></span>
    </div>
    <AuditLogTable v-else :items="audit.items.value" />
    <OperationalPager
      :can-next="audit.canGoNext.value"
      :can-previous="audit.canGoPrevious.value"
      :count="audit.items.value.length"
      :limit="audit.limit.value"
      :loading="audit.isLoading.value"
      :page="audit.pageNumber.value"
      @next="audit.goNext(audit.data.value?.next_before)"
      @previous="audit.goPrevious"
      @refresh="audit.refetch"
      @update-limit="audit.setLimit"
    />
  </section>
</template>

<style scoped>
.operation-panel {
  overflow: hidden;
}

.operation-panel-heading {
  display: flex;
  padding: 1rem 1.15rem;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--line-soft);
}

.operation-panel-heading h2,
.operation-panel-heading p {
  margin: 0;
}

.operation-panel-heading > span {
  color: var(--muted);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.58rem;
}

.operation-alert {
  margin: 1rem;
}

.operation-loading {
  display: flex;
  min-height: 16rem;
  align-items: center;
  justify-content: center;
  gap: 0.35rem;
}

.operation-loading span {
  width: 0.4rem;
  height: 2rem;
  background: var(--cyan);
  opacity: 0.25;
}

.operation-loading span:nth-child(2) {
  height: 3rem;
  opacity: 0.65;
}
</style>
