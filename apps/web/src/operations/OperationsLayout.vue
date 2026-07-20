<script setup lang="ts">
import { storeToRefs } from 'pinia'

import { useProjectStore } from '@/projects'

const projects = useProjectStore()
const { activeProjectId } = storeToRefs(projects)
</script>

<template>
  <div class="page operations-page">
    <header class="page-header operations-header">
      <div>
        <p class="eyebrow">PROJECT / {{ activeProjectId }}</p>
        <h1>Operations ledger</h1>
        <p class="page-summary">
          Trace control-plane mutations, processing failures, remediation records, and bounded sink
          delivery outcomes.
        </p>
      </div>
      <div class="operations-signal" aria-hidden="true">
        <span></span><span></span><span></span><span></span>
      </div>
    </header>

    <nav class="operations-tabs" aria-label="Operations views">
      <RouterLink :to="{ name: 'project-operations', params: { projectId: activeProjectId } }"
        >Audit log</RouterLink
      >
      <RouterLink :to="{ name: 'project-failures', params: { projectId: activeProjectId } }"
        >Failures</RouterLink
      >
      <RouterLink
        :to="{ name: 'project-delivery-outcomes', params: { projectId: activeProjectId } }"
      >
        Delivery outcomes
      </RouterLink>
    </nav>

    <RouterView />
  </div>
</template>

<style scoped>
.operations-page {
  width: min(100%, 108rem);
}

.operations-header {
  margin-bottom: 1.25rem;
}

.operations-signal {
  display: flex;
  height: 2.5rem;
  align-items: end;
  gap: 0.3rem;
}

.operations-signal span {
  width: 0.35rem;
  height: 35%;
  background: var(--cyan);
  opacity: 0.3;
}

.operations-signal span:nth-child(2) {
  height: 75%;
  opacity: 0.55;
}

.operations-signal span:nth-child(3) {
  height: 100%;
  opacity: 0.9;
}

.operations-signal span:nth-child(4) {
  height: 55%;
  opacity: 0.45;
}

.operations-tabs {
  display: flex;
  margin-bottom: 1rem;
  overflow-x: auto;
  border-bottom: 1px solid var(--line);
}

.operations-tabs a {
  padding: 0.8rem 1rem;
  color: var(--muted);
  border-bottom: 2px solid transparent;
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.64rem;
  letter-spacing: 0.04em;
  text-decoration: none;
  text-transform: uppercase;
  white-space: nowrap;
}

.operations-tabs a:hover,
.operations-tabs a.router-link-exact-active {
  color: var(--cyan);
  border-bottom-color: var(--cyan);
  background: rgba(85, 201, 195, 0.05);
}

@media (max-width: 700px) {
  .operations-header {
    align-items: start;
  }
}
</style>
