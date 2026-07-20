<script setup lang="ts">
import { storeToRefs } from 'pinia'

import { useProjectStore } from '@/projects'
import DeliveryOutcomeTable from './components/DeliveryOutcomeTable.vue'
import OperationalPager from './components/OperationalPager.vue'
import { useDeliveryOutcomes } from './composables/useOperations'

const projects = useProjectStore()
const { activeProjectId } = storeToRefs(projects)
const outcomes = useDeliveryOutcomes(activeProjectId)
</script>

<template>
  <section class="operation-panel panel" aria-labelledby="deliveries-heading">
    <header class="operation-panel-heading">
      <div>
        <p class="kicker">WEBHOOK FORWARDER OUTCOMES</p>
        <h2 id="deliveries-heading">Sink deliveries</h2>
      </div>
      <span>Terminal attempts</span>
    </header>

    <div v-if="outcomes.errorMessage.value" class="operation-alert alert alert-danger" role="alert">
      <div>
        <strong>Delivery outcomes unavailable</strong><span>{{ outcomes.errorMessage.value }}</span>
      </div>
      <button type="button" @click="outcomes.refetch()">Retry</button>
    </div>
    <div
      v-if="outcomes.isLoading.value && !outcomes.data.value"
      class="operation-loading"
      aria-label="Loading delivery outcomes"
    >
      <span></span><span></span><span></span>
    </div>
    <DeliveryOutcomeTable v-else :items="outcomes.items.value" />
    <OperationalPager
      :can-next="outcomes.canGoNext.value"
      :can-previous="outcomes.canGoPrevious.value"
      :count="outcomes.items.value.length"
      :limit="outcomes.limit.value"
      :loading="outcomes.isLoading.value"
      :page="outcomes.pageNumber.value"
      @next="outcomes.goNext(outcomes.data.value?.next_before)"
      @previous="outcomes.goPrevious"
      @refresh="outcomes.refetch"
      @update-limit="outcomes.setLimit"
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
