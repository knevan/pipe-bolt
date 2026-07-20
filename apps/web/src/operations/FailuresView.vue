<script setup lang="ts">
import { shallowRef, watch } from 'vue'
import { storeToRefs } from 'pinia'

import type {
  PipeBoltApiDtoFailureEventResponse,
  PipeBoltApiDtoResolveFailureRequest,
} from '@/api/generated'
import { useProjectStore } from '@/projects'
import FailureTable from './components/FailureTable.vue'
import OperationalPager from './components/OperationalPager.vue'
import ResolveFailureDialog from './components/ResolveFailureDialog.vue'
import { useFailureLog, useResolveFailure, type FailureFilter } from './composables/useOperations'

const projects = useProjectStore()
const { activeProjectId } = storeToRefs(projects)
const failures = useFailureLog(activeProjectId)
const resolution = useResolveFailure(activeProjectId)
const selectedFailure = shallowRef<PipeBoltApiDtoFailureEventResponse>()

function updateFilter(event: Event): void {
  failures.setFilter((event.currentTarget as HTMLSelectElement).value as FailureFilter)
}

function openResolution(failure: PipeBoltApiDtoFailureEventResponse): void {
  resolution.reset()
  selectedFailure.value = failure
}

async function resolveSelected(body: PipeBoltApiDtoResolveFailureRequest): Promise<void> {
  if (!selectedFailure.value) return
  const resolved = await resolution.resolve(selectedFailure.value.failure_id, body)
  if (resolved) selectedFailure.value = undefined
}

watch(activeProjectId, () => {
  selectedFailure.value = undefined
})
</script>

<template>
  <section class="operation-panel panel" aria-labelledby="failures-heading">
    <header class="operation-panel-heading">
      <div>
        <p class="kicker">INGESTION / PROCESSING / SINK</p>
        <h2 id="failures-heading">Failure log</h2>
      </div>
      <label class="failure-filter">
        <span>STATUS</span>
        <select
          :value="failures.filter.value"
          :disabled="failures.isLoading.value"
          @change="updateFilter"
        >
          <option value="unresolved">Unresolved</option>
          <option value="all">All statuses</option>
        </select>
      </label>
    </header>

    <div v-if="failures.errorMessage.value" class="operation-alert alert alert-danger" role="alert">
      <div>
        <strong>Failure log unavailable</strong><span>{{ failures.errorMessage.value }}</span>
      </div>
      <button type="button" @click="failures.refetch()">Retry</button>
    </div>
    <div
      v-if="failures.isLoading.value && !failures.data.value"
      class="operation-loading"
      aria-label="Loading failures"
    >
      <span></span><span></span><span></span>
    </div>
    <FailureTable v-else :items="failures.items.value" @resolve="openResolution" />
    <OperationalPager
      :can-next="failures.canGoNext.value"
      :can-previous="failures.canGoPrevious.value"
      :count="failures.items.value.length"
      :limit="failures.limit.value"
      :loading="failures.isLoading.value"
      :page="failures.pageNumber.value"
      @next="failures.goNext(failures.data.value?.next_before)"
      @previous="failures.goPrevious"
      @refresh="failures.refetch"
      @update-limit="failures.setLimit"
    />
  </section>

  <ResolveFailureDialog
    v-if="selectedFailure"
    :key="selectedFailure.failure_id"
    :error="resolution.errorMessage.value"
    :failure="selectedFailure"
    :loading="resolution.isLoading.value"
    @close="selectedFailure = undefined"
    @submit="resolveSelected"
  />
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

.failure-filter {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.failure-filter span {
  color: var(--muted);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.54rem;
  letter-spacing: 0.08em;
}

.failure-filter select {
  min-height: 2.3rem;
  padding: 0.35rem 2rem 0.35rem 0.55rem;
  font-size: 0.68rem;
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
