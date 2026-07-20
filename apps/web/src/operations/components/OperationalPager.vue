<script setup lang="ts">
const props = defineProps<{
  canNext: boolean
  canPrevious: boolean
  count: number
  limit: number
  loading: boolean
  page: number
}>()
const emit = defineEmits<{
  next: []
  previous: []
  refresh: []
  updateLimit: [value: number]
}>()

function updateLimit(event: Event): void {
  emit('updateLimit', Number((event.currentTarget as HTMLSelectElement).value))
}
</script>

<template>
  <footer class="operational-pager" aria-label="Pagination controls">
    <div class="pager-summary">
      <span>PAGE {{ props.page }}</span>
      <strong>{{ props.count }} records</strong>
    </div>
    <label>
      <span>ROWS</span>
      <select :value="props.limit" :disabled="props.loading" @change="updateLimit">
        <option v-for="value in [25, 50, 100, 250, 500]" :key="value" :value="value">
          {{ value }}
        </option>
      </select>
    </label>
    <div class="pager-actions">
      <button
        class="button button-secondary"
        type="button"
        :disabled="props.loading"
        @click="emit('refresh')"
      >
        Refresh
      </button>
      <button
        class="button button-secondary"
        type="button"
        :disabled="props.loading || !props.canPrevious"
        @click="emit('previous')"
      >
        Previous
      </button>
      <button
        class="button button-primary"
        type="button"
        :disabled="props.loading || !props.canNext"
        @click="emit('next')"
      >
        Next
      </button>
    </div>
  </footer>
</template>

<style scoped>
.operational-pager {
  display: flex;
  padding: 0.85rem 1rem;
  align-items: center;
  justify-content: flex-end;
  gap: 1rem;
  border-top: 1px solid var(--line-soft);
  background: rgba(7, 17, 21, 0.4);
}

.pager-summary {
  display: grid;
  margin-right: auto;
  gap: 0.15rem;
}

.pager-summary span,
.operational-pager label span {
  color: var(--muted);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.52rem;
  letter-spacing: 0.08em;
}

.pager-summary strong {
  color: var(--text);
  font-size: 0.7rem;
}

.operational-pager label {
  display: flex;
  align-items: center;
  gap: 0.45rem;
}

.operational-pager select {
  width: 5rem;
  min-height: 2.25rem;
  padding: 0.35rem;
  font-size: 0.7rem;
}

.pager-actions {
  display: flex;
  gap: 0.45rem;
}

@media (max-width: 700px) {
  .operational-pager {
    flex-wrap: wrap;
  }

  .pager-summary {
    width: 100%;
  }

  .pager-actions {
    margin-left: auto;
  }
}
</style>
