<script setup lang="ts">
import type { OperationTone } from '../operations.format'

defineProps<{
  label: string
  tone?: OperationTone
}>()
</script>

<template>
  <span class="operation-badge" :class="`operation-badge-${tone ?? 'neutral'}`">{{ label }}</span>
</template>

<style scoped>
.operation-badge {
  display: inline-flex;
  padding: 0.2rem 0.45rem;
  align-items: center;
  border: 1px solid var(--line);
  border-radius: 999px;
  color: #9eafb3;
  background: rgba(92, 111, 117, 0.08);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.58rem;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  white-space: nowrap;
}

.operation-badge-safe {
  color: #83d9aa;
  border-color: rgba(98, 200, 149, 0.3);
  background: rgba(24, 75, 52, 0.24);
}

.operation-badge-warning {
  color: #f4be63;
  border-color: rgba(229, 167, 70, 0.28);
  background: rgba(75, 52, 18, 0.24);
}

.operation-badge-danger {
  color: #ff9b8b;
  border-color: rgba(239, 128, 110, 0.3);
  background: rgba(80, 27, 24, 0.25);
}
</style>
