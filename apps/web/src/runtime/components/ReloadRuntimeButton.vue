<script setup lang="ts">
import { computed } from 'vue'
import type { PipeBoltApiDtoRuntimeReloadResponse } from '@/api/generated'
import { useRuntimeReload } from '../composables/useRuntimeReload'

const props = withDefaults(
  defineProps<{
    configVersion: number
    projectId: string
    reloadRequired?: boolean
  }>(),
  { reloadRequired: false },
)
const emit = defineEmits<{ reloaded: [response: PipeBoltApiDtoRuntimeReloadResponse] }>()
const runtime = useRuntimeReload(() => props.projectId)
const requiresReload = computed(
  () =>
    props.reloadRequired ||
    (runtime.activeVersion.value !== undefined &&
      runtime.activeVersion.value !== props.configVersion),
)
const statusLabel = computed(() =>
  runtime.activeVersion.value === undefined
    ? 'Runtime status unavailable'
    : requiresReload.value
      ? 'Reload required'
      : 'Runtime synchronized',
)

async function submit(): Promise<void> {
  try {
    const response = await runtime.reload()
    if (
      response.project_id === props.projectId &&
      response.active_version === props.configVersion
    ) {
      emit('reloaded', response)
    }
  } catch {
    // Mutation state exposes the structured error to the template.
  }
}
</script>

<template>
  <section class="reload-panel" :class="{ 'reload-panel-required': requiresReload }">
    <div>
      <p class="kicker">RUNTIME ACTIVATION</p>
      <h2>{{ statusLabel }}</h2>
      <p>
        Persisted version {{ configVersion }} · active version
        {{ runtime.activeVersion.value ?? 'unavailable' }}. Runtime changes require explicit
        activation.
      </p>
    </div>
    <form class="reload-form" @submit.prevent="submit">
      <label>
        <span>Audit reason (optional)</span>
        <input
          v-model="runtime.reason.value"
          maxlength="1024"
          placeholder="Activate reviewed config"
        />
      </label>
      <button class="button button-primary" type="submit" :disabled="runtime.isLoading.value">
        {{ runtime.isLoading.value ? 'Reloading...' : 'Reload runtime' }}
      </button>
    </form>
    <p v-if="runtime.errorMessage.value" class="form-error" role="alert">
      {{ runtime.errorMessage.value }}
    </p>
  </section>
</template>

<style scoped>
.reload-panel {
  display: grid;
  padding: 1.35rem;
  grid-template-columns: minmax(14rem, 1fr) minmax(18rem, 1.3fr);
  align-items: end;
  gap: 1.5rem;
  border: 1px solid var(--line);
  border-radius: 0.35rem;
  background: var(--surface);
}

.reload-panel-required {
  border-color: rgba(240, 184, 76, 0.45);
  background: linear-gradient(120deg, rgba(240, 184, 76, 0.1), var(--surface) 60%);
}

.reload-panel h2,
.reload-panel p {
  margin-bottom: 0.35rem;
}

.reload-panel p:not(.kicker, .form-error) {
  color: var(--muted);
  font-size: 0.8rem;
}

.reload-form {
  display: flex;
  align-items: end;
  gap: 0.65rem;
}

.reload-form label {
  display: grid;
  flex: 1;
  gap: 0.4rem;
}

.reload-form label span {
  color: var(--muted);
  font-size: 0.68rem;
}

.reload-form input {
  width: 100%;
  height: 2.75rem;
  padding: 0.65rem;
  color: var(--text);
  border: 1px solid var(--line);
  border-radius: 0.25rem;
  background: #0c171c;
}

.form-error {
  grid-column: 1 / -1;
}

@media (max-width: 760px) {
  .reload-panel {
    grid-template-columns: 1fr;
  }

  .reload-form {
    align-items: stretch;
    flex-direction: column;
  }
}
</style>
