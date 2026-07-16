<script setup lang="ts">
import { useProjectPicker } from './composables/useProjectPicker'

const { errorMessage, openProject, projectId, projectIds } = useProjectPicker()
</script>

<template>
  <div class="page page-narrow">
    <header class="page-header">
      <div>
        <p class="eyebrow">PROJECT CONTEXT</p>
        <h1>Choose operating scope</h1>
        <p class="page-summary">
          Every config, runtime, event, and command action is isolated by project ID.
        </p>
      </div>
    </header>

    <section class="panel project-picker-panel">
      <div class="panel-accent"></div>
      <div>
        <p class="kicker">OPEN PROJECT</p>
        <h2>Enter a project identifier</h2>
        <p class="muted">
          Project discovery is not exposed by the current API contract. Known deployment IDs appear
          below.
        </p>
      </div>

      <form class="project-picker-form" @submit.prevent="openProject">
        <label for="project-id">Project ID</label>
        <div class="input-action-row">
          <input
            id="project-id"
            v-model="projectId"
            autocomplete="off"
            list="known-projects"
            placeholder="project-production"
            required
            spellcheck="false"
          />
          <datalist id="known-projects">
            <option v-for="id in projectIds" :key="id" :value="id"></option>
          </datalist>
          <button class="button button-primary" type="submit">Open project</button>
        </div>
        <p v-if="errorMessage" class="form-error" role="alert">{{ errorMessage }}</p>
      </form>
    </section>
  </div>
</template>
