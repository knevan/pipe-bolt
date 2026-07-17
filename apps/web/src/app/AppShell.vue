<script setup lang="ts">
import { ref, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useRoute, useRouter } from 'vue-router'

import { useAuthStore } from '@/auth'
import { useProjectStore } from '@/projects'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()
const projects = useProjectStore()
const { activeProjectId, projectIds } = storeToRefs(projects)
const menuOpen = ref(false)
const projectInput = ref(activeProjectId.value ?? projectIds.value[0] ?? '')
const projectError = ref<string>()

watch(activeProjectId, (projectId) => {
  if (projectId) projectInput.value = projectId
})
watch(
  () => route.fullPath,
  () => {
    menuOpen.value = false
  },
)

async function openProject(): Promise<void> {
  projectError.value = undefined
  try {
    const projectId = projects.selectProject(projectInput.value)
    await router.push({ name: 'project-overview', params: { projectId } })
  } catch (error) {
    projectError.value = error instanceof Error ? error.message : 'Invalid project ID.'
  }
}

async function logout(): Promise<void> {
  auth.clearAccessToken()
  projects.clearActiveProject()
  await router.replace({ name: 'login' })
}
</script>

<template>
  <div class="app-shell">
    <button
      class="mobile-menu"
      type="button"
      aria-label="Toggle navigation"
      @click="menuOpen = !menuOpen"
    >
      <span></span><span></span><span></span>
    </button>
    <div v-if="menuOpen" class="nav-scrim" @click="menuOpen = false"></div>

    <aside class="sidebar" :class="{ 'sidebar-open': menuOpen }">
      <RouterLink class="shell-brand" to="/projects">
        <span class="brand-mark brand-mark-small">PB</span>
        <span><strong>Pipe Bolt</strong><small>CONTROL PLANE</small></span>
      </RouterLink>

      <nav class="primary-nav" aria-label="Main navigation">
        <p class="nav-label">OPERATE</p>
        <RouterLink :to="{ name: 'projects' }">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 5h16v14H4zM8 9h8M8 13h5" /></svg>
          Projects
        </RouterLink>
        <RouterLink
          v-if="activeProjectId"
          :to="{ name: 'project-overview', params: { projectId: activeProjectId } }"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M4 19V9l8-5 8 5v10M8 19v-6h8v6" />
          </svg>
          Overview
        </RouterLink>
        <RouterLink
          v-if="activeProjectId"
          :to="{ name: 'project-config', params: { projectId: activeProjectId } }"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M5 7h14M8 12h8M10 17h4M7 5v4M17 10v4M12 15v4" />
          </svg>
          Configuration
        </RouterLink>

        <p class="nav-label nav-label-spaced">DIAGNOSE</p>
        <RouterLink :to="{ name: 'runtime-status' }">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 12h3l2-6 4 12 2-6h5" /></svg>
          System status
        </RouterLink>
      </nav>

      <div class="sidebar-footer">
        <div><span class="status-dot status-dot-safe"></span><span>Session active</span></div>
        <button type="button" @click="logout">End session</button>
      </div>
    </aside>

    <div class="shell-main">
      <header class="topbar">
        <div class="topbar-context">
          <span class="topbar-label">ACTIVE PROJECT</span>
          <form @submit.prevent="openProject">
            <input
              v-model="projectInput"
              aria-label="Active project ID"
              autocomplete="off"
              list="shell-projects"
              placeholder="Select project"
              spellcheck="false"
            />
            <datalist id="shell-projects">
              <option v-for="id in projectIds" :key="id" :value="id"></option>
            </datalist>
            <button type="submit" aria-label="Open selected project">Go</button>
          </form>
          <span v-if="projectError" class="topbar-error" role="alert">{{ projectError }}</span>
        </div>
        <div class="topbar-meta"><span>ENV</span><strong>CONTROL</strong></div>
      </header>

      <main class="shell-content">
        <RouterView />
      </main>
    </div>
  </div>
</template>
