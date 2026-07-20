<script setup lang="ts">
import { nextTick, onMounted, onUnmounted, ref, useTemplateRef, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { isNavigationFailure, useRoute, useRouter } from 'vue-router'

import { useAuthStore } from '@/auth'
import { useProjectStore } from '@/projects'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()
const projects = useProjectStore()
const { activeProjectId, projectIds } = storeToRefs(projects)
const menuOpen = ref(false)
const mobileNavigation = ref(false)
const projectInput = ref(activeProjectId.value ?? projectIds.value[0] ?? '')
const projectError = ref<string>()
const mobileMenu = useTemplateRef<HTMLButtonElement>('mobileMenu')

watch(activeProjectId, (projectId) => {
  if (projectId) projectInput.value = projectId
})
watch(
  () => route.fullPath,
  () => {
    closeMobileMenu(true)
  },
)

async function openProject(): Promise<void> {
  projectError.value = undefined
  const previousProjectId = activeProjectId.value
  try {
    const projectId = projects.selectProject(projectInput.value)
    const navigationResult = await router.push({
      name: 'project-overview',
      params: { projectId },
    })
    if (
      navigationResult &&
      isNavigationFailure(navigationResult) &&
      route.params.projectId !== projectId
    ) {
      throw navigationResult
    }
  } catch (error) {
    if (previousProjectId) projects.selectProject(previousProjectId)
    else projects.clearActiveProject()
    projectError.value = error instanceof Error ? error.message : 'Invalid project ID.'
  }
}

async function logout(): Promise<void> {
  auth.clearAccessToken()
  projects.clearActiveProject()
  await router.replace({ name: 'login' })
}

function handleEscape(event: KeyboardEvent): void {
  if (event.key === 'Escape') closeMobileMenu(true)
}

function closeMobileMenu(returnFocus = false): void {
  if (!menuOpen.value) return
  menuOpen.value = false
  if (returnFocus && mobileNavigation.value) void nextTick(() => mobileMenu.value?.focus())
}

let mobileMedia: MediaQueryList | undefined
function syncMobileNavigation(event: MediaQueryListEvent | MediaQueryList): void {
  mobileNavigation.value = event.matches
  if (!event.matches) menuOpen.value = false
}

onMounted(() => {
  mobileMedia = window.matchMedia('(max-width: 900px)')
  syncMobileNavigation(mobileMedia)
  mobileMedia.addEventListener('change', syncMobileNavigation)
  document.addEventListener('keydown', handleEscape)
})
onUnmounted(() => {
  mobileMedia?.removeEventListener('change', syncMobileNavigation)
  document.removeEventListener('keydown', handleEscape)
})
</script>

<template>
  <div class="app-shell">
    <button
      ref="mobileMenu"
      class="mobile-menu"
      type="button"
      aria-label="Toggle navigation"
      aria-controls="primary-sidebar"
      :aria-expanded="menuOpen"
      @click="menuOpen = !menuOpen"
    >
      <span></span><span></span><span></span>
    </button>
    <div v-if="menuOpen" class="nav-scrim" @click="closeMobileMenu(true)"></div>

    <aside
      id="primary-sidebar"
      class="sidebar"
      :class="{ 'sidebar-open': menuOpen }"
      :inert="mobileNavigation && !menuOpen"
      :aria-hidden="mobileNavigation && !menuOpen ? 'true' : undefined"
    >
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
        <RouterLink
          v-if="activeProjectId"
          :to="{ name: 'project-realtime', params: { projectId: activeProjectId } }"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M4 12h3l2-5 3 10 3-7 2 2h3" />
          </svg>
          Realtime events
        </RouterLink>
        <RouterLink
          v-if="activeProjectId"
          :to="{ name: 'project-commands', params: { projectId: activeProjectId } }"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M5 12h10M12 7l5 5-5 5M5 7v10" />
          </svg>
          Commands
        </RouterLink>
        <RouterLink
          v-if="activeProjectId"
          :to="{ name: 'project-rules', params: { projectId: activeProjectId } }"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M5 6h5l2 3h7M5 18h5l2-3h7M5 12h14" />
          </svg>
          Rules
        </RouterLink>

        <p class="nav-label nav-label-spaced">DIAGNOSE</p>
        <RouterLink
          v-if="activeProjectId"
          :to="{ name: 'project-operations', params: { projectId: activeProjectId } }"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M5 5h14v14H5zM8 9h8M8 13h5M8 17h7" />
          </svg>
          Operations
        </RouterLink>
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
