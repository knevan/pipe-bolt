import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

const MAX_PROJECT_ID_LENGTH = 128
const MAX_RECENT_PROJECTS = 20

function normalizeProjectId(value: string): string {
  const projectId = value.trim()
  if (!projectId) throw new Error('Project ID is required.')
  if (projectId.length > MAX_PROJECT_ID_LENGTH) throw new Error('Project ID is too long.')
  const containsControlCharacter = [...projectId].some((character) => {
    const codePoint = character.codePointAt(0) ?? 0
    return codePoint <= 0x1f || codePoint === 0x7f
  })
  if (/[\s/#?]/u.test(projectId) || containsControlCharacter) {
    throw new Error('Project ID contains invalid path characters.')
  }
  return projectId
}

function initialProjectIds(): string[] {
  const seen = new Set<string>()

  for (const value of (import.meta.env.VITE_PROJECT_IDS ?? '').split(',')) {
    try {
      seen.add(normalizeProjectId(value))
    } catch {
      // Ignore malformed deployment defaults; operators can still enter a valid project ID.
    }
  }

  return [...seen].slice(0, MAX_RECENT_PROJECTS)
}

export const useProjectStore = defineStore('projects', () => {
  const projectIds = ref(initialProjectIds())
  const activeProjectId = ref<string>()
  const hasActiveProject = computed(() => Boolean(activeProjectId.value))

  function selectProject(value: string): string {
    const projectId = normalizeProjectId(value)
    activeProjectId.value = projectId

    const existingIndex = projectIds.value.indexOf(projectId)
    if (existingIndex >= 0) projectIds.value.splice(existingIndex, 1)
    projectIds.value.unshift(projectId)
    if (projectIds.value.length > MAX_RECENT_PROJECTS) projectIds.value.length = MAX_RECENT_PROJECTS

    return projectId
  }

  function clearActiveProject(): void {
    activeProjectId.value = undefined
  }

  return { activeProjectId, clearActiveProject, hasActiveProject, projectIds, selectProject }
})
