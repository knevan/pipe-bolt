import { ref } from 'vue'
import { useRouter } from 'vue-router'

import { useProjectStore } from '../project.store'

export function useProjectPicker() {
  const router = useRouter()
  const projects = useProjectStore()
  const projectId = ref(projects.activeProjectId ?? projects.projectIds[0] ?? '')
  const errorMessage = ref<string>()

  async function openProject(): Promise<void> {
    errorMessage.value = undefined
    try {
      const selectedId = projects.selectProject(projectId.value)
      await router.push({ name: 'project-overview', params: { projectId: selectedId } })
    } catch (error) {
      errorMessage.value = error instanceof Error ? error.message : 'Project could not be opened.'
    }
  }

  return { errorMessage, openProject, projectId, projectIds: projects.projectIds }
}
