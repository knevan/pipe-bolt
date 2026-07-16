<script setup lang="ts">
import { watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { useProjectStore } from './project.store'

const route = useRoute()
const router = useRouter()
const projects = useProjectStore()

watch(
  () => route.params.projectId,
  (value) => {
    const projectId = Array.isArray(value) ? value[0] : value
    try {
      projects.selectProject(projectId ?? '')
    } catch {
      void router.replace({ name: 'projects' })
    }
  },
  { immediate: true },
)
</script>

<template>
  <RouterView />
</template>
