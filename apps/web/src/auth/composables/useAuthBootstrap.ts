import { ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { useAuthStore } from '../auth.store'

function safeRedirect(value: unknown): string {
  if (typeof value !== 'string' || !value.startsWith('/') || value.startsWith('//'))
    return '/projects'
  return value
}

export function useAuthBootstrap() {
  const route = useRoute()
  const router = useRouter()
  const auth = useAuthStore()
  const token = ref('')
  const errorMessage = ref<string>()

  async function submit(): Promise<void> {
    errorMessage.value = undefined

    try {
      auth.setAccessToken(token.value)
      token.value = ''
      await router.replace(safeRedirect(route.query.redirect))
    } catch (error) {
      errorMessage.value = error instanceof Error ? error.message : 'Token could not be accepted.'
    }
  }

  return { errorMessage, submit, token }
}
