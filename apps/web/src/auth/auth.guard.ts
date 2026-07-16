import type { Pinia } from 'pinia'
import type { NavigationGuard } from 'vue-router'

import { useAuthStore } from './auth.store'

export function createAuthGuard(pinia: Pinia): NavigationGuard {
  return (to) => {
    const auth = useAuthStore(pinia)

    if (to.meta.requiresAuth && !auth.isAuthenticated) {
      return {
        name: 'login',
        query: { redirect: to.fullPath },
      }
    }

    if (to.name === 'login' && auth.isAuthenticated) return { name: 'projects' }
  }
}
