import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'
import type { Pinia } from 'pinia'

import AppShell from './AppShell.vue'
import { authRoutes, createAuthGuard } from '@/auth'
import { commandRoutes } from '@/commands'
import { configRoutes } from '@/config'
import { createProjectRoutes } from '@/projects'
import { realtimeRoutes } from '@/realtime'
import { runtimeRoutes } from '@/runtime'

const projectRoutes = createProjectRoutes([...configRoutes, ...realtimeRoutes, ...commandRoutes])

const protectedRoutes: RouteRecordRaw[] = [
  {
    path: '/',
    component: AppShell,
    meta: { requiresAuth: true, title: 'Pipe Bolt' },
    children: [{ path: '', redirect: { name: 'projects' } }, ...projectRoutes, ...runtimeRoutes],
  },
]

export function createAppRouter(pinia: Pinia) {
  const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    routes: [...authRoutes, ...protectedRoutes],
    scrollBehavior: () => ({ top: 0 }),
  })

  router.beforeEach(createAuthGuard(pinia))
  router.afterEach((to) => {
    document.title = `${to.meta.title} | Pipe Bolt`
  })

  return router
}
