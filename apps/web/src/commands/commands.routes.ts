import type { RouteRecordRaw } from 'vue-router'

export const commandRoutes: RouteRecordRaw[] = [
  {
    path: 'commands',
    name: 'project-commands',
    component: () => import('./CommandTemplatesView.vue'),
    meta: { requiresAuth: true, title: 'Command gateway' },
  },
]
