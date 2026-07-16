import type { RouteRecordRaw } from 'vue-router'

export const runtimeRoutes: RouteRecordRaw[] = [
  {
    path: 'runtime',
    name: 'runtime-status',
    component: () => import('./RuntimeStatusView.vue'),
    meta: { requiresAuth: true, title: 'System status' },
  },
]
