import type { RouteRecordRaw } from 'vue-router'

export const configRoutes: RouteRecordRaw[] = [
  {
    path: 'config',
    name: 'project-config',
    component: () => import('./ConfigEditorView.vue'),
    meta: { requiresAuth: true, title: 'Project configuration' },
  },
]
