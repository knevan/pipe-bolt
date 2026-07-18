import type { RouteRecordRaw } from 'vue-router'

export const realtimeRoutes: RouteRecordRaw[] = [
  {
    path: 'realtime',
    name: 'project-realtime',
    component: () => import('./RealtimeEventsView.vue'),
    meta: { requiresAuth: true, title: 'Realtime events' },
  },
]
