import type { RouteRecordRaw } from 'vue-router'

export const authRoutes: RouteRecordRaw[] = [
  {
    path: '/login',
    name: 'login',
    component: () => import('./LoginView.vue'),
    meta: { requiresAuth: false, title: 'Sign in' },
  },
]
