import type { RouteRecordRaw } from 'vue-router'

export const operationRoutes: RouteRecordRaw[] = [
  {
    path: 'operations',
    component: () => import('./OperationsLayout.vue'),
    meta: { requiresAuth: true, title: 'Operations' },
    children: [
      {
        path: '',
        name: 'project-operations',
        component: () => import('./AuditLogView.vue'),
        meta: { requiresAuth: true, title: 'Audit log' },
      },
      {
        path: 'failures',
        name: 'project-failures',
        component: () => import('./FailuresView.vue'),
        meta: { requiresAuth: true, title: 'Failures' },
      },
      {
        path: 'delivery-outcomes',
        name: 'project-delivery-outcomes',
        component: () => import('./DeliveryOutcomesView.vue'),
        meta: { requiresAuth: true, title: 'Delivery outcomes' },
      },
    ],
  },
]
