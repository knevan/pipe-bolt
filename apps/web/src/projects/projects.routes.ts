import type { RouteRecordRaw } from 'vue-router'

export function createProjectRoutes(featureRoutes: RouteRecordRaw[] = []): RouteRecordRaw[] {
  return [
    {
      path: 'projects',
      name: 'projects',
      component: () => import('./ProjectSelectView.vue'),
      meta: { requiresAuth: true, title: 'Projects' },
    },
    {
      path: 'projects/:projectId',
      component: () => import('./ProjectLayout.vue'),
      meta: { requiresAuth: true, title: 'Project' },
      children: [
        {
          path: '',
          name: 'project-overview',
          component: () => import('./ProjectOverviewView.vue'),
          meta: { requiresAuth: true, title: 'Project overview' },
        },
        ...featureRoutes,
      ],
    },
  ]
}
