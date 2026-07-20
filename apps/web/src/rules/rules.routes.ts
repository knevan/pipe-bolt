import type { RouteRecordRaw } from 'vue-router'

export const ruleRoutes: RouteRecordRaw[] = [
  {
    path: 'rules',
    name: 'project-rules',
    component: () => import('./RuleListView.vue'),
    meta: { requiresAuth: true, title: 'Rules' },
  },
  {
    path: 'rules/new',
    name: 'project-rule-new',
    component: () => import('./RuleBuilderView.vue'),
    meta: { requiresAuth: true, title: 'New rule' },
  },
  {
    path: 'rules/:ruleId',
    name: 'project-rule-edit',
    component: () => import('./RuleBuilderView.vue'),
    meta: { requiresAuth: true, title: 'Edit rule' },
  },
]
