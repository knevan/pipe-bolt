<script setup lang="ts">
import { computed } from 'vue'

import { useSystemStatus } from '../composables/useSystemStatus'

const system = useSystemStatus()
const isRefreshing = computed(
  () => system.health.isLoading.value || system.readiness.isLoading.value,
)
</script>

<template>
  <section class="runtime-monitor">
    <div class="monitor-heading">
      <div>
        <p class="eyebrow">CONTROL PLANE DIAGNOSTICS</p>
        <h1>System status</h1>
        <p class="page-summary">
          Liveness and dependency readiness reported directly by backend probes.
        </p>
      </div>
      <button
        class="button button-secondary"
        type="button"
        :disabled="isRefreshing"
        @click="system.refresh"
      >
        {{ isRefreshing ? 'Checking...' : 'Run checks' }}
      </button>
    </div>

    <div class="probe-grid">
      <article
        class="probe-card"
        :class="{ 'probe-card-good': system.health.data.value?.status === 'ok' }"
      >
        <div class="probe-icon"><span></span></div>
        <div>
          <p class="kicker">LIVENESS / HEALTHZ</p>
          <h2>
            {{
              system.health.data.value?.status ??
              (system.healthError.value ? 'unreachable' : 'checking')
            }}
          </h2>
          <p>
            {{
              system.health.data.value?.service ??
              system.healthError.value ??
              'Awaiting backend response.'
            }}
          </p>
        </div>
      </article>

      <article
        class="probe-card"
        :class="{
          'probe-card-good': system.readiness.data.value?.status === 'ready',
          'probe-card-bad': system.readiness.data.value?.status === 'not_ready',
        }"
      >
        <div class="probe-icon"><span></span></div>
        <div>
          <p class="kicker">READINESS / READYZ</p>
          <h2>
            {{
              system.readiness.data.value?.status ??
              (system.readinessError.value ? 'unreachable' : 'checking')
            }}
          </h2>
          <p>
            {{
              system.readiness.data.value?.service ??
              system.readinessError.value ??
              'Checking dependencies.'
            }}
          </p>
        </div>
      </article>
    </div>

    <div v-if="system.readiness.data.value" class="subsystem-grid">
      <article class="panel subsystem-card">
        <div class="subsystem-title">
          <span
            class="status-dot"
            :class="
              system.readiness.data.value.storage.status === 'ready'
                ? 'status-dot-safe'
                : 'status-dot-danger'
            "
          ></span>
          <div>
            <p class="kicker">DEPENDENCY</p>
            <h2>Storage readiness</h2>
          </div>
        </div>
        <p class="subsystem-state">{{ system.readiness.data.value.storage.status }}</p>
        <p class="muted">
          {{ system.readiness.data.value.storage.message ?? 'Storage accepted readiness probe.' }}
        </p>
      </article>

      <article class="panel subsystem-card">
        <div class="subsystem-title">
          <span
            class="status-dot"
            :class="
              system.readiness.data.value.runtime.status === 'ready'
                ? 'status-dot-safe'
                : 'status-dot-danger'
            "
          ></span>
          <div>
            <p class="kicker">DATA PLANE</p>
            <h2>Runtime readiness</h2>
          </div>
        </div>
        <dl class="subsystem-facts">
          <div>
            <dt>Status</dt>
            <dd>{{ system.readiness.data.value.runtime.status }}</dd>
          </div>
          <div>
            <dt>Lifecycle</dt>
            <dd>{{ system.readiness.data.value.runtime.lifecycle }}</dd>
          </div>
          <div>
            <dt>Active version</dt>
            <dd>{{ system.readiness.data.value.runtime.active_version ?? 'none' }}</dd>
          </div>
          <div>
            <dt>Project</dt>
            <dd>{{ system.readiness.data.value.runtime.project_id }}</dd>
          </div>
        </dl>
        <p v-if="system.readiness.data.value.runtime.message" class="muted">
          {{ system.readiness.data.value.runtime.message }}
        </p>
      </article>
    </div>
  </section>
</template>
