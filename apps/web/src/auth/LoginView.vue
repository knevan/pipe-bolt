<script setup lang="ts">
import { useAuthBootstrap } from './composables/useAuthBootstrap'

const { errorMessage, submit, token } = useAuthBootstrap()
</script>

<template>
  <main class="login-page">
    <section class="login-visual" aria-hidden="true">
      <div class="brand-mark">PB</div>
      <p class="eyebrow">CONTROL PLANE / INTERNAL ACCESS</p>
      <h1>Signals in.<br />Decisions out.</h1>
      <p class="login-summary">
        Operate broker ingestion, normalized pipelines, and governed commands from one project-aware
        surface.
      </p>
      <div class="signal-lines">
        <span></span><span></span><span></span><span></span><span></span>
      </div>
    </section>

    <section class="login-panel">
      <div class="login-form-wrap">
        <p class="eyebrow">AUTH BOOTSTRAP</p>
        <h2>Connect to Pipe Bolt</h2>
        <p class="muted">
          Enter an API bearer token. It stays in memory and is cleared when this tab reloads.
        </p>

        <form class="login-form" @submit.prevent="submit">
          <label for="access-token">Bearer token</label>
          <textarea
            id="access-token"
            v-model="token"
            autocomplete="off"
            autofocus
            placeholder="eyJhbGciOi..."
            required
            rows="4"
            spellcheck="false"
          ></textarea>
          <p v-if="errorMessage" class="form-error" role="alert">{{ errorMessage }}</p>
          <button class="button button-primary" type="submit">Open control plane</button>
        </form>

        <p class="security-note">
          <span class="status-dot status-dot-safe"></span>
          Token is never persisted to localStorage or sessionStorage.
        </p>
      </div>
    </section>
  </main>
</template>
