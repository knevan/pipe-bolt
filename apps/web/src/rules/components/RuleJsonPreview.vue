<script setup lang="ts">
import type { RuleValidationIssue } from '../rules.types'

defineProps<{
  issues: ReadonlyArray<RuleValidationIssue>
  serialized?: string
}>()
</script>

<template>
  <aside class="json-preview panel">
    <header>
      <div>
        <p class="kicker">TYPED AST</p>
        <h2>JSON preview</h2>
      </div>
      <span :class="{ valid: !issues.length }">
        {{ issues.length ? `${issues.length} issue(s)` : 'VALID LOCALLY' }}
      </span>
    </header>
    <pre v-if="serialized">{{ serialized }}</pre>
    <p v-else class="preview-empty">Preview unavailable until invalid literal JSON is corrected.</p>
    <ol v-if="issues.length" class="preview-issues">
      <li v-for="issue in issues" :key="`${issue.path}-${issue.message}`">
        <code>{{ issue.path }}</code
        ><span>{{ issue.message }}</span>
      </li>
    </ol>
    <p class="preview-note">Read-only preview. Backend remains final validator.</p>
  </aside>
</template>

<style scoped>
.json-preview {
  position: sticky;
  top: 5.3rem;
  align-self: start;
  overflow: hidden;
}

.json-preview header {
  display: flex;
  padding: 1rem;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  border-bottom: 1px solid var(--line-soft);
}

.json-preview h2,
.json-preview p {
  margin-bottom: 0;
}

.json-preview header > span {
  color: var(--danger);
  font-family: 'Cascadia Code', 'SFMono-Regular', Consolas, monospace;
  font-size: 0.55rem;
}

.json-preview header > span.valid {
  color: var(--safe);
}

.json-preview pre {
  max-height: 38rem;
  overflow: auto;
  margin: 0;
  padding: 1rem;
  color: #bdd1d2;
  background: #081216;
  font-size: 0.64rem;
  line-height: 1.55;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.preview-empty,
.preview-note {
  padding: 1rem;
  color: var(--muted);
  font-size: 0.68rem;
}

.preview-issues {
  display: grid;
  max-height: 18rem;
  margin: 0;
  padding: 0.8rem 1rem 0.8rem 2.2rem;
  overflow: auto;
  gap: 0.5rem;
  color: #ff9b8b;
  border-top: 1px solid rgba(239, 128, 110, 0.2);
  font-size: 0.65rem;
}

.preview-issues code,
.preview-issues span {
  display: block;
}

.preview-issues code {
  margin-bottom: 0.15rem;
  color: var(--accent);
}

.preview-note {
  margin: 0;
  border-top: 1px solid var(--line-soft);
}

@media (max-width: 1100px) {
  .json-preview {
    position: static;
  }
}
</style>
