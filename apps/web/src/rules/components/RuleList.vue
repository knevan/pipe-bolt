<script setup lang="ts">
import type { PipeBoltDomainRuleRuleDefinition } from '@/api/generated'

defineProps<{
  isSaving: boolean
  projectId: string
  rules: ReadonlyArray<PipeBoltDomainRuleRuleDefinition>
}>()
const emit = defineEmits<{
  toggle: [rule: PipeBoltDomainRuleRuleDefinition, enabled: boolean]
}>()

function conditionLabel(rule: PipeBoltDomainRuleRuleDefinition): string {
  return rule.condition ? rule.condition.op.replaceAll('_', ' ') : 'always'
}
</script>

<template>
  <section v-if="rules.length" class="rule-grid" aria-label="Project rules">
    <article v-for="rule in rules" :key="rule.id" class="rule-card panel">
      <header>
        <div>
          <p class="kicker">{{ rule.id }}</p>
          <h2>{{ rule.name }}</h2>
        </div>
        <label class="status-toggle">
          <input
            :checked="rule.enabled"
            type="checkbox"
            :disabled="isSaving"
            @change="emit('toggle', rule, ($event.currentTarget as HTMLInputElement).checked)"
          />
          {{ rule.enabled ? 'Enabled' : 'Disabled' }}
        </label>
      </header>
      <dl>
        <div>
          <dt>Trigger</dt>
          <dd>{{ rule.trigger.type.replaceAll('_', ' ') }}</dd>
        </div>
        <div>
          <dt>Condition</dt>
          <dd>{{ conditionLabel(rule) }}</dd>
        </div>
        <div>
          <dt>Actions</dt>
          <dd>{{ rule.actions.length }}</dd>
        </div>
      </dl>
      <ul class="action-summary">
        <li v-for="(action, index) in rule.actions" :key="`${action.type}-${index}`">
          {{ action.type.replaceAll('_', ' ') }}
        </li>
      </ul>
      <footer>
        <span>Changes require explicit runtime reload.</span>
        <RouterLink
          class="button button-secondary"
          :to="{ name: 'project-rule-edit', params: { projectId, ruleId: rule.id } }"
        >
          Edit rule
        </RouterLink>
      </footer>
    </article>
  </section>
  <section v-else class="empty-rules panel">
    <p class="kicker">NO RULES</p>
    <h2>Rule pipeline is empty</h2>
    <p>Create a typed Rule AST without scripts or direct side effects.</p>
  </section>
</template>

<style scoped>
.rule-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(min(100%, 24rem), 1fr));
  gap: 1rem;
}

.rule-card {
  display: grid;
  overflow: hidden;
  grid-template-rows: auto auto 1fr auto;
}

.rule-card header,
.rule-card footer {
  display: flex;
  padding: 1rem;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
}

.rule-card header {
  border-bottom: 1px solid var(--line-soft);
}

.rule-card h2,
.rule-card p {
  margin-bottom: 0;
}

.status-toggle {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  color: var(--muted);
  font-size: 0.66rem;
}

.status-toggle input {
  accent-color: var(--safe);
}

.rule-card dl {
  display: grid;
  margin: 0;
  grid-template-columns: 1.3fr 1fr 0.55fr;
  border-bottom: 1px solid var(--line-soft);
}

.rule-card dl div {
  min-width: 0;
  padding: 0.75rem;
  border-right: 1px solid var(--line-soft);
}

.rule-card dt {
  color: var(--muted);
  font-size: 0.56rem;
}

.rule-card dd {
  overflow: hidden;
  margin: 0.25rem 0 0;
  font-size: 0.7rem;
  text-overflow: ellipsis;
  text-transform: capitalize;
  white-space: nowrap;
}

.action-summary {
  display: flex;
  margin: 0;
  padding: 0.9rem 1rem;
  align-content: start;
  flex-wrap: wrap;
  gap: 0.4rem;
  list-style: none;
}

.action-summary li {
  padding: 0.3rem 0.4rem;
  color: var(--cyan);
  border: 1px solid var(--line);
  font-size: 0.58rem;
  text-transform: capitalize;
}

.rule-card footer {
  border-top: 1px solid var(--line-soft);
}

.rule-card footer span {
  color: var(--muted);
  font-size: 0.62rem;
}

.rule-card footer .button {
  display: inline-flex;
  align-items: center;
  text-decoration: none;
}

.empty-rules {
  padding: 2.5rem;
  text-align: center;
}

.empty-rules p:last-child {
  margin-bottom: 0;
  color: var(--muted);
}
</style>
