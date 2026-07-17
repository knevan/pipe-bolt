<script setup lang="ts">
import { useSecretMasking } from '../composables/useSecretMasking'

const model = defineModel<string>({ required: true })
const { blur, displayValue, focus, input } = useSecretMasking(model)
</script>

<template>
  <input
    :value="displayValue"
    autocomplete="new-password"
    type="password"
    @blur="blur"
    @focus="focus"
    @input="input"
  />
</template>
