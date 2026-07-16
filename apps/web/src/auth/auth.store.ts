import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

const MAX_TOKEN_LENGTH = 16_384

function normalizeToken(value: string): string {
  const token = value.trim().replace(/^Bearer\s+/i, '')
  if (!token) throw new Error('Bearer token is required.')
  if (token.length > MAX_TOKEN_LENGTH) throw new Error('Bearer token is too long.')
  if (
    /\s/u.test(token) ||
    [...token].some((character) => {
      const codePoint = character.codePointAt(0) ?? 0
      return codePoint <= 0x1f || codePoint === 0x7f
    })
  )
    throw new Error('Bearer token contains invalid characters.')
  return token
}

export const useAuthStore = defineStore('auth', () => {
  const accessToken = ref<string>()
  const isAuthenticated = computed(() => Boolean(accessToken.value))

  function setAccessToken(value: string): void {
    accessToken.value = normalizeToken(value)
  }

  function clearAccessToken(): void {
    accessToken.value = undefined
  }

  return { accessToken, clearAccessToken, isAuthenticated, setAccessToken }
})
