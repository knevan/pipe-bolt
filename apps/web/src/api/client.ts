import { client } from './generated/client.gen'
import { toApiError } from './errors'

const DEFAULT_TIMEOUT_MS = 10_000
const MIN_TIMEOUT_MS = 1_000
const MAX_TIMEOUT_MS = 120_000

let errorInterceptorId: number | undefined

function getBaseUrl(): string {
  const configuredUrl = import.meta.env.VITE_API_BASE_URL?.trim()
  const baseUrl = configuredUrl || window.location.origin

  try {
    const url = new URL(baseUrl, window.location.origin)
    if (url.protocol !== 'http:' && url.protocol !== 'https:')
      throw new Error('Unsupported protocol')
    return url.href.replace(/\/$/, '')
  } catch (error) {
    throw new Error('VITE_API_BASE_URL must be a valid HTTP(S) URL.', { cause: error })
  }
}

function getTimeout(): number {
  const value = Number(import.meta.env.VITE_API_TIMEOUT_MS ?? DEFAULT_TIMEOUT_MS)
  if (!Number.isFinite(value)) return DEFAULT_TIMEOUT_MS

  return Math.min(MAX_TIMEOUT_MS, Math.max(MIN_TIMEOUT_MS, Math.trunc(value)))
}

export interface ApiClientBootstrap {
  getAccessToken: () => string | undefined
}

export function initializeApiClient({ getAccessToken }: ApiClientBootstrap): void {
  client.setConfig({
    auth: getAccessToken,
    baseUrl: getBaseUrl(),
    retry: 0,
    timeout: getTimeout(),
  })

  if (errorInterceptorId !== undefined) client.interceptors.error.eject(errorInterceptorId)
  errorInterceptorId = client.interceptors.error.use((error, response) =>
    toApiError(error, response),
  )
}

export { client as apiClient }
