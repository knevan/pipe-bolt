import { apiClient } from '@/api/client'
import { ApiError, toApiError } from '@/api/errors'
import type { ProjectRealtimeSseData } from '@/api/generated'
import type { RealtimeFilters } from './realtime.types'

const MAX_ERROR_BODY_BYTES = 64 * 1024

async function readLimitedText(response: Response): Promise<string> {
  if (!response.body) return ''
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let byteCount = 0
  let output = ''

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      byteCount += value.byteLength
      if (byteCount > MAX_ERROR_BODY_BYTES) throw new Error('Error response exceeded size limit.')
      output += decoder.decode(value, { stream: true })
    }
    return output + decoder.decode()
  } finally {
    await reader.cancel().catch(() => undefined)
    reader.releaseLock()
  }
}

function buildRealtimeUrl(projectId: string, filters: RealtimeFilters): string {
  const data: ProjectRealtimeSseData = {
    path: { project_id: projectId },
    query: filters,
    url: '/projects/{project_id}/realtime/sse',
  }
  return apiClient.buildUrl(data)
}

export async function openRealtimeStream(
  projectId: string,
  filters: RealtimeFilters,
  accessToken: string,
  signal: AbortSignal,
): Promise<Response> {
  const response = await fetch(buildRealtimeUrl(projectId, filters), {
    cache: 'no-store',
    credentials: 'omit',
    headers: {
      Accept: 'text/event-stream',
      Authorization: `Bearer ${accessToken}`,
    },
    method: 'GET',
    redirect: 'follow',
    signal,
  })

  if (!response.ok) {
    let text: string
    try {
      text = await readLimitedText(response)
    } catch (error) {
      throw toApiError(error, response)
    }
    let error: unknown = text
    try {
      error = JSON.parse(text) as unknown
    } catch {
      // Keep non-JSON error text for structured fallback mapping.
    }
    throw toApiError(error, response)
  }

  if (!response.headers.get('content-type')?.toLowerCase().includes('text/event-stream')) {
    await response.body?.cancel()
    throw new ApiError({
      code: 'invalid_stream_content_type',
      kind: 'unknown',
      message: 'Backend returned a non-SSE realtime response.',
    })
  }
  if (!response.body) {
    throw new ApiError({
      code: 'missing_stream_body',
      kind: 'unknown',
      message: 'Backend returned an empty realtime stream.',
    })
  }

  return response
}
