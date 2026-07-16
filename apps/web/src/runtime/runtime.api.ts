import {
  getHealthz,
  getReadyz,
  type PipeBoltApiDtoHealthResponse,
  type PipeBoltApiDtoReadinessResponse,
} from '@/api/generated'
import { ApiError, toApiError } from '@/api/errors'

function isReadinessResponse(value: unknown): value is PipeBoltApiDtoReadinessResponse {
  if (typeof value !== 'object' || value === null) return false
  const response = value as Partial<PipeBoltApiDtoReadinessResponse>
  return (
    (response.status === 'ready' || response.status === 'not_ready') &&
    typeof response.runtime === 'object' &&
    response.runtime !== null &&
    typeof response.storage === 'object' &&
    response.storage !== null
  )
}

export async function fetchLiveness(signal?: AbortSignal): Promise<PipeBoltApiDtoHealthResponse> {
  const { data } = await getHealthz({ signal, throwOnError: true })
  return data
}

export async function fetchReadiness(
  signal?: AbortSignal,
): Promise<PipeBoltApiDtoReadinessResponse> {
  try {
    const { data } = await getReadyz({ signal, throwOnError: true })
    return data
  } catch (error) {
    const apiError = toApiError(error)
    if (
      apiError instanceof ApiError &&
      apiError.status === 503 &&
      isReadinessResponse(apiError.details)
    ) {
      return apiError.details
    }
    throw apiError
  }
}
