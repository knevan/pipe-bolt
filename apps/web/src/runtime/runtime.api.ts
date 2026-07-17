import {
  getHealthz,
  getReadyz,
  getRuntimeStatus,
  postRuntimeReload,
  type PipeBoltApiDtoHealthResponse,
  type PipeBoltApiDtoReadinessResponse,
  type PipeBoltApiDtoRuntimeReloadResponse,
  type PipeBoltApiDtoRuntimeStatusResponse,
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

export async function reloadProjectRuntime(
  projectId: string,
  reason?: string,
): Promise<PipeBoltApiDtoRuntimeReloadResponse> {
  const { data } = await postRuntimeReload({
    body: { reason },
    path: { project_id: projectId },
    throwOnError: true,
  })
  if (
    data.project_id !== projectId ||
    !Number.isSafeInteger(data.previous_version) ||
    data.previous_version < 0 ||
    !Number.isSafeInteger(data.active_version) ||
    data.active_version < 0 ||
    !data.audit_event_id ||
    !data.reloaded_at ||
    Number.isNaN(Date.parse(data.reloaded_at)) ||
    (data.old_runtime_shutdown_error !== undefined &&
      data.old_runtime_shutdown_error !== null &&
      typeof data.old_runtime_shutdown_error !== 'string')
  ) {
    throw new ApiError({
      code: 'contract_violation',
      kind: 'unknown',
      message: 'Backend returned an invalid runtime reload response.',
    })
  }
  return data
}

export async function fetchRuntimeStatus(
  projectId: string,
  signal?: AbortSignal,
): Promise<PipeBoltApiDtoRuntimeStatusResponse> {
  const { data } = await getRuntimeStatus({
    path: { project_id: projectId },
    signal,
    throwOnError: true,
  })
  const lifecycleStates = new Set(['running', 'reloading', 'stopping', 'stopped'])
  if (
    data.project_id !== projectId ||
    !lifecycleStates.has(data.state) ||
    (data.active_version !== undefined &&
      data.active_version !== null &&
      (!Number.isSafeInteger(data.active_version) || data.active_version < 0))
  ) {
    throw new ApiError({
      code: 'contract_violation',
      kind: 'unknown',
      message: 'Backend returned an invalid runtime status response.',
    })
  }
  return data
}
