import {
  getAuditEvents,
  getDeliveryOutcomes,
  getFailures,
  resolveFailure,
  type PipeBoltApiDtoAuditEventListResponse,
  type PipeBoltApiDtoDeliveryOutcomeListResponse,
  type PipeBoltApiDtoFailureListResponse,
  type PipeBoltApiDtoResolveFailureRequest,
  type PipeBoltApiDtoResolveFailureResponse,
} from '@/api/generated'
import { ApiError } from '@/api/errors'

export const DEFAULT_OPERATION_LIMIT = 100
export const MIN_OPERATION_LIMIT = 1
export const MAX_OPERATION_LIMIT = 500
export const MAX_RESOLUTION_BYTES = 2_048
export const MAX_RESOLUTION_REASON_BYTES = 1_024

export interface OperationListOptions {
  before?: string
  limit: number
}

function contractError(message: string, details?: unknown): ApiError {
  return new ApiError({
    code: 'contract_violation',
    details,
    kind: 'unknown',
    message,
  })
}

function validationError(message: string): ApiError {
  return new ApiError({ code: 'invalid_request', kind: 'validation', message })
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0
}

function isTimestamp(value: unknown): value is string {
  return typeof value === 'string' && !Number.isNaN(Date.parse(value))
}

function isNonNegativeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) >= 0
}

function assertProjectId(projectId: string): void {
  if (!projectId) throw validationError('Project ID is required.')
}

function assertPage(
  data: { items: unknown; limit: unknown; next_before?: unknown },
  requestedLimit: number,
): void {
  if (
    !Array.isArray(data.items) ||
    data.limit !== requestedLimit ||
    (data.next_before != null && !isTimestamp(data.next_before))
  ) {
    throw contractError('Backend returned an invalid operational page.', data)
  }
}

function assertAuditPage(
  data: PipeBoltApiDtoAuditEventListResponse,
  projectId: string,
  requestedLimit: number,
): void {
  assertPage(data, requestedLimit)
  const valid = data.items.every(
    (item) =>
      isNonEmptyString(item.audit_event_id) &&
      isNonEmptyString(item.action) &&
      isNonEmptyString(item.status) &&
      isNonEmptyString(item.target_id) &&
      isNonEmptyString(item.target_type) &&
      isTimestamp(item.occurred_at) &&
      isRecord(item.metadata) &&
      item.project_id === projectId,
  )
  if (!valid) throw contractError('Backend returned an invalid audit event.', data)
}

function assertFailurePage(
  data: PipeBoltApiDtoFailureListResponse,
  projectId: string,
  requestedLimit: number,
): void {
  assertPage(data, requestedLimit)
  const valid = data.items.every(
    (item) =>
      item.project_id === projectId &&
      isNonEmptyString(item.failure_id) &&
      isNonEmptyString(item.component) &&
      isNonEmptyString(item.failure_kind) &&
      isNonEmptyString(item.message) &&
      isNonEmptyString(item.severity) &&
      isTimestamp(item.occurred_at) &&
      (item.resolved_at == null || isTimestamp(item.resolved_at)) &&
      isRecord(item.details),
  )
  if (!valid) throw contractError('Backend returned an invalid failure event.', data)
}

function assertDeliveryPage(
  data: PipeBoltApiDtoDeliveryOutcomeListResponse,
  projectId: string,
  requestedLimit: number,
): void {
  assertPage(data, requestedLimit)
  const valid = data.items.every(
    (item) =>
      item.project_id === projectId &&
      isNonEmptyString(item.delivery_id) &&
      isNonEmptyString(item.event_id) &&
      isNonEmptyString(item.sink_id) &&
      isNonEmptyString(item.status) &&
      isTimestamp(item.occurred_at) &&
      Number.isSafeInteger(item.attempt) &&
      item.attempt > 0 &&
      (item.duration_ms == null || isNonNegativeInteger(item.duration_ms)) &&
      (item.response_body_bytes == null || isNonNegativeInteger(item.response_body_bytes)) &&
      (item.http_status == null ||
        (Number.isSafeInteger(item.http_status) &&
          item.http_status >= 100 &&
          item.http_status <= 599)),
  )
  if (!valid) throw contractError('Backend returned an invalid delivery outcome.', data)
}

export function clampOperationLimit(value: number): number {
  if (!Number.isFinite(value)) return DEFAULT_OPERATION_LIMIT
  return Math.min(MAX_OPERATION_LIMIT, Math.max(MIN_OPERATION_LIMIT, Math.trunc(value)))
}

export async function fetchAuditEvents(
  projectId: string,
  options: OperationListOptions,
  signal?: AbortSignal,
): Promise<PipeBoltApiDtoAuditEventListResponse> {
  assertProjectId(projectId)
  const limit = clampOperationLimit(options.limit)
  const { data } = await getAuditEvents({
    path: { project_id: projectId },
    query: { before: options.before, limit },
    signal,
    throwOnError: true,
  })
  assertAuditPage(data, projectId, limit)
  return data
}

export async function fetchFailures(
  projectId: string,
  options: OperationListOptions & { unresolvedOnly: boolean },
  signal?: AbortSignal,
): Promise<PipeBoltApiDtoFailureListResponse> {
  assertProjectId(projectId)
  const limit = clampOperationLimit(options.limit)
  const { data } = await getFailures({
    path: { project_id: projectId },
    query: {
      before: options.before,
      limit,
      unresolved_only: options.unresolvedOnly || undefined,
    },
    signal,
    throwOnError: true,
  })
  assertFailurePage(data, projectId, limit)
  return data
}

export async function fetchDeliveryOutcomes(
  projectId: string,
  options: OperationListOptions,
  signal?: AbortSignal,
): Promise<PipeBoltApiDtoDeliveryOutcomeListResponse> {
  assertProjectId(projectId)
  const limit = clampOperationLimit(options.limit)
  const { data } = await getDeliveryOutcomes({
    path: { project_id: projectId },
    query: { before: options.before, limit },
    signal,
    throwOnError: true,
  })
  assertDeliveryPage(data, projectId, limit)
  return data
}

function normalizeResolutionRequest(
  body: PipeBoltApiDtoResolveFailureRequest,
): PipeBoltApiDtoResolveFailureRequest {
  const resolution = body.resolution.trim()
  const reason = body.reason?.trim() || undefined
  const encoder = new TextEncoder()
  if (!resolution) throw validationError('Resolution note is required.')
  if (encoder.encode(resolution).byteLength > MAX_RESOLUTION_BYTES) {
    throw validationError(`Resolution note exceeds ${MAX_RESOLUTION_BYTES} UTF-8 bytes.`)
  }
  if (reason && encoder.encode(reason).byteLength > MAX_RESOLUTION_REASON_BYTES) {
    throw validationError(`Resolution reason exceeds ${MAX_RESOLUTION_REASON_BYTES} UTF-8 bytes.`)
  }
  return { reason, resolution }
}

export async function submitFailureResolution(
  projectId: string,
  failureId: string,
  body: PipeBoltApiDtoResolveFailureRequest,
): Promise<PipeBoltApiDtoResolveFailureResponse> {
  assertProjectId(projectId)
  if (!failureId) throw validationError('Failure ID is required.')
  const { data } = await resolveFailure({
    body: normalizeResolutionRequest(body),
    path: { failure_id: failureId, project_id: projectId },
    throwOnError: true,
  })
  if (data.failure_id !== failureId || data.resolved !== true) {
    throw contractError('Backend returned an invalid failure resolution receipt.', data)
  }
  return data
}
