import type { PipeBoltApiDtoErrorResponse } from './generated'

export type ApiErrorKind =
  | 'authentication'
  | 'authorization'
  | 'validation'
  | 'conflict'
  | 'unavailable'
  | 'network'
  | 'timeout'
  | 'unknown'

interface ApiErrorOptions {
  cause?: unknown
  code: string
  details?: unknown
  kind: ApiErrorKind
  message: string
  status?: number
}

export class ApiError extends Error {
  readonly code: string
  readonly details?: unknown
  readonly kind: ApiErrorKind
  readonly status?: number

  constructor({ cause, code, details, kind, message, status }: ApiErrorOptions) {
    super(message, { cause })
    this.name = 'ApiError'
    this.code = code
    this.details = details
    this.kind = kind
    this.status = status
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isErrorResponse(value: unknown): value is PipeBoltApiDtoErrorResponse {
  if (!isRecord(value) || !isRecord(value.error)) return false

  return typeof value.error.code === 'string' && typeof value.error.message === 'string'
}

function kindFromStatus(status?: number): ApiErrorKind {
  switch (status) {
    case 400:
    case 413:
    case 422:
      return 'validation'
    case 401:
      return 'authentication'
    case 403:
      return 'authorization'
    case 409:
      return 'conflict'
    case 502:
    case 503:
    case 504:
      return 'unavailable'
    default:
      return 'unknown'
  }
}

export function toApiError(error: unknown, response?: Response): ApiError {
  if (error instanceof ApiError) return error

  const status = response?.status
  if (isErrorResponse(error)) {
    return new ApiError({
      cause: error,
      code: error.error.code,
      details: error.error.details,
      kind: kindFromStatus(status),
      message: error.error.message,
      status,
    })
  }

  if (error instanceof DOMException && error.name === 'AbortError') {
    return new ApiError({
      cause: error,
      code: 'request_aborted',
      kind: 'network',
      message: 'Request was cancelled.',
      status,
    })
  }

  if (error instanceof Error && error.name === 'TimeoutError') {
    return new ApiError({
      cause: error,
      code: 'request_timeout',
      kind: 'timeout',
      message: 'Backend request timed out.',
      status,
    })
  }

  if (!response) {
    return new ApiError({
      cause: error,
      code: 'network_error',
      kind: 'network',
      message: 'Backend is unreachable.',
    })
  }

  return new ApiError({
    cause: error,
    code: `http_${status}`,
    details: error,
    kind: kindFromStatus(status),
    message: `Backend request failed with status ${status}.`,
    status,
  })
}

export function getApiErrorMessage(error: unknown): string {
  return toApiError(error).message
}
