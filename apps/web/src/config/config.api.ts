import {
  getProjectConfig,
  putProjectConfig,
  type PipeBoltApiDtoProjectConfigResponse,
  type PipeBoltApiDtoProjectConfigWriteResponse,
  type PipeBoltApiDtoUpdateProjectConfigRequest,
} from '@/api/generated'
import { ApiError } from '@/api/errors'
import { isConfigDocument } from './config.validation'
import { REDACTED_SECRET } from './composables/useSecretMasking'

export const CONFIG_QUERY_KEYS = {
  byProject: (projectId: string) => ['project', projectId, 'config'] as const,
}

function contractError(message: string, details?: unknown): ApiError {
  return new ApiError({
    code: 'contract_violation',
    details,
    kind: 'unknown',
    message,
  })
}

function assertSafeVersion(value: number, path: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw contractError(`Backend returned an unsafe ${path}.`)
  }
}

function assertSecretsRedacted(config: PipeBoltApiDtoProjectConfigResponse['config']): void {
  for (const broker of config.brokers) {
    if (broker.credentials && broker.credentials.password !== REDACTED_SECRET) {
      throw contractError('Backend returned an unredacted broker password.')
    }
  }
  for (const sink of config.sinks) {
    if (sink.kind.type !== 'webhook') continue
    if (sink.kind.headers.some((header) => header.value !== REDACTED_SECRET)) {
      throw contractError('Backend returned an unredacted webhook header.')
    }
  }
}

export async function fetchProjectConfig(
  projectId: string,
  signal?: AbortSignal,
): Promise<PipeBoltApiDtoProjectConfigResponse> {
  const { data } = await getProjectConfig({
    path: { project_id: projectId },
    signal,
    throwOnError: true,
  })
  if (!isConfigDocument(data.config) || data.config.project_id !== projectId) {
    throw contractError('Backend returned an invalid project configuration document.')
  }
  assertSafeVersion(data.version, 'configuration version')
  assertSafeVersion(data.schema_version, 'schema version')
  assertSecretsRedacted(data.config)
  return data
}

export async function saveProjectConfig(
  projectId: string,
  body: PipeBoltApiDtoUpdateProjectConfigRequest,
): Promise<PipeBoltApiDtoProjectConfigWriteResponse> {
  const { data } = await putProjectConfig({
    body,
    path: { project_id: projectId },
    throwOnError: true,
  })
  if (data.project_id !== projectId)
    throw contractError('Config write response project ID mismatch.')
  assertSafeVersion(data.version, 'configuration version')
  if (typeof data.reload_required !== 'boolean' || !data.revision_id || !data.config_hash) {
    throw contractError('Backend returned an invalid config write response.')
  }
  return data
}
