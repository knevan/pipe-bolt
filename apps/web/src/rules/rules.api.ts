import * as v from 'valibot'

import {
  getProjectConfig,
  putProjectConfig,
  type PipeBoltApiDtoProjectConfigDocumentV1,
  type PipeBoltApiDtoProjectConfigResponse,
  type PipeBoltApiDtoProjectConfigWriteResponse,
} from '@/api/generated'
import {
  vPipeBoltApiDtoProjectConfigResponse,
  vPipeBoltApiDtoProjectConfigWriteResponse,
} from '@/api/generated/valibot.gen'
import { ApiError } from '@/api/errors'

const REDACTED_SECRET = '<redacted>'
const MAX_CONFIG_NODES = 100_000

export const RULE_CONFIG_QUERY_KEYS = {
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

function assertSafeVersion(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw contractError(`Backend returned an unsafe ${label}.`)
  }
}

function assertSecretsRedacted(config: PipeBoltApiDtoProjectConfigDocumentV1): void {
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

function assertSafeJsonNumbers(input: unknown): void {
  const stack: unknown[] = [input]
  let nodes = 0
  while (stack.length > 0) {
    const current = stack.pop()
    nodes += 1
    if (nodes > MAX_CONFIG_NODES) {
      throw contractError(`Configuration exceeds ${MAX_CONFIG_NODES} JSON nodes.`)
    }
    if (typeof current === 'number') {
      if (
        !Number.isFinite(current) ||
        (Number.isInteger(current) && !Number.isSafeInteger(current))
      ) {
        throw contractError('Configuration contains an unsafe JSON number.')
      }
    } else if (Array.isArray(current)) {
      if (nodes + stack.length + current.length > MAX_CONFIG_NODES) {
        throw contractError(`Configuration exceeds ${MAX_CONFIG_NODES} JSON nodes.`)
      }
      for (const item of current) stack.push(item)
    } else if (typeof current === 'object' && current !== null) {
      const values = Object.values(current)
      if (nodes + stack.length + values.length > MAX_CONFIG_NODES) {
        throw contractError(`Configuration exceeds ${MAX_CONFIG_NODES} JSON nodes.`)
      }
      for (const item of values) stack.push(item)
    }
  }
}

export async function fetchRuleConfig(
  projectId: string,
  signal?: AbortSignal,
): Promise<PipeBoltApiDtoProjectConfigResponse> {
  const { data } = await getProjectConfig({
    path: { project_id: projectId },
    signal,
    throwOnError: true,
  })
  let parsed: v.SafeParseResult<typeof vPipeBoltApiDtoProjectConfigResponse>
  try {
    parsed = v.safeParse(vPipeBoltApiDtoProjectConfigResponse, data)
  } catch (error) {
    throw contractError('Backend returned an invalid project configuration.', error)
  }
  if (!parsed.success || data.config.project_id !== projectId) {
    throw contractError('Backend returned an invalid project configuration.', parsed.issues)
  }
  assertSafeVersion(data.version, 'configuration version')
  assertSafeVersion(data.schema_version, 'schema version')
  assertSafeJsonNumbers(data.config)
  assertSecretsRedacted(data.config)
  return data
}

export async function saveRuleConfig(
  projectId: string,
  config: PipeBoltApiDtoProjectConfigDocumentV1,
  expectedVersion: number,
  reason: string,
  signal?: AbortSignal,
): Promise<PipeBoltApiDtoProjectConfigWriteResponse> {
  const { data } = await putProjectConfig({
    body: {
      config,
      expected_version: expectedVersion,
      reason,
    },
    path: { project_id: projectId },
    signal,
    throwOnError: true,
  })
  let parsed: v.SafeParseResult<typeof vPipeBoltApiDtoProjectConfigWriteResponse>
  try {
    parsed = v.safeParse(vPipeBoltApiDtoProjectConfigWriteResponse, data)
  } catch (error) {
    throw contractError('Backend returned an invalid config write receipt.', error)
  }
  if (!parsed.success || data.project_id !== projectId) {
    throw contractError('Backend returned an invalid config write receipt.', parsed.issues)
  }
  assertSafeVersion(data.version, 'configuration version')
  return data
}
