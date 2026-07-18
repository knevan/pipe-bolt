import {
  getAuditEvents,
  getProjectConfig,
  postExecuteCommand,
  type PipeBoltApiDtoAuditEventResponse,
  type PipeBoltApiDtoCommandExecutionStatusResponse,
  type PipeBoltApiDtoExecuteCommandRequest,
  type PipeBoltApiDtoExecuteCommandResponse,
  type PipeBoltDomainConfigCommandTemplate,
} from '@/api/generated'
import { ApiError } from '@/api/errors'
import type { CommandCatalog, CommandStatusObservation } from './commands.types'

const AUDIT_PAGE_LIMIT = 100
const commandStatuses = new Set<PipeBoltApiDtoCommandExecutionStatusResponse>([
  'queued',
  'published',
  'failed',
])

function contractError(message: string, details?: unknown): ApiError {
  return new ApiError({
    code: 'contract_violation',
    details,
    kind: 'unknown',
    message,
  })
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isCommandTemplate(value: unknown): value is PipeBoltDomainConfigCommandTemplate {
  if (!isRecord(value)) return false
  return (
    typeof value.id === 'string' &&
    typeof value.name === 'string' &&
    typeof value.broker_id === 'string' &&
    typeof value.topic_template === 'string' &&
    typeof value.enabled === 'boolean' &&
    typeof value.retain === 'boolean' &&
    ['at_most_once', 'at_least_once', 'exactly_once'].includes(String(value.qos)) &&
    'payload_template' in value
  )
}

function assertReceipt(
  value: PipeBoltApiDtoExecuteCommandResponse,
  projectId: string,
  templateId: string,
): void {
  if (
    value.project_id !== projectId ||
    value.command_template_id !== templateId ||
    !value.command_execution_id ||
    !value.audit_event_id ||
    !value.broker_id ||
    !value.topic ||
    !commandStatuses.has(value.status) ||
    !Number.isSafeInteger(value.payload_size_bytes) ||
    value.payload_size_bytes < 0 ||
    Number.isNaN(Date.parse(value.queued_at))
  ) {
    throw contractError('Backend returned an invalid command execution receipt.', value)
  }
}

export async function fetchCommandCatalog(
  projectId: string,
  signal?: AbortSignal,
): Promise<CommandCatalog> {
  const { data } = await getProjectConfig({
    path: { project_id: projectId },
    signal,
    throwOnError: true,
  })
  if (
    data.config.project_id !== projectId ||
    !Number.isSafeInteger(data.version) ||
    data.version < 0 ||
    !Array.isArray(data.config.brokers) ||
    !data.config.brokers.every(
      (broker) =>
        isRecord(broker) && typeof broker.id === 'string' && typeof broker.name === 'string',
    ) ||
    !Array.isArray(data.config.command_templates) ||
    !data.config.command_templates.every(isCommandTemplate)
  ) {
    throw contractError('Backend returned an invalid command catalog.')
  }

  const brokers = data.config.brokers.map((broker) => ({ id: broker.id, name: broker.name }))
  return {
    brokers,
    templates: structuredClone(data.config.command_templates),
    version: data.version,
  }
}

export async function executeCommand(
  projectId: string,
  templateId: string,
  body: PipeBoltApiDtoExecuteCommandRequest,
  signal?: AbortSignal,
): Promise<PipeBoltApiDtoExecuteCommandResponse> {
  const { data } = await postExecuteCommand({
    body,
    path: { command_template_id: templateId, project_id: projectId },
    signal,
    throwOnError: true,
  })
  assertReceipt(data, projectId, templateId)
  return data
}

function commandStatusFromAudit(
  event: PipeBoltApiDtoAuditEventResponse,
  executionId: string,
): PipeBoltApiDtoCommandExecutionStatusResponse | undefined {
  if (event.action !== 'command.execute') return undefined
  if (event.metadata.command_execution_id !== executionId) return undefined

  const metadataStatus = event.metadata.status
  if (
    typeof metadataStatus === 'string' &&
    commandStatuses.has(metadataStatus as PipeBoltApiDtoCommandExecutionStatusResponse)
  ) {
    return metadataStatus as PipeBoltApiDtoCommandExecutionStatusResponse
  }
  return event.status === 'failed' ? 'failed' : undefined
}

export async function fetchCommandStatusObservation(
  projectId: string,
  auditEventId: string,
  executionId: string,
  signal?: AbortSignal,
): Promise<CommandStatusObservation | undefined> {
  const { data } = await getAuditEvents({
    path: { project_id: projectId },
    query: { limit: AUDIT_PAGE_LIMIT },
    signal,
    throwOnError: true,
  })
  if (!Array.isArray(data.items)) throw contractError('Backend returned an invalid audit list.')

  const event = data.items.find((item) => item.audit_event_id === auditEventId)
  if (!event) return undefined
  if (event.project_id && event.project_id !== projectId) {
    throw contractError('Audit event project ID mismatch.')
  }
  const status = commandStatusFromAudit(event, executionId)
  return status ? { status } : undefined
}
