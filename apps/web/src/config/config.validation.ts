import * as v from 'valibot'

import type {
  PipeBoltApiDtoProjectConfigDocumentV1,
  PipeBoltDomainConfigTopicRouteConfig,
} from '@/api/generated'
import { vPipeBoltApiDtoProjectConfigDocumentV1 } from '@/api/generated/valibot.gen'
import { MASKED_SECRET, REDACTED_SECRET } from './composables/useSecretMasking'

export interface ConfigValidationIssue {
  message: string
  path: string
}

const MAX_ISSUES = 100
const ID_PATTERN = /^[A-Za-z0-9_.:-]+$/u

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function addIssue(issues: ConfigValidationIssue[], path: string, message: string): void {
  if (issues.length < MAX_ISSUES) issues.push({ message, path })
}

function validateJsonNumbers(
  input: unknown,
  issues: ConfigValidationIssue[],
  path = 'config',
): void {
  if (typeof input === 'number') {
    if (!Number.isFinite(input) || (Number.isInteger(input) && !Number.isSafeInteger(input))) {
      addIssue(issues, path, 'JSON number must be finite and safely representable.')
    }
    return
  }
  if (Array.isArray(input)) {
    for (const [index, value] of input.entries())
      validateJsonNumbers(value, issues, `${path}.${index}`)
    return
  }
  if (isRecord(input)) {
    for (const [key, value] of Object.entries(input))
      validateJsonNumbers(value, issues, `${path}.${key}`)
  }
}

function validateIntegerField(issues: ConfigValidationIssue[], value: unknown, path: string): void {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    addIssue(issues, path, 'Value must be a non-negative safe integer.')
  }
}

function validateContractNumbers(input: unknown, issues: ConfigValidationIssue[]): void {
  if (!isRecord(input)) return
  if (Array.isArray(input.brokers)) {
    for (const [index, value] of input.brokers.entries()) {
      if (!isRecord(value)) continue
      validateIntegerField(issues, value.keep_alive, `brokers.${index}.keep_alive`)
      if (isRecord(value.reconnect)) {
        validateIntegerField(
          issues,
          value.reconnect.min_delay,
          `brokers.${index}.reconnect.min_delay`,
        )
        validateIntegerField(
          issues,
          value.reconnect.max_delay,
          `brokers.${index}.reconnect.max_delay`,
        )
      }
    }
  }
  if (Array.isArray(input.routes)) {
    for (const [index, value] of input.routes.entries()) {
      if (!isRecord(value) || !isRecord(value.device_id)) continue
      if (value.device_id.type === 'topic_wildcard_index') {
        validateIntegerField(issues, value.device_id.index, `routes.${index}.device_id.index`)
      }
    }
  }
  if (Array.isArray(input.sinks)) {
    for (const [index, value] of input.sinks.entries()) {
      if (!isRecord(value) || !isRecord(value.kind) || value.kind.type !== 'webhook') continue
      validateIntegerField(issues, value.kind.timeout, `sinks.${index}.kind.timeout`)
    }
  }
}

function parseStructure(input: unknown) {
  const numericIssues: ConfigValidationIssue[] = []
  validateJsonNumbers(input, numericIssues)
  validateContractNumbers(input, numericIssues)
  if (numericIssues.length) return { issues: numericIssues, success: false } as const

  try {
    return v.safeParse(vPipeBoltApiDtoProjectConfigDocumentV1, input)
  } catch {
    return {
      issues: [{ message: 'Configuration contains an invalid numeric value.', path: 'config' }],
      success: false,
    } as const
  }
}

function validateText(
  issues: ConfigValidationIssue[],
  path: string,
  value: string,
  maxBytes: number,
): void {
  if (!value.trim()) addIssue(issues, path, 'Value is required.')
  if (new TextEncoder().encode(value).length > maxBytes) {
    addIssue(issues, path, `Value must not exceed ${maxBytes} UTF-8 bytes.`)
  }
}

function validateId(issues: ConfigValidationIssue[], path: string, value: string): void {
  validateText(issues, path, value, 128)
  if (value && !ID_PATTERN.test(value)) {
    addIssue(issues, path, 'Use only letters, numbers, underscore, dash, dot, or colon.')
  }
}

function validateTopicFilter(
  issues: ConfigValidationIssue[],
  path: string,
  route: PipeBoltDomainConfigTopicRouteConfig,
): void {
  validateText(issues, path, route.topic_filter, 1024)
  const segments = route.topic_filter.split('/')
  for (const [index, segment] of segments.entries()) {
    if (segment.includes('#') && (segment !== '#' || index !== segments.length - 1)) {
      addIssue(issues, path, '`#` must be the final complete topic segment.')
      break
    }
    if (segment.includes('+') && segment !== '+') {
      addIssue(issues, path, '`+` must occupy a complete topic segment.')
      break
    }
  }
}

function validateUniqueIds(
  issues: ConfigValidationIssue[],
  resource: string,
  values: ReadonlyArray<{ id: string }>,
): void {
  const seen = new Set<string>()
  for (const [index, value] of values.entries()) {
    validateId(issues, `${resource}.${index}.id`, value.id)
    if (seen.has(value.id)) addIssue(issues, `${resource}.${index}.id`, 'ID must be unique.')
    seen.add(value.id)
  }
}

export function validateConfigDocument(
  input: unknown,
  expectedProjectId: string,
  baseline?: PipeBoltApiDtoProjectConfigDocumentV1,
): ConfigValidationIssue[] {
  const structural = parseStructure(input)
  if (!structural.success) {
    return structural.issues
      .slice(0, MAX_ISSUES)
      .map((issue) =>
        'path' in issue && typeof issue.path === 'string'
          ? issue
          : { message: issue.message, path: v.getDotPath(issue) ?? 'config' },
      )
  }

  const config = input as PipeBoltApiDtoProjectConfigDocumentV1
  const issues: ConfigValidationIssue[] = []
  validateId(issues, 'project_id', config.project_id)
  if (config.project_id !== expectedProjectId) {
    addIssue(issues, 'project_id', 'Project ID must match the active route context.')
  }
  validateText(issues, 'name', config.name, 160)
  if (config.description != null) validateText(issues, 'description', config.description, 2048)
  if (!config.enabled) addIssue(issues, 'enabled', 'Runtime candidate requires an enabled project.')

  validateUniqueIds(issues, 'brokers', config.brokers)
  validateUniqueIds(issues, 'routes', config.routes)
  validateUniqueIds(issues, 'schema_mappings', config.schema_mappings)
  validateUniqueIds(issues, 'rules', config.rules)
  validateUniqueIds(issues, 'command_templates', config.command_templates)
  validateUniqueIds(issues, 'sinks', config.sinks)

  const enabledBrokers = new Set(
    config.brokers.filter((broker) => broker.enabled).map((broker) => broker.id),
  )
  if (enabledBrokers.size !== 1) {
    addIssue(issues, 'brokers', 'Runtime candidate requires exactly one enabled broker.')
  }
  const mappingIds = new Set(config.schema_mappings.map((mapping) => mapping.id))

  for (const [index, broker] of config.brokers.entries()) {
    const root = `brokers.${index}`
    validateText(issues, `${root}.name`, broker.name, 160)
    validateText(issues, `${root}.host`, broker.host, 255)
    validateText(issues, `${root}.client_id`, broker.client_id, 160)
    if (broker.port < 1 || broker.port > 65_535)
      addIssue(issues, `${root}.port`, 'Port must be between 1 and 65535.')
    if (broker.keep_alive < 5)
      addIssue(issues, `${root}.keep_alive`, 'Keep alive must be at least 5 seconds.')
    if (broker.reconnect.min_delay < 1)
      addIssue(issues, `${root}.reconnect.min_delay`, 'Minimum reconnect delay must be positive.')
    if (broker.reconnect.max_delay < broker.reconnect.min_delay) {
      addIssue(
        issues,
        `${root}.reconnect.max_delay`,
        'Maximum reconnect delay must not be lower than minimum delay.',
      )
    }
    if (broker.credentials) {
      validateText(issues, `${root}.credentials.username`, broker.credentials.username, 160)
      if (!broker.credentials.password || broker.credentials.password === MASKED_SECRET) {
        addIssue(
          issues,
          `${root}.credentials.password`,
          'Password is required or must retain the existing secret.',
        )
      }
      if (
        baseline &&
        broker.credentials.password === REDACTED_SECRET &&
        !baseline.brokers.find((item) => item.id === broker.id)?.credentials
      ) {
        addIssue(
          issues,
          `${root}.credentials.password`,
          'Redacted password cannot be retained because no existing broker secret matches this ID.',
        )
      }
    }
  }

  const enabledRoutes = config.routes.filter((route) => route.enabled)
  if (!enabledRoutes.length)
    addIssue(issues, 'routes', 'Runtime candidate requires at least one enabled route.')
  for (const [index, route] of config.routes.entries()) {
    const root = `routes.${index}`
    validateText(issues, `${root}.name`, route.name, 160)
    validateText(issues, `${root}.event_type`, route.event_type, 160)
    validateTopicFilter(issues, `${root}.topic_filter`, route)
    if (route.enabled && !enabledBrokers.has(route.broker_id)) {
      addIssue(issues, `${root}.broker_id`, 'Enabled route must reference the enabled broker.')
    }
    if (route.schema_mapping_id && !mappingIds.has(route.schema_mapping_id)) {
      addIssue(issues, `${root}.schema_mapping_id`, 'Referenced schema mapping does not exist.')
    }
    if (route.enabled && route.backpressure !== 'reject') {
      addIssue(
        issues,
        `${root}.backpressure`,
        'Current runtime supports `reject` for enabled routes.',
      )
    }
  }

  for (const [index, mapping] of config.schema_mappings.entries()) {
    validateText(issues, `schema_mappings.${index}.name`, mapping.name, 160)
    for (const [fieldIndex, field] of mapping.fields.entries()) {
      validateText(
        issues,
        `schema_mappings.${index}.fields.${fieldIndex}.target`,
        field.target,
        160,
      )
      validateText(
        issues,
        `schema_mappings.${index}.fields.${fieldIndex}.source`,
        field.source,
        256,
      )
    }
  }

  for (const [index, sink] of config.sinks.entries()) {
    const root = `sinks.${index}`
    validateText(issues, `${root}.name`, sink.name, 160)
    if (sink.kind.type === 'webhook') {
      try {
        const url = new URL(sink.kind.url)
        if (!['http:', 'https:'].includes(url.protocol)) throw new Error('Unsupported protocol')
      } catch {
        addIssue(issues, `${root}.kind.url`, 'Webhook URL must use HTTP or HTTPS.')
      }
      if (sink.kind.timeout < 1)
        addIssue(issues, `${root}.kind.timeout`, 'Timeout must be positive milliseconds.')
      for (const [headerIndex, header] of sink.kind.headers.entries()) {
        validateText(issues, `${root}.kind.headers.${headerIndex}.name`, header.name, 160)
        if (!header.value || header.value === MASKED_SECRET) {
          addIssue(
            issues,
            `${root}.kind.headers.${headerIndex}.value`,
            'Header secret is required or must retain the existing value.',
          )
        }
        if (baseline && header.value === REDACTED_SECRET) {
          const baselineSink = baseline.sinks.find((item) => item.id === sink.id)
          const hasExistingHeader =
            baselineSink?.kind.type === 'webhook' &&
            baselineSink.kind.headers.some(
              (item) => item.name.toLowerCase() === header.name.toLowerCase(),
            )
          if (!hasExistingHeader) {
            addIssue(
              issues,
              `${root}.kind.headers.${headerIndex}.value`,
              'Redacted header cannot be retained because no existing secret matches this sink and header name.',
            )
          }
        }
      }
    } else {
      validateText(issues, `${root}.kind.connection_ref`, sink.kind.connection_ref, 255)
      validateText(issues, `${root}.kind.table`, sink.kind.table, 255)
    }
  }

  for (const [index, template] of config.command_templates.entries()) {
    const root = `command_templates.${index}`
    validateText(issues, `${root}.name`, template.name, 160)
    validateText(issues, `${root}.topic_template`, template.topic_template, 1024)
    if (template.enabled && !enabledBrokers.has(template.broker_id)) {
      addIssue(
        issues,
        `${root}.broker_id`,
        'Enabled command template must reference the enabled broker.',
      )
    }
  }

  return issues
}

export function isConfigDocument(input: unknown): input is PipeBoltApiDtoProjectConfigDocumentV1 {
  return parseStructure(input).success
}
