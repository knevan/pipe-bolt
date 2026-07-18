import type { PipeBoltDomainConfigCommandTemplate } from '@/api/generated'
import type { CommandParameterDefinition, CommandParameterExtraction } from './commands.types'

const MAX_PARAMETER_COUNT = 128
const MAX_PARAMETER_NAME_BYTES = 256
const MAX_PAYLOAD_NODES = 10_000
const textEncoder = new TextEncoder()

function addParameter(
  name: string,
  location: 'payload' | 'topic',
  parameters: Map<string, CommandParameterDefinition>,
): string | undefined {
  if (!name.trim()) return `Template contains an empty ${location} placeholder.`
  if (textEncoder.encode(name).byteLength > MAX_PARAMETER_NAME_BYTES) {
    return `Placeholder name exceeds ${MAX_PARAMETER_NAME_BYTES} UTF-8 bytes.`
  }

  const existing = parameters.get(name)
  if (existing) {
    existing[location] = true
  } else {
    if (parameters.size >= MAX_PARAMETER_COUNT) {
      return `Template exceeds the UI limit of ${MAX_PARAMETER_COUNT} parameters.`
    }
    parameters.set(name, {
      name,
      payload: location === 'payload',
      topic: location === 'topic',
    })
  }
  return undefined
}

function collectFromString(
  text: string,
  location: 'payload' | 'topic',
  parameters: Map<string, CommandParameterDefinition>,
): string | undefined {
  let rest = text
  while (true) {
    const start = rest.indexOf('{')
    if (start < 0) break
    if (rest.slice(0, start).includes('}')) {
      return `Template contains an unopened ${location} placeholder.`
    }
    const afterStart = rest.slice(start + 1)
    const end = afterStart.indexOf('}')
    if (end < 0) return `Template contains an unclosed ${location} placeholder.`

    const name = afterStart.slice(0, end)
    const parameterError = addParameter(name, location, parameters)
    if (parameterError) return parameterError
    rest = afterStart.slice(end + 1)
  }

  if (rest.includes('}')) return `Template contains an unopened ${location} placeholder.`
  return undefined
}

function collectFromPayloadString(
  text: string,
  parameters: Map<string, CommandParameterDefinition>,
): string | undefined {
  if (text.startsWith('{') && text.endsWith('}')) {
    return addParameter(text.slice(1, -1), 'payload', parameters)
  }
  return collectFromString(text, 'payload', parameters)
}

export function extractCommandParameters(
  template: PipeBoltDomainConfigCommandTemplate,
): CommandParameterExtraction {
  const parameters = new Map<string, CommandParameterDefinition>()
  const topicError = collectFromString(template.topic_template, 'topic', parameters)
  if (topicError) return { error: topicError, parameters: [] }

  const stack: unknown[] = [template.payload_template]
  let visitedNodes = 0
  while (stack.length > 0) {
    const value = stack.pop()
    visitedNodes += 1
    if (visitedNodes > MAX_PAYLOAD_NODES) {
      return {
        error: `Payload template exceeds the UI traversal limit of ${MAX_PAYLOAD_NODES} nodes.`,
        parameters: [],
      }
    }

    if (typeof value === 'string') {
      const payloadError = collectFromPayloadString(value, parameters)
      if (payloadError) return { error: payloadError, parameters: [] }
    } else if (Array.isArray(value)) {
      if (visitedNodes + stack.length + value.length > MAX_PAYLOAD_NODES) {
        return {
          error: `Payload template exceeds the UI traversal limit of ${MAX_PAYLOAD_NODES} nodes.`,
          parameters: [],
        }
      }
      for (const item of value) stack.push(item)
    } else if (typeof value === 'object' && value !== null) {
      const values = Object.values(value)
      if (visitedNodes + stack.length + values.length > MAX_PAYLOAD_NODES) {
        return {
          error: `Payload template exceeds the UI traversal limit of ${MAX_PAYLOAD_NODES} nodes.`,
          parameters: [],
        }
      }
      for (const item of values) stack.push(item)
    }
  }

  return { parameters: [...parameters.values()] }
}
