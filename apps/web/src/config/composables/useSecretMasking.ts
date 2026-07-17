import { shallowRef, watch, type Ref } from 'vue'
import type { PipeBoltApiDtoProjectConfigDocumentV1 } from '@/api/generated'

export const REDACTED_SECRET = '<redacted>'
export const MASKED_SECRET = '••••••••'

function mapSecrets(
  config: PipeBoltApiDtoProjectConfigDocumentV1,
  mapValue: (value: string) => string,
): PipeBoltApiDtoProjectConfigDocumentV1 {
  const clone = structuredClone(config)

  for (const broker of clone.brokers) {
    if (broker.credentials) broker.credentials.password = mapValue(broker.credentials.password)
  }
  for (const sink of clone.sinks) {
    if (sink.kind.type !== 'webhook') continue
    for (const header of sink.kind.headers) header.value = mapValue(header.value)
  }

  return clone
}

export function maskConfigSecrets(
  config: PipeBoltApiDtoProjectConfigDocumentV1,
): PipeBoltApiDtoProjectConfigDocumentV1 {
  return mapSecrets(config, () => MASKED_SECRET)
}

export function restoreConfigSecrets(
  config: PipeBoltApiDtoProjectConfigDocumentV1,
  source?: PipeBoltApiDtoProjectConfigDocumentV1,
): PipeBoltApiDtoProjectConfigDocumentV1 {
  const clone = structuredClone(config)

  for (const broker of clone.brokers) {
    if (!broker.credentials || broker.credentials.password !== MASKED_SECRET) continue
    const sourcePassword = source?.brokers.find((item) => item.id === broker.id)?.credentials
      ?.password
    if (!sourcePassword) {
      throw new Error(`Masked password has no existing secret for broker ${broker.id}.`)
    }
    broker.credentials.password = sourcePassword
  }
  for (const sink of clone.sinks) {
    if (sink.kind.type !== 'webhook') continue
    const sourceSink = source?.sinks.find((item) => item.id === sink.id)
    for (const header of sink.kind.headers) {
      if (header.value !== MASKED_SECRET) continue
      const sourceValue =
        sourceSink?.kind.type === 'webhook'
          ? sourceSink.kind.headers.find(
              (item) => item.name.toLowerCase() === header.name.toLowerCase(),
            )?.value
          : undefined
      if (!sourceValue) {
        throw new Error(
          `Masked header has no existing secret for sink ${sink.id} and header ${header.name}.`,
        )
      }
      header.value = sourceValue
    }
  }

  return clone
}

export function redactConfigSecrets(
  config: PipeBoltApiDtoProjectConfigDocumentV1,
): PipeBoltApiDtoProjectConfigDocumentV1 {
  return mapSecrets(config, () => REDACTED_SECRET)
}

export function useSecretMasking(model: Ref<string>) {
  const displayValue = shallowRef(model.value === REDACTED_SECRET ? MASKED_SECRET : model.value)
  let preservesExistingSecret = model.value === REDACTED_SECRET

  watch(model, (value) => {
    preservesExistingSecret = value === REDACTED_SECRET
    displayValue.value = preservesExistingSecret ? MASKED_SECRET : value
  })

  function focus(event: FocusEvent): void {
    if (preservesExistingSecret) (event.currentTarget as HTMLInputElement | null)?.select()
  }

  function input(event: Event): void {
    const value = (event.currentTarget as HTMLInputElement).value
    displayValue.value = value

    if (value) {
      preservesExistingSecret = false
      model.value = value
    } else if (!preservesExistingSecret) {
      model.value = ''
    }
  }

  function blur(): void {
    if (preservesExistingSecret && !displayValue.value) displayValue.value = MASKED_SECRET
  }

  return { blur, displayValue, focus, input }
}
