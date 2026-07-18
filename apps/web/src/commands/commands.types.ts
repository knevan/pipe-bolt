import type {
  PipeBoltApiDtoCommandExecutionStatusResponse,
  PipeBoltDomainConfigCommandTemplate,
} from '@/api/generated'

export interface CommandBrokerSummary {
  id: string
  name: string
}

export interface CommandCatalog {
  brokers: ReadonlyArray<CommandBrokerSummary>
  templates: ReadonlyArray<PipeBoltDomainConfigCommandTemplate>
  version: number
}

export type CommandParameterKind = 'text' | 'number' | 'boolean'

export interface CommandParameterDefinition {
  name: string
  payload: boolean
  topic: boolean
}

export interface CommandParameterExtraction {
  error?: string
  parameters: ReadonlyArray<CommandParameterDefinition>
}

export type CommandTrackingState = 'idle' | 'polling' | 'settled' | 'timed_out' | 'error'

export interface CommandStatusObservation {
  status: PipeBoltApiDtoCommandExecutionStatusResponse
}
