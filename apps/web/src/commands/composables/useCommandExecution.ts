import { computed, onScopeDispose, readonly, shallowRef } from 'vue'

import type {
  PipeBoltApiDtoCommandExecutionStatusResponse,
  PipeBoltApiDtoExecuteCommandRequest,
  PipeBoltApiDtoExecuteCommandResponse,
} from '@/api/generated'
import { ApiError, toApiError } from '@/api/errors'
import { executeCommand, fetchCommandStatusObservation } from '../commands.api'
import type { CommandTrackingState } from '../commands.types'

const POLL_INTERVAL_MS = 2_000
const MAX_POLL_ATTEMPTS = 30
const MAX_CONSECUTIVE_FAILURES = 3

export function useCommandExecution(projectId: string, templateId: string) {
  const receipt = shallowRef<PipeBoltApiDtoExecuteCommandResponse>()
  const currentStatus = shallowRef<PipeBoltApiDtoCommandExecutionStatusResponse>()
  const executeError = shallowRef<ApiError>()
  const trackerError = shallowRef<ApiError>()
  const trackingState = shallowRef<CommandTrackingState>('idle')
  const isExecuting = shallowRef(false)
  let executeController: AbortController | undefined
  let pollController: AbortController | undefined
  let pollTimer: number | undefined
  let pollAttempts = 0
  let consecutiveFailures = 0

  const isTerminal = computed(
    () => currentStatus.value === 'published' || currentStatus.value === 'failed',
  )

  function stopTracking(): void {
    window.clearTimeout(pollTimer)
    pollTimer = undefined
    pollController?.abort()
    pollController = undefined
  }

  function schedulePoll(): void {
    stopTracking()
    pollTimer = window.setTimeout(() => void poll(), POLL_INTERVAL_MS)
  }

  async function poll(): Promise<void> {
    const activeReceipt = receipt.value
    if (!activeReceipt || isTerminal.value || trackingState.value !== 'polling') return
    if (pollAttempts >= MAX_POLL_ATTEMPTS) {
      trackingState.value = 'timed_out'
      return
    }

    pollAttempts += 1
    const controller = new AbortController()
    pollController = controller
    try {
      const observation = await fetchCommandStatusObservation(
        projectId,
        activeReceipt.audit_event_id,
        activeReceipt.command_execution_id,
        controller.signal,
      )
      if (receipt.value !== activeReceipt) return
      consecutiveFailures = 0
      trackerError.value = undefined
      if (observation) currentStatus.value = observation.status
      if (isTerminal.value) {
        trackingState.value = 'settled'
      } else {
        schedulePoll()
      }
    } catch (error) {
      if (controller.signal.aborted || receipt.value !== activeReceipt) return
      consecutiveFailures += 1
      trackerError.value = toApiError(error)
      if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
        trackingState.value = 'error'
      } else {
        schedulePoll()
      }
    }
  }

  function startTracking(): void {
    if (!receipt.value || isTerminal.value) {
      trackingState.value = receipt.value ? 'settled' : 'idle'
      return
    }
    pollAttempts = 0
    consecutiveFailures = 0
    trackerError.value = undefined
    trackingState.value = 'polling'
    schedulePoll()
  }

  async function execute(body: PipeBoltApiDtoExecuteCommandRequest): Promise<boolean> {
    if (isExecuting.value) return false
    stopTracking()
    executeController?.abort()
    executeController = new AbortController()
    executeError.value = undefined
    trackerError.value = undefined
    receipt.value = undefined
    currentStatus.value = undefined
    trackingState.value = 'idle'
    isExecuting.value = true
    try {
      const response = await executeCommand(projectId, templateId, body, executeController.signal)
      receipt.value = response
      currentStatus.value = response.status
      startTracking()
      return true
    } catch (error) {
      if (!executeController.signal.aborted) executeError.value = toApiError(error)
      return false
    } finally {
      isExecuting.value = false
    }
  }

  onScopeDispose(() => {
    executeController?.abort()
    stopTracking()
  })

  return {
    currentStatus: readonly(currentStatus),
    execute,
    executeError: readonly(executeError),
    isExecuting: readonly(isExecuting),
    isTerminal,
    receipt: readonly(receipt),
    trackerError: readonly(trackerError),
    trackingState: readonly(trackingState),
  }
}
