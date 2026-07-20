import {
  computed,
  onMounted,
  onUnmounted,
  shallowRef,
  toValue,
  watch,
  type MaybeRefOrGetter,
} from 'vue'
import { createParser } from 'eventsource-parser'

import { ApiError, getApiErrorMessage } from '@/api/errors'
import { useAuthStore } from '@/auth'
import { openRealtimeStream } from '../realtime.api'
import { RealtimeEventBuffer } from '../realtime.buffer'
import type { RealtimeConnectionState, RealtimeEvent, RealtimeFilters } from '../realtime.types'
import { parseRealtimeMessage, RealtimeProtocolError } from '../realtime.validation'

const MAX_SSE_FRAME_CHARS = 1024 * 1024
const INITIAL_RETRY_MS = 1_000
const MAX_RETRY_MS = 30_000
const STALE_CONNECTION_MS = 40_000
const WATCHDOG_INTERVAL_MS = 5_000
const READY_TIMEOUT_MS = 15_000

type DesiredState = 'running' | 'paused' | 'stopped'

interface UseRealtimeStreamOptions {
  filters: MaybeRefOrGetter<RealtimeFilters>
  projectId: MaybeRefOrGetter<string | undefined>
}

function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<boolean> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve(false)
    const timer = window.setTimeout(() => {
      signal.removeEventListener('abort', abort)
      resolve(true)
    }, milliseconds)
    function abort(): void {
      window.clearTimeout(timer)
      resolve(false)
    }
    signal.addEventListener('abort', abort, { once: true })
  })
}

function isTerminalError(error: unknown): boolean {
  return (
    error instanceof RealtimeProtocolError ||
    (error instanceof ApiError &&
      (error.code === 'invalid_stream_content_type' ||
        error.code === 'missing_stream_body' ||
        error.status === 400 ||
        error.status === 401 ||
        error.status === 403 ||
        error.status === 404 ||
        error.status === 422))
  )
}

function streamErrorMessage(error: unknown): string {
  if (error instanceof RealtimeProtocolError) return error.message
  if (error instanceof Error && error.message === 'Realtime stream closed.') return error.message
  return getApiErrorMessage(error)
}

export function useRealtimeStream({ filters, projectId }: UseRealtimeStreamOptions) {
  const auth = useAuthStore()
  const eventBuffer = new RealtimeEventBuffer()
  const events = shallowRef<ReadonlyArray<RealtimeEvent>>([])
  const connectionState = shallowRef<RealtimeConnectionState>('idle')
  const errorMessage = shallowRef<string>()
  const streamNotice = shallowRef<string>()
  const backendSkipped = shallowRef(0)
  const browserDropped = shallowRef(0)
  const reconnectAttempt = shallowRef(0)
  const lagVisible = shallowRef(false)
  const lagMessage = shallowRef<string>()
  const lastEventAt = shallowRef<number>()
  const connectedAt = shallowRef<number>()
  const desiredState = shallowRef<DesiredState>('stopped')

  let sessionController: AbortController | undefined
  let sessionGeneration = 0
  let animationFrame: number | undefined
  let watchdog: number | undefined
  let lastActivityMs = 0

  const isConnected = computed(() => connectionState.value === 'connected')
  const isPaused = computed(() => desiredState.value === 'paused')
  const isStreaming = computed(() => desiredState.value === 'running')

  function flushEvents(): void {
    animationFrame = undefined
    events.value = [...eventBuffer.events]
    lastEventAt.value = Date.now()
  }

  function scheduleFlush(): void {
    if (animationFrame !== undefined) return
    animationFrame = window.requestAnimationFrame(flushEvents)
  }

  function flushScheduledEvents(): void {
    if (animationFrame === undefined) return
    window.cancelAnimationFrame(animationFrame)
    flushEvents()
  }

  function markLag(message: string): void {
    lagMessage.value = message
    lagVisible.value = true
  }

  function pushEvent(event: RealtimeEvent, frameLength: number): void {
    const result = eventBuffer.push(event, frameLength)
    if (result.dropped > 0) {
      browserDropped.value = Math.min(
        Number.MAX_SAFE_INTEGER,
        browserDropped.value + result.dropped,
      )
      markLag('Browser buffer overflowed; oldest events were discarded.')
    }
    if (result.accepted) scheduleFlush()
  }

  function handleMessage(
    data: string,
    eventName: string | undefined,
    expectedProjectId: string,
  ): boolean {
    lastActivityMs = Date.now()
    const message = parseRealtimeMessage(data, eventName, expectedProjectId)
    switch (message.type) {
      case 'ready':
        connectionState.value = 'connected'
        connectedAt.value = Date.now()
        reconnectAttempt.value = 0
        errorMessage.value = undefined
        streamNotice.value = undefined
        return true
      case 'event':
        if (connectionState.value !== 'connected') connectionState.value = 'connected'
        pushEvent(message.data, data.length)
        return true
      case 'lagged':
        backendSkipped.value = Math.min(
          Number.MAX_SAFE_INTEGER,
          backendSkipped.value + message.skipped,
        )
        markLag(`Backend stream skipped ${message.skipped.toLocaleString()} event(s).`)
        return false
      case 'error':
        streamNotice.value = message.message.slice(0, 500)
        return false
      case 'filter_updated':
        return false
    }
  }

  async function consumeResponse(
    response: Response,
    expectedProjectId: string,
    signal: AbortSignal,
    setRetryDelay: (milliseconds: number) => void,
    onConnected: () => void,
    isCurrent: () => boolean,
  ): Promise<void> {
    const reader = response.body!.getReader()
    const decoder = new TextDecoder()
    let eventCharacterCount = 0
    let lineCharacterCount = 0
    let previousWasCarriageReturn = false
    const parser = createParser({
      onComment: () => {
        lastActivityMs = Date.now()
      },
      // EventSource specification ignores unsupported fields and invalid retry hints.
      onError: () => undefined,
      onEvent: (event) => {
        if (signal.aborted || !isCurrent()) return
        if (handleMessage(event.data, event.event, expectedProjectId)) onConnected()
      },
      onRetry: (milliseconds) =>
        setRetryDelay(Math.min(MAX_RETRY_MS, Math.max(INITIAL_RETRY_MS, milliseconds))),
    })

    function lineEnded(): void {
      if (lineCharacterCount === 0) eventCharacterCount = 0
      lineCharacterCount = 0
    }

    function guardFrames(chunk: string): void {
      for (const character of chunk) {
        if (previousWasCarriageReturn) {
          previousWasCarriageReturn = false
          if (character === '\n') continue
        }
        if (character === '\r') {
          lineEnded()
          previousWasCarriageReturn = true
          continue
        }
        if (character === '\n') {
          lineEnded()
          continue
        }
        lineCharacterCount++
        eventCharacterCount++
        if (eventCharacterCount > MAX_SSE_FRAME_CHARS) {
          throw new RealtimeProtocolError('Realtime SSE frame exceeded browser safety limit.')
        }
      }
    }

    try {
      while (!signal.aborted) {
        const { done, value } = await reader.read()
        if (done) break
        if (signal.aborted || !isCurrent()) return
        const text = decoder.decode(value, { stream: true })
        guardFrames(text)
        parser.feed(text)
      }
      const finalText = decoder.decode()
      if (finalText) {
        guardFrames(finalText)
        parser.feed(finalText)
      }
      parser.reset({ consume: true })
    } finally {
      await reader.cancel().catch(() => undefined)
      reader.releaseLock()
    }
  }

  async function runSession(generation: number, controller: AbortController): Promise<void> {
    const signal = controller.signal
    let failureCount = 0
    let retryDelay = INITIAL_RETRY_MS
    let wasConnected = false

    while (
      !signal.aborted &&
      generation === sessionGeneration &&
      desiredState.value === 'running'
    ) {
      const currentProjectId = toValue(projectId)
      const accessToken = auth.accessToken
      if (!currentProjectId || !accessToken) {
        connectionState.value = 'error'
        errorMessage.value = 'Project context and bearer token are required.'
        desiredState.value = 'stopped'
        return
      }

      connectionState.value = failureCount ? 'reconnecting' : 'connecting'
      const attemptController = new AbortController()
      const abortAttempt = () => attemptController.abort(signal.reason)
      signal.addEventListener('abort', abortAttempt, { once: true })
      const readyTimer = window.setTimeout(() => {
        attemptController.abort(new DOMException('Realtime ready event timed out.', 'TimeoutError'))
      }, READY_TIMEOUT_MS)
      try {
        const response = await openRealtimeStream(
          currentProjectId,
          toValue(filters),
          accessToken,
          attemptController.signal,
        )
        lastActivityMs = Date.now()
        await consumeResponse(
          response,
          currentProjectId,
          attemptController.signal,
          (milliseconds) => {
            retryDelay = milliseconds
          },
          () => {
            window.clearTimeout(readyTimer)
            failureCount = 0
            wasConnected = true
          },
          () => generation === sessionGeneration && desiredState.value === 'running',
        )
        if (signal.aborted) return
        throw new Error('Realtime stream closed.')
      } catch (error) {
        if (signal.aborted || generation !== sessionGeneration) return
        if (isTerminalError(error)) {
          connectionState.value = 'error'
          errorMessage.value = streamErrorMessage(error)
          desiredState.value = 'stopped'
          return
        }

        failureCount++
        reconnectAttempt.value = failureCount
        connectionState.value = 'reconnecting'
        errorMessage.value = streamErrorMessage(error)
        if (wasConnected || connectedAt.value !== undefined) {
          markLag('Connection interrupted; events emitted during reconnect cannot be replayed.')
        }
        const delay = Math.min(retryDelay * 2 ** Math.min(failureCount - 1, 10), MAX_RETRY_MS)
        if (!(await abortableDelay(delay, signal))) return
      } finally {
        window.clearTimeout(readyTimer)
        signal.removeEventListener('abort', abortAttempt)
      }
    }
  }

  function startSession(): void {
    sessionController?.abort()
    const controller = new AbortController()
    sessionController = controller
    sessionGeneration++
    const generation = sessionGeneration
    desiredState.value = 'running'
    errorMessage.value = undefined
    streamNotice.value = undefined
    connectedAt.value = undefined
    void runSession(generation, controller).catch((error: unknown) => {
      if (generation !== sessionGeneration || controller.signal.aborted) return
      connectionState.value = 'error'
      errorMessage.value = streamErrorMessage(error)
      desiredState.value = 'stopped'
    })
  }

  function connect(): void {
    if (desiredState.value === 'running') return
    startSession()
  }

  function pause(): void {
    desiredState.value = 'paused'
    sessionGeneration++
    sessionController?.abort()
    flushScheduledEvents()
    connectionState.value = 'paused'
  }

  function resume(): void {
    if (desiredState.value !== 'paused') return
    startSession()
  }

  function disconnect(): void {
    desiredState.value = 'stopped'
    sessionGeneration++
    sessionController?.abort()
    flushScheduledEvents()
    connectionState.value = 'idle'
    reconnectAttempt.value = 0
    errorMessage.value = undefined
    streamNotice.value = undefined
  }

  function clearEvents(): void {
    eventBuffer.clear()
    if (animationFrame !== undefined) window.cancelAnimationFrame(animationFrame)
    animationFrame = undefined
    events.value = []
    backendSkipped.value = 0
    browserDropped.value = 0
    lagVisible.value = false
    lagMessage.value = undefined
    lastEventAt.value = undefined
  }

  function dismissLag(): void {
    lagVisible.value = false
  }

  watch(
    () => [toValue(projectId), JSON.stringify(toValue(filters))] as const,
    ([currentProject, currentFilters], [previousProject, previousFilters]) => {
      if (currentProject === previousProject && currentFilters === previousFilters) return
      clearEvents()
      if (desiredState.value === 'running') startSession()
    },
  )

  onMounted(() => {
    startSession()
    watchdog = window.setInterval(() => {
      if (
        desiredState.value === 'running' &&
        connectionState.value === 'connected' &&
        Date.now() - lastActivityMs > STALE_CONNECTION_MS
      ) {
        markLag('Heartbeat timed out; reconnecting because event continuity is uncertain.')
        startSession()
      }
    }, WATCHDOG_INTERVAL_MS)
  })

  onUnmounted(() => {
    disconnect()
    window.clearInterval(watchdog)
    if (animationFrame !== undefined) window.cancelAnimationFrame(animationFrame)
  })

  return {
    backendSkipped,
    browserDropped,
    clearEvents,
    connect,
    connectedAt,
    connectionState,
    dismissLag,
    disconnect,
    errorMessage,
    events,
    isConnected,
    isPaused,
    isStreaming,
    lagMessage,
    lagVisible,
    lastEventAt,
    pause,
    reconnectAttempt,
    resume,
    streamNotice,
  }
}
