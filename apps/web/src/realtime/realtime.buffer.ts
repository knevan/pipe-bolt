import type { RealtimeEvent } from './realtime.types'

export const REALTIME_EVENT_LIMIT = 200
export const REALTIME_BUFFER_WEIGHT_LIMIT = 16 * 1024 * 1024

export interface RealtimeBufferPushResult {
  accepted: boolean
  dropped: number
}

export interface RealtimeEventBufferOptions {
  maxEvents?: number
  maxWeight?: number
}

export class RealtimeEventBuffer {
  readonly #events: RealtimeEvent[] = []
  readonly #weights: number[] = []
  readonly #maxEvents: number
  readonly #maxWeight: number
  #weight = 0

  constructor(options: RealtimeEventBufferOptions = {}) {
    this.#maxEvents = options.maxEvents ?? REALTIME_EVENT_LIMIT
    this.#maxWeight = options.maxWeight ?? REALTIME_BUFFER_WEIGHT_LIMIT
    if (!Number.isSafeInteger(this.#maxEvents) || this.#maxEvents < 1) {
      throw new RangeError('Realtime event limit must be a positive safe integer.')
    }
    if (!Number.isSafeInteger(this.#maxWeight) || this.#maxWeight < 1) {
      throw new RangeError('Realtime buffer weight limit must be a positive safe integer.')
    }
  }

  get events(): ReadonlyArray<RealtimeEvent> {
    return this.#events
  }

  get weight(): number {
    return this.#weight
  }

  push(event: RealtimeEvent, frameLength: number): RealtimeBufferPushResult {
    const weight = Math.max(1, frameLength * 2)
    if (!Number.isSafeInteger(weight) || weight > this.#maxWeight) {
      return { accepted: false, dropped: 1 }
    }

    let removedWeight = 0
    let removeCount = 0
    while (
      this.#events.length - removeCount >= this.#maxEvents ||
      this.#weight - removedWeight + weight > this.#maxWeight
    ) {
      removedWeight += this.#weights[removeCount] ?? 0
      removeCount += 1
    }
    if (removeCount > 0) {
      this.#events.splice(0, removeCount)
      this.#weights.splice(0, removeCount)
      this.#weight -= removedWeight
    }
    this.#events.push(event)
    this.#weights.push(weight)
    this.#weight += weight
    return { accepted: true, dropped: removeCount }
  }

  clear(): void {
    this.#events.length = 0
    this.#weights.length = 0
    this.#weight = 0
  }
}
