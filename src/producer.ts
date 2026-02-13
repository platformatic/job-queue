import type { Storage } from './storage/types.ts'
import type { Serde } from './serde/index.ts'
import type {
  QueueMessage,
  EnqueueOptions,
  EnqueueAndWaitOptions,
  EnqueueResult,
  CancelResult,
  MessageStatus,
  MessageState
} from './types.ts'
import { TimeoutError, JobFailedError } from './errors.ts'
import { createJsonSerde } from './serde/index.ts'

interface ProducerConfig<TPayload, TResult> {
  storage: Storage
  payloadSerde?: Serde<TPayload>
  resultSerde?: Serde<TResult>
  maxRetries?: number
  resultTTL?: number
}

/**
 * Parse job state string into components
 */
function parseState (state: string): { status: MessageState, timestamp: number, extra?: string } {
  const parts = state.split(':')
  return {
    status: parts[0] as MessageState,
    timestamp: parseInt(parts[1], 10),
    extra: parts[2]
  }
}

/**
 * Producer handles enqueueing jobs and retrieving results
 */
export class Producer<TPayload, TResult> {
  #storage: Storage
  #payloadSerde: Serde<TPayload>
  #resultSerde: Serde<TResult>
  #maxRetries: number
  #resultTTL: number

  constructor (config: ProducerConfig<TPayload, TResult>) {
    this.#storage = config.storage
    this.#payloadSerde = config.payloadSerde ?? createJsonSerde<TPayload>()
    this.#resultSerde = config.resultSerde ?? createJsonSerde<TResult>()
    this.#maxRetries = config.maxRetries ?? 3
    this.#resultTTL = config.resultTTL ?? 3600000 // 1 hour
  }

  /**
   * Enqueue a job (fire-and-forget)
   */
  async enqueue (
    id: string,
    payload: TPayload,
    options?: EnqueueOptions
  ): Promise<EnqueueResult<TResult>> {
    const timestamp = Date.now()
    const maxAttempts = options?.maxAttempts ?? this.#maxRetries

    const message: QueueMessage<TPayload> = {
      id,
      payload,
      createdAt: timestamp,
      attempts: 0,
      maxAttempts
    }

    const serialized = this.#payloadSerde.serialize(message as unknown as TPayload)
    const existingState = await this.#storage.enqueue(id, serialized, timestamp)

    if (existingState) {
      const { status } = parseState(existingState)

      if (status === 'completed') {
        const result = await this.getResult(id)
        if (result !== null) {
          return { status: 'completed', result }
        }
      }

      return { status: 'duplicate', existingState: status }
    }

    return { status: 'queued' }
  }

  /**
   * Enqueue a job and wait for the result
   */
  async enqueueAndWait (
    id: string,
    payload: TPayload,
    options?: EnqueueAndWaitOptions
  ): Promise<TResult> {
    const timeout = options?.timeout ?? 30000

    // Subscribe BEFORE enqueue to avoid race conditions
    let unsubscribe: (() => Promise<void>) | null = null
    let resolvePromise: ((result: TResult) => void) | null = null
    let rejectPromise: ((error: Error) => void) | null = null

    const resultPromise = new Promise<TResult>((resolve, reject) => {
      resolvePromise = resolve
      rejectPromise = reject
    })

    unsubscribe = await this.#storage.subscribeToJob(id, async (status) => {
      if (status === 'completed') {
        const result = await this.getResult(id)
        if (result !== null && resolvePromise) {
          resolvePromise(result)
        }
      } else if (status === 'failed') {
        const error = await this.#storage.getError(id)
        const errorMessage = error ? error.toString() : 'Job failed'
        if (rejectPromise) {
          rejectPromise(new JobFailedError(id, errorMessage))
        }
      }
    })

    try {
      // Now enqueue
      const enqueueResult = await this.enqueue(id, payload, options)

      if (enqueueResult.status === 'completed') {
        return enqueueResult.result
      }

      // If duplicate, check if already completed
      if (enqueueResult.status === 'duplicate') {
        if (enqueueResult.existingState === 'completed') {
          const result = await this.getResult(id)
          if (result !== null) {
            return result
          }
        } else if (enqueueResult.existingState === 'failed') {
          const error = await this.#storage.getError(id)
          const errorMessage = error ? error.toString() : 'Job failed'
          throw new JobFailedError(id, errorMessage)
        }
        // Otherwise wait for completion
      }

      // Wait for result with timeout
      const timeoutPromise = new Promise<never>((_resolve, reject) => {
        setTimeout(() => {
          reject(new TimeoutError(id, timeout))
        }, timeout)
      })

      return await Promise.race([resultPromise, timeoutPromise])
    } finally {
      if (unsubscribe) {
        await unsubscribe()
      }
    }
  }

  /**
   * Cancel a pending job
   */
  async cancel (id: string): Promise<CancelResult> {
    const state = await this.#storage.getJobState(id)

    if (!state) {
      return { status: 'not_found' }
    }

    const { status } = parseState(state)

    if (status === 'completed') {
      return { status: 'completed' }
    }

    if (status === 'processing') {
      return { status: 'processing' }
    }

    // Can cancel if queued or failing
    const deleted = await this.#storage.deleteJob(id)
    if (deleted) {
      return { status: 'cancelled' }
    }

    return { status: 'not_found' }
  }

  /**
   * Get the result of a completed job
   */
  async getResult (id: string): Promise<TResult | null> {
    const resultBuffer = await this.#storage.getResult(id)
    if (!resultBuffer) {
      return null
    }
    return this.#resultSerde.deserialize(resultBuffer)
  }

  /**
   * Get the status of a job
   */
  async getStatus (id: string): Promise<MessageStatus<TResult> | null> {
    const state = await this.#storage.getJobState(id)
    if (!state) {
      return null
    }

    const { status, timestamp } = parseState(state)

    const messageStatus: MessageStatus<TResult> = {
      id,
      state: status,
      createdAt: timestamp,
      attempts: 0
    }

    if (status === 'completed') {
      const result = await this.getResult(id)
      if (result !== null) {
        messageStatus.result = result
      }
    } else if (status === 'failed') {
      const error = await this.#storage.getError(id)
      if (error) {
        messageStatus.error = error.toString()
      }
    }

    return messageStatus
  }
}
