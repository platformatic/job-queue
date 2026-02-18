import { EventEmitter } from 'node:events'
import { randomUUID } from 'node:crypto'
import type { Storage } from './storage/types.ts'
import type { Serde } from './serde/index.ts'
import type {
  QueueConfig,
  EnqueueOptions,
  EnqueueAndWaitOptions,
  EnqueueResult,
  CancelResult,
  MessageStatus,
  JobHandler,
  QueueEvents
} from './types.ts'
import { Producer } from './producer.ts'
import { Consumer } from './consumer.ts'
import { createJsonSerde } from './serde/index.ts'

/**
 * Queue class combining Producer and Consumer functionality
 */
export class Queue<TPayload, TResult = void> extends EventEmitter<QueueEvents<TResult>> {
  #storage: Storage
  #producer: Producer<TPayload, TResult>
  #consumer: Consumer<TPayload, TResult> | null = null
  #workerId: string
  #handler: JobHandler<TPayload, TResult> | null = null
  #started = false

  #payloadSerde: Serde<TPayload>
  #resultSerde: Serde<TResult>
  #concurrency: number
  #blockTimeout: number
  #maxRetries: number
  #visibilityTimeout: number

  constructor (config: QueueConfig<TPayload, TResult>) {
    super()

    this.#storage = config.storage
    this.#workerId = config.workerId ?? randomUUID()
    this.#payloadSerde = config.payloadSerde ?? createJsonSerde<TPayload>()
    this.#resultSerde = config.resultSerde ?? createJsonSerde<TResult>()
    this.#concurrency = config.concurrency ?? 1
    this.#blockTimeout = config.blockTimeout ?? 5
    this.#maxRetries = config.maxRetries ?? 3
    this.#visibilityTimeout = config.visibilityTimeout ?? 30000

    this.#producer = new Producer<TPayload, TResult>({
      storage: this.#storage,
      payloadSerde: this.#payloadSerde,
      resultSerde: this.#resultSerde,
      maxRetries: this.#maxRetries
    })
  }

  /**
   * Start the queue (connects storage and starts consumer if handler registered)
   */
  async start (): Promise<void> {
    if (this.#started) return

    await this.#storage.connect()
    this.#started = true

    // If handler was registered, start consumer
    if (this.#handler) {
      this.#startConsumer()
    }

    this.emit('started')
  }

  /**
   * Stop the queue gracefully
   */
  async stop (): Promise<void> {
    if (!this.#started) return

    if (this.#consumer) {
      await this.#consumer.stop()
    }

    await this.#storage.disconnect()
    this.#started = false

    this.emit('stopped')
  }

  /**
   * Register a job handler (makes this queue a consumer)
   */
  execute (handler: JobHandler<TPayload, TResult>): void {
    this.#handler = handler

    // If already started, create and start consumer
    if (this.#started) {
      this.#startConsumer()
    }
  }

  /**
   * Enqueue a job (fire-and-forget)
   */
  async enqueue (
    id: string,
    payload: TPayload,
    options?: EnqueueOptions
  ): Promise<EnqueueResult<TResult>> {
    const result = await this.#producer.enqueue(id, payload, options)
    if (result.status === 'queued') {
      this.emit('enqueued', id)
    }
    return result
  }

  /**
   * Enqueue a job and wait for the result
   */
  async enqueueAndWait (
    id: string,
    payload: TPayload,
    options?: EnqueueAndWaitOptions
  ): Promise<TResult> {
    return this.#producer.enqueueAndWait(id, payload, options)
  }

  /**
   * Cancel a pending job
   */
  async cancel (id: string): Promise<CancelResult> {
    const result = await this.#producer.cancel(id)
    if (result.status === 'cancelled') {
      this.emit('cancelled', id)
    }
    return result
  }

  /**
   * Get the result of a completed job
   */
  async getResult (id: string): Promise<TResult | null> {
    return this.#producer.getResult(id)
  }

  /**
   * Get the status of a job
   */
  async getStatus (id: string): Promise<MessageStatus<TResult> | null> {
    return this.#producer.getStatus(id)
  }

  #startConsumer (): void {
    if (this.#consumer || !this.#handler) return

    this.#consumer = new Consumer<TPayload, TResult>({
      storage: this.#storage,
      workerId: this.#workerId,
      payloadSerde: this.#payloadSerde,
      resultSerde: this.#resultSerde,
      concurrency: this.#concurrency,
      blockTimeout: this.#blockTimeout,
      maxRetries: this.#maxRetries,
      visibilityTimeout: this.#visibilityTimeout
    })

    // Forward consumer events
    this.#consumer.on('error', (error) => {
      this.emit('error', error)
    })

    this.#consumer.on('completed', (id, result) => {
      this.emit('completed', id, result)
    })

    this.#consumer.on('failed', (id, error) => {
      this.emit('failed', id, error)
    })

    this.#consumer.on('failing', (id, error, attempt) => {
      this.emit('failing', id, error, attempt)
    })

    this.#consumer.on('requeued', (id) => {
      this.emit('requeued', id)
    })

    this.#consumer.execute(this.#handler)
    this.#consumer.start().catch((err) => {
      this.emit('error', err)
    })
  }
}
