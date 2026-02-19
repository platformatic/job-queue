import { EventEmitter } from 'node:events'
import { randomUUID } from 'node:crypto'
import type { Logger } from 'pino'
import type { Storage } from './storage/types.ts'
import type { Serde } from './serde/index.ts'
import type {
  QueueConfig,
  EnqueueOptions,
  EnqueueAndWaitOptions,
  EnqueueResult,
  CancelResult,
  MessageStatus,
  UpdateResultTTLResult,
  JobHandler,
  QueueEvents,
  AfterExecutionHook
} from './types.ts'
import { Producer } from './producer.ts'
import { Consumer } from './consumer.ts'
import { createJsonSerde } from './serde/index.ts'
import { abstractLogger, ensureLoggableError } from './utils/logging.ts'

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
  #resultTTL: number
  #visibilityTimeout: number
  #afterExecution: AfterExecutionHook<TPayload, TResult> | undefined
  #logger: Logger

  constructor (config: QueueConfig<TPayload, TResult>) {
    super()

    this.#storage = config.storage
    this.#workerId = config.workerId ?? randomUUID()
    this.#payloadSerde = config.payloadSerde ?? createJsonSerde<TPayload>()
    this.#resultSerde = config.resultSerde ?? createJsonSerde<TResult>()
    this.#concurrency = config.concurrency ?? 1
    this.#blockTimeout = config.blockTimeout ?? 5
    this.#maxRetries = config.maxRetries ?? 3
    this.#resultTTL = config.resultTTL ?? 3600000
    this.#visibilityTimeout = config.visibilityTimeout ?? 30000
    this.#afterExecution = config.afterExecution
    this.#logger = (config.logger ?? abstractLogger).child({ component: 'queue', workerId: this.#workerId })

    this.#producer = new Producer<TPayload, TResult>({
      storage: this.#storage,
      payloadSerde: this.#payloadSerde,
      resultSerde: this.#resultSerde,
      maxRetries: this.#maxRetries,
      resultTTL: this.#resultTTL,
      logger: this.#logger
    })
  }

  /**
   * Start the queue (connects storage and starts consumer if handler registered)
   */
  async start (): Promise<void> {
    if (this.#started) {
      this.#logger.trace('Queue already started.')
      return
    }

    this.#logger.debug('Starting queue.')
    await this.#storage.connect()
    this.#started = true

    // If handler was registered, start consumer
    if (this.#handler) {
      this.#startConsumer()
    }

    this.#logger.debug('Queue started.')
    this.emit('started')
  }

  /**
   * Stop the queue gracefully
   */
  async stop (): Promise<void> {
    if (!this.#started) {
      this.#logger.trace('Queue already stopped.')
      return
    }

    this.#logger.debug('Stopping queue.')

    if (this.#consumer) {
      await this.#consumer.stop()
    }

    await this.#storage.disconnect()
    this.#started = false

    this.#logger.debug('Queue stopped.')
    this.emit('stopped')
  }

  /**
   * Register a job handler (makes this queue a consumer)
   */
  execute (handler: JobHandler<TPayload, TResult>): void {
    this.#handler = handler
    this.#logger.debug('Registered queue handler.')

    // If already started, create and start consumer
    if (this.#started) {
      this.#startConsumer()
    }
  }

  /**
   * Enqueue a job (fire-and-forget)
   */
  async enqueue (id: string, payload: TPayload, options?: EnqueueOptions): Promise<EnqueueResult<TResult>> {
    this.#logger.trace({ id }, 'Enqueue requested.')
    const result = await this.#producer.enqueue(id, payload, options)
    this.#logger.trace({ id, status: result.status }, 'Enqueue completed.')
    if (result.status === 'queued') {
      this.emit('enqueued', id)
    }
    return result
  }

  /**
   * Enqueue a job and wait for the result
   */
  async enqueueAndWait (id: string, payload: TPayload, options?: EnqueueAndWaitOptions): Promise<TResult> {
    this.#logger.trace({ id }, 'EnqueueAndWait requested.')
    const result = await this.#producer.enqueueAndWait(id, payload, options)
    this.#logger.trace({ id }, 'EnqueueAndWait resolved.')
    return result
  }

  /**
   * Cancel a pending job
   */
  async cancel (id: string): Promise<CancelResult> {
    this.#logger.trace({ id }, 'Cancel requested.')
    const result = await this.#producer.cancel(id)
    this.#logger.trace({ id, status: result.status }, 'Cancel completed.')
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
   * Update TTL for a terminal job payload (result or error).
   */
  async updateResultTTL (id: string, ttlMs: number): Promise<UpdateResultTTLResult> {
    this.#logger.trace({ id, ttlMs }, 'UpdateResultTTL requested.')
    const result = await this.#producer.updateResultTTL(id, ttlMs)
    this.#logger.trace({ id, status: result.status }, 'UpdateResultTTL completed.')
    return result
  }

  /**
   * Get the status of a job
   */
  async getStatus (id: string): Promise<MessageStatus<TResult> | null> {
    return this.#producer.getStatus(id)
  }

  #startConsumer (): void {
    if (this.#consumer || !this.#handler) return

    this.#logger.debug({ concurrency: this.#concurrency }, 'Starting consumer.')

    this.#consumer = new Consumer<TPayload, TResult>({
      storage: this.#storage,
      workerId: this.#workerId,
      payloadSerde: this.#payloadSerde,
      resultSerde: this.#resultSerde,
      concurrency: this.#concurrency,
      blockTimeout: this.#blockTimeout,
      maxRetries: this.#maxRetries,
      resultTTL: this.#resultTTL,
      visibilityTimeout: this.#visibilityTimeout,
      afterExecution: this.#afterExecution,
      logger: this.#logger
    })

    // Forward consumer events
    this.#consumer.on('error', error => {
      this.#logger.error({ err: ensureLoggableError(error) }, 'Consumer emitted error.')
      this.emit('error', error)
    })

    this.#consumer.on('completed', (id, result) => {
      this.#logger.debug({ id }, 'Job completed.')
      this.emit('completed', id, result)
    })

    this.#consumer.on('failed', (id, error) => {
      this.#logger.warn({ id, err: ensureLoggableError(error) }, 'Job failed.')
      this.emit('failed', id, error)
    })

    this.#consumer.on('failing', (id, error, attempt) => {
      this.#logger.warn({ id, attempt, err: ensureLoggableError(error) }, 'Job failing and retrying.')
      this.emit('failing', id, error, attempt)
    })

    this.#consumer.on('requeued', id => {
      this.#logger.debug({ id }, 'Job requeued.')
      this.emit('requeued', id)
    })

    this.#consumer.execute(this.#handler)
    this.#consumer.start().catch(err => {
      const error = err instanceof Error ? err : new Error(String(err))
      this.#logger.error({ err: ensureLoggableError(error) }, 'Failed to start consumer.')
      this.emit('error', error)
    })
  }
}
