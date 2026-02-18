import { EventEmitter } from 'node:events'
import type { Storage } from './storage/types.ts'
import type { Serde } from './serde/index.ts'
import type { QueueMessage, Job, JobHandler } from './types.ts'
import { MaxRetriesError } from './errors.ts'
import { createJsonSerde } from './serde/index.ts'

interface ConsumerConfig<TPayload, TResult> {
  storage: Storage
  workerId: string
  payloadSerde?: Serde<TPayload>
  resultSerde?: Serde<TResult>
  concurrency?: number
  blockTimeout?: number
  maxRetries?: number
  resultTTL?: number
  visibilityTimeout?: number
}

interface ConsumerEvents<TResult> {
  error: [error: Error]
  completed: [id: string, result: TResult]
  failed: [id: string, error: Error]
  failing: [id: string, error: Error, attempt: number]
  requeued: [id: string]
}

type ExtendedError = Error & { code?: string; toJSON?: () => Record<string, any> }

/**
 * Consumer handles processing jobs from the queue
 */
export class Consumer<TPayload, TResult> extends EventEmitter<ConsumerEvents<TResult>> {
  #storage: Storage
  #workerId: string
  #payloadSerde: Serde<TPayload>
  #resultSerde: Serde<TResult>
  #concurrency: number
  #blockTimeout: number
  #maxRetries: number
  #resultTTL: number
  #visibilityTimeout: number

  #handler: JobHandler<TPayload, TResult> | null = null
  #running = false
  #activeJobs = 0
  #abortController: AbortController | null = null
  #jobAbortControllers: Map<string, AbortController> = new Map()

  constructor (config: ConsumerConfig<TPayload, TResult>) {
    super()
    this.#storage = config.storage
    this.#workerId = config.workerId
    this.#payloadSerde = config.payloadSerde ?? createJsonSerde<TPayload>()
    this.#resultSerde = config.resultSerde ?? createJsonSerde<TResult>()
    this.#concurrency = config.concurrency ?? 1
    this.#blockTimeout = config.blockTimeout ?? 5
    this.#maxRetries = config.maxRetries ?? 3
    this.#resultTTL = config.resultTTL ?? 3600000
    this.#visibilityTimeout = config.visibilityTimeout ?? 30000
  }

  /**
   * Register a job handler
   */
  execute (handler: JobHandler<TPayload, TResult>): void {
    this.#handler = handler
  }

  /**
   * Start consuming jobs
   */
  async start (): Promise<void> {
    if (this.#running) return
    if (!this.#handler) {
      throw new Error('No handler registered. Call execute() first.')
    }

    this.#running = true
    this.#abortController = new AbortController()

    // Register worker
    await this.#storage.registerWorker(this.#workerId, this.#visibilityTimeout * 2)

    // Start worker loops based on concurrency
    const loops: Promise<void>[] = []
    for (let i = 0; i < this.#concurrency; i++) {
      loops.push(this.#workerLoop())
    }

    // Don't await - let them run in background
    Promise.all(loops).catch(err => {
      this.emit('error', err)
    })
  }

  /**
   * Stop consuming jobs gracefully
   */
  async stop (): Promise<void> {
    if (!this.#running) return

    this.#running = false

    // Signal abort to worker loops
    if (this.#abortController) {
      this.#abortController.abort()
    }

    // Wait for active jobs to complete (with timeout)
    const maxWait = this.#visibilityTimeout
    const startTime = Date.now()

    while (this.#activeJobs > 0 && Date.now() - startTime < maxWait) {
      await new Promise(resolve => setTimeout(resolve, 100))
    }

    // Abort any remaining jobs
    for (const [, controller] of this.#jobAbortControllers) {
      controller.abort()
    }
    this.#jobAbortControllers.clear()

    // Unregister worker
    await this.#storage.unregisterWorker(this.#workerId)
  }

  /**
   * Worker loop that continuously dequeues and processes jobs
   */
  async #workerLoop (): Promise<void> {
    while (this.#running) {
      try {
        const message = await this.#storage.dequeue(this.#workerId, this.#blockTimeout)

        if (!message) {
          // Timeout, check if still running
          continue
        }

        // Check if aborted
        if (this.#abortController?.signal.aborted) {
          // Put message back
          const queueMessage = this.#deserializeMessage(message)
          await this.#storage.requeue(queueMessage.id, message, this.#workerId)
          this.emit('requeued', queueMessage.id)
          break
        }

        // Process the job
        await this.#processJob(message)
      } catch (err) {
        if (!this.#running) break
        this.emit('error', err as Error)
        // Brief pause before retrying loop
        await new Promise(resolve => setTimeout(resolve, 1000))
      }
    }
  }

  /**
   * Process a single job
   */
  async #processJob (message: Buffer): Promise<void> {
    const queueMessage = this.#deserializeMessage(message)
    const { id, payload, attempts, maxAttempts } = queueMessage
    const resultTTL = queueMessage.resultTTL ?? this.#resultTTL

    // Check if job was cancelled (deleted from jobs hash)
    const state = await this.#storage.getJobState(id)
    if (!state) {
      // Job was cancelled, just ack it
      await this.#storage.ack(id, message, this.#workerId)
      return
    }

    this.#activeJobs++

    // Create abort controller for this job
    const jobAbortController = new AbortController()
    this.#jobAbortControllers.set(id, jobAbortController)

    // Set up visibility timeout
    const visibilityTimer = setTimeout(() => {
      jobAbortController.abort()
    }, this.#visibilityTimeout)

    // Update state to processing
    const timestamp = Date.now()
    await this.#storage.setJobState(id, `processing:${timestamp}:${this.#workerId}`)
    await this.#storage.publishEvent(id, 'processing')

    try {
      const job: Job<TPayload> = {
        id,
        payload,
        attempts: attempts + 1,
        signal: jobAbortController.signal
      }

      const result = await this.#executeHandler(job)

      // Clear visibility timer
      clearTimeout(visibilityTimer)

      // Complete the job
      const serializedResult = this.#resultSerde.serialize(result)
      await this.#storage.completeJob(id, message, this.#workerId, serializedResult, resultTTL)

      this.emit('completed', id, result)
    } catch (err) {
      // Clear visibility timer
      clearTimeout(visibilityTimer)

      const error = err as ExtendedError
      const currentAttempts = attempts + 1

      if (currentAttempts < maxAttempts) {
        // Retry - update message with incremented attempts
        const updatedMessage: QueueMessage<TPayload> = {
          ...queueMessage,
          attempts: currentAttempts
        }
        const serializedMessage = this.#payloadSerde.serialize(updatedMessage as unknown as TPayload)

        await this.#storage.retryJob(id, serializedMessage, this.#workerId, currentAttempts)

        this.emit('failing', id, error, currentAttempts)
      } else {
        // Max retries exceeded - fail the job
        const maxRetriesError = new MaxRetriesError(id, currentAttempts, error)
        const serializedError = Buffer.from(
          JSON.stringify(
            typeof error.toJSON === 'function'
              ? error.toJSON()
              : {
                  message: error.message,
                  code: error.code,
                  stack: error.stack
                }
          )
        )

        await this.#storage.failJob(id, message, this.#workerId, serializedError, resultTTL)

        this.emit('failed', id, maxRetriesError)
      }
    } finally {
      this.#jobAbortControllers.delete(id)
      this.#activeJobs--
    }
  }

  /**
   * Execute the job handler (supports both async and callback styles)
   */
  async #executeHandler (job: Job<TPayload>): Promise<TResult> {
    const handler = this.#handler!

    // Check if callback style (handler.length > 1)
    if (handler.length > 1) {
      return new Promise<TResult>((resolve, reject) => {
        ;(handler as (job: Job<TPayload>, callback: (err: Error | null, result?: TResult) => void) => void)(job, (
          err,
          result
        ) => {
          if (err) reject(err)
          else resolve(result as TResult)
        })
      })
    }

    // Async style
    return (handler as (job: Job<TPayload>) => Promise<TResult>)(job)
  }

  #deserializeMessage (message: Buffer): QueueMessage<TPayload> {
    return this.#payloadSerde.deserialize(message) as unknown as QueueMessage<TPayload>
  }
}
