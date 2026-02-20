import { EventEmitter } from 'node:events'
import type { Logger } from 'pino'
import { MaxRetriesError } from './errors.ts'
import type { Serde } from './serde/index.ts'
import { createJsonSerde } from './serde/index.ts'
import type { Storage } from './storage/types.ts'
import type { AfterExecutionContext, AfterExecutionHook, Job, JobHandler, QueueMessage } from './types.ts'
import { abstractLogger, ensureLoggableError } from './utils/logging.ts'

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
  afterExecution?: AfterExecutionHook<TPayload, TResult>
  logger?: Logger
}

interface ConsumerEvents<TResult> {
  error: [error: Error]
  completed: [id: string, result: TResult]
  failed: [id: string, error: Error]
  failing: [id: string, error: Error, attempt: number]
  requeued: [id: string]
}

type ExtendedError = Error & { code?: string; toJSON?: () => Record<string, any> }

const noopAfterExecution = async <TPayload, TResult>(
  _context: AfterExecutionContext<TPayload, TResult>
): Promise<void> => {}

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
  #afterExecution: AfterExecutionHook<TPayload, TResult>
  #logger: Logger

  #handler: JobHandler<TPayload, TResult> | null = null
  #running = false
  #activeJobs = 0
  #abortController: AbortController | null = null
  #workerLoops: Promise<void>[] = []
  #loopsDone: Promise<void> | null = null
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
    this.#afterExecution = config.afterExecution ?? noopAfterExecution
    this.#logger = (config.logger ?? abstractLogger).child({ component: 'consumer', workerId: this.#workerId })
  }

  /**
   * Register a job handler
   */
  execute (handler: JobHandler<TPayload, TResult>): void {
    this.#handler = handler
    this.#logger.debug('Registered consumer handler.')
  }

  /**
   * Start consuming jobs
   */
  async start (): Promise<void> {
    if (this.#running) {
      this.#logger.trace('Consumer already running.')
      return
    }
    if (!this.#handler) {
      throw new Error('No handler registered. Call execute() first.')
    }

    // Ensure any previous worker loops have exited before starting a new run.
    if (this.#loopsDone) {
      await this.#loopsDone
      this.#workerLoops = []
      this.#loopsDone = null
    }

    this.#running = true
    this.#abortController = new AbortController()

    this.#logger.debug({ concurrency: this.#concurrency }, 'Starting consumer.')

    // Register worker
    await this.#storage.registerWorker(this.#workerId, this.#visibilityTimeout * 2)

    // Start worker loops based on concurrency
    const abortSignal = this.#abortController.signal
    this.#workerLoops = []
    for (let i = 0; i < this.#concurrency; i++) {
      const loopPromise = this.#workerLoop(abortSignal).catch(err => {
        const error = err instanceof Error ? err : new Error(String(err))
        this.#logger.error({ err: ensureLoggableError(error) }, 'Worker loop terminated with error.')
        this.emit('error', error)
      })
      this.#workerLoops.push(loopPromise)
    }

    this.#loopsDone = Promise.allSettled(this.#workerLoops).then(() => {})

    this.#logger.debug('Consumer started.')
  }

  /**
   * Stop consuming jobs gracefully
   */
  async stop (): Promise<void> {
    if (!this.#running) {
      this.#logger.trace('Consumer already stopped.')
      return
    }

    this.#logger.debug('Stopping consumer.')
    this.#running = false

    // Signal abort to worker loops
    const abortController = this.#abortController
    this.#abortController = null
    abortController?.abort()

    // Wait for active jobs to complete (with timeout)
    const maxWait = this.#visibilityTimeout
    const startTime = Date.now()

    while (this.#activeJobs > 0 && Date.now() - startTime < maxWait) {
      await new Promise(resolve => setTimeout(resolve, 100))
    }

    if (this.#activeJobs > 0) {
      this.#logger.warn({ activeJobs: this.#activeJobs }, 'Forcing abort of active jobs during stop.')
    }

    // Abort any remaining jobs
    for (const [, controller] of this.#jobAbortControllers) {
      controller.abort()
    }
    this.#jobAbortControllers.clear()

    // Worker loops are awaited on the next start() call.

    // Unregister worker
    await this.#storage.unregisterWorker(this.#workerId)
    this.#logger.debug('Consumer stopped.')
  }

  /**
   * Worker loop that continuously dequeues and processes jobs
   */
  async #workerLoop (abortSignal: AbortSignal): Promise<void> {
    while (this.#running && !abortSignal.aborted) {
      try {
        const message = await this.#storage.dequeue(this.#workerId, this.#blockTimeout)

        if (!message) {
          // Timeout, check if still running
          continue
        }

        this.#logger.trace('Dequeued job message.')

        // Check if aborted
        if (abortSignal.aborted) {
          // Put message back
          const queueMessage = this.#deserializeMessage(message)
          this.#logger.warn({ id: queueMessage.id }, 'Consumer aborted while holding job, requeueing.')
          await this.#storage.requeue(queueMessage.id, message, this.#workerId)
          this.emit('requeued', queueMessage.id)
          break
        }

        // Process the job
        await this.#processJob(message)
      } catch (err) {
        if (!this.#running) break
        const error = err instanceof Error ? err : new Error(String(err))
        this.#logger.error({ err: ensureLoggableError(error) }, 'Worker loop error.')
        this.emit('error', error)
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
    const { id, payload, attempts, maxAttempts, createdAt } = queueMessage
    const resolvedTTL = queueMessage.resultTTL ?? this.#resultTTL

    this.#logger.trace({ id, attempts, maxAttempts, resolvedTTL }, 'Processing job.')

    // Check if job was cancelled (deleted from jobs hash)
    const state = await this.#storage.getJobState(id)
    if (!state) {
      // Job was cancelled, just ack it
      this.#logger.debug({ id }, 'Acknowledging cancelled job.')
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

    const currentAttempts = attempts + 1

    // Update state to processing
    const startedAt = Date.now()
    await this.#storage.setJobState(id, `processing:${startedAt}:${this.#workerId}`)
    await this.#storage.publishEvent(id, 'processing')
    this.#logger.trace({ id, attempt: currentAttempts }, 'Job marked as processing.')

    try {
      const job: Job<TPayload> = {
        id,
        payload,
        attempts: currentAttempts,
        signal: jobAbortController.signal
      }

      const result = await this.#executeHandler(job)

      // Clear visibility timer
      clearTimeout(visibilityTimer)

      const finishedAt = Date.now()
      const context = await this.#runAfterExecution({
        id,
        payload,
        attempts: currentAttempts,
        maxAttempts,
        createdAt,
        status: 'completed',
        result,
        ttl: resolvedTTL,
        workerId: this.#workerId,
        startedAt,
        finishedAt,
        durationMs: finishedAt - startedAt
      })

      const finalResult = context.result as TResult

      // Complete the job
      const serializedResult = this.#resultSerde.serialize(finalResult)
      await this.#storage.completeJob(id, message, this.#workerId, serializedResult, context.ttl)
      this.#logger.debug({ id, ttl: context.ttl }, 'Job completed and persisted.')

      this.emit('completed', id, finalResult)
    } catch (err) {
      // Clear visibility timer
      clearTimeout(visibilityTimer)

      const error = err as ExtendedError
      this.#logger.warn({ id, attempt: currentAttempts, err: ensureLoggableError(error) }, 'Job handler failed.')

      if (currentAttempts < maxAttempts) {
        // Retry - update message with incremented attempts
        const updatedMessage: QueueMessage<TPayload> = {
          ...queueMessage,
          attempts: currentAttempts
        }
        const serializedMessage = this.#payloadSerde.serialize(updatedMessage as unknown as TPayload)

        await this.#storage.retryJob(id, serializedMessage, this.#workerId, currentAttempts)
        this.#logger.warn({ id, attempt: currentAttempts }, 'Job scheduled for retry.')

        this.emit('failing', id, error, currentAttempts)
      } else {
        // Max retries exceeded - fail the job
        const finishedAt = Date.now()
        const context = await this.#runAfterExecution({
          id,
          payload,
          attempts: currentAttempts,
          maxAttempts,
          createdAt,
          status: 'failed',
          error,
          ttl: resolvedTTL,
          workerId: this.#workerId,
          startedAt,
          finishedAt,
          durationMs: finishedAt - startedAt
        })

        const finalError = (context.error as ExtendedError) ?? error
        const maxRetriesError = new MaxRetriesError(id, currentAttempts, finalError)
        const serializedError = Buffer.from(
          JSON.stringify(
            typeof finalError.toJSON === 'function'
              ? finalError.toJSON()
              : {
                  message: finalError.message,
                  code: finalError.code,
                  stack: finalError.stack
                }
          )
        )

        await this.#storage.failJob(id, message, this.#workerId, serializedError, context.ttl)
        this.#logger.error(
          { id, attempts: currentAttempts, ttl: context.ttl, err: ensureLoggableError(maxRetriesError) },
          'Job failed permanently.'
        )

        this.emit('failed', id, maxRetriesError)
      }
    } finally {
      this.#jobAbortControllers.delete(id)
      this.#activeJobs--
      this.#logger.trace({ id, activeJobs: this.#activeJobs }, 'Finished job processing cycle.')
    }
  }

  async #runAfterExecution (
    context: AfterExecutionContext<TPayload, TResult>
  ): Promise<AfterExecutionContext<TPayload, TResult>> {
    const originalTTL = context.ttl

    try {
      await this.#afterExecution(context)
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err))
      this.#logger.error(
        { id: context.id, err: ensureLoggableError(error) },
        'AfterExecution hook failed, restoring original TTL.'
      )
      this.emit('error', error)
      context.ttl = originalTTL
    }

    if (!Number.isFinite(context.ttl) || !Number.isInteger(context.ttl) || context.ttl <= 0) {
      this.#logger.warn(
        { id: context.id, ttl: context.ttl },
        'Invalid TTL from afterExecution, restoring original TTL.'
      )
      this.emit('error', new TypeError('resultTTL must be a positive integer in milliseconds'))
      context.ttl = originalTTL
    }

    return context
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
