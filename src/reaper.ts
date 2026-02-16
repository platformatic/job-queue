import { EventEmitter } from 'node:events'
import type { Storage } from './storage/types.ts'
import type { Serde } from './serde/index.ts'
import type { QueueMessage } from './types.ts'
import { createJsonSerde } from './serde/index.ts'
import { parseState } from './utils/state.ts'

interface ReaperConfig<TPayload> {
  storage: Storage
  payloadSerde?: Serde<TPayload>
  visibilityTimeout?: number
  checkInterval?: number
}

interface ReaperEvents {
  error: [error: Error]
  stalled: [id: string]
}

/**
 * Reaper monitors for stalled jobs and requeues them.
 *
 * A job is considered stalled if it has been in "processing" state
 * longer than the visibility timeout.
 */
export class Reaper<TPayload> extends EventEmitter<ReaperEvents> {
  #storage: Storage
  #payloadSerde: Serde<TPayload>
  #visibilityTimeout: number
  #checkInterval: number

  #running = false
  #unsubscribe: (() => Promise<void>) | null = null
  #checkIntervalTimer: ReturnType<typeof setInterval> | null = null
  #processingTimers: Map<string, ReturnType<typeof setTimeout>> = new Map()

  constructor (config: ReaperConfig<TPayload>) {
    super()
    this.#storage = config.storage
    this.#payloadSerde = config.payloadSerde ?? createJsonSerde<TPayload>()
    this.#visibilityTimeout = config.visibilityTimeout ?? 30000
    this.#checkInterval = config.checkInterval ?? 60000
  }

  /**
   * Start the reaper
   */
  async start (): Promise<void> {
    if (this.#running) return

    this.#running = true

    // Subscribe to job events
    this.#unsubscribe = await this.#storage.subscribeToEvents((id, event) => {
      this.#handleEvent(id, event)
    })

    // Do an initial check for stalled jobs
    await this.#checkStalledJobs()

    // Start periodic check interval
    this.#checkIntervalTimer = setInterval(() => {
      this.#checkStalledJobs().catch((err) => {
        this.emit('error', err)
      })
    }, this.#checkInterval)
  }

  /**
   * Stop the reaper gracefully
   */
  async stop (): Promise<void> {
    if (!this.#running) return

    this.#running = false

    // Clear check interval
    if (this.#checkIntervalTimer) {
      clearInterval(this.#checkIntervalTimer)
      this.#checkIntervalTimer = null
    }

    // Clear all processing timers
    for (const timer of this.#processingTimers.values()) {
      clearTimeout(timer)
    }
    this.#processingTimers.clear()

    // Unsubscribe from events
    if (this.#unsubscribe) {
      await this.#unsubscribe()
      this.#unsubscribe = null
    }
  }

  /**
   * Handle a job state change event
   */
  #handleEvent (id: string, event: string): void {
    if (event === 'processing') {
      // Start a timer for this job
      this.#startTimer(id)
    } else if (event === 'completed' || event === 'failed' || event === 'cancelled') {
      // Cancel any existing timer
      this.#cancelTimer(id)
    }
  }

  /**
   * Start a visibility timeout timer for a job
   */
  #startTimer (id: string): void {
    // Cancel any existing timer first
    this.#cancelTimer(id)

    const timer = setTimeout(() => {
      this.#processingTimers.delete(id)
      this.#checkJob(id).catch((err) => {
        this.emit('error', err)
      })
    }, this.#visibilityTimeout)

    this.#processingTimers.set(id, timer)
  }

  /**
   * Cancel the timer for a job
   */
  #cancelTimer (id: string): void {
    const timer = this.#processingTimers.get(id)
    if (timer) {
      clearTimeout(timer)
      this.#processingTimers.delete(id)
    }
  }

  /**
   * Check if a job is stalled and requeue if necessary
   */
  async #checkJob (id: string): Promise<void> {
    if (!this.#running) return

    const state = await this.#storage.getJobState(id)
    if (!state) return

    const { status, timestamp, workerId } = parseState(state)

    if (status !== 'processing') {
      // Job is no longer processing, nothing to do
      return
    }

    // Check if visibility timeout has elapsed
    const elapsed = Date.now() - timestamp
    if (elapsed < this.#visibilityTimeout) {
      // Not yet stalled, restart timer for remaining time
      const remaining = this.#visibilityTimeout - elapsed
      const timer = setTimeout(() => {
        this.#processingTimers.delete(id)
        this.#checkJob(id).catch((err) => {
          this.emit('error', err)
        })
      }, remaining)
      this.#processingTimers.set(id, timer)
      return
    }

    // Job is stalled - need to recover it
    await this.#recoverStalledJob(id, workerId)
  }

  /**
   * Recover a stalled job by requeueing it
   */
  async #recoverStalledJob (id: string, workerId?: string): Promise<void> {
    if (!workerId) {
      this.emit('error', new Error(`Cannot recover stalled job ${id}: no workerId in state`))
      return
    }

    // Get the job from the worker's processing queue
    const processingJobs = await this.#storage.getProcessingJobs(workerId)

    // Find the message for this job
    let jobMessage: Buffer | null = null
    for (const message of processingJobs) {
      try {
        const queueMessage = this.#payloadSerde.deserialize(message) as unknown as QueueMessage<TPayload>
        if (queueMessage.id === id) {
          jobMessage = message
          break
        }
      } catch {
        // Ignore deserialization errors
      }
    }

    if (!jobMessage) {
      // Job not found in processing queue - it may have already been processed
      // Clear the state to prevent future checks
      return
    }

    // Requeue the job
    await this.#storage.requeue(id, jobMessage, workerId)

    // Update state to reflect retry
    const queueMessage = this.#payloadSerde.deserialize(jobMessage) as unknown as QueueMessage<TPayload>
    const newState = `failing:${Date.now()}:${queueMessage.attempts + 1}`
    await this.#storage.setJobState(id, newState)

    // Emit stalled event
    this.emit('stalled', id)
  }

  /**
   * Periodically check all workers' processing queues for stalled jobs
   */
  async #checkStalledJobs (): Promise<void> {
    if (!this.#running) return

    const workers = await this.#storage.getWorkers()

    for (const workerId of workers) {
      await this.#checkWorkerProcessingQueue(workerId)
    }
  }

  /**
   * Check a single worker's processing queue for stalled jobs
   */
  async #checkWorkerProcessingQueue (workerId: string): Promise<void> {
    if (!this.#running) return

    const processingJobs = await this.#storage.getProcessingJobs(workerId)

    for (const message of processingJobs) {
      try {
        const queueMessage = this.#payloadSerde.deserialize(message) as unknown as QueueMessage<TPayload>
        const state = await this.#storage.getJobState(queueMessage.id)

        if (!state) continue

        const { status, timestamp } = parseState(state)

        if (status === 'processing') {
          const elapsed = Date.now() - timestamp
          if (elapsed >= this.#visibilityTimeout) {
            // Job is stalled
            await this.#recoverStalledJob(queueMessage.id, workerId)
          } else if (!this.#processingTimers.has(queueMessage.id)) {
            // Start a timer for remaining time
            const remaining = this.#visibilityTimeout - elapsed
            const timer = setTimeout(() => {
              this.#processingTimers.delete(queueMessage.id)
              this.#checkJob(queueMessage.id).catch((err) => {
                this.emit('error', err)
              })
            }, remaining)
            this.#processingTimers.set(queueMessage.id, timer)
          }
        }
      } catch {
        // Ignore deserialization errors
      }
    }
  }
}
