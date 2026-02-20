import { randomUUID } from 'node:crypto'
import { EventEmitter } from 'node:events'
import type { Logger } from 'pino'
import type { Serde } from './serde/index.ts'
import { createJsonSerde } from './serde/index.ts'
import type { Storage } from './storage/types.ts'
import type { QueueMessage } from './types.ts'
import { abstractLogger, ensureLoggableError } from './utils/logging.ts'
import { parseState } from './utils/state.ts'

interface LeaderElectionConfig {
  enabled: boolean
  lockTTL?: number
  renewalInterval?: number
  acquireRetryInterval?: number
}

interface ReaperConfig<TPayload> {
  storage: Storage
  payloadSerde?: Serde<TPayload>
  visibilityTimeout?: number
  leaderElection?: LeaderElectionConfig
  logger?: Logger
}

interface ReaperEvents {
  error: [error: Error]
  stalled: [id: string]
  leadershipAcquired: []
  leadershipLost: []
}

const LOCK_KEY = 'reaper:lock'
const DEFAULT_LOCK_TTL = 30000
const DEFAULT_RENEWAL_INTERVAL = 10000
const DEFAULT_ACQUIRE_RETRY_INTERVAL = 5000

/**
 * Reaper monitors for stalled jobs and requeues them.
 *
 * A job is considered stalled if it has been in "processing" state
 * longer than the visibility timeout.
 *
 * When leader election is enabled, multiple Reaper instances can run
 * for high availability, with only one active at a time.
 */
export class Reaper<TPayload> extends EventEmitter<ReaperEvents> {
  #storage: Storage
  #payloadSerde: Serde<TPayload>
  #visibilityTimeout: number
  #leaderElection: LeaderElectionConfig
  #logger: Logger

  #running = false
  #unsubscribe: (() => Promise<void>) | null = null
  #processingTimers: Map<string, ReturnType<typeof setTimeout>> = new Map()

  // Leader election state
  #reaperId: string
  #isLeader = false
  #leadershipTimer: ReturnType<typeof setInterval> | null = null

  constructor (config: ReaperConfig<TPayload>) {
    super()
    this.#storage = config.storage
    this.#payloadSerde = config.payloadSerde ?? createJsonSerde<TPayload>()
    this.#visibilityTimeout = config.visibilityTimeout ?? 30000
    this.#leaderElection = config.leaderElection ?? { enabled: false }
    this.#reaperId = randomUUID()
    this.#logger = (config.logger ?? abstractLogger).child({ component: 'reaper', reaperId: this.#reaperId })
  }

  /**
   * Get the unique identifier for this reaper instance
   */
  get reaperId (): string {
    return this.#reaperId
  }

  /**
   * Check if this reaper is currently the leader
   */
  get isLeader (): boolean {
    return this.#isLeader
  }

  /**
   * Start the reaper
   */
  async start (): Promise<void> {
    if (this.#running) return

    this.#running = true

    if (this.#leaderElection.enabled) {
      await this.#startLeadershipLoop()
    } else {
      // No leader election, become active immediately
      await this.#becomeActive()
    }
  }

  /**
   * Stop the reaper gracefully
   */
  async stop (): Promise<void> {
    if (!this.#running) return

    this.#running = false

    // Stop leadership loop
    if (this.#leadershipTimer) {
      clearInterval(this.#leadershipTimer)
      this.#leadershipTimer = null
    }

    // Release lock if we're leader
    if (this.#isLeader && this.#leaderElection.enabled) {
      await this.#releaseLeadership()
      this.#isLeader = false
    }

    // Become inactive (cleanup)
    await this.#becomeInactive()
  }

  /**
   * Become active: subscribe to events and do initial scan
   */
  async #becomeActive (): Promise<void> {
    // Subscribe to job events
    this.#unsubscribe = await this.#storage.subscribeToEvents((id, event) => {
      this.#handleEvent(id, event)
    })

    // Do an initial scan for any jobs that were processing before we started
    await this.#checkStalledJobs()
  }

  /**
   * Become inactive: clear timers and unsubscribe
   */
  async #becomeInactive (): Promise<void> {
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
   * Start the leadership acquisition loop
   */
  async #startLeadershipLoop (): Promise<void> {
    const lockTTL = this.#leaderElection.lockTTL ?? DEFAULT_LOCK_TTL

    // Try to acquire lock immediately
    const acquired = await this.#tryAcquireLock(lockTTL)
    if (acquired) {
      this.#isLeader = true
      await this.#becomeActive()
      this.emit('leadershipAcquired')
    }

    // Start the appropriate timer based on current role
    this.#scheduleLeadershipCheck()
  }

  /**
   * Schedule the next leadership check (renewal or acquisition)
   */
  #scheduleLeadershipCheck (): void {
    const lockTTL = this.#leaderElection.lockTTL ?? DEFAULT_LOCK_TTL
    const renewalInterval = this.#leaderElection.renewalInterval ?? DEFAULT_RENEWAL_INTERVAL
    const acquireRetryInterval = this.#leaderElection.acquireRetryInterval ?? DEFAULT_ACQUIRE_RETRY_INTERVAL

    // Clear any existing timer
    if (this.#leadershipTimer) {
      clearInterval(this.#leadershipTimer)
    }

    const interval = this.#isLeader ? renewalInterval : acquireRetryInterval

    this.#leadershipTimer = setInterval(async () => {
      if (!this.#running) return

      try {
        if (this.#isLeader) {
          // Renew lock
          const renewed = await this.#tryRenewLock(lockTTL)
          if (!renewed) {
            // Lost leadership
            await this.#transitionToFollower()
          }
        } else {
          // Try to acquire lock
          const acquired = await this.#tryAcquireLock(lockTTL)
          if (acquired) {
            await this.#transitionToLeader()
          }
        }
      } catch (err) {
        this.#emitError(err, 'Leadership check failed.')
      }
    }, interval)
  }

  /**
   * Try to acquire the leader lock
   */
  async #tryAcquireLock (ttlMs: number): Promise<boolean> {
    if (!this.#storage.acquireLeaderLock) {
      // Storage doesn't support leader election
      this.#emitError(new Error('Storage does not support leader election'))
      return false
    }

    return this.#storage.acquireLeaderLock(LOCK_KEY, this.#reaperId, ttlMs)
  }

  /**
   * Try to renew the leader lock
   */
  async #tryRenewLock (ttlMs: number): Promise<boolean> {
    if (!this.#storage.renewLeaderLock) {
      return false
    }

    return this.#storage.renewLeaderLock(LOCK_KEY, this.#reaperId, ttlMs)
  }

  /**
   * Release the leader lock
   */
  async #releaseLeadership (): Promise<void> {
    if (!this.#storage.releaseLeaderLock) {
      return
    }

    await this.#storage.releaseLeaderLock(LOCK_KEY, this.#reaperId)
  }

  /**
   * Transition from follower to leader
   */
  async #transitionToLeader (): Promise<void> {
    this.#isLeader = true

    // Become active
    await this.#becomeActive()

    // Update interval timing (leaders use renewalInterval)
    this.#scheduleLeadershipCheck()

    this.emit('leadershipAcquired')
  }

  /**
   * Transition from leader to follower
   */
  async #transitionToFollower (): Promise<void> {
    this.#isLeader = false

    // Become inactive
    await this.#becomeInactive()

    // Update interval timing (followers use acquireRetryInterval)
    this.#scheduleLeadershipCheck()

    this.emit('leadershipLost')
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
      this.#checkJob(id).catch(err => {
        this.#emitError(err, 'Failed checking job after visibility timer.')
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
    if (this.#leaderElection.enabled && !this.#isLeader) return

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
        this.#checkJob(id).catch(err => {
          this.#emitError(err, 'Failed re-checking job after visibility timeout.')
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
      this.#emitError(new Error(`Cannot recover stalled job ${id}: no workerId in state`))
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
    if (this.#leaderElection.enabled && !this.#isLeader) return

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
    if (this.#leaderElection.enabled && !this.#isLeader) return

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
              this.#checkJob(queueMessage.id).catch(err => {
                this.#emitError(err, 'Failed checking worker processing job.')
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

  #emitError (err: unknown, message = 'Reaper emitted error.'): void {
    const error = err instanceof Error ? err : new Error(String(err))

    if (this.listenerCount('error') > 0) {
      this.emit('error', error)
      return
    }

    this.#logger.error({ err: ensureLoggableError(error) }, message)
  }
}
