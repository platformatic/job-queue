/**
 * Storage interface for pluggable backends
 */
export interface Storage {
  // ═══════════════════════════════════════════════════════════════════
  // LIFECYCLE
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Initialize the storage connection.
   * Called once when Queue.start() is invoked.
   */
  connect (): Promise<void>

  /**
   * Close the storage connection gracefully.
   * Called when Queue.stop() is invoked.
   */
  disconnect (): Promise<void>

  // ═══════════════════════════════════════════════════════════════════
  // QUEUE OPERATIONS
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Atomically enqueue a job if not already present.
   * Must check for duplicates and set initial state in one atomic operation.
   *
   * @returns null if job was enqueued, existing state string if duplicate
   */
  enqueue (id: string, message: Buffer, timestamp: number): Promise<string | null>

  /**
   * Blocking dequeue: move a job from main queue to worker's processing queue.
   * Should block up to `timeout` seconds if queue is empty.
   *
   * @returns The job message, or null if timeout
   */
  dequeue (workerId: string, timeout: number): Promise<Buffer | null>

  /**
   * Move a job from worker's processing queue back to main queue.
   * Used for retries and stall recovery.
   */
  requeue (id: string, message: Buffer, workerId: string): Promise<void>

  /**
   * Remove a job from worker's processing queue (after completion or failure).
   */
  ack (id: string, message: Buffer, workerId: string): Promise<void>

  // ═══════════════════════════════════════════════════════════════════
  // JOB STATE
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Get the current state of a job.
   * State format: "status:timestamp[:workerId]"
   *
   * @returns State string, or null if job doesn't exist
   */
  getJobState (id: string): Promise<string | null>

  /**
   * Set the state of a job.
   * Should also publish a notification for state changes.
   */
  setJobState (id: string, state: string): Promise<void>

  /**
   * Delete a job from the jobs registry.
   * Used for cancellation.
   *
   * @returns true if job existed and was deleted
   */
  deleteJob (id: string): Promise<boolean>

  /**
   * Get multiple job states in one call (batch operation).
   */
  getJobStates (ids: string[]): Promise<Map<string, string | null>>

  // ═══════════════════════════════════════════════════════════════════
  // RESULTS
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Store a job result with TTL.
   */
  setResult (id: string, result: Buffer, ttlMs: number): Promise<void>

  /**
   * Retrieve a stored job result.
   *
   * @returns Result buffer, or null if not found/expired
   */
  getResult (id: string): Promise<Buffer | null>

  /**
   * Store a job error with TTL.
   */
  setError (id: string, error: Buffer, ttlMs: number): Promise<void>

  /**
   * Retrieve a stored job error.
   */
  getError (id: string): Promise<Buffer | null>

  // ═══════════════════════════════════════════════════════════════════
  // WORKERS
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Register a worker as active.
   * Should set a TTL so crashed workers are automatically removed.
   */
  registerWorker (workerId: string, ttlMs: number): Promise<void>

  /**
   * Refresh worker registration (heartbeat).
   */
  refreshWorker (workerId: string, ttlMs: number): Promise<void>

  /**
   * Unregister a worker (graceful shutdown).
   */
  unregisterWorker (workerId: string): Promise<void>

  /**
   * Get list of all registered workers.
   */
  getWorkers (): Promise<string[]>

  /**
   * Get all jobs in a worker's processing queue.
   * Used by reaper to find stalled jobs.
   */
  getProcessingJobs (workerId: string): Promise<Buffer[]>

  // ═══════════════════════════════════════════════════════════════════
  // NOTIFICATIONS (for request/response)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Subscribe to notifications for a specific job.
   * Used by enqueueAndWait to receive completion notification.
   *
   * @param id - Job ID to subscribe to
   * @param handler - Called when notification received
   * @returns Unsubscribe function
   */
  subscribeToJob (
    id: string,
    handler: (status: 'completed' | 'failed' | 'failing') => void
  ): Promise<() => Promise<void>>

  /**
   * Publish a job completion/failure notification.
   * Called by worker after job finishes or is retried.
   */
  notifyJobComplete (id: string, status: 'completed' | 'failed' | 'failing'): Promise<void>

  // ═══════════════════════════════════════════════════════════════════
  // EVENTS (for monitoring/reaper)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Subscribe to all job state change events.
   * Used by reaper and monitoring.
   *
   * @returns Unsubscribe function
   */
  subscribeToEvents (
    handler: (id: string, event: string) => void
  ): Promise<() => Promise<void>>

  /**
   * Publish a job state change event.
   */
  publishEvent (id: string, event: string): Promise<void>

  // ═══════════════════════════════════════════════════════════════════
  // LEADER ELECTION (optional, for Reaper high availability)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Try to acquire the reaper leader lock.
   * Uses SET NX PX pattern for atomic acquisition.
   *
   * @param lockKey - The lock key name
   * @param ownerId - Unique identifier for this reaper instance
   * @param ttlMs - Lock TTL in milliseconds
   * @returns true if lock was acquired, false if already held by another
   */
  acquireLeaderLock?(lockKey: string, ownerId: string, ttlMs: number): Promise<boolean>

  /**
   * Renew the leader lock if still owned by this reaper.
   * Extends the TTL atomically only if the current owner matches.
   *
   * @param lockKey - The lock key name
   * @param ownerId - Unique identifier for this reaper instance
   * @param ttlMs - New TTL in milliseconds
   * @returns true if lock was renewed, false if not owned
   */
  renewLeaderLock?(lockKey: string, ownerId: string, ttlMs: number): Promise<boolean>

  /**
   * Release the leader lock if owned by this reaper.
   * Deletes the lock atomically only if the current owner matches.
   *
   * @param lockKey - The lock key name
   * @param ownerId - Unique identifier for this reaper instance
   * @returns true if lock was released, false if not owned
   */
  releaseLeaderLock?(lockKey: string, ownerId: string): Promise<boolean>

  // ═══════════════════════════════════════════════════════════════════
  // ATOMIC OPERATIONS (Lua scripts in Redis)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Atomically complete a job:
   * - Set state to completed
   * - Store result
   * - Remove from processing queue
   * - Publish notification
   */
  completeJob (
    id: string,
    message: Buffer,
    workerId: string,
    result: Buffer,
    resultTtlMs: number
  ): Promise<void>

  /**
   * Atomically fail a job:
   * - Set state to failed
   * - Store error
   * - Remove from processing queue
   * - Publish notification
   */
  failJob (
    id: string,
    message: Buffer,
    workerId: string,
    error: Buffer,
    errorTtlMs: number
  ): Promise<void>

  /**
   * Atomically retry a job:
   * - Set state to failing
   * - Move from processing queue to main queue
   * - Publish event
   */
  retryJob (
    id: string,
    message: Buffer,
    workerId: string,
    attempts: number
  ): Promise<void>
}
