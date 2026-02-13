import { EventEmitter } from 'node:events'
import type { Storage } from './types.ts'

interface StoredResult {
  data: Buffer
  expiresAt: number
}

interface WorkerInfo {
  expiresAt: number
}

/**
 * In-memory storage implementation for testing and single-process scenarios
 */
export class MemoryStorage implements Storage {
  private queue: Buffer[] = []
  private processingQueues: Map<string, Buffer[]> = new Map()
  private jobs: Map<string, string> = new Map()
  private results: Map<string, StoredResult> = new Map()
  private errors: Map<string, StoredResult> = new Map()
  private workers: Map<string, WorkerInfo> = new Map()

  private eventEmitter = new EventEmitter()
  private jobEmitters: Map<string, EventEmitter> = new Map()

  private cleanupInterval: ReturnType<typeof setInterval> | null = null
  private dequeueWaiters: Array<{
    workerId: string
    resolve: (value: Buffer | null) => void
    timeoutId: ReturnType<typeof setTimeout>
  }> = []

  async connect (): Promise<void> {
    // Start TTL cleanup interval (every 1 second)
    this.cleanupInterval = setInterval(() => {
      this.cleanupExpired()
    }, 1000)
  }

  async disconnect (): Promise<void> {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval)
      this.cleanupInterval = null
    }

    // Clear all dequeue waiters
    for (const waiter of this.dequeueWaiters) {
      clearTimeout(waiter.timeoutId)
      waiter.resolve(null)
    }
    this.dequeueWaiters = []

    // Clear all event emitters
    this.eventEmitter.removeAllListeners()
    for (const emitter of this.jobEmitters.values()) {
      emitter.removeAllListeners()
    }
    this.jobEmitters.clear()
  }

  async enqueue (id: string, message: Buffer, timestamp: number): Promise<string | null> {
    const existing = this.jobs.get(id)
    if (existing) {
      return existing
    }

    this.jobs.set(id, `queued:${timestamp}`)
    this.queue.push(message)

    // Publish event
    this.eventEmitter.emit('event', id, 'queued')

    // Notify any waiting dequeue calls
    this.notifyDequeueWaiters()

    return null
  }

  async dequeue (workerId: string, timeout: number): Promise<Buffer | null> {
    // Try to get a job immediately
    const message = this.queue.shift()
    if (message) {
      this.addToProcessingQueue(workerId, message)
      return message
    }

    // No job available, wait for one
    return new Promise((resolve) => {
      const timeoutId = setTimeout(() => {
        // Remove this waiter from the list
        const index = this.dequeueWaiters.findIndex(w => w.resolve === resolve)
        if (index !== -1) {
          this.dequeueWaiters.splice(index, 1)
        }
        resolve(null)
      }, timeout * 1000)

      this.dequeueWaiters.push({ workerId, resolve, timeoutId })
    })
  }

  private notifyDequeueWaiters (): void {
    while (this.dequeueWaiters.length > 0 && this.queue.length > 0) {
      const waiter = this.dequeueWaiters.shift()
      if (waiter) {
        clearTimeout(waiter.timeoutId)
        const message = this.queue.shift()
        if (message) {
          this.addToProcessingQueue(waiter.workerId, message)
          waiter.resolve(message)
        } else {
          waiter.resolve(null)
        }
      }
    }
  }

  private addToProcessingQueue (workerId: string, message: Buffer): void {
    let processingQueue = this.processingQueues.get(workerId)
    if (!processingQueue) {
      processingQueue = []
      this.processingQueues.set(workerId, processingQueue)
    }
    processingQueue.push(message)
  }

  async requeue (id: string, message: Buffer, workerId: string): Promise<void> {
    // Remove from processing queue
    const processingQueue = this.processingQueues.get(workerId)
    if (processingQueue) {
      const index = processingQueue.findIndex(m => m.equals(message))
      if (index !== -1) {
        processingQueue.splice(index, 1)
      }
    }

    // Add back to main queue
    this.queue.unshift(message)

    // Notify waiters
    this.notifyDequeueWaiters()
  }

  async ack (id: string, message: Buffer, workerId: string): Promise<void> {
    const processingQueue = this.processingQueues.get(workerId)
    if (processingQueue) {
      const index = processingQueue.findIndex(m => m.equals(message))
      if (index !== -1) {
        processingQueue.splice(index, 1)
      }
    }
  }

  async getJobState (id: string): Promise<string | null> {
    return this.jobs.get(id) ?? null
  }

  async setJobState (id: string, state: string): Promise<void> {
    this.jobs.set(id, state)
  }

  async deleteJob (id: string): Promise<boolean> {
    const existed = this.jobs.has(id)
    this.jobs.delete(id)

    if (existed) {
      this.eventEmitter.emit('event', id, 'cancelled')
    }

    return existed
  }

  async getJobStates (ids: string[]): Promise<Map<string, string | null>> {
    const result = new Map<string, string | null>()
    for (const id of ids) {
      result.set(id, this.jobs.get(id) ?? null)
    }
    return result
  }

  async setResult (id: string, result: Buffer, ttlMs: number): Promise<void> {
    this.results.set(id, {
      data: result,
      expiresAt: Date.now() + ttlMs
    })
  }

  async getResult (id: string): Promise<Buffer | null> {
    const stored = this.results.get(id)
    if (!stored) return null
    if (Date.now() > stored.expiresAt) {
      this.results.delete(id)
      return null
    }
    return stored.data
  }

  async setError (id: string, error: Buffer, ttlMs: number): Promise<void> {
    this.errors.set(id, {
      data: error,
      expiresAt: Date.now() + ttlMs
    })
  }

  async getError (id: string): Promise<Buffer | null> {
    const stored = this.errors.get(id)
    if (!stored) return null
    if (Date.now() > stored.expiresAt) {
      this.errors.delete(id)
      return null
    }
    return stored.data
  }

  async registerWorker (workerId: string, ttlMs: number): Promise<void> {
    this.workers.set(workerId, {
      expiresAt: Date.now() + ttlMs
    })
  }

  async refreshWorker (workerId: string, ttlMs: number): Promise<void> {
    this.workers.set(workerId, {
      expiresAt: Date.now() + ttlMs
    })
  }

  async unregisterWorker (workerId: string): Promise<void> {
    this.workers.delete(workerId)
    this.processingQueues.delete(workerId)
  }

  async getWorkers (): Promise<string[]> {
    const now = Date.now()
    const activeWorkers: string[] = []

    for (const [workerId, info] of this.workers) {
      if (now <= info.expiresAt) {
        activeWorkers.push(workerId)
      }
    }

    return activeWorkers
  }

  async getProcessingJobs (workerId: string): Promise<Buffer[]> {
    return this.processingQueues.get(workerId) ?? []
  }

  async subscribeToJob (
    id: string,
    handler: (status: 'completed' | 'failed') => void
  ): Promise<() => Promise<void>> {
    let emitter = this.jobEmitters.get(id)
    if (!emitter) {
      emitter = new EventEmitter()
      this.jobEmitters.set(id, emitter)
    }

    emitter.on('notify', handler)

    return async () => {
      emitter!.off('notify', handler)
      if (emitter!.listenerCount('notify') === 0) {
        this.jobEmitters.delete(id)
      }
    }
  }

  async notifyJobComplete (id: string, status: 'completed' | 'failed'): Promise<void> {
    const emitter = this.jobEmitters.get(id)
    if (emitter) {
      emitter.emit('notify', status)
    }
  }

  async subscribeToEvents (
    handler: (id: string, event: string) => void
  ): Promise<() => Promise<void>> {
    this.eventEmitter.on('event', handler)

    return async () => {
      this.eventEmitter.off('event', handler)
    }
  }

  async publishEvent (id: string, event: string): Promise<void> {
    this.eventEmitter.emit('event', id, event)
  }

  async completeJob (
    id: string,
    message: Buffer,
    workerId: string,
    result: Buffer,
    resultTtlMs: number
  ): Promise<void> {
    const timestamp = Date.now()

    // Set state to completed
    this.jobs.set(id, `completed:${timestamp}`)

    // Store result
    await this.setResult(id, result, resultTtlMs)

    // Remove from processing queue
    await this.ack(id, message, workerId)

    // Publish notification
    await this.notifyJobComplete(id, 'completed')

    // Publish event
    this.eventEmitter.emit('event', id, 'completed')
  }

  async failJob (
    id: string,
    message: Buffer,
    workerId: string,
    error: Buffer,
    errorTtlMs: number
  ): Promise<void> {
    const timestamp = Date.now()

    // Set state to failed
    this.jobs.set(id, `failed:${timestamp}`)

    // Store error
    await this.setError(id, error, errorTtlMs)

    // Remove from processing queue
    await this.ack(id, message, workerId)

    // Publish notification
    await this.notifyJobComplete(id, 'failed')

    // Publish event
    this.eventEmitter.emit('event', id, 'failed')
  }

  async retryJob (
    id: string,
    message: Buffer,
    workerId: string,
    attempts: number
  ): Promise<void> {
    const timestamp = Date.now()

    // Set state to failing
    this.jobs.set(id, `failing:${timestamp}:${attempts}`)

    // Move from processing queue to main queue
    await this.requeue(id, message, workerId)

    // Publish event
    this.eventEmitter.emit('event', id, 'failing')
  }

  private cleanupExpired (): void {
    const now = Date.now()

    // Clean expired results
    for (const [id, stored] of this.results) {
      if (now > stored.expiresAt) {
        this.results.delete(id)
      }
    }

    // Clean expired errors
    for (const [id, stored] of this.errors) {
      if (now > stored.expiresAt) {
        this.errors.delete(id)
      }
    }

    // Clean expired workers
    for (const [workerId, info] of this.workers) {
      if (now > info.expiresAt) {
        this.workers.delete(workerId)
      }
    }
  }

  /**
   * Clear all data (useful for testing)
   */
  clear (): void {
    this.queue = []
    this.processingQueues.clear()
    this.jobs.clear()
    this.results.clear()
    this.errors.clear()
    this.workers.clear()
  }
}
