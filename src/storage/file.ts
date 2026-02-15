import { mkdir, readdir, readFile, unlink, watch, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { EventEmitter } from 'node:events'
import fastWriteAtomic from 'fast-write-atomic'
import type { Storage } from './types.ts'

const writeFileAtomic = fastWriteAtomic.promise

interface FileStorageConfig {
  basePath: string
}

interface DequeueWaiter {
  workerId: string
  resolve: (value: Buffer | null) => void
  timeoutId: ReturnType<typeof setTimeout>
}

/**
 * File-based storage implementation
 * Uses the filesystem for persistence with atomic writes
 */
export class FileStorage implements Storage {
  #basePath: string
  #queuePath: string
  #processingPath: string
  #jobsPath: string
  #resultsPath: string
  #errorsPath: string
  #workersPath: string
  #notifyPath: string

  #sequence = 0
  #eventEmitter = new EventEmitter({ captureRejections: true })
  #notifyEmitter = new EventEmitter({ captureRejections: true })
  #dequeueWaiters: DequeueWaiter[] = []
  #queueWatcher: AsyncIterable<{ eventType: string, filename: string | null }> | null = null
  #notifyWatcher: AsyncIterable<{ eventType: string, filename: string | null }> | null = null
  #watchAbortController: AbortController | null = null
  #cleanupInterval: ReturnType<typeof setInterval> | null = null
  #connected = false

  constructor (config: FileStorageConfig) {
    this.#basePath = config.basePath
    this.#queuePath = join(this.#basePath, 'queue')
    this.#processingPath = join(this.#basePath, 'processing')
    this.#jobsPath = join(this.#basePath, 'jobs')
    this.#resultsPath = join(this.#basePath, 'results')
    this.#errorsPath = join(this.#basePath, 'errors')
    this.#workersPath = join(this.#basePath, 'workers')
    this.#notifyPath = join(this.#basePath, 'notify')

    this.#eventEmitter.setMaxListeners(0)
    this.#notifyEmitter.setMaxListeners(0)
  }

  async connect (): Promise<void> {
    if (this.#connected) return

    // Create directory structure
    await Promise.all([
      mkdir(this.#queuePath, { recursive: true }),
      mkdir(this.#processingPath, { recursive: true }),
      mkdir(this.#jobsPath, { recursive: true }),
      mkdir(this.#resultsPath, { recursive: true }),
      mkdir(this.#errorsPath, { recursive: true }),
      mkdir(this.#workersPath, { recursive: true }),
      mkdir(this.#notifyPath, { recursive: true })
    ])

    // Initialize sequence number from existing queue files
    await this.#initSequence()

    // Start watching directories
    this.#watchAbortController = new AbortController()
    this.#startQueueWatcher()
    this.#startNotifyWatcher()

    // Start cleanup interval
    this.#cleanupInterval = setInterval(() => {
      this.#cleanupExpired().catch(() => {})
    }, 1000)

    this.#connected = true
  }

  async disconnect (): Promise<void> {
    if (!this.#connected) return

    // Stop watchers
    if (this.#watchAbortController) {
      this.#watchAbortController.abort()
      this.#watchAbortController = null
    }

    // Stop cleanup
    if (this.#cleanupInterval) {
      clearInterval(this.#cleanupInterval)
      this.#cleanupInterval = null
    }

    // Clear dequeue waiters
    for (const waiter of this.#dequeueWaiters) {
      clearTimeout(waiter.timeoutId)
      waiter.resolve(null)
    }
    this.#dequeueWaiters = []

    this.#eventEmitter.removeAllListeners()
    this.#notifyEmitter.removeAllListeners()
    this.#connected = false
  }

  async #initSequence (): Promise<void> {
    try {
      const files = await readdir(this.#queuePath)
      let maxSeq = 0
      for (const file of files) {
        const seq = parseInt(file.split('-')[0], 10)
        if (!isNaN(seq) && seq > maxSeq) {
          maxSeq = seq
        }
      }
      this.#sequence = maxSeq
    } catch {
      this.#sequence = 0
    }
  }

  #startQueueWatcher (): void {
    if (!this.#watchAbortController) return

    const runWatcher = async () => {
      try {
        const watcher = watch(this.#queuePath, { signal: this.#watchAbortController?.signal })
        for await (const event of watcher) {
          if (event.eventType === 'rename' && event.filename?.endsWith('.msg')) {
            this.#notifyDequeueWaiters()
          }
        }
      } catch (err: unknown) {
        if (err instanceof Error && err.name === 'AbortError') return
        // Restart watcher on error
        setTimeout(() => this.#startQueueWatcher(), 100)
      }
    }
    runWatcher()
  }

  #startNotifyWatcher (): void {
    if (!this.#watchAbortController) return

    const runWatcher = async () => {
      try {
        const watcher = watch(this.#notifyPath, { signal: this.#watchAbortController?.signal })
        for await (const event of watcher) {
          if (event.eventType === 'rename' && event.filename?.endsWith('.notify')) {
            // Read and process notification
            const notifyFile = join(this.#notifyPath, event.filename)
            try {
              const content = await readFile(notifyFile, 'utf8')
              const [id, status] = content.split(':')
              this.#notifyEmitter.emit(`notify:${id}`, status as 'completed' | 'failed')
              // Clean up notification file
              await unlink(notifyFile).catch(() => {})
            } catch {
              // File may have been deleted already
            }
          }
        }
      } catch (err: unknown) {
        if (err instanceof Error && err.name === 'AbortError') return
        setTimeout(() => this.#startNotifyWatcher(), 100)
      }
    }
    runWatcher()
  }

  async #notifyDequeueWaiters (): Promise<void> {
    while (this.#dequeueWaiters.length > 0) {
      const waiter = this.#dequeueWaiters[0]
      const message = await this.#getNextQueueMessage(waiter.workerId)
      if (message) {
        this.#dequeueWaiters.shift()
        clearTimeout(waiter.timeoutId)
        waiter.resolve(message)
      } else {
        break
      }
    }
  }

  async #getNextQueueMessage (workerId: string): Promise<Buffer | null> {
    const files = await this.#getQueueFiles()
    if (files.length === 0) return null

    // Try to claim the first file
    const file = files[0]
    const srcPath = join(this.#queuePath, file)
    const id = this.#extractIdFromFilename(file)
    const dstPath = join(this.#processingPath, workerId, `${id}.msg`)

    try {
      // Ensure processing directory exists
      await mkdir(join(this.#processingPath, workerId), { recursive: true })

      // Read and move atomically
      const message = await readFile(srcPath)
      await writeFileAtomic(dstPath, message)
      await unlink(srcPath)
      return message
    } catch {
      // Another worker may have claimed it
      return null
    }
  }

  async #getQueueFiles (): Promise<string[]> {
    try {
      const files = await readdir(this.#queuePath)
      return files
        .filter(f => f.endsWith('.msg'))
        .sort((a, b) => {
          const seqA = parseInt(a.split('-')[0], 10)
          const seqB = parseInt(b.split('-')[0], 10)
          return seqA - seqB
        })
    } catch {
      return []
    }
  }

  #extractIdFromFilename (filename: string): string {
    // Format: sequence-id.msg
    const withoutExt = filename.replace('.msg', '')
    const dashIndex = withoutExt.indexOf('-')
    return dashIndex >= 0 ? withoutExt.substring(dashIndex + 1) : withoutExt
  }

  async enqueue (id: string, message: Buffer, timestamp: number): Promise<string | null> {
    const jobFile = join(this.#jobsPath, `${id}.state`)

    // Check if job already exists
    try {
      const existing = await readFile(jobFile, 'utf8')
      return existing
    } catch {
      // Job doesn't exist, continue
    }

    // Create job state atomically
    const state = `queued:${timestamp}`
    try {
      // Try to create job file - if it exists, this will overwrite but we check above
      await writeFileAtomic(jobFile, state, { mode: 0o644 })
    } catch {
      // Check if another process created it
      try {
        const existing = await readFile(jobFile, 'utf8')
        return existing
      } catch {
        throw new Error('Failed to create job state')
      }
    }

    // Add to queue
    const seq = ++this.#sequence
    const queueFile = join(this.#queuePath, `${seq.toString().padStart(12, '0')}-${id}.msg`)
    await writeFileAtomic(queueFile, message)

    // Publish event
    this.#eventEmitter.emit('event', id, 'queued')

    // Notify waiters
    await this.#notifyDequeueWaiters()

    return null
  }

  async dequeue (workerId: string, timeout: number): Promise<Buffer | null> {
    // Try to get a job immediately
    const message = await this.#getNextQueueMessage(workerId)
    if (message) return message

    // Wait for a job
    return new Promise((resolve) => {
      const timeoutId = setTimeout(() => {
        const index = this.#dequeueWaiters.findIndex(w => w.resolve === resolve)
        if (index !== -1) {
          this.#dequeueWaiters.splice(index, 1)
        }
        resolve(null)
      }, timeout * 1000)

      this.#dequeueWaiters.push({ workerId, resolve, timeoutId })
    })
  }

  async requeue (id: string, message: Buffer, workerId: string): Promise<void> {
    // Remove from processing queue
    const processingFile = join(this.#processingPath, workerId, `${id}.msg`)
    await unlink(processingFile).catch(() => {})

    // Add back to front of queue
    const seq = ++this.#sequence
    const queueFile = join(this.#queuePath, `${seq.toString().padStart(12, '0')}-${id}.msg`)
    await writeFileAtomic(queueFile, message)

    // Notify waiters
    await this.#notifyDequeueWaiters()
  }

  async ack (id: string, message: Buffer, workerId: string): Promise<void> {
    const processingFile = join(this.#processingPath, workerId, `${id}.msg`)
    await unlink(processingFile).catch(() => {})
  }

  async getJobState (id: string): Promise<string | null> {
    try {
      return await readFile(join(this.#jobsPath, `${id}.state`), 'utf8')
    } catch {
      return null
    }
  }

  async setJobState (id: string, state: string): Promise<void> {
    await writeFileAtomic(join(this.#jobsPath, `${id}.state`), state)
  }

  async deleteJob (id: string): Promise<boolean> {
    try {
      await unlink(join(this.#jobsPath, `${id}.state`))
      this.#eventEmitter.emit('event', id, 'cancelled')
      return true
    } catch {
      return false
    }
  }

  async getJobStates (ids: string[]): Promise<Map<string, string | null>> {
    const result = new Map<string, string | null>()
    await Promise.all(ids.map(async (id) => {
      result.set(id, await this.getJobState(id))
    }))
    return result
  }

  async setResult (id: string, result: Buffer, ttlMs: number): Promise<void> {
    const filePath = join(this.#resultsPath, `${id}.result`)
    // Store TTL expiry time in a companion file
    const ttlPath = join(this.#resultsPath, `${id}.ttl`)
    const expiresAt = Date.now() + ttlMs

    await Promise.all([
      writeFileAtomic(filePath, result),
      writeFileAtomic(ttlPath, expiresAt.toString())
    ])
  }

  async getResult (id: string): Promise<Buffer | null> {
    try {
      const ttlPath = join(this.#resultsPath, `${id}.ttl`)
      const expiresAt = parseInt(await readFile(ttlPath, 'utf8'), 10)

      if (Date.now() > expiresAt) {
        // Expired - delete files
        await Promise.all([
          unlink(join(this.#resultsPath, `${id}.result`)).catch(() => {}),
          unlink(ttlPath).catch(() => {})
        ])
        return null
      }

      return await readFile(join(this.#resultsPath, `${id}.result`))
    } catch {
      return null
    }
  }

  async setError (id: string, error: Buffer, ttlMs: number): Promise<void> {
    const filePath = join(this.#errorsPath, `${id}.error`)
    const ttlPath = join(this.#errorsPath, `${id}.ttl`)
    const expiresAt = Date.now() + ttlMs

    await Promise.all([
      writeFileAtomic(filePath, error),
      writeFileAtomic(ttlPath, expiresAt.toString())
    ])
  }

  async getError (id: string): Promise<Buffer | null> {
    try {
      const ttlPath = join(this.#errorsPath, `${id}.ttl`)
      const expiresAt = parseInt(await readFile(ttlPath, 'utf8'), 10)

      if (Date.now() > expiresAt) {
        await Promise.all([
          unlink(join(this.#errorsPath, `${id}.error`)).catch(() => {}),
          unlink(ttlPath).catch(() => {})
        ])
        return null
      }

      return await readFile(join(this.#errorsPath, `${id}.error`))
    } catch {
      return null
    }
  }

  async registerWorker (workerId: string, ttlMs: number): Promise<void> {
    const filePath = join(this.#workersPath, `${workerId}.worker`)
    const expiresAt = Date.now() + ttlMs
    await writeFileAtomic(filePath, expiresAt.toString())
  }

  async refreshWorker (workerId: string, ttlMs: number): Promise<void> {
    await this.registerWorker(workerId, ttlMs)
  }

  async unregisterWorker (workerId: string): Promise<void> {
    await unlink(join(this.#workersPath, `${workerId}.worker`)).catch(() => {})
    // Also clean up processing queue
    await rm(join(this.#processingPath, workerId), { recursive: true, force: true }).catch(() => {})
  }

  async getWorkers (): Promise<string[]> {
    try {
      const files = await readdir(this.#workersPath)
      const now = Date.now()
      const workers: string[] = []

      for (const file of files) {
        if (!file.endsWith('.worker')) continue
        const workerId = file.replace('.worker', '')

        try {
          const expiresAt = parseInt(
            await readFile(join(this.#workersPath, file), 'utf8'),
            10
          )
          if (now <= expiresAt) {
            workers.push(workerId)
          }
        } catch {
          // Ignore unreadable files
        }
      }

      return workers
    } catch {
      return []
    }
  }

  async getProcessingJobs (workerId: string): Promise<Buffer[]> {
    const processingDir = join(this.#processingPath, workerId)
    try {
      const files = await readdir(processingDir)
      const jobs: Buffer[] = []

      for (const file of files) {
        if (!file.endsWith('.msg')) continue
        try {
          const content = await readFile(join(processingDir, file))
          jobs.push(content)
        } catch {
          // Ignore unreadable files
        }
      }

      return jobs
    } catch {
      return []
    }
  }

  async subscribeToJob (
    id: string,
    handler: (status: 'completed' | 'failed') => void
  ): Promise<() => Promise<void>> {
    const eventName = `notify:${id}`
    this.#notifyEmitter.on(eventName, handler)

    return async () => {
      this.#notifyEmitter.off(eventName, handler)
    }
  }

  async notifyJobComplete (id: string, status: 'completed' | 'failed'): Promise<void> {
    // Write a notification file that will be picked up by the watcher
    const notifyFile = join(this.#notifyPath, `${id}-${Date.now()}.notify`)
    await writeFileAtomic(notifyFile, `${id}:${status}`)

    // Also emit locally for in-process subscribers
    this.#notifyEmitter.emit(`notify:${id}`, status)
  }

  async subscribeToEvents (
    handler: (id: string, event: string) => void
  ): Promise<() => Promise<void>> {
    this.#eventEmitter.on('event', handler)

    return async () => {
      this.#eventEmitter.off('event', handler)
    }
  }

  async publishEvent (id: string, event: string): Promise<void> {
    this.#eventEmitter.emit('event', id, event)
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
    await this.setJobState(id, `completed:${timestamp}`)

    // Store result
    await this.setResult(id, result, resultTtlMs)

    // Remove from processing queue
    await this.ack(id, message, workerId)

    // Publish notification
    await this.notifyJobComplete(id, 'completed')

    // Publish event
    this.#eventEmitter.emit('event', id, 'completed')
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
    await this.setJobState(id, `failed:${timestamp}`)

    // Store error
    await this.setError(id, error, errorTtlMs)

    // Remove from processing queue
    await this.ack(id, message, workerId)

    // Publish notification
    await this.notifyJobComplete(id, 'failed')

    // Publish event
    this.#eventEmitter.emit('event', id, 'failed')
  }

  async retryJob (
    id: string,
    message: Buffer,
    workerId: string,
    attempts: number
  ): Promise<void> {
    const timestamp = Date.now()

    // Set state to failing
    await this.setJobState(id, `failing:${timestamp}:${attempts}`)

    // Move from processing queue to main queue
    await this.requeue(id, message, workerId)

    // Publish event
    this.#eventEmitter.emit('event', id, 'failing')
  }

  async #cleanupExpired (): Promise<void> {
    const now = Date.now()

    // Clean expired results
    try {
      const resultFiles = await readdir(this.#resultsPath)
      for (const file of resultFiles) {
        if (!file.endsWith('.ttl')) continue
        const id = file.replace('.ttl', '')
        try {
          const expiresAt = parseInt(
            await readFile(join(this.#resultsPath, file), 'utf8'),
            10
          )
          if (now > expiresAt) {
            await unlink(join(this.#resultsPath, `${id}.result`)).catch(() => {})
            await unlink(join(this.#resultsPath, file)).catch(() => {})
          }
        } catch {
          // Ignore errors
        }
      }
    } catch {
      // Ignore errors
    }

    // Clean expired errors
    try {
      const errorFiles = await readdir(this.#errorsPath)
      for (const file of errorFiles) {
        if (!file.endsWith('.ttl')) continue
        const id = file.replace('.ttl', '')
        try {
          const expiresAt = parseInt(
            await readFile(join(this.#errorsPath, file), 'utf8'),
            10
          )
          if (now > expiresAt) {
            await unlink(join(this.#errorsPath, `${id}.error`)).catch(() => {})
            await unlink(join(this.#errorsPath, file)).catch(() => {})
          }
        } catch {
          // Ignore errors
        }
      }
    } catch {
      // Ignore errors
    }

    // Clean expired workers
    try {
      const workerFiles = await readdir(this.#workersPath)
      for (const file of workerFiles) {
        if (!file.endsWith('.worker')) continue
        try {
          const expiresAt = parseInt(
            await readFile(join(this.#workersPath, file), 'utf8'),
            10
          )
          if (now > expiresAt) {
            await unlink(join(this.#workersPath, file)).catch(() => {})
          }
        } catch {
          // Ignore errors
        }
      }
    } catch {
      // Ignore errors
    }
  }

  /**
   * Clear all data (useful for testing)
   */
  async clear (): Promise<void> {
    await Promise.all([
      rm(this.#queuePath, { recursive: true, force: true }),
      rm(this.#processingPath, { recursive: true, force: true }),
      rm(this.#jobsPath, { recursive: true, force: true }),
      rm(this.#resultsPath, { recursive: true, force: true }),
      rm(this.#errorsPath, { recursive: true, force: true }),
      rm(this.#workersPath, { recursive: true, force: true }),
      rm(this.#notifyPath, { recursive: true, force: true })
    ])

    // Recreate directories
    await Promise.all([
      mkdir(this.#queuePath, { recursive: true }),
      mkdir(this.#processingPath, { recursive: true }),
      mkdir(this.#jobsPath, { recursive: true }),
      mkdir(this.#resultsPath, { recursive: true }),
      mkdir(this.#errorsPath, { recursive: true }),
      mkdir(this.#workersPath, { recursive: true }),
      mkdir(this.#notifyPath, { recursive: true })
    ])
  }
}
