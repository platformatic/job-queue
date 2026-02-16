import { readFileSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { EventEmitter } from 'node:events'
import { Redis } from 'iovalkey'
import type { Storage } from './types.ts'

const __dirname = dirname(fileURLToPath(import.meta.url))

interface RedisStorageConfig {
  url?: string
  keyPrefix?: string
}

interface ScriptSHAs {
  enqueue: string
  complete: string
  fail: string
  retry: string
  cancel: string
}

/**
 * Redis/Valkey storage implementation
 */
export class RedisStorage implements Storage {
  #url: string
  #keyPrefix: string
  #client: Redis | null = null
  #blockingClient: Redis | null = null // Single blocking client for BLMOVE
  #subscriber: Redis | null = null
  #scriptSHAs: ScriptSHAs | null = null
  #eventEmitter = new EventEmitter({ captureRejections: true })
  #notifyEmitter = new EventEmitter({ captureRejections: true })
  #eventSubscription: boolean = false

  constructor (config: RedisStorageConfig = {}) {
    this.#url = config.url ?? process.env.REDIS_URL ?? 'redis://localhost:6379'
    this.#keyPrefix = config.keyPrefix ?? 'jq:'

    // Disable max listeners warning for high-throughput scenarios
    this.#eventEmitter.setMaxListeners(0)
    this.#notifyEmitter.setMaxListeners(0)
  }

  // Key helpers
  #key (name: string): string {
    return `${this.#keyPrefix}${name}`
  }

  #jobsKey (): string {
    return this.#key('jobs')
  }

  #queueKey (): string {
    return this.#key('queue')
  }

  #processingKey (workerId: string): string {
    return this.#key(`processing:${workerId}`)
  }

  #resultsKey (): string {
    return this.#key('results')
  }

  #errorsKey (): string {
    return this.#key('errors')
  }

  #workersKey (): string {
    return this.#key('workers')
  }

  async connect (): Promise<void> {
    if (this.#client) return

    this.#client = new Redis(this.#url)
    this.#subscriber = new Redis(this.#url)
    this.#blockingClient = new Redis(this.#url)

    // Load Lua scripts
    await this.#loadScripts()

    // Set up pub/sub message handler
    this.#subscriber.on('message', (channel: string, message: string) => {
      this.#handlePubSubMessage(channel, message)
    })

    this.#subscriber.on('pmessage', (_pattern: string, channel: string, message: string) => {
      this.#handlePubSubMessage(channel, message)
    })
  }

  async disconnect (): Promise<void> {
    if (this.#subscriber) {
      this.#subscriber.disconnect()
      this.#subscriber = null
    }

    if (this.#blockingClient) {
      this.#blockingClient.disconnect()
      this.#blockingClient = null
    }

    if (this.#client) {
      this.#client.disconnect()
      this.#client = null
    }

    this.#eventEmitter.removeAllListeners()
    this.#notifyEmitter.removeAllListeners()
    this.#eventSubscription = false
  }

  async #loadScripts (): Promise<void> {
    const scriptsDir = join(__dirname, '..', 'scripts')

    const enqueueScript = readFileSync(join(scriptsDir, 'enqueue.lua'), 'utf8')
    const completeScript = readFileSync(join(scriptsDir, 'complete.lua'), 'utf8')
    const failScript = readFileSync(join(scriptsDir, 'fail.lua'), 'utf8')
    const retryScript = readFileSync(join(scriptsDir, 'retry.lua'), 'utf8')
    const cancelScript = readFileSync(join(scriptsDir, 'cancel.lua'), 'utf8')

    const [enqueue, complete, fail, retry, cancel] = await Promise.all([
      this.#client!.script('LOAD', enqueueScript) as Promise<string>,
      this.#client!.script('LOAD', completeScript) as Promise<string>,
      this.#client!.script('LOAD', failScript) as Promise<string>,
      this.#client!.script('LOAD', retryScript) as Promise<string>,
      this.#client!.script('LOAD', cancelScript) as Promise<string>
    ])

    this.#scriptSHAs = { enqueue, complete, fail, retry, cancel }
  }

  #notifyChannelPrefix (): string {
    return `${this.#keyPrefix}notify:`
  }

  #eventsChannel (): string {
    return `${this.#keyPrefix}events`
  }

  #handlePubSubMessage (channel: string, message: string): void {
    const notifyPrefix = this.#notifyChannelPrefix()
    const eventsChannel = this.#eventsChannel()

    // Job notification (prefix:notify:jobId)
    if (channel.startsWith(notifyPrefix)) {
      const jobId = channel.substring(notifyPrefix.length)
      this.#notifyEmitter.emit(`notify:${jobId}`, message as 'completed' | 'failed')
      return
    }

    // Job events (prefix:events)
    if (channel === eventsChannel) {
      const [id, event] = message.split(':')
      this.#eventEmitter.emit('event', id, event)
    }
  }

  async enqueue (id: string, message: Buffer, timestamp: number): Promise<string | null> {
    const state = `queued:${timestamp}`
    const result = await this.#client!.evalsha(
      this.#scriptSHAs!.enqueue,
      2,
      this.#jobsKey(),
      this.#queueKey(),
      id,
      message,
      state
    )

    // If this is a new job (not duplicate), publish the queued event
    if (result === null) {
      await this.publishEvent(id, 'queued')
    }

    return result as string | null
  }

  async dequeue (workerId: string, timeout: number): Promise<Buffer | null> {
    // BLMOVE: blocking move from queue to processing queue
    // A single blocking client can handle multiple concurrent BLMOVE calls -
    // iovalkey multiplexes them and Redis queues them internally.
    const result = await this.#blockingClient!.blmove(
      this.#queueKey(),
      this.#processingKey(workerId),
      'LEFT',
      'RIGHT',
      timeout
    )
    return result ? Buffer.from(result) : null
  }

  async requeue (id: string, message: Buffer, workerId: string): Promise<void> {
    // Remove from processing queue and add to front of main queue
    await this.#client!.lrem(this.#processingKey(workerId), 1, message)
    await this.#client!.lpush(this.#queueKey(), message)
  }

  async ack (id: string, message: Buffer, workerId: string): Promise<void> {
    await this.#client!.lrem(this.#processingKey(workerId), 1, message)
  }

  async getJobState (id: string): Promise<string | null> {
    const state = await this.#client!.hget(this.#jobsKey(), id)
    return state
  }

  async setJobState (id: string, state: string): Promise<void> {
    await this.#client!.hset(this.#jobsKey(), id, state)
  }

  async deleteJob (id: string): Promise<boolean> {
    const result = await this.#client!.evalsha(
      this.#scriptSHAs!.cancel,
      1,
      this.#jobsKey(),
      id
    )
    return result === 1
  }

  async getJobStates (ids: string[]): Promise<Map<string, string | null>> {
    if (ids.length === 0) {
      return new Map()
    }

    const states = await this.#client!.hmget(this.#jobsKey(), ...ids)
    const result = new Map<string, string | null>()
    for (let i = 0; i < ids.length; i++) {
      result.set(ids[i], states[i])
    }
    return result
  }

  async setResult (id: string, result: Buffer, ttlMs: number): Promise<void> {
    await this.#client!.hset(this.#resultsKey(), id, result)
    // Note: HEXPIRE is not widely supported, so we set TTL on the whole hash
    // For production, consider using separate keys per result
  }

  async getResult (id: string): Promise<Buffer | null> {
    const result = await this.#client!.hgetBuffer(this.#resultsKey(), id)
    return result
  }

  async setError (id: string, error: Buffer, ttlMs: number): Promise<void> {
    await this.#client!.hset(this.#errorsKey(), id, error)
  }

  async getError (id: string): Promise<Buffer | null> {
    const result = await this.#client!.hgetBuffer(this.#errorsKey(), id)
    return result
  }

  async registerWorker (workerId: string, ttlMs: number): Promise<void> {
    await this.#client!.hset(this.#workersKey(), workerId, Date.now().toString())
    // Set expiry on the worker entry
    await this.#client!.pexpire(this.#workersKey(), ttlMs)
  }

  async refreshWorker (workerId: string, ttlMs: number): Promise<void> {
    await this.#client!.hset(this.#workersKey(), workerId, Date.now().toString())
    await this.#client!.pexpire(this.#workersKey(), ttlMs)
  }

  async unregisterWorker (workerId: string): Promise<void> {
    if (!this.#client) return
    await this.#client.hdel(this.#workersKey(), workerId)
    // Also clear the processing queue
    await this.#client.del(this.#processingKey(workerId))
  }

  async getWorkers (): Promise<string[]> {
    const workers = await this.#client!.hkeys(this.#workersKey())
    return workers
  }

  async getProcessingJobs (workerId: string): Promise<Buffer[]> {
    const jobs = await this.#client!.lrangeBuffer(this.#processingKey(workerId), 0, -1)
    return jobs
  }

  async subscribeToJob (
    id: string,
    handler: (status: 'completed' | 'failed') => void
  ): Promise<() => Promise<void>> {
    const eventName = `notify:${id}`
    this.#notifyEmitter.on(eventName, handler)

    // Subscribe to the job notification channel
    const channel = `${this.#notifyChannelPrefix()}${id}`
    await this.#subscriber!.subscribe(channel)

    return async () => {
      this.#notifyEmitter.off(eventName, handler)
      await this.#subscriber!.unsubscribe(channel)
    }
  }

  async notifyJobComplete (id: string, status: 'completed' | 'failed'): Promise<void> {
    const channel = `${this.#notifyChannelPrefix()}${id}`
    await this.#client!.publish(channel, status)
  }

  async subscribeToEvents (
    handler: (id: string, event: string) => void
  ): Promise<() => Promise<void>> {
    this.#eventEmitter.on('event', handler)

    if (!this.#eventSubscription) {
      await this.#subscriber!.subscribe(this.#eventsChannel())
      this.#eventSubscription = true
    }

    return async () => {
      this.#eventEmitter.off('event', handler)
    }
  }

  async publishEvent (id: string, event: string): Promise<void> {
    await this.#client!.publish(this.#eventsChannel(), `${id}:${event}`)
  }

  async completeJob (
    id: string,
    message: Buffer,
    workerId: string,
    result: Buffer,
    resultTtlMs: number
  ): Promise<void> {
    const timestamp = Date.now()
    const state = `completed:${timestamp}`

    await this.#client!.evalsha(
      this.#scriptSHAs!.complete,
      3,
      this.#jobsKey(),
      this.#resultsKey(),
      this.#processingKey(workerId),
      id,
      message,
      state,
      result,
      resultTtlMs.toString()
    )

    // Notify subscribers and publish event
    await this.notifyJobComplete(id, 'completed')
    await this.publishEvent(id, 'completed')
  }

  async failJob (
    id: string,
    message: Buffer,
    workerId: string,
    error: Buffer,
    errorTtlMs: number
  ): Promise<void> {
    const timestamp = Date.now()
    const state = `failed:${timestamp}`

    await this.#client!.evalsha(
      this.#scriptSHAs!.fail,
      3,
      this.#jobsKey(),
      this.#errorsKey(),
      this.#processingKey(workerId),
      id,
      message,
      state,
      error,
      errorTtlMs.toString()
    )

    // Notify subscribers and publish event
    await this.notifyJobComplete(id, 'failed')
    await this.publishEvent(id, 'failed')
  }

  async retryJob (
    id: string,
    message: Buffer,
    workerId: string,
    attempts: number
  ): Promise<void> {
    const timestamp = Date.now()
    const state = `failing:${timestamp}:${attempts}`

    // Get the old message from processing queue to remove it
    const processingJobs = await this.getProcessingJobs(workerId)
    let oldMessage: Buffer | null = null

    // Find the message with matching job id
    for (const job of processingJobs) {
      try {
        const parsed = JSON.parse(job.toString())
        if (parsed.id === id) {
          oldMessage = job
          break
        }
      } catch {
        // Ignore parse errors
      }
    }

    if (oldMessage) {
      await this.#client!.evalsha(
        this.#scriptSHAs!.retry,
        3,
        this.#jobsKey(),
        this.#queueKey(),
        this.#processingKey(workerId),
        id,
        message,
        oldMessage,
        state
      )
    } else {
      // Fallback: just set state and push new message
      await this.setJobState(id, state)
      await this.#client!.lpush(this.#queueKey(), message)
      await this.publishEvent(id, 'failing')
    }
  }

  /**
   * Clear all data (useful for testing)
   */
  async clear (): Promise<void> {
    // If not connected, nothing to clear
    if (!this.#client) {
      return
    }

    const keys = await this.#client.keys(`${this.#keyPrefix}*`)
    if (keys.length > 0) {
      await this.#client.del(...keys)
    }
  }
}
