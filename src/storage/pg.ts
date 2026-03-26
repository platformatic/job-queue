import { EventEmitter } from 'node:events'
import type { Logger } from 'pino'
import type { Storage } from './types.ts'
import { loadOptionalDependency } from './utils.ts'
import { abstractLogger } from '../utils/logging.ts'

// Minimal type definitions for pg (optional dependency)
interface PgPoolConfig {
  connectionString?: string
  max?: number
  application_name?: string
  [key: string]: unknown
}

interface PgQueryResult {
  rows: Record<string, unknown>[]
  rowCount: number | null
}

interface PgPool {
  query (text: string, values?: unknown[]): Promise<PgQueryResult>
  connect (): Promise<PgPoolClient>
  end (): Promise<void>
  on (event: string, handler: (...args: unknown[]) => void): void
}

interface PgPoolClient {
  query (text: string, values?: unknown[]): Promise<PgQueryResult>
  release (destroy?: boolean): void
}

interface PgClient {
  query (text: string, values?: unknown[]): Promise<PgQueryResult>
  connect (): Promise<void>
  end (): Promise<void>
  on (event: string, handler: (...args: any[]) => void): void
  off (event: string, handler: (...args: any[]) => void): void
}

interface PgNotification {
  channel: string
  payload?: string
}

interface PgModule {
  Pool: new (config: PgPoolConfig) => PgPool
  Client: new (config: PgPoolConfig) => PgClient
}

interface PgStorageConfig {
  connectionString?: string
  tablePrefix?: string
  logger?: Logger
}

interface DequeueWaiter {
  workerId: string
  resolve: (value: Buffer | null) => void
  timeoutId: ReturnType<typeof setTimeout>
}

/**
 * PostgreSQL storage implementation
 *
 * Uses SELECT FOR UPDATE SKIP LOCKED for concurrent dequeue (pg-boss pattern)
 * and LISTEN/NOTIFY for pub/sub notifications.
 *
 * Connection architecture:
 * - Pool: for all queries (enqueue, state, results, etc.)
 * - Dedicated listener client: stays in LISTEN mode for pub/sub
 */
export class PgStorage implements Storage {
  #connectionString: string
  #tablePrefix: string
  #pool: PgPool | null = null
  #listener: PgClient | null = null
  #eventEmitter = new EventEmitter({ captureRejections: true })
  #notifyEmitter = new EventEmitter({ captureRejections: true })
  #eventSubscription: boolean = false
  #logger: Logger
  #dequeueWaiters: DequeueWaiter[] = []

  // Namespace support
  #parentStorage: PgStorage | null = null
  #refCount: number = 0

  // Notification handler (stored for removal on disconnect)
  #notificationHandler: ((msg: PgNotification) => void) | null = null

  // Table names (computed from prefix)
  #jobsTable: string
  #queueTable: string
  #processingTable: string
  #resultsTable: string
  #errorsTable: string
  #workersTable: string
  #locksTable: string

  // Channel names
  #notifyChannel: string
  #eventsChannel: string
  #newJobChannel: string

  constructor (config: PgStorageConfig = {}) {
    this.#connectionString =
      config.connectionString ?? process.env.DATABASE_URL ?? 'postgresql://localhost:5432/job_queue'
    this.#tablePrefix = config.tablePrefix ?? 'jq_'
    this.#logger = (config.logger ?? abstractLogger).child({ component: 'pg-storage', tablePrefix: this.#tablePrefix })

    // Compute table names
    this.#jobsTable = `${this.#tablePrefix}jobs`
    this.#queueTable = `${this.#tablePrefix}queue`
    this.#processingTable = `${this.#tablePrefix}processing`
    this.#resultsTable = `${this.#tablePrefix}results`
    this.#errorsTable = `${this.#tablePrefix}errors`
    this.#workersTable = `${this.#tablePrefix}workers`
    this.#locksTable = `${this.#tablePrefix}locks`

    // Compute channel names (underscores only, safe for LISTEN)
    this.#notifyChannel = `${this.#tablePrefix}notify`
    this.#eventsChannel = `${this.#tablePrefix}events`
    this.#newJobChannel = `${this.#tablePrefix}new_job`

    this.#eventEmitter.setMaxListeners(0)
    this.#notifyEmitter.setMaxListeners(0)
  }

  // ═══════════════════════════════════════════════════════════════════
  // LIFECYCLE
  // ═══════════════════════════════════════════════════════════════════

  async connect (): Promise<void> {
    // Namespace view: connect parent and share pool/listener
    if (this.#parentStorage) {
      if (this.#pool) return // already connected
      this.#parentStorage.#refCount++
      await this.#parentStorage.connect()

      this.#pool = this.#parentStorage.#pool
      this.#listener = this.#parentStorage.#listener

      // Create own tables
      await this.#createSchema()

      // Register own notification handler on shared listener
      this.#notificationHandler = (msg: PgNotification) => {
        this.#handleNotification(msg)
      }
      this.#listener!.on('notification', this.#notificationHandler)

      // LISTEN on own channels
      await this.#listenChannels()
      return
    }

    // Root instance: idempotent connect (no ref count for direct callers)
    if (this.#pool) return

    const pgModule = await loadOptionalDependency<PgModule>('pg', 'PgStorage')

    this.#pool = new pgModule.Pool({
      connectionString: this.#connectionString,
      application_name: 'job-queue'
    })

    this.#pool.on('error', (err: unknown) => {
      this.#logger.error({ err }, 'Pool error.')
    })

    this.#listener = new pgModule.Client({
      connectionString: this.#connectionString,
      application_name: 'job-queue-listener'
    })
    await this.#listener.connect()

    // Create tables
    await this.#createSchema()

    // Set up notification handler
    this.#notificationHandler = (msg: PgNotification) => {
      this.#handleNotification(msg)
    }
    this.#listener.on('notification', this.#notificationHandler)

    // LISTEN on channels
    await this.#listenChannels()
  }

  async disconnect (): Promise<void> {
    // Namespace view
    if (this.#parentStorage) {
      // UNLISTEN own channels
      if (this.#listener) {
        await this.#listener.query(`UNLISTEN "${this.#notifyChannel}"`).catch(() => {})
        await this.#listener.query(`UNLISTEN "${this.#eventsChannel}"`).catch(() => {})
        await this.#listener.query(`UNLISTEN "${this.#newJobChannel}"`).catch(() => {})

        if (this.#notificationHandler) {
          this.#listener.off('notification', this.#notificationHandler)
          this.#notificationHandler = null
        }
      }

      this.#clearDequeueWaiters()
      this.#pool = null
      this.#listener = null

      this.#eventEmitter.removeAllListeners()
      this.#notifyEmitter.removeAllListeners()
      this.#eventSubscription = false

      this.#parentStorage.#refCount--
      return
    }

    // Root instance: only destroy when no namespace children are connected
    if (this.#refCount > 0) return

    if (this.#listener) {
      if (this.#notificationHandler) {
        this.#listener.off('notification', this.#notificationHandler)
        this.#notificationHandler = null
      }
      await this.#listener.end().catch(() => {})
      this.#listener = null
    }

    this.#clearDequeueWaiters()

    if (this.#pool) {
      await this.#pool.end().catch(() => {})
      this.#pool = null
    }

    this.#eventEmitter.removeAllListeners()
    this.#notifyEmitter.removeAllListeners()
    this.#eventSubscription = false
  }

  async #createSchema (): Promise<void> {
    await this.#pool!.query(`
      CREATE TABLE IF NOT EXISTS "${this.#jobsTable}" (
        id TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        expires_at BIGINT
      )
    `)

    await this.#pool!.query(`
      CREATE TABLE IF NOT EXISTS "${this.#queueTable}" (
        seq BIGSERIAL PRIMARY KEY,
        message BYTEA NOT NULL
      )
    `)

    await this.#pool!.query(`
      CREATE TABLE IF NOT EXISTS "${this.#processingTable}" (
        seq BIGSERIAL PRIMARY KEY,
        worker_id TEXT NOT NULL,
        message BYTEA NOT NULL
      )
    `)

    // Index for worker lookups on processing table
    await this.#pool!.query(`
      CREATE INDEX IF NOT EXISTS "${this.#processingTable}_worker_idx"
      ON "${this.#processingTable}" (worker_id)
    `)

    await this.#pool!.query(`
      CREATE TABLE IF NOT EXISTS "${this.#resultsTable}" (
        id TEXT PRIMARY KEY,
        data BYTEA NOT NULL,
        expires_at BIGINT NOT NULL
      )
    `)

    await this.#pool!.query(`
      CREATE TABLE IF NOT EXISTS "${this.#errorsTable}" (
        id TEXT PRIMARY KEY,
        data BYTEA NOT NULL,
        expires_at BIGINT NOT NULL
      )
    `)

    await this.#pool!.query(`
      CREATE TABLE IF NOT EXISTS "${this.#workersTable}" (
        worker_id TEXT PRIMARY KEY,
        expires_at BIGINT NOT NULL
      )
    `)

    await this.#pool!.query(`
      CREATE TABLE IF NOT EXISTS "${this.#locksTable}" (
        lock_key TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        expires_at BIGINT NOT NULL
      )
    `)
  }

  async #listenChannels (): Promise<void> {
    await this.#listener!.query(`LISTEN "${this.#notifyChannel}"`)
    await this.#listener!.query(`LISTEN "${this.#eventsChannel}"`)
    await this.#listener!.query(`LISTEN "${this.#newJobChannel}"`)
  }

  #handleNotification (msg: PgNotification): void {
    if (msg.channel === this.#newJobChannel) {
      this.#notifyDequeueWaiters()
      return
    }

    if (msg.channel === this.#notifyChannel && msg.payload) {
      // payload format: jobId:status
      const sepIdx = msg.payload.indexOf(':')
      if (sepIdx > 0) {
        const jobId = msg.payload.substring(0, sepIdx)
        const status = msg.payload.substring(sepIdx + 1)
        this.#notifyEmitter.emit(`notify:${jobId}`, status as 'completed' | 'failed' | 'failing')
      }
      return
    }

    if (msg.channel === this.#eventsChannel && msg.payload) {
      // payload format: id:event
      const sepIdx = msg.payload.indexOf(':')
      if (sepIdx > 0) {
        const id = msg.payload.substring(0, sepIdx)
        const event = msg.payload.substring(sepIdx + 1)
        this.#eventEmitter.emit('event', id, event)
      }
    }
  }

  #clearDequeueWaiters (): void {
    for (const waiter of this.#dequeueWaiters) {
      clearTimeout(waiter.timeoutId)
      waiter.resolve(null)
    }
    this.#dequeueWaiters = []
  }

  #notifyDequeueWaiters (): void {
    // Try to fulfill waiting dequeue calls
    // Each waiter will do its own SELECT FOR UPDATE SKIP LOCKED,
    // so concurrent fulfillment is safe
    const waiters = this.#dequeueWaiters.splice(0)
    for (const waiter of waiters) {
      this.#tryDequeue(waiter.workerId)
        .then(result => {
          clearTimeout(waiter.timeoutId)
          if (result) {
            waiter.resolve(result)
          } else {
            // No job available, put waiter back
            this.#dequeueWaiters.push(waiter)
          }
        })
        .catch(() => {
          // On error, put waiter back
          this.#dequeueWaiters.push(waiter)
        })
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // QUEUE OPERATIONS
  // ═══════════════════════════════════════════════════════════════════

  async enqueue (id: string, message: Buffer, timestamp: number): Promise<string | null> {
    const state = `queued:${timestamp}`
    const now = Date.now()

    const client = await this.#pool!.connect()
    try {
      await client.query('BEGIN')

      // Check for existing job (with dedup expiry handling)
      const existing = await client.query(
        `SELECT state, expires_at FROM "${this.#jobsTable}" WHERE id = $1 FOR UPDATE`,
        [id]
      )

      if (existing.rows.length > 0) {
        const row = existing.rows[0]
        const expiresAt = row.expires_at as number | null
        if (expiresAt && now >= expiresAt) {
          // Expired: remove and fall through to enqueue as new
          await client.query(`DELETE FROM "${this.#jobsTable}" WHERE id = $1`, [id])
        } else {
          await client.query('COMMIT')
          return row.state as string
        }
      }

      // Insert new job
      await client.query(`INSERT INTO "${this.#jobsTable}" (id, state) VALUES ($1, $2)`, [id, state])

      // Push to queue
      await client.query(`INSERT INTO "${this.#queueTable}" (message) VALUES ($1)`, [message])

      await client.query('COMMIT')

      // Publish event and notify (outside transaction so NOTIFY fires immediately)
      await this.publishEvent(id, 'queued')
      await this.#pool!.query("SELECT pg_notify($1, '')", [this.#newJobChannel])

      return null
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  async dequeue (workerId: string, timeout: number): Promise<Buffer | null> {
    // Try immediate fetch
    const result = await this.#tryDequeue(workerId)
    if (result) return result

    // Wait for NOTIFY or timeout
    return new Promise<Buffer | null>(resolve => {
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

  async #tryDequeue (workerId: string): Promise<Buffer | null> {
    // Atomic: delete from queue (SKIP LOCKED) + insert into processing
    const result = await this.#pool!.query(
      `
      WITH deleted AS (
        DELETE FROM "${this.#queueTable}"
        WHERE seq = (
          SELECT seq FROM "${this.#queueTable}"
          ORDER BY seq
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        )
        RETURNING message
      )
      INSERT INTO "${this.#processingTable}" (worker_id, message)
      SELECT $1, message FROM deleted
      RETURNING message
    `,
      [workerId]
    )

    if (result.rows.length === 0) return null
    return result.rows[0].message as Buffer
  }

  async requeue (id: string, message: Buffer, workerId: string): Promise<void> {
    const client = await this.#pool!.connect()
    try {
      await client.query('BEGIN')

      // Remove from processing queue
      await client.query(`DELETE FROM "${this.#processingTable}" WHERE worker_id = $1 AND message = $2`, [
        workerId,
        message
      ])

      // Add to front of main queue (use seq = 0 trick won't work since BIGSERIAL auto-increments)
      // Just insert normally - ordering is by seq which is auto-increment
      await client.query(`INSERT INTO "${this.#queueTable}" (message) VALUES ($1)`, [message])

      await client.query('COMMIT')

      // Notify waiters
      await this.#pool!.query("SELECT pg_notify($1, '')", [this.#newJobChannel])
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  async ack (id: string, message: Buffer, workerId: string): Promise<void> {
    await this.#pool!.query(`DELETE FROM "${this.#processingTable}" WHERE worker_id = $1 AND message = $2`, [
      workerId,
      message
    ])
  }

  // ═══════════════════════════════════════════════════════════════════
  // JOB STATE
  // ═══════════════════════════════════════════════════════════════════

  async getJobState (id: string): Promise<string | null> {
    const result = await this.#pool!.query(`SELECT state, expires_at FROM "${this.#jobsTable}" WHERE id = $1`, [id])

    if (result.rows.length === 0) return null

    const row = result.rows[0]
    const expiresAt = row.expires_at as number | null
    if (expiresAt && Date.now() >= expiresAt) {
      await this.#pool!.query(`DELETE FROM "${this.#jobsTable}" WHERE id = $1`, [id])
      return null
    }

    return row.state as string
  }

  async setJobState (id: string, state: string): Promise<void> {
    await this.#pool!.query(`UPDATE "${this.#jobsTable}" SET state = $2 WHERE id = $1`, [id, state])
  }

  async deleteJob (id: string): Promise<boolean> {
    const result = await this.#pool!.query(`DELETE FROM "${this.#jobsTable}" WHERE id = $1`, [id])

    if (result.rowCount && result.rowCount > 0) {
      await this.publishEvent(id, 'cancelled')
      return true
    }
    return false
  }

  async getJobStates (ids: string[]): Promise<Map<string, string | null>> {
    const result = new Map<string, string | null>()
    if (ids.length === 0) return result

    const rows = await this.#pool!.query(`SELECT id, state, expires_at FROM "${this.#jobsTable}" WHERE id = ANY($1)`, [
      ids
    ])

    const now = Date.now()
    const found = new Set<string>()
    const expiredIds: string[] = []

    for (const row of rows.rows) {
      const id = row.id as string
      const expiresAt = row.expires_at as number | null
      found.add(id)

      if (expiresAt && now >= expiresAt) {
        expiredIds.push(id)
        result.set(id, null)
      } else {
        result.set(id, row.state as string)
      }
    }

    // Clean up expired entries
    if (expiredIds.length > 0) {
      await this.#pool!.query(`DELETE FROM "${this.#jobsTable}" WHERE id = ANY($1)`, [expiredIds])
    }

    // Set null for IDs not found
    for (const id of ids) {
      if (!found.has(id)) {
        result.set(id, null)
      }
    }

    return result
  }

  async setJobExpiry (id: string, ttlMs: number): Promise<void> {
    const expiresAt = Date.now() + ttlMs
    await this.#pool!.query(`UPDATE "${this.#jobsTable}" SET expires_at = $2 WHERE id = $1`, [id, expiresAt])
  }

  // ═══════════════════════════════════════════════════════════════════
  // RESULTS
  // ═══════════════════════════════════════════════════════════════════

  async setResult (id: string, result: Buffer, ttlMs: number): Promise<void> {
    const expiresAt = Date.now() + ttlMs
    await this.#pool!.query(
      `INSERT INTO "${this.#resultsTable}" (id, data, expires_at)
       VALUES ($1, $2, $3)
       ON CONFLICT (id) DO UPDATE SET data = $2, expires_at = $3`,
      [id, result, expiresAt]
    )
  }

  async getResult (id: string): Promise<Buffer | null> {
    const result = await this.#pool!.query(`SELECT data, expires_at FROM "${this.#resultsTable}" WHERE id = $1`, [id])

    if (result.rows.length === 0) return null

    const row = result.rows[0]
    if (Date.now() > (row.expires_at as number)) {
      await this.#pool!.query(`DELETE FROM "${this.#resultsTable}" WHERE id = $1`, [id])
      return null
    }

    return row.data as Buffer
  }

  async setError (id: string, error: Buffer, ttlMs: number): Promise<void> {
    const expiresAt = Date.now() + ttlMs
    await this.#pool!.query(
      `INSERT INTO "${this.#errorsTable}" (id, data, expires_at)
       VALUES ($1, $2, $3)
       ON CONFLICT (id) DO UPDATE SET data = $2, expires_at = $3`,
      [id, error, expiresAt]
    )
  }

  async getError (id: string): Promise<Buffer | null> {
    const result = await this.#pool!.query(`SELECT data, expires_at FROM "${this.#errorsTable}" WHERE id = $1`, [id])

    if (result.rows.length === 0) return null

    const row = result.rows[0]
    if (Date.now() > (row.expires_at as number)) {
      await this.#pool!.query(`DELETE FROM "${this.#errorsTable}" WHERE id = $1`, [id])
      return null
    }

    return row.data as Buffer
  }

  // ═══════════════════════════════════════════════════════════════════
  // WORKERS
  // ═══════════════════════════════════════════════════════════════════

  async registerWorker (workerId: string, ttlMs: number): Promise<void> {
    const expiresAt = Date.now() + ttlMs
    await this.#pool!.query(
      `INSERT INTO "${this.#workersTable}" (worker_id, expires_at)
       VALUES ($1, $2)
       ON CONFLICT (worker_id) DO UPDATE SET expires_at = $2`,
      [workerId, expiresAt]
    )
  }

  async refreshWorker (workerId: string, ttlMs: number): Promise<void> {
    await this.registerWorker(workerId, ttlMs)
  }

  async unregisterWorker (workerId: string): Promise<void> {
    if (!this.#pool) return
    await this.#pool.query(`DELETE FROM "${this.#workersTable}" WHERE worker_id = $1`, [workerId])
    await this.#pool.query(`DELETE FROM "${this.#processingTable}" WHERE worker_id = $1`, [workerId])
  }

  async getWorkers (): Promise<string[]> {
    const result = await this.#pool!.query(`SELECT worker_id FROM "${this.#workersTable}" WHERE expires_at > $1`, [
      Date.now()
    ])
    return result.rows.map(row => row.worker_id as string)
  }

  async getProcessingJobs (workerId: string): Promise<Buffer[]> {
    const result = await this.#pool!.query(`SELECT message FROM "${this.#processingTable}" WHERE worker_id = $1`, [
      workerId
    ])
    return result.rows.map(row => row.message as Buffer)
  }

  // ═══════════════════════════════════════════════════════════════════
  // NOTIFICATIONS (for request/response)
  // ═══════════════════════════════════════════════════════════════════

  async subscribeToJob (
    id: string,
    handler: (status: 'completed' | 'failed' | 'failing') => void
  ): Promise<() => Promise<void>> {
    const eventName = `notify:${id}`
    this.#notifyEmitter.on(eventName, handler)

    return async () => {
      this.#notifyEmitter.off(eventName, handler)
    }
  }

  async notifyJobComplete (id: string, status: 'completed' | 'failed' | 'failing'): Promise<void> {
    await this.#pool!.query('SELECT pg_notify($1, $2)', [this.#notifyChannel, `${id}:${status}`])
  }

  // ═══════════════════════════════════════════════════════════════════
  // EVENTS (for monitoring/reaper)
  // ═══════════════════════════════════════════════════════════════════

  async subscribeToEvents (handler: (id: string, event: string) => void): Promise<() => Promise<void>> {
    this.#eventEmitter.on('event', handler)
    this.#eventSubscription = true

    return async () => {
      this.#eventEmitter.off('event', handler)
    }
  }

  async publishEvent (id: string, event: string): Promise<void> {
    await this.#pool!.query('SELECT pg_notify($1, $2)', [this.#eventsChannel, `${id}:${event}`])
  }

  // ═══════════════════════════════════════════════════════════════════
  // ATOMIC OPERATIONS
  // ═══════════════════════════════════════════════════════════════════

  async completeJob (id: string, message: Buffer, workerId: string, result: Buffer, resultTTL: number): Promise<void> {
    const timestamp = Date.now()
    const state = `completed:${timestamp}`
    const expiresAt = timestamp + resultTTL

    const client = await this.#pool!.connect()
    try {
      await client.query('BEGIN')

      // Set state to completed + dedup expiry
      await client.query(`UPDATE "${this.#jobsTable}" SET state = $2, expires_at = $3 WHERE id = $1`, [
        id,
        state,
        expiresAt
      ])

      // Store result
      await client.query(
        `INSERT INTO "${this.#resultsTable}" (id, data, expires_at)
         VALUES ($1, $2, $3)
         ON CONFLICT (id) DO UPDATE SET data = $2, expires_at = $3`,
        [id, result, expiresAt]
      )

      // Remove from processing queue
      await client.query(`DELETE FROM "${this.#processingTable}" WHERE worker_id = $1 AND message = $2`, [
        workerId,
        message
      ])

      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }

    // Notify after commit (NOTIFY is transactional, but we do it separately for the emitter)
    await this.notifyJobComplete(id, 'completed')
    await this.publishEvent(id, 'completed')
  }

  async failJob (id: string, message: Buffer, workerId: string, error: Buffer, errorTTL: number): Promise<void> {
    const timestamp = Date.now()
    const state = `failed:${timestamp}`
    const expiresAt = timestamp + errorTTL

    const client = await this.#pool!.connect()
    try {
      await client.query('BEGIN')

      // Set state to failed + dedup expiry
      await client.query(`UPDATE "${this.#jobsTable}" SET state = $2, expires_at = $3 WHERE id = $1`, [
        id,
        state,
        expiresAt
      ])

      // Store error
      await client.query(
        `INSERT INTO "${this.#errorsTable}" (id, data, expires_at)
         VALUES ($1, $2, $3)
         ON CONFLICT (id) DO UPDATE SET data = $2, expires_at = $3`,
        [id, error, expiresAt]
      )

      // Remove from processing queue
      await client.query(`DELETE FROM "${this.#processingTable}" WHERE worker_id = $1 AND message = $2`, [
        workerId,
        message
      ])

      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }

    // Notify after commit
    await this.notifyJobComplete(id, 'failed')
    await this.publishEvent(id, 'failed')
  }

  async retryJob (id: string, message: Buffer, workerId: string, attempts: number): Promise<void> {
    const timestamp = Date.now()
    const state = `failing:${timestamp}:${attempts}`

    // Get the old message from processing queue
    const processingJobs = await this.getProcessingJobs(workerId)
    let oldMessage: Buffer | null = null

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

    const client = await this.#pool!.connect()
    try {
      await client.query('BEGIN')

      // Set state to failing
      await client.query(`UPDATE "${this.#jobsTable}" SET state = $2 WHERE id = $1`, [id, state])

      // Remove old message from processing queue
      if (oldMessage) {
        await client.query(`DELETE FROM "${this.#processingTable}" WHERE worker_id = $1 AND message = $2`, [
          workerId,
          oldMessage
        ])
      }

      // Add new message to queue
      await client.query(`INSERT INTO "${this.#queueTable}" (message) VALUES ($1)`, [message])

      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }

    // Notify after commit
    await this.notifyJobComplete(id, 'failing')
    await this.publishEvent(id, 'failing')
    await this.#pool!.query("SELECT pg_notify($1, '')", [this.#newJobChannel])
  }

  // ═══════════════════════════════════════════════════════════════════
  // LEADER ELECTION
  // ═══════════════════════════════════════════════════════════════════

  async acquireLeaderLock (lockKey: string, ownerId: string, ttlMs: number): Promise<boolean> {
    const expiresAt = Date.now() + ttlMs

    // Try to insert, or take over if expired
    const result = await this.#pool!.query(
      `INSERT INTO "${this.#locksTable}" (lock_key, owner_id, expires_at)
       VALUES ($1, $2, $3)
       ON CONFLICT (lock_key) DO UPDATE
         SET owner_id = $2, expires_at = $3
         WHERE "${this.#locksTable}".expires_at < $4
       RETURNING lock_key`,
      [lockKey, ownerId, expiresAt, Date.now()]
    )

    return result.rows.length > 0
  }

  async renewLeaderLock (lockKey: string, ownerId: string, ttlMs: number): Promise<boolean> {
    const expiresAt = Date.now() + ttlMs
    const result = await this.#pool!.query(
      `UPDATE "${this.#locksTable}" SET expires_at = $3 WHERE lock_key = $1 AND owner_id = $2`,
      [lockKey, ownerId, expiresAt]
    )
    return result.rowCount !== null && result.rowCount > 0
  }

  async releaseLeaderLock (lockKey: string, ownerId: string): Promise<boolean> {
    if (!this.#pool) return false
    const result = await this.#pool.query(`DELETE FROM "${this.#locksTable}" WHERE lock_key = $1 AND owner_id = $2`, [
      lockKey,
      ownerId
    ])
    return result.rowCount !== null && result.rowCount > 0
  }

  // ═══════════════════════════════════════════════════════════════════
  // NAMESPACE
  // ═══════════════════════════════════════════════════════════════════

  createNamespace (name: string): Storage {
    const root = this.#parentStorage ?? this
    const ns = new PgStorage({
      connectionString: root.#connectionString,
      tablePrefix: `${this.#tablePrefix}${name}_`,
      logger: this.#logger
    })
    ns.#parentStorage = root
    return ns
  }

  /**
   * Clear all data (useful for testing)
   */
  async clear (): Promise<void> {
    if (!this.#pool) return

    await this.#pool.query(`TRUNCATE "${this.#queueTable}" RESTART IDENTITY`)
    await this.#pool.query(`TRUNCATE "${this.#processingTable}" RESTART IDENTITY`)
    await this.#pool.query(`DELETE FROM "${this.#jobsTable}"`)
    await this.#pool.query(`DELETE FROM "${this.#resultsTable}"`)
    await this.#pool.query(`DELETE FROM "${this.#errorsTable}"`)
    await this.#pool.query(`DELETE FROM "${this.#workersTable}"`)
    await this.#pool.query(`DELETE FROM "${this.#locksTable}"`)
  }
}
