import { EventEmitter } from 'node:events'
import { randomUUID } from 'node:crypto'
import { DatabaseSync, type StatementSync } from 'node:sqlite'
import type { Logger } from 'pino'
import type { Storage } from './types.ts'
import { abstractLogger } from '../utils/logging.ts'
import { StorageError } from '../errors.ts'

const SCHEMA_VERSION = 1

const CLEANUP_LOCK_KEY = 'cleanup-leader'
const CLEANUP_LOCK_TTL_MS = 10_000
const CLEANUP_ACQUIRE_RETRY_MS = 5_000

// busy_timeout stays at 100ms (Rails 8 default is 5000ms). With node:sqlite's
// synchronous API, a multi-second busy_timeout would stall the Node event loop
// under writer contention; ED1 in the design review pairs this short timeout
// with an in-process write mutex + jittered retry.
const DEFAULT_BUSY_TIMEOUT_MS = 100
const DEFAULT_CLEANUP_INTERVAL_MS = 30_000
const DEFAULT_VACUUM_INTERVAL_MS = 24 * 60 * 60 * 1000
// Caps the on-disk size of the WAL file after checkpoint. Without it, the WAL
// can grow unbounded between checkpoints.
const DEFAULT_JOURNAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024
// Memory-mapped I/O for SQLite reads.
const DEFAULT_MMAP_SIZE_BYTES = 128 * 1024 * 1024
// Page cache size in number of pages (positive value).
// With the SQLite 4 KiB default page size this is ~8 MiB.
const DEFAULT_CACHE_SIZE_PAGES = 2000

const DEFAULT_PRAGMAS: Record<string, string | number> = {
  // Must be set before the first table is created, so it comes first. Lets the
  // maintenance loop hand freed pages back to the OS with incremental_vacuum,
  // which only touches the freelist, instead of a full VACUUM that rewrites
  // the whole file and blocks the event loop for as long as that takes.
  auto_vacuum: 'INCREMENTAL',
  journal_mode: 'WAL',
  synchronous: 'NORMAL',
  busy_timeout: DEFAULT_BUSY_TIMEOUT_MS,
  journal_size_limit: DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
  mmap_size: DEFAULT_MMAP_SIZE_BYTES,
  cache_size: DEFAULT_CACHE_SIZE_PAGES,
  temp_store: 'MEMORY'
}

// Chunk size for IN-list queries; well under SQLITE_MAX_VARIABLE_NUMBER
// (32766 by default) so bulk calls never hit the bound-parameter limit.
const MAX_BIND_PARAMS_PER_QUERY = 500

// A file-backed database is owned by one live SQLiteStorage root at a time.
// The owner refreshes its heartbeat in <prefix>meta; a claim older than
// OWNER_STALE_MS is treated as abandoned even if its pid has been reused.
const OWNER_META_KEY = 'owner'
const OWNER_HEARTBEAT_MS = 5_000
const OWNER_STALE_MS = 15_000

// Instance ids of roots in this process that currently own a database. Lets a
// claim left behind by this same pid (e.g. pid 1 after a container restart) be
// told apart from a sibling instance that is still connected.
const liveOwnerInstances = new Set<string>()

const WRITE_RETRY_BASE_MS = 5
const WRITE_RETRY_MAX_MS = 50
const WRITE_RETRY_ATTEMPTS = 5

interface DequeueWaiter {
  workerId: string
  resolve: (value: Buffer | null) => void
  reject: (err: unknown) => void
  timeoutId: ReturnType<typeof setTimeout>
  // True while a #tryDequeue attempt for this waiter is in flight; prevents
  // double-dispatch and tells the timeout callback to defer settling.
  inflight: boolean
  // Set by the timeout while an attempt is in flight; the attempt's completion
  // settles the promise instead of the timeout.
  timedOut: boolean
  // A new job was announced while an attempt was in flight. That attempt may
  // already have looked at the queue, so try again once it comes back empty.
  renotify: boolean
  // Set by disconnect(). The promise has been settled with null; attempts
  // that haven't run yet must not claim anything for this waiter.
  abandoned: boolean
  // The job an attempt claimed for this waiter, recorded in the same
  // synchronous transaction that moved it to processing and cleared once it is
  // handed over. disconnect() puts it back in the queue if it is still here.
  claimed: { jobId: string; message: Buffer } | null
}

interface VacuumOption {
  enabled: boolean
  intervalMs: number
}

interface SQLiteStorageConfig {
  /**
   * Database path. Use ':memory:' for an in-memory database (default).
   * For persistence pass an explicit filesystem path. No silent cwd writes.
   */
  path?: string

  /**
   * Table name prefix.
   * Default: 'jq_'.
   */
  tablePrefix?: string

  /**
   * Background cleanup interval in milliseconds. Pass false to disable
   * (useful if you run cleanup externally via SQL).
   * Default: 30000.
   */
  cleanupIntervalMs?: number | false

  /**
   * Periodic maintenance cadence: `PRAGMA optimize` plus `PRAGMA incremental_vacuum`,
   * which returns pages freed by cleanup to the OS without rewriting the file.
   * Pass false to disable.
   * Default: { enabled: true, intervalMs: 24 * 60 * 60 * 1000 }.
   */
  vacuum?: VacuumOption | false

  /**
   * PRAGMA overrides merged over the defaults.
   * Useful for tuning busy_timeout, cache_size, mmap_size, etc.
   */
  pragmas?: Record<string, string | number>

  /**
   * Pino logger for structured operational log lines.
   */
  logger?: Logger
}

function toBuffer (value: unknown): Buffer {
  if (Buffer.isBuffer(value)) return value
  if (value instanceof Uint8Array) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength)
  }
  if (typeof value === 'string') return Buffer.from(value)
  throw new StorageError(`SQLiteStorage: expected Buffer/Uint8Array, got ${typeof value}`)
}

function isProcessAlive (pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch (err) {
    // EPERM: the process exists but belongs to another user.
    return (err as NodeJS.ErrnoException).code === 'EPERM'
  }
}

function sleep (ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// tablePrefix and namespace names are interpolated into SQL identifiers (bind
// parameters cannot name tables), and node:sqlite's exec() runs multiple
// statements — a hostile name could break out of the quoted identifier and
// execute arbitrary SQL. Allowlist the characters instead of escaping.
const SAFE_IDENTIFIER_RE = /^[A-Za-z0-9_.:-]+$/

function assertSafeIdentifier (value: string, what: string): void {
  if (!SAFE_IDENTIFIER_RE.test(value)) {
    throw new StorageError(
      `SQLiteStorage: ${what} '${value}' contains characters that are not allowed ` +
        'in SQL table names. Use only letters, digits, and _ . : -'
    )
  }
}

/**
 * SQLite storage implementation.
 *
 * Single-process only. Uses node:sqlite (built in since Node 22.5) with WAL mode.
 * Cross-process use is intentionally unsupported: every call asserts that the
 * caller's process.pid matches the pid at connect(), and throws StorageError
 * otherwise. For multi-process queues, use PgStorage.
 *
 * Notification semantics: subscribeToJob handlers must run in the same process
 * as the publisher. SQLite has no LISTEN/NOTIFY equivalent; we use an in-process
 * EventEmitter and document the contract explicitly.
 *
 * Atomic dequeue uses BEGIN IMMEDIATE; in-process writers are serialized through
 * an async mutex to avoid event-loop stalls when contention would otherwise let
 * busy_timeout block the synchronous driver.
 */
export class SQLiteStorage implements Storage {
  #path: string
  #tablePrefix: string
  #cleanupIntervalMs: number | false
  #vacuum: VacuumOption | false
  #pragmas: Record<string, string | number>
  #logger: Logger

  #db: DatabaseSync | null = null
  #connectedPid: number | null = null
  // Cache of prepared statements keyed by SQL. node:sqlite's prepare() re-parses
  // every call; reusing StatementSync instances avoids that cost on hot paths.
  // Cleared on disconnect (statements become invalid once the db handle closes).
  #stmts: Map<string, StatementSync> = new Map()

  #eventEmitter = new EventEmitter({ captureRejections: true })
  #notifyEmitter = new EventEmitter({ captureRejections: true })
  // Listeners this instance registered on the (possibly shared) root emitters,
  // so disconnect() removes exactly its own subscriptions and nothing else.
  #subscriptions: Array<{ emitter: EventEmitter; channel: string; handler: (...args: unknown[]) => void }> = []
  #dequeueWaiters: DequeueWaiter[] = []

  #cleanupInterval: ReturnType<typeof setInterval> | null = null
  #leadershipTimer: ReturnType<typeof setInterval> | null = null
  #vacuumInterval: ReturnType<typeof setInterval> | null = null
  #ownerHeartbeat: ReturnType<typeof setInterval> | null = null
  #writeMutex: Promise<void> = Promise.resolve()
  #lifecycle: Promise<void> = Promise.resolve()

  #instanceId = randomUUID()
  #isCleanupLeader = false
  // Set to true if disconnect() was called on the root while children were still
  // connected. The last child to disconnect re-triggers root teardown.
  #pendingClose = false

  // Namespace support — shares the database handle with a root instance.
  #parentStorage: SQLiteStorage | null = null
  #refCount = 0
  // Set of child namespace prefixes whose tables the root cleanup loop should
  // sweep. Populated when children connect, cleared on child disconnect.
  // Only meaningful on the root (parentStorage === null).
  #childPrefixes: Map<string, number> = new Map()

  // Table names (computed from prefix).
  #jobsTable: string
  #queueTable: string
  #processingTable: string
  #resultsTable: string
  #errorsTable: string
  #workersTable: string
  #locksTable: string
  #metaTable: string

  constructor (config: SQLiteStorageConfig = {}) {
    this.#path = config.path ?? ':memory:'
    this.#tablePrefix = config.tablePrefix ?? 'jq_'
    assertSafeIdentifier(this.#tablePrefix, 'tablePrefix')

    this.#cleanupIntervalMs = config.cleanupIntervalMs ?? DEFAULT_CLEANUP_INTERVAL_MS
    this.#vacuum =
      config.vacuum === undefined ? { enabled: true, intervalMs: DEFAULT_VACUUM_INTERVAL_MS } : config.vacuum

    this.#pragmas = { ...DEFAULT_PRAGMAS, ...(config.pragmas ?? {}) }
    this.#logger = (config.logger ?? abstractLogger).child({
      component: 'sqlite-storage',
      tablePrefix: this.#tablePrefix
    })

    this.#jobsTable = `${this.#tablePrefix}jobs`
    this.#queueTable = `${this.#tablePrefix}queue`
    this.#processingTable = `${this.#tablePrefix}processing`
    this.#resultsTable = `${this.#tablePrefix}results`
    this.#errorsTable = `${this.#tablePrefix}errors`
    this.#workersTable = `${this.#tablePrefix}workers`
    this.#locksTable = `${this.#tablePrefix}locks`
    this.#metaTable = `${this.#tablePrefix}meta`

    this.#eventEmitter.setMaxListeners(0)
    this.#notifyEmitter.setMaxListeners(0)
  }

  // ═══════════════════════════════════════════════════════════════════
  // LIFECYCLE
  // ═══════════════════════════════════════════════════════════════════

  // connect() and disconnect() on one instance run one at a time, in call
  // order. Overlapping calls otherwise race: two connects double-count the
  // parent's refCount, and a disconnect() issued while a connect() is still
  // in flight would see nothing to tear down and return, leaving the instance
  // attached once the connect lands.
  async connect (): Promise<void> {
    const parent = this.#parentStorage
    if (!parent) return this.#serializeLifecycle(() => this.#doConnect())

    // A namespace registers with its root when connect() is CALLED, not when
    // its own lifecycle chain gets to it (which may be much later, e.g. behind
    // a disconnect() waiting on the root). Root operations then take effect
    // in call order: a root.disconnect() issued after this call sees the
    // reference and defers, and the "open the root if closed" step is queued
    // on the root's chain ahead of it, so it can't reopen a root the user
    // closed afterwards. #doConnectNamespace releases the reference if it
    // doesn't end up attaching.
    parent.#refCount++
    const rootOpen = parent.#serializeLifecycle(async () => {
      if (!parent.#db) await parent.#doConnect()
    })
    return this.#serializeLifecycle(() => this.#doConnectNamespace(parent, rootOpen))
  }

  #serializeLifecycle (fn: () => Promise<void>): Promise<void> {
    const run = this.#lifecycle.then(fn)
    this.#lifecycle = run.catch(() => {})
    return run
  }

  async #doConnectNamespace (parent: SQLiteStorage, rootOpen: Promise<void>): Promise<void> {
    let attached = false
    try {
      await rootOpen
      if (this.#db) return // already connected; this call's reference is released below
      this.#db = parent.#db
      this.#connectedPid = parent.#connectedPid
      try {
        this.#createSchema()
        this.#checkSchemaVersion()
      } catch (err) {
        // Undo the attach: a half-connected namespace would report success on
        // the next connect() and keep the root open.
        this.#stmts.clear()
        this.#db = null
        this.#connectedPid = null
        throw err
      }
      attached = true
      // Register with parent so the cleanup leader sweeps our tables too.
      // Counted: several instances of one namespace share the same tables.
      parent.#childPrefixes.set(this.#tablePrefix, (parent.#childPrefixes.get(this.#tablePrefix) ?? 0) + 1)
      this.#subscribeToNewJobs()
    } finally {
      if (!attached) {
        // Release the reference taken in connect() so a pending root close
        // isn't blocked by a namespace that never attached.
        parent.#refCount--
        await this.#closeParentIfPending()
      }
    }
  }

  async #doConnect (): Promise<void> {
    if (this.#db) {
      // connect() after a deferred disconnect(): cancel the pending close and
      // restart the timers that disconnect() stopped.
      if (this.#pendingClose) {
        this.#pendingClose = false
        this.#startCleanupLeaderLoop()
        this.#startVacuumLoop()
      }
      return
    }

    try {
      this.#db = new DatabaseSync(this.#path)
    } catch (err) {
      const error = err as NodeJS.ErrnoException
      throw new StorageError(`SQLiteStorage: failed to open '${this.#path}': ${error.message}`, error)
    }
    this.#connectedPid = process.pid

    try {
      this.#applyPragmas()
      this.#assertWalActive()
      this.#createSchema()
      this.#checkSchemaVersion()
      this.#claimOwnership()
    } catch (err) {
      // Don't leave a half-open handle behind: a later connect() would see
      // #db set and return early as if it had succeeded.
      this.#stmts.clear()
      try {
        this.#db.close()
      } catch {
        // best-effort
      }
      this.#db = null
      this.#connectedPid = null
      throw err
    }

    this.#subscribeToNewJobs()
    this.#startOwnerHeartbeat()
    this.#startCleanupLeaderLoop()
    this.#startVacuumLoop()
  }

  #detachFromParent (): void {
    const parent = this.#parentStorage!
    const count = (parent.#childPrefixes.get(this.#tablePrefix) ?? 1) - 1
    if (count > 0) {
      parent.#childPrefixes.set(this.#tablePrefix, count)
    } else {
      parent.#childPrefixes.delete(this.#tablePrefix)
    }
    this.#stmts.clear()
    this.#db = null
    this.#connectedPid = null
    parent.#refCount--
  }

  // If the root was waiting on its children to close, complete its teardown.
  // Decided when this runs on the root's lifecycle chain, not now: a
  // root.connect() queued ahead of it cancels the pending close, and must win.
  async #closeParentIfPending (): Promise<void> {
    const parent = this.#parentStorage!
    await parent.#serializeLifecycle(async () => {
      if (parent.#pendingClose && parent.#refCount === 0) await parent.#doDisconnect()
    })
  }

  async disconnect (): Promise<void> {
    return this.#serializeLifecycle(() => this.#doDisconnect())
  }

  async #doDisconnect (): Promise<void> {
    // Forked-child path: drop local refs only. The inherited #db points at the
    // parent process's open OS handle; closing it would corrupt the parent.
    // Also don't touch parent.#refCount — that's parent's bookkeeping. Clearing
    // local timers IS safe (each process has its own event loop).
    if (this.#db && this.#connectedPid !== null && process.pid !== this.#connectedPid) {
      this.#stopTimers()
      if (this.#ownerHeartbeat) {
        clearInterval(this.#ownerHeartbeat)
        this.#ownerHeartbeat = null
      }
      this.#db = null
      this.#connectedPid = null
      this.#stmts.clear()
      this.#clearDequeueWaiters()
      this.#removeSubscriptions()
      return
    }

    // Namespace path
    if (this.#parentStorage) {
      if (!this.#db) return // idempotent: already disconnected
      this.#removeSubscriptions()
      // Let writes already queued on this instance finish before dropping the
      // shared handle, as the root does before closing it.
      const release = await this.#acquireMutex()
      let requeued = 0
      try {
        requeued = this.#abandonDequeueWaiters(this.#db)
        this.#detachFromParent()
      } finally {
        release()
      }
      // Wake other instances of this namespace for the jobs we put back.
      if (requeued > 0) this.#announceNewJob()
      await this.#closeParentIfPending()
      return
    }

    // Root path: idempotent
    if (!this.#db) return

    // Marks the close as requested before the first await, so a connect()
    // that arrives while we wait below cancels it (see #doConnect) and we
    // back off instead of closing a handle the caller now expects to be open.
    this.#pendingClose = true

    // Tear down timers first so cleanup/leadership/vacuum ticks don't enqueue
    // new writes after we start closing.
    this.#stopTimers()

    if (this.#isCleanupLeader) {
      this.#isCleanupLeader = false
      try {
        await this.releaseLeaderLock(CLEANUP_LOCK_KEY, this.#instanceId)
      } catch {
        // best-effort
      }
    }

    // If children are still connected, defer the actual close. The last child
    // to disconnect re-triggers this path.
    if (this.#refCount > 0) return

    // Drain in-flight writes by acquiring the mutex before closing the handle.
    const release = await this.#acquireMutex()
    try {
      // Re-check under the lock; everything below is synchronous, so nothing
      // can slip in between these checks and the close.
      if (!this.#db || !this.#pendingClose) return // closed already, or reconnected
      if (this.#refCount > 0) return // a namespace attached; it closes us when it leaves

      this.#abandonDequeueWaiters(this.#db)
      this.#removeSubscriptions()
      this.#eventEmitter.removeAllListeners()
      this.#notifyEmitter.removeAllListeners()
      // The heartbeat runs until the handle actually closes: children still
      // using it during a deferred close need the claim to stay fresh.
      if (this.#ownerHeartbeat) {
        clearInterval(this.#ownerHeartbeat)
        this.#ownerHeartbeat = null
      }
      this.#releaseOwnership()
      this.#stmts.clear()

      try {
        this.#db.close()
      } catch {
        // best-effort
      }
      this.#db = null
      this.#connectedPid = null
      this.#pendingClose = false
    } finally {
      release()
    }
  }

  #stopTimers (): void {
    if (this.#leadershipTimer) {
      clearInterval(this.#leadershipTimer)
      this.#leadershipTimer = null
    }
    if (this.#cleanupInterval) {
      clearInterval(this.#cleanupInterval)
      this.#cleanupInterval = null
    }
    if (this.#vacuumInterval) {
      clearInterval(this.#vacuumInterval)
      this.#vacuumInterval = null
    }
  }

  // SQLite does not accept bind parameters inside PRAGMA statements, so values
  // are interpolated. Pragma keys/values come from the SQLiteStorageConfig
  // object — trusted at the same level as tablePrefix and path.
  #applyPragmas (): void {
    const db = this.#db!
    // busy_timeout goes first: journal_mode and auto_vacuum need a lock, and
    // without a timeout they fail instantly with SQLITE_BUSY whenever another
    // connection happens to be writing.
    const entries = Object.entries(this.#pragmas).sort(
      ([a], [b]) => Number(b === 'busy_timeout') - Number(a === 'busy_timeout')
    )
    for (const [key, value] of entries) {
      const formattedValue = typeof value === 'string' ? value : String(value)
      db.exec(`PRAGMA ${key} = ${formattedValue}`)
    }
  }

  #assertWalActive (): void {
    if (this.#path === ':memory:') return // WAL not applicable for :memory:
    const requestedJournalMode = String(this.#pragmas.journal_mode ?? '').toLowerCase()
    if (requestedJournalMode !== 'wal') return
    const row = this.#stmt('PRAGMA journal_mode').get() as { journal_mode?: string } | undefined
    const actual = row?.journal_mode?.toLowerCase()
    if (actual !== 'wal') {
      this.#logger.warn(
        { requested: 'wal', actual },
        'SQLiteStorage: WAL mode requested but not active. This usually means the path is on a filesystem that does not support WAL (NFS, some bind-mounts). Cleanup and dequeue throughput will be reduced.'
      )
    }
  }

  #createSchema (): void {
    const db = this.#db!
    db.exec(`
      CREATE TABLE IF NOT EXISTS "${this.#jobsTable}" (
        id TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        expires_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS "${this.#queueTable}" (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT NOT NULL,
        message BLOB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS "${this.#processingTable}" (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        worker_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        message BLOB NOT NULL
      );
      CREATE INDEX IF NOT EXISTS "${this.#processingTable}_worker_idx"
        ON "${this.#processingTable}" (worker_id);
      CREATE TABLE IF NOT EXISTS "${this.#resultsTable}" (
        id TEXT PRIMARY KEY,
        data BLOB NOT NULL,
        expires_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS "${this.#resultsTable}_expires_idx"
        ON "${this.#resultsTable}" (expires_at);
      CREATE TABLE IF NOT EXISTS "${this.#errorsTable}" (
        id TEXT PRIMARY KEY,
        data BLOB NOT NULL,
        expires_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS "${this.#errorsTable}_expires_idx"
        ON "${this.#errorsTable}" (expires_at);
      CREATE TABLE IF NOT EXISTS "${this.#workersTable}" (
        worker_id TEXT PRIMARY KEY,
        expires_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS "${this.#workersTable}_expires_idx"
        ON "${this.#workersTable}" (expires_at);
      CREATE TABLE IF NOT EXISTS "${this.#locksTable}" (
        lock_key TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        expires_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS "${this.#metaTable}" (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    `)
  }

  #checkSchemaVersion (): void {
    const row = this.#stmt(`SELECT value FROM "${this.#metaTable}" WHERE key = 'schema_version'`).get() as
      | { value?: string }
      | undefined

    if (!row) {
      this.#stmt(`INSERT OR IGNORE INTO "${this.#metaTable}" (key, value) VALUES ('schema_version', ?)`).run(
        String(SCHEMA_VERSION)
      )
      return
    }

    const found = parseInt(row.value ?? '0', 10)
    if (Number.isNaN(found) || found > SCHEMA_VERSION) {
      const echoed = JSON.stringify(row.value ?? null).slice(0, 32)
      throw new StorageError(
        `SQLiteStorage: database schema version ${echoed} is not supported ` +
          `by this library (supports schema v${SCHEMA_VERSION}). Downgrade is not supported.`
      )
    }
  }

  // Node's cluster and child_process start fresh processes, so a second
  // process never inherits this instance — it opens its own connection to the
  // same file. Storage stays consistent, but dequeue wake-ups and job
  // notifications are in-process EventEmitters and would never reach it:
  // enqueueAndWait would hang until its timeout. Refuse the second connection
  // instead of degrading silently. Separate table prefixes are separate
  // queues, so ownership is per prefix.
  #claimOwnership (): void {
    if (this.#path === ':memory:' || this.#path === '') return // private to this connection

    const db = this.#db!
    db.exec('BEGIN IMMEDIATE')
    try {
      const row = this.#stmt(`SELECT value FROM "${this.#metaTable}" WHERE key = ?`).get(OWNER_META_KEY) as
        | { value?: string }
        | undefined
      const owner = row?.value ? this.#parseOwner(row.value) : null

      if (owner && owner.instanceId !== this.#instanceId && this.#isOwnerLive(owner)) {
        throw new StorageError(
          `SQLiteStorage: '${this.#path}' (tablePrefix '${this.#tablePrefix}') is already in use by ` +
            `another SQLiteStorage (pid=${owner.pid}). SQLiteStorage is single-process: job notifications ` +
            'and dequeue wake-ups do not cross connections. Share one SQLiteStorage instance (use ' +
            'createNamespace() for separate queues), or use RedisStorage/PgStorage for multiple processes.'
        )
      }

      this.#stmt(
        `INSERT INTO "${this.#metaTable}" (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`
      ).run(OWNER_META_KEY, this.#ownerValue())
      db.exec('COMMIT')
    } catch (err) {
      this.#safeRollback(db)
      throw err
    }
    liveOwnerInstances.add(this.#instanceId)
  }

  #isOwnerLive (owner: { pid: number; instanceId: string; heartbeatAt: number }): boolean {
    if (Date.now() - owner.heartbeatAt >= OWNER_STALE_MS) return false
    if (owner.pid === process.pid) return liveOwnerInstances.has(owner.instanceId)
    return isProcessAlive(owner.pid)
  }

  #parseOwner (value: string): { pid: number; instanceId: string; heartbeatAt: number } | null {
    try {
      const parsed = JSON.parse(value) as { pid?: unknown; instanceId?: unknown; heartbeatAt?: unknown }
      if (
        typeof parsed.pid === 'number' &&
        typeof parsed.instanceId === 'string' &&
        typeof parsed.heartbeatAt === 'number'
      ) {
        return { pid: parsed.pid, instanceId: parsed.instanceId, heartbeatAt: parsed.heartbeatAt }
      }
    } catch {
      // unreadable claim; treat as absent
    }
    return null
  }

  #ownerValue (): string {
    return JSON.stringify({ pid: process.pid, instanceId: this.#instanceId, heartbeatAt: Date.now() })
  }

  #startOwnerHeartbeat (): void {
    if (!liveOwnerInstances.has(this.#instanceId)) return // nothing claimed (:memory:)
    this.#ownerHeartbeat = setInterval(() => {
      this.#runWrite(() => {
        if (!this.#db) return
        this.#stmt(
          `UPDATE "${this.#metaTable}" SET value = ? WHERE key = ? AND json_extract(value, '$.instanceId') = ?`
        ).run(this.#ownerValue(), OWNER_META_KEY, this.#instanceId)
      }).catch(err => {
        this.#logger.warn({ err }, 'SQLiteStorage: owner heartbeat failed')
      })
    }, OWNER_HEARTBEAT_MS)
  }

  #releaseOwnership (): void {
    if (!liveOwnerInstances.delete(this.#instanceId)) return
    try {
      this.#stmt(`DELETE FROM "${this.#metaTable}" WHERE key = ? AND json_extract(value, '$.instanceId') = ?`).run(
        OWNER_META_KEY,
        this.#instanceId
      )
    } catch (err) {
      // best-effort: a stale claim expires after OWNER_STALE_MS anyway
      this.#logger.debug({ err }, 'SQLiteStorage: failed to release database ownership')
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // INTERNAL HELPERS
  // ═══════════════════════════════════════════════════════════════════

  #assertConnected (): DatabaseSync {
    if (!this.#db) {
      throw new StorageError('SQLiteStorage: not connected. Call connect() first.')
    }
    this.#assertSamePid()
    return this.#db
  }

  #assertSamePid (): void {
    if (this.#connectedPid !== null && process.pid !== this.#connectedPid) {
      throw new StorageError(
        'SQLiteStorage: detected use from a forked process ' +
          `(connected pid=${this.#connectedPid}, current pid=${process.pid}). ` +
          'SQLiteStorage is single-process only; forked children must call connect() after fork(). ' +
          'For multi-process queues, use PgStorage.'
      )
    }
  }

  #stmt (sql: string): StatementSync {
    let stmt = this.#stmts.get(sql)
    if (!stmt) {
      stmt = this.#db!.prepare(sql)
      this.#stmts.set(sql, stmt)
    }
    return stmt
  }

  /**
   * Serializes write operations through an async mutex and retries with jitter
   * on SQLITE_BUSY. Reads do NOT take the mutex; SQLite handles concurrent reads
   * via WAL.
   */
  async #runWrite<T> (fn: () => T): Promise<T> {
    const release = await this.#acquireMutex()
    try {
      // Writes queued before a disconnect() run after it; they must not touch
      // a handle this instance no longer holds.
      if (!this.#db) {
        throw new StorageError('SQLiteStorage: not connected. Call connect() first.')
      }
      let lastError: unknown
      for (let attempt = 0; attempt < WRITE_RETRY_ATTEMPTS; attempt++) {
        try {
          return fn()
        } catch (err) {
          lastError = err
          if (!this.#isBusyError(err)) {
            throw err
          }
          const backoff = Math.min(WRITE_RETRY_BASE_MS * 2 ** attempt, WRITE_RETRY_MAX_MS)
          const jitter = Math.floor(Math.random() * backoff)
          this.#logger.warn(
            { attempt: attempt + 1, backoffMs: backoff + jitter, err: (err as Error).message },
            'SQLiteStorage: writer locked, retrying'
          )
          await sleep(backoff + jitter)
        }
      }
      throw new StorageError(
        `SQLiteStorage: database is locked after ${WRITE_RETRY_ATTEMPTS} retries ` +
          `(exponential backoff up to ${WRITE_RETRY_MAX_MS}ms per attempt). ` +
          'Another writer is holding the lock. ' +
          'Either reduce contention, increase busy_timeout via pragmas, or switch to PgStorage.',
        lastError instanceof Error ? lastError : undefined
      )
    } finally {
      release()
    }
  }

  async #acquireMutex (): Promise<() => void> {
    let resolver!: () => void
    const next = new Promise<void>(resolve => {
      resolver = resolve
    })
    const previous = this.#writeMutex
    this.#writeMutex = previous.then(() => next)
    return previous.then(() => resolver)
  }

  // node:sqlite errors expose .errcode as SQLite's extended result code; mask
  // to the low byte to compare against the primary code (BUSY=5, LOCKED=6).
  #isBusyError (err: unknown): boolean {
    const primary = ((err as { errcode?: number } | null)?.errcode ?? 0) & 0xff
    return primary === 5 || primary === 6
  }

  // SQLite auto-rolls back on hard errors (SQLITE_FULL, SQLITE_IOERR,
  // SQLITE_NOMEM, SQLITE_INTERRUPT, some SQLITE_BUSY-on-COMMIT cases), so a
  // follow-up ROLLBACK then errors with "no transaction is active." That's
  // benign; log at debug so unexpected failures (closed handle, driver bug)
  // are still greppable.
  //
  // Takes the handle the transaction was opened on rather than reading #db,
  // which a concurrent disconnect() may already have cleared: skipping the
  // ROLLBACK would leave the transaction open on a connection that namespaces
  // share with the root, breaking every later BEGIN on it.
  #safeRollback (db: DatabaseSync): void {
    try {
      db.exec('ROLLBACK')
    } catch (err) {
      this.#logger.debug({ err }, 'SQLiteStorage: ROLLBACK after failed tx errored (likely auto-rolled back)')
    }
  }

  // Forked-child path only: the handle belongs to the parent process, so
  // nothing can be written back.
  #clearDequeueWaiters (): void {
    for (const waiter of this.#dequeueWaiters) {
      waiter.abandoned = true
      clearTimeout(waiter.timeoutId)
      waiter.resolve(null)
    }
    this.#dequeueWaiters = []
  }

  // Called by disconnect() while holding the write lock. Settles every parked
  // dequeue with null and returns to the queue any job an attempt claimed but
  // hasn't handed over yet — otherwise it would sit in processing under a
  // worker that is shutting down (and usually already unregistered), where
  // the reaper never looks. Attempts that run after this see `abandoned` and
  // claim nothing. Returns the number of jobs put back.
  #abandonDequeueWaiters (db: DatabaseSync): number {
    const waiters = this.#dequeueWaiters
    this.#dequeueWaiters = []
    const stranded: Array<{ workerId: string; jobId: string; message: Buffer }> = []
    for (const waiter of waiters) {
      waiter.abandoned = true
      clearTimeout(waiter.timeoutId)
      if (waiter.claimed) stranded.push({ workerId: waiter.workerId, ...waiter.claimed })
      waiter.claimed = null
      waiter.resolve(null)
    }
    if (stranded.length === 0) return 0

    db.exec('BEGIN IMMEDIATE')
    try {
      for (const { workerId, jobId, message } of stranded) {
        this.#stmt(
          `DELETE FROM "${this.#processingTable}" WHERE seq = (
             SELECT seq FROM "${this.#processingTable}"
              WHERE worker_id = ? AND job_id = ?
              ORDER BY seq LIMIT 1
           )`
        ).run(workerId, jobId)
        this.#stmt(`INSERT INTO "${this.#queueTable}" (job_id, message) VALUES (?, ?)`).run(jobId, message)
      }
      db.exec('COMMIT')
    } catch (err) {
      this.#safeRollback(db)
      this.#logger.error(
        { err, count: stranded.length },
        'SQLiteStorage: failed to requeue jobs claimed during disconnect'
      )
      return 0
    }
    return stranded.length
  }

  // Dequeue wake-ups go through the root's emitter on a prefix-scoped channel,
  // so a job enqueued through one namespace instance wakes consumers parked on
  // any other instance of the same name (e.g. separate producer and consumer
  // Queues), as Pg's LISTEN new_job and Redis' BLMOVE do.
  #newJobChannel (): string {
    return `newjob:${this.#tablePrefix}`
  }

  #subscribeToNewJobs (): void {
    this.#subscribe(this.#events(), this.#newJobChannel(), () => this.#notifyDequeueWaiters())
  }

  #announceNewJob (): void {
    this.#events().emit(this.#newJobChannel())
  }

  #notifyDequeueWaiters (): void {
    for (const waiter of [...this.#dequeueWaiters]) {
      this.#attemptDequeue(waiter, false)
    }
  }

  // Try to hand a parked waiter a message. Each attempt is its own write tx.
  // Waiters stay in #dequeueWaiters until their promise settles: removing them
  // earlier races the dequeue timeout, which could resolve null while
  // #tryDequeue commits a message into the processing table — silently losing
  // the job.
  #attemptDequeue (waiter: DequeueWaiter, first: boolean): void {
    if (waiter.abandoned) return
    if (waiter.inflight) {
      waiter.renotify = true
      return
    }
    waiter.inflight = true
    waiter.renotify = false
    this.#tryDequeue(waiter).then(
      msg => {
        waiter.inflight = false
        if (waiter.abandoned) return // disconnect() settled it and owns any claim
        if (msg) {
          waiter.claimed = null
          this.#settleDequeueWaiter(waiter, msg)
        } else if (waiter.timedOut) {
          this.#settleDequeueWaiter(waiter, null)
        } else if (waiter.renotify) {
          this.#attemptDequeue(waiter, false)
        }
      },
      err => {
        waiter.inflight = false
        if (waiter.abandoned) return
        if (first) {
          // Surface errors from the initial attempt (not connected, forked
          // process, ...) to the caller, as a plain failed dequeue would.
          clearTimeout(waiter.timeoutId)
          this.#removeDequeueWaiter(waiter)
          waiter.reject(err)
          return
        }
        this.#logger.error({ err }, 'SQLiteStorage: dequeue waiter failed; will retry')
        if (waiter.timedOut) {
          this.#settleDequeueWaiter(waiter, null)
        } else if (waiter.renotify) {
          // A job was announced while this attempt was failing; don't drop it.
          this.#attemptDequeue(waiter, false)
        }
      }
    )
  }

  #settleDequeueWaiter (waiter: DequeueWaiter, value: Buffer | null): void {
    clearTimeout(waiter.timeoutId)
    this.#removeDequeueWaiter(waiter)
    waiter.resolve(value)
  }

  #removeDequeueWaiter (waiter: DequeueWaiter): void {
    const index = this.#dequeueWaiters.indexOf(waiter)
    if (index !== -1) this.#dequeueWaiters.splice(index, 1)
  }

  // Deletes exactly one matching in-flight row (the oldest). A bare
  // worker_id+message predicate would wipe every byte-identical sibling
  // message the worker holds, losing their crash-recovery records.
  #deleteOneProcessingRow (workerId: string, message: Buffer): void {
    this.#stmt(
      `DELETE FROM "${this.#processingTable}" WHERE seq = (
         SELECT seq FROM "${this.#processingTable}"
          WHERE worker_id = ? AND message = ?
          ORDER BY seq LIMIT 1
       )`
    ).run(workerId, message)
  }

  // ═══════════════════════════════════════════════════════════════════
  // QUEUE OPERATIONS
  // ═══════════════════════════════════════════════════════════════════

  async enqueue (id: string, message: Buffer, timestamp: number): Promise<string | null> {
    const db = this.#assertConnected()
    const state = `queued:${timestamp}`
    const now = Date.now()

    const existing = await this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        const row = this.#stmt(`SELECT state, expires_at FROM "${this.#jobsTable}" WHERE id = ?`).get(id) as
          | { state?: string; expires_at?: number | null }
          | undefined

        if (row) {
          const expiresAt = row.expires_at ?? null
          if (expiresAt && now >= expiresAt) {
            this.#stmt(`DELETE FROM "${this.#jobsTable}" WHERE id = ?`).run(id)
          } else {
            db.exec('COMMIT')
            return row.state ?? null
          }
        }

        this.#stmt(`INSERT INTO "${this.#jobsTable}" (id, state) VALUES (?, ?)`).run(id, state)
        this.#stmt(`INSERT INTO "${this.#queueTable}" (job_id, message) VALUES (?, ?)`).run(id, message)
        db.exec('COMMIT')
        return null
      } catch (err) {
        this.#safeRollback(db)
        throw err
      }
    })

    if (existing === null) {
      this.#events().emit(this.#eventChannel(), id, 'queued')
      this.#announceNewJob()
    }

    return existing
  }

  async dequeue (workerId: string, timeout: number): Promise<Buffer | null> {
    this.#assertConnected()

    // The waiter is registered before the first attempt, not after it comes
    // back empty: a job announced from another instance in between would
    // otherwise find no one to wake and sit in the queue until the timeout.
    return new Promise<Buffer | null>((resolve, reject) => {
      const waiter: DequeueWaiter = {
        workerId,
        resolve,
        reject,
        inflight: false,
        timedOut: false,
        renotify: false,
        abandoned: false,
        claimed: null,
        timeoutId: setTimeout(() => {
          if (waiter.inflight) {
            // An attempt may already have claimed a message for this waiter;
            // let its completion settle the promise so the message isn't lost.
            waiter.timedOut = true
            return
          }
          this.#settleDequeueWaiter(waiter, null)
        }, timeout * 1000)
      }
      this.#dequeueWaiters.push(waiter)
      this.#attemptDequeue(waiter, true)
    })
  }

  async #tryDequeue (waiter: DequeueWaiter): Promise<Buffer | null> {
    const db = this.#assertConnected()
    return this.#runWrite(() => {
      if (waiter.abandoned) return null
      db.exec('BEGIN IMMEDIATE')
      try {
        const row = this.#stmt(
          `DELETE FROM "${this.#queueTable}"
             WHERE seq = (SELECT seq FROM "${this.#queueTable}" ORDER BY seq LIMIT 1)
             RETURNING job_id, message`
        ).get() as { job_id?: string; message?: unknown } | undefined

        if (!row || row.message === undefined) {
          db.exec('COMMIT')
          return null
        }

        const message = toBuffer(row.message)
        this.#stmt(`INSERT INTO "${this.#processingTable}" (worker_id, job_id, message) VALUES (?, ?, ?)`).run(
          waiter.workerId,
          row.job_id!,
          message
        )
        db.exec('COMMIT')
        waiter.claimed = { jobId: row.job_id!, message }
        return message
      } catch (err) {
        this.#safeRollback(db)
        throw err
      }
    })
  }

  async requeue (id: string, message: Buffer, workerId: string): Promise<void> {
    const db = this.#assertConnected()
    await this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        this.#deleteOneProcessingRow(workerId, message)
        this.#stmt(`INSERT INTO "${this.#queueTable}" (job_id, message) VALUES (?, ?)`).run(id, message)
        db.exec('COMMIT')
      } catch (err) {
        this.#safeRollback(db)
        throw err
      }
    })
    this.#announceNewJob()
  }

  async ack (id: string, message: Buffer, workerId: string): Promise<void> {
    this.#assertConnected()
    await this.#runWrite(() => {
      this.#deleteOneProcessingRow(workerId, message)
    })
  }

  // ═══════════════════════════════════════════════════════════════════
  // JOB STATE
  // ═══════════════════════════════════════════════════════════════════

  async getJobState (id: string): Promise<string | null> {
    this.#assertConnected()
    const row = this.#stmt(`SELECT state, expires_at FROM "${this.#jobsTable}" WHERE id = ?`).get(id) as
      | { state?: string; expires_at?: number | null }
      | undefined

    if (!row) return null
    const expiresAt = row.expires_at ?? null
    const now = Date.now()
    if (expiresAt && now >= expiresAt) {
      // Re-check expiry in the DELETE: an enqueue() queued ahead of this
      // write may already have replaced the expired row with a live job.
      await this.#runWrite(() => {
        this.#stmt(`DELETE FROM "${this.#jobsTable}" WHERE id = ? AND expires_at <= ?`).run(id, now)
      })
      return null
    }
    return row.state ?? null
  }

  async setJobState (id: string, state: string): Promise<void> {
    this.#assertConnected()
    await this.#runWrite(() => {
      this.#stmt(`UPDATE "${this.#jobsTable}" SET state = ? WHERE id = ?`).run(state, id)
    })
  }

  async deleteJob (id: string): Promise<boolean> {
    this.#assertConnected()
    const changes = await this.#runWrite(() => {
      const result = this.#stmt(`DELETE FROM "${this.#jobsTable}" WHERE id = ?`).run(id)
      return result.changes
    })
    if (changes > 0) {
      this.#events().emit(this.#eventChannel(), id, 'cancelled')
      return true
    }
    return false
  }

  async getJobStates (ids: string[]): Promise<Map<string, string | null>> {
    const result = new Map<string, string | null>()
    if (ids.length === 0) return result

    const db = this.#assertConnected()
    const now = Date.now()
    const found = new Set<string>()
    const expiredIds: string[] = []

    for (let i = 0; i < ids.length; i += MAX_BIND_PARAMS_PER_QUERY) {
      const chunk = ids.slice(i, i + MAX_BIND_PARAMS_PER_QUERY)
      // Bypass the #stmts cache for variable-arity IN-list SQL — caching here
      // would grow the cache by one entry per distinct batch size.
      const placeholders = chunk.map(() => '?').join(',')
      const rows = db
        .prepare(`SELECT id, state, expires_at FROM "${this.#jobsTable}" WHERE id IN (${placeholders})`)
        .all(...chunk) as Array<{ id: string; state: string; expires_at: number | null }>

      for (const row of rows) {
        found.add(row.id)
        if (row.expires_at && now >= row.expires_at) {
          expiredIds.push(row.id)
          result.set(row.id, null)
        } else {
          result.set(row.id, row.state)
        }
      }
    }

    if (expiredIds.length > 0) {
      await this.#runWrite(() => {
        for (let i = 0; i < expiredIds.length; i += MAX_BIND_PARAMS_PER_QUERY) {
          const chunk = expiredIds.slice(i, i + MAX_BIND_PARAMS_PER_QUERY)
          const placeholders = chunk.map(() => '?').join(',')
          db.prepare(`DELETE FROM "${this.#jobsTable}" WHERE id IN (${placeholders}) AND expires_at <= ?`).run(
            ...chunk,
            now
          )
        }
      })
    }

    for (const id of ids) {
      if (!found.has(id)) result.set(id, null)
    }

    return result
  }

  async setJobExpiry (id: string, ttlMs: number): Promise<void> {
    this.#assertConnected()
    const expiresAt = Date.now() + ttlMs
    await this.#runWrite(() => {
      this.#stmt(`UPDATE "${this.#jobsTable}" SET expires_at = ? WHERE id = ?`).run(expiresAt, id)
    })
  }

  // ═══════════════════════════════════════════════════════════════════
  // RESULTS
  // ═══════════════════════════════════════════════════════════════════

  async setResult (id: string, result: Buffer, ttlMs: number): Promise<void> {
    this.#assertConnected()
    const expiresAt = Date.now() + ttlMs
    await this.#runWrite(() => {
      this.#stmt(
        `INSERT INTO "${this.#resultsTable}" (id, data, expires_at)
         VALUES (?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET data = excluded.data, expires_at = excluded.expires_at`
      ).run(id, result, expiresAt)
    })
  }

  async getResult (id: string): Promise<Buffer | null> {
    this.#assertConnected()
    const row = this.#stmt(`SELECT data, expires_at FROM "${this.#resultsTable}" WHERE id = ?`).get(id) as
      | { data?: unknown; expires_at?: number }
      | undefined
    if (!row) return null
    const now = Date.now()
    if (row.expires_at !== undefined && now >= row.expires_at) {
      // Re-check expiry: a set queued ahead of this write may have replaced it.
      await this.#runWrite(() => {
        this.#stmt(`DELETE FROM "${this.#resultsTable}" WHERE id = ? AND expires_at <= ?`).run(id, now)
      })
      return null
    }
    return toBuffer(row.data)
  }

  async setError (id: string, error: Buffer, ttlMs: number): Promise<void> {
    this.#assertConnected()
    const expiresAt = Date.now() + ttlMs
    await this.#runWrite(() => {
      this.#stmt(
        `INSERT INTO "${this.#errorsTable}" (id, data, expires_at)
         VALUES (?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET data = excluded.data, expires_at = excluded.expires_at`
      ).run(id, error, expiresAt)
    })
  }

  async getError (id: string): Promise<Buffer | null> {
    this.#assertConnected()
    const row = this.#stmt(`SELECT data, expires_at FROM "${this.#errorsTable}" WHERE id = ?`).get(id) as
      | { data?: unknown; expires_at?: number }
      | undefined
    if (!row) return null
    const now = Date.now()
    if (row.expires_at !== undefined && now >= row.expires_at) {
      // Re-check expiry: a set queued ahead of this write may have replaced it.
      await this.#runWrite(() => {
        this.#stmt(`DELETE FROM "${this.#errorsTable}" WHERE id = ? AND expires_at <= ?`).run(id, now)
      })
      return null
    }
    return toBuffer(row.data)
  }

  // ═══════════════════════════════════════════════════════════════════
  // WORKERS
  // ═══════════════════════════════════════════════════════════════════

  async registerWorker (workerId: string, ttlMs: number): Promise<void> {
    this.#assertConnected()
    const expiresAt = Date.now() + ttlMs
    await this.#runWrite(() => {
      this.#stmt(
        `INSERT INTO "${this.#workersTable}" (worker_id, expires_at)
         VALUES (?, ?)
         ON CONFLICT(worker_id) DO UPDATE SET expires_at = excluded.expires_at`
      ).run(workerId, expiresAt)
    })
  }

  async refreshWorker (workerId: string, ttlMs: number): Promise<void> {
    return this.registerWorker(workerId, ttlMs)
  }

  async unregisterWorker (workerId: string): Promise<void> {
    if (!this.#db) return
    this.#assertSamePid()
    await this.#runWrite(() => {
      this.#stmt(`DELETE FROM "${this.#workersTable}" WHERE worker_id = ?`).run(workerId)
      this.#stmt(`DELETE FROM "${this.#processingTable}" WHERE worker_id = ?`).run(workerId)
    })
  }

  async getWorkers (): Promise<string[]> {
    this.#assertConnected()
    const rows = this.#stmt(`SELECT worker_id FROM "${this.#workersTable}" WHERE expires_at > ?`).all(
      Date.now()
    ) as Array<{ worker_id: string }>
    return rows.map(r => r.worker_id)
  }

  async getProcessingJobs (workerId: string): Promise<Buffer[]> {
    this.#assertConnected()
    const rows = this.#stmt(`SELECT message FROM "${this.#processingTable}" WHERE worker_id = ?`).all(
      workerId
    ) as Array<{ message: unknown }>
    return rows.map(r => toBuffer(r.message))
  }

  // ═══════════════════════════════════════════════════════════════════
  // NOTIFICATIONS (in-process only)
  // ═══════════════════════════════════════════════════════════════════

  // Events and notifications go through the ROOT's emitters on channels scoped
  // by table prefix, so every namespace instance created for the same name
  // shares a channel (mirroring Redis/Pg prefix-keyed pub/sub) while distinct
  // namespaces stay isolated. Per-instance emitters would strand a Reaper or
  // enqueueAndWait producer that holds its own createNamespace(name) instance.
  #events (): EventEmitter {
    return (this.#parentStorage ?? this).#eventEmitter
  }

  #notifications (): EventEmitter {
    return (this.#parentStorage ?? this).#notifyEmitter
  }

  #eventChannel (): string {
    return `event:${this.#tablePrefix}`
  }

  #notifyChannel (id: string): string {
    return `notify:${this.#tablePrefix}:${id}`
  }

  #subscribe (emitter: EventEmitter, channel: string, handler: (...args: unknown[]) => void): () => Promise<void> {
    const sub = { emitter, channel, handler }
    emitter.on(channel, handler)
    this.#subscriptions.push(sub)
    return async () => {
      emitter.off(channel, handler)
      const index = this.#subscriptions.indexOf(sub)
      if (index !== -1) this.#subscriptions.splice(index, 1)
    }
  }

  #removeSubscriptions (): void {
    for (const sub of this.#subscriptions) {
      sub.emitter.off(sub.channel, sub.handler)
    }
    this.#subscriptions = []
  }

  async subscribeToJob (
    id: string,
    handler: (status: 'completed' | 'failed' | 'failing') => void
  ): Promise<() => Promise<void>> {
    return this.#subscribe(this.#notifications(), this.#notifyChannel(id), handler as (...args: unknown[]) => void)
  }

  async notifyJobComplete (id: string, status: 'completed' | 'failed' | 'failing'): Promise<void> {
    this.#assertConnected()
    this.#notifications().emit(this.#notifyChannel(id), status)
  }

  // ═══════════════════════════════════════════════════════════════════
  // EVENTS
  // ═══════════════════════════════════════════════════════════════════

  async subscribeToEvents (handler: (id: string, event: string) => void): Promise<() => Promise<void>> {
    return this.#subscribe(this.#events(), this.#eventChannel(), handler as (...args: unknown[]) => void)
  }

  async publishEvent (id: string, event: string): Promise<void> {
    this.#assertConnected()
    this.#events().emit(this.#eventChannel(), id, event)
  }

  // ═══════════════════════════════════════════════════════════════════
  // ATOMIC OPERATIONS
  // ═══════════════════════════════════════════════════════════════════

  async completeJob (id: string, message: Buffer, workerId: string, result: Buffer, resultTTL: number): Promise<void> {
    const db = this.#assertConnected()
    const timestamp = Date.now()
    const state = `completed:${timestamp}`
    const expiresAt = timestamp + resultTTL

    await this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        this.#stmt(`UPDATE "${this.#jobsTable}" SET state = ?, expires_at = ? WHERE id = ?`).run(state, expiresAt, id)
        this.#stmt(
          `INSERT INTO "${this.#resultsTable}" (id, data, expires_at)
           VALUES (?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET data = excluded.data, expires_at = excluded.expires_at`
        ).run(id, result, expiresAt)
        this.#deleteOneProcessingRow(workerId, message)
        db.exec('COMMIT')
      } catch (err) {
        this.#safeRollback(db)
        throw err
      }
    })

    this.#notifications().emit(this.#notifyChannel(id), 'completed')
    this.#events().emit(this.#eventChannel(), id, 'completed')
  }

  async failJob (id: string, message: Buffer, workerId: string, error: Buffer, errorTTL: number): Promise<void> {
    const db = this.#assertConnected()
    const timestamp = Date.now()
    const state = `failed:${timestamp}`
    const expiresAt = timestamp + errorTTL

    await this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        this.#stmt(`UPDATE "${this.#jobsTable}" SET state = ?, expires_at = ? WHERE id = ?`).run(state, expiresAt, id)
        this.#stmt(
          `INSERT INTO "${this.#errorsTable}" (id, data, expires_at)
           VALUES (?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET data = excluded.data, expires_at = excluded.expires_at`
        ).run(id, error, expiresAt)
        this.#deleteOneProcessingRow(workerId, message)
        db.exec('COMMIT')
      } catch (err) {
        this.#safeRollback(db)
        throw err
      }
    })

    this.#notifications().emit(this.#notifyChannel(id), 'failed')
    this.#events().emit(this.#eventChannel(), id, 'failed')
  }

  async retryJob (id: string, message: Buffer, workerId: string, attempts: number): Promise<void> {
    const db = this.#assertConnected()
    const timestamp = Date.now()
    const state = `failing:${timestamp}:${attempts}`

    await this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        // `message` is the NEW retry payload (attempts incremented), so it
        // can't be matched byte-for-byte against the in-flight row. Match on
        // the job id recorded at dequeue time instead — this works for any
        // payload serde, not just JSON.
        this.#stmt(`UPDATE "${this.#jobsTable}" SET state = ? WHERE id = ?`).run(state, id)
        this.#stmt(
          `DELETE FROM "${this.#processingTable}" WHERE seq = (
             SELECT seq FROM "${this.#processingTable}"
              WHERE worker_id = ? AND job_id = ?
              ORDER BY seq LIMIT 1
           )`
        ).run(workerId, id)
        this.#stmt(`INSERT INTO "${this.#queueTable}" (job_id, message) VALUES (?, ?)`).run(id, message)
        db.exec('COMMIT')
      } catch (err) {
        this.#safeRollback(db)
        throw err
      }
    })

    this.#notifications().emit(this.#notifyChannel(id), 'failing')
    this.#events().emit(this.#eventChannel(), id, 'failing')
    this.#announceNewJob()
  }

  // ═══════════════════════════════════════════════════════════════════
  // LEADER ELECTION
  // ═══════════════════════════════════════════════════════════════════

  async acquireLeaderLock (lockKey: string, ownerId: string, ttlMs: number): Promise<boolean> {
    const db = this.#assertConnected()
    const expiresAt = Date.now() + ttlMs

    return this.#runWrite(() => {
      db.exec('BEGIN IMMEDIATE')
      try {
        const row = this.#stmt(`SELECT owner_id, expires_at FROM "${this.#locksTable}" WHERE lock_key = ?`).get(
          lockKey
        ) as { owner_id?: string; expires_at?: number } | undefined

        const now = Date.now()
        if (row && row.expires_at !== undefined && now < row.expires_at) {
          db.exec('COMMIT')
          return false
        }

        this.#stmt(
          `INSERT INTO "${this.#locksTable}" (lock_key, owner_id, expires_at)
           VALUES (?, ?, ?)
           ON CONFLICT(lock_key) DO UPDATE
             SET owner_id = excluded.owner_id, expires_at = excluded.expires_at`
        ).run(lockKey, ownerId, expiresAt)
        db.exec('COMMIT')
        return true
      } catch (err) {
        this.#safeRollback(db)
        throw err
      }
    })
  }

  async renewLeaderLock (lockKey: string, ownerId: string, ttlMs: number): Promise<boolean> {
    this.#assertConnected()
    const expiresAt = Date.now() + ttlMs
    return this.#runWrite(() => {
      const result = this.#stmt(
        `UPDATE "${this.#locksTable}" SET expires_at = ? WHERE lock_key = ? AND owner_id = ?`
      ).run(expiresAt, lockKey, ownerId)
      return result.changes > 0
    })
  }

  async releaseLeaderLock (lockKey: string, ownerId: string): Promise<boolean> {
    if (!this.#db) return false
    this.#assertSamePid()
    return this.#runWrite(() => {
      const result = this.#stmt(`DELETE FROM "${this.#locksTable}" WHERE lock_key = ? AND owner_id = ?`).run(
        lockKey,
        ownerId
      )
      return result.changes > 0
    })
  }

  // ═══════════════════════════════════════════════════════════════════
  // CLEANUP LEADER + VACUUM
  // ═══════════════════════════════════════════════════════════════════

  #startCleanupLeaderLoop (): void {
    if (this.#cleanupIntervalMs === false) return
    if (this.#parentStorage) return // children don't run cleanup

    const tick = (): void => {
      this.#leadershipTick().catch(err => {
        this.#logger.error({ err }, 'SQLiteStorage: leadership tick failed')
      })
    }

    this.#leadershipTimer = setInterval(tick, CLEANUP_ACQUIRE_RETRY_MS)
    // Try once immediately so the first cleanup runs soon.
    setImmediate(tick)
  }

  async #leadershipTick (): Promise<void> {
    if (!this.#db) return

    if (this.#isCleanupLeader) {
      const renewed = await this.renewLeaderLock(CLEANUP_LOCK_KEY, this.#instanceId, CLEANUP_LOCK_TTL_MS)
      if (!renewed) {
        this.#isCleanupLeader = false
        this.#stopCleanupInterval()
        this.#logger.info('SQLiteStorage: lost cleanup leadership')
      }
    } else {
      const acquired = await this.acquireLeaderLock(CLEANUP_LOCK_KEY, this.#instanceId, CLEANUP_LOCK_TTL_MS)
      if (acquired && !this.#leadershipTimer) {
        // disconnect() stopped the loop while we waited for the write lock.
        // Don't start a cleanup interval nothing would ever clear.
        await this.releaseLeaderLock(CLEANUP_LOCK_KEY, this.#instanceId).catch(() => {})
        return
      }
      if (acquired) {
        this.#isCleanupLeader = true
        this.#startCleanupInterval()
        this.#logger.info('SQLiteStorage: acquired cleanup leadership')
      }
    }
  }

  #startCleanupInterval (): void {
    if (this.#cleanupInterval || this.#cleanupIntervalMs === false) return
    const intervalMs = this.#cleanupIntervalMs as number
    this.#cleanupInterval = setInterval(() => {
      this.#cleanupExpired().catch(err => {
        this.#logger.error({ err }, 'SQLiteStorage: cleanup sweep failed')
      })
    }, intervalMs)
  }

  #stopCleanupInterval (): void {
    if (this.#cleanupInterval) {
      clearInterval(this.#cleanupInterval)
      this.#cleanupInterval = null
    }
  }

  async #cleanupExpired (): Promise<void> {
    this.#assertConnected()
    const now = Date.now()
    const start = now

    // Sweep the root's prefix plus every registered child namespace prefix —
    // namespaces opt out of running their own cleanup loop, so without this
    // their results/errors/workers/locks/jobs rows accumulate forever.
    const prefixes = [this.#tablePrefix, ...this.#childPrefixes.keys()]
    await this.#runWrite(() => {
      for (const prefix of prefixes) {
        this.#stmt(`DELETE FROM "${prefix}results" WHERE expires_at < ?`).run(now)
        this.#stmt(`DELETE FROM "${prefix}errors" WHERE expires_at < ?`).run(now)
        this.#stmt(`DELETE FROM "${prefix}workers" WHERE expires_at < ?`).run(now)
        this.#stmt(`DELETE FROM "${prefix}jobs" WHERE expires_at IS NOT NULL AND expires_at < ?`).run(now)
        this.#stmt(`DELETE FROM "${prefix}locks" WHERE expires_at < ?`).run(now)
      }
    })

    const duration = Date.now() - start
    if (duration > 1000) {
      this.#logger.warn({ durationMs: duration, prefixCount: prefixes.length }, 'SQLiteStorage: cleanup sweep slow')
    }
  }

  #startVacuumLoop (): void {
    if (this.#vacuum === false || !this.#vacuum.enabled) return
    if (this.#parentStorage) return
    this.#vacuumInterval = setInterval(() => {
      this.#runWrite(() => {
        const db = this.#db
        if (!db) return
        db.exec('PRAGMA optimize')
        // Returns the pages freed by cleanup sweeps to the OS. Cost scales
        // with the size of the freelist, not the database (unlike VACUUM).
        // A no-op if auto_vacuum was overridden away from INCREMENTAL.
        db.exec('PRAGMA incremental_vacuum')
      }).catch(err => {
        this.#logger.warn({ err }, 'SQLiteStorage: maintenance failed')
      })
    }, this.#vacuum.intervalMs)
  }

  // ═══════════════════════════════════════════════════════════════════
  // NAMESPACE
  // ═══════════════════════════════════════════════════════════════════

  createNamespace (name: string): Storage {
    assertSafeIdentifier(name, 'namespace name')
    const root = this.#parentStorage ?? this
    const ns = new SQLiteStorage({
      path: root.#path,
      tablePrefix: `${this.#tablePrefix}${name}_`,
      cleanupIntervalMs: false, // children do not run cleanup
      vacuum: false,
      pragmas: this.#pragmas,
      logger: this.#logger
    })
    ns.#parentStorage = root
    return ns
  }

  /**
   * Clear all data (useful for testing).
   */
  async clear (): Promise<void> {
    if (!this.#db) return
    this.#assertSamePid()
    const db = this.#db
    await this.#runWrite(() => {
      db.exec(`
        DELETE FROM "${this.#queueTable}";
        DELETE FROM "${this.#processingTable}";
        DELETE FROM "${this.#jobsTable}";
        DELETE FROM "${this.#resultsTable}";
        DELETE FROM "${this.#errorsTable}";
        DELETE FROM "${this.#workersTable}";
        DELETE FROM "${this.#locksTable}";
      `)
    })
  }
}
