# Reliable Queue Library Design Document

A TypeScript library implementing the [reliable queue pattern](https://redis.antirez.com/fundamental/@platformatic/job-queue.md) with deduplication, request/response support, item removal, and result caching.

## Overview

This library provides a Redis-backed job queue with at-least-once delivery guarantees. It supports three usage modes: producer-only, consumer-only, or both combined.

**Compatible with [Redis](https://redis.io/) and [Valkey](https://valkey.io/).** Valkey is a Redis 7.2.4 fork with full command compatibility. All operations used by this library (BLMOVE, Lua scripts, pub/sub, sorted sets) work identically on both.

**This queue is designed for idempotent jobs.** Due to at-least-once semantics, jobs may be executed multiple times in failure scenarios (worker crash after processing but before acknowledgment, network partitions, lease expiration during slow processing). Job handlers must produce the same result when executed multiple times with the same input.

Examples of idempotent operations:
- Setting a value in a database (not incrementing)
- Sending an email with a unique message ID (provider deduplicates)
- Processing an image and storing by content hash
- HTTP PUT requests

Examples of non-idempotent operations (require external safeguards):
- Incrementing a counter
- Charging a credit card (use idempotency keys)
- Sending notifications without deduplication

## Core Requirements

1. **Two Invocation Modes**: Fire-and-forget and request/response
2. **Deduplication**: Prevent duplicate job processing using message IDs
3. **Item Removal**: Cancel pending jobs while maintaining deduplication integrity
4. **Result Cache**: Retrieve processed job results within a configurable TTL
5. **Type Stripping**: Native Node.js TypeScript execution (22.6+)
6. **Flexible Modes**: Producer, consumer, or combined operation

## Invocation Modes

The queue supports two distinct invocation patterns:

### Fire-and-Forget

Submit a job and return immediately without waiting for the result. Ideal for:
- Background tasks (sending emails, processing uploads)
- Jobs where the result isn't needed by the caller
- High-throughput scenarios where latency matters

```typescript
// Returns immediately after job is queued
await queue.enqueue('job-123', { email: 'user@example.com' });
```

### Request/Response

Submit a job and wait for the result, similar to an RPC call. Ideal for:
- Offloading CPU-intensive work to dedicated workers
- Distributed computation with result aggregation
- Synchronous APIs backed by async processing

```typescript
// Blocks until job completes and returns the result
const result = await queue.enqueueAndWait('job-123', { data: 'input' }, {
  timeout: 30000,
});
```

### Mode Comparison

| Aspect | Fire-and-Forget | Request/Response |
|--------|-----------------|------------------|
| Method | `enqueue()` | `enqueueAndWait()` |
| Returns | Immediately | When job completes |
| Result | Via `getResult()` later | Directly returned |
| Use case | Background tasks | RPC-style calls |
| Timeout | N/A | Configurable |
| Notification | Events | Pub/sub (instant) |

### Request/Response Performance

The request/response mode is optimized for low latency:

1. **Pub/sub notification**: No polling. The caller subscribes before enqueueing and receives instant notification when the worker completes.
2. **Lightweight notifications**: Pub/sub carries only the job ID and status — result is fetched separately to handle large payloads.
3. **Cached results**: If the job was already completed, the cached result is returned immediately without waiting.
4. **Subscribe-first**: Subscription happens before enqueue to eliminate race conditions.

## Architecture

### Redis Data Structures

```
{prefix}:queue                    # LIST - main work queue
{prefix}:processing:{workerId}    # LIST - per-worker in-flight jobs
{prefix}:jobs                     # HASH - message ID → state mapping (status + timestamp + workerId)
{prefix}:results:{messageId}      # STRING - job results with TTL
{prefix}:workers                  # SET - active worker IDs (avoids SCAN in reaper)
```

### Message Format

```typescript
interface QueueMessage<T> {
  id: string;                    // Unique message ID (used for deduplication)
  payload: T;                    // User-defined payload
  createdAt: number;             // Unix timestamp ms
  attempts: number;              // Retry count
  maxAttempts: number;           // Maximum retry attempts
  correlationId?: string;        // For request/response pattern
}
```

### Job States

The `{prefix}:jobs` hash tracks message states:

| State | Meaning |
|-------|---------|
| `queued` | Message is in the queue, not yet picked up |
| `processing` | Message is being processed by a worker |
| `failing` | Processing failed but retries remain |
| `completed` | Processing finished, result available |
| `failed` | Processing failed after max retries |

## API Design

### Storage Interface

The queue uses a pluggable storage backend. The library provides Redis and in-memory implementations, and users can provide custom implementations.

```typescript
interface Storage {
  // ═══════════════════════════════════════════════════════════════════
  // LIFECYCLE
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Initialize the storage connection.
   * Called once when Queue.start() is invoked.
   */
  connect(): Promise<void>;

  /**
   * Close the storage connection gracefully.
   * Called when Queue.stop() is invoked.
   */
  disconnect(): Promise<void>;

  // ═══════════════════════════════════════════════════════════════════
  // QUEUE OPERATIONS
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Atomically enqueue a job if not already present.
   * Must check for duplicates and set initial state in one atomic operation.
   *
   * @returns null if job was enqueued, existing state string if duplicate
   */
  enqueue(id: string, message: Buffer, timestamp: number): Promise<string | null>;

  /**
   * Blocking dequeue: move a job from main queue to worker's processing queue.
   * Should block up to `timeout` seconds if queue is empty.
   *
   * @returns The job message, or null if timeout
   */
  dequeue(workerId: string, timeout: number): Promise<Buffer | null>;

  /**
   * Move a job from worker's processing queue back to main queue.
   * Used for retries and stall recovery.
   */
  requeue(id: string, message: Buffer, workerId: string): Promise<void>;

  /**
   * Remove a job from worker's processing queue (after completion or failure).
   */
  ack(id: string, message: Buffer, workerId: string): Promise<void>;

  // ═══════════════════════════════════════════════════════════════════
  // JOB STATE
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Get the current state of a job.
   * State format: "status:timestamp[:workerId]"
   *
   * @returns State string, or null if job doesn't exist
   */
  getJobState(id: string): Promise<string | null>;

  /**
   * Set the state of a job.
   * Should also publish a notification for state changes.
   */
  setJobState(id: string, state: string): Promise<void>;

  /**
   * Delete a job from the jobs registry.
   * Used for cancellation.
   *
   * @returns true if job existed and was deleted
   */
  deleteJob(id: string): Promise<boolean>;

  /**
   * Get multiple job states in one call (batch operation).
   */
  getJobStates(ids: string[]): Promise<Map<string, string | null>>;

  // ═══════════════════════════════════════════════════════════════════
  // RESULTS
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Store a job result with TTL.
   */
  setResult(id: string, result: Buffer, ttlMs: number): Promise<void>;

  /**
   * Retrieve a stored job result.
   *
   * @returns Result buffer, or null if not found/expired
   */
  getResult(id: string): Promise<Buffer | null>;

  /**
   * Store a job error with TTL.
   */
  setError(id: string, error: Buffer, ttlMs: number): Promise<void>;

  /**
   * Retrieve a stored job error.
   */
  getError(id: string): Promise<Buffer | null>;

  // ═══════════════════════════════════════════════════════════════════
  // WORKERS
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Register a worker as active.
   * Should set a TTL so crashed workers are automatically removed.
   */
  registerWorker(workerId: string, ttlMs: number): Promise<void>;

  /**
   * Refresh worker registration (heartbeat).
   */
  refreshWorker(workerId: string, ttlMs: number): Promise<void>;

  /**
   * Unregister a worker (graceful shutdown).
   */
  unregisterWorker(workerId: string): Promise<void>;

  /**
   * Get list of all registered workers.
   */
  getWorkers(): Promise<string[]>;

  /**
   * Get all jobs in a worker's processing queue.
   * Used by reaper to find stalled jobs.
   */
  getProcessingJobs(workerId: string): Promise<Buffer[]>;

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
  subscribeToJob(
    id: string,
    handler: (status: 'completed' | 'failed' | 'failing') => void
  ): Promise<() => Promise<void>>;

  /**
   * Publish a job completion/failure/retry notification.
   * Called by worker after job finishes or is retried.
   */
  notifyJobComplete(id: string, status: 'completed' | 'failed' | 'failing'): Promise<void>;

  // ═══════════════════════════════════════════════════════════════════
  // EVENTS (for monitoring/reaper)
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Subscribe to all job state change events.
   * Used by reaper and monitoring.
   *
   * @returns Unsubscribe function
   */
  subscribeToEvents(
    handler: (id: string, event: string) => void
  ): Promise<() => Promise<void>>;

  /**
   * Publish a job state change event.
   */
  publishEvent(id: string, event: string): Promise<void>;

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
  completeJob(
    id: string,
    message: Buffer,
    workerId: string,
    result: Buffer,
    resultTtlMs: number
  ): Promise<void>;

  /**
   * Atomically fail a job:
   * - Set state to failed
   * - Store error
   * - Remove from processing queue
   * - Publish notification
   */
  failJob(
    id: string,
    message: Buffer,
    workerId: string,
    error: Buffer,
    errorTtlMs: number
  ): Promise<void>;

  /**
   * Atomically retry a job:
   * - Set state to failing
   * - Move from processing queue to main queue
   * - Publish event
   */
  retryJob(
    id: string,
    message: Buffer,
    workerId: string,
    attempts: number
  ): Promise<void>;
}
```

### Built-in Storage Implementations

```typescript
import { RedisStorage, MemoryStorage, FileStorage } from '@platformatic/job-queue';

// Redis/Valkey storage (distributed, persistent)
const redisStorage = new RedisStorage({
  host: 'localhost',
  port: 6379,
  prefix: 'myqueue',
});

// In-memory storage (single process, testing)
const memoryStorage = new MemoryStorage();

// File storage (single node, persistent)
const fileStorage = new FileStorage({
  directory: '/var/lib/job-queue',
});
```

### RedisStorage Configuration

```typescript
interface RedisStorageConfig {
  // Connection (one of these is required)
  host?: string;                    // Redis host (default: 'localhost')
  port?: number;                    // Redis port (default: 6379)
  url?: string;                     // Redis URL (alternative to host/port)
  client?: Redis;                   // Existing iovalkey client

  // Namespace
  prefix?: string;                  // Key prefix (default: 'jq')

  // Timeouts
  blockTimeout?: number;            // BLMOVE timeout in seconds (default: 5)
  processingQueueTTL?: number;      // TTL for processing queues (default: 7 days)
}
```

### MemoryStorage Characteristics

- Single process only — no distributed workers
- Jobs processed immediately by the local handler
- No persistence — all state lost on process exit
- No blocking dequeue — uses polling internally
- Full API compatibility with RedisStorage
- Useful for unit tests, development, and embedded use cases

### FileStorage

Filesystem-based storage for single-node persistence without Redis. Useful for edge deployments, embedded systems, or scenarios where Redis is unavailable but persistence is needed.

```typescript
import { FileStorage } from '@platformatic/job-queue';

const fileStorage = new FileStorage({
  directory: '/var/lib/job-queue',  // Base directory for all data
});
```

### FileStorage Configuration

```typescript
interface FileStorageConfig {
  directory: string;                  // Base directory for queue data (required)
}
```

### FileStorage Directory Structure

```
{directory}/
├── queue/                           # Main work queue
│   ├── 0000000001.job               # Job files ordered by sequence number
│   ├── 0000000002.job
│   └── ...
├── processing/                      # Per-worker processing queues
│   └── {workerId}/
│       ├── 0000000001.job
│       └── ...
├── jobs/                            # Job state files
│   └── {jobId}.json                 # State: { status, timestamp, workerId, attempts }
├── results/                         # Job results with TTL
│   └── {jobId}.result               # Binary result data
├── errors/                          # Job errors with TTL
│   └── {jobId}.error                # Binary error data
├── workers/                         # Worker registration
│   └── {workerId}.worker            # Heartbeat file with mtime-based TTL
├── notifications/                   # Job completion notifications
│   └── {jobId}.notify               # Notification files watched by waiters
├── sequence.lock                    # Sequence number lock file
└── sequence                         # Current sequence number
```

### FileStorage Characteristics

- **Single node only** — no distributed workers (files not shared across machines)
- **Persistent** — survives process restarts
- **Event-driven dequeue** — uses `fs.watch` on queue directory (no polling)
- **Atomic writes** — uses `fast-write-atomic` for safe file operations
- **Notification via fs.watch** — watches notification files for completion
- **TTL via mtime** — background cleaner removes expired files based on modification time

### FileStorage Implementation Notes

**Atomic Operations:**
```typescript
// Use fast-write-atomic for atomic file writes
import writeFileAtomic from 'fast-write-atomic';

async enqueue(id: string, message: Buffer, timestamp: number): Promise<string | null> {
  // Check if job exists (atomic read)
  const existing = await this.getJobState(id);
  if (existing) return existing;

  // Write job state atomically
  await writeFileAtomic.promise(
    this.jobPath(id),
    Buffer.from(JSON.stringify({ status: 'queued', timestamp }))
  );

  // Append to queue atomically
  const seq = await this.nextSequence();
  await writeFileAtomic.promise(
    path.join(this.queueDir, `${seq}.job`),
    message
  );

  return null;
}
```

**Blocking Dequeue (File Watching):**
```typescript
async dequeue(workerId: string, timeout: number): Promise<Buffer | null> {
  // First, try to acquire any existing job
  const existing = await this.tryAcquireNextJob(workerId);
  if (existing) return existing;

  // No jobs available, watch for new ones
  return new Promise((resolve) => {
    const timeoutId = setTimeout(() => {
      watcher.close();
      resolve(null);
    }, timeout * 1000);

    const watcher = fs.watch(this.queueDir, async (event, filename) => {
      if (event === 'rename' && filename?.endsWith('.job')) {
        const job = await this.tryAcquireNextJob(workerId);
        if (job) {
          clearTimeout(timeoutId);
          watcher.close();
          resolve(job);
        }
      }
    });
  });
}

private async tryAcquireNextJob(workerId: string): Promise<Buffer | null> {
  const files = await fs.readdir(this.queueDir);
  const sorted = files.filter(f => f.endsWith('.job')).sort();

  for (const file of sorted) {
    const acquired = await this.tryAcquireJob(file, workerId);
    if (acquired) return acquired;
  }
  return null;
}
```

**Notification via fs.watch:**
```typescript
async subscribeToJob(
  id: string,
  handler: (status: 'completed' | 'failed' | 'failing') => void
): Promise<() => Promise<void>> {
  const notifyPath = this.notifyPath(id);

  const watcher = fs.watch(path.dirname(notifyPath), (event, filename) => {
    if (filename === path.basename(notifyPath)) {
      fs.readFile(notifyPath, 'utf8').then(status => {
        handler(status as 'completed' | 'failed' | 'failing');
      });
    }
  });

  return async () => { watcher.close(); };
}

async notifyJobComplete(id: string, status: 'completed' | 'failed' | 'failing'): Promise<void> {
  await writeFileAtomic.promise(this.notifyPath(id), Buffer.from(status));
}
```

**TTL Cleanup (Background):**
```typescript
private async cleanupExpired(): Promise<void> {
  // Results
  for (const file of await fs.readdir(this.resultsDir)) {
    const stat = await fs.stat(path.join(this.resultsDir, file));
    if (Date.now() - stat.mtimeMs > this.resultTTL) {
      await fs.unlink(path.join(this.resultsDir, file));
    }
  }

  // Worker heartbeats
  for (const file of await fs.readdir(this.workersDir)) {
    const stat = await fs.stat(path.join(this.workersDir, file));
    if (Date.now() - stat.mtimeMs > this.workerTTL) {
      await fs.unlink(path.join(this.workersDir, file));
    }
  }
}
```

### FileStorage Limitations

| Aspect | FileStorage | RedisStorage |
|--------|-------------|--------------|
| Distribution | Single node | Multi-node |
| Persistence | Yes | Yes (with AOF/RDB) |
| Dequeue latency | ~0 (fs.watch) | ~0 (blocking) |
| Throughput | ~1000 jobs/sec | ~100k jobs/sec |
| Atomic operations | Atomic writes | Lua scripts |
| Notifications | fs.watch | Pub/sub |
| Use case | Edge, embedded | Production |

### FileStorage Use Cases

- **Edge deployments** — IoT devices, edge servers without Redis
- **Embedded applications** — Desktop apps, CLI tools needing persistent queues
- **Development** — Local development with persistence (unlike MemoryStorage)
- **Simple deployments** — Single-server apps where Redis is overkill
- **Offline-first** — Apps that need to queue jobs while disconnected

### Serialization

The queue supports pluggable serialization for binary encoding. Messages and results are serialized before storing and deserialized when retrieved.

```typescript
interface Serde<T> {
  serialize(value: T): Buffer;
  deserialize(buffer: Buffer): T;
}
```

The library ships with a JSON serde (default) which can be accessed directly if needed:

```typescript
import { JsonSerde, createJsonSerde } from '@platformatic/job-queue';

// Using the class directly
const serde = new JsonSerde<MyType>();

// Using the factory function
const serde = createJsonSerde<MyType>();
```

Custom serdes can be provided for binary formats like MessagePack, CBOR, or Protocol Buffers:

```typescript
import { type Serde } from '@platformatic/job-queue';
import { encode, decode } from '@msgpack/msgpack';

// MessagePack serde example
const msgpackSerde = <T>(): Serde<T> => ({
  serialize: (value) => Buffer.from(encode(value)),
  deserialize: (buffer) => decode(buffer) as T,
});

// Protocol Buffers serde example
import { MyMessage } from './generated/my_message_pb.ts';

const protobufSerde: Serde<MyMessage> = {
  serialize: (value) => Buffer.from(value.toBinary()),
  deserialize: (buffer) => MyMessage.fromBinary(buffer),
};
```

### Configuration

```typescript
interface QueueConfig<TPayload, TResult> {
  // Storage backend (required)
  storage: Storage;

  // Serialization
  payloadSerde?: Serde<TPayload>;   // Payload serde (default: JSON)
  resultSerde?: Serde<TResult>;     // Result serde (default: JSON)

  // Consumer options
  workerId?: string;                   // Unique worker ID (default: random UUID)
  concurrency?: number;                // Parallel job processing (default: 1)
  blockTimeout?: number;               // Blocking dequeue timeout in seconds (default: 5)
  maxRetries?: number;                 // Default max retry attempts (default: 3)

  // Stalled job recovery
  visibilityTimeout?: number;          // Max processing time before job is considered stalled (default: 30000ms)
  processingQueueTTL?: number;         // TTL for processing queue keys in ms (default: 604800000 = 7 days)

  // Result cache options
  resultTTL?: number;                  // TTL for stored results and errors in ms (default: 3600000 = 1 hour)
}
```

### Memory Storage (Testing)

For testing and single-process scenarios, use the in-memory storage:

```typescript
import { Queue, MemoryStorage } from '@platformatic/job-queue';

const queue = new Queue<MyPayload, MyResult>({
  storage: new MemoryStorage(),
  concurrency: 1,
});

queue.execute(async (job) => {
  return processJob(job.payload);
});

await queue.start();

// Works exactly like Redis storage
const result = await queue.enqueueAndWait('job-1', payload);
```

**MemoryStorage characteristics:**

- Single process only — no distributed workers
- Jobs processed immediately by the local handler
- No persistence — all state lost on process exit
- No stalled job recovery needed (single worker)
- Full API compatibility with RedisStorage
- Useful for unit tests, development, and embedded use cases

### Queue Class

```typescript
class Queue<TPayload, TResult = void> {
  constructor(config: QueueConfig);

  // Lifecycle
  async start(): Promise<void>;
  async stop(): Promise<void>;

  // Fire-and-forget mode
  async enqueue(
    id: string,
    payload: TPayload,
    options?: EnqueueOptions
  ): Promise<EnqueueResult>;

  // Request/response mode
  async enqueueAndWait(
    id: string,
    payload: TPayload,
    options?: EnqueueAndWaitOptions
  ): Promise<TResult>;

  // Job management
  async cancel(id: string): Promise<CancelResult>;
  async getResult(id: string): Promise<TResult | null>;
  async getStatus(id: string): Promise<MessageStatus | null>;

  // Consumer
  execute(handler: JobHandler<TPayload, TResult>): void;

  // Events (from internal Consumer)
  on(event: 'error', handler: (error: Error) => void): void;
  on(event: 'completed', handler: (id: string, result: TResult) => void): void;
  on(event: 'failed', handler: (id: string, error: Error) => void): void;
}

// Note: 'stalled' events are emitted by the Reaper class, not Queue
class Reaper<TPayload> extends EventEmitter {
  on(event: 'error', handler: (error: Error) => void): void;
  on(event: 'stalled', handler: (id: string) => void): void;
}
```

### Supporting Types

```typescript
interface EnqueueOptions {
  maxAttempts?: number;
}

interface EnqueueAndWaitOptions extends EnqueueOptions {
  timeout?: number;  // Max wait time in ms (default: 30000)
}

type EnqueueResult =
  | { status: 'queued' }
  | { status: 'duplicate'; existingState: MessageState }
  | { status: 'completed'; result: TResult };  // Already processed, return cached

type CancelResult =
  | { status: 'cancelled' }
  | { status: 'not_found' }
  | { status: 'processing' }  // Cannot cancel, already being processed
  | { status: 'completed' };  // Already done, nothing to cancel

interface MessageStatus {
  id: string;
  state: MessageState;
  createdAt: number;
  attempts: number;
  result?: TResult;
  error?: string;
}

type MessageState = 'queued' | 'processing' | 'failing' | 'completed' | 'failed';

type JobHandler<TPayload, TResult> =
  | ((job: Job<TPayload>) => Promise<TResult>)
  | ((job: Job<TPayload>, callback: (err: Error | null, result?: TResult) => void) => void);

interface Job<TPayload> {
  id: string;
  payload: TPayload;
  attempts: number;
  signal: AbortSignal;  // For cancellation/timeout
}
```

### Worker ID Strategy

Each worker needs a unique ID to track its in-flight jobs.

**Random UUIDs are safe** — the storage backend handles cleanup of orphaned worker data. Use whatever ID generation works for your environment:

```typescript
import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';

// Option 1: Random UUID (safe - storage handles cleanup)
const workerId = randomUUID();

// Option 2: Hostname (good for VMs, bare metal)
const workerId = hostname();

// Option 3: Kubernetes pod name
const workerId = process.env.HOSTNAME ?? hostname();

// Option 4: Process-scoped
const workerId = `${hostname()}-${process.pid}`;
```

**Auto-generated ID (default)**

If `workerId` is omitted, the library generates a random UUID.

**Why stable IDs might still be useful**

- Easier to monitor/debug (correlate logs with worker IDs)
- Metrics grouping by worker

**Cleanup on shutdown**

```typescript
closeWithGrace({ delay: 10000 }, async () => {
  await queue.stop();  // Finishes in-flight jobs, unregisters worker
});
```

## Operations Detail

### Enqueue Flow

```
enqueue(id, payload)
    │
    ▼
┌─────────────────────────────────┐
│ HGET {prefix}:jobs {id}         │
│ Check if message ID exists      │
└─────────────────────────────────┘
    │
    ├── exists with state "completed" ──► Return cached result
    │
    ├── exists with state "queued" or "processing" ──► Return duplicate
    │
    └── not exists or "failed"
            │
            ▼
    ┌─────────────────────────────────┐
    │ MULTI                           │
    │   HSET {prefix}:jobs {id}       │
    │        "queued:{timestamp}"     │
    │   LPUSH {prefix}:queue {msg}    │
    │   PUBLISH {prefix}:events       │
    │           {id}:queued           │
    │ EXEC                            │
    └─────────────────────────────────┘
            │
            ▼
    Return { status: 'queued' }
```

### Consumer Flow

```
start()
    │
    ▼
┌─────────────────────────────────────────────┐
│ SADD {prefix}:workers {workerId}             │
│ (register worker for reaper)                 │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────┐
│ Loop:                                        │
│   BLMOVE {prefix}:queue                      │
│          {prefix}:processing:{workerId}      │
│          RIGHT LEFT {blockTimeout}           │
└─────────────────────────────────────────────┘
    │
    ▼ (message received)
    │
┌─────────────────────────────────────────────┐
│ MULTI                                        │
│   EXPIRE {prefix}:processing:{workerId}      │
│          {processingQueueTTL}                │
│   HSET {prefix}:jobs {id}                    │
│        "processing:{timestamp}:{workerId}"   │
│   PUBLISH {prefix}:events                    │
│           {id}:processing                    │
│ EXEC                                         │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────┐
│ Execute job handler                          │
│ (with AbortSignal for timeout)               │
└─────────────────────────────────────────────┘
    │
    ├── success
    │       │
    │       ▼
    │   ┌─────────────────────────────────┐
    │   │ MULTI                           │
    │   │   SET {prefix}:results:{id}     │
    │   │       {result} EX {resultTTL}   │
    │   │   HSET {prefix}:jobs {id}       │
    │   │        "completed:{timestamp}"  │
    │   │   LREM {prefix}:processing:     │
    │   │        {workerId} 1 {msg}       │
    │   │   PUBLISH {prefix}:events       │
    │   │           {id}:completed        │
    │   │ EXEC                            │
    │   └─────────────────────────────────┘
    │
    └── failure
            │
            ▼
    ┌─────────────────────────────────────────────┐
    │ if attempts < maxAttempts:                   │
    │   MULTI                                      │
    │     HSET {prefix}:jobs {id}                  │
    │          "failing:{timestamp}:{attempts}"    │
    │     LMOVE processing → queue                 │
    │     PUBLISH {prefix}:events                  │
    │             {id}:failing                     │
    │   EXEC                                       │
    │ else:                                        │
    │   MULTI                                      │
    │     HSET {prefix}:jobs {id}                  │
    │          "failed:{timestamp}"                │
    │     LREM from processing                     │
    │     PUBLISH {prefix}:events                  │
    │             {id}:failed                      │
    │   EXEC                                       │
    └─────────────────────────────────────────────┘
```

### Request/Response Flow (enqueueAndWait)

Optimized for minimal latency — pure pub/sub, no polling.

```
enqueueAndWait(id, payload, { timeout })
    │
    ▼
┌─────────────────────────────────────────────┐
│ SUBSCRIBE {prefix}:notifications:{id}       │
│ (subscribe BEFORE enqueue to avoid races)   │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│ enqueue(id, payload)            │
└─────────────────────────────────┘
    │
    ├── status: 'completed' ──► GET result, return immediately
    │
    ├── status: 'duplicate' ──► Wait on subscription
    │
    └── status: 'queued'
            │
            ▼
    ┌─────────────────────────────────────────────┐
    │ Wait for notification                        │
    │ (blocks until message received or timeout)  │
    └─────────────────────────────────────────────┘
            │
            ├── 'completed' notification ──► GET result, return
            │
            ├── 'failed' notification ──► GET error, throw
            │
            └── timeout ──► Throw TimeoutError
```

**Key optimizations:**
- Subscribe before enqueue to avoid race conditions
- Pub/sub carries only notification (not the result) — handles large payloads
- Result fetched with single GET after notification
- Minimal latency: enqueue → worker processes → notification → fetch result

### Cancel Flow

```
cancel(id)
    │
    ▼
┌─────────────────────────────────┐
│ HGET {prefix}:jobs {id}         │
└─────────────────────────────────┘
    │
    ├── not found ──► Return { status: 'not_found' }
    │
    ├── "completed" ──► Return { status: 'completed' }
    │
    ├── "processing" ──► Return { status: 'processing' }
    │                    (cannot cancel mid-processing)
    │
    └── "queued"
            │
            ▼
    ┌─────────────────────────────────┐
    │ MULTI                           │
    │   HDEL {prefix}:jobs {id}       │
    │   PUBLISH {prefix}:events       │
    │           {id}:cancelled        │
    │ EXEC                            │
    └─────────────────────────────────┘
            │
            ▼
    Return { status: 'cancelled' }
```

Note: We don't remove from the LIST directly (O(n) operation). The job entry is deleted from the jobs hash. When a worker picks it up, it checks the jobs hash first — if the entry is missing, the job is skipped and removed from the processing queue.

### Stalled Job Recovery

Recovery is **event-driven** using pub/sub, with the `{prefix}:jobs` hash as the source of truth for job state.

#### Event-Based Architecture

All state changes publish to `{prefix}:events` channel:
- `{id}:queued` — job added to queue
- `{id}:processing` — worker picked up job
- `{id}:failing` — job failed, will retry
- `{id}:completed` — job finished successfully
- `{id}:failed` — job failed permanently
- `{id}:cancelled` — job was cancelled
- `{id}:stalled` — job was detected as stalled and requeued

#### Background Reaper

The reaper subscribes to events and maintains timers for processing jobs. It uses the `{prefix}:workers` set to know which workers to check:

```
on start:
    │
    ▼
┌─────────────────────────────────────────────┐
│ SUBSCRIBE {prefix}:events                    │
│ SMEMBERS {prefix}:workers                    │
│ For each worker, check processing queue     │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────┐
│ On {id}:processing event:                    │
│   Start timer for visibilityTimeout          │
│   If timer fires, check job state and        │
│   requeue if still processing                │
└─────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────┐
│ On {id}:completed/failed event:              │
│   Cancel timer for that job                  │
└─────────────────────────────────────────────┘
```

#### Requeue Stalled Job

```
recoverStalledJob(id, workerId):
    │
    ▼
┌─────────────────────────────────────────────┐
│ Lua script (atomic):                         │
│   local state = HGET {prefix}:jobs {id}     │
│   if state starts with "processing:" then   │
│     -- Get message from processing queue    │
│     -- LREM + LPUSH to main queue           │
│     -- Update state to "queued"             │
│     -- PUBLISH {id}:stalled                 │
│     return "requeued"                       │
│   end                                        │
│   return "already_handled"                   │
└─────────────────────────────────────────────┘
```

#### Recovery Timing Summary

| Scenario | Recovery Time |
|----------|---------------|
| Stalled job (any mode) | ~visibilityTimeout (reaper detects via timer) |
| Worker graceful shutdown | Immediate (worker requeues own jobs) |

Note: `enqueueAndWait` does not poll for stalls. If a worker stalls, the background reaper detects it and requeues the job, which then triggers a notification to the waiting caller.

## Deduplication Strategy

### ID Generation

Users provide message IDs. For content-based deduplication, users can hash the payload:

```typescript
import { createHash } from 'node:crypto';

function contentId(payload: unknown): string {
  return createHash('sha256')
    .update(JSON.stringify(payload))
    .digest('hex')
    .slice(0, 16);
}

await queue.enqueue(contentId(job), job);
```

### Jobs Hash Cleanup

The jobs hash stores job states indefinitely. Results and errors are stored separately with TTL (via `resultTTL`). In newer Redis 7.4+ / Valkey 8+, per-field TTL on hashes (`HEXPIRE`) could be used for automatic job state cleanup.

**Note:** Job state entries persist to allow duplicate detection. Implement application-level cleanup if needed.

### Race Condition Handling

The enqueue operation uses a Lua script for atomic "check and set":

```typescript
// Lua script for atomic enqueue with dedup check
const ENQUEUE_SCRIPT = `
local jobsKey = KEYS[1]
local queueKey = KEYS[2]
local eventsChannel = KEYS[3]
local messageId = ARGV[1]
local message = ARGV[2]
local timestamp = ARGV[3]

local existing = redis.call('HGET', jobsKey, messageId)
if existing then
  return existing
end

redis.call('HSET', jobsKey, messageId, 'queued:' .. timestamp)
redis.call('LPUSH', queueKey, message)
redis.call('PUBLISH', eventsChannel, messageId .. ':queued')
return nil
`;
```

## File Structure

```
src/
├── index.ts                 # Public API exports
├── queue.ts                 # Main Queue class
├── producer.ts              # Producer functionality
├── consumer.ts              # Consumer functionality
├── reaper.ts                # Stalled job recovery
├── types.ts                 # Type definitions
├── errors.ts                # Custom errors
├── serde/
│   └── index.ts             # Serde interface + JSON serde
├── storage/
│   ├── index.ts             # Storage interface export
│   ├── types.ts             # Storage interface definition
│   ├── redis.ts             # RedisStorage implementation (includes Lua scripts)
│   ├── memory.ts            # MemoryStorage implementation
│   └── file.ts              # FileStorage implementation
├── types/
│   └── fast-write-atomic.d.ts  # Type declarations for fast-write-atomic
└── utils/
    └── id.ts                # ID generation helpers

test/
├── queue.test.ts            # Queue lifecycle and processing tests
├── deduplication.test.ts    # Deduplication behavior tests
├── request-response.test.ts # enqueueAndWait tests
├── reaper.test.ts           # Stalled job recovery tests
├── memory-storage.test.ts   # MemoryStorage tests
├── file-storage.test.ts     # FileStorage tests
├── redis-storage.test.ts    # RedisStorage tests
├── helpers/
│   └── events.ts            # Event-driven test utilities
├── fixtures/
│   └── redis.ts             # Test Redis setup
└── integration/
    └── e2e.test.ts          # End-to-end integration tests
```

## TypeScript Configuration

### eslint.config.js

```javascript
import neostandard from 'neostandard'

export default neostandard({
  ts: true,
})
```

### tsconfig.json (Development)

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "noEmit": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "verbatimModuleSyntax": true,
    "allowImportingTsExtensions": true,
    "lib": ["ES2022"],
    "types": ["node"]
  },
  "include": ["src/**/*.ts", "test/**/*.ts"],
  "exclude": ["node_modules"]
}
```

### tsconfig.build.json (Publishing)

```json
{
  "extends": "./tsconfig.json",
  "compilerOptions": {
    "noEmit": false,
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "outDir": "dist",
    "rootDir": "src",
    "rewriteRelativeImportExtensions": true
  },
  "include": ["src/**/*.ts"],
  "exclude": ["node_modules", "test"]
}
```

### package.json

```json
{
  "name": "@platformatic/job-queue",
  "type": "module",
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.js"
    }
  },
  "files": ["dist"],
  "scripts": {
    "build": "tsc -p tsconfig.build.json",
    "clean": "rm -rf dist",
    "lint": "eslint",
    "lint:fix": "eslint --fix",
    "prepublishOnly": "npm run clean && npm run build",
    "test": "node --test test/*.test.ts",
    "typecheck": "tsc --noEmit"
  },
  "engines": {
    "node": ">=22.19.0"
  },
  "dependencies": {
    "fast-write-atomic": "^0.4.0",
    "iovalkey": "^0.2.0"
  },
  "devDependencies": {
    "eslint": "^9.0.0",
    "neostandard": "^0.12.0",
    "typescript": "^5.7.0"
  }
}
```

## Usage Examples

### Producer Only

```typescript
import { Queue, RedisStorage } from '@platformatic/job-queue';

const queue = new Queue<{ email: string; template: string }, void>({
  storage: new RedisStorage({ host: 'localhost', port: 6379, prefix: 'email-queue' }),
});

await queue.start();

// Fire and forget
await queue.enqueue('email-123', {
  email: 'user@example.com',
  template: 'welcome',
});

// Deduplicated - same ID won't be queued twice
const result = await queue.enqueue('email-123', {
  email: 'user@example.com',
  template: 'welcome',
});
// result.status === 'duplicate'

await queue.stop();
```

### Consumer Only

```typescript
import { Queue, RedisStorage } from '@platformatic/job-queue';

const queue = new Queue<{ email: string; template: string }, void>({
  storage: new RedisStorage({ host: 'localhost', port: 6379, prefix: 'email-queue' }),
  workerId: 'worker-1',
  concurrency: 5,
});

// Handler MUST be idempotent - it may be called multiple times for the same job
queue.execute(async (job) => {
  // Use job.id as idempotency key with email provider
  await sendEmail(job.payload.email, job.payload.template, {
    idempotencyKey: job.id,
  });
});

queue.on('error', (error) => {
  console.error('Queue error:', error);
});

await queue.start();

// Graceful shutdown
process.on('SIGTERM', async () => {
  await queue.stop();
});
```

### Request/Response Pattern

```typescript
import { Queue, RedisStorage } from '@platformatic/job-queue';

interface ImageJob {
  url: string;
  width: number;
  height: number;
}

interface ImageResult {
  thumbnailUrl: string;
  processingTime: number;
}

const queue = new Queue<ImageJob, ImageResult>({
  storage: new RedisStorage({ host: 'localhost', port: 6379, prefix: 'image-processor' }),
  resultTTL: 3600000, // 1 hour
});

// Producer side - wait for result
const result = await queue.enqueueAndWait(
  'img-abc123',
  { url: 'https://example.com/photo.jpg', width: 200, height: 200 },
  { timeout: 60000 }
);
console.log('Thumbnail:', result.thumbnailUrl);

// Consumer side - naturally idempotent: same input image produces same thumbnail
queue.execute(async (job) => {
  const thumbnail = await processImage(job.payload);
  return {
    thumbnailUrl: thumbnail.url,
    processingTime: Date.now() - job.createdAt,
  };
});
```

### Custom Serde (MessagePack)

```typescript
import { Queue, type Serde } from '@platformatic/job-queue';
import { encode, decode } from '@msgpack/msgpack';

interface VideoJob {
  videoId: string;
  frames: Buffer;
}

interface VideoResult {
  thumbnails: Buffer[];
}

const msgpackSerde = <T>(): Serde<T> => ({
  serialize: (value) => Buffer.from(encode(value)),
  deserialize: (buffer) => decode(buffer) as T,
});

const queue = new Queue<VideoJob, VideoResult>({
  storage: new RedisStorage({ host: 'localhost', port: 6379, prefix: 'video-processor' }),
  payloadSerde: msgpackSerde<VideoJob>(),
  resultSerde: msgpackSerde<VideoResult>(),
});
```

### Retrieving Cached Results

```typescript
// Check if already processed
const status = await queue.getStatus('job-123');

if (status?.state === 'completed') {
  const result = await queue.getResult('job-123');
  console.log('Cached result:', result);
} else {
  // Enqueue if not processed
  await queue.enqueue('job-123', payload);
}
```

### Cancellation

```typescript
// Cancel a pending job
const result = await queue.cancel('job-456');

switch (result.status) {
  case 'cancelled':
    console.log('Job cancelled');
    break;
  case 'processing':
    console.log('Cannot cancel - job is being processed');
    break;
  case 'completed':
    console.log('Job already completed');
    break;
  case 'not_found':
    console.log('Job not found');
    break;
}
```

## Error Handling

```typescript
import {
  Queue,
  TimeoutError,
  MaxRetriesError,
  JobFailedError,
  JobNotFoundError,
  JobCancelledError,
  StorageError
} from '@platformatic/job-queue';

queue.on('failed', (id, error) => {
  if (error instanceof MaxRetriesError) {
    // Move to dead letter queue or alert
    await deadLetterQueue.enqueue(id, error.payload);
  }
});

try {
  await queue.enqueueAndWait('job-1', payload, { timeout: 5000 });
} catch (error) {
  if (error instanceof TimeoutError) {
    // Job may still complete later - check status
    const status = await queue.getStatus('job-1');
  } else if (error instanceof JobFailedError) {
    // Job failed after max retries
    console.error('Job failed:', error.originalError);
  }
}

// Duplicates are handled via return value, not exceptions
const result = await queue.enqueue('job-1', payload);
if (result.status === 'duplicate') {
  console.log('Job already exists with state:', result.existingState);
}
```

## Graceful Shutdown

```typescript
import closeWithGrace from 'close-with-grace';

const queue = new Queue(config);

queue.execute(handler);
await queue.start();

closeWithGrace({ delay: 10000 }, async ({ signal }) => {
  console.log(`${signal} received, stopping queue...`);

  // stop() waits for in-flight jobs to complete
  // or until delay timeout
  await queue.stop();
});
```

## Testing with MemoryStorage

Use MemoryStorage for unit tests without Redis:

```typescript
import { describe, it, beforeEach } from 'node:test';
import { Queue, MemoryStorage } from '@platformatic/job-queue';

describe('MyService', () => {
  let queue: Queue<MyPayload, MyResult>;

  beforeEach(() => {
    queue = new Queue<MyPayload, MyResult>({
      storage: new MemoryStorage(),
    });

    queue.execute(async (job) => {
      return myService.process(job.payload);
    });
  });

  it('should process job and return result', async (t) => {
    await queue.start();

    const result = await queue.enqueueAndWait('test-1', { data: 'test' });

    t.assert.deepEqual(result, { processed: true });

    await queue.stop();
  });

  it('should deduplicate jobs', async (t) => {
    await queue.start();

    const result1 = await queue.enqueue('job-1', payload);
    const result2 = await queue.enqueue('job-1', payload);

    t.assert.equal(result1.status, 'queued');
    t.assert.equal(result2.status, 'duplicate');

    await queue.stop();
  });
});
```

## Performance Considerations

### Memory Usage

- Keep payloads small; store large data externally and reference by ID
- Monitor the dedup hash size; adjust `deduplicationTTL` accordingly
- Result TTL should match actual retrieval patterns

## Limitations

1. **Idempotent jobs only**: This queue provides at-least-once delivery. Jobs may be processed multiple times after crashes, lease expirations, or network issues. Handlers **must** be idempotent—processing the same job twice must produce the same result as processing it once.

2. **No priority queues**: All jobs are FIFO. Priority support would require multiple queues.

3. **Single Redis instance**: No built-in clustering support. Use Redis Cluster or a proxy.

4. **No delayed jobs**: Jobs are processed immediately. For delayed execution, consider a separate scheduler.

5. **List-based removal is O(n)**: We use cancellation marking instead of direct removal to avoid performance issues.

## Future Considerations

- Priority queues (multiple Redis lists)
- Delayed/scheduled jobs
- Rate limiting per job type
- Job dependencies (job B waits for job A)
- Metrics and observability hooks
- Redis Cluster support
