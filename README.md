# @platformatic/job-queue

A reliable job queue for Node.js with deduplication, request/response support, and pluggable storage backends.

## Features

- **Deduplication** - Prevents duplicate job processing with configurable result caching
- **Request/Response** - Enqueue jobs and wait for results with `enqueueAndWait()`
- **Multiple Storage Backends** - Redis/Valkey, filesystem, or in-memory
- **Automatic Retries** - Configurable retry attempts with exponential backoff
- **Stalled Job Recovery** - Reaper automatically recovers jobs from crashed workers
- **Graceful Shutdown** - Complete in-flight jobs before stopping
- **TypeScript Native** - Full type safety with generic payload and result types
- **Node.js 22.19+** - Uses native TypeScript type stripping

## Installation

```bash
npm install @platformatic/job-queue
```

## Quick Start

```typescript
import { Queue, MemoryStorage } from '@platformatic/job-queue'

// Create a queue with in-memory storage
const storage = new MemoryStorage()
const queue = new Queue<{ email: string }, { sent: boolean }>({
  storage,
  concurrency: 5
})

// Register a job handler
queue.execute(async (job) => {
  console.log(`Processing job ${job.id}:`, job.payload)
  // Send email...
  return { sent: true }
})

// Start the queue
await queue.start()

// Enqueue jobs
await queue.enqueue('email-1', { email: 'user@example.com' })

// Or wait for the result
const result = await queue.enqueueAndWait('email-2', { email: 'another@example.com' }, {
  timeout: 30000
})
console.log('Result:', result) // { sent: true }

// Graceful shutdown
await queue.stop()
```

## API Reference

### Queue

The main class that combines producer and consumer functionality.

```typescript
import { Queue } from '@platformatic/job-queue'

const queue = new Queue<TPayload, TResult>(config)
```

#### Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `storage` | `Storage` | *required* | Storage backend instance |
| `workerId` | `string` | `uuid()` | Unique identifier for this worker |
| `concurrency` | `number` | `1` | Number of jobs to process in parallel |
| `maxRetries` | `number` | `3` | Maximum retry attempts for failed jobs |
| `blockTimeout` | `number` | `5` | Seconds to wait when polling for jobs |
| `visibilityTimeout` | `number` | `30000` | Milliseconds before a processing job is considered stalled |
| `resultTTL` | `number` | `3600000` | Milliseconds to cache job results (1 hour) |
| `payloadSerde` | `Serde<TPayload>` | `JsonSerde` | Custom serializer for job payloads |
| `resultSerde` | `Serde<TResult>` | `JsonSerde` | Custom serializer for job results |

#### Methods

##### `queue.start(): Promise<void>`

Connect to storage and start processing jobs.

##### `queue.stop(): Promise<void>`

Gracefully stop processing. Waits for in-flight jobs to complete.

##### `queue.execute(handler): void`

Register a job handler. Call before or after `start()`.

```typescript
queue.execute(async (job) => {
  // job.id - unique job identifier
  // job.payload - the job data
  // job.attempts - current attempt number (starts at 1)
  // job.signal - AbortSignal for cancellation
  return result
})
```

##### `queue.enqueue(id, payload, options?): Promise<EnqueueResult>`

Enqueue a job (fire-and-forget).

```typescript
const result = await queue.enqueue('job-123', { data: 'value' })

// result.status can be:
// - 'queued': Job was added to the queue
// - 'duplicate': Job with this ID already exists
// - 'completed': Job already completed (returns cached result)
```

##### `queue.enqueueAndWait(id, payload, options?): Promise<TResult>`

Enqueue a job and wait for the result.

```typescript
const result = await queue.enqueueAndWait('job-123', payload, {
  timeout: 30000,      // Timeout in milliseconds
  maxAttempts: 5       // Override default max retries
})
```

Throws `TimeoutError` if the job doesn't complete within the timeout.

##### `queue.cancel(id): Promise<CancelResult>`

Cancel a queued job.

```typescript
const result = await queue.cancel('job-123')

// result.status can be:
// - 'cancelled': Job was successfully cancelled
// - 'not_found': Job doesn't exist
// - 'processing': Job is currently being processed (cannot cancel)
// - 'completed': Job already completed
```

##### `queue.getResult(id): Promise<TResult | null>`

Get the cached result of a completed job.

##### `queue.getStatus(id): Promise<MessageStatus | null>`

Get the current status of a job.

```typescript
const status = await queue.getStatus('job-123')
// {
//   id: 'job-123',
//   state: 'completed', // 'queued' | 'processing' | 'failing' | 'completed' | 'failed'
//   createdAt: 1234567890,
//   attempts: 1,
//   result?: { ... },
//   error?: { message: 'Error message', code?: 'ERROR_CODE', stack?: '...' }
// }
```

#### Events

```typescript
// Lifecycle events
queue.on('started', () => {
  console.log('Queue started')
})

queue.on('stopped', () => {
  console.log('Queue stopped')
})

// Job events
queue.on('enqueued', (id) => {
  console.log(`Job ${id} was enqueued`)
})

queue.on('completed', (id, result) => {
  console.log(`Job ${id} completed:`, result)
})

queue.on('failed', (id, error) => {
  console.log(`Job ${id} failed:`, error.message)
})

queue.on('failing', (id, error, attempt) => {
  console.log(`Job ${id} failed attempt ${attempt}, will retry:`, error.message)
})

queue.on('requeued', (id) => {
  console.log(`Job ${id} was returned to queue (e.g., during graceful shutdown)`)
})

queue.on('cancelled', (id) => {
  console.log(`Job ${id} was cancelled`)
})

// Error events
queue.on('error', (error) => {
  console.error('Queue error:', error)
})
```

### Storage Backends

#### MemoryStorage

In-memory storage for development and testing.

```typescript
import { MemoryStorage } from '@platformatic/job-queue'

const storage = new MemoryStorage()
```

#### RedisStorage

Production-ready storage using Redis or Valkey.

```typescript
import { RedisStorage } from '@platformatic/job-queue'

const storage = new RedisStorage({
  url: 'redis://localhost:6379',
  keyPrefix: 'myapp:'  // Optional prefix for all keys
})
```

Features:
- Atomic operations via Lua scripts
- Blocking dequeue with `BLMOVE`
- Pub/sub for real-time notifications
- Compatible with Redis 7+ and Valkey 8+

#### FileStorage

Filesystem-based storage for single-node deployments.

```typescript
import { FileStorage } from '@platformatic/job-queue'

const storage = new FileStorage({
  basePath: '/var/lib/myapp/queue'
})
```

Features:
- Atomic writes with `fast-write-atomic`
- FIFO ordering via sequence numbers
- `fs.watch` for real-time notifications
- Survives process restarts

### Reaper

The Reaper monitors for stalled jobs and requeues them. Use when running multiple workers.

```typescript
import { Reaper } from '@platformatic/job-queue'

const reaper = new Reaper({
  storage,
  visibilityTimeout: 30000  // Same as your queue's visibilityTimeout
})

await reaper.start()

reaper.on('stalled', (id) => {
  console.log(`Job ${id} was stalled and requeued`)
})

// On shutdown
await reaper.stop()
```

The Reaper uses event-based monitoring: it subscribes to job state changes and sets per-job timers. An initial scan at startup catches any jobs that were processing before the Reaper started.

#### Leader Election

For high availability, you can run multiple Reaper instances with leader election enabled. Only one instance will be active at a time, and if it fails, another will automatically take over.

```typescript
const reaper = new Reaper({
  storage,
  visibilityTimeout: 30000,
  leaderElection: {
    enabled: true,              // Enable leader election (default: false)
    lockTTL: 30000,             // Lock expiry in ms (default: 30s)
    renewalInterval: 10000,     // Renew lock every 10s (default: 1/3 of TTL)
    acquireRetryInterval: 5000  // Followers retry every 5s (default: 5s)
  }
})

reaper.on('leadershipAcquired', () => {
  console.log('This reaper is now the leader')
})

reaper.on('leadershipLost', () => {
  console.log('This reaper lost leadership')
})

await reaper.start()
```

Leader election uses Redis's `SET NX PX` pattern for atomic lock acquisition:
- The leader acquires a lock with a TTL and renews it periodically
- If the leader stops gracefully, it releases the lock immediately
- If the leader crashes, the lock expires and a follower takes over
- Only RedisStorage supports leader election; other storage backends will emit an error

### Custom Serialization

Implement the `Serde` interface for custom serialization:

```typescript
import { Serde } from '@platformatic/job-queue'
import * as msgpack from 'msgpackr'

class MsgPackSerde<T> implements Serde<T> {
  serialize(value: T): Buffer {
    return msgpack.pack(value)
  }

  deserialize(buffer: Buffer): T {
    return msgpack.unpack(buffer) as T
  }
}

const queue = new Queue({
  storage,
  payloadSerde: new MsgPackSerde(),
  resultSerde: new MsgPackSerde()
})
```

## Patterns

### Producer/Consumer Separation

Run producers and consumers as separate processes:

```typescript
// producer.ts
import { Queue, RedisStorage } from '@platformatic/job-queue'

const storage = new RedisStorage({ url: process.env.REDIS_URL })
const producer = new Queue({ storage })

await producer.start()
await producer.enqueue('task-1', { ... })
await producer.stop()
```

```typescript
// worker.ts
import { Queue, RedisStorage, Reaper } from '@platformatic/job-queue'

const storage = new RedisStorage({ url: process.env.REDIS_URL })
const queue = new Queue({
  storage,
  workerId: `worker-${process.pid}`,
  concurrency: 10
})

const reaper = new Reaper({
  storage,
  visibilityTimeout: 30000
})

queue.execute(async (job) => {
  // Process job
  return result
})

await queue.start()
await reaper.start()

process.on('SIGTERM', async () => {
  await queue.stop()
  await reaper.stop()
})
```

### Request/Response with Timeout

```typescript
try {
  const result = await queue.enqueueAndWait('request-1', payload, {
    timeout: 10000
  })
  console.log('Got result:', result)
} catch (error) {
  if (error instanceof TimeoutError) {
    console.log('Request timed out')
  } else if (error instanceof JobFailedError) {
    console.log('Job failed:', error.originalError)
  }
}
```

### Graceful Shutdown

```typescript
const queue = new Queue({
  storage,
  visibilityTimeout: 30000  // Jobs have 30s to complete
})

queue.execute(async (job) => {
  // Check for cancellation
  if (job.signal.aborted) {
    throw new Error('Job cancelled')
  }

  // Long-running work...
  await doWork()

  return result
})

await queue.start()

// Handle shutdown signals
process.on('SIGTERM', async () => {
  console.log('Shutting down...')
  await queue.stop()  // Waits for in-flight jobs
  process.exit(0)
})
```

## Error Handling

The library exports typed errors for specific failure conditions:

```typescript
import {
  TimeoutError,      // enqueueAndWait timeout
  MaxRetriesError,   // Job failed after all retries
  JobNotFoundError,  // Job doesn't exist
  JobCancelledError, // Job was cancelled
  JobFailedError,    // Job failed with error
  StorageError       // Storage backend error
} from '@platformatic/job-queue'
```

## Benchmarks

Run request/response benchmarks over Redis:

```bash
# With Redis
npm run bench:redis

# With Valkey
npm run bench:valkey
```

Sample output:

```
Single Request Baseline (100 requests, concurrency=1)
──────────────────────────────────────────────────
  Requests:    100
  Throughput:  312.50 req/s
  Latency:
    min:       2.15 ms
    p50:       3.02 ms
    p95:       4.21 ms
    p99:       5.43 ms
    max:       6.12 ms
    avg:       3.20 ms
```

## Testing

Run tests against different backends:

```bash
# Memory storage only
npm run test:memory

# With Redis (requires Redis on localhost:6379)
npm run test:redis

# With Valkey (requires Valkey on localhost:6380)
npm run test:valkey

# All backends
npm test
```

Start test infrastructure with Docker:

```bash
npm run docker:up    # Start Redis and Valkey
npm run docker:down  # Stop containers
```

## Requirements

- Node.js 22.19.0 or later (for native TypeScript support)
- Redis 7+ or Valkey 8+ (for RedisStorage)

## License

Apache 2.0
