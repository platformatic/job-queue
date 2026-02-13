# Implementation Plan

This document outlines the implementation plan for `@platformatic/job-queue`.

## Phase 1: Project Setup

### 1.1 Initialize Project Structure
- [ ] Create `package.json` with dependencies (`iovalkey`, `fast-write-atomic`)
- [ ] Create `tsconfig.json` and `tsconfig.build.json`
- [ ] Set up file structure as defined in DESIGN.md
- [ ] Add `.gitignore`, `.npmignore`
- [ ] Create `docker-compose.yml` for Redis and Valkey

### 1.2 Docker Compose Setup

Create `docker-compose.yml` to run both Redis and Valkey for testing:

```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - "127.0.0.1:6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 1s
      timeout: 3s
      retries: 5

  valkey:
    image: valkey/valkey:8-alpine
    ports:
      - "127.0.0.1:6380:6379"
    healthcheck:
      test: ["CMD", "valkey-cli", "ping"]
      interval: 1s
      timeout: 3s
      retries: 5
```

**Scripts in package.json:**

```json
{
  "scripts": {
    "docker:up": "docker compose up -d",
    "docker:down": "docker compose down",
    "test": "npm run test:memory && npm run test:redis && npm run test:valkey",
    "test:memory": "node --test test/*.test.ts",
    "test:redis": "REDIS_URL=redis://localhost:6379 node --test test/*.test.ts",
    "test:valkey": "REDIS_URL=redis://localhost:6380 node --test test/*.test.ts",
    "build": "tsc -p tsconfig.build.json",
    "clean": "rm -rf dist",
    "prepublishOnly": "npm run clean && npm run build",
    "typecheck": "tsc --noEmit"
  }
}
```

**Note:** Node.js 22.19+/23.6+/24+ runs TypeScript directly without flags via type stripping.

### 1.3 Define Core Types
- [ ] `src/types.ts` - All TypeScript interfaces and types
  - `QueueMessage<T>`
  - `QueueConfig<TPayload, TResult>`
  - `EnqueueOptions`, `EnqueueAndWaitOptions`
  - `EnqueueResult`, `CancelResult`
  - `MessageStatus`, `MessageState`
  - `Job<TPayload>`, `JobHandler<TPayload, TResult>`

### 1.4 Define Errors
- [ ] `src/errors.ts` - Custom error classes
  - `TimeoutError`
  - `MaxRetriesError`
  - `JobNotFoundError`
  - `StorageError`

### 1.5 Define Serde Interface
- [ ] `src/serde/index.ts`
  - `Serde<T>` interface
  - `JsonSerde` implementation (default)

## Phase 2: Storage Interface

### 2.1 Storage Types
- [ ] `src/storage/types.ts` - Storage interface definition
  - Lifecycle methods (`connect`, `disconnect`)
  - Queue operations (`enqueue`, `dequeue`, `requeue`, `ack`)
  - Job state methods
  - Results/errors methods
  - Worker registration methods
  - Notification methods
  - Atomic operations

### 2.2 MemoryStorage Implementation
- [ ] `src/storage/memory.ts`
  - In-memory data structures (Map, Array)
  - Event emitter for notifications
  - Simulated blocking with `setTimeout`
  - TTL cleanup via `setInterval`

**Tests:**
- [ ] `test/memory-storage.test.ts`
  - Basic enqueue/dequeue
  - Deduplication
  - Job state transitions
  - Result storage with TTL
  - Worker registration
  - Notifications

## Phase 3: Queue Core

### 3.1 Producer Implementation
- [ ] `src/producer.ts`
  - `enqueue()` - fire-and-forget
  - `enqueueAndWait()` - request/response with pub/sub
  - `cancel()` - job cancellation
  - `getResult()` - retrieve cached result
  - `getStatus()` - get job status

**Tests:**
- [ ] `test/producer.test.ts`
  - Fire-and-forget enqueue
  - Duplicate detection
  - Cached result retrieval
  - Cancellation scenarios

### 3.2 Consumer Implementation
- [ ] `src/consumer.ts`
  - `execute()` - register job handler
  - Dequeue loop with concurrency control
  - Job execution with AbortSignal
  - Success/failure handling
  - Retry logic

**Tests:**
- [ ] `test/consumer.test.ts`
  - Job processing
  - Concurrency limits
  - Retry on failure
  - Max retries exceeded
  - Graceful shutdown

### 3.3 Queue Class
- [ ] `src/queue.ts`
  - Combines Producer and Consumer
  - Lifecycle management (`start`, `stop`)
  - Event emission
  - Configuration handling

**Tests:**
- [ ] `test/queue.test.ts`
  - Combined producer/consumer
  - Start/stop lifecycle
  - Event emissions

## Phase 4: Request/Response

### 4.1 Pub/Sub Notifications
- [ ] Implement subscribe-before-enqueue pattern
- [ ] Handle completion notifications
- [ ] Timeout handling

**Tests:**
- [ ] `test/request-response.test.ts`
  - Basic request/response flow
  - Timeout handling
  - Already-completed jobs
  - Duplicate job handling
  - Large result handling

## Phase 5: Stalled Job Recovery

### 5.1 Reaper Implementation
- [ ] `src/reaper.ts`
  - Subscribe to events
  - Track processing jobs with timers
  - Detect and recover stalled jobs
  - Coordinate with workers registry

**Tests:**
- [ ] `test/reaper.test.ts`
  - Stalled job detection
  - Requeue stalled jobs
  - Timer cancellation on completion

## Phase 6: Redis Storage

### 6.1 Lua Scripts
- [ ] `src/scripts/enqueue.lua` - Atomic enqueue with dedup
- [ ] `src/scripts/complete.lua` - Atomic job completion
- [ ] `src/scripts/fail.lua` - Atomic job failure
- [ ] `src/scripts/cancel.lua` - Atomic cancellation
- [ ] `src/scripts/recoverStalled.lua` - Atomic stall recovery

### 6.2 RedisStorage Implementation
- [ ] `src/storage/redis.ts`
  - Connection management (iovalkey)
  - Lua script loading and execution
  - BLMOVE for blocking dequeue
  - Pub/sub for notifications
  - Processing queue TTL

- [ ] `src/storage/redis-scripts.ts`
  - Script loading utilities
  - SHA caching

**Tests:**
- [ ] `test/redis-storage.test.ts` (runs against both Redis and Valkey)
  - All MemoryStorage tests against Redis/Valkey
  - Lua script atomicity
  - BLMOVE blocking behavior
  - Pub/sub notifications
  - Processing queue TTL
  - Tests run twice: once with `REDIS_URL=redis://localhost:6379` (Redis), once with `REDIS_URL=redis://localhost:6380` (Valkey)

### 6.3 Test Fixtures
- [ ] `test/fixtures/redis.ts`
  - Connection setup from `REDIS_URL` env var
  - Key cleanup between tests
  - Shared fixture for Redis and Valkey

## Phase 7: File Storage

### 7.1 FileStorage Implementation
- [ ] `src/storage/file.ts`
  - Directory structure initialization
  - Atomic writes with `fast-write-atomic`
  - `fs.watch` for dequeue and notifications
  - Sequence number management
  - TTL cleanup via mtime

**Tests:**
- [ ] `test/file-storage.test.ts`
  - All MemoryStorage tests against FileStorage
  - Atomic file operations
  - fs.watch notifications
  - Persistence across restarts
  - TTL cleanup

## Phase 8: Integration Tests

### 8.1 End-to-End Tests
- [ ] `test/integration/e2e.test.ts`
  - Full workflow with Redis and Valkey
  - Multiple producers/consumers
  - Failure scenarios
  - Recovery scenarios
  - CI runs against both backends

### 8.2 Serde Tests
- [ ] `test/serde.test.ts`
  - JSON serde (default)
  - MessagePack serde compatibility
  - Large payload handling

## Phase 9: Deduplication Tests

### 9.1 Deduplication Scenarios
- [ ] `test/deduplication.test.ts`
  - Same ID rejected while queued
  - Same ID rejected while processing
  - Same ID allowed after completion (with different payload)
  - Same ID returns cached result
  - Jobs hash cleanup

## Phase 10: Documentation & Polish

### 10.1 Public API
- [ ] `src/index.ts` - Public exports
  - Queue class
  - Storage implementations
  - Types
  - Errors
  - Serde

### 10.2 README
- [ ] Quick start guide
- [ ] API reference
- [ ] Configuration options
- [ ] Storage implementations
- [ ] Examples

### 10.3 Final Polish
- [ ] Type checking (`npm run typecheck`)
- [ ] All tests passing
- [ ] Build verification (`npm run build`)
- [ ] Package publishing dry-run

---

## Implementation Order

Recommended order to enable incremental testing:

1. **Phase 1** - Project setup and types
2. **Phase 2** - MemoryStorage (enables all testing without Redis)
3. **Phase 3** - Queue core (producer, consumer, queue class)
4. **Phase 4** - Request/response
5. **Phase 5** - Reaper
6. **Phase 9** - Deduplication tests (validates core logic)
7. **Phase 6** - Redis storage (production backend)
8. **Phase 7** - File storage (alternative backend)
9. **Phase 8** - Integration tests
10. **Phase 10** - Documentation and polish

## Dependencies

```
Phase 1 ─────► Phase 2 ─────► Phase 3 ─────► Phase 4
                                │               │
                                ▼               ▼
                            Phase 5 ◄───── Phase 9
                                │
                                ▼
                 ┌──────────────┼──────────────┐
                 ▼              ▼              ▼
             Phase 6        Phase 7        Phase 8
                 │              │              │
                 └──────────────┼──────────────┘
                                ▼
                            Phase 10
```

## Milestones

| Milestone | Phases | Deliverable |
|-----------|--------|-------------|
| M1: Core Working | 1-5 | Queue works with MemoryStorage |
| M2: Production Ready | 6, 9 | Redis storage, full deduplication |
| M3: Complete | 7-10 | All storage backends, docs |

## Estimated Effort

| Phase | Effort |
|-------|--------|
| Phase 1 | Small |
| Phase 2 | Medium |
| Phase 3 | Large |
| Phase 4 | Medium |
| Phase 5 | Medium |
| Phase 6 | Large |
| Phase 7 | Medium |
| Phase 8 | Medium |
| Phase 9 | Small |
| Phase 10 | Medium |
