import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { Queue } from '../src/queue.ts'
import { MemoryStorage } from '../src/storage/memory.ts'
import type { Job } from '../src/types.ts'

describe('Deduplication', () => {
  let storage: MemoryStorage
  let queue: Queue<{ value: number }, { result: number }>

  beforeEach(async () => {
    storage = new MemoryStorage()
    queue = new Queue({
      storage,
      concurrency: 1,
      visibilityTimeout: 5000
    })

    queue.execute(async (job: Job<{ value: number }>) => {
      return { result: job.payload.value * 2 }
    })
  })

  afterEach(async () => {
    await queue.stop()
  })

  describe('while queued', () => {
    it('should reject duplicate job while original is queued', async () => {
      // Enqueue without starting the queue (job stays in queued state)
      const result1 = await queue.enqueue('job-1', { value: 42 })
      assert.strictEqual(result1.status, 'queued')

      // Try to enqueue same job ID
      const result2 = await queue.enqueue('job-1', { value: 99 })
      assert.strictEqual(result2.status, 'duplicate')
      if (result2.status === 'duplicate') {
        assert.strictEqual(result2.existingState, 'queued')
      }
    })

    it('should return duplicate status with different payload', async () => {
      const result1 = await queue.enqueue('job-1', { value: 1 })
      assert.strictEqual(result1.status, 'queued')

      // Different payload, same ID - should still be duplicate
      const result2 = await queue.enqueue('job-1', { value: 100 })
      assert.strictEqual(result2.status, 'duplicate')
    })
  })

  describe('while processing', () => {
    it('should reject duplicate job while original is processing', async () => {
      let handlerStarted: () => void
      const waitForHandler = new Promise<void>(resolve => {
        handlerStarted = resolve
      })

      const slowStorage = new MemoryStorage()
      const slowQueue = new Queue<{ value: number }, { result: number }>({
        storage: slowStorage,
        concurrency: 1,
        visibilityTimeout: 5000
      })

      slowQueue.execute(async (job: Job<{ value: number }>) => {
        handlerStarted!()
        await sleep(100)
        return { result: job.payload.value * 2 }
      })

      await slowQueue.start()

      // Enqueue the job
      const result1 = await slowQueue.enqueue('job-1', { value: 42 })
      assert.strictEqual(result1.status, 'queued')

      // Wait for handler to start processing
      await waitForHandler

      // Now try to enqueue same job ID while it's processing
      const result2 = await slowQueue.enqueue('job-1', { value: 99 })
      assert.strictEqual(result2.status, 'duplicate')
      if (result2.status === 'duplicate') {
        assert.strictEqual(result2.existingState, 'processing')
      }

      // Wait for job to complete
      await sleep(150)

      await slowQueue.stop()
    })
  })

  describe('after completion', () => {
    it('should return cached result for completed job', async () => {
      await queue.start()

      // Enqueue and wait for completion
      const result1 = await queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })
      assert.deepStrictEqual(result1, { result: 42 })

      // Enqueue same job ID again - should get cached result
      const result2 = await queue.enqueue('job-1', { value: 999 })
      assert.strictEqual(result2.status, 'completed')
      if (result2.status === 'completed') {
        // Should return original result, not new computation
        assert.deepStrictEqual(result2.result, { result: 42 })
      }
    })

    it('should return cached result even with different payload', async () => {
      await queue.start()

      await queue.enqueueAndWait('job-1', { value: 5 }, { timeout: 5000 })

      // Different payload, same ID - should still return cached result
      const result2 = await queue.enqueue('job-1', { value: 1000 })
      assert.strictEqual(result2.status, 'completed')
      if (result2.status === 'completed') {
        assert.deepStrictEqual(result2.result, { result: 10 }) // Original result, not 2000
      }
    })
  })

  describe('after failure', () => {
    it('should return duplicate with failed state for failed job', async () => {
      const failingStorage = new MemoryStorage()
      const failingQueue = new Queue<{ value: number }, { result: number }>({
        storage: failingStorage,
        concurrency: 1,
        visibilityTimeout: 5000
      })

      failingQueue.execute(async () => {
        throw new Error('Job failed')
      })

      await failingQueue.start()

      // Enqueue and wait for failure
      await failingQueue.enqueue('job-1', { value: 42 })

      // Wait for the job to fail (maxRetries defaults to 3)
      await sleep(200)

      // Enqueue same job ID - should see failed status as duplicate
      const result2 = await failingQueue.enqueue('job-1', { value: 99 })
      assert.strictEqual(result2.status, 'duplicate')
      if (result2.status === 'duplicate') {
        assert.strictEqual(result2.existingState, 'failed')
      }

      await failingQueue.stop()
    })
  })

  describe('after cancellation', () => {
    it('should allow new job after cancelled job', async () => {
      // Enqueue job without starting queue
      const result1 = await queue.enqueue('job-1', { value: 42 })
      assert.strictEqual(result1.status, 'queued')

      // Cancel it
      const cancelResult = await queue.cancel('job-1')
      assert.strictEqual(cancelResult.status, 'cancelled')

      // Now should be able to enqueue same ID
      const result2 = await queue.enqueue('job-1', { value: 99 })
      assert.strictEqual(result2.status, 'queued')
    })
  })

  describe('concurrent enqueue', () => {
    it('should handle concurrent enqueue of same job ID', async () => {
      // Start multiple enqueues concurrently
      const results = await Promise.all([
        queue.enqueue('job-1', { value: 1 }),
        queue.enqueue('job-1', { value: 2 }),
        queue.enqueue('job-1', { value: 3 })
      ])

      // Exactly one should be queued, others should be duplicate
      const queued = results.filter(r => r.status === 'queued')
      const duplicates = results.filter(r => r.status === 'duplicate')

      assert.strictEqual(queued.length, 1)
      assert.strictEqual(duplicates.length, 2)
    })
  })

  describe('result TTL', () => {
    it('should expire result after TTL', async () => {
      const shortTtlStorage = new MemoryStorage()
      const shortTtlQueue = new Queue<{ value: number }, { result: number }>({
        storage: shortTtlStorage,
        resultTTL: 50, // 50ms TTL
        concurrency: 1,
        visibilityTimeout: 5000
      })

      shortTtlQueue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await shortTtlQueue.start()

      // Complete a job
      const result1 = await shortTtlQueue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })
      assert.deepStrictEqual(result1, { result: 42 })

      // Wait for TTL to expire
      await sleep(100)

      // Result should be expired
      const cachedResult = await shortTtlStorage.getResult('job-1')
      assert.strictEqual(cachedResult, null, 'Result should be expired')

      await shortTtlQueue.stop()
    })
  })
})
