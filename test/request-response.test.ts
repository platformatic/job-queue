import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'
import { Queue, MemoryStorage, type Job } from '../src/index.ts'
import { createLatch } from './helpers/events.ts'

describe('Request/Response', () => {
  let storage: MemoryStorage
  let queue: Queue<{ value: number }, { result: number }>

  beforeEach(async () => {
    storage = new MemoryStorage()
    queue = new Queue({
      storage,
      concurrency: 1,
      maxRetries: 3,
      visibilityTimeout: 5000
    })
  })

  afterEach(async () => {
    await queue.stop()
  })

  describe('basic flow', () => {
    it('should wait for job result', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()

      const result = await queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })
      assert.deepStrictEqual(result, { result: 42 })
    })

    it('should handle multiple concurrent requests', async () => {
      const concurrentQueue = new Queue<{ value: number }, { result: number }>({
        storage,
        concurrency: 3,
        visibilityTimeout: 5000
      })

      concurrentQueue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await concurrentQueue.start()

      const results = await Promise.all([
        concurrentQueue.enqueueAndWait('job-1', { value: 1 }, { timeout: 5000 }),
        concurrentQueue.enqueueAndWait('job-2', { value: 2 }, { timeout: 5000 }),
        concurrentQueue.enqueueAndWait('job-3', { value: 3 }, { timeout: 5000 })
      ])

      await concurrentQueue.stop()

      assert.deepStrictEqual(results, [
        { result: 2 },
        { result: 4 },
        { result: 6 }
      ])
    })
  })

  describe('timeout handling', () => {
    it('should timeout if job takes too long', async () => {
      queue.execute(async () => {
        await new Promise(() => {}) // Never resolves
        return { result: 0 }
      })

      await queue.start()

      // Use a short real timeout
      await assert.rejects(
        queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 50 }),
        (err: Error) => err.name === 'TimeoutError'
      )
    })

    it('should use default timeout when not specified', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value }
      })

      await queue.start()

      const result = await queue.enqueueAndWait('job-1', { value: 42 })
      assert.deepStrictEqual(result, { result: 42 })
    })
  })

  describe('already-completed jobs', () => {
    it('should return cached result immediately', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()

      // First call processes the job
      const result1 = await queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })
      assert.deepStrictEqual(result1, { result: 42 })

      // Second call should return cached result immediately (even with short timeout)
      const result2 = await queue.enqueueAndWait('job-1', { value: 999 }, { timeout: 50 })
      assert.deepStrictEqual(result2, { result: 42 })
    })
  })

  describe('duplicate job handling', () => {
    it('should wait for in-progress job to complete', async () => {
      let callCount = 0
      const jobStarted = createLatch()
      const jobCanComplete = createLatch()

      queue.execute(async (job: Job<{ value: number }>) => {
        callCount++
        jobStarted.resolve()
        // Wait for test to signal job can complete
        await jobCanComplete.promise
        return { result: job.payload.value * 2 }
      })

      await queue.start()

      // Start first request
      const promise1 = queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })

      // Wait for job to actually start processing
      await jobStarted.promise

      // Start second request with same ID while first is processing
      const promise2 = queue.enqueueAndWait('job-1', { value: 999 }, { timeout: 5000 })

      // Now let the job complete
      jobCanComplete.resolve()

      const [result1, result2] = await Promise.all([promise1, promise2])

      // Both should get the same result
      assert.deepStrictEqual(result1, { result: 42 })
      assert.deepStrictEqual(result2, { result: 42 })

      // Job should only have been executed once
      assert.strictEqual(callCount, 1)
    })

    it('should handle duplicate enqueue while job is queued', async () => {
      let callCount = 0

      const slowQueue = new Queue<{ value: number }, { result: number }>({
        storage,
        concurrency: 1,
        visibilityTimeout: 5000
      })

      slowQueue.execute(async (job: Job<{ value: number }>) => {
        callCount++
        return { result: job.payload.value * 2 }
      })

      await slowQueue.start()

      // Both requests for same job ID - second will find duplicate
      const promise1 = slowQueue.enqueueAndWait('slow-job', { value: 10 }, { timeout: 5000 })
      const promise2 = slowQueue.enqueueAndWait('slow-job', { value: 20 }, { timeout: 5000 })

      const [result1, result2] = await Promise.all([promise1, promise2])

      await slowQueue.stop()

      // Both should get same result (first payload wins)
      assert.deepStrictEqual(result1, { result: 20 })
      assert.deepStrictEqual(result2, { result: 20 })

      // Job should only have been executed once
      assert.strictEqual(callCount, 1)
    })
  })

  describe('failed job handling', () => {
    it('should throw when job fails after max retries', async () => {
      queue.execute(async () => {
        throw new Error('Job failed')
      })

      await queue.start()

      await assert.rejects(
        queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000, maxAttempts: 1 }),
        (err: Error) => err.name === 'JobFailedError'
      )
    })

    it('should return error for already-failed job', async () => {
      queue.execute(async () => {
        throw new Error('Always fails')
      })

      await queue.start()

      // First call triggers the failure
      await assert.rejects(
        queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000, maxAttempts: 1 }),
        (err: Error) => err.name === 'JobFailedError'
      )

      // Second call should also fail immediately (cached failure)
      await assert.rejects(
        queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 100 }),
        (err: Error) => err.name === 'JobFailedError'
      )
    })
  })

  describe('large result handling', () => {
    it('should handle large results', async () => {
      const largeQueue = new Queue<{ size: number }, { data: string }>({
        storage,
        concurrency: 1,
        visibilityTimeout: 5000
      })

      largeQueue.execute(async (job: Job<{ size: number }>) => {
        // Generate a large string (100KB)
        const data = 'x'.repeat(job.payload.size)
        return { data }
      })

      await largeQueue.start()

      const result = await largeQueue.enqueueAndWait('large-job', { size: 100000 }, { timeout: 5000 })

      await largeQueue.stop()

      assert.strictEqual(result.data.length, 100000)
    })
  })

  describe('subscription cleanup', () => {
    it('should cleanup subscriptions after timeout', async () => {
      // Use concurrency 2 so job-2 can run while job-1 is stuck
      const cleanupQueue = new Queue<{ value: number }, { result: number }>({
        storage: new MemoryStorage(),
        concurrency: 2,
        visibilityTimeout: 5000
      })

      cleanupQueue.execute(async (job: Job<{ value: number }>) => {
        if (job.id === 'job-1') {
          // This job will never complete
          await new Promise(() => {})
        }
        return { result: job.payload.value }
      })

      await cleanupQueue.start()

      // This will timeout (short real timeout, no fake timers)
      await assert.rejects(
        cleanupQueue.enqueueAndWait('job-1', { value: 21 }, { timeout: 50 }),
        (err: Error) => err.name === 'TimeoutError'
      )

      // Should be able to start more jobs without subscription leaks
      const result = await cleanupQueue.enqueueAndWait('job-2', { value: 100 }, { timeout: 5000 })
      assert.deepStrictEqual(result, { result: 100 })

      await cleanupQueue.stop()
    })
  })
})
