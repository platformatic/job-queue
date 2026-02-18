import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { once } from 'node:events'
import { Queue, MemoryStorage, type Job } from '../src/index.ts'

describe('Queue', () => {
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

  describe('lifecycle', () => {
    it('should start and stop', async () => {
      await queue.start()
      await queue.stop()
    })

    it('should handle multiple start calls', async () => {
      await queue.start()
      await queue.start()
      await queue.stop()
    })

    it('should handle multiple stop calls', async () => {
      await queue.start()
      await queue.stop()
      await queue.stop()
    })
  })

  describe('enqueue', () => {
    it('should enqueue a job', async () => {
      await queue.start()

      const result = await queue.enqueue('job-1', { value: 42 })
      assert.strictEqual(result.status, 'queued')
    })

    it('should detect duplicate jobs', async () => {
      await queue.start()

      await queue.enqueue('job-1', { value: 42 })
      const result = await queue.enqueue('job-1', { value: 42 })

      assert.strictEqual(result.status, 'duplicate')
    })
  })

  describe('processing', () => {
    it('should process a job', async () => {
      let processed = false

      queue.execute(async (job: Job<{ value: number }>) => {
        processed = true
        return { result: job.payload.value * 2 }
      })

      await queue.start()
      await queue.enqueue('job-1', { value: 21 })

      // Wait for completed event
      await once(queue, 'completed')

      assert.strictEqual(processed, true)
    })

    it('should emit completed event', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()
      await queue.enqueue('job-1', { value: 21 })

      const [completedId, completedResult] = await once(queue, 'completed')

      assert.strictEqual(completedId, 'job-1')
      assert.deepStrictEqual(completedResult, { result: 42 })
    })

    it('should store result after completion', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()
      await queue.enqueue('job-1', { value: 21 })

      // Wait for completed event
      await once(queue, 'completed')

      const result = await queue.getResult('job-1')
      assert.deepStrictEqual(result, { result: 42 })
    })

    it('should return cached result for duplicate completed job', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()
      await queue.enqueue('job-1', { value: 21 })

      // Wait for completed event
      await once(queue, 'completed')

      const duplicateResult = await queue.enqueue('job-1', { value: 999 })
      assert.strictEqual(duplicateResult.status, 'completed')
      if (duplicateResult.status === 'completed') {
        assert.deepStrictEqual(duplicateResult.result, { result: 42 })
      }
    })
  })

  describe('enqueueAndWait', () => {
    it('should wait for job result', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()

      const result = await queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })
      assert.deepStrictEqual(result, { result: 42 })
    })

    it('should timeout if job takes too long', async () => {
      let jobStarted = false
      queue.execute(async () => {
        jobStarted = true
        // This will never complete within timeout
        await new Promise(() => {}) // Never resolves
        return { result: 0 }
      })

      await queue.start()

      // Use a short real timeout - no fake timers needed
      await assert.rejects(
        queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 50 }),
        (err: Error) => err.name === 'TimeoutError'
      )

      assert.strictEqual(jobStarted, true)
    })

    it('should return immediately for already completed job', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()

      // First call processes the job
      await queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })

      // Second call should return cached result immediately
      const result = await queue.enqueueAndWait('job-1', { value: 999 }, { timeout: 100 })
      assert.deepStrictEqual(result, { result: 42 })
    })
  })

  describe('retry', () => {
    it('should retry failed jobs', async () => {
      let attempts = 0

      queue.execute(async () => {
        attempts++
        if (attempts < 3) {
          throw new Error('Temporary failure')
        }
        return { result: 100 }
      })

      await queue.start()
      await queue.enqueue('job-1', { value: 1 })

      // Wait for completed event (after retries succeed)
      await once(queue, 'completed')

      assert.strictEqual(attempts, 3)

      const result = await queue.getResult('job-1')
      assert.deepStrictEqual(result, { result: 100 })
    })

    it('should emit failed event after max retries', async () => {
      queue.execute(async () => {
        throw new Error('Always fails')
      })

      await queue.start()
      await queue.enqueue('job-1', { value: 1 }, { maxAttempts: 2 })

      // Wait for failed event
      const [failedId, failedError] = await once(queue, 'failed')

      assert.strictEqual(failedId, 'job-1')
      assert.ok(failedError)
      assert.strictEqual((failedError as Error).name, 'MaxRetriesError')
    })
  })

  describe('cancel', () => {
    it('should cancel a queued job', async () => {
      await queue.start()

      await queue.enqueue('job-1', { value: 42 })
      const result = await queue.cancel('job-1')

      assert.strictEqual(result.status, 'cancelled')
    })

    it('should return not_found for non-existent job', async () => {
      await queue.start()

      const result = await queue.cancel('non-existent')
      assert.strictEqual(result.status, 'not_found')
    })

    it('should not process cancelled job', async () => {
      let processed = false

      queue.execute(async () => {
        processed = true
        return { result: 0 }
      })

      // Enqueue before starting so job sits in queue
      await storage.connect()
      const msg = Buffer.from(JSON.stringify({
        id: 'job-1',
        payload: { value: 42 },
        createdAt: Date.now(),
        attempts: 0,
        maxAttempts: 3
      }))
      await storage.enqueue('job-1', msg, Date.now())

      // Cancel before starting consumer
      await queue.cancel('job-1')

      queue.execute(async () => {
        processed = true
        return { result: 0 }
      })
      await queue.start()

      // Give consumer time to attempt dequeue
      await sleep(50)

      // Job should not have been processed (it was cancelled)
      assert.strictEqual(processed, false)
    })
  })

  describe('getStatus', () => {
    it('should return job status', async () => {
      await queue.start()

      await queue.enqueue('job-1', { value: 42 })

      const status = await queue.getStatus('job-1')
      assert.ok(status)
      assert.strictEqual(status.id, 'job-1')
      assert.strictEqual(status.state, 'queued')
    })

    it('should return null for non-existent job', async () => {
      await queue.start()

      const status = await queue.getStatus('non-existent')
      assert.strictEqual(status, null)
    })

    it('should include result for completed job', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()
      await queue.enqueue('job-1', { value: 21 })

      // Wait for completed event
      await once(queue, 'completed')

      const status = await queue.getStatus('job-1')
      assert.ok(status)
      assert.strictEqual(status.state, 'completed')
      assert.deepStrictEqual(status.result, { result: 42 })
    })
  })

  describe('concurrency', () => {
    it('should process multiple jobs concurrently', async () => {
      const concurrentQueue = new Queue<{ value: number }, { result: number }>({
        storage,
        concurrency: 3,
        visibilityTimeout: 5000
      })

      const processingTimes: number[] = []
      const startTime = Date.now()

      // Create promise that resolves when 3 jobs complete
      let resolveAll: () => void
      const allCompleted = new Promise<void>(resolve => { resolveAll = resolve })
      let completedCount = 0

      concurrentQueue.execute(async (job: Job<{ value: number }>) => {
        const processStart = Date.now() - startTime
        await sleep(50)
        processingTimes.push(processStart)
        return { result: job.payload.value }
      })

      concurrentQueue.on('completed', () => {
        completedCount++
        if (completedCount === 3) resolveAll()
      })

      await concurrentQueue.start()

      // Enqueue 3 jobs
      await concurrentQueue.enqueue('job-1', { value: 1 })
      await concurrentQueue.enqueue('job-2', { value: 2 })
      await concurrentQueue.enqueue('job-3', { value: 3 })

      // Wait for all 3 to complete
      await allCompleted

      await concurrentQueue.stop()

      // All 3 jobs should have started roughly at the same time
      assert.strictEqual(processingTimes.length, 3)

      const maxDiff = Math.max(...processingTimes) - Math.min(...processingTimes)
      assert.ok(maxDiff < 100, `Jobs should start concurrently, but start times differ by ${maxDiff}ms`)
    })
  })
})
