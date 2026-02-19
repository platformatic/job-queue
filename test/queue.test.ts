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
      resultTTL: 60000,
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

      const completedPromise = once(queue, 'completed')
      await queue.enqueue('job-1', { value: 21 })

      // Wait for completed event
      await completedPromise

      assert.strictEqual(processed, true)
    })

    it('should emit completed event', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()

      const completedPromise = once(queue, 'completed')
      await queue.enqueue('job-1', { value: 21 })

      const [completedId, completedResult] = await completedPromise

      assert.strictEqual(completedId, 'job-1')
      assert.deepStrictEqual(completedResult, { result: 42 })
    })

    it('should store result after completion', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()

      const completedPromise = once(queue, 'completed')
      await queue.enqueue('job-1', { value: 21 })

      // Wait for completed event
      await completedPromise

      const result = await queue.getResult('job-1')
      assert.deepStrictEqual(result, { result: 42 })
    })

    it('should return cached result for duplicate completed job', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()

      const completedPromise = once(queue, 'completed')
      await queue.enqueue('job-1', { value: 21 })

      // Wait for completed event
      await completedPromise

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

      const completedPromise = once(queue, 'completed')
      await queue.enqueue('job-1', { value: 1 })

      // Wait for completed event (after retries succeed)
      await completedPromise

      assert.strictEqual(attempts, 3)

      const result = await queue.getResult('job-1')
      assert.deepStrictEqual(result, { result: 100 })
    })

    it('should emit failed event after max retries', async () => {
      queue.execute(async () => {
        throw new Error('Always fails')
      })

      await queue.start()

      const failedPromise = once(queue, 'failed')
      await queue.enqueue('job-1', { value: 1 }, { maxAttempts: 2 })

      // Wait for failed event
      const [failedId, failedError] = await failedPromise

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
      const msg = Buffer.from(
        JSON.stringify({
          id: 'job-1',
          payload: { value: 42 },
          createdAt: Date.now(),
          attempts: 0,
          maxAttempts: 3
        })
      )
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

      const completedPromise = once(queue, 'completed')
      await queue.enqueue('job-1', { value: 21 })

      // Wait for completed event
      await completedPromise

      const status = await queue.getStatus('job-1')
      assert.ok(status)
      assert.strictEqual(status.state, 'completed')
      assert.deepStrictEqual(status.result, { result: 42 })
    })

    it('should use error.toJSON() for failed job status when available', async () => {
      class JsonError extends Error {
        toJSON (): unknown {
          return {
            message: this.message,
            code: 'CUSTOM_ERROR',
            details: { source: 'toJSON' }
          }
        }
      }

      queue.execute(async () => {
        throw new JsonError('Serialized by toJSON')
      })

      await queue.start()

      const failedPromise = once(queue, 'failed')
      await queue.enqueue('job-1', { value: 21 }, { maxAttempts: 1 })
      await failedPromise

      const status = await queue.getStatus('job-1')
      assert.ok(status)
      assert.strictEqual(status.state, 'failed')
      assert.deepStrictEqual(status.error, {
        message: 'Serialized by toJSON',
        code: 'CUSTOM_ERROR',
        details: { source: 'toJSON' }
      })
    })
  })

  describe('result TTL override', () => {
    it('should override default TTL for completed jobs', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await queue.start()

      const completedPromise = once(queue, 'completed')
      await queue.enqueue('job-1', { value: 21 }, { resultTTL: 20 })
      await completedPromise

      assert.deepStrictEqual(await queue.getResult('job-1'), { result: 42 })

      await sleep(60)

      const expiredResult = await queue.getResult('job-1')
      assert.strictEqual(expiredResult, null)
    })

    it('should use the first accepted TTL when duplicates provide different values', async () => {
      const localStorage = new MemoryStorage()
      const localQueue = new Queue<{ value: number }, { result: number }>({
        storage: localStorage,
        resultTTL: 5000,
        visibilityTimeout: 5000
      })

      localQueue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      const first = await localQueue.enqueue('job-1', { value: 21 }, { resultTTL: 20 })
      const duplicate = await localQueue.enqueue('job-1', { value: 21 }, { resultTTL: 5000 })

      assert.strictEqual(first.status, 'queued')
      assert.strictEqual(duplicate.status, 'duplicate')

      await localQueue.start()
      await once(localQueue, 'completed')

      await sleep(60)

      const expiredResult = await localQueue.getResult('job-1')
      assert.strictEqual(expiredResult, null)

      await localQueue.stop()
    })

    it('should use producer default resultTTL when no per-job override is provided', async () => {
      const sharedStorage = new MemoryStorage()

      const producerOnlyQueue = new Queue<{ value: number }, { result: number }>({
        storage: sharedStorage,
        resultTTL: 20,
        visibilityTimeout: 5000
      })

      await producerOnlyQueue.start()
      await producerOnlyQueue.enqueue('job-1', { value: 21 })
      await producerOnlyQueue.stop()

      const consumerOnlyQueue = new Queue<{ value: number }, { result: number }>({
        storage: sharedStorage,
        resultTTL: 5000,
        visibilityTimeout: 5000
      })

      consumerOnlyQueue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await consumerOnlyQueue.start()
      await once(consumerOnlyQueue, 'completed')

      assert.deepStrictEqual(await consumerOnlyQueue.getResult('job-1'), { result: 42 })

      await sleep(60)

      const expiredResult = await consumerOnlyQueue.getResult('job-1')
      assert.strictEqual(expiredResult, null)

      await consumerOnlyQueue.stop()
    })

    it('should reject invalid per-job resultTTL values', async () => {
      await assert.rejects(queue.enqueue('job-1', { value: 1 }, { resultTTL: 0 }), (err: Error) => {
        assert.strictEqual(err.name, 'TypeError')
        assert.match(err.message, /resultTTL must be a positive integer/)
        return true
      })
    })
  })

  describe('afterExecution hook', () => {
    it('should allow overriding TTL and replacing result in afterExecution', async () => {
      const localStorage = new MemoryStorage()
      const localQueue = new Queue<{ value: number }, { result: number }>({
        storage: localStorage,
        resultTTL: 5000,
        visibilityTimeout: 5000,
        afterExecution: context => {
          assert.strictEqual(context.status, 'completed')
          assert.strictEqual(context.id, 'job-1')
          assert.strictEqual(context.payload.value, 21)
          assert.strictEqual(context.attempts, 1)
          assert.strictEqual(context.maxAttempts, 3)
          assert.ok(context.durationMs >= 0)

          context.ttl = 20
          context.result = { result: 777 }
        }
      })

      localQueue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await localQueue.start()
      await localQueue.enqueue('job-1', { value: 21 })
      const [, completedResult] = await once(localQueue, 'completed')

      assert.deepStrictEqual(completedResult, { result: 777 })
      assert.deepStrictEqual(await localQueue.getResult('job-1'), { result: 777 })

      await sleep(60)
      assert.strictEqual(await localQueue.getResult('job-1'), null)

      await localQueue.stop()
    })

    it('should support async afterExecution hook on failed jobs', async () => {
      const localStorage = new MemoryStorage()
      const localQueue = new Queue<{ value: number }, { result: number }>({
        storage: localStorage,
        resultTTL: 20,
        visibilityTimeout: 5000,
        afterExecution: async context => {
          await sleep(5)
          assert.strictEqual(context.status, 'failed')
          context.ttl = 200
          context.error = new Error('updated boom')
        }
      })

      localQueue.execute(async () => {
        throw new Error('boom')
      })

      await localQueue.start()
      await localQueue.enqueue('job-1', { value: 21 }, { maxAttempts: 1 })
      await once(localQueue, 'failed')

      await sleep(60)
      const error = await localStorage.getError('job-1')
      assert.ok(error)
      assert.match(error.toString(), /updated boom/)

      await localQueue.stop()
    })
  })

  describe('updateResultTTL', () => {
    it('should update TTL for completed jobs', async () => {
      const localStorage = new MemoryStorage()
      const localQueue = new Queue<{ value: number }, { result: number }>({
        storage: localStorage,
        resultTTL: 20,
        visibilityTimeout: 5000
      })

      localQueue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await localQueue.start()
      await localQueue.enqueue('job-1', { value: 21 })
      await once(localQueue, 'completed')

      const updateResult = await localQueue.updateResultTTL('job-1', 200)
      assert.deepStrictEqual(updateResult, { status: 'updated' })

      await sleep(60)
      assert.deepStrictEqual(await localQueue.getResult('job-1'), { result: 42 })

      await localQueue.stop()
    })

    it('should update TTL for failed jobs', async () => {
      const localStorage = new MemoryStorage()
      const localQueue = new Queue<{ value: number }, { result: number }>({
        storage: localStorage,
        resultTTL: 20,
        visibilityTimeout: 5000
      })

      localQueue.execute(async () => {
        throw new Error('boom')
      })

      await localQueue.start()
      await localQueue.enqueue('job-1', { value: 1 }, { maxAttempts: 1 })
      await once(localQueue, 'failed')

      const updateResult = await localQueue.updateResultTTL('job-1', 200)
      assert.deepStrictEqual(updateResult, { status: 'updated' })

      await sleep(60)
      const error = await localStorage.getError('job-1')
      assert.ok(error)

      await localQueue.stop()
    })

    it('should return not_found when job does not exist', async () => {
      await queue.start()

      const updateResult = await queue.updateResultTTL('missing-job', 100)
      assert.deepStrictEqual(updateResult, { status: 'not_found' })
    })

    it('should return not_terminal for queued jobs', async () => {
      await queue.start()
      await queue.enqueue('job-1', { value: 21 })

      const updateResult = await queue.updateResultTTL('job-1', 100)
      assert.deepStrictEqual(updateResult, { status: 'not_terminal' })
    })

    it('should return missing_payload when terminal payload has expired', async () => {
      const localStorage = new MemoryStorage()
      const localQueue = new Queue<{ value: number }, { result: number }>({
        storage: localStorage,
        resultTTL: 20,
        visibilityTimeout: 5000
      })

      localQueue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      await localQueue.start()
      await localQueue.enqueue('job-1', { value: 21 })
      await once(localQueue, 'completed')

      await sleep(60)

      const updateResult = await localQueue.updateResultTTL('job-1', 100)
      assert.deepStrictEqual(updateResult, { status: 'missing_payload' })

      await localQueue.stop()
    })

    it('should reject invalid TTL values', async () => {
      await queue.start()
      await assert.rejects(queue.updateResultTTL('job-1', 0), (err: Error) => {
        assert.strictEqual(err.name, 'TypeError')
        assert.match(err.message, /resultTTL must be a positive integer/)
        return true
      })
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
      const allCompleted = new Promise<void>(resolve => {
        resolveAll = resolve
      })
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
