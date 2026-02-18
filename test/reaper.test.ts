import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { Queue, Reaper, MemoryStorage, type Job } from '../src/index.ts'
import { once, waitForEvents } from './helpers/events.ts'

describe('Reaper', () => {
  let storage: MemoryStorage
  let queue: Queue<{ value: number }, { result: number }>
  let reaper: Reaper<{ value: number }>

  beforeEach(async () => {
    storage = new MemoryStorage()
    queue = new Queue({
      storage,
      concurrency: 1,
      maxRetries: 3,
      visibilityTimeout: 100 // Short timeout for testing
    })
    reaper = new Reaper({
      storage,
      visibilityTimeout: 100
    })
  })

  afterEach(async () => {
    await reaper.stop()
    await queue.stop()
    // Also disconnect storage directly in case it was connected without starting queue
    await storage.disconnect()
  })

  describe('lifecycle', () => {
    it('should start and stop', async () => {
      await storage.connect()
      await reaper.start()
      await reaper.stop()
    })

    it('should handle multiple start calls', async () => {
      await storage.connect()
      await reaper.start()
      await reaper.start()
      await reaper.stop()
    })

    it('should handle multiple stop calls', async () => {
      await storage.connect()
      await reaper.start()
      await reaper.stop()
      await reaper.stop()
    })
  })

  describe('stalled job detection', () => {
    it('should detect and recover a stalled job', async () => {
      // Use concurrency 2 so requeued job can be picked up while first is stalled
      // Use longer visibility timeout than reaper to ensure reaper handles it, not consumer
      const testQueue = new Queue<{ value: number }, { result: number }>({
        storage,
        concurrency: 2,
        maxRetries: 3,
        visibilityTimeout: 500 // Longer than reaper's 100ms
      })

      let processCount = 0
      let resolveFirstJob: (() => void) | null = null
      const firstJobStarted = new Promise<void>(resolve => { resolveFirstJob = resolve })
      let abortFirstHandler: (() => void) | undefined

      testQueue.execute(async (job: Job<{ value: number }>) => {
        processCount++
        if (processCount === 1) {
          resolveFirstJob!()
          // First attempt: stall but allow cleanup via abort
          await new Promise<void>((resolve) => {
            abortFirstHandler = resolve
            job.signal.addEventListener('abort', () => resolve())
          })
          throw new Error('Aborted for cleanup')
        }
        // Second attempt: complete normally
        return { result: job.payload.value * 2 }
      })

      await testQueue.start()
      await reaper.start()

      // Enqueue a job
      const resultPromise = testQueue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })

      // Wait for first processing attempt
      await firstJobStarted

      // Wait for stall detection
      await once(reaper, 'stalled')

      // Job should be reprocessed and complete
      const result = await resultPromise

      // Clean up - abort the stalled handler before stopping
      abortFirstHandler?.()
      await testQueue.stop()

      assert.deepStrictEqual(result, { result: 42 })
      assert.strictEqual(processCount, 2, 'Job should have been processed twice')
    })

    it('should emit stalled event when recovering a job', async () => {
      // Use separate queue with longer visibility timeout so handler doesn't abort
      const testQueue = new Queue<{ value: number }, { result: number }>({
        storage,
        concurrency: 1,
        visibilityTimeout: 500 // Longer than reaper's 100ms
      })

      let resolveFirstJob: (() => void) | null = null
      const firstJobStarted = new Promise<void>(resolve => { resolveFirstJob = resolve })
      let abortHandler: (() => void) | undefined

      testQueue.execute(async (job: Job<{ value: number }>) => {
        resolveFirstJob!()
        // Simulate stall but allow abort for cleanup
        await new Promise<void>((resolve) => {
          abortHandler = resolve
          job.signal.addEventListener('abort', () => resolve())
        })
        return { result: 0 }
      })

      await testQueue.start()
      await reaper.start()

      // Start listening for stalled event
      const stalledPromise = once(reaper, 'stalled')

      // Enqueue a job (don't wait for result)
      testQueue.enqueue('job-1', { value: 21 })

      // Wait for processing to start
      await firstJobStarted

      // Wait for stalled event
      const [stalledId] = await stalledPromise

      // Clean up - abort the stalled handler so queue can stop
      abortHandler?.()
      await testQueue.stop()

      assert.strictEqual(stalledId, 'job-1')
    })

    it('should not recover job that completes in time', async () => {
      let processCount = 0

      queue.execute(async (job: Job<{ value: number }>) => {
        processCount++
        // Complete quickly (before visibility timeout)
        return { result: job.payload.value * 2 }
      })

      await queue.start()
      await reaper.start()

      const result = await queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })

      // Wait past visibility timeout
      await sleep(150)

      assert.deepStrictEqual(result, { result: 42 })
      assert.strictEqual(processCount, 1, 'Job should only have been processed once')
    })

    it('should cancel timer when job completes', async () => {
      queue.execute(async (job: Job<{ value: number }>) => {
        // Complete quickly
        return { result: job.payload.value * 2 }
      })

      await queue.start()
      await reaper.start()

      await queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })

      // Wait past visibility timeout - reaper should have cancelled the timer
      await sleep(150)

      // No stalled event should be emitted, no errors
    })

    it('should cancel timer when job fails', async () => {
      queue.execute(async () => {
        throw new Error('Job failed')
      })

      await queue.start()
      await reaper.start()

      // Job will fail after retries
      await assert.rejects(
        queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000, maxAttempts: 1 }),
        (err: Error) => err.name === 'JobFailedError'
      )

      // Wait past visibility timeout - reaper should have cancelled the timer
      await sleep(150)

      // No stalled event should be emitted for completed job
    })
  })

  describe('periodic check', () => {
    it('should detect stalled jobs on startup', async () => {
      // Manually set up a "stalled" job state
      await storage.connect()

      const oldTimestamp = Date.now() - 200 // 200ms ago (past visibility timeout)

      // Add job to worker's processing queue
      const message = Buffer.from(JSON.stringify({
        id: 'stalled-job',
        payload: { value: 42 },
        createdAt: oldTimestamp,
        attempts: 0,
        maxAttempts: 3
      }))
      await storage.registerWorker('worker-1', 60000)

      // Enqueue first, then dequeue to worker's processing queue
      await storage.enqueue('stalled-job', message, oldTimestamp)
      await storage.dequeue('worker-1', 1)

      // Now set the state to processing with old timestamp (simulating a stall)
      await storage.setJobState('stalled-job', `processing:${oldTimestamp}:worker-1`)

      // Track stalled events
      const stalledJobs: string[] = []
      reaper.on('stalled', (id) => {
        stalledJobs.push(id)
      })

      // Set up wait BEFORE starting reaper to ensure listener is registered
      const stallDetected = waitForEvents(reaper, 'stalled', 1)

      // Start reaper - should detect the stalled job on startup check
      await reaper.start()

      // Wait for detection
      await stallDetected

      assert.strictEqual(stalledJobs.length, 1)
      assert.strictEqual(stalledJobs[0], 'stalled-job')
    })

    it('should check all workers processing queues', async () => {
      await storage.connect()

      // Set up multiple workers with stalled jobs
      const oldTimestamp = Date.now() - 200

      for (let i = 1; i <= 2; i++) {
        const workerId = `worker-${i}`
        const jobId = `stalled-job-${i}`

        await storage.registerWorker(workerId, 60000)

        const message = Buffer.from(JSON.stringify({
          id: jobId,
          payload: { value: i },
          createdAt: oldTimestamp,
          attempts: 0,
          maxAttempts: 3
        }))

        // Enqueue then dequeue to get into processing queue
        await storage.enqueue(jobId, message, oldTimestamp)
        await storage.dequeue(workerId, 1)

        // Set state to processing with old timestamp
        await storage.setJobState(jobId, `processing:${oldTimestamp}:${workerId}`)
      }

      const stalledJobs: string[] = []
      reaper.on('stalled', (id) => {
        stalledJobs.push(id)
      })

      // Set up wait BEFORE starting reaper to ensure listener is registered
      const stallsDetected = waitForEvents(reaper, 'stalled', 2)

      await reaper.start()

      // Wait for both jobs to be detected
      await stallsDetected

      assert.strictEqual(stalledJobs.length, 2)
      assert.ok(stalledJobs.includes('stalled-job-1'))
      assert.ok(stalledJobs.includes('stalled-job-2'))
    })
  })

  describe('timer management', () => {
    it('should not leak timers on stop', async () => {
      // Use separate queue for cleanup control
      const testQueue = new Queue<{ value: number }, { result: number }>({
        storage,
        concurrency: 1,
        visibilityTimeout: 500
      })

      let resolveJob: (() => void) | null = null
      const jobStarted = new Promise<void>(resolve => { resolveJob = resolve })
      let abortHandler: (() => void) | undefined

      testQueue.execute(async (job: Job<{ value: number }>) => {
        resolveJob!()
        // Allow abort for cleanup
        await new Promise<void>((resolve) => {
          abortHandler = resolve
          job.signal.addEventListener('abort', () => resolve())
        })
        return { result: 0 }
      })

      await testQueue.start()
      await reaper.start()

      // Enqueue a job to start a timer
      testQueue.enqueue('job-1', { value: 21 })

      // Wait for processing to start
      await jobStarted

      // Stop reaper - should clear all timers
      await reaper.stop()

      // Wait past visibility timeout - no errors should occur
      await sleep(150)

      // Clean up the stuck handler
      abortHandler?.()
      await testQueue.stop()
    })
  })
})
