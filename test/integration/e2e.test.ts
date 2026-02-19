import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { Queue } from '../../src/queue.ts'
import { Reaper } from '../../src/reaper.ts'
import { MemoryStorage } from '../../src/storage/memory.ts'
import { FileStorage } from '../../src/storage/file.ts'
import { RedisStorage } from '../../src/storage/redis.ts'
import type { Storage } from '../../src/storage/types.ts'
import type { Job } from '../../src/types.ts'
import { once, waitForEvents, createLatch } from '../helpers/events.ts'

interface TestPayload {
  value: number
  processTime?: number
  shouldFail?: boolean
}

interface TestResult {
  computed: number
  workerId: string
}

type StorageFactory = () => Promise<{ storage: Storage; cleanup: () => Promise<void> }>

// Storage factories for different backends
const memoryStorageFactory: StorageFactory = async () => {
  const storage = new MemoryStorage()
  return { storage, cleanup: async () => {} }
}

const fileStorageFactory: StorageFactory = async () => {
  const basePath = await mkdtemp(join(tmpdir(), 'job-queue-e2e-'))
  const storage = new FileStorage({ basePath })
  return {
    storage,
    cleanup: async () => {
      await rm(basePath, { recursive: true, force: true })
    }
  }
}

const redisStorageFactory: StorageFactory = async () => {
  if (!process.env.REDIS_URL) {
    throw new Error('REDIS_URL not set')
  }
  const prefix = `e2e:${Date.now()}:${Math.random().toString(36).slice(2)}:`
  const storage = new RedisStorage({
    url: process.env.REDIS_URL,
    keyPrefix: prefix
  })
  return {
    storage,
    cleanup: async () => {
      await (storage as RedisStorage).clear()
    }
  }
}

// Determine which storage backends to test
function getStorageFactories (): Array<{ name: string; factory: StorageFactory }> {
  const factories: Array<{ name: string; factory: StorageFactory }> = [
    { name: 'MemoryStorage', factory: memoryStorageFactory },
    { name: 'FileStorage', factory: fileStorageFactory }
  ]

  if (process.env.REDIS_URL) {
    factories.push({ name: 'RedisStorage', factory: redisStorageFactory })
  }

  return factories
}

// Run integration tests for each storage backend
for (const { name, factory } of getStorageFactories()) {
  describe(`Integration Tests (${name})`, () => {
    let storage: Storage
    let cleanup: () => Promise<void>

    beforeEach(async () => {
      const result = await factory()
      storage = result.storage
      cleanup = result.cleanup
    })

    afterEach(async () => {
      await cleanup()
    })

    describe('basic workflow', () => {
      it('should process jobs end-to-end', async () => {
        const queue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-1',
          concurrency: 1,
          visibilityTimeout: 5000
        })

        const results: TestResult[] = []

        queue.execute(async (job: Job<TestPayload>) => {
          const result = { computed: job.payload.value * 2, workerId: 'worker-1' }
          results.push(result)
          return result
        })

        // Set up wait BEFORE starting to ensure listener is registered
        const allCompleted = waitForEvents(queue, 'completed', 3)

        await queue.start()

        // Enqueue multiple jobs
        await queue.enqueue('job-1', { value: 10 })
        await queue.enqueue('job-2', { value: 20 })
        await queue.enqueue('job-3', { value: 30 })

        // Wait for all jobs to complete
        await allCompleted

        assert.strictEqual(results.length, 3)
        assert.deepStrictEqual(results.map(r => r.computed).sort(), [20, 40, 60])

        await queue.stop()
      })

      it('should handle enqueueAndWait correctly', async () => {
        const queue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-1',
          concurrency: 1,
          visibilityTimeout: 5000
        })

        queue.execute(async (job: Job<TestPayload>) => {
          if (job.payload.processTime) {
            await sleep(job.payload.processTime)
          }
          return { computed: job.payload.value * 2, workerId: 'worker-1' }
        })

        await queue.start()

        const result = await queue.enqueueAndWait('job-1', { value: 25 }, { timeout: 5000 })

        assert.deepStrictEqual(result, { computed: 50, workerId: 'worker-1' })

        await queue.stop()
      })
    })

    describe('multiple workers', () => {
      it('should distribute work across workers', async () => {
        const worker1Results: string[] = []
        const worker2Results: string[] = []
        let completedCount = 0
        const allCompleted = createLatch()

        const queue1 = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-1',
          concurrency: 1,
          visibilityTimeout: 5000
        })

        const queue2 = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-2',
          concurrency: 1,
          visibilityTimeout: 5000
        })

        queue1.execute(async (job: Job<TestPayload>) => {
          worker1Results.push(job.id)
          await sleep(10) // Small delay to allow distribution
          return { computed: job.payload.value * 2, workerId: 'worker-1' }
        })

        queue2.execute(async (job: Job<TestPayload>) => {
          worker2Results.push(job.id)
          await sleep(10)
          return { computed: job.payload.value * 2, workerId: 'worker-2' }
        })

        // Track completed events from both queues
        const onCompleted = () => {
          completedCount++
          if (completedCount === 10) allCompleted.resolve()
        }
        queue1.on('completed', onCompleted)
        queue2.on('completed', onCompleted)

        await queue1.start()
        await queue2.start()

        // Enqueue multiple jobs
        for (let i = 0; i < 10; i++) {
          await queue1.enqueue(`job-${i}`, { value: i })
        }

        // Wait for all jobs to be processed
        await allCompleted.promise

        // Both workers should have processed some jobs
        const totalProcessed = worker1Results.length + worker2Results.length
        assert.strictEqual(totalProcessed, 10)

        await queue1.stop()
        await queue2.stop()
      })
    })

    describe('producer/consumer separation', () => {
      it('should allow separate producer and consumer queues', async () => {
        // Producer queue (no handler)
        const producer = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'producer-1',
          visibilityTimeout: 5000
        })

        // Consumer queue (with handler)
        const consumer = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'consumer-1',
          concurrency: 2,
          visibilityTimeout: 5000
        })

        const processedJobs: string[] = []

        consumer.execute(async (job: Job<TestPayload>) => {
          processedJobs.push(job.id)
          return { computed: job.payload.value * 2, workerId: 'consumer-1' }
        })

        // Set up wait BEFORE starting to ensure listener is registered
        const allProcessed = waitForEvents(consumer, 'completed', 2)

        await producer.start()
        await consumer.start()

        // Producer enqueues jobs
        const result1 = await producer.enqueue('job-1', { value: 100 })
        const result2 = await producer.enqueue('job-2', { value: 200 })

        assert.strictEqual(result1.status, 'queued')
        assert.strictEqual(result2.status, 'queued')

        // Wait for consumer to process
        await allProcessed

        assert.deepStrictEqual(processedJobs.sort(), ['job-1', 'job-2'])

        await producer.stop()
        await consumer.stop()
      })
    })

    describe('failure and retry', () => {
      it('should retry failed jobs up to maxRetries', async () => {
        let attempts = 0

        const queue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-1',
          concurrency: 1,
          maxRetries: 3,
          visibilityTimeout: 5000
        })

        queue.execute(async (job: Job<TestPayload>) => {
          attempts++
          if (attempts < 3) {
            throw new Error('Temporary failure')
          }
          return { computed: job.payload.value * 2, workerId: 'worker-1' }
        })

        await queue.start()

        const result = await queue.enqueueAndWait('job-1', { value: 50 }, { timeout: 5000 })

        assert.strictEqual(attempts, 3)
        assert.deepStrictEqual(result, { computed: 100, workerId: 'worker-1' })

        await queue.stop()
      })

      it('should emit failed event after max retries exceeded', async () => {
        const queue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-1',
          concurrency: 1,
          maxRetries: 2,
          visibilityTimeout: 5000
        })

        queue.execute(async () => {
          throw new Error('Permanent failure')
        })

        await queue.start()

        const failedPromise = once(queue, 'failed')
        await queue.enqueue('job-1', { value: 50 })

        const [id, error] = await failedPromise

        assert.strictEqual(id, 'job-1')
        assert.ok(error instanceof Error)
        assert.ok(error.message.includes('Permanent failure'))

        await queue.stop()
      })
    })

    describe('cancellation', () => {
      it('should cancel a queued job before processing', async () => {
        const processedJobs: string[] = []

        const queue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-1',
          concurrency: 1,
          visibilityTimeout: 5000
        })

        queue.execute(async (job: Job<TestPayload>) => {
          processedJobs.push(job.id)
          return { computed: job.payload.value * 2, workerId: 'worker-1' }
        })

        // Don't start the queue yet - enqueue first
        await storage.connect()

        await queue.enqueue('job-1', { value: 10 })
        await queue.enqueue('job-2', { value: 20 })

        // Cancel job-1 before processing starts
        const cancelResult = await queue.cancel('job-1')
        assert.strictEqual(cancelResult.status, 'cancelled')

        // Set up wait BEFORE starting to ensure listener is registered
        const jobCompleted = once(queue, 'completed')

        // Now start processing
        await queue.start()
        await jobCompleted

        // Only job-2 should be processed
        assert.deepStrictEqual(processedJobs, ['job-2'])

        await queue.stop()
      })
    })

    describe('reaper integration', () => {
      it('should recover stalled jobs', async () => {
        const processedJobs: string[] = []
        const stallingJobStarted = createLatch()
        const releaseStallingHandler = createLatch()

        // First worker that will deterministically stall
        // Use a longer consumer visibility timeout than reaper timeout,
        // so the reaper always handles recovery first.
        const stallingQueue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'stalling-worker',
          concurrency: 1,
          visibilityTimeout: 500
        })

        stallingQueue.execute(async (job: Job<TestPayload>) => {
          stallingJobStarted.resolve()
          await releaseStallingHandler.promise
          return { computed: job.payload.value * 2, workerId: 'stalling-worker' }
        })

        // Recovery worker
        const recoveryQueue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'recovery-worker',
          concurrency: 1,
          visibilityTimeout: 5000
        })

        recoveryQueue.execute(async (job: Job<TestPayload>) => {
          processedJobs.push(job.id)
          return { computed: job.payload.value * 2, workerId: 'recovery-worker' }
        })

        // Reaper to detect stalled jobs
        const reaper = new Reaper({
          storage,
          visibilityTimeout: 100
        })

        // Set up waits BEFORE starting components that emit those events
        const jobRecovered = once(recoveryQueue, 'completed')
        const stalledDetected = once(reaper, 'stalled')

        // Start stalling worker first and ensure it has claimed the job.
        // This avoids races where the recovery worker consumes the job first.
        await stallingQueue.start()
        await stallingQueue.enqueue('job-1', { value: 42 })
        await stallingJobStarted.promise

        // Start recovery components after the job is definitely in processing
        await recoveryQueue.start()
        await reaper.start()

        // Reaper must detect the stalled job and recovery worker must complete it
        const [stalledId] = await stalledDetected
        assert.strictEqual(stalledId, 'job-1')

        const [recoveredId] = await jobRecovered
        assert.strictEqual(recoveredId, 'job-1')

        // The recovery worker should have processed the job
        assert.ok(processedJobs.includes('job-1'), 'Job should have been recovered')

        // Release stalled handler so queue shutdown is fast and clean
        releaseStallingHandler.resolve()

        await reaper.stop()
        await stallingQueue.stop()
        await recoveryQueue.stop()
      })
    })

    describe('high concurrency', () => {
      it('should handle concurrent jobs', async () => {
        const processedJobs = new Set<string>()
        const jobCount = 10

        const queue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-1',
          concurrency: 3,
          visibilityTimeout: 5000
        })

        queue.execute(async (job: Job<TestPayload>) => {
          processedJobs.add(job.id)
          return { computed: job.payload.value * 2, workerId: 'worker-1' }
        })

        // Set up wait BEFORE starting to ensure listener is registered
        const allProcessed = waitForEvents(queue, 'completed', jobCount)

        await queue.start()

        // Enqueue jobs sequentially to avoid race conditions
        for (let i = 0; i < jobCount; i++) {
          await queue.enqueue(`job-${i}`, { value: i })
        }

        // Wait for all to be processed
        await allProcessed

        assert.strictEqual(processedJobs.size, jobCount)

        await queue.stop()
      })
    })

    describe('deduplication end-to-end', () => {
      it('should deduplicate sequential enqueue requests', async () => {
        let processCount = 0
        const jobStarted = createLatch()
        const jobCanComplete = createLatch()

        const queue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-1',
          concurrency: 1,
          visibilityTimeout: 5000
        })

        queue.execute(async (job: Job<TestPayload>) => {
          processCount++
          jobStarted.resolve()
          await jobCanComplete.promise // Wait for test to allow completion
          return { computed: job.payload.value * 2, workerId: 'worker-1' }
        })

        await queue.start()

        // Enqueue same job multiple times sequentially
        const result1 = await queue.enqueue('same-job', { value: 1 })

        // Wait for processing to start to ensure deduplication check happens while processing
        await jobStarted.promise

        const result2 = await queue.enqueue('same-job', { value: 2 })
        const result3 = await queue.enqueue('same-job', { value: 3 })

        // Let job complete
        const completedPromise = once(queue, 'completed')
        jobCanComplete.resolve()
        await completedPromise

        // First should be queued, rest are duplicates
        assert.strictEqual(result1.status, 'queued')
        assert.strictEqual(result2.status, 'duplicate')
        assert.strictEqual(result3.status, 'duplicate')
        assert.strictEqual(processCount, 1)

        await queue.stop()
      })

      it('should return cached result for completed job', async () => {
        const queue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-1',
          concurrency: 1,
          visibilityTimeout: 5000,
          resultTTL: 60000
        })

        queue.execute(async (job: Job<TestPayload>) => {
          return { computed: job.payload.value * 2, workerId: 'worker-1' }
        })

        await queue.start()

        // First enqueue and wait
        const result1 = await queue.enqueueAndWait('cached-job', { value: 100 }, { timeout: 5000 })
        assert.deepStrictEqual(result1, { computed: 200, workerId: 'worker-1' })

        // Second enqueue should return cached result
        const result2 = await queue.enqueue('cached-job', { value: 999 })
        assert.strictEqual(result2.status, 'completed')
        if (result2.status === 'completed') {
          assert.deepStrictEqual(result2.result, { computed: 200, workerId: 'worker-1' })
        }

        await queue.stop()
      })
    })

    describe('graceful shutdown', () => {
      it('should complete in-flight jobs on stop', async () => {
        let jobCompleted = false
        const jobStarted = createLatch()
        const jobCanComplete = createLatch()

        const queue = new Queue<TestPayload, TestResult>({
          storage,
          workerId: 'worker-1',
          concurrency: 1,
          visibilityTimeout: 5000
        })

        queue.execute(async (job: Job<TestPayload>) => {
          jobStarted.resolve()
          await jobCanComplete.promise // Wait for test to allow completion
          jobCompleted = true
          return { computed: job.payload.value * 2, workerId: 'worker-1' }
        })

        await queue.start()

        // Enqueue a job
        await queue.enqueue('job-1', { value: 50 })

        // Wait for processing to start
        await jobStarted.promise

        // Let the job complete before stopping
        jobCanComplete.resolve()

        // Stop the queue - should wait for in-flight job
        await queue.stop()

        // Job should have completed
        assert.strictEqual(jobCompleted, true)
      })
    })
  })
}
