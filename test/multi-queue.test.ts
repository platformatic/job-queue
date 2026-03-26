import assert from 'node:assert'
import { afterEach, beforeEach, describe, it } from 'node:test'
import { Queue } from '../src/queue.ts'
import { MemoryStorage } from '../src/storage/memory.ts'
import type { Job } from '../src/types.ts'
import { createLatch } from './helpers/events.ts'

describe('Multi-Queue (named queues on shared storage)', () => {
  let storage: MemoryStorage
  let queueA: Queue<{ value: number }, { result: number }>
  let queueB: Queue<{ value: string }, { result: string }>

  beforeEach(() => {
    storage = new MemoryStorage()
    queueA = new Queue({
      storage,
      name: 'emails',
      concurrency: 1,
      visibilityTimeout: 5000
    })
    queueB = new Queue({
      storage,
      name: 'images',
      concurrency: 1,
      visibilityTimeout: 5000
    })
  })

  afterEach(async () => {
    await queueA.stop()
    await queueB.stop()
  })

  describe('job isolation', () => {
    it('should process jobs independently across queues', async () => {
      const latchA = createLatch()
      const latchB = createLatch()

      queueA.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      queueB.execute(async (job: Job<{ value: string }>) => {
        return { result: job.payload.value.toUpperCase() }
      })

      queueA.on('completed', () => latchA.resolve())
      queueB.on('completed', () => latchB.resolve())

      await queueA.start()
      await queueB.start()

      await queueA.enqueue('job-1', { value: 21 })
      await queueB.enqueue('job-1', { value: 'hello' })

      await latchA.promise
      await latchB.promise

      const resultA = await queueA.getResult('job-1')
      const resultB = await queueB.getResult('job-1')

      assert.deepStrictEqual(resultA, { result: 42 })
      assert.deepStrictEqual(resultB, { result: 'HELLO' })
    })

    it('should not deliver queue A jobs to queue B consumers', async () => {
      const latchA = createLatch()

      queueA.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 2 }
      })

      // Queue B has a handler but should never receive anything
      queueB.execute(async () => {
        assert.fail('Queue B should not receive queue A jobs')
        return { result: 'never' }
      })

      queueA.on('completed', () => latchA.resolve())

      await queueA.start()
      await queueB.start()

      await queueA.enqueue('job-1', { value: 5 })
      await latchA.promise

      const resultA = await queueA.getResult('job-1')
      assert.deepStrictEqual(resultA, { result: 10 })
    })
  })

  describe('dedup isolation', () => {
    it('should allow same job ID in different queues', async () => {
      await queueA.start()
      await queueB.start()

      const resultA = await queueA.enqueue('shared-id', { value: 1 })
      const resultB = await queueB.enqueue('shared-id', { value: 'test' })

      assert.strictEqual(resultA.status, 'queued')
      assert.strictEqual(resultB.status, 'queued')
    })

    it('should enforce dedup within the same named queue', async () => {
      await queueA.start()

      const result1 = await queueA.enqueue('dup-id', { value: 1 })
      const result2 = await queueA.enqueue('dup-id', { value: 2 })

      assert.strictEqual(result1.status, 'queued')
      assert.strictEqual(result2.status, 'duplicate')
    })
  })

  describe('result isolation', () => {
    it('should store and retrieve results independently per queue', async () => {
      const latchA = createLatch()
      const latchB = createLatch()

      queueA.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value + 100 }
      })

      queueB.execute(async (job: Job<{ value: string }>) => {
        return { result: `processed:${job.payload.value}` }
      })

      queueA.on('completed', () => latchA.resolve())
      queueB.on('completed', () => latchB.resolve())

      await queueA.start()
      await queueB.start()

      await queueA.enqueue('result-test', { value: 5 })
      await queueB.enqueue('result-test', { value: 'data' })

      await latchA.promise
      await latchB.promise

      const resultA = await queueA.getResult('result-test')
      const resultB = await queueB.getResult('result-test')

      assert.deepStrictEqual(resultA, { result: 105 })
      assert.deepStrictEqual(resultB, { result: 'processed:data' })
    })
  })

  describe('status isolation', () => {
    it('should track status independently per queue', async () => {
      const latchA = createLatch()

      queueA.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value }
      })

      queueA.on('completed', () => latchA.resolve())

      await queueA.start()
      await queueB.start()

      await queueA.enqueue('status-test', { value: 1 })
      await queueB.enqueue('status-test', { value: 'pending' })

      await latchA.promise

      const statusA = await queueA.getStatus('status-test')
      const statusB = await queueB.getStatus('status-test')

      assert.strictEqual(statusA?.state, 'completed')
      assert.strictEqual(statusB?.state, 'queued')
    })
  })

  describe('backward compatibility', () => {
    it('should work without a name (no namespace)', async () => {
      const latch = createLatch()
      const plainQueue = new Queue<{ value: number }, { result: number }>({
        storage,
        concurrency: 1,
        visibilityTimeout: 5000
      })

      plainQueue.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value * 3 }
      })

      plainQueue.on('completed', () => latch.resolve())

      await plainQueue.start()
      await plainQueue.enqueue('plain-job', { value: 10 })

      await latch.promise

      const result = await plainQueue.getResult('plain-job')
      assert.deepStrictEqual(result, { result: 30 })

      await plainQueue.stop()
    })
  })

  describe('connect/disconnect lifecycle', () => {
    it('should handle multiple start/stop cycles', async () => {
      queueA.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value }
      })

      await queueA.start()
      await queueB.start()

      await queueA.stop()
      await queueB.stop()

      // Second cycle
      queueA = new Queue({
        storage,
        name: 'emails',
        concurrency: 1,
        visibilityTimeout: 5000
      })
      queueA.execute(async (job: Job<{ value: number }>) => {
        return { result: job.payload.value }
      })

      const latch = createLatch()
      queueA.on('completed', () => latch.resolve())

      await queueA.start()
      await queueA.enqueue('cycle-test', { value: 7 })
      await latch.promise

      const result = await queueA.getResult('cycle-test')
      assert.deepStrictEqual(result, { result: 7 })
    })
  })
})
