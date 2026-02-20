import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as sleep } from 'node:timers/promises'
import { createFileStorage } from './fixtures/file.ts'
import { FileStorage } from '../src/storage/file.ts'
import { promisifyCallback } from './helpers/events.ts'

describe('FileStorage', () => {
  let storage: FileStorage
  let cleanup: () => Promise<void>

  beforeEach(async () => {
    const result = await createFileStorage()
    storage = result.storage
    cleanup = result.cleanup
    await storage.connect()
  })

  afterEach(async () => {
    await storage.clear()
    await storage.disconnect()
    await cleanup()
  })

  describe('enqueue/dequeue', () => {
    it('should enqueue and dequeue a job', async () => {
      const message = Buffer.from(JSON.stringify({ id: 'job-1', payload: 'test' }))
      const result = await storage.enqueue('job-1', message, Date.now())

      assert.strictEqual(result, null, 'enqueue should return null for new job')

      const dequeued = await storage.dequeue('worker-1', 1)
      assert.ok(dequeued, 'dequeue should return the message')
      assert.deepStrictEqual(dequeued, message)
    })

    it('should return existing state for duplicate job', async () => {
      const message = Buffer.from(JSON.stringify({ id: 'job-1', payload: 'test' }))
      const timestamp = Date.now()

      await storage.enqueue('job-1', message, timestamp)
      const result = await storage.enqueue('job-1', message, timestamp)

      assert.ok(result, 'should return existing state')
      assert.ok(result.startsWith('queued:'), 'state should start with queued:')
    })

    it('should return null on dequeue timeout', async () => {
      const result = await storage.dequeue('worker-1', 0.1)
      assert.strictEqual(result, null)
    })

    it('should dequeue in FIFO order', async () => {
      const msg1 = Buffer.from('msg-1')
      const msg2 = Buffer.from('msg-2')
      const msg3 = Buffer.from('msg-3')

      await storage.enqueue('job-1', msg1, Date.now())
      await storage.enqueue('job-2', msg2, Date.now())
      await storage.enqueue('job-3', msg3, Date.now())

      const d1 = await storage.dequeue('worker-1', 1)
      const d2 = await storage.dequeue('worker-1', 1)
      const d3 = await storage.dequeue('worker-1', 1)

      assert.deepStrictEqual(d1, msg1)
      assert.deepStrictEqual(d2, msg2)
      assert.deepStrictEqual(d3, msg3)
    })

    it('should wait for job when queue is empty', async () => {
      // Start waiting for a job
      const dequeuePromise = storage.dequeue('worker-1', 1)

      // Enqueue a job after a short delay
      await sleep(50)
      const message = Buffer.from('delayed-job')
      await storage.enqueue('job-1', message, Date.now())

      // Should get the job
      const dequeued = await dequeuePromise
      assert.deepStrictEqual(dequeued, message)
    })
  })

  describe('job state', () => {
    it('should get and set job state', async () => {
      await storage.setJobState('job-1', 'processing:123456:worker-1')
      const state = await storage.getJobState('job-1')

      assert.strictEqual(state, 'processing:123456:worker-1')
    })

    it('should return null for non-existent job', async () => {
      const state = await storage.getJobState('non-existent')
      assert.strictEqual(state, null)
    })

    it('should delete job', async () => {
      await storage.setJobState('job-1', 'queued:123456')
      const deleted = await storage.deleteJob('job-1')

      assert.strictEqual(deleted, true)
      assert.strictEqual(await storage.getJobState('job-1'), null)
    })

    it('should return false when deleting non-existent job', async () => {
      const deleted = await storage.deleteJob('non-existent')
      assert.strictEqual(deleted, false)
    })

    it('should get multiple job states', async () => {
      await storage.setJobState('job-1', 'queued:1')
      await storage.setJobState('job-2', 'processing:2')

      const states = await storage.getJobStates(['job-1', 'job-2', 'job-3'])

      assert.strictEqual(states.get('job-1'), 'queued:1')
      assert.strictEqual(states.get('job-2'), 'processing:2')
      assert.strictEqual(states.get('job-3'), null)
    })

    it('should call getJobState from getJobStates sequentially', async () => {
      class TrackingFileStorage extends FileStorage {
        getJobStateCalls = 0

        async getJobState (id: string): Promise<string | null> {
          this.getJobStateCalls++
          return super.getJobState(id)
        }
      }

      const basePath = await mkdtemp(join(tmpdir(), 'job-queue-file-test-'))
      const trackingStorage = new TrackingFileStorage({ basePath })

      try {
        await trackingStorage.connect()
        await trackingStorage.setJobState('job-1', 'queued:1')
        await trackingStorage.setJobState('job-2', 'processing:2')

        const ids = ['job-1', 'job-2', 'job-3']
        const states = await trackingStorage.getJobStates(ids)

        assert.strictEqual(states.get('job-1'), 'queued:1')
        assert.strictEqual(states.get('job-2'), 'processing:2')
        assert.strictEqual(states.get('job-3'), null)
        assert.strictEqual(trackingStorage.getJobStateCalls, ids.length)
      } finally {
        await trackingStorage.clear()
        await trackingStorage.disconnect()
        await rm(basePath, { recursive: true, force: true })
      }
    })
  })

  describe('requeue', () => {
    it('should move job from processing queue back to main queue', async () => {
      const message = Buffer.from('requeue-test')
      await storage.enqueue('job-1', message, Date.now())

      // Dequeue to worker-1
      const dequeued = await storage.dequeue('worker-1', 1)
      assert.deepStrictEqual(dequeued, message)

      // Verify processing queue has the job
      const processing = await storage.getProcessingJobs('worker-1')
      assert.strictEqual(processing.length, 1)

      // Requeue
      await storage.requeue('job-1', message, 'worker-1')

      // Processing queue should be empty
      const processingAfter = await storage.getProcessingJobs('worker-1')
      assert.strictEqual(processingAfter.length, 0)

      // Should be able to dequeue again
      const redequeued = await storage.dequeue('worker-2', 1)
      assert.deepStrictEqual(redequeued, message)
    })
  })

  describe('ack', () => {
    it('should remove job from processing queue', async () => {
      const message = Buffer.from('ack-test')
      await storage.enqueue('job-1', message, Date.now())

      const dequeued = await storage.dequeue('worker-1', 1)
      assert.ok(dequeued)

      await storage.ack('job-1', message, 'worker-1')

      const processing = await storage.getProcessingJobs('worker-1')
      assert.strictEqual(processing.length, 0)
    })
  })

  describe('results', () => {
    it('should store and retrieve result', async () => {
      const result = Buffer.from(JSON.stringify({ success: true }))
      await storage.setResult('job-1', result, 60000)

      const retrieved = await storage.getResult('job-1')
      assert.deepStrictEqual(retrieved, result)
    })

    it('should return null for non-existent result', async () => {
      const result = await storage.getResult('non-existent')
      assert.strictEqual(result, null)
    })

    it('should return null for expired result', async () => {
      const result = Buffer.from('expiring')
      await storage.setResult('job-1', result, 10) // 10ms TTL

      await sleep(50)

      const retrieved = await storage.getResult('job-1')
      assert.strictEqual(retrieved, null)
    })
  })

  describe('errors', () => {
    it('should store and retrieve error', async () => {
      const error = Buffer.from(JSON.stringify({ message: 'Something failed' }))
      await storage.setError('job-1', error, 60000)

      const retrieved = await storage.getError('job-1')
      assert.deepStrictEqual(retrieved, error)
    })

    it('should return null for non-existent error', async () => {
      const error = await storage.getError('non-existent')
      assert.strictEqual(error, null)
    })
  })

  describe('workers', () => {
    it('should register and get workers', async () => {
      await storage.registerWorker('worker-1', 60000)
      await storage.registerWorker('worker-2', 60000)

      const workers = await storage.getWorkers()
      assert.deepStrictEqual(workers.sort(), ['worker-1', 'worker-2'])
    })

    it('should unregister worker', async () => {
      await storage.registerWorker('worker-1', 60000)
      await storage.unregisterWorker('worker-1')

      const workers = await storage.getWorkers()
      assert.deepStrictEqual(workers, [])
    })

    it('should not return expired workers', async () => {
      await storage.registerWorker('worker-1', 10) // 10ms TTL

      await sleep(50)

      const workers = await storage.getWorkers()
      assert.deepStrictEqual(workers, [])
    })
  })

  describe('notifications', () => {
    it('should notify on job completion', async () => {
      const { value, unsubscribe } = await promisifyCallback<string>(handler =>
        storage.subscribeToJob('job-1', handler))

      await storage.notifyJobComplete('job-1', 'completed')

      const notifiedStatus = await value

      assert.strictEqual(notifiedStatus, 'completed')

      await unsubscribe()
    })

    it('should notify on job failure', async () => {
      const { value, unsubscribe } = await promisifyCallback<string>(handler =>
        storage.subscribeToJob('job-1', handler))

      await storage.notifyJobComplete('job-1', 'failed')

      const notifiedStatus = await value

      assert.strictEqual(notifiedStatus, 'failed')

      await unsubscribe()
    })
  })

  describe('events', () => {
    it('should emit events on state changes', async () => {
      const events: Array<{ id: string; event: string }> = []

      const unsubscribe = await storage.subscribeToEvents((id, event) => {
        events.push({ id, event })
      })

      await storage.publishEvent('job-1', 'processing')
      await storage.publishEvent('job-1', 'completed')

      assert.deepStrictEqual(events, [
        { id: 'job-1', event: 'processing' },
        { id: 'job-1', event: 'completed' }
      ])

      await unsubscribe()
    })

    it('should emit queued event on enqueue', async () => {
      const events: Array<{ id: string; event: string }> = []

      const unsubscribe = await storage.subscribeToEvents((id, event) => {
        events.push({ id, event })
      })

      await storage.enqueue('job-1', Buffer.from('test'), Date.now())

      assert.deepStrictEqual(events, [{ id: 'job-1', event: 'queued' }])

      await unsubscribe()
    })
  })

  describe('atomic operations', () => {
    it('should complete job atomically', async () => {
      const message = Buffer.from('complete-test')
      const result = Buffer.from(JSON.stringify({ success: true }))

      await storage.enqueue('job-1', message, Date.now())
      await storage.dequeue('worker-1', 1)
      await storage.setJobState('job-1', 'processing:123:worker-1')

      const { value: notificationReceived, unsubscribe } = await promisifyCallback<string>(handler =>
        storage.subscribeToJob('job-1', handler))

      await storage.completeJob('job-1', message, 'worker-1', result, 60000)

      // Wait for notification
      await notificationReceived

      // Verify state
      const state = await storage.getJobState('job-1')
      assert.ok(state?.startsWith('completed:'))

      // Verify result stored
      const storedResult = await storage.getResult('job-1')
      assert.deepStrictEqual(storedResult, result)

      // Verify removed from processing queue
      const processing = await storage.getProcessingJobs('worker-1')
      assert.strictEqual(processing.length, 0)

      await unsubscribe()
    })

    it('should fail job atomically', async () => {
      const message = Buffer.from('fail-test')
      const error = Buffer.from(JSON.stringify({ message: 'Error' }))

      await storage.enqueue('job-1', message, Date.now())
      await storage.dequeue('worker-1', 1)
      await storage.setJobState('job-1', 'processing:123:worker-1')

      const { value: notificationReceived, unsubscribe } = await promisifyCallback<string>(handler =>
        storage.subscribeToJob('job-1', handler))

      await storage.failJob('job-1', message, 'worker-1', error, 60000)

      // Wait for notification
      const notifiedStatus = await notificationReceived

      // Verify state
      const state = await storage.getJobState('job-1')
      assert.ok(state?.startsWith('failed:'))

      // Verify error stored
      const storedError = await storage.getError('job-1')
      assert.deepStrictEqual(storedError, error)

      // Verify notification
      assert.strictEqual(notifiedStatus, 'failed')

      await unsubscribe()
    })

    it('should retry job atomically', async () => {
      const message = Buffer.from(JSON.stringify({ id: 'job-1', payload: 'test', attempts: 0 }))

      await storage.enqueue('job-1', message, Date.now())
      await storage.dequeue('worker-1', 1)
      await storage.setJobState('job-1', 'processing:123:worker-1')

      const updatedMessage = Buffer.from(JSON.stringify({ id: 'job-1', payload: 'test', attempts: 1 }))
      await storage.retryJob('job-1', updatedMessage, 'worker-1', 1)

      // Verify state
      const state = await storage.getJobState('job-1')
      assert.ok(state?.startsWith('failing:'))
      assert.ok(state?.endsWith(':1'))

      // Verify job is back in queue
      const dequeued = await storage.dequeue('worker-2', 1)
      assert.deepStrictEqual(dequeued, updatedMessage)
    })
  })

  describe('clear', () => {
    it('should clear all data', async () => {
      await storage.enqueue('job-1', Buffer.from('test'), Date.now())
      await storage.setResult('job-1', Buffer.from('result'), 60000)
      await storage.registerWorker('worker-1', 60000)

      await storage.clear()

      assert.strictEqual(await storage.getJobState('job-1'), null)
      assert.strictEqual(await storage.getResult('job-1'), null)
      assert.deepStrictEqual(await storage.getWorkers(), [])
    })
  })

  describe('leader election', () => {
    let basePath: string
    let leaderStorage: FileStorage
    let leaderCleanup: () => Promise<void>

    beforeEach(async () => {
      basePath = await mkdtemp(join(tmpdir(), 'job-queue-leader-test-'))
      const result = await createFileStorage({ basePath })
      leaderStorage = result.storage
      leaderCleanup = result.cleanup
      await leaderStorage.connect()
    })

    afterEach(async () => {
      await leaderStorage.clear()
      await leaderStorage.disconnect()
      await leaderCleanup()
    })

    it('should acquire lock when no lock exists', async () => {
      const acquired = await leaderStorage.acquireLeaderLock('test-lock', 'owner-1', 10000)
      assert.strictEqual(acquired, true)
    })

    it('should fail to acquire lock held by another', async () => {
      await leaderStorage.acquireLeaderLock('test-lock', 'owner-1', 10000)
      const acquired = await leaderStorage.acquireLeaderLock('test-lock', 'owner-2', 10000)
      assert.strictEqual(acquired, false)
    })

    it('should acquire lock when expired', async () => {
      await leaderStorage.acquireLeaderLock('test-lock', 'owner-1', 10)
      await sleep(50)
      const acquired = await leaderStorage.acquireLeaderLock('test-lock', 'owner-2', 10000)
      assert.strictEqual(acquired, true)
    })

    it('should renew lock by same owner', async () => {
      await leaderStorage.acquireLeaderLock('test-lock', 'owner-1', 10000)
      const renewed = await leaderStorage.renewLeaderLock('test-lock', 'owner-1', 10000)
      assert.strictEqual(renewed, true)
    })

    it('should fail to renew lock by different owner', async () => {
      await leaderStorage.acquireLeaderLock('test-lock', 'owner-1', 10000)
      const renewed = await leaderStorage.renewLeaderLock('test-lock', 'owner-2', 10000)
      assert.strictEqual(renewed, false)
    })

    it('should release lock by same owner and make it acquirable', async () => {
      await leaderStorage.acquireLeaderLock('test-lock', 'owner-1', 10000)
      const released = await leaderStorage.releaseLeaderLock('test-lock', 'owner-1')
      assert.strictEqual(released, true)

      const acquired = await leaderStorage.acquireLeaderLock('test-lock', 'owner-2', 10000)
      assert.strictEqual(acquired, true)
    })

    it('should fail to release lock by different owner', async () => {
      await leaderStorage.acquireLeaderLock('test-lock', 'owner-1', 10000)
      const released = await leaderStorage.releaseLeaderLock('test-lock', 'owner-2')
      assert.strictEqual(released, false)
    })
  })

  describe('cleanup leader behavior', () => {
    it('should clean up expired results with only one leader among two instances', async () => {
      const basePath = await mkdtemp(join(tmpdir(), 'job-queue-cleanup-test-'))

      const { storage: storage1 } = await createFileStorage({ basePath })
      const { storage: storage2 } = await createFileStorage({ basePath })

      await storage1.connect()
      await storage2.connect()

      try {
        // Store a result with very short TTL (50ms)
        await storage1.setResult('job-1', Buffer.from('result-1'), 50)

        // Wait for result to expire and cleanup to run
        await sleep(300)

        // Result should be cleaned up by whichever instance is leader
        const result = await storage1.getResult('job-1')
        assert.strictEqual(result, null, 'expired result should be cleaned up')
      } finally {
        await storage1.disconnect()
        await storage2.disconnect()
        await rm(basePath, { recursive: true, force: true })
      }
    })

    it('should failover cleanup when leader disconnects', async () => {
      const basePath = await mkdtemp(join(tmpdir(), 'job-queue-failover-test-'))

      const { storage: storage1 } = await createFileStorage({ basePath })
      await storage1.connect()

      // storage1 is now leader. Disconnect it.
      await storage1.disconnect()

      // Connect storage2 - it should become leader
      const { storage: storage2 } = await createFileStorage({ basePath })
      await storage2.connect()

      try {
        // Store a result with very short TTL (50ms)
        await storage2.setResult('job-2', Buffer.from('result-2'), 50)

        // Wait for result to expire and cleanup to run
        await sleep(300)

        // Result should be cleaned up by storage2 as the new leader
        const result = await storage2.getResult('job-2')
        assert.strictEqual(result, null, 'expired result should be cleaned up by new leader')
      } finally {
        await storage2.disconnect()
        await rm(basePath, { recursive: true, force: true })
      }
    })
  })
})
