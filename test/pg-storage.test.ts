import assert from 'node:assert'
import { afterEach, beforeEach, describe, it } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { type PgStorage } from '../src/storage/pg.ts'
import { createPgStorage } from './fixtures/pg.ts'
import { promisifyCallback, waitForCallbacks } from './helpers/events.ts'

describe('PgStorage', () => {
  let storage: PgStorage

  beforeEach(async () => {
    storage = createPgStorage()
    await storage.connect()
  })

  afterEach(async () => {
    await storage.clear()
    await storage.disconnect()
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

    it('should wake up dequeue waiter when job is enqueued', async () => {
      // Start dequeue that will block
      const dequeuePromise = storage.dequeue('worker-1', 5)

      // Give it time to register the waiter
      await sleep(50)

      // Enqueue a job - should wake up the waiter via NOTIFY
      const msg = Buffer.from('wakeup-test')
      await storage.enqueue('job-1', msg, Date.now())

      const result = await dequeuePromise
      assert.deepStrictEqual(result, msg)
    })
  })

  describe('job state', () => {
    it('should get and set job state', async () => {
      await storage.enqueue('job-1', Buffer.from('test'), Date.now())
      await storage.setJobState('job-1', 'processing:123456:worker-1')
      const state = await storage.getJobState('job-1')

      assert.strictEqual(state, 'processing:123456:worker-1')
    })

    it('should return null for non-existent job', async () => {
      const state = await storage.getJobState('non-existent')
      assert.strictEqual(state, null)
    })

    it('should delete job', async () => {
      await storage.enqueue('job-1', Buffer.from('test'), Date.now())
      const deleted = await storage.deleteJob('job-1')

      assert.strictEqual(deleted, true)
      assert.strictEqual(await storage.getJobState('job-1'), null)
    })

    it('should return false when deleting non-existent job', async () => {
      const deleted = await storage.deleteJob('non-existent')
      assert.strictEqual(deleted, false)
    })

    it('should get multiple job states', async () => {
      await storage.enqueue('job-1', Buffer.from('a'), Date.now())
      await storage.setJobState('job-1', 'queued:1')
      await storage.enqueue('job-2', Buffer.from('b'), Date.now())
      await storage.setJobState('job-2', 'processing:2')

      const states = await storage.getJobStates(['job-1', 'job-2', 'job-3'])

      assert.strictEqual(states.get('job-1'), 'queued:1')
      assert.strictEqual(states.get('job-2'), 'processing:2')
      assert.strictEqual(states.get('job-3'), null)
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
      await storage.setResult('job-1', Buffer.from('short-lived'), 20)
      await sleep(30)

      const result = await storage.getResult('job-1')
      assert.strictEqual(result, null)
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
      await storage.registerWorker('worker-1', 20)
      await sleep(30)

      const workers = await storage.getWorkers()
      assert.deepStrictEqual(workers, [])
    })
  })

  describe('notifications', () => {
    it('should notify on job completion', async () => {
      const { value, unsubscribe } = await promisifyCallback<string>(handler =>
        storage.subscribeToJob('job-1', handler))

      await sleep(50)

      await storage.notifyJobComplete('job-1', 'completed')

      const notifiedStatus = await value
      assert.strictEqual(notifiedStatus, 'completed')

      await unsubscribe()
    })

    it('should notify on job failure', async () => {
      const { value, unsubscribe } = await promisifyCallback<string>(handler =>
        storage.subscribeToJob('job-1', handler))

      await sleep(50)

      await storage.notifyJobComplete('job-1', 'failed')

      const notifiedStatus = await value
      assert.strictEqual(notifiedStatus, 'failed')

      await unsubscribe()
    })
  })

  describe('events', () => {
    it('should emit events on state changes', async () => {
      const events: Array<{ id: string; event: string }> = []
      const { callback, promise: eventsReceived } = waitForCallbacks(2)

      const unsubscribe = await storage.subscribeToEvents((id, event) => {
        events.push({ id, event })
        callback()
      })

      await sleep(50)

      await storage.publishEvent('job-1', 'processing')
      await storage.publishEvent('job-1', 'completed')

      await eventsReceived

      assert.deepStrictEqual(events, [
        { id: 'job-1', event: 'processing' },
        { id: 'job-1', event: 'completed' }
      ])

      await unsubscribe()
    })

    it('should emit queued event on enqueue', async () => {
      const events: Array<{ id: string; event: string }> = []
      const { callback, promise: eventReceived } = waitForCallbacks(1)

      const unsubscribe = await storage.subscribeToEvents((id, event) => {
        events.push({ id, event })
        callback()
      })

      await sleep(50)

      await storage.enqueue('job-1', Buffer.from('test'), Date.now())

      await eventReceived

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

      await sleep(50)

      await storage.completeJob('job-1', message, 'worker-1', result, 60000)

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

      await sleep(50)

      await storage.failJob('job-1', message, 'worker-1', error, 60000)

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

  describe('leader election', () => {
    it('should acquire lock when no lock exists', async () => {
      const acquired = await storage.acquireLeaderLock('test-lock', 'owner-1', 60000)
      assert.strictEqual(acquired, true)
    })

    it('should fail to acquire lock held by another', async () => {
      await storage.acquireLeaderLock('test-lock', 'owner-1', 60000)
      const acquired = await storage.acquireLeaderLock('test-lock', 'owner-2', 60000)
      assert.strictEqual(acquired, false)
    })

    it('should acquire lock when expired', async () => {
      await storage.acquireLeaderLock('test-lock', 'owner-1', 10)
      await sleep(20)

      const acquired = await storage.acquireLeaderLock('test-lock', 'owner-2', 60000)
      assert.strictEqual(acquired, true)
    })

    it('should renew lock by same owner', async () => {
      await storage.acquireLeaderLock('test-lock', 'owner-1', 60000)
      const renewed = await storage.renewLeaderLock('test-lock', 'owner-1', 60000)
      assert.strictEqual(renewed, true)
    })

    it('should fail to renew lock by different owner', async () => {
      await storage.acquireLeaderLock('test-lock', 'owner-1', 60000)
      const renewed = await storage.renewLeaderLock('test-lock', 'owner-2', 60000)
      assert.strictEqual(renewed, false)
    })

    it('should release lock by same owner', async () => {
      await storage.acquireLeaderLock('test-lock', 'owner-1', 60000)
      const released = await storage.releaseLeaderLock('test-lock', 'owner-1')
      assert.strictEqual(released, true)

      // Should be acquirable again
      const acquired = await storage.acquireLeaderLock('test-lock', 'owner-2', 60000)
      assert.strictEqual(acquired, true)
    })

    it('should fail to release lock by different owner', async () => {
      await storage.acquireLeaderLock('test-lock', 'owner-1', 60000)
      const released = await storage.releaseLeaderLock('test-lock', 'owner-2')
      assert.strictEqual(released, false)
    })
  })

  describe('dedup expiry', () => {
    it('should allow re-enqueue after dedup expiry', async () => {
      await storage.enqueue('job-1', Buffer.from('first'), Date.now())
      await storage.setJobExpiry('job-1', 20)

      await sleep(30)

      const result = await storage.enqueue('job-1', Buffer.from('second'), Date.now())
      assert.strictEqual(result, null, 'should allow re-enqueue after expiry')
    })

    it('should block re-enqueue within TTL window', async () => {
      await storage.enqueue('job-1', Buffer.from('first'), Date.now())
      await storage.setJobExpiry('job-1', 60000)

      const result = await storage.enqueue('job-1', Buffer.from('second'), Date.now())
      assert.ok(result, 'should block re-enqueue within TTL')
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

  describe('namespace', () => {
    it('should isolate data between namespaces', async () => {
      const ns1 = storage.createNamespace('emails') as PgStorage
      const ns2 = storage.createNamespace('images') as PgStorage

      await ns1.connect()
      await ns2.connect()

      try {
        await ns1.enqueue('job-1', Buffer.from('email-data'), Date.now())
        await ns2.enqueue('job-1', Buffer.from('image-data'), Date.now())

        const d1 = await ns1.dequeue('worker-1', 1)
        const d2 = await ns2.dequeue('worker-1', 1)

        assert.deepStrictEqual(d1, Buffer.from('email-data'))
        assert.deepStrictEqual(d2, Buffer.from('image-data'))
      } finally {
        await ns1.clear()
        await ns2.clear()
        await ns2.disconnect()
        await ns1.disconnect()
      }
    })
  })
})
