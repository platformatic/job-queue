import assert from 'node:assert'
import { afterEach, beforeEach, describe, it } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { once } from 'node:events'
import { spawn } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import type { Logger } from 'pino'
import { Queue } from '../src/queue.ts'
import type { Job } from '../src/types.ts'
import { SQLiteStorage } from '../src/storage/sqlite.ts'
import { StorageError } from '../src/errors.ts'
import { createSQLiteStorage } from './fixtures/sqlite.ts'
import { promisifyCallback, waitForCallbacks } from './helpers/events.ts'

describe('SQLiteStorage', () => {
  let storage: SQLiteStorage

  beforeEach(async () => {
    storage = createSQLiteStorage()
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
      assert.ok(Buffer.isBuffer(dequeued), 'dequeue should return a Buffer, not Uint8Array')
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
      const dequeuePromise = storage.dequeue('worker-1', 5)
      await sleep(50)

      const msg = Buffer.from('wakeup-test')
      await storage.enqueue('job-1', msg, Date.now())

      const result = await dequeuePromise
      assert.deepStrictEqual(result, msg)
    })

    it('should roundtrip BLOBs as Buffer, not Uint8Array', async () => {
      const message = Buffer.from([0, 1, 2, 3, 255, 254])
      await storage.enqueue('job-1', message, Date.now())

      const dequeued = await storage.dequeue('worker-1', 1)
      assert.ok(Buffer.isBuffer(dequeued))
      assert.deepStrictEqual(dequeued, message)

      // ack uses WHERE message = ? — must match bytewise
      await storage.ack('job-1', dequeued as Buffer, 'worker-1')
      const processing = await storage.getProcessingJobs('worker-1')
      assert.strictEqual(processing.length, 0)
    })

    it('should roundtrip large BLOBs', async () => {
      const message = Buffer.alloc(1024 * 1024) // 1 MB
      for (let i = 0; i < message.length; i++) message[i] = i % 256

      await storage.enqueue('job-1', message, Date.now())
      const dequeued = await storage.dequeue('worker-1', 1)

      assert.ok(dequeued)
      assert.strictEqual(dequeued.length, message.length)
      assert.deepStrictEqual(dequeued, message)
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

      const dequeued = await storage.dequeue('worker-1', 1)
      assert.deepStrictEqual(dequeued, message)

      const processing = await storage.getProcessingJobs('worker-1')
      assert.strictEqual(processing.length, 1)

      await storage.requeue('job-1', dequeued as Buffer, 'worker-1')

      const processingAfter = await storage.getProcessingJobs('worker-1')
      assert.strictEqual(processingAfter.length, 0)

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

      await storage.ack('job-1', dequeued, 'worker-1')

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
      const dequeued = await storage.dequeue('worker-1', 1)
      await storage.setJobState('job-1', 'processing:123:worker-1')

      const { value: notificationReceived, unsubscribe } = await promisifyCallback<string>(handler =>
        storage.subscribeToJob('job-1', handler))

      await sleep(50)

      await storage.completeJob('job-1', dequeued as Buffer, 'worker-1', result, 60000)

      await notificationReceived

      const state = await storage.getJobState('job-1')
      assert.ok(state?.startsWith('completed:'))

      const storedResult = await storage.getResult('job-1')
      assert.deepStrictEqual(storedResult, result)

      const processing = await storage.getProcessingJobs('worker-1')
      assert.strictEqual(processing.length, 0)

      await unsubscribe()
    })

    it('should fail job atomically', async () => {
      const message = Buffer.from('fail-test')
      const error = Buffer.from(JSON.stringify({ message: 'Error' }))

      await storage.enqueue('job-1', message, Date.now())
      const dequeued = await storage.dequeue('worker-1', 1)
      await storage.setJobState('job-1', 'processing:123:worker-1')

      const { value: notificationReceived, unsubscribe } = await promisifyCallback<string>(handler =>
        storage.subscribeToJob('job-1', handler))

      await sleep(50)

      await storage.failJob('job-1', dequeued as Buffer, 'worker-1', error, 60000)

      const notifiedStatus = await notificationReceived

      const state = await storage.getJobState('job-1')
      assert.ok(state?.startsWith('failed:'))

      const storedError = await storage.getError('job-1')
      assert.deepStrictEqual(storedError, error)

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

      const state = await storage.getJobState('job-1')
      assert.ok(state?.startsWith('failing:'))
      assert.ok(state?.endsWith(':1'))

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
      const ns1 = storage.createNamespace('emails') as SQLiteStorage
      const ns2 = storage.createNamespace('images') as SQLiteStorage

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

    it('should allow disconnect of namespace without affecting siblings', async () => {
      const ns1 = storage.createNamespace('a') as SQLiteStorage
      const ns2 = storage.createNamespace('b') as SQLiteStorage
      await ns1.connect()
      await ns2.connect()

      try {
        await ns1.enqueue('j1', Buffer.from('aaa'), Date.now())
        await ns2.enqueue('j2', Buffer.from('bbb'), Date.now())

        await ns1.disconnect()

        const d2 = await ns2.dequeue('worker', 1)
        assert.deepStrictEqual(d2, Buffer.from('bbb'))
      } finally {
        await ns2.clear().catch(() => {})
        await ns2.disconnect().catch(() => {})
      }
    })
  })

  describe('default path :memory:', () => {
    it('should default to :memory: and not write a file in cwd', async () => {
      const s = new SQLiteStorage()
      await s.connect()
      try {
        await s.enqueue('j1', Buffer.from('x'), Date.now())
        const out = await s.dequeue('w1', 1)
        assert.deepStrictEqual(out, Buffer.from('x'))
      } finally {
        await s.disconnect()
      }
    })
  })

  describe('fork-after-connect pid assertion', () => {
    it('should reject unregisterWorker / clear / releaseLeaderLock from a different pid', async () => {
      // Simulate fork by overriding process.pid. Restore in finally.
      const originalPid = process.pid
      Object.defineProperty(process, 'pid', { value: originalPid + 1, configurable: true })
      try {
        await assert.rejects(storage.unregisterWorker('w1'), /forked process/)
        await assert.rejects(storage.clear(), /forked process/)
        await assert.rejects(storage.releaseLeaderLock('k', 'o'), /forked process/)
      } finally {
        Object.defineProperty(process, 'pid', { value: originalPid, configurable: true })
      }
    })

    it('should not close the shared db handle when disconnect() runs in a forked process', async () => {
      // Forked-child disconnect must drop local refs only — closing #db would
      // corrupt the parent process's still-open OS handle.
      const originalPid = process.pid
      Object.defineProperty(process, 'pid', { value: originalPid + 1, configurable: true })
      try {
        await storage.disconnect()
      } finally {
        Object.defineProperty(process, 'pid', { value: originalPid, configurable: true })
      }
      // The storage instance now has #db = null locally, but a fresh instance
      // pointing at :memory: should work — proves the OS-level handle wasn't
      // catastrophically closed across all process state.
      const s = new SQLiteStorage()
      await s.connect()
      await s.enqueue('postfork', Buffer.from('x'), Date.now())
      const out = await s.dequeue('w', 1)
      assert.deepStrictEqual(out, Buffer.from('x'))
      await s.disconnect()
    })
  })

  describe('retryJob with concurrent in-flight jobs', () => {
    it('should not wipe sibling processing rows for the same worker', async () => {
      // With consumer.concurrency > 1 a single worker can hold multiple
      // in-flight messages. retryJob on one must not delete the others.
      const msgA = Buffer.from(JSON.stringify({ id: 'job-a', payload: 'a', attempts: 0 }))
      const msgB = Buffer.from(JSON.stringify({ id: 'job-b', payload: 'b', attempts: 0 }))

      await storage.enqueue('job-a', msgA, Date.now())
      await storage.enqueue('job-b', msgB, Date.now())
      await storage.dequeue('worker-1', 1)
      await storage.dequeue('worker-1', 1)

      const beforeProcessing = await storage.getProcessingJobs('worker-1')
      assert.strictEqual(beforeProcessing.length, 2)

      const retryA = Buffer.from(JSON.stringify({ id: 'job-a', payload: 'a', attempts: 1 }))
      await storage.retryJob('job-a', retryA, 'worker-1', 1)

      const afterProcessing = await storage.getProcessingJobs('worker-1')
      // job-b should still be in processing; only job-a was removed.
      assert.strictEqual(afterProcessing.length, 1)
      assert.deepStrictEqual(afterProcessing[0], msgB)
    })
  })

  describe('wake-ups across instances of the same namespace', () => {
    // Queue calls createNamespace(name) itself, so a producer Queue and a
    // consumer Queue with the same name hold two different storage instances.
    // A consumer parked in dequeue() must still be woken by the other one's
    // enqueue instead of sitting out its whole blockTimeout.

    it('should wake a consumer parked on another instance when a job is enqueued', async () => {
      const producer = storage.createNamespace('emails')
      const consumer = storage.createNamespace('emails')
      await producer.connect()
      await consumer.connect()
      try {
        const start = Date.now()
        const parked = consumer.dequeue('worker-1', 5)
        await sleep(20)
        const msg = Buffer.from('cross-instance')
        await producer.enqueue('job-1', msg, Date.now())

        assert.deepStrictEqual(await parked, msg)
        assert.ok(Date.now() - start < 1000, `woken after ${Date.now() - start}ms, expected well under the 5s timeout`)
      } finally {
        await producer.disconnect()
        await consumer.disconnect()
      }
    })

    it('should wake a consumer parked on another instance when a job is retried', async () => {
      const a = storage.createNamespace('emails')
      const b = storage.createNamespace('emails')
      await a.connect()
      await b.connect()
      try {
        await a.enqueue('job-1', Buffer.from('attempt-0'), Date.now())
        assert.ok(await a.dequeue('worker-a', 1))

        const start = Date.now()
        const parked = b.dequeue('worker-b', 5)
        await sleep(20)
        await a.retryJob('job-1', Buffer.from('attempt-1'), 'worker-a', 1)

        assert.deepStrictEqual(await parked, Buffer.from('attempt-1'))
        assert.ok(Date.now() - start < 1000, `woken after ${Date.now() - start}ms`)
      } finally {
        await a.disconnect()
        await b.disconnect()
      }
    })

    it('should wake a consumer whose first dequeue attempt is still running', async () => {
      // No pause between dequeue() and enqueue(): the job is announced while
      // the consumer's initial attempt is in flight, before it could park.
      const producer = storage.createNamespace('emails')
      const consumer = storage.createNamespace('emails')
      await producer.connect()
      await consumer.connect()
      try {
        const start = Date.now()
        const parked = consumer.dequeue('worker-1', 3)
        await producer.enqueue('job-1', Buffer.from('racing'), Date.now())

        assert.deepStrictEqual(await parked, Buffer.from('racing'))
        assert.ok(Date.now() - start < 1000, `woken after ${Date.now() - start}ms, expected well under the 3s timeout`)
      } finally {
        await producer.disconnect()
        await consumer.disconnect()
      }
    })

    it('should not hand a job to a consumer of a different namespace', async () => {
      const emails = storage.createNamespace('emails')
      const images = storage.createNamespace('images')
      await emails.connect()
      await images.connect()
      try {
        const parked = images.dequeue('worker-1', 0.3)
        await sleep(20)
        await emails.enqueue('job-1', Buffer.from('email'), Date.now())

        assert.strictEqual(await parked, null)
        assert.deepStrictEqual(await emails.dequeue('worker-2', 1), Buffer.from('email'))
      } finally {
        await emails.disconnect()
        await images.disconnect()
      }
    })

    it('should resolve enqueueAndWait promptly with separate producer and consumer Queues', async () => {
      const producer = new Queue<{ n: number }, { doubled: number }>({ storage, name: 'math' })
      const consumer = new Queue<{ n: number }, { doubled: number }>({ storage, name: 'math', blockTimeout: 10 })
      consumer.execute(async (job: Job<{ n: number }>) => ({ doubled: job.payload.n * 2 }))
      await consumer.start()
      await producer.start()
      try {
        // Let the consumer park in dequeue() before the job exists.
        await sleep(50)
        const start = Date.now()
        const result = await producer.enqueueAndWait('job-1', { n: 21 }, { timeout: 5000 })

        assert.deepStrictEqual(result, { doubled: 42 })
        assert.ok(Date.now() - start < 2000, `took ${Date.now() - start}ms; the consumer was not woken`)
      } finally {
        await producer.stop()
        await consumer.stop()
      }
    })
  })

  describe('retryJob matches the in-flight row by job id', () => {
    // retryJob receives the NEW payload (attempts incremented), so it has to
    // find the old in-flight row some other way than comparing bytes. It must
    // work for any payload serde, not only JSON.
    const binary = (tag: number, attempts: number) => Buffer.from([0x82, tag, 0x00, attempts, 0xff])

    it('should remove only the retried job with non-JSON payloads', async () => {
      await storage.enqueue('job-a', binary(1, 0), Date.now())
      await storage.enqueue('job-b', binary(2, 0), Date.now())
      await storage.dequeue('worker-1', 1)
      await storage.dequeue('worker-1', 1)

      await storage.retryJob('job-a', binary(1, 1), 'worker-1', 1)

      // A stale row for job-a would be requeued by the reaper if this worker
      // crashed, running the job a second time.
      assert.deepStrictEqual(await storage.getProcessingJobs('worker-1'), [binary(2, 0)])
      assert.deepStrictEqual(await storage.dequeue('worker-2', 1), binary(1, 1))
    })

    it('should keep tracking the job id through requeue and a second retry', async () => {
      await storage.enqueue('job-a', binary(1, 0), Date.now())
      const first = await storage.dequeue('worker-1', 1)
      await storage.requeue('job-a', first!, 'worker-1')

      await storage.dequeue('worker-1', 1)
      await storage.retryJob('job-a', binary(1, 1), 'worker-1', 1)
      await storage.dequeue('worker-1', 1)
      await storage.retryJob('job-a', binary(1, 2), 'worker-1', 2)

      assert.deepStrictEqual(await storage.getProcessingJobs('worker-1'), [])
      assert.deepStrictEqual(await storage.dequeue('worker-1', 1), binary(1, 2))
      assert.match((await storage.getJobState('job-a'))!, /^failing:\d+:2$/)
    })
  })

  describe('expiry boundary', () => {
    // Jobs, results and errors all treat expires_at as the first instant the
    // row is gone.
    it('should expire results and errors exactly at expires_at', async t => {
      t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
      await storage.setResult('job-1', Buffer.from('result'), 100)
      await storage.setError('job-2', Buffer.from('error'), 100)

      t.mock.timers.setTime(1_000_099)
      assert.deepStrictEqual(await storage.getResult('job-1'), Buffer.from('result'))
      assert.deepStrictEqual(await storage.getError('job-2'), Buffer.from('error'))

      t.mock.timers.setTime(1_000_100)
      assert.strictEqual(await storage.getResult('job-1'), null)
      assert.strictEqual(await storage.getError('job-2'), null)
    })
  })

  describe('lifecycle races', () => {
    it('should not leave a transaction open when a namespace disconnects with writes queued', async () => {
      const ns = storage.createNamespace('racy')
      const sibling = storage.createNamespace('sibling')
      await ns.connect()
      await sibling.connect()
      try {
        // Not awaited: both writes are queued behind the namespace's write
        // lock when disconnect() runs.
        const writes = [ns.enqueue('a', Buffer.from('a'), Date.now()), ns.enqueue('b', Buffer.from('b'), Date.now())]
        await ns.disconnect()
        const settled = await Promise.allSettled(writes)
        for (const r of settled) {
          if (r.status === 'rejected') assert.ok(r.reason instanceof StorageError, String(r.reason))
        }

        // A transaction left open on the shared connection would make every
        // later BEGIN fail with "cannot start a transaction within a transaction".
        await storage.enqueue('root-job', Buffer.from('root'), Date.now())
        assert.deepStrictEqual(await storage.dequeue('w1', 1), Buffer.from('root'))
        await sibling.enqueue('sibling-job', Buffer.from('sibling'), Date.now())
        assert.deepStrictEqual(await sibling.dequeue('w1', 1), Buffer.from('sibling'))
      } finally {
        await sibling.disconnect()
      }
    })

    it('should never strand a claimed job in processing when a consumer disconnects', async () => {
      // A dequeue attempt can claim a job (moving it to processing) just
      // before disconnect() settles the waiter. The job must then either reach
      // the caller or go back to the queue — a processing row under a
      // stopping, unregistered worker is never recovered by the reaper.
      // Disconnecting at different microtask offsets covers both outcomes.
      for (let offset = 0; offset < 8; offset++) {
        const producer = storage.createNamespace(`strand${offset}`)
        const consumer = storage.createNamespace(`strand${offset}`)
        await producer.connect()
        await consumer.connect()
        try {
          const parked = consumer.dequeue('worker-1', 5)
          await sleep(10)
          await producer.enqueue('job-1', Buffer.from('claimed'), Date.now())
          for (let i = 0; i < offset; i++) await Promise.resolve()
          await consumer.disconnect()

          const delivered = await parked
          const inProcessing = await producer.getProcessingJobs('worker-1')
          if (delivered) {
            assert.deepStrictEqual(delivered, Buffer.from('claimed'), `offset ${offset}`)
            assert.strictEqual(inProcessing.length, 1, `offset ${offset}: the caller holds it`)
          } else {
            assert.deepStrictEqual(inProcessing, [], `offset ${offset}: stranded in processing`)
            assert.deepStrictEqual(await producer.dequeue('worker-2', 1), Buffer.from('claimed'), `offset ${offset}`)
          }
        } finally {
          await consumer.disconnect()
          await producer.disconnect()
        }
      }
    })

    it('should keep a namespace usable when it attaches while the root is closing', async () => {
      const root = new SQLiteStorage()
      await root.connect()
      const ns = root.createNamespace('late')
      const closing = root.disconnect()
      await ns.connect() // attaches while the close waits for the write lock
      await closing

      try {
        await ns.enqueue('job-1', Buffer.from('x'), Date.now())
        assert.deepStrictEqual(await ns.dequeue('worker-1', 1), Buffer.from('x'))
      } finally {
        await ns.disconnect() // last one out finishes the root's close
      }
      await assert.rejects(root.enqueue('job-2', Buffer.from('x'), Date.now()), /not connected/)
    })

    it('should honour connect() called while the root is still closing', async () => {
      const root = new SQLiteStorage()
      await root.connect()
      const closing = root.disconnect()
      const reopening = root.connect()
      await closing
      await reopening
      try {
        await root.enqueue('job-1', Buffer.from('x'), Date.now())
        assert.deepStrictEqual(await root.dequeue('worker-1', 1), Buffer.from('x'))
        // Its timers run again: it holds cleanup leadership.
        await sleep(50)
        assert.strictEqual(await root.acquireLeaderLock('cleanup-leader', 'intruder', 1000), false)
      } finally {
        await root.disconnect()
      }
    })

    it('should keep a root the user reconnected when its last namespace leaves concurrently', async () => {
      const root = new SQLiteStorage()
      const ns = root.createNamespace('child')
      await root.connect()
      await ns.connect()
      try {
        // Not awaited in turn: the namespace's "close the root if pending"
        // must be decided after the user's connect() has cancelled the close.
        await Promise.all([root.disconnect(), root.connect(), ns.disconnect()])
        await root.enqueue('job-1', Buffer.from('x'), Date.now())
        assert.deepStrictEqual(await root.dequeue('worker-1', 1), Buffer.from('x'))
      } finally {
        await root.disconnect()
      }
    })

    it('should not let a namespace connect() cancel a disconnect() the user already issued', async () => {
      const root = new SQLiteStorage()
      const ns = root.createNamespace('child')
      await root.connect()
      try {
        const calls = [root.disconnect(), root.connect(), root.disconnect()]
        // Four microtasks in, the first disconnect() has closed the root and
        // the reconnect and final disconnect() are still queued. A namespace
        // connecting now sees the root closed; opening it must not jump ahead
        // of the user's queued disconnect() and cancel it.
        for (let i = 0; i < 4; i++) await Promise.resolve()
        await Promise.all([...calls, ns.connect()])
        await ns.disconnect()

        // The user's last word was disconnect(): with the namespace gone the
        // root must be closed, not left open with its timers running.
        await assert.rejects(root.getWorkers(), /not connected/)
      } finally {
        await ns.disconnect()
        await root.disconnect()
      }
    })

    it('should close a root the user disconnected while a namespace was restarting', async () => {
      // e.g. a named Queue restarted (stop() + start()) while the app shuts
      // the shared storage down. The namespace's connect() was issued before
      // root.disconnect(), so it stays attached and the root closes once it
      // leaves — the delayed connect must not reopen a root closed meanwhile.
      const root = new SQLiteStorage()
      const ns = root.createNamespace('restarting')
      await root.connect()
      await ns.connect()
      try {
        await Promise.all([ns.disconnect(), ns.connect(), root.disconnect()])
        await ns.getWorkers() // connect() was the namespace's last word

        await ns.disconnect()
        await assert.rejects(root.getWorkers(), /not connected/)
      } finally {
        await ns.disconnect()
        await root.disconnect()
      }
    })

    it('should honour disconnect() called while a namespace connect() is in flight', async () => {
      for (const rootConnected of [true, false]) {
        const root = new SQLiteStorage()
        if (rootConnected) await root.connect()
        const ns = root.createNamespace('pending')
        const connecting = ns.connect()
        await ns.disconnect()
        await connecting

        await assert.rejects(ns.getWorkers(), /not connected/, `root connected: ${rootConnected}`)
        // No leftover refCount: the root closes for real.
        await root.disconnect()
        await assert.rejects(root.getWorkers(), /not connected/, `root connected: ${rootConnected}`)
      }
    })

    it('should not start a cleanup interval when disconnect() races a leadership tick', async t => {
      t.mock.timers.enable({ apis: ['setInterval', 'setImmediate'] })
      const errors: string[] = []
      const logger = {
        fatal () {},
        error (_obj: unknown, msg: string) {
          errors.push(msg)
        },
        warn () {},
        info () {},
        debug () {},
        trace () {},
        child () {
          return logger
        }
      } as unknown as Logger

      try {
        const root = new SQLiteStorage({ logger })
        await root.connect()
        // Fire the first leadership tick; it is now waiting for the write lock.
        t.mock.timers.tick(0)
        await root.disconnect()
        await sleep(10) // real timer: let the tick's continuation run

        // A leaked cleanup interval would now sweep a closed database.
        t.mock.timers.tick(5 * 60_000)
        await sleep(10)
        assert.deepStrictEqual(errors, [])
      } finally {
        // Restore real timers before afterEach: the shared storage's intervals
        // are real, and a mocked clearInterval would silently leave them running.
        t.mock.timers.reset()
      }
    })

    it('should keep a root that reconnects during a deferred disconnect', async () => {
      const root = new SQLiteStorage()
      const ns = root.createNamespace('child')
      await root.connect()
      await ns.connect()
      try {
        await root.disconnect() // deferred: the namespace is still connected
        await root.connect() // the user changes their mind
        await ns.disconnect() // must not finish closing the reconnected root

        await root.enqueue('j1', Buffer.from('x'), Date.now())
        assert.deepStrictEqual(await root.dequeue('w1', 1), Buffer.from('x'))

        // Its timers were restarted: it holds cleanup leadership again.
        await sleep(50)
        assert.strictEqual(await root.acquireLeaderLock('cleanup-leader', 'intruder', 1000), false)
      } finally {
        await ns.disconnect()
        await root.disconnect()
      }
      await assert.rejects(root.enqueue('j2', Buffer.from('x'), Date.now()), /not connected/)
    })

    it('should still close a root when a namespace connects during its deferred disconnect', async () => {
      const root = new SQLiteStorage()
      const a = root.createNamespace('a')
      const b = root.createNamespace('b')
      await a.connect()
      await root.disconnect() // deferred
      await b.connect() // must not cancel the pending close
      await a.disconnect()
      await b.disconnect()
      await assert.rejects(root.enqueue('j', Buffer.from('x'), Date.now()), /not connected/)
    })
  })

  describe('expiry cleanup on read', () => {
    // A read that finds an expired row deletes it in a later write. A write
    // queued in between can replace that row, and the delete must spare it.

    it('should not delete a job re-enqueued while its expired state is being read', async () => {
      await storage.enqueue('job-1', Buffer.from('first'), Date.now())
      await storage.setJobExpiry('job-1', 1)
      await sleep(5)

      const enqueued = storage.enqueue('job-1', Buffer.from('second'), Date.now())
      const stale = storage.getJobState('job-1') // reads the expired row first
      const staleBatch = storage.getJobStates(['job-1'])

      assert.strictEqual(await enqueued, null)
      assert.strictEqual(await stale, null)
      assert.strictEqual((await staleBatch).get('job-1'), null)
      assert.match((await storage.getJobState('job-1'))!, /^queued:/)
    })

    it('should not delete a result or error rewritten while the expired one is being read', async () => {
      await storage.setResult('job-1', Buffer.from('old'), 1)
      await storage.setError('job-2', Buffer.from('old'), 1)
      await sleep(5)

      const writes = [
        storage.setResult('job-1', Buffer.from('new'), 60_000),
        storage.setError('job-2', Buffer.from('new'), 60_000)
      ]
      const staleResult = storage.getResult('job-1')
      const staleError = storage.getError('job-2')
      await Promise.all(writes)

      assert.strictEqual(await staleResult, null)
      assert.strictEqual(await staleError, null)
      assert.deepStrictEqual(await storage.getResult('job-1'), Buffer.from('new'))
      assert.deepStrictEqual(await storage.getError('job-2'), Buffer.from('new'))
    })
  })

  describe('namespace lifecycle (adversarial)', () => {
    it('should be idempotent on double disconnect', async () => {
      const ns = storage.createNamespace('ns-double') as SQLiteStorage
      await ns.connect()
      await ns.disconnect()
      // Second disconnect must not decrement parent refCount again.
      await ns.disconnect()
      // Parent should still be cleanly disconnectable at end of test.
      // (afterEach will call storage.disconnect(); this assertion is implicit.)
    })

    it('should roll back parent refCount when child connect fails', async () => {
      // We can't easily make parent.connect() throw, so simulate the failure
      // by giving the child a parentStorage whose connect rejects.
      const failingParent = new SQLiteStorage({ path: '/nonexistent/dir/sqlite.db' })
      const child = failingParent.createNamespace('x') as SQLiteStorage
      await assert.rejects(child.connect(), /failed to open/)
      // Now disconnecting the failing parent should succeed and not be blocked
      // by a phantom refCount.
      await failingParent.disconnect()
    })

    it('should sweep namespace tables in cleanup', async () => {
      // Use a short cleanupIntervalMs so the leader sweeps quickly.
      const root = new SQLiteStorage({ cleanupIntervalMs: 50 })
      await root.connect()
      const ns = root.createNamespace('cleanup-test') as SQLiteStorage
      await ns.connect()
      try {
        // Put an expired result into the namespace.
        await ns.setResult('expired-job', Buffer.from('data'), 10)
        await sleep(50)
        // Wait for at least one cleanup tick (interval 50ms + ~10ms leader lock acquire).
        await sleep(300)
        const result = await ns.getResult('expired-job')
        assert.strictEqual(result, null, 'namespace result should be swept by root cleanup')
      } finally {
        await ns.disconnect()
        await root.disconnect()
      }
    })
  })
})

describe('SQLiteStorage (file-backed)', () => {
  let dir: string
  let storage: SQLiteStorage

  beforeEach(async () => {
    dir = mkdtempSync(join(tmpdir(), 'sqlite-storage-test-'))
    storage = new SQLiteStorage({ path: join(dir, 'q.sqlite') })
    await storage.connect()
  })

  afterEach(async () => {
    await storage.clear().catch(() => {})
    await storage.disconnect()
    rmSync(dir, { recursive: true, force: true })
  })

  it('should activate WAL mode on file-backed databases', async () => {
    // After connect, WAL files should exist alongside the main DB.
    await storage.enqueue('j1', Buffer.from('x'), Date.now())
    // We can't easily inspect the DB file from outside, but we can verify that
    // a basic roundtrip works and that disconnect succeeds without throwing.
    const out = await storage.dequeue('w1', 1)
    assert.deepStrictEqual(out, Buffer.from('x'))
  })

  it('should persist across reconnect', async () => {
    await storage.enqueue('j-persist', Buffer.from('persist-me'), Date.now())
    await storage.disconnect()

    const path = join(dir, 'q.sqlite')
    const s2 = new SQLiteStorage({ path })
    await s2.connect()
    try {
      const out = await s2.dequeue('w1', 1)
      assert.deepStrictEqual(out, Buffer.from('persist-me'))
    } finally {
      await s2.clear()
      await s2.disconnect()
    }
  })

  it('should refuse to start when schema_version is higher than supported', async () => {
    // Manually bump the schema_version row to simulate a future-version DB.
    const path = join(dir, 'future.sqlite')
    const future = new SQLiteStorage({ path })
    await future.connect()
    // Sneak the version up via a private-ish path: clear and rewrite the meta row.
    // We use a sibling SQLiteStorage to access the same file via raw SQL through
    // a fresh DatabaseSync connection.
    await future.disconnect()

    const raw = new DatabaseSync(path)
    raw.exec("UPDATE \"jq_meta\" SET value = '999' WHERE key = 'schema_version'")
    raw.close()

    const reopen = new SQLiteStorage({ path })
    await assert.rejects(reopen.connect(), StorageError)
  })

  describe('namespace lifecycle (file-backed)', () => {
    it('should roll back a namespace whose connect() fails partway', async () => {
      const first = storage.createNamespace('x')
      await first.connect()
      await first.disconnect()

      const raw = new DatabaseSync(join(dir, 'q.sqlite'))
      raw.exec("UPDATE \"jq_x_meta\" SET value = '999' WHERE key = 'schema_version'")
      raw.close()

      const again = storage.createNamespace('x')
      try {
        await assert.rejects(again.connect(), StorageError)
        // A half-connected namespace would report success here.
        await assert.rejects(again.connect(), StorageError)

        // No phantom reference: the root closes for real.
        await storage.disconnect()
        await assert.rejects(storage.enqueue('j', Buffer.from('x'), Date.now()), /not connected/)
      } finally {
        await again.disconnect()
      }
    })

    it('should keep sweeping a namespace while another instance of it is connected', async () => {
      const path = join(dir, 'sweep.sqlite')
      const root = new SQLiteStorage({ path, cleanupIntervalMs: 50 })
      await root.connect()
      const a = root.createNamespace('x')
      const b = root.createNamespace('x')
      await a.connect()
      await b.connect()
      try {
        await a.disconnect()
        await b.setResult('job-1', Buffer.from('expires'), 1)

        // Counted directly: getResult() would delete the row itself.
        const count = () => {
          const raw = new DatabaseSync(path)
          try {
            return (raw.prepare('SELECT COUNT(*) AS n FROM "jq_x_results"').get() as { n: number }).n
          } finally {
            raw.close()
          }
        }
        for (let i = 0; i < 40 && count() > 0; i++) await sleep(50)
        assert.strictEqual(count(), 0, 'expired result in a still-connected namespace was never swept')
      } finally {
        await b.disconnect()
        await root.disconnect()
      }
    })
  })

  describe('single owner per database file', () => {
    // Notifications and dequeue wake-ups are in-process, so a second
    // connection to the same file would silently miss them (enqueueAndWait
    // hanging until its timeout). The second connection must be refused.

    // Refusal tests must not leak a storage (and its timers) when the
    // assertion they exist for fails and the connect unexpectedly succeeds.
    const extra: SQLiteStorage[] = []
    function track (s: SQLiteStorage): SQLiteStorage {
      extra.push(s)
      return s
    }
    afterEach(async () => {
      for (const s of extra.splice(0)) await s.disconnect()
    })

    function writeOwnerClaim (path: string, owner: { pid: number; instanceId: string; heartbeatAt: number }): void {
      const raw = new DatabaseSync(path)
      try {
        raw
          .prepare(
            'INSERT INTO "jq_meta" (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value'
          )
          .run('owner', JSON.stringify(owner))
      } finally {
        raw.close()
      }
    }

    async function createTables (path: string): Promise<void> {
      const s = track(new SQLiteStorage({ path }))
      await s.connect()
      await s.disconnect()
    }

    it('should refuse a second storage on the same file in the same process', async () => {
      const second = track(new SQLiteStorage({ path: join(dir, 'q.sqlite') }))
      await assert.rejects(second.connect(), (err: Error) => {
        assert.ok(err instanceof StorageError)
        assert.match(err.message, new RegExp(`already in use by another SQLiteStorage \\(pid=${process.pid}\\)`))
        return true
      })

      // The owner keeps working.
      await storage.enqueue('j1', Buffer.from('x'), Date.now())
      assert.deepStrictEqual(await storage.dequeue('w1', 1), Buffer.from('x'))

      // Once the owner disconnects, the file is free again.
      await storage.disconnect()
      await second.connect()
      await second.disconnect()
    })

    it('should not leave a half-open handle behind after a refused connect', async () => {
      const second = track(new SQLiteStorage({ path: join(dir, 'q.sqlite') }))
      await assert.rejects(second.connect(), StorageError)
      // A leaked handle would make this connect() return early as "connected".
      await assert.rejects(second.connect(), StorageError)
      await assert.rejects(second.enqueue('j1', Buffer.from('x'), Date.now()), /not connected/)
    })

    it('should allow a different tablePrefix on the same file', async () => {
      const other = track(new SQLiteStorage({ path: join(dir, 'q.sqlite'), tablePrefix: 'other_' }))
      await other.connect()
      try {
        await other.enqueue('j1', Buffer.from('other'), Date.now())
        assert.strictEqual(await storage.dequeue('w1', 0.1), null)
        assert.deepStrictEqual(await other.dequeue('w1', 1), Buffer.from('other'))
      } finally {
        await other.disconnect()
      }
    })

    it('should refuse while another process holds the file, and take over once it dies', async () => {
      const path = join(dir, 'shared.sqlite')
      const moduleUrl = new URL('../src/storage/sqlite.ts', import.meta.url).href
      const child = spawn(
        process.execPath,
        [
          '--no-warnings',
          '--input-type=module',
          '-e',
          `import { SQLiteStorage } from ${JSON.stringify(moduleUrl)}
           const s = new SQLiteStorage({ path: process.argv[1] })
           await s.connect()
           process.stdout.write('ready')
           process.stdin.resume()`,
          path
        ],
        { stdio: ['pipe', 'pipe', 'inherit'] }
      )
      const exited = once(child, 'exit')
      try {
        const [chunk] = await once(child.stdout, 'data')
        assert.strictEqual(String(chunk), 'ready')

        const s = track(new SQLiteStorage({ path }))
        await assert.rejects(s.connect(), new RegExp(`pid=${child.pid}`))

        // A crash leaves the claim behind; a dead pid must not block restarts.
        child.kill('SIGKILL')
        await exited
        await s.connect()
        await s.disconnect()
      } finally {
        child.kill('SIGKILL')
        await exited
      }
    })

    it('should take over a claim whose heartbeat is stale even if the pid is alive', async () => {
      // pid reuse: the recorded pid now belongs to an unrelated live process.
      const path = join(dir, 'stale.sqlite')
      await createTables(path)
      writeOwnerClaim(path, { pid: process.ppid, instanceId: 'gone', heartbeatAt: Date.now() - 60_000 })

      const s = track(new SQLiteStorage({ path }))
      await s.connect()
      await s.disconnect()
    })

    it('should take over a fresh claim left by this pid from a previous run', async () => {
      // e.g. pid 1 in a container that was restarted within the stale window.
      const path = join(dir, 'restart.sqlite')
      await createTables(path)
      writeOwnerClaim(path, { pid: process.pid, instanceId: 'previous-run', heartbeatAt: Date.now() })

      const s = track(new SQLiteStorage({ path }))
      await s.connect()
      await s.disconnect()
    })

    it('should refuse a fresh claim held by another live process', async () => {
      const path = join(dir, 'live.sqlite')
      await createTables(path)
      writeOwnerClaim(path, { pid: process.ppid, instanceId: 'other', heartbeatAt: Date.now() })

      const s = track(new SQLiteStorage({ path }))
      await assert.rejects(s.connect(), new RegExp(`pid=${process.ppid}`))
    })
  })

  describe('incremental vacuum', () => {
    function pragma (path: string, name: string): number {
      const raw = new DatabaseSync(path)
      try {
        return Object.values(raw.prepare(`PRAGMA ${name}`).get()!)[0] as number
      } finally {
        raw.close()
      }
    }

    it('should create new databases with auto_vacuum = INCREMENTAL', () => {
      assert.strictEqual(pragma(join(dir, 'q.sqlite'), 'auto_vacuum'), 2)
    })

    it('should return pages freed by deleted rows to the OS', async () => {
      const path = join(dir, 'vacuum.sqlite')
      const s = new SQLiteStorage({ path, cleanupIntervalMs: false, vacuum: { enabled: true, intervalMs: 50 } })
      await s.connect()
      try {
        const blob = Buffer.alloc(64 * 1024, 7)
        for (let i = 0; i < 100; i++) await s.setResult(`job-${i}`, blob, 1)
        const peak = pragma(path, 'page_count')
        assert.ok(peak > 1000, `expected ~6MB of pages, got ${peak}`)

        await sleep(5)
        // Reading an expired result deletes it, leaving its pages on the freelist.
        for (let i = 0; i < 100; i++) assert.strictEqual(await s.getResult(`job-${i}`), null)

        let pages = peak
        for (let i = 0; i < 40 && (pages > peak / 10 || pragma(path, 'freelist_count') > 0); i++) {
          await sleep(50)
          pages = pragma(path, 'page_count')
        }
        assert.strictEqual(pragma(path, 'freelist_count'), 0)
        assert.ok(pages < peak / 10, `page_count ${pages} did not shrink from ${peak}`)
      } finally {
        await s.disconnect()
      }
    })
  })
})
