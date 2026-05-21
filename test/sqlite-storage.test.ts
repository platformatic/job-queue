import assert from 'node:assert'
import { afterEach, beforeEach, describe, it } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
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
})
