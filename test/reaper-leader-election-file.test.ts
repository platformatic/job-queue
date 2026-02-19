import { describe, it, afterEach } from 'node:test'
import assert from 'node:assert'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as sleep } from 'node:timers/promises'
import { Queue, Reaper, FileStorage, type Job } from '../src/index.ts'
import { once } from './helpers/events.ts'

describe('Reaper Leader Election (FileStorage)', () => {
  const resources: Array<{ stop: () => Promise<void> }> = []
  let basePath: string

  afterEach(async () => {
    // Stop all resources in reverse order
    for (const resource of resources.reverse()) {
      await resource.stop().catch(() => {})
    }
    resources.length = 0
    if (basePath) {
      await rm(basePath, { recursive: true, force: true }).catch(() => {})
    }
  })

  function createStorage (): FileStorage {
    const storage = new FileStorage({ basePath })
    resources.push({ stop: () => storage.disconnect() })
    return storage
  }

  function createReaper (
    storage: FileStorage,
    opts?: {
      visibilityTimeout?: number
      lockTTL?: number
      renewalInterval?: number
      acquireRetryInterval?: number
    }
  ): Reaper<unknown> {
    const reaper = new Reaper({
      storage,
      visibilityTimeout: opts?.visibilityTimeout ?? 100,
      leaderElection: {
        enabled: true,
        lockTTL: opts?.lockTTL ?? 5000,
        renewalInterval: opts?.renewalInterval ?? 1000,
        acquireRetryInterval: opts?.acquireRetryInterval ?? 200
      }
    })
    resources.push({ stop: () => reaper.stop() })
    return reaper
  }

  describe('single reaper', () => {
    it('should acquire leadership on start', async () => {
      basePath = await mkdtemp(join(tmpdir(), 'reaper-leader-file-'))
      const storage = createStorage()
      await storage.connect()

      const reaper = createReaper(storage)
      const leaderPromise = once(reaper, 'leadershipAcquired')
      await reaper.start()
      await leaderPromise

      assert.strictEqual(reaper.isLeader, true)
    })

    it('should release leadership on stop', async () => {
      basePath = await mkdtemp(join(tmpdir(), 'reaper-leader-file-'))
      const storage = createStorage()
      await storage.connect()

      const reaper = createReaper(storage)
      const leaderPromise = once(reaper, 'leadershipAcquired')
      await reaper.start()
      await leaderPromise

      assert.strictEqual(reaper.isLeader, true)

      await reaper.stop()
      assert.strictEqual(reaper.isLeader, false)
    })

    it('should detect and recover stalled jobs', async () => {
      basePath = await mkdtemp(join(tmpdir(), 'reaper-leader-file-'))
      const storage = createStorage()
      await storage.connect()

      const queue = new Queue<{ value: number }, { result: number }>({
        storage,
        concurrency: 2,
        visibilityTimeout: 500
      })
      resources.push({ stop: () => queue.stop() })

      const reaper = createReaper(storage)

      let processCount = 0
      let abortFirstHandler: (() => void) | undefined

      queue.execute(async (job: Job<{ value: number }>) => {
        processCount++
        if (processCount === 1) {
          // First attempt: stall
          await new Promise<void>(resolve => {
            abortFirstHandler = resolve
            job.signal.addEventListener('abort', () => resolve())
          })
          throw new Error('Aborted for cleanup')
        }
        return { result: job.payload.value * 2 }
      })

      try {
        await queue.start()
        const leaderPromise = once(reaper, 'leadershipAcquired')
        await reaper.start()
        await leaderPromise

        const resultPromise = queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })

        await once(reaper, 'stalled')

        const result = await resultPromise
        assert.deepStrictEqual(result, { result: 42 })
        assert.strictEqual(processCount, 2)
      } finally {
        abortFirstHandler?.()
      }
    })
  })

  describe('multiple reapers competing', () => {
    it('only one reaper should become leader', async () => {
      basePath = await mkdtemp(join(tmpdir(), 'reaper-leader-file-'))
      const storage1 = createStorage()
      const storage2 = createStorage()
      await storage1.connect()
      await storage2.connect()

      const reaper1 = createReaper(storage1)
      const reaper2 = createReaper(storage2)

      // Start first reaper - should become leader
      const leader1Promise = once(reaper1, 'leadershipAcquired')
      await reaper1.start()
      await leader1Promise

      assert.strictEqual(reaper1.isLeader, true)

      // Start second reaper - should NOT become leader
      await reaper2.start()
      await sleep(500)

      assert.strictEqual(reaper1.isLeader, true)
      assert.strictEqual(reaper2.isLeader, false)
    })

    it('follower should take over when leader stops', async () => {
      basePath = await mkdtemp(join(tmpdir(), 'reaper-leader-file-'))
      const storage1 = createStorage()
      const storage2 = createStorage()
      await storage1.connect()
      await storage2.connect()

      const reaper1 = createReaper(storage1)
      const reaper2 = createReaper(storage2)

      // Start reaper1 as leader
      const leader1Promise = once(reaper1, 'leadershipAcquired')
      await reaper1.start()
      await leader1Promise

      // Start reaper2 as follower
      await reaper2.start()
      await sleep(300)

      assert.strictEqual(reaper1.isLeader, true)
      assert.strictEqual(reaper2.isLeader, false)

      // Set up listener for reaper2 acquiring leadership
      const reaper2LeaderPromise = once(reaper2, 'leadershipAcquired')

      // Stop reaper1 - releases lock
      await reaper1.stop()

      // Reaper2 should become leader
      await reaper2LeaderPromise

      assert.strictEqual(reaper1.isLeader, false)
      assert.strictEqual(reaper2.isLeader, true)
    })

    it('follower should take over when leader lock expires', async () => {
      basePath = await mkdtemp(join(tmpdir(), 'reaper-leader-file-'))
      const storage1 = createStorage()
      const storage2 = createStorage()
      await storage1.connect()
      await storage2.connect()

      const reaper1 = createReaper(storage1, {
        lockTTL: 500,
        renewalInterval: 100,
        acquireRetryInterval: 100
      })

      const reaper2 = createReaper(storage2, {
        lockTTL: 500,
        renewalInterval: 100,
        acquireRetryInterval: 100
      })

      // Start reaper1 as leader
      const leader1Promise = once(reaper1, 'leadershipAcquired')
      await reaper1.start()
      await leader1Promise

      // Start reaper2 as follower
      await reaper2.start()
      await sleep(200)

      assert.strictEqual(reaper1.isLeader, true)
      assert.strictEqual(reaper2.isLeader, false)

      const reaper2LeaderPromise = once(reaper2, 'leadershipAcquired')

      // Stop reaper1 - this releases the lock, reaper2 can acquire
      await reaper1.stop()

      await reaper2LeaderPromise

      assert.strictEqual(reaper2.isLeader, true)
    })
  })

  describe('lock renewal', () => {
    it('should renew lock periodically and remain leader', async () => {
      basePath = await mkdtemp(join(tmpdir(), 'reaper-leader-file-'))
      const storage = createStorage()
      await storage.connect()

      const reaper = createReaper(storage, {
        lockTTL: 500,
        renewalInterval: 100,
        acquireRetryInterval: 100
      })

      const leaderPromise = once(reaper, 'leadershipAcquired')
      await reaper.start()
      await leaderPromise

      // Wait longer than TTL - should still be leader due to renewal
      await sleep(800)

      assert.strictEqual(reaper.isLeader, true)
    })
  })
})
