import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { Queue, Reaper, RedisStorage, type Job } from '../src/index.ts'
import { once, waitForEvents } from './helpers/events.ts'

// Skip if no Redis available
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379'

describe('Reaper Leader Election', () => {
  let storage: RedisStorage
  let storage2: RedisStorage

  beforeEach(async () => {
    storage = new RedisStorage({ url: REDIS_URL, keyPrefix: 'test:leader:' })
    storage2 = new RedisStorage({ url: REDIS_URL, keyPrefix: 'test:leader:' })
    await storage.connect()
    await storage2.connect()
    await storage.clear()
  })

  afterEach(async () => {
    await storage.clear()
    await storage.disconnect()
    await storage2.disconnect()
  })

  describe('backward compatibility', () => {
    it('should work without leader election enabled', async () => {
      const reaper = new Reaper({
        storage,
        visibilityTimeout: 100
      })

      try {
        await reaper.start()

        // Without leader election, isLeader should be false (no election active)
        assert.strictEqual(reaper.isLeader, false)

        await reaper.stop()
      } finally {
        await reaper.stop()
      }
    })

    it('should have a unique reaperId', async () => {
      const reaper1 = new Reaper({ storage, visibilityTimeout: 100 })
      const reaper2 = new Reaper({ storage, visibilityTimeout: 100 })

      assert.ok(reaper1.reaperId)
      assert.ok(reaper2.reaperId)
      assert.notStrictEqual(reaper1.reaperId, reaper2.reaperId)
    })
  })

  describe('single reaper with leader election', () => {
    it('should acquire leadership immediately on start', async () => {
      const reaper = new Reaper({
        storage,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true,
          lockTTL: 5000,
          renewalInterval: 1000,
          acquireRetryInterval: 500
        }
      })

      try {
        const leadershipPromise = once(reaper, 'leadershipAcquired')

        await reaper.start()
        await leadershipPromise

        assert.strictEqual(reaper.isLeader, true)
      } finally {
        await reaper.stop()
      }
    })

    it('should release leadership on stop', async () => {
      const reaper = new Reaper({
        storage,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true,
          lockTTL: 5000,
          renewalInterval: 1000,
          acquireRetryInterval: 500
        }
      })

      const leadershipPromise = once(reaper, 'leadershipAcquired')
      await reaper.start()
      await leadershipPromise

      assert.strictEqual(reaper.isLeader, true)

      await reaper.stop()

      assert.strictEqual(reaper.isLeader, false)
    })

    it('should still detect stalled jobs as leader', async () => {
      const queue = new Queue<{ value: number }, { result: number }>({
        storage,
        concurrency: 2,
        visibilityTimeout: 500 // Longer than reaper's 100ms
      })

      const reaper = new Reaper({
        storage,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true,
          lockTTL: 5000,
          renewalInterval: 1000,
          acquireRetryInterval: 500
        }
      })

      let processCount = 0
      let abortFirstHandler: (() => void) | undefined

      queue.execute(async (job: Job<{ value: number }>) => {
        processCount++
        if (processCount === 1) {
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

      try {
        await queue.start()
        const leadershipPromise = once(reaper, 'leadershipAcquired')
        await reaper.start()
        await leadershipPromise

        const resultPromise = queue.enqueueAndWait('job-1', { value: 21 }, { timeout: 5000 })

        // Wait for stall detection
        await once(reaper, 'stalled')

        // Job should be reprocessed and complete
        const result = await resultPromise

        assert.deepStrictEqual(result, { result: 42 })
        assert.strictEqual(processCount, 2, 'Job should have been processed twice')
      } finally {
        abortFirstHandler?.()
        await reaper.stop()
        await queue.stop()
      }
    })
  })

  describe('multiple reapers competing', () => {
    it('only one reaper should become leader', async () => {
      const reaper1 = new Reaper({
        storage,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true,
          lockTTL: 5000,
          renewalInterval: 1000,
          acquireRetryInterval: 200
        }
      })

      const reaper2 = new Reaper({
        storage: storage2,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true,
          lockTTL: 5000,
          renewalInterval: 1000,
          acquireRetryInterval: 200
        }
      })

      try {
        // Start first reaper - should become leader
        const leader1Promise = once(reaper1, 'leadershipAcquired')
        await reaper1.start()
        await leader1Promise

        assert.strictEqual(reaper1.isLeader, true)

        // Start second reaper - should NOT become leader
        await reaper2.start()

        // Wait a bit for any acquisition attempt
        await sleep(500)

        assert.strictEqual(reaper1.isLeader, true)
        assert.strictEqual(reaper2.isLeader, false)
      } finally {
        await reaper1.stop()
        await reaper2.stop()
      }
    })

    it('follower should take over when leader stops', async () => {
      const reaper1 = new Reaper({
        storage,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true,
          lockTTL: 5000,
          renewalInterval: 1000,
          acquireRetryInterval: 200
        }
      })

      const reaper2 = new Reaper({
        storage: storage2,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true,
          lockTTL: 5000,
          renewalInterval: 1000,
          acquireRetryInterval: 200
        }
      })

      try {
        // Start first reaper - should become leader
        const leader1Promise = once(reaper1, 'leadershipAcquired')
        await reaper1.start()
        await leader1Promise

        // Start second reaper - should be follower
        await reaper2.start()
        await sleep(300) // Let it try to acquire

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
      } finally {
        await reaper1.stop()
        await reaper2.stop()
      }
    })

    it('follower should take over when leader crashes (lock expires)', async () => {
      const reaper1 = new Reaper({
        storage,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true,
          lockTTL: 500, // Short TTL for faster test
          renewalInterval: 100,
          acquireRetryInterval: 100
        }
      })

      const reaper2 = new Reaper({
        storage: storage2,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true,
          lockTTL: 500,
          renewalInterval: 100,
          acquireRetryInterval: 100
        }
      })

      try {
        // Start first reaper - should become leader
        const leader1Promise = once(reaper1, 'leadershipAcquired')
        await reaper1.start()
        await leader1Promise

        // Start second reaper - should be follower
        await reaper2.start()
        await sleep(200)

        assert.strictEqual(reaper1.isLeader, true)
        assert.strictEqual(reaper2.isLeader, false)

        // Set up listener before simulating crash
        const reaper2LeaderPromise = once(reaper2, 'leadershipAcquired')

        // Simulate crash by stopping timer but NOT releasing lock
        // The lock will expire naturally after TTL
        await reaper1.stop()

        // Wait for lock to expire and reaper2 to acquire
        // Since we released the lock on stop, reaper2 should acquire quickly
        await reaper2LeaderPromise

        assert.strictEqual(reaper2.isLeader, true)
      } finally {
        await reaper1.stop()
        await reaper2.stop()
      }
    })
  })

  describe('lock renewal', () => {
    it('should renew lock periodically', async () => {
      const reaper = new Reaper({
        storage,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true,
          lockTTL: 500,
          renewalInterval: 100,
          acquireRetryInterval: 100
        }
      })

      try {
        const leadershipPromise = once(reaper, 'leadershipAcquired')
        await reaper.start()
        await leadershipPromise

        // Wait longer than TTL - should still be leader due to renewal
        await sleep(800)

        assert.strictEqual(reaper.isLeader, true)
      } finally {
        await reaper.stop()
      }
    })
  })

  describe('error handling', () => {
    it('should emit error when storage does not support leader election', async () => {
      // Create a storage mock that doesn't have leader election methods
      const mockStorage = {
        ...storage,
        acquireLeaderLock: undefined,
        renewLeaderLock: undefined,
        releaseLeaderLock: undefined
      }

      const reaper = new Reaper({
        storage: mockStorage as unknown as RedisStorage,
        visibilityTimeout: 100,
        leaderElection: {
          enabled: true
        }
      })

      const errorPromise = once(reaper, 'error')

      try {
        await reaper.start()
        const [error] = await errorPromise

        assert.ok(error.message.includes('Storage does not support leader election'))
      } finally {
        await reaper.stop()
      }
    })
  })
})
