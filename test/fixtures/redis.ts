import { RedisStorage } from '../../src/storage/redis.ts'

/**
 * Create a RedisStorage instance for testing
 */
export function createRedisStorage (): RedisStorage {
  const prefix = `test:${Date.now()}:${Math.random().toString(36).slice(2)}:`
  return new RedisStorage({
    url: process.env.REDIS_URL ?? 'redis://localhost:6379',
    keyPrefix: prefix
  })
}
