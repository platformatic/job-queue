import { RedisStorage } from '../../src/storage/redis.ts'

/**
 * Check if Redis tests should run
 */
export function shouldRunRedisTests (): boolean {
  return !!process.env.REDIS_URL
}

/**
 * Create a RedisStorage instance for testing
 */
export function createRedisStorage (): RedisStorage {
  const prefix = `test:${Date.now()}:${Math.random().toString(36).slice(2)}:`
  return new RedisStorage({
    url: process.env.REDIS_URL,
    keyPrefix: prefix
  })
}
