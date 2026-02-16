-- Atomic job retry
-- KEYS[1] = jobs hash key
-- KEYS[2] = queue key
-- KEYS[3] = processing queue key (worker-specific)
-- ARGV[1] = job id
-- ARGV[2] = message (updated with new attempt count)
-- ARGV[3] = old message (to remove from processing queue)
-- ARGV[4] = state (failing:timestamp:attempts)
-- Returns: 1 on success

-- Set state to failing
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])

-- Remove from processing queue
redis.call('LREM', KEYS[3], 1, ARGV[3])

-- Add back to main queue (at the front for retry)
redis.call('LPUSH', KEYS[2], ARGV[2])

-- Publish event
redis.call('PUBLISH', 'job:events', ARGV[1] .. ':failing')

return 1
