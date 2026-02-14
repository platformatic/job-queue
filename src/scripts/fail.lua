-- Atomic job failure
-- KEYS[1] = jobs hash key
-- KEYS[2] = errors hash key
-- KEYS[3] = processing queue key (worker-specific)
-- ARGV[1] = job id
-- ARGV[2] = message (to remove from processing queue)
-- ARGV[3] = state (failed:timestamp)
-- ARGV[4] = error (serialized)
-- ARGV[5] = error TTL in ms
-- Returns: 1 on success

-- Set state to failed
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])

-- Store error with TTL
redis.call('HSET', KEYS[2], ARGV[1], ARGV[4])
redis.call('PEXPIRE', KEYS[2], ARGV[5])

-- Remove from processing queue
redis.call('LREM', KEYS[3], 1, ARGV[2])

-- Publish notifications
redis.call('PUBLISH', 'job:notify:' .. ARGV[1], 'failed')
redis.call('PUBLISH', 'job:events', ARGV[1] .. ':failed')

return 1
