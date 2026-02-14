-- Atomic enqueue with deduplication
-- KEYS[1] = jobs hash key
-- KEYS[2] = queue key
-- ARGV[1] = job id
-- ARGV[2] = message (serialized)
-- ARGV[3] = state (queued:timestamp)
-- Returns: nil if enqueued, existing state if duplicate

local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
  return existing
end

redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('RPUSH', KEYS[2], ARGV[2])
redis.call('PUBLISH', 'job:events', ARGV[1] .. ':queued')

return nil
