-- Atomic enqueue with deduplication
-- KEYS[1] = jobs hash key
-- KEYS[2] = queue key
-- ARGV[1] = job id
-- ARGV[2] = message (serialized)
-- ARGV[3] = state (queued:timestamp)
-- Returns: nil if enqueued, existing state if duplicate

local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
  -- Check if the job has a dedup expiry and if it has passed
  local expiry = redis.call('HGET', KEYS[1], ARGV[1] .. ':exp')
  if expiry then
    local now = redis.call('TIME')
    local nowMs = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
    if nowMs >= tonumber(expiry) then
      -- Expired: remove both fields and fall through to enqueue as new
      redis.call('HDEL', KEYS[1], ARGV[1], ARGV[1] .. ':exp')
    else
      return existing
    end
  else
    return existing
  end
end

redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('RPUSH', KEYS[2], ARGV[2])
redis.call('PUBLISH', 'job:events', ARGV[1] .. ':queued')

return nil
