-- Atomic job cancellation
-- KEYS[1] = jobs hash key
-- ARGV[1] = job id
-- Returns: 1 if cancelled, 0 if not found

local deleted = redis.call('HDEL', KEYS[1], ARGV[1])
if deleted == 1 then
  redis.call('PUBLISH', 'job:events', ARGV[1] .. ':cancelled')
  return 1
end

return 0
