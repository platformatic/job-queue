-- Renew leader lock if owned by this reaper
-- KEYS[1] = lock key
-- ARGV[1] = owner ID
-- ARGV[2] = TTL in milliseconds
-- Returns: 1 if renewed, 0 if not owned

if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
