-- Release leader lock if owned by this reaper
-- KEYS[1] = lock key
-- ARGV[1] = owner ID
-- Returns: 1 if released, 0 if not owned

if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
