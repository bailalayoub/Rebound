-- cancel.lua: set the cancelled flag of an existing stream (idempotent).
-- KEYS[1] meta hash. Returns -1 not found, 1 otherwise.
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
redis.call('HSET', KEYS[1], 'cancelled', 1)
return 1
