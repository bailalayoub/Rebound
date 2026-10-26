-- touch.lua: record producer liveness of a running stream and refresh its idle TTL.
-- KEYS[1] meta hash, KEYS[2] log stream; ARGV[1] at_ms
-- Returns -1 not found, 0 terminal (unchanged: Touch must not extend retention), 1 touched.
local state = redis.call('HGET', KEYS[1], 'state')
if not state then
  return -1
end
if state ~= 'running' then
  return 0
end
redis.call('HSET', KEYS[1], 'last_activity_ms', ARGV[1])
local ttl = tonumber(redis.call('HGET', KEYS[1], 'idle_ttl_ms'))
if ttl and ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
  if redis.call('EXISTS', KEYS[2]) == 1 then
    redis.call('PEXPIRE', KEYS[2], ttl)
  end
end
return 1
