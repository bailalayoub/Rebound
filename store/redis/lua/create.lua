-- create.lua: register a running, empty stream unless it exists.
-- KEYS[1] meta hash, KEYS[2] log stream
-- ARGV[1] created_ms, ARGV[2] idle_ttl_ms, ARGV[3] retention_ms, ARGV[4] max_events, ARGV[5] attrs (JSON)
-- Returns 1 when created, 0 when the stream exists.
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
-- A log left behind by an expired meta would make XADD reject seq 1: start clean.
redis.call('DEL', KEYS[2])
redis.call('HSET', KEYS[1],
  'state', 'running', 'seq', 0, 'cancelled', 0, 'error_code', '',
  'created_ms', ARGV[1], 'last_activity_ms', ARGV[1],
  'idle_ttl_ms', ARGV[2], 'retention_ms', ARGV[3], 'max_events', ARGV[4], 'attrs', ARGV[5])
if tonumber(ARGV[2]) > 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 1
