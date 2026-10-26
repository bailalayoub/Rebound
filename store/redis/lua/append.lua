-- append.lua: atomically store one event under the next seq (spec §6, ADR 0001, 0004, 0005).
-- KEYS[1] meta hash, KEYS[2] log stream
-- ARGV[1] kind, ARGV[2] data, ARGV[3] at_ms, ARGV[4] force ('1' or '0'),
-- ARGV[5] terminal state ('' | 'done' | 'error'), ARGV[6] error code,
-- ARGV[7] expected seq ('0': assign the next one)
-- Returns the seq, or -1 not found, -2 closed (terminal), -3 cancelled, -4 too many events,
-- -5 conflict with the expected seq.
local state = redis.call('HGET', KEYS[1], 'state')
if not state then
  return -1
end
local last = tonumber(redis.call('HGET', KEYS[1], 'seq'))
local expected = tonumber(ARGV[7])
if expected > 0 and expected <= last then
  -- Replay of a write whose reply was lost: the same event at that seq is success, unchanged.
  local entry = redis.call('XRANGE', KEYS[2], expected .. '-0', expected .. '-0')
  if #entry == 1 then
    local fields, kind, data = entry[1][2], nil, nil
    for i = 1, #fields, 2 do
      if fields[i] == 'kind' then
        kind = fields[i + 1]
      elseif fields[i] == 'data' then
        data = fields[i + 1]
      end
    end
    if kind == ARGV[1] and data == ARGV[2] then
      return expected
    end
  end
  if state ~= 'running' then
    return -2
  end
  return -5
end
if state ~= 'running' then
  return -2
end
if expected > last + 1 then
  return -5
end
if ARGV[4] ~= '1' and redis.call('HGET', KEYS[1], 'cancelled') == '1' then
  return -3
end
local max_events = tonumber(redis.call('HGET', KEYS[1], 'max_events'))
if ARGV[5] == '' and max_events and max_events > 0 and last >= max_events then
  return -4 -- a hard cap: running streams are never trimmed
end
local seq = redis.call('HINCRBY', KEYS[1], 'seq', 1)
redis.call('XADD', KEYS[2], seq .. '-0', 'kind', ARGV[1], 'data', ARGV[2])
redis.call('HSET', KEYS[1], 'last_activity_ms', ARGV[3])
local ttl
if ARGV[5] ~= '' then
  redis.call('HSET', KEYS[1], 'state', ARGV[5], 'error_code', ARGV[6])
  ttl = tonumber(redis.call('HGET', KEYS[1], 'retention_ms'))
else
  ttl = tonumber(redis.call('HGET', KEYS[1], 'idle_ttl_ms'))
end
if ttl and ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
  redis.call('PEXPIRE', KEYS[2], ttl)
else
  redis.call('PERSIST', KEYS[1])
  redis.call('PERSIST', KEYS[2])
end
return seq
