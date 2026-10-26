-- read.lua: events with seq > after, checked against trimming, in one atomic step.
-- KEYS[1] meta hash, KEYS[2] log stream
-- ARGV[1] first id to return ('<after+1>-0'), ARGV[2] count, ARGV[3] after
-- Returns -1 not found, {-2, first_seq} gap, or {state, last_seq, entries}.
local state = redis.call('HGET', KEYS[1], 'state')
if not state then
  return -1
end
local first = redis.call('XRANGE', KEYS[2], '-', '+', 'COUNT', 1)
if #first > 0 then
  local first_seq = tonumber(string.match(first[1][1], '^(%d+)'))
  if tonumber(ARGV[3]) + 1 < first_seq then
    return {-2, first_seq}
  end
end
local last_seq = tonumber(redis.call('HGET', KEYS[1], 'seq'))
local entries = redis.call('XRANGE', KEYS[2], ARGV[1], '+', 'COUNT', ARGV[2])
return {state, last_seq, entries}
