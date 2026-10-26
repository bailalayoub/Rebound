-- status.lua: the meta hash and the first retained id, consistent with the last append.
-- KEYS[1] meta hash, KEYS[2] log stream
-- Returns -1 not found, or {meta fields (HGETALL), first id or ''}.
local meta = redis.call('HGETALL', KEYS[1])
if #meta == 0 then
  return -1
end
local first = redis.call('XRANGE', KEYS[2], '-', '+', 'COUNT', 1)
local first_id = ''
if #first > 0 then
  first_id = first[1][1]
end
return {meta, first_id}
