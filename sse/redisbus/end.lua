-- Append the end-of-stream sentinel unless it is already the last entry, then
-- set the done TTL. Idempotent. Returns the sentinel's entry ID.
--
-- KEYS[1] = stream key
-- ARGV[1] = ttl ms (0 = none)  ARGV[2] = maxlen (0 = none)
local id
local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
if #last > 0 then
  local f = last[1][2]
  for i = 1, #f, 2 do
    if f[i] == 'type' and f[i + 1] == '__sse_end__' then
      id = last[1][1]
    end
  end
end
if not id then
  local maxlen = tonumber(ARGV[2])
  if maxlen > 0 then
    id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', maxlen, '*', 'type', '__sse_end__', 'data', '')
  else
    id = redis.call('XADD', KEYS[1], '*', 'type', '__sse_end__', 'data', '')
  end
end
local ttl = tonumber(ARGV[1])
if ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
end
return id
