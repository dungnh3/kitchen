-- Append one entry unless the stream has ended; optionally mirror it to an
-- outbox shard that shares the stream key's hash slot. One round trip, atomic.
--
-- KEYS[1] = stream key        KEYS[2] = outbox shard key (optional)
-- ARGV[1] = type              ARGV[2] = data
-- ARGV[3] = maxlen (0 = none) ARGV[4] = ttl ms (0 = none)
-- ARGV[5] = stream id         ARGV[6] = outbox maxlen (0 = none)
-- ARGV[7] = outbox minid ('' = none)
local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
if #last > 0 then
  local f = last[1][2]
  for i = 1, #f, 2 do
    if f[i] == 'type' and f[i + 1] == '__sse_end__' then
      return redis.error_reply('SSEENDED stream has ended')
    end
  end
end

local id
local maxlen = tonumber(ARGV[3])
if maxlen > 0 then
  id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', maxlen, '*', 'type', ARGV[1], 'data', ARGV[2])
else
  id = redis.call('XADD', KEYS[1], '*', 'type', ARGV[1], 'data', ARGV[2])
end
local ttl = tonumber(ARGV[4])
if ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
end

if #KEYS > 1 then
  local omax = tonumber(ARGV[6])
  local minid = ARGV[7]
  if minid ~= '' then
    redis.call('XADD', KEYS[2], 'MINID', '~', minid, '*', 'type', ARGV[1], 'data', ARGV[2], 'stream', ARGV[5])
    if omax > 0 then
      redis.call('XTRIM', KEYS[2], 'MAXLEN', '~', omax)
    end
  elseif omax > 0 then
    redis.call('XADD', KEYS[2], 'MAXLEN', '~', omax, '*', 'type', ARGV[1], 'data', ARGV[2], 'stream', ARGV[5])
  else
    redis.call('XADD', KEYS[2], '*', 'type', ARGV[1], 'data', ARGV[2], 'stream', ARGV[5])
  end
end
return id
