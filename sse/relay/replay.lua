-- Move one dead letter back to its shard. Returns the new entry ID or false.
-- KEYS[1] = shard, KEYS[2] = dlq; ARGV[1] = dlq entry id
local e = redis.call('XRANGE', KEYS[2], ARGV[1], ARGV[1])
if #e == 0 then
  return false
end
local f = e[1][2]
local v = {}
for i = 1, #f, 2 do
  v[f[i]] = f[i + 1]
end
local id = redis.call('XADD', KEYS[1], '*', 'type', v['type'] or '', 'data', v['data'] or '', 'stream', v['stream'] or '')
redis.call('XDEL', KEYS[2], ARGV[1])
return id
