-- Move one entry to the dead-letter stream and acknowledge it, atomically.
-- KEYS[1] = shard, KEYS[2] = dlq (same hash slot)
-- ARGV: group, entry id, type, data, stream, reason, error
redis.call('XADD', KEYS[2], '*', 'type', ARGV[3], 'data', ARGV[4], 'stream', ARGV[5],
  'reason', ARGV[6], 'error', ARGV[7], 'source_id', ARGV[2])
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
