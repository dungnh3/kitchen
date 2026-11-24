-- GCRA (generic cell rate algorithm) for kitchen/ratelimit.
-- Algorithm after go-redis/redis_rate (BSD-2-Clause); rewritten for kitchen.
--
-- All times are integer microseconds, so every value stays exact in a Lua
-- number (float64) and the decisions match kitchen/ratelimit.LocalStore bit
-- for bit. Never format a time with tostring(): Redis's Lua 5.1 keeps 14
-- significant digits. Never compute in float seconds: the subtraction of
-- two large timestamps loses digits and makes floor() off by one.
--
-- KEYS[1]  bucket key (holds the theoretical arrival time, TAT, in us)
-- ARGV[1]  emission interval (us per request, integer >= 1)
-- ARGV[2]  burst tolerance (interval * burst, us)
--
-- Returns {allowed (0|1), remaining, retry_after_us, reset_after_us}.

-- Redis 5/6 with lua-replicate-commands off: replicate effects, so TIME
-- may precede a write. A no-op on Redis 7.
if redis.replicate_commands then
  redis.replicate_commands()
end

local key = KEYS[1]
local interval = tonumber(ARGV[1])
local tolerance = tonumber(ARGV[2])

-- Server time (one clock for every instance), shifted to a 2020 epoch.
local t = redis.call("TIME")
local now = (tonumber(t[1]) - 1577836800) * 1000000 + tonumber(t[2])

local tat = tonumber(redis.call("GET", key))
if tat == nil or tat < now then
  tat = now
end

local new_tat = tat + interval
local allow_at = new_tat - tolerance

if now < allow_at then
  -- Denied: nothing is written.
  return {0, 0, allow_at - now, tat - now}
end

redis.call("SET", key, string.format("%d", new_tat), "PX", math.ceil((new_tat - now) / 1000))
return {1, math.floor((now - allow_at) / interval), 0, new_tat - now}
