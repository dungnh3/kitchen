-- GCRA (generic cell rate algorithm) for kitchen/ratelimit.
-- Algorithm after go-redis/redis_rate (BSD-2-Clause); rewritten for kitchen.
--
-- KEYS[1]  bucket key (holds the theoretical arrival time, TAT)
-- ARGV[1]  burst
-- ARGV[2]  rate   (requests per period)
-- ARGV[3]  period (seconds, may be fractional)
-- ARGV[4]  cost   (requests to take, usually 1)
--
-- Returns {allowed (0|1), remaining, retry_after_seconds, reset_after_seconds};
-- the last two are strings to keep their fractions.

local key = KEYS[1]
local burst = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local period = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local interval = period / rate          -- time per request
local increment = interval * cost
local tolerance = interval * burst      -- how far TAT may run ahead of now

-- Server time, shifted to a 2020 epoch to keep float precision.
local t = redis.call("TIME")
local now = (tonumber(t[1]) - 1577836800) + tonumber(t[2]) / 1000000

local tat = tonumber(redis.call("GET", key))
if tat == nil or tat < now then
  tat = now
end

local new_tat = tat + increment
local allow_at = new_tat - tolerance

if now < allow_at then
  -- Denied: nothing is written.
  return {0, 0, tostring(allow_at - now), tostring(tat - now)}
end

local reset_after = new_tat - now
redis.call("SET", key, tostring(new_tat), "PX", math.ceil(reset_after * 1000))
local remaining = math.floor((now - allow_at) / interval)
return {1, remaining, "0", tostring(reset_after)}
