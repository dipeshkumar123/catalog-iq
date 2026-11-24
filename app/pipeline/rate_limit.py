"""Optional cross-process token bucket backed by Redis Lua atomicity."""
import asyncio
import time
from typing import Optional
from app.core.config import settings

_client = None
_SCRIPT = """
local function calculate(key, capacity, amount, now)
  if capacity <= 0 then return 0, 0, 0 end
  local state = redis.call('HMGET', key, 'tokens', 'updated')
  local tokens = tonumber(state[1]) or capacity
  local updated = tonumber(state[2]) or now
  local rate = capacity / 60000
  tokens = math.min(capacity, tokens + math.max(0, now - updated) * rate)
  local delay = 0
  if tokens < amount then delay = math.ceil((amount - tokens) / rate) end
  return tokens, delay, capacity > 0 and 1 or 0
end
local now = tonumber(ARGV[5])
local ta, a, ea = calculate(KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[3]), now)
local tb, b, eb = calculate(KEYS[2], tonumber(ARGV[2]), tonumber(ARGV[4]), now)
local delay = math.max(a,b)
if delay == 0 then
  if ea == 1 then redis.call('HMSET', KEYS[1], 'tokens', ta-tonumber(ARGV[3]), 'updated', now); redis.call('PEXPIRE', KEYS[1], 120000) end
  if eb == 1 then redis.call('HMSET', KEYS[2], 'tokens', tb-tonumber(ARGV[4]), 'updated', now); redis.call('PEXPIRE', KEYS[2], 120000) end
end
return delay
"""


async def acquire_llm_capacity(estimated_tokens: int = 1) -> None:
    """Wait for configured RPM and TPM capacity; no-op in local single-process mode."""
    rpm, tpm = settings.LLM_RATE_LIMIT_RPM, settings.LLM_RATE_LIMIT_TPM
    if not settings.REDIS_URL or (rpm <= 0 and tpm <= 0):
        return
    global _client
    if _client is None:
        try:
            from redis.asyncio import Redis
        except ImportError as exc:
            raise RuntimeError("REDIS_URL rate limiting requires the redis package") from exc
        _client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    amount = min(max(1, estimated_tokens), max(1, tpm))
    while True:
        delay_ms = await _client.eval(_SCRIPT, 2, "catalogiq:llm:rpm", "catalogiq:llm:tpm",
                                      rpm, tpm, 1, amount, int(time.time() * 1000))
        if delay_ms <= 0:
            return
        await asyncio.sleep(min(delay_ms / 1000, 60))


async def close_rate_limiter() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
