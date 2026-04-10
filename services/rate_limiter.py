"""
Async token-bucket rate limiter for per-platform request throttling.

Each platform gets its own bucket. A call to `acquire(platform)` waits
until a token is available, then consumes one.  This enforces a maximum
request rate without ever dropping requests.

Usage:
    from services.rate_limiter import rate_limiter

    async def scrape():
        await rate_limiter.acquire("taobao")
        # ... make the request
"""

import asyncio
import time
import logging

logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Token-bucket for a single platform.

    Tokens refill at `rate` per second up to `capacity`.
    Each `acquire()` consumes one token; if the bucket is empty the
    coroutine sleeps until a token becomes available.
    """

    def __init__(self, rate: float, capacity: float):
        self._rate = rate          # tokens per second
        self._capacity = capacity  # max tokens (burst ceiling)
        self._tokens = capacity    # start full so first requests are instant
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Calculate how long until next token is available
                wait = (1.0 - self._tokens) / self._rate

            await asyncio.sleep(wait)


# ── Default per-platform limits ────────────────────────────────────────────
#
# Taobao / Tmall: ~12 req/min measured safe ceiling; burst up to 3 requests
#   immediately (covers search-page clicks which arrive in quick succession).
# 1688: slightly more relaxed — B2B platform with different bot-detection.
# default: conservative fallback for any unknown platform key.
#
_DEFAULT_CONFIGS: dict[str, dict] = {
    "taobao":  {"rate": 5 / 60, "capacity": 2},    # 5 req/min, burst 2 — matches real user browsing speed
    "tmall":   {"rate": 5 / 60, "capacity": 2},
    "1688":    {"rate": 20 / 60, "capacity": 5},   # 20 req/min, burst 5
    "alibaba": {"rate": 15 / 60, "capacity": 4},   # 15 req/min, burst 4 — international B2B
    "default": {"rate": 10 / 60, "capacity": 2},
}


class PlatformRateLimiter:
    """
    Manages one TokenBucket per platform.

    Platforms not in the default config fall back to the "default" bucket.
    """

    def __init__(self, configs: dict[str, dict] | None = None):
        cfg = configs or _DEFAULT_CONFIGS
        self._buckets: dict[str, TokenBucket] = {
            name: TokenBucket(**limits)
            for name, limits in cfg.items()
        }

    def _bucket(self, platform: str) -> TokenBucket:
        return self._buckets.get(platform) or self._buckets["default"]

    async def acquire(self, platform: str) -> None:
        """Wait until a token is available for *platform*, then consume it."""
        bucket = self._bucket(platform)
        before = time.monotonic()
        await bucket.acquire()
        waited = time.monotonic() - before
        if waited > 0.05:
            logger.debug("rate_limiter: waited %.2fs for %s token", waited, platform)


# Singleton — import and use directly
rate_limiter = PlatformRateLimiter()
