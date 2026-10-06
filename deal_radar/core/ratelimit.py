"""Async token buckets, a per-host limiter registry and a circuit breaker.

Politeness is a latency feature: an upstream that starts returning 429/403 is
far slower than one polled at a sustainable rate. Every outbound host gets a token
bucket (configurable per host suffix), and server back-off hints are applied to the
whole bucket via :meth:`TokenBucket.penalize` so that *all* coroutines sharing the
host pause, not only the one that received the 429.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass


class TokenBucket:
    """FIFO-fair async token bucket.

    ``rate`` tokens are added per second up to ``capacity``. ``acquire`` waits until
    enough tokens exist; waiters are served in arrival order via an internal lock.
    """

    def __init__(
        self,
        rate: float,
        capacity: float | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], "asyncio.Future[None] | object"] = asyncio.sleep,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else max(1.0, rate))
        if self.capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._tokens = self.capacity
        self._clock = clock
        self._sleep = sleep
        self._updated = clock()
        self._blocked_until = 0.0
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._updated = now

    @property
    def tokens(self) -> float:
        self._refill()
        return self._tokens

    def try_acquire(self, tokens: float = 1.0) -> bool:
        self._refill()
        if self._clock() < self._blocked_until:
            return False
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: float = 1.0) -> float:
        """Wait for ``tokens``; returns the number of seconds spent waiting."""
        if tokens > self.capacity:
            raise ValueError("cannot acquire more tokens than the bucket capacity")
        waited = 0.0
        async with self._lock:
            while True:
                self._refill()
                now = self._clock()
                if now < self._blocked_until:
                    delay = self._blocked_until - now
                elif self._tokens >= tokens:
                    self._tokens -= tokens
                    return waited
                else:
                    delay = (tokens - self._tokens) / self.rate
                waited += delay
                await self._sleep(delay)

    def penalize(self, seconds: float) -> None:
        """Block every acquirer for ``seconds`` (server said "slow down")."""
        if seconds <= 0:
            return
        self._blocked_until = max(self._blocked_until, self._clock() + seconds)
        self._tokens = 0.0
        self._updated = self._clock()


@dataclass(frozen=True, slots=True)
class HostLimit:
    rate_per_second: float
    burst: float = 1.0


class RateLimiterRegistry:
    """Lazily creates one :class:`TokenBucket` per host (or arbitrary key).

    ``overrides`` maps a host or a host *suffix* (``".bestbuy.com"``) to a limit;
    the longest matching suffix wins. Hosts without an override use the default; when
    the default is ``None`` such hosts are not rate limited at all.
    """

    def __init__(self, default: HostLimit | None = None, overrides: Mapping[str, HostLimit] | None = None) -> None:
        self._default = default
        self._overrides = {k.lower(): v for k, v in (overrides or {}).items()}
        self._buckets: dict[str, TokenBucket] = {}

    def _limit_for(self, host: str) -> HostLimit | None:
        host = host.lower()
        if host in self._overrides:
            return self._overrides[host]
        best: tuple[int, HostLimit] | None = None
        for pattern, limit in self._overrides.items():
            suffix = pattern if pattern.startswith(".") else "." + pattern
            if host.endswith(suffix) and (best is None or len(suffix) > best[0]):
                best = (len(suffix), limit)
        if best is not None:
            return best[1]
        return self._default

    def for_host(self, host: str) -> TokenBucket | None:
        key = host.lower()
        bucket = self._buckets.get(key)
        if bucket is not None:
            return bucket
        limit = self._limit_for(key)
        if limit is None:
            return None
        bucket = TokenBucket(limit.rate_per_second, max(1.0, limit.burst))
        self._buckets[key] = bucket
        return bucket

    def for_key(self, key: str, rate_per_second: float, burst: float = 1.0) -> TokenBucket:
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = TokenBucket(rate_per_second, max(1.0, burst))
            self._buckets[key] = bucket
        return bucket


class CircuitOpenError(RuntimeError):
    """Raised when a call is attempted while the circuit is open."""


class CircuitBreaker:
    """Classic closed → open → half-open breaker.

    After ``failure_threshold`` consecutive failures the circuit opens for
    ``recovery_timeout`` seconds; then exactly one trial call is allowed (half-open).
    Success closes it, failure re-opens it with the timeout doubled up to ``max_timeout``.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        *,
        max_timeout: float = 600.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self.failure_threshold = failure_threshold
        self.base_timeout = recovery_timeout
        self.max_timeout = max_timeout
        self._clock = clock
        self._failures = 0
        self._state = self.CLOSED
        self._opened_at = 0.0
        self._timeout = recovery_timeout
        self._trial_in_flight = False

    @property
    def state(self) -> str:
        if self._state == self.OPEN and self._clock() - self._opened_at >= self._timeout:
            return self.HALF_OPEN
        return self._state

    @property
    def consecutive_failures(self) -> int:
        return self._failures

    def seconds_until_retry(self) -> float:
        if self._state != self.OPEN:
            return 0.0
        return max(0.0, self._timeout - (self._clock() - self._opened_at))

    def allow(self) -> bool:
        state = self.state
        if state == self.CLOSED:
            return True
        if state == self.HALF_OPEN and not self._trial_in_flight:
            self._trial_in_flight = True
            return True
        return False

    def record_success(self) -> None:
        self._failures = 0
        self._state = self.CLOSED
        self._timeout = self.base_timeout
        self._trial_in_flight = False

    def record_failure(self) -> None:
        self._failures += 1
        was_trial = self._trial_in_flight
        self._trial_in_flight = False
        if was_trial or self.state == self.HALF_OPEN:
            self._timeout = min(self.max_timeout, self._timeout * 2)
            self._state = self.OPEN
            self._opened_at = self._clock()
        elif self._failures >= self.failure_threshold:
            self._state = self.OPEN
            self._opened_at = self._clock()


__all__ = [
    "CircuitBreaker",
    "CircuitOpenError",
    "HostLimit",
    "RateLimiterRegistry",
    "TokenBucket",
]
