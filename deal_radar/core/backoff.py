"""Async retry primitives: capped exponential backoff with jitter + Retry-After support.

The delay schedule follows the "full jitter" strategy (sleep = U(0, min(cap, base * 2**n))),
which minimises synchronized retry storms when many workers hit the same failing
upstream at once. A server-provided ``Retry-After`` always wins over the computed
delay (bounded by ``max_retry_after``), and an overall deadline guarantees a retry
loop never stalls a poll cycle indefinitely.
"""

from __future__ import annotations

import asyncio
import email.utils
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TypeVar

T = TypeVar("T")


class RetryableError(Exception):
    """Raise from inside a retried call to request another attempt.

    ``retry_after`` (seconds) is the server's explicit back-off hint (HTTP 429/503
    ``Retry-After``, Discord ``retry_after``, Telegram ``parameters.retry_after``).
    """

    def __init__(self, message: str, *, retry_after: float | None = None, status: int | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after
        self.status = status


class RetryExhausted(Exception):
    """All attempts failed (or the deadline expired); wraps the last exception."""

    def __init__(self, attempts: int, last_exc: BaseException) -> None:
        super().__init__(f"gave up after {attempts} attempt(s): {last_exc!r}")
        self.attempts = attempts
        self.last_exc = last_exc


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    max_attempts: int = 4
    base_delay: float = 0.4
    max_delay: float = 15.0
    max_total_seconds: float | None = 30.0
    max_retry_after: float = 120.0
    jitter: str = "full"  # "full" | "equal" | "none"

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be non-negative")
        if self.jitter not in ("full", "equal", "none"):
            raise ValueError(f"unknown jitter mode {self.jitter!r}")

    def delay(self, failed_attempt: int, rng: random.Random | None = None) -> float:
        """Delay to sleep after the ``failed_attempt``-th (1-based) failure."""
        exp = min(self.max_delay, self.base_delay * (2 ** max(0, failed_attempt - 1)))
        r = (rng or random).random()
        if self.jitter == "full":
            return r * exp
        if self.jitter == "equal":
            return exp / 2 + r * exp / 2
        return exp


def parse_retry_after(value: str | None, *, now: float | None = None) -> float | None:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date) into seconds."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError):
            return None
        if parsed is None:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        current = now if now is not None else time.time()
        seconds = parsed.timestamp() - current
    if seconds != seconds:  # NaN
        return None
    return max(0.0, seconds)


DEFAULT_RETRY_ON: tuple[type[BaseException], ...] = (RetryableError, asyncio.TimeoutError, ConnectionError)


async def retry_async(
    fn: Callable[[], Awaitable[T]],
    *,
    policy: BackoffPolicy,
    retry_on: tuple[type[BaseException], ...] = DEFAULT_RETRY_ON,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    clock: Callable[[], float] = time.monotonic,
    rng: random.Random | None = None,
) -> T:
    """Call ``fn`` until it succeeds, retrying exceptions in ``retry_on``.

    Raises ``RetryExhausted`` when attempts or the deadline run out. Exceptions not in
    ``retry_on`` (and ``asyncio.CancelledError``) propagate immediately.
    """
    started = clock()
    attempt = 0
    while True:
        attempt += 1
        try:
            return await fn()
        except asyncio.CancelledError:
            raise
        except retry_on as exc:
            if attempt >= policy.max_attempts:
                raise RetryExhausted(attempt, exc) from exc
            server_hint = getattr(exc, "retry_after", None)
            if server_hint is not None:
                if server_hint > policy.max_retry_after:
                    raise RetryExhausted(attempt, exc) from exc
                delay = float(server_hint) + (rng or random).random() * 0.25
            else:
                delay = policy.delay(attempt, rng)
            if policy.max_total_seconds is not None:
                remaining = policy.max_total_seconds - (clock() - started)
                if remaining <= 0 or delay > remaining:
                    raise RetryExhausted(attempt, exc) from exc
            if on_retry is not None:
                on_retry(attempt, exc, delay)
            await sleep(delay)


class ExponentialBackoff:
    """Stateful backoff for long-running loops (e.g. consecutive poll failures)."""

    def __init__(
        self,
        base: float = 1.0,
        cap: float = 300.0,
        *,
        jitter: str = "equal",
        rng: random.Random | None = None,
    ) -> None:
        self._policy = BackoffPolicy(max_attempts=1, base_delay=base, max_delay=cap, jitter=jitter)
        self._failures = 0
        self._rng = rng

    @property
    def failures(self) -> int:
        return self._failures

    def next_delay(self) -> float:
        self._failures += 1
        return self._policy.delay(self._failures, self._rng)

    def reset(self) -> None:
        self._failures = 0


def utc_from_timestamp(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


__all__ = [
    "BackoffPolicy",
    "DEFAULT_RETRY_ON",
    "ExponentialBackoff",
    "RetryExhausted",
    "RetryableError",
    "parse_retry_after",
    "retry_async",
    "utc_from_timestamp",
]
