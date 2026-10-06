"""Abstract ingestor: polling loop, change detection, health, metrics and fail-over leases.

Concrete sources only implement :meth:`BaseIngestor.poll` (one polling cycle that
returns :class:`RawListing` objects). Everything operational lives here so every
source behaves identically under failure:

* jittered polling intervals (avoid phase-locking with upstream cache TTLs and with
  other workers),
* a per-source circuit breaker + exponential backoff on consecutive failures,
* an LRU *change detector*: only listings that are new or whose price/stock changed
  are emitted downstream, so a 200-result eBay page that did not change costs zero
  pipeline work,
* a stale-item guard (``max_item_age_minutes``) so restarts don't replay old feed posts,
* optional Redis *leases* for active/passive fail-over between nodes: when several
  nodes enable the same source only the lease holder polls; if it dies the lease
  expires and a standby takes over within ``lease_ttl_seconds``.
"""

from __future__ import annotations

import abc
import asyncio
import random
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Hashable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

from deal_radar.core.backoff import ExponentialBackoff
from deal_radar.core.http import HttpClient
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.core.ratelimit import CircuitBreaker
from deal_radar.engine.types import RawListing, SourceKind, utcnow

if TYPE_CHECKING:  # pragma: no cover
    from redis.asyncio import Redis

    from deal_radar.config_schema import AppConfig, Profile, SourceCommon

EmitFn = Callable[[RawListing], Awaitable[None]]
NotifyFn = Callable[[str, str], Awaitable[None]]  # (title, message) -> operator notice


class SourceError(Exception):
    """Generic recoverable ingestion failure."""


class SourceBlocked(SourceError):
    """Upstream is actively blocking us (checkpoint, captcha, 403 wall).

    The loop backs off for ``cooldown_seconds`` (or the source's configured cooldown)
    and sends an operator notice, instead of hammering a blocked endpoint.
    """

    def __init__(self, message: str, *, cooldown_seconds: float | None = None) -> None:
        super().__init__(message)
        self.cooldown_seconds = cooldown_seconds


class SourceAuthError(SourceError):
    """Credentials are missing/invalid/expired and could not be refreshed."""


@dataclass
class IngestorContext:
    http: HttpClient
    metrics: Metrics
    config: "AppConfig"
    node_id: str
    redis: "Redis | None" = None
    notify: NotifyFn | None = None
    rng: random.Random = field(default_factory=random.Random)


@dataclass
class SourceHealth:
    name: str
    state: str = "idle"  # idle | ok | degraded | open | standby | stopped
    polls: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    items_seen: int = 0
    items_emitted: int = 0
    last_poll_ms: float | None = None
    last_success_at: datetime | None = None
    last_error: str | None = None
    last_error_at: datetime | None = None
    next_poll_in_s: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state,
            "polls": self.polls,
            "failures": self.failures,
            "consecutive_failures": self.consecutive_failures,
            "items_seen": self.items_seen,
            "items_emitted": self.items_emitted,
            "last_poll_ms": self.last_poll_ms,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "last_error": self.last_error,
            "last_error_at": self.last_error_at.isoformat() if self.last_error_at else None,
            "next_poll_in_s": self.next_poll_in_s,
        }


class ChangeDetector:
    """Bounded LRU of listing_key -> signature; tells whether a listing is new/changed."""

    def __init__(self, max_entries: int = 50_000) -> None:
        self.max_entries = max_entries
        self._entries: OrderedDict[str, Hashable] = OrderedDict()

    def changed(self, key: str, signature: Hashable) -> bool:
        previous = self._entries.get(key)
        self._entries[key] = signature
        self._entries.move_to_end(key)
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)
        return previous != signature or previous is None

    def forget(self, key: str) -> None:
        self._entries.pop(key, None)

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, key: str) -> bool:
        return key in self._entries


_LEASE_ACQUIRE = """
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
if current == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
"""

_LEASE_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class SourceLease:
    """Redis-backed active/passive lease so only one node polls a given source."""

    def __init__(self, redis: "Redis", key: str, owner: str, ttl_seconds: float) -> None:
        self.redis = redis
        self.key = key
        self.owner = owner
        self.ttl_ms = int(ttl_seconds * 1000)
        self._acquire = redis.register_script(_LEASE_ACQUIRE)
        self._release = redis.register_script(_LEASE_RELEASE)

    async def acquire(self) -> bool:
        return bool(await self._acquire(keys=[self.key], args=[self.owner, self.ttl_ms]))

    async def release(self) -> None:
        await self._release(keys=[self.key], args=[self.owner])


class BaseIngestor(abc.ABC):
    """Base class for every source. Subclasses set ``name``/``kind`` and implement ``poll``."""

    name: ClassVar[str] = "base"
    kind: ClassVar[SourceKind] = SourceKind.RETAIL

    def __init__(self, cfg: "SourceCommon", ctx: IngestorContext) -> None:
        self.cfg = cfg
        self.ctx = ctx
        self.log = get_logger(f"sources.{self.name}")
        self.health = SourceHealth(self.name)
        self.changes = ChangeDetector()
        self._breaker = CircuitBreaker(
            failure_threshold=max(1, cfg.max_consecutive_failures),
            recovery_timeout=cfg.cooldown_seconds,
            max_timeout=max(cfg.cooldown_seconds, 3600.0),
        )
        self._backoff = ExponentialBackoff(base=max(1.0, cfg.poll_interval_seconds), cap=cfg.cooldown_seconds, rng=ctx.rng)
        m = ctx.metrics
        self._m_polls = m.counter("source_polls_total", "Source poll cycles", ("source", "outcome"))
        self._m_items = m.counter("source_items_total", "Listings returned by sources", ("source",))
        self._m_emitted = m.counter("source_emitted_total", "New/changed listings emitted downstream", ("source",))
        self._m_poll_ms = m.histogram("source_poll_ms", "Duration of one poll cycle (ms)", ("source",))
        self._m_state = m.gauge("source_up", "1 if the source's last poll succeeded", ("source",))
        self._lease: SourceLease | None = None
        lease_ttl = getattr(cfg, "lease_ttl_seconds", None)
        if lease_ttl and ctx.redis is not None:
            prefix = ctx.config.storage.redis_key_prefix
            self._lease = SourceLease(ctx.redis, f"{prefix}lease:{self.name}", ctx.node_id, lease_ttl)

    # ------------------------------------------------------------------ hooks

    async def setup(self) -> None:
        """Open long-lived resources (browsers, OAuth tokens). Called once before polling."""

    async def teardown(self) -> None:
        """Release resources. Must be safe to call even if ``setup`` failed."""

    @abc.abstractmethod
    async def poll(self) -> list[RawListing]:
        """Run one polling cycle and return every listing currently visible."""

    def signature(self, raw: RawListing) -> Hashable:
        """What counts as a change for a listing. Override for source-specific fields."""
        return (str(raw.price), str(raw.shipping), raw.in_stock, raw.title)

    def next_interval(self) -> float:
        """Seconds until the next poll (jittered). Sources with quotas override this."""
        base = float(self.cfg.poll_interval_seconds)
        jitter = float(self.cfg.jitter_pct)
        return max(0.05, base * (1.0 + self.ctx.rng.uniform(-jitter, jitter)))

    # ------------------------------------------------------------------ helpers

    def search_profiles(self) -> list["Profile"]:
        """Enabled profiles that have search terms and allow this source."""
        allowed = set(self.cfg.profiles)
        out = []
        for profile in self.ctx.config.profiles:
            if not profile.enabled or not profile.search.terms:
                continue
            if allowed and profile.id not in allowed:
                continue
            if profile.search.sources and self.name not in profile.search.sources:
                continue
            out.append(profile)
        return out

    def select_changed(self, listings: Sequence[RawListing]) -> list[RawListing]:
        """Drop stale and unchanged listings; stamp node ids."""
        fresh: list[RawListing] = []
        max_age = self.cfg.max_item_age_minutes
        cutoff = utcnow() - timedelta(minutes=max_age) if max_age else None
        for raw in listings:
            if cutoff is not None and raw.posted_at is not None and raw.posted_at < cutoff:
                # Remember it so it is not re-evaluated when it stays in the feed.
                self.changes.changed(raw.listing_key, self.signature(raw))
                continue
            if self.changes.changed(raw.listing_key, self.signature(raw)):
                if raw.node_id is None:
                    raw.node_id = self.ctx.node_id
                fresh.append(raw)
        return fresh

    async def run_once(self) -> list[RawListing]:
        """One poll + change detection, without the scheduling loop (CLI ``--once``, tests)."""
        started = time.perf_counter()
        listings = await asyncio.wait_for(self.poll(), timeout=self.cfg.poll_timeout_seconds)
        elapsed = (time.perf_counter() - started) * 1000.0
        fresh = self.select_changed(listings)
        self._record_success(len(listings), len(fresh), elapsed)
        return fresh

    # ------------------------------------------------------------------ loop

    async def run(self, emit: EmitFn, stop: asyncio.Event) -> None:
        """Poll forever until ``stop`` is set, emitting new/changed listings."""
        self.log.info("source starting", extra={"source": self.name, "interval_s": self.cfg.poll_interval_seconds})
        try:
            await self.setup()
        except Exception as exc:  # noqa: BLE001 - surface any setup failure as source state
            self.health.state = "stopped"
            self._record_failure(exc)
            self.log.exception("source setup failed", extra={"source": self.name})
            await self._notify(f"{self.name} failed to start", repr(exc))
            return
        try:
            while not stop.is_set():
                delay = await self._cycle(emit)
                self.health.next_poll_in_s = round(delay, 2)
                if await _wait(stop, delay):
                    break
        finally:
            self.health.state = "stopped"
            if self._lease is not None:
                try:
                    await self._lease.release()
                except Exception:  # noqa: BLE001 - best effort on shutdown
                    pass
            try:
                await self.teardown()
            except Exception:  # noqa: BLE001
                self.log.exception("source teardown failed", extra={"source": self.name})
            self.log.info("source stopped", extra={"source": self.name})

    async def _cycle(self, emit: EmitFn) -> float:
        if self._lease is not None:
            try:
                if not await self._lease.acquire():
                    self.health.state = "standby"
                    return self.next_interval()
            except Exception as exc:  # noqa: BLE001 - Redis down: degrade to polling locally
                self.log.warning("lease check failed; polling anyway", extra={"source": self.name, "error": repr(exc)})
        if not self._breaker.allow():
            self.health.state = "open"
            return max(1.0, self._breaker.seconds_until_retry())
        try:
            fresh = await self.run_once()
        except asyncio.CancelledError:
            raise
        except SourceBlocked as exc:
            self._breaker.record_failure()
            self._record_failure(exc)
            self.health.state = "open"
            cooldown = exc.cooldown_seconds or self.cfg.cooldown_seconds
            self.log.error("source blocked", extra={"source": self.name, "error": str(exc), "cooldown_s": cooldown})
            await self._notify(f"{self.name} blocked", f"{exc} — pausing {cooldown:.0f}s")
            return cooldown
        except Exception as exc:  # noqa: BLE001 - any poll failure is handled uniformly
            self._breaker.record_failure()
            self._record_failure(exc)
            delay = max(self.next_interval(), self._backoff.next_delay())
            level = self.log.error if self._breaker.state != CircuitBreaker.CLOSED else self.log.warning
            level("poll failed", extra={"source": self.name, "error": repr(exc), "retry_in_s": round(delay, 1)})
            if self._breaker.state == CircuitBreaker.OPEN and self.health.consecutive_failures == self.cfg.max_consecutive_failures:
                await self._notify(f"{self.name} circuit open", f"{self.health.consecutive_failures} consecutive failures: {exc!r}")
            return delay
        self._breaker.record_success()
        self._backoff.reset()
        for raw in fresh:
            await emit(raw)
        return self.next_interval()

    # ------------------------------------------------------------------ bookkeeping

    def _record_success(self, seen: int, emitted: int, elapsed_ms: float) -> None:
        h = self.health
        h.state = "ok"
        h.polls += 1
        h.consecutive_failures = 0
        h.items_seen += seen
        h.items_emitted += emitted
        h.last_poll_ms = round(elapsed_ms, 2)
        h.last_success_at = utcnow()
        self._m_polls.inc(source=self.name, outcome="ok")
        self._m_items.inc(seen, source=self.name)
        self._m_emitted.inc(emitted, source=self.name)
        self._m_poll_ms.observe(elapsed_ms, source=self.name)
        self._m_state.set(1, source=self.name)

    def _record_failure(self, exc: BaseException) -> None:
        h = self.health
        h.polls += 1
        h.failures += 1
        h.consecutive_failures += 1
        h.state = "degraded"
        h.last_error = f"{type(exc).__name__}: {exc}"[:500]
        h.last_error_at = utcnow()
        self._m_polls.inc(source=self.name, outcome=type(exc).__name__)
        self._m_state.set(0, source=self.name)

    async def _notify(self, title: str, message: str) -> None:
        if self.ctx.notify is None:
            return
        try:
            await self.ctx.notify(title, message)
        except Exception:  # noqa: BLE001 - notices must never break ingestion
            self.log.warning("operator notice failed", extra={"source": self.name})


async def _wait(stop: asyncio.Event, timeout: float) -> bool:
    """Sleep up to ``timeout`` seconds; return True early if ``stop`` gets set."""
    if timeout <= 0:
        return stop.is_set()
    try:
        await asyncio.wait_for(stop.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        return False
    return True


__all__ = [
    "BaseIngestor",
    "ChangeDetector",
    "EmitFn",
    "IngestorContext",
    "NotifyFn",
    "SourceAuthError",
    "SourceBlocked",
    "SourceError",
    "SourceHealth",
    "SourceLease",
]
