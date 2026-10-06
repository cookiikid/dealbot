"""Listing bus: hands :class:`RawListing` objects from collectors to processors.

Two interchangeable backends share one tiny interface (:class:`ListingBus`):

``MemoryBus`` (single node)
    A bounded :class:`asyncio.Queue`. ``publish`` awaits when the queue is full, so a
    burst from a fast source exerts *backpressure* on the source loop instead of
    growing memory without bound. ``ack`` is a no-op (nothing survives a restart
    anyway). ``close()`` wakes every consumer through a sentinel that each consumer
    re-queues for its siblings, and cancels publishers blocked on a full queue, so
    shutdown never hangs.

``RedisStreamBus`` (distributed)
    One Redis Stream (``{prefix}{bus.stream_key}``) and one consumer group
    (``bus.group``). Collectors ``XADD`` with ``MAXLEN ~ bus.maxlen`` (approximate
    trimming is O(1) amortised: Redis drops whole macro-nodes) a single field ``d``
    holding ``RawListing.model_dump_json()``. Encoding never fails: scraped text with
    lone UTF-16 surrogates (truncated emoji in upstream JSON), undecodable ``bytes`` or
    reference cycles in ``extra`` are scrubbed (:func:`encode_listing`) instead of
    raising out of the source loop.

    ``publish`` treats an unreachable Redis exactly like a full memory queue: it waits
    (capped exponential backoff, rate-limited warnings) until the entry is written or
    the bus is closed (:class:`BusClosedError`). A collector on a flaky residential
    link therefore pauses during an outage and resumes afterwards instead of losing
    its source loops; only non-transient errors (``WRONGTYPE``, ``NOPERM``...) raise
    :class:`BusError`, and those already surface at :meth:`start` on boot.
    ``OOM`` is treated as transient for publishing: memory comes back as processors
    ack and TTL'd keys expire.

    Processors use the node id as consumer name and fetch, in priority order:

    1. their *own* pending entries (``XREADGROUP ... 0``) once at start-up — a
       restarted node resumes exactly what it held when it crashed, without waiting
       for the idle timeout;
    2. entries other (dead) consumers left pending for longer than
       ``bus.claim_idle_ms`` (``XAUTOCLAIM``). The scan walks the whole PEL through the
       XAUTOCLAIM cursor and is repeated every ``max(block_ms, claim_idle_ms / 2)``,
       so a processor that dies while the others keep running is recovered too;
    3. new entries (``XREADGROUP ... >`` with ``BLOCK bus.block_ms COUNT
       bus.batch_size``).

    Delivery is *at-least-once*: an entry stays in the PEL until :meth:`ack`
    (``XACK``); the Redis deduplicator downstream makes duplicate deliveries
    harmless. Entries this process is still working on are tracked locally, so when a
    scan claims one of them (slow vision call) the claim only refreshes its idle time
    instead of handing the same listing to a second local worker.

    Poison entries (undecodable JSON, schema violations, missing payload) are
    acknowledged, logged and counted instead of being redelivered forever. Entries
    trimmed away by ``MAXLEN`` while still pending are acknowledged and counted as
    lost (a signal that ``bus.maxlen`` is too small for the processing lag).

    The consumer never dies on Redis trouble: connection errors, failovers and even a
    vanished stream/group (``FLUSHALL``, eviction → ``NOGROUP``) are retried with
    capped exponential backoff; the group is recreated when needed. Initial group
    creation uses ``$`` (renaming the group must not replay up to ``maxlen`` stale
    listings) while recreation after a loss uses ``0`` (the stream was recreated by
    later ``XADD`` calls and holds only fresh entries). Publishers also create the
    group, so entries published before the first processor starts are never skipped.

    ``close()`` makes ``consume()`` return within ~``block_ms`` (the blocking read is
    allowed to finish rather than cancelled, so no fetched entry is stranded on a
    half-read connection). The effective ``BLOCK`` is clamped below the client's
    ``socket_timeout`` so a blocking read is never mistaken for a dead socket.

Metrics: ``bus_published_total``, ``bus_consumed_total``, ``bus_poison_total``,
``bus_backlog`` (memory: queue size; Redis: consumer-group lag — entries not yet
delivered to the group — falling back to ``XLEN`` when the lag is unknown),
plus ``bus_pending``, ``bus_recovered_total{via}``, ``bus_lost_total``,
``bus_backpressure_total``, ``bus_sanitized_total`` and ``bus_errors_total{op}``.
"""

from __future__ import annotations

import abc
import asyncio
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError
from redis import exceptions as redis_exc

from deal_radar.core.backoff import BackoffPolicy, ExponentialBackoff, RetryExhausted, retry_async
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import RawListing

if TYPE_CHECKING:  # pragma: no cover
    from redis.asyncio import Redis

    from deal_radar.config_schema import AppConfig

log = get_logger("bus")

PAYLOAD_FIELD = "d"
_PAYLOAD_KEYS = (PAYLOAD_FIELD.encode(), PAYLOAD_FIELD)

# Redis errors worth retrying: network trouble, server loading/failing over.
TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    redis_exc.ConnectionError,  # includes BusyLoadingError / MaxConnectionsError
    redis_exc.TimeoutError,
    redis_exc.ReadOnlyError,  # connected to a replica right after a failover
    redis_exc.TryAgainError,
    redis_exc.ClusterDownError,
    OSError,
    asyncio.TimeoutError,
)

# Errors a publisher waits out instead of failing: transport trouble plus a full Redis
# (memory returns as processors ack and TTL'd dedup keys expire).
PUBLISH_RETRY_ERRORS: tuple[type[BaseException], ...] = (*TRANSIENT_ERRORS, redis_exc.OutOfMemoryError)

_PUBLISH_BACKOFF_BASE = 0.05  # seconds; doubles per consecutive failure ("equal" jitter)
_PUBLISH_BACKOFF_CAP = 2.0
_PUBLISH_LOG_INTERVAL = 30.0  # at most one "still failing" warning per publisher per interval
_START_POLICY = BackoffPolicy(max_attempts=6, base_delay=0.2, max_delay=2.0, max_total_seconds=15.0)
_BACKLOG_REFRESH_SECONDS = 2.0
_INFLIGHT_MAX = 100_000  # bound on locally tracked un-acked entry ids


class BusClosedError(RuntimeError):
    """Raised by ``publish`` once the bus has been closed."""


class BusError(Exception):
    """Non-transient Redis failure (``WRONGTYPE``, ``NOPERM``...), or ``start`` gave up."""


@dataclass
class BusMessage:
    """One delivered listing. ``id`` is the stream entry id (``None`` for the memory bus)."""

    raw: RawListing
    id: str | None = None


class ListingBus(abc.ABC):
    """Transport between collectors (``publish``) and the processor pool (``consume``)."""

    backend: str = "abstract"

    def __init__(self, *, metrics: Metrics | None = None) -> None:
        self.metrics = metrics if metrics is not None else Metrics()
        m = self.metrics
        self._m_published = m.counter("bus_published_total", "Listings published onto the bus")
        self._m_consumed = m.counter("bus_consumed_total", "Listings delivered to the processor by the bus")
        self._m_poison = m.counter("bus_poison_total", "Undecodable bus entries acknowledged and dropped")
        self._m_backlog = m.gauge("bus_backlog", "Listings waiting on the bus (queue size / consumer-group lag)")
        self._closed = False
        self._closed_event = asyncio.Event()

    @property
    def closed(self) -> bool:
        return self._closed

    async def start(self) -> None:
        """Prepare the transport (create stream/group). Idempotent."""

    @abc.abstractmethod
    async def publish(self, raw: RawListing) -> None:
        """Hand a listing to the processors. May wait (backpressure); raises once closed."""

    @abc.abstractmethod
    def consume(self) -> AsyncIterator[BusMessage]:
        """Async iterator of delivered listings; ends promptly after :meth:`close`."""

    @abc.abstractmethod
    async def ack(self, msg: BusMessage) -> None:
        """Mark a message as fully processed. Never raises for transport failures."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Stop consumption and reject further publishes. Idempotent; does not wait for consumers."""

    @abc.abstractmethod
    def backlog(self) -> int:
        """Listings waiting to be consumed (cheap, non-blocking)."""

    async def _sleep(self, delay: float) -> None:
        """Sleep that ends early when the bus is closed."""
        if delay <= 0 or self._closed:
            return
        try:
            await asyncio.wait_for(self._closed_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass


# --------------------------------------------------------------------------- memory


_STOP = object()  # close() sentinel; every consumer that sees it puts it back for its siblings


class MemoryBus(ListingBus):
    """In-process bounded queue for single-node deployments."""

    backend = "memory"

    def __init__(self, maxsize: int = 10_000, *, metrics: Metrics | None = None) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        super().__init__(metrics=metrics)
        self.maxsize = maxsize
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=maxsize)
        self._m_backpressure = self.metrics.counter(
            "bus_backpressure_total", "Publishes that had to wait for queue space"
        )

    async def publish(self, raw: RawListing) -> None:
        if self._closed:
            raise BusClosedError("bus is closed")
        try:
            self._queue.put_nowait(raw)
        except asyncio.QueueFull:
            self._m_backpressure.inc()
            await self._put_waiting(raw)
        self._m_published.inc()
        self._m_backlog.set(self._queue.qsize())

    async def _put_waiting(self, raw: RawListing) -> None:
        """Slow path: wait for queue space, but give up as soon as the bus closes."""
        put = asyncio.ensure_future(self._queue.put(raw))
        closing = asyncio.ensure_future(self._closed_event.wait())
        try:
            await asyncio.wait({put, closing}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (put, closing):
                if not task.done():
                    task.cancel()
        if not put.done() or put.cancelled():
            raise BusClosedError("bus closed while waiting for queue space")
        put.result()
        if self._closed:
            # Enqueued behind the stop sentinel: it will never be consumed.
            raise BusClosedError("bus closed while publishing")

    async def consume(self) -> AsyncIterator[BusMessage]:
        queue = self._queue
        while not self._closed:
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                item = await queue.get()
            if item is _STOP:
                # Re-queue synchronously: the slot we just freed is still ours.
                queue.put_nowait(_STOP)
                return
            self._m_consumed.inc()
            self._m_backlog.set(queue.qsize())
            yield BusMessage(raw=item)

    async def ack(self, msg: BusMessage) -> None:
        return None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._closed_event.set()
        dropped = 0
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is not _STOP:
                dropped += 1
        # The queue is empty and nothing ran since draining, so this cannot overflow.
        self._queue.put_nowait(_STOP)
        self._m_backlog.set(0)
        if dropped:
            log.warning("memory bus closed with queued listings", extra={"dropped": dropped})

    def backlog(self) -> int:
        if self._closed:
            return 0
        return self._queue.qsize()


# --------------------------------------------------------------------------- redis streams


def _as_str(value: Any) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


def _fields_dict(fields: Any) -> Mapping[Any, Any] | None:
    if fields is None:
        return None
    if isinstance(fields, Mapping):
        return fields
    if isinstance(fields, (list, tuple)):  # unparsed flat [k1, v1, k2, v2...]
        it = iter(fields)
        return dict(zip(it, it))
    return None


def _entry_list(entries: Any) -> list[tuple[Any, Any]]:
    """Normalise a list of stream entries into ``[(id, fields), ...]``."""
    if not entries:
        return []
    # RESP3 "legacy" parsing wraps each stream's entry list in one more list.
    first = entries[0]
    if (
        len(entries) == 1
        and isinstance(first, list)
        and first
        and isinstance(first[0], (list, tuple))
    ):
        entries = first
    out: list[tuple[Any, Any]] = []
    for entry in entries:
        if entry is None:
            continue
        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            out.append((entry[0], _fields_dict(entry[1])))
        elif isinstance(entry, (list, tuple)) and len(entry) == 1:
            out.append((entry[0], None))
    return out


def stream_entries(response: Any) -> list[tuple[Any, Any]]:
    """Flatten an XREAD/XREADGROUP reply (RESP2 list or RESP3 dict shapes)."""
    if not response:
        return []
    if isinstance(response, Mapping):
        groups: Sequence[Any] = list(response.values())
    else:
        groups = [item[1] for item in response if isinstance(item, (list, tuple)) and len(item) >= 2]
    out: list[tuple[Any, Any]] = []
    for entries in groups:
        out.extend(_entry_list(entries))
    return out


def parse_autoclaim(response: Any) -> tuple[str, list[tuple[Any, Any]], list[str]]:
    """``XAUTOCLAIM`` reply -> (next cursor, claimed entries, ids deleted from the PEL)."""
    if not response:
        return "0-0", [], []
    cursor = _as_str(response[0]) if response[0] is not None else "0-0"
    entries = _entry_list(response[1]) if len(response) > 1 else []
    deleted = [_as_str(i) for i in response[2]] if len(response) > 2 and response[2] else []
    return cursor, entries, deleted


def decode_payload(fields: Mapping[Any, Any] | None) -> tuple[RawListing | None, str | None]:
    """Return ``(listing, None)`` or ``(None, reason)`` for one stream entry's fields."""
    if not fields:
        return None, "trimmed"  # entry deleted from the stream while still pending
    payload = None
    for key in _PAYLOAD_KEYS:
        payload = fields.get(key)
        if payload is not None:
            break
    if payload is None:
        return None, "missing_payload"
    if isinstance(payload, memoryview):
        payload = bytes(payload)
    try:
        return RawListing.model_validate_json(payload), None
    except ValidationError as exc:
        first = exc.errors()[0] if exc.error_count() else {}
        where = ".".join(str(p) for p in first.get("loc", ())) or "<root>"
        return None, f"invalid: {exc.error_count()} error(s), first at {where}: {first.get('msg', '')}"[:300]
    except (TypeError, ValueError, UnicodeDecodeError) as exc:
        return None, f"invalid: {exc!r}"[:300]


def _json_fallback(value: Any) -> Any:
    """Serialise exotic ``extra`` values (objects, enums from other libs) as strings."""
    return str(value)


_SCRUB_MAX_DEPTH = 24  # deeper (or cyclic) ``extra`` structures collapse to their repr


def _clean_str(value: str) -> str:
    """Valid UTF-8 text: re-pair split surrogates, replace lone ones with U+FFFD."""
    try:
        value.encode("utf-8")
        return value
    except UnicodeEncodeError:
        return value.encode("utf-16-le", "surrogatepass").decode("utf-16-le", "replace")


def _scrub(value: Any, depth: int = 0) -> Any:
    """JSON-safe copy of ``value``: clean text, decoded bytes, no cycles."""
    if isinstance(value, str):
        return _clean_str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", "replace")
    if depth >= _SCRUB_MAX_DEPTH and isinstance(value, (Mapping, list, tuple, set, frozenset, BaseModel)):
        try:
            return _clean_str(repr(value))[:1000]  # repr() renders cycles as {...} / [...]
        except Exception:  # noqa: BLE001 - hostile __repr__
            return "<unrepresentable>"
    if isinstance(value, BaseModel):
        return _scrub(value.model_dump(), depth + 1)
    if isinstance(value, Mapping):
        return {
            (_scrub(k, depth + 1) if isinstance(k, (str, bytes)) else k): _scrub(v, depth + 1)
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_scrub(v, depth + 1) for v in value]
    return value  # numbers, datetimes, enums...; anything else goes through _json_fallback


def encode_listing(raw: RawListing) -> tuple[str, bool]:
    """``RawListing`` -> stream payload. Returns ``(json, sanitized)`` and never raises.

    The fast path is plain ``model_dump_json``. It fails on lone surrogates, invalid
    UTF-8 ``bytes`` and reference cycles, so the slow path scrubs every field and
    re-validates; if ``extra`` still cannot be serialised it is replaced by a marker.
    """
    try:
        return raw.model_dump_json(fallback=_json_fallback), False
    except (ValueError, TypeError):  # PydanticSerializationError is a ValueError
        pass
    data = {name: _scrub(getattr(raw, name)) for name in type(raw).model_fields}
    try:
        return RawListing.model_validate(data).model_dump_json(fallback=_json_fallback), True
    except (ValueError, TypeError):
        data["extra"] = {"bus_unserializable_extra": True}
        return RawListing.model_validate(data).model_dump_json(fallback=_json_fallback), True


class RedisStreamBus(ListingBus):
    """Redis Streams consumer-group bus (see module docstring for the delivery protocol)."""

    backend = "redis"

    def __init__(
        self,
        redis: "Redis",
        config: "AppConfig",
        consumer_name: str,
        *,
        metrics: Metrics | None = None,
    ) -> None:
        if not consumer_name:
            raise ValueError("consumer_name must be non-empty")
        super().__init__(metrics=metrics)
        bus = config.bus
        self.redis = redis
        self.stream = f"{config.storage.redis_key_prefix}{bus.stream_key}"
        self.group = bus.group
        self.consumer = consumer_name
        self.maxlen = bus.maxlen
        self.batch_size = bus.batch_size
        self.claim_idle_ms = bus.claim_idle_ms
        self.block_ms = self._effective_block_ms(bus.block_ms)
        self.claim_interval = max(self.block_ms, self.claim_idle_ms // 2) / 1000.0
        # A claimed entry that this process is still working on is re-delivered locally
        # only when it has been in flight this long (worker lost it without acking).
        self.reclaim_grace = 4 * self.claim_idle_ms / 1000.0
        self._group_ready = False
        self._group_lost = False
        self._own_cursor: str | None = "0"  # replay our own PEL first; None once done
        self._claim_cursor = "0-0"
        self._next_claim_at = 0.0
        self._autoclaim_supported = True
        self._inflight: OrderedDict[str, float] = OrderedDict()
        self._backlog = 0
        self._next_backlog_at = 0.0
        self._log_extra = {"stream": self.stream, "group": self.group, "consumer": self.consumer}
        m = self.metrics
        self._m_pending = m.gauge("bus_pending", "Entries delivered to the consumer group but not yet acked")
        self._m_recovered = m.counter(
            "bus_recovered_total", "Pending entries re-delivered after a crash or stall", ("via",)
        )
        self._m_lost = m.counter("bus_lost_total", "Pending entries trimmed from the stream before processing")
        self._m_errors = m.counter("bus_errors_total", "Redis errors in bus operations", ("op",))

    # ------------------------------------------------------------------ setup

    def _effective_block_ms(self, block_ms: int) -> int:
        """Keep BLOCK below the client's socket timeout (else every idle read times out)."""
        pool = getattr(self.redis, "connection_pool", None)
        kwargs = getattr(pool, "connection_kwargs", None) or {}
        socket_timeout = kwargs.get("socket_timeout") if isinstance(kwargs, Mapping) else None
        if isinstance(socket_timeout, (int, float)) and socket_timeout > 0:
            cap = max(10, int(socket_timeout * 1000 * 0.75))
            if block_ms > cap:
                log.warning(
                    "bus.block_ms exceeds the Redis socket timeout; clamping",
                    extra={"block_ms": block_ms, "effective_block_ms": cap, "socket_timeout_s": socket_timeout},
                )
                return cap
        return block_ms

    async def _create_group(self) -> None:
        start_id = "0" if self._group_lost else "$"
        try:
            await self.redis.xgroup_create(self.stream, self.group, id=start_id, mkstream=True)
            log.info("bus consumer group created", extra={**self._log_extra, "start_id": start_id})
        except redis_exc.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._group_ready = True
        self._group_lost = False

    async def start(self) -> None:
        if self._group_ready:
            return
        try:
            await retry_async(self._create_group, policy=_START_POLICY, retry_on=TRANSIENT_ERRORS)
        except RetryExhausted as exc:
            self._m_errors.inc(op="start")
            raise BusError(f"cannot create consumer group {self.group!r} on {self.stream!r}: {exc.last_exc!r}") from exc
        except redis_exc.RedisError as exc:  # e.g. WRONGTYPE: the key is not a stream
            self._m_errors.inc(op="start")
            raise BusError(f"cannot create consumer group {self.group!r} on {self.stream!r}: {exc!r}") from exc
        await self._maybe_refresh_backlog(force=True)

    # ------------------------------------------------------------------ publish

    async def publish(self, raw: RawListing) -> None:
        if self._closed:
            raise BusClosedError("bus is closed")
        payload = raw.model_dump_json(fallback=_json_fallback)

        async def _xadd() -> Any:
            if not self._group_ready:
                await self._create_group()
            return await self.redis.xadd(
                self.stream, {PAYLOAD_FIELD: payload}, maxlen=self.maxlen, approximate=True
            )

        def _on_retry(attempt: int, exc: BaseException, delay: float) -> None:
            self._m_errors.inc(op="publish")
            log.warning(
                "bus publish failed; retrying",
                extra={**self._log_extra, "attempt": attempt, "error": repr(exc), "retry_in_s": round(delay, 3)},
            )

        try:
            await retry_async(_xadd, policy=_PUBLISH_POLICY, retry_on=TRANSIENT_ERRORS, on_retry=_on_retry)
        except RetryExhausted as exc:
            self._m_errors.inc(op="publish")
            raise BusError(f"publish to {self.stream!r} failed: {exc.last_exc!r}") from exc
        except redis_exc.RedisError as exc:
            self._m_errors.inc(op="publish")
            raise BusError(f"publish to {self.stream!r} failed: {exc!r}") from exc
        self._m_published.inc()
        await self._maybe_refresh_backlog()

    # ------------------------------------------------------------------ consume

    async def consume(self) -> AsyncIterator[BusMessage]:
        backoff = ExponentialBackoff(base=0.1, cap=5.0, jitter="equal")
        while not self._closed:
            try:
                if not self._group_ready:
                    await self._create_group()
                entries = await self._next_batch()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the consumer must outlive any Redis trouble
                delay = backoff.next_delay()
                self._m_errors.inc(op="consume")
                transient = isinstance(exc, TRANSIENT_ERRORS)
                if isinstance(exc, redis_exc.ResponseError) and not transient:
                    # NOGROUP (stream/group vanished) and friends: recreate before the next read.
                    self._group_ready = False
                    self._group_lost = "NOGROUP" in str(exc) or "requires the key to exist" in str(exc)
                    self._claim_cursor = "0-0"
                log.warning(
                    "bus read failed; backing off",
                    extra={**self._log_extra, "error": repr(exc), "retry_in_s": round(delay, 3)},
                    exc_info=not isinstance(exc, redis_exc.RedisError),
                )
                await self._sleep(delay)
                continue
            backoff.reset()
            index = 0
            try:
                while index < len(entries):
                    if self._closed:
                        return
                    entry_id, fields = entries[index]
                    index += 1
                    raw, problem = decode_payload(fields)
                    if raw is None:
                        await self._drop(entry_id, problem or "invalid")
                        continue
                    self._m_consumed.inc()
                    yield BusMessage(raw=raw, id=entry_id)
            finally:
                # Fetched but never handed out (closed, or the caller stopped iterating): the
                # entries stay pending in Redis; forget them locally so a scan can redeliver them.
                for entry_id, _ in entries[index:]:
                    self._inflight.pop(entry_id, None)
            await self._maybe_refresh_backlog()

    async def _next_batch(self) -> list[tuple[str, Any]]:
        # 1) Our own PEL: entries this consumer name held when the process last died.
        if self._own_cursor is not None:
            response = await self.redis.xreadgroup(
                self.group, self.consumer, {self.stream: self._own_cursor}, count=self.batch_size
            )
            entries = stream_entries(response)
            if entries:
                self._own_cursor = _as_str(entries[-1][0])
                admitted = self._admit(entries, via="pel")
                if admitted:
                    log.info("bus resuming own pending entries", extra={**self._log_extra, "count": len(admitted)})
                return admitted
            self._own_cursor = None

        # 2) Crash recovery: claim entries other consumers left idle for too long.
        now = time.monotonic()
        if self._autoclaim_supported and (self._claim_cursor != "0-0" or now >= self._next_claim_at):
            response = await self._autoclaim()
            if response is not None:
                cursor, entries, deleted = parse_autoclaim(response)
                self._claim_cursor = cursor
                if cursor == "0-0":
                    self._next_claim_at = now + self.claim_interval
                if deleted:
                    for entry_id in deleted:
                        self._inflight.pop(entry_id, None)
                    self._m_lost.inc(len(deleted))
                    log.warning(
                        "bus pending entries were trimmed before processing (increase bus.maxlen?)",
                        extra={**self._log_extra, "count": len(deleted)},
                    )
                admitted = self._admit(entries, via="autoclaim")
                if admitted:
                    log.info("bus reclaimed idle entries", extra={**self._log_extra, "count": len(admitted)})
                    return admitted
                if cursor != "0-0":
                    return []  # keep walking the PEL before reading new entries

        # 3) New entries.
        started = time.monotonic()
        response = await self.redis.xreadgroup(
            self.group, self.consumer, {self.stream: ">"}, count=self.batch_size, block=self.block_ms
        )
        entries = stream_entries(response)
        if not entries:
            # Some servers/proxies (and test doubles) ignore BLOCK; never busy-spin.
            remaining = self.block_ms / 1000.0 - (time.monotonic() - started)
            if remaining > self.block_ms / 2000.0:
                await self._sleep(remaining)
            return []
        return self._admit(entries, via=None)

    async def _autoclaim(self) -> Any:
        try:
            return await self.redis.xautoclaim(
                self.stream,
                self.group,
                self.consumer,
                self.claim_idle_ms,
                start_id=self._claim_cursor,
                count=self.batch_size,
            )
        except redis_exc.ResponseError as exc:
            if "unknown command" in str(exc).lower():
                self._autoclaim_supported = False
                log.error(
                    "XAUTOCLAIM unsupported (Redis < 6.2); crashed consumers' entries will not be recovered",
                    extra=self._log_extra,
                )
                return None
            raise

    def _admit(self, entries: list[tuple[Any, Any]], *, via: str | None) -> list[tuple[str, Any]]:
        """Track entries as in flight; skip re-deliveries of entries still being processed here."""
        now = time.monotonic()
        inflight = self._inflight
        out: list[tuple[str, Any]] = []
        for raw_id, fields in entries:
            if raw_id is None:
                continue
            entry_id = _as_str(raw_id)
            if via is not None:
                since = inflight.get(entry_id)
                if since is not None and now - since < self.reclaim_grace:
                    continue  # our claim just refreshed its idle time; a local worker still has it
                self._m_recovered.inc(via=via)
            inflight[entry_id] = now
            inflight.move_to_end(entry_id)
            out.append((entry_id, fields))
        while len(inflight) > _INFLIGHT_MAX:
            inflight.popitem(last=False)
        return out

    async def _drop(self, entry_id: str, reason: str) -> None:
        """Acknowledge an entry that can never be processed (poison or trimmed)."""
        if reason == "trimmed":
            self._m_lost.inc()
            log.warning("bus entry trimmed before processing", extra={**self._log_extra, "entry_id": entry_id})
        else:
            self._m_poison.inc()
            log.warning(
                "bus poison entry dropped", extra={**self._log_extra, "entry_id": entry_id, "reason": reason}
            )
        await self._xack(entry_id)

    async def _xack(self, entry_id: str) -> None:
        self._inflight.pop(entry_id, None)
        try:
            await self.redis.xack(self.stream, self.group, entry_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - an unacked entry is simply redelivered later
            self._m_errors.inc(op="ack")
            log.warning("bus ack failed", extra={**self._log_extra, "entry_id": entry_id, "error": repr(exc)})

    # ------------------------------------------------------------------ ack / close / backlog

    async def ack(self, msg: BusMessage) -> None:
        if msg.id is not None:
            await self._xack(msg.id)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._closed_event.set()
        log.info("bus closing", extra={**self._log_extra, "inflight": len(self._inflight)})

    def backlog(self) -> int:
        return self._backlog

    async def refresh_backlog(self) -> int:
        """Query the group lag (``XINFO GROUPS``; ``XLEN`` fallback) and update the gauges."""
        lag: Any = None
        pending: Any = None
        try:
            groups = await self.redis.xinfo_groups(self.stream)
        except redis_exc.ResponseError:
            groups = []  # stream does not exist yet
        for info in groups or []:
            if not isinstance(info, Mapping):
                continue
            if _as_str(info.get("name", "")) == self.group:
                lag = info.get("lag")
                pending = info.get("pending")
                break
        if lag is None:
            lag = await self.redis.xlen(self.stream)
        self._backlog = int(lag or 0)
        self._m_backlog.set(self._backlog)
        if pending is not None:
            self._m_pending.set(int(pending))
        return self._backlog

    async def _maybe_refresh_backlog(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now < self._next_backlog_at:
            return
        self._next_backlog_at = now + _BACKLOG_REFRESH_SECONDS
        try:
            await self.refresh_backlog()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a stale gauge must never break the data path
            self._m_errors.inc(op="backlog")
            log.debug("bus backlog refresh failed", extra={**self._log_extra, "error": repr(exc)})


# --------------------------------------------------------------------------- factory


def build_bus(config: "AppConfig", redis: "Redis | None" = None, *, metrics: Metrics | None = None) -> ListingBus:
    """Bus selected by ``config.bus.backend``; the Redis consumer name is the node id."""
    if config.bus.backend == "redis":
        if redis is None:
            raise ValueError("bus.backend=redis requires a Redis client (storage.redis_url)")
        return RedisStreamBus(redis, config, config.app.node_id, metrics=metrics)
    return MemoryBus(config.bus.queue_maxsize, metrics=metrics)


__all__ = [
    "BusClosedError",
    "BusError",
    "BusMessage",
    "ListingBus",
    "MemoryBus",
    "PAYLOAD_FIELD",
    "RedisStreamBus",
    "TRANSIENT_ERRORS",
    "build_bus",
    "decode_payload",
    "parse_autoclaim",
    "stream_entries",
]
