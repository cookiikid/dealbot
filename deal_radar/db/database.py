"""Async persistence layer: listings, price snapshots, alerts, the batch Recorder and Redis.

Design decisions
----------------
* **One code path for SQLite and PostgreSQL.** Everything goes through SQLAlchemy Core
  statements built from the ORM tables in :mod:`deal_radar.db.models`; the only
  dialect-specific piece is the ``INSERT ... ON CONFLICT DO UPDATE`` construct, which both
  backends support with identical semantics (``sqlite.insert`` / ``postgresql.insert``).
* **SQLite tuning** (applied on every new DBAPI connection): ``busy_timeout=5000`` (writers
  wait instead of failing with "database is locked"), ``journal_mode=WAL`` (readers never
  block the writer — the API can query while the Recorder flushes), ``synchronous=NORMAL``
  (safe with WAL, far fewer fsyncs) and ``foreign_keys=ON`` (snapshot cascade on prune).
  SQLite file databases get their parent directory created. PostgreSQL gets a sized pool
  with pre-ping/recycle, and ``create_all`` runs under a transaction-scoped advisory lock
  so several processor nodes can start concurrently against the same database.
* **Snapshot writes are one transaction**: listings are upserted (merged in Python first
  so a batch never touches the same row twice — PostgreSQL forbids that inside a single
  ``ON CONFLICT DO UPDATE`` command — and sorted by primary key so concurrent writers
  lock rows in one global order and cannot deadlock), then snapshots are bulk-inserted.
  The upsert is *order independent*: ``last_*``/``title``/``status`` only move forward
  in time, ``first_*`` only backward, and ``min_price`` is the minimum ever seen, so late
  or retried batches can never regress a listing.
* **One bad record must not sink a batch.** Scraped strings are sanitised for every
  backend (:func:`~deal_radar.db.models.db_text`: NUL bytes, lone surrogates, column
  widths), JSON documents are coerced by :func:`json_safe` (``\\u0000``, >64-bit ints,
  NaN), and records with a non-finite price are skipped.
* **Snapshot ``accepted`` means "valid market observation"**: the text filter accepted the
  item *and* the scorer did not trip a disqualifying hard gate (``above_ceiling`` is not
  disqualifying: the price is real, just too high to alert). This mirrors
  ``AnomalyScorer.observe`` so :meth:`Database.load_price_history` warm-starts exactly the
  samples the live scorer would have learned from. ``reject_code`` holds the text-filter
  code or the scorer's gate reason.
* **History warm start** uses two ``ROW_NUMBER()`` windows (SQLite >= 3.25 and every
  PostgreSQL): first the latest accepted snapshot per listing, then the ``max_per_key``
  most recent of those per ``(product_key, condition_class)``. The database does the
  ranking; Python only receives the rows it will keep.
* **The Recorder keeps the database off the hot path.** ``record()`` is a non-blocking
  ``put_nowait`` onto a bounded queue (overflow is dropped and counted, never awaited); a
  single background task flushes when ``batch_size`` records are waiting or
  ``flush_seconds`` after the first pending record arrived, and retries a failed flush
  once. If the retry fails too, a *transient* error (outage, lock timeout, deadlock —
  see :func:`is_transient_error`) drops the batch; any other error is treated as a
  poison record and the batch is bisected so only the offending records are dropped.
  Drops are logged once per batch and counted, so a database outage can never stall or
  crash the pipeline. ``stop()`` drains everything that is queued.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import parse_qsl, urlencode, urlsplit

import orjson
import redis.asyncio as redis_asyncio
from sqlalchemy import ColumnElement, Select, Table, case, delete, event, func, insert, select, true
from sqlalchemy import exc as sa_exc
from sqlalchemy.dialects import postgresql as pg_dialect
from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.sql.dml import Insert

from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.db.models import (
    CATEGORY_LEN,
    CONDITION_LEN,
    PRODUCT_KEY_LEN,
    PROFILE_LEN,
    REJECT_CODE_LEN,
    SEVERITY_LEN,
    SHORT_TEXT_LEN,
    SOURCE_LEN,
    AlertRow,
    Base,
    Listing,
    PriceSnapshot,
    db_text,
    ensure_utc,
)
from deal_radar.engine.types import Alert, DealItem, DispatchReport, FilterResult, ScoreResult, utcnow

if TYPE_CHECKING:  # pragma: no cover
    from redis.asyncio import Redis

log = get_logger("db")

LISTINGS: Table = Listing.__table__  # type: ignore[assignment]
SNAPSHOTS: Table = PriceSnapshot.__table__  # type: ignore[assignment]
ALERTS: Table = AlertRow.__table__  # type: ignore[assignment]

SUPPORTED_DIALECTS = ("sqlite", "postgresql")

# Applied to every new SQLite connection. busy_timeout first so the journal-mode switch
# itself waits for a concurrent writer instead of failing with SQLITE_BUSY.
SQLITE_PRAGMAS: tuple[str, ...] = (
    "PRAGMA busy_timeout=5000",
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA foreign_keys=ON",
)

# Arbitrary constant key for pg_advisory_xact_lock around schema creation.
_SCHEMA_LOCK_ID = 0x0DEA1_2ADA

# Listing statuses (latest classification of the listing).
STATUS_ACCEPTED = "accepted"  # passed the text filter and no hard scorer gate
STATUS_GATED = "gated"  # passed the text filter, but a scorer hard gate tripped
STATUS_REJECTED = "rejected"  # rejected by the text filter
STATUS_UNCLASSIFIED = "unclassified"  # recorded without a filter result

# Scorer gates that still describe a genuine market price (kept in the price history).
HISTORY_COMPATIBLE_GATES = frozenset({"above_ceiling"})

# Listing upsert merge rules (identical in SQL and in the in-batch Python merge).
_LATEST_FIELDS = ("last_seen", "last_price", "last_score", "title", "url", "condition", "status")
_LATEST_COALESCE_FIELDS = (
    "profile_id",
    "variant_id",
    "product_key",
    "category",
    "retailer",
    "seller_name",
    "location_text",
    "image_url",
)
_FIRST_FIELDS = ("first_seen", "first_price")

HistoryRow = tuple[str, str, str, float, datetime, str]

_T = TypeVar("_T")

# orjson's integer range (signed 64-bit minimum .. unsigned 64-bit maximum).
_JSON_INT_MIN, _JSON_INT_MAX = -(2**63), 2**64 - 1

# SQLSTATE classes that describe the server/connection, not the data: connection
# exception, transaction rollback (deadlock, serialization failure), insufficient
# resources, operator intervention, system error.
_TRANSIENT_SQLSTATE_CLASSES = frozenset({"08", "40", "53", "57", "58"})

# Query-string parameters that carry credentials (redis-py reads ``?password=``).
_SECRET_QUERY_KEYS = frozenset({"password", "pass", "passwd", "pwd", "secret", "token", "auth"})


def json_safe(value: Any) -> Any:
    """Coerce a JSON document into the subset every backend and orjson accept.

    Strings go through :func:`db_text` (no NUL — ``jsonb`` rejects ``\\u0000`` — and no
    lone surrogates), integers beyond 64 bits become strings, non-finite floats become
    ``null``, mapping keys become strings, sequences/sets become lists and anything else
    is stringified. A clean document is returned equal to the input.
    """
    if isinstance(value, str):
        return db_text(value)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value if _JSON_INT_MIN <= value <= _JSON_INT_MAX else str(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {db_text(str(k)): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [json_safe(v) for v in value]
    return db_text(str(value))


def _json_dumps(value: Any) -> str:
    try:
        return orjson.dumps(value, option=orjson.OPT_NON_STR_KEYS).decode("utf-8")
    except TypeError:  # orjson.JSONEncodeError: >64-bit int, lone surrogate, unknown type
        return orjson.dumps(json_safe(value), option=orjson.OPT_NON_STR_KEYS).decode("utf-8")


def _json_loads(value: str | bytes) -> Any:
    return orjson.loads(value)


def _finite(value: float | None) -> float | None:
    """``value`` as a float, or ``None`` when missing/NaN/infinite (SQLite stores NaN as NULL)."""
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def redact_url(url: str | URL) -> str:
    """Render a database/redis URL without its password (userinfo or query string), for logs."""
    if isinstance(url, URL):
        return url.render_as_string(hide_password=True)
    try:
        parts = urlsplit(url)
        password = parts.password
    except ValueError:
        return "<unparseable url>"
    # Splice the redacted pieces into the original string (urlunsplit would turn
    # ``unix:///path`` into ``unix:/path``).
    redacted = url
    if password is not None:
        host = parts.netloc.rsplit("@", 1)[1]
        userinfo = f"{parts.username}:***@" if parts.username else ":***@"
        redacted = redacted.replace(parts.netloc, userinfo + host, 1)
    if parts.query:
        pairs = parse_qsl(parts.query, keep_blank_values=True)
        if any(k.lower() in _SECRET_QUERY_KEYS for k, _ in pairs):
            query = urlencode([(k, "***" if k.lower() in _SECRET_QUERY_KEYS else v) for k, v in pairs], safe="*")
            redacted = redacted.replace(f"?{parts.query}", f"?{query}", 1)
    return redacted


def is_transient_error(exc: BaseException) -> bool:
    """True when a failed write says nothing about the data (outage, lock, deadlock...).

    Used by the :class:`Recorder` to decide between dropping a batch (the database is
    unavailable: retrying record by record would only hammer it) and bisecting it to
    isolate a poison record (constraint/encoding/serialisation errors are per-row).
    """
    if isinstance(exc, (OSError, TimeoutError, sa_exc.TimeoutError, sa_exc.DisconnectionError)):
        return True  # OSError covers ConnectionError/ConnectionRefusedError
    if isinstance(exc, sa_exc.DBAPIError):
        if exc.connection_invalidated or isinstance(exc, (sa_exc.OperationalError, sa_exc.InterfaceError)):
            return True
        sqlstate = getattr(exc.orig, "sqlstate", None)
        return isinstance(sqlstate, str) and sqlstate[:2] in _TRANSIENT_SQLSTATE_CLASSES
    return False


# --------------------------------------------------------------------------- records


@dataclass(slots=True)
class SnapshotRecord:
    """One observation of a listing as the pipeline saw it (accepted or rejected)."""

    item: DealItem
    filter_result: FilterResult | None
    score: ScoreResult | None
    observed_at: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class _AlertRecord:
    alert: Alert
    report: DispatchReport


def classify_record(record: SnapshotRecord) -> tuple[str, bool, str | None]:
    """``(listing status, usable as market observation, reject code)`` for a record."""
    fr, score = record.filter_result, record.score
    if fr is None:
        return STATUS_UNCLASSIFIED, False, None
    if not fr.accepted:
        return STATUS_REJECTED, False, fr.reject_code or "rejected"
    if score is not None and score.rejected:
        reason = score.reject_reason or "gated"
        return STATUS_GATED, reason in HISTORY_COMPATIBLE_GATES and fr.product_key is not None, reason
    return STATUS_ACCEPTED, fr.product_key is not None, None


def _location_text(item: DealItem) -> str | None:
    loc = item.location
    if loc is None:
        return None
    if loc.text:
        return loc.text
    parts = [p for p in (loc.city, loc.region, loc.postal_code, loc.country) if p]
    return ", ".join(parts) or None


def _listing_row(record: SnapshotRecord, observed_at: datetime, status: str) -> dict[str, Any]:
    item, fr, score = record.item, record.filter_result, record.score
    price = float(item.total_price)
    return {
        "id": item.fingerprint,
        "source": db_text(item.source, SOURCE_LEN),
        "source_id": db_text(item.source_id),
        "listing_key": db_text(item.listing_key),
        "url": db_text(item.url),
        "title": db_text(item.title),
        "profile_id": db_text(fr.profile_id, PROFILE_LEN) if fr else None,
        "variant_id": db_text(fr.variant_id, PROFILE_LEN) if fr else None,
        "product_key": db_text(fr.product_key, PRODUCT_KEY_LEN) if fr else None,
        "category": db_text(fr.category, CATEGORY_LEN) if fr else None,
        "condition": db_text(item.condition.value, CONDITION_LEN),
        "retailer": db_text(item.retailer, SHORT_TEXT_LEN),
        "seller_name": db_text(item.seller.name, SHORT_TEXT_LEN) if item.seller else None,
        "location_text": db_text(_location_text(item), SHORT_TEXT_LEN),
        "image_url": db_text(item.primary_image),
        "first_seen": observed_at,
        "last_seen": observed_at,
        "first_price": price,
        "last_price": price,
        "min_price": price,
        "last_score": _finite(score.score) if score is not None else None,
        "status": status,
    }


def _merge_listing_rows(current: dict[str, Any], new: dict[str, Any]) -> None:
    """In-place merge of two rows for the same listing; mirrors the SQL upsert exactly."""
    is_newer = new["last_seen"] >= current["last_seen"]
    is_older = new["first_seen"] < current["first_seen"]
    for name in _LATEST_FIELDS:
        if is_newer:
            current[name] = new[name]
    for name in _LATEST_COALESCE_FIELDS:
        preferred, fallback = (new[name], current[name]) if is_newer else (current[name], new[name])
        current[name] = preferred if preferred is not None else fallback
    if is_older:
        for name in _FIRST_FIELDS:
            current[name] = new[name]
    current["min_price"] = min(current["min_price"], new["min_price"])


def build_snapshot_rows(records: Sequence[SnapshotRecord]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return ``(listing upsert rows (one per listing, primary-key order), snapshot insert rows)``.

    Records whose price is NaN/infinite are skipped with a warning: they are not market
    observations, and SQLite would turn NaN into NULL and fail the NOT NULL constraint
    (aborting the whole batch). Listing rows are sorted by primary key so concurrent
    writers (several processor nodes) always lock listings in the same order and can
    never deadlock each other on PostgreSQL.
    """
    listings: dict[str, dict[str, Any]] = {}
    snapshots: list[dict[str, Any]] = []
    for record in records:
        item, fr, score = record.item, record.filter_result, record.score
        if not (math.isfinite(item.price) and math.isfinite(item.total_price)):
            log.warning(
                "skipping snapshot with a non-finite price",
                extra={"listing_key": item.listing_key, "price": repr(item.price), "total_price": repr(item.total_price)},
            )
            continue
        observed_at = ensure_utc(record.observed_at)
        status, usable, reject_code = classify_record(record)
        row = _listing_row(record, observed_at, status)
        existing = listings.get(row["id"])
        if existing is None:
            listings[row["id"]] = row
        else:
            _merge_listing_rows(existing, row)
        snapshots.append(
            {
                "listing_id": row["id"],
                "product_key": row["product_key"],
                "condition_class": item.condition.market_class,
                "source": row["source"],
                "price": float(item.price),
                "shipping": _finite(item.shipping),
                "total_price": float(item.total_price),
                "observed_at": observed_at,
                "accepted": usable,
                "reject_code": db_text(reject_code, REJECT_CODE_LEN),
                "score": _finite(score.score) if score is not None else None,
                "risk": _finite(score.risk) if score is not None else None,
            }
        )
    return [listings[key] for key in sorted(listings)], snapshots


def build_alert_row(alert: Alert, report: DispatchReport) -> dict[str, Any]:
    """Alert row; every string and JSON document is sanitised (see :func:`json_safe`)."""
    score = alert.score
    return {
        "id": db_text(alert.alert_id),
        "listing_id": alert.item.fingerprint,
        "product_key": db_text(alert.product_key, PRODUCT_KEY_LEN),
        "severity": db_text(alert.severity.value, SEVERITY_LEN),
        "score": float(score.score),
        "price": float(alert.item.total_price),
        "market_price": _finite(score.market_price),
        "discount_pct": _finite(score.discount_pct),
        "is_price_error": score.is_price_error,
        "is_update": alert.is_update,
        "previous_price": _finite(alert.dedup.previous_price),
        "targets": json_safe([r.target for r in report.results]),
        "results": json_safe([r.model_dump(mode="json") for r in report.results]),
        "suppressed_reason": db_text(report.suppressed_reason, SHORT_TEXT_LEN),
        "ok": report.any_ok,
        "created_at": ensure_utc(alert.created_at),
        "pipeline_ms": _finite(alert.pipeline_ms) or 0.0,
        "ingest_lag_ms": _finite(alert.ingest_lag_ms),
        "payload": json_safe(alert.model_dump(mode="json")),
    }


# --------------------------------------------------------------------------- statements


def _dialect_insert(dialect_name: str, table: Table) -> Any:
    if dialect_name == "postgresql":
        return pg_dialect.insert(table)
    if dialect_name == "sqlite":
        return sqlite_dialect.insert(table)
    raise ValueError(f"unsupported database dialect {dialect_name!r}; expected one of {SUPPORTED_DIALECTS}")


def listing_upsert_statement(dialect_name: str) -> Insert:
    """``INSERT INTO listings ... ON CONFLICT (id) DO UPDATE`` with order-independent merging."""
    stmt = _dialect_insert(dialect_name, LISTINGS)
    new, cur = stmt.excluded, LISTINGS.c
    is_newer = new.last_seen >= cur.last_seen
    is_older = new.first_seen < cur.first_seen
    set_: dict[str, ColumnElement[Any]] = {}
    for name in _LATEST_FIELDS:
        set_[name] = case((is_newer, new[name]), else_=cur[name])
    for name in _LATEST_COALESCE_FIELDS:
        set_[name] = case(
            (is_newer, func.coalesce(new[name], cur[name])),
            else_=func.coalesce(cur[name], new[name]),
        )
    for name in _FIRST_FIELDS:
        set_[name] = case((is_older, new[name]), else_=cur[name])
    set_["min_price"] = case((new.min_price < cur.min_price, new.min_price), else_=cur.min_price)
    return stmt.on_conflict_do_update(index_elements=[cur.id], set_=set_)


def alert_upsert_statement(dialect_name: str) -> Insert:
    """Idempotent alert write: re-writing an alert id refreshes its delivery outcome."""
    stmt = _dialect_insert(dialect_name, ALERTS)
    new = stmt.excluded
    set_ = {col.name: new[col.name] for col in ALERTS.c if col.name != "id"}
    return stmt.on_conflict_do_update(index_elements=[ALERTS.c.id], set_=set_)


def price_history_query(since: datetime, max_per_key: int, max_risk: float | None = None) -> Select[Any]:
    """Latest accepted snapshot per listing, capped to ``max_per_key`` most recent per key."""
    s = SNAPSHOTS.c
    conditions: list[ColumnElement[bool]] = [
        s.accepted == true(),
        s.observed_at >= ensure_utc(since),
        s.product_key.is_not(None),
    ]
    if max_risk is not None:
        conditions.append((s.risk.is_(None)) | (s.risk <= max_risk))
    per_listing = (
        select(
            s.listing_id,
            s.product_key,
            s.condition_class,
            s.total_price,
            s.observed_at,
            s.source,
            func.row_number()
            .over(partition_by=s.listing_id, order_by=(s.observed_at.desc(), s.id.desc()))
            .label("rn_listing"),
        )
        .where(*conditions)
        .subquery("per_listing")
    )
    p = per_listing.c
    per_key = (
        select(
            p.listing_id,
            p.product_key,
            p.condition_class,
            p.total_price,
            p.observed_at,
            p.source,
            func.row_number()
            .over(partition_by=(p.product_key, p.condition_class), order_by=(p.observed_at.desc(), p.listing_id))
            .label("rn_key"),
        )
        .where(p.rn_listing == 1)
        .subquery("per_key")
    )
    k = per_key.c
    return (
        select(k.product_key, k.condition_class, LISTINGS.c.listing_key, k.total_price, k.observed_at, k.source)
        .join_from(per_key, LISTINGS, LISTINGS.c.id == k.listing_id)
        .where(k.rn_key <= max_per_key)
        .order_by(k.product_key, k.condition_class, k.observed_at, k.listing_id)
    )


def _apply_sqlite_pragmas(dbapi_connection: Any, connection_record: Any) -> None:
    cursor = dbapi_connection.cursor()
    try:
        for pragma in SQLITE_PRAGMAS:
            cursor.execute(pragma)
    finally:
        cursor.close()


def _sqlite_file_path(url: URL) -> Path | None:
    """Filesystem path of a SQLite URL, or ``None`` for in-memory databases."""
    database = url.database
    if not database or database == ":memory:" or url.query.get("mode") == "memory":
        return None
    if database.startswith("file:"):
        path_part, _, query = database[len("file:"):].partition("?")
        if not path_part or path_part == ":memory:" or "mode=memory" in query:
            return None
        database = path_part
    return Path(database).expanduser()


# --------------------------------------------------------------------------- database


class Database:
    """Async database facade shared by the pipeline, the Recorder and the HTTP API."""

    def __init__(self, url: str, *, echo: bool = False, pool_size: int = 5) -> None:
        self.url = make_url(url)
        self.echo = echo
        self.pool_size = max(1, int(pool_size))
        self.dialect_name = self.url.get_backend_name()
        if self.dialect_name not in SUPPORTED_DIALECTS:
            raise ValueError(f"unsupported database URL {redact_url(self.url)!r}; use sqlite+aiosqlite or postgresql+asyncpg")
        self._engine: AsyncEngine | None = None
        self._listing_upsert = listing_upsert_statement(self.dialect_name)
        self._alert_upsert = alert_upsert_statement(self.dialect_name)
        self._snapshot_insert = insert(SNAPSHOTS)

    # ------------------------------------------------------------------ lifecycle

    @property
    def connected(self) -> bool:
        return self._engine is not None

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError("Database.connect() has not been called")
        return self._engine

    async def connect(self) -> None:
        """Create the engine, apply backend tuning and create missing tables. Idempotent."""
        if self._engine is not None:
            return
        common: dict[str, Any] = {"echo": self.echo, "json_serializer": _json_dumps, "json_deserializer": _json_loads}
        if self.dialect_name == "sqlite":
            path = _sqlite_file_path(self.url)
            if path is not None:
                path.parent.mkdir(parents=True, exist_ok=True)
                # SQLAlchemy's default AsyncAdaptedQueuePool; SQLite has a single writer
                # anyway (busy_timeout queues writers), so no pool sizing is passed.
                engine = create_async_engine(self.url, **common)
            else:
                # An in-memory database lives in exactly one connection. SQLAlchemy's
                # default StaticPool hands that connection to every concurrent checkout,
                # so a reader's reset-on-return ROLLBACK would wipe a Recorder transaction
                # half-way. A one-connection queue pool makes checkouts exclusive instead.
                engine = create_async_engine(
                    self.url, poolclass=AsyncAdaptedQueuePool, pool_size=1, max_overflow=0, pool_recycle=-1, **common
                )
            event.listen(engine.sync_engine, "connect", _apply_sqlite_pragmas)
        else:
            engine = create_async_engine(
                self.url,
                pool_size=self.pool_size,
                max_overflow=max(2, self.pool_size // 2),
                pool_pre_ping=True,
                pool_recycle=1800,
                pool_timeout=30,
                connect_args={"server_settings": {"application_name": "deal_radar"}},
                **common,
            )
        try:
            async with engine.begin() as conn:
                if self.dialect_name == "postgresql":
                    # Serialise concurrent schema creation from several processor nodes.
                    await conn.execute(select(func.pg_advisory_xact_lock(_SCHEMA_LOCK_ID)))
                await conn.run_sync(Base.metadata.create_all)
        except BaseException:
            await engine.dispose()
            raise
        self._engine = engine
        log.info("database connected", extra={"db_url": redact_url(self.url), "dialect": self.dialect_name})

    async def close(self) -> None:
        engine, self._engine = self._engine, None
        if engine is not None:
            await engine.dispose()
            log.info("database closed", extra={"dialect": self.dialect_name})

    async def ping(self) -> bool:
        """Cheap liveness probe for ``/health``; never raises."""
        if self._engine is None:
            return False
        try:
            async with self._engine.connect() as conn:
                await conn.execute(select(1))
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("database ping failed", extra={"error": repr(exc)})
            return False

    # ------------------------------------------------------------------ writes

    async def write_snapshots(self, records: Sequence[SnapshotRecord]) -> None:
        """Upsert the listings and append one snapshot per record, in one transaction."""
        if not records:
            return
        listing_rows, snapshot_rows = build_snapshot_rows(records)
        if not snapshot_rows:  # every record was invalid (non-finite price)
            return
        async with self.engine.begin() as conn:
            await conn.execute(self._listing_upsert, listing_rows)
            await conn.execute(self._snapshot_insert, snapshot_rows)

    async def write_alert(self, alert: Alert, report: DispatchReport) -> None:
        await self.write_alerts([(alert, report)])

    async def write_alerts(self, alerts: Sequence[tuple[Alert, DispatchReport]]) -> None:
        """Persist several alerts in one transaction (idempotent per ``alert_id``)."""
        if not alerts:
            return
        rows: dict[str, dict[str, Any]] = {}
        for alert, report in alerts:
            rows[alert.alert_id] = build_alert_row(alert, report)  # last write per id wins
        async with self.engine.begin() as conn:
            await conn.execute(self._alert_upsert, list(rows.values()))

    # ------------------------------------------------------------------ reads

    async def load_price_history(
        self, since: datetime, *, max_per_key: int = 400, max_risk: float | None = None
    ) -> list[HistoryRow]:
        """Warm-start rows ``(product_key, condition_class, listing_key, total_price, observed_at, source)``.

        Only accepted snapshots observed at/after ``since``; the latest one per listing; at
        most ``max_per_key`` most recent listings per ``(product_key, condition_class)``.
        ``max_risk`` optionally mirrors ``scoring.history_max_risk`` (unscored rows pass).
        Rows are ordered by key, then chronologically.
        """
        if max_per_key < 1:
            return []
        stmt = price_history_query(since, max_per_key, max_risk)
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt)
            return [
                (row.product_key, row.condition_class, row.listing_key, float(row.total_price), row.observed_at, row.source)
                for row in result
            ]

    async def recent_alerts(self, limit: int = 50) -> list[dict[str, Any]]:
        """Newest alerts first, as JSON-ready dicts (``created_at`` is ISO-8601 UTC)."""
        if limit < 1:
            return []
        stmt = select(ALERTS).order_by(ALERTS.c.created_at.desc(), ALERTS.c.id).limit(limit)
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt)
            return [self._alert_dict(row) for row in result.mappings()]

    @staticmethod
    def _alert_dict(row: Mapping[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {"alert_id": row["id"]}
        for key, value in row.items():
            if key == "id":
                continue
            out[key] = value.isoformat() if isinstance(value, datetime) else value
        return out

    async def counts(self) -> dict[str, int]:
        """Row counts per table (for ``/status``)."""
        stmt = select(
            select(func.count()).select_from(LISTINGS).scalar_subquery().label("listings"),
            select(func.count()).select_from(SNAPSHOTS).scalar_subquery().label("snapshots"),
            select(func.count()).select_from(ALERTS).scalar_subquery().label("alerts"),
        )
        async with self.engine.connect() as conn:
            row = (await conn.execute(stmt)).one()
        return {"listings": int(row.listings), "snapshots": int(row.snapshots), "alerts": int(row.alerts)}

    # ------------------------------------------------------------------ maintenance

    async def prune(self, retention_days: int, *, chunk_size: int = 5000) -> int:
        """Delete snapshots, alerts and listings older than ``retention_days``; returns rows deleted.

        Deletes run in ``chunk_size`` id batches, each in its own short transaction, so a
        large prune never holds SQLite's single write lock long enough to stall the Recorder.
        """
        if retention_days < 1:
            raise ValueError("retention_days must be >= 1")
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        cutoff = utcnow() - timedelta(days=retention_days)
        deleted = {
            # Snapshots first so listings are never deleted under live children.
            "snapshots": await self._delete_chunked(SNAPSHOTS, SNAPSHOTS.c.observed_at < cutoff, chunk_size),
            "alerts": await self._delete_chunked(ALERTS, ALERTS.c.created_at < cutoff, chunk_size),
            "listings": await self._delete_chunked(LISTINGS, LISTINGS.c.last_seen < cutoff, chunk_size),
        }
        total = sum(deleted.values())
        log.info("database pruned", extra={"retention_days": retention_days, "deleted": deleted, "cutoff": cutoff.isoformat()})
        return total

    async def _delete_chunked(self, table: Table, condition: ColumnElement[bool], chunk_size: int) -> int:
        id_col = table.c.id
        total = 0
        while True:
            ids = select(id_col).where(condition).limit(chunk_size)
            # ``condition`` is repeated on the outer DELETE: when PostgreSQL has to wait for
            # a row a concurrent writer just refreshed (e.g. an expired listing observed
            # again), it re-evaluates only the outer WHERE on the new row version, so the
            # live row is skipped instead of being deleted (and cascading its snapshots).
            stmt = delete(table).where(id_col.in_(ids), condition)
            async with self.engine.begin() as conn:
                result = await conn.execute(stmt)
            deleted = max(int(result.rowcount or 0), 0)
            total += deleted
            if deleted < chunk_size:
                return total


# --------------------------------------------------------------------------- recorder

_Entry = SnapshotRecord | _AlertRecord


class Recorder:
    """Bounded, batching, fire-and-forget writer that keeps persistence off the hot path."""

    DROP_LOG_INTERVAL_S = 10.0  # rate-limit drop warnings (a full queue drops per record)

    def __init__(
        self,
        db: Database,
        *,
        batch_size: int,
        flush_seconds: float,
        metrics: Metrics | None = None,
        queue_max: int = 50_000,
        retry_delay: float = 0.5,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if flush_seconds <= 0:
            raise ValueError("flush_seconds must be > 0")
        if queue_max < 1:
            raise ValueError("queue_max must be >= 1")
        self.db = db
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self.retry_delay = max(0.0, retry_delay)
        self.metrics = metrics if metrics is not None else Metrics()
        self._queue: asyncio.Queue[_Entry] = asyncio.Queue(maxsize=queue_max)
        self._has_items = asyncio.Event()  # queue went non-empty
        self._flush_now = asyncio.Event()  # batch is full or we are stopping
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._closed = False
        self._last_drop_log: dict[str, float] = {}

        self._m_written = self.metrics.counter("db_records_written_total", "Records persisted by the recorder", ("kind",))
        self._m_dropped = self.metrics.counter(
            "db_records_dropped_total", "Records the recorder dropped (queue full, write errors, shutdown)", ("kind", "reason")
        )
        self._m_failures = self.metrics.counter("db_flush_failures_total", "Failed recorder flush attempts", ("kind",))
        self._m_flush_ms = self.metrics.histogram("db_flush_ms", "Recorder flush latency (ms)", ("kind",))
        self._m_depth = self.metrics.gauge("db_recorder_queue_depth", "Records waiting in the recorder queue")

    # ------------------------------------------------------------------ public API

    @property
    def pending(self) -> int:
        return self._queue.qsize()

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("Recorder was stopped and cannot be restarted")
        if self.running:
            return
        self._task = asyncio.create_task(self._run(), name="deal_radar.recorder")
        self._task.add_done_callback(self._on_task_done)

    async def stop(self, timeout: float | None = 30.0) -> None:
        """Refuse new records and flush everything queued (bounded by ``timeout``)."""
        self._closed = True
        self._stopping = True
        self._has_items.set()
        self._flush_now.set()
        task = self._task
        if task is None or task.done():
            if self._queue.empty():
                return
            # Never started (or crashed): drain on a fresh task with the same loop.
            task = asyncio.create_task(self._run(), name="deal_radar.recorder.drain")
            self._task = task
        try:
            await asyncio.wait_for(task, timeout)
        except TimeoutError:
            leftover = self._discard_queue("shutdown_timeout")
            log.error("recorder stop timed out; dropped queued records", extra={"dropped": leftover, "timeout_s": timeout})
        finally:
            self._m_depth.set(self._queue.qsize())

    def record(self, record: SnapshotRecord) -> None:
        """Queue a snapshot for persistence. Never blocks, never raises."""
        self._enqueue(record, "snapshot")

    def record_alert(self, alert: Alert, report: DispatchReport) -> None:
        """Queue an alert + delivery report for persistence. Never blocks, never raises."""
        self._enqueue(_AlertRecord(alert, report), "alert")

    # ------------------------------------------------------------------ internals

    def _enqueue(self, entry: _Entry, kind: str) -> None:
        if self._closed:
            self._drop(kind, "closed", 1)
            return
        try:
            self._queue.put_nowait(entry)
        except asyncio.QueueFull:
            self._drop(kind, "queue_full", 1)
            return
        self._has_items.set()
        if self._queue.qsize() >= self.batch_size:
            self._flush_now.set()

    def _drop(self, kind: str, reason: str, count: int) -> None:
        if count <= 0:
            return
        self._m_dropped.inc(count, kind=kind, reason=reason)
        now = time.monotonic()
        key = f"{kind}:{reason}"
        if now - self._last_drop_log.get(key, float("-inf")) >= self.DROP_LOG_INTERVAL_S:
            self._last_drop_log[key] = now
            log.warning("recorder dropped records", extra={"kind": kind, "reason": reason, "count": count})

    def _discard_queue(self, reason: str) -> int:
        dropped = 0
        while True:
            try:
                entry = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return dropped
            dropped += 1
            self._drop("alert" if isinstance(entry, _AlertRecord) else "snapshot", reason, 1)

    def _take_batch(self) -> list[_Entry]:
        batch: list[_Entry] = []
        while len(batch) < self.batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    async def _run(self) -> None:
        while True:
            if self._queue.empty():
                if self._stopping:
                    return
                self._has_items.clear()
                await self._has_items.wait()
                continue
            if not self._stopping and self._queue.qsize() < self.batch_size:
                # Accumulate until the batch fills up or the flush window closes.
                self._flush_now.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._flush_now.wait(), self.flush_seconds)
            await self._flush(self._take_batch())

    async def _flush(self, batch: list[_Entry]) -> None:
        snapshots = [e for e in batch if isinstance(e, SnapshotRecord)]
        alerts = [(e.alert, e.report) for e in batch if isinstance(e, _AlertRecord)]
        # Records of this batch that are neither written nor dropped yet. If the task is
        # cancelled mid-flush (stop() timeout) they are counted as "cancelled", so no
        # record ever disappears uncounted (including alerts queued behind snapshots).
        unresolved = {"snapshot": len(snapshots), "alert": len(alerts)}
        try:
            # Snapshots first: an alert's listing row then usually exists when the alert lands.
            if snapshots:
                await self._write("snapshot", snapshots, self.db.write_snapshots, unresolved)
            if alerts:
                await self._write("alert", alerts, self.db.write_alerts, unresolved)
        except asyncio.CancelledError:
            for kind, count in unresolved.items():
                self._drop(kind, "cancelled", count)
            raise
        finally:
            self._m_depth.set(self._queue.qsize())

    async def _write(
        self, kind: str, items: list[_T], writer: Callable[[list[_T]], Awaitable[None]], unresolved: dict[str, int]
    ) -> None:
        """Write ``items``; retry once after ``retry_delay``; then drop or isolate.

        After the second failure a transient error (database unavailable) drops the batch,
        while any other error is assumed to come from one or more poison records and the
        batch is bisected so that only those records are lost.
        """
        error = await self._attempt(kind, items, writer, unresolved)
        if error is None:
            return
        log.warning("recorder flush failed; retrying once", extra={"kind": kind, "records": len(items), "error": repr(error)})
        await asyncio.sleep(self.retry_delay)
        error = await self._attempt(kind, items, writer, unresolved)
        if error is None:
            return
        if len(items) > 1 and not is_transient_error(error):
            await self._isolate(kind, items, writer, unresolved)
            return
        self._fail(kind, len(items), unresolved)
        log.error(
            "recorder flush failed twice; dropping batch",
            extra={"kind": kind, "records": len(items), "error": repr(error)},
            exc_info=error,
        )

    async def _attempt(
        self, kind: str, items: list[_T], writer: Callable[[list[_T]], Awaitable[None]], unresolved: dict[str, int]
    ) -> Exception | None:
        """One write; returns the error instead of raising (``CancelledError`` propagates)."""
        started = time.perf_counter()
        try:
            await writer(items)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - classified by the caller
            self._m_failures.inc(kind=kind)
            return exc
        self._m_flush_ms.observe((time.perf_counter() - started) * 1000.0, kind=kind)
        self._m_written.inc(len(items), kind=kind)
        unresolved[kind] -= len(items)
        return None

    async def _isolate(
        self, kind: str, items: list[_T], writer: Callable[[list[_T]], Awaitable[None]], unresolved: dict[str, int]
    ) -> None:
        """Bisect a batch that keeps failing with a per-record error, dropping only the culprits.

        Costs at most ``2 * len(items)`` extra writes (one bad record: ~``2 * log2(n)``)
        and stops at the first transient error, since a database that went away will not
        accept the remaining halves either.
        """
        half = len(items) // 2
        stack = [items[half:], items[:half]]  # LIFO: first half first, keeps record order
        written = dropped = 0
        last_error: Exception | None = None
        while stack:
            chunk = stack.pop()
            error = await self._attempt(kind, chunk, writer, unresolved)
            if error is None:
                written += len(chunk)
                continue
            last_error = error
            if is_transient_error(error):
                lost = len(chunk) + sum(len(rest) for rest in stack)
                stack.clear()
                self._fail(kind, lost, unresolved)
                dropped += lost
            elif len(chunk) == 1:
                self._fail(kind, 1, unresolved)
                dropped += 1
            else:
                half = len(chunk) // 2
                stack.append(chunk[half:])
                stack.append(chunk[:half])
        extra = {"kind": kind, "records": len(items), "written": written, "dropped": dropped, "error": repr(last_error)}
        if dropped:
            log.error("recorder batch failed twice; isolated and dropped bad records", extra=extra, exc_info=last_error)
        else:
            log.warning("recorder batch failed twice but succeeded when split", extra=extra)

    def _fail(self, kind: str, count: int, unresolved: dict[str, int]) -> None:
        self._drop(kind, "write_error", count)
        unresolved[kind] -= count

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            log.error("recorder task crashed", extra={"error": repr(exc)}, exc_info=exc)


# --------------------------------------------------------------------------- redis


async def connect_redis(url: str, *, max_connections: int, socket_timeout: float) -> "Redis":
    """Create a pooled ``redis.asyncio`` client and verify it with ``PING`` (raises on failure).

    Responses stay ``bytes`` (``decode_responses=False``): the dedup Lua script and the
    stream bus handle raw payloads, and skipping decode saves work on the hot path. TCP
    keepalive is only requested for ``redis://``/``rediss://``: redis-py's Unix-socket
    connection class rejects that option, which would break ``unix://`` URLs (allowed
    by ``storage.redis_url``) on the first command.
    """
    options: dict[str, Any] = {
        "max_connections": max_connections,
        "socket_timeout": socket_timeout,
        "socket_connect_timeout": socket_timeout,
        "health_check_interval": 30,
        "decode_responses": False,
    }
    if urlsplit(url).scheme.lower() in ("redis", "rediss"):
        options["socket_keepalive"] = True
    client: Redis = redis_asyncio.from_url(url, **options)
    try:
        await client.ping()
    except BaseException as exc:
        with contextlib.suppress(Exception):  # never mask the connection error
            await asyncio.shield(client.aclose())
        if not isinstance(exc, asyncio.CancelledError):
            log.error("redis connection failed", extra={"redis_url": redact_url(url), "error": repr(exc)})
        raise
    log.info("redis connected", extra={"redis_url": redact_url(url), "max_connections": max_connections})
    return client


__all__ = [
    "Database",
    "HISTORY_COMPATIBLE_GATES",
    "Recorder",
    "STATUS_ACCEPTED",
    "STATUS_GATED",
    "STATUS_REJECTED",
    "STATUS_UNCLASSIFIED",
    "SnapshotRecord",
    "alert_upsert_statement",
    "build_alert_row",
    "build_snapshot_rows",
    "classify_record",
    "connect_redis",
    "is_transient_error",
    "json_safe",
    "listing_upsert_statement",
    "price_history_query",
    "redact_url",
]
