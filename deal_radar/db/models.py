"""SQLAlchemy 2.x ORM models for DealRadar's persistent store.

The schema is deliberately small and portable: it must run unchanged on
``sqlite+aiosqlite`` (single-node / laptop deployments) and ``postgresql+asyncpg``
(the GCP processor VM). Design decisions:

* **Only portable column types** — ``String``/``Text``/``Float``/``Boolean``/
  ``DateTime(timezone=True)``/``JSON``. Two dialect *variants* are used where they
  are strictly better and transparent to the code: ``JSONB`` on PostgreSQL (indexable,
  compact) and plain ``INTEGER`` for the SQLite surrogate key (SQLite only
  auto-increments ``INTEGER PRIMARY KEY``; PostgreSQL gets ``BIGSERIAL``).
* **UTC everywhere.** :class:`UTCDateTime` normalises every bound datetime to UTC.
  SQLite has no timezone-aware type, so values are stored there as naive UTC in
  SQLAlchemy's fixed-width ``YYYY-MM-DD HH:MM:SS.ffffff`` format (lexical order ==
  chronological order, so ``>=``/``ORDER BY`` and the upsert's "is this observation
  newer" comparisons work on the raw text) and are re-attached to ``timezone.utc``
  on load. PostgreSQL uses ``TIMESTAMP WITH TIME ZONE``. Callers therefore always get
  timezone-aware UTC datetimes back, on both backends.
* **Hostile text is neutralised, not fatal.** Every string the writer binds goes through
  :func:`db_text` (NUL removal, lone-surrogate repair, clipping to the column width), so
  one malformed scraped listing cannot abort a batch transaction on either backend.
* **Listings are keyed by** :attr:`DealItem.fingerprint` (20 hex chars of
  ``sha1(listing_key)``), so the hot-path writer can compute the primary key without
  a lookup and upsert with ``ON CONFLICT (id)``. ``listing_key`` stays unique and is
  ``TEXT`` because upstream ids have no length guarantee.
* **Snapshots are append-only** price observations. Indexes are chosen for the two
  real access paths — warm-starting the price history
  (``product_key, condition_class, observed_at``) and "latest observation per listing"
  (``listing_id, observed_at``) — plus ``observed_at`` for retention pruning. The
  leading column of each composite index also serves single-column lookups on
  ``product_key`` / ``listing_id``, so no redundant single-column indexes are created
  (every extra index slows the write path).
* **Alerts are an audit log**: ``alerts.listing_id`` intentionally has no foreign key
  so an alert can be persisted even when the listing snapshot was not recorded
  (e.g. ``record_rejected: false`` or a dropped snapshot batch).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Dialect,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import TypeDecorator

# --------------------------------------------------------------------------- column helpers

# Column widths. VARCHAR limits are enforced by PostgreSQL (not SQLite), so every value
# bound to a bounded column is clipped to these lengths by the writer (see ``db_text``).
ID_LEN = 40
SOURCE_LEN = 64
PROFILE_LEN = 128
PRODUCT_KEY_LEN = 300
CATEGORY_LEN = 64
CONDITION_LEN = 32
CLASS_LEN = 16
STATUS_LEN = 16
SHORT_TEXT_LEN = 255
REJECT_CODE_LEN = 64
SEVERITY_LEN = 16


def ensure_utc(value: datetime) -> datetime:
    """Return ``value`` as a timezone-aware UTC datetime (naive values are assumed UTC)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def clip(value: str | None, length: int) -> str | None:
    """Truncate ``value`` to ``length`` characters (``None`` passes through)."""
    if value is None:
        return None
    return value if len(value) <= length else value[:length]


def db_text(value: str | None, length: int | None = None) -> str | None:
    """Make ``value`` storable on every backend, then optionally ``clip`` it to ``length``.

    Scraped text is hostile: PostgreSQL rejects NUL characters in ``text``/``varchar``
    (and ``\\u0000`` in ``jsonb``), and no driver can UTF-8-encode a lone UTF-16 surrogate
    (e.g. an emoji cut in half by a JSON feed). Either would abort the whole batch
    transaction, so NULs are removed and surrogates are re-paired where possible and
    otherwise replaced by U+FFFD. Clean strings (the norm) cost two C-level scans.
    """
    if value is None:
        return None
    if "\x00" in value:
        value = value.replace("\x00", "")
    if not value.isascii():
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            value = value.encode("utf-16", "surrogatepass").decode("utf-16", "replace")
    return value if length is None else clip(value, length)


class UTCDateTime(TypeDecorator[datetime]):
    """``DateTime(timezone=True)`` that always round-trips timezone-aware UTC.

    PostgreSQL stores ``timestamptz`` natively. SQLite stores naive UTC text; the
    timezone is stripped on the way in (after converting to UTC) and re-attached on the
    way out, so results never depend on the backend.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if not isinstance(value, datetime):
            raise TypeError(f"UTCDateTime expects datetime, got {type(value).__name__}")
        value = ensure_utc(value)
        if dialect.name == "sqlite":
            return value.replace(tzinfo=None)
        return value

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        return ensure_utc(value)


# Surrogate key: BIGSERIAL on PostgreSQL, INTEGER PRIMARY KEY (rowid alias) on SQLite.
BigIntPK = BigInteger().with_variant(Integer(), "sqlite")
# JSON documents: JSONB on PostgreSQL, JSON (text) elsewhere.
JSONDoc = JSON().with_variant(JSONB(), "postgresql")

NAMING_CONVENTION: dict[str, str] = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Declarative base with a deterministic constraint naming convention (migration friendly)."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


# --------------------------------------------------------------------------- tables


class Listing(Base):
    """One row per distinct listing (``source:source_id``), updated on every observation."""

    __tablename__ = "listings"

    id: Mapped[str] = mapped_column(String(ID_LEN), primary_key=True)  # DealItem.fingerprint
    source: Mapped[str] = mapped_column(String(SOURCE_LEN))
    source_id: Mapped[str] = mapped_column(Text)
    listing_key: Mapped[str] = mapped_column(Text, unique=True)
    url: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text)
    profile_id: Mapped[str | None] = mapped_column(String(PROFILE_LEN), nullable=True)
    variant_id: Mapped[str | None] = mapped_column(String(PROFILE_LEN), nullable=True)
    product_key: Mapped[str | None] = mapped_column(String(PRODUCT_KEY_LEN), nullable=True, index=True)
    category: Mapped[str | None] = mapped_column(String(CATEGORY_LEN), nullable=True)
    condition: Mapped[str] = mapped_column(String(CONDITION_LEN))
    retailer: Mapped[str | None] = mapped_column(String(SHORT_TEXT_LEN), nullable=True)
    seller_name: Mapped[str | None] = mapped_column(String(SHORT_TEXT_LEN), nullable=True)
    location_text: Mapped[str | None] = mapped_column(String(SHORT_TEXT_LEN), nullable=True)
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_seen: Mapped[datetime] = mapped_column(UTCDateTime())
    last_seen: Mapped[datetime] = mapped_column(UTCDateTime(), index=True)
    first_price: Mapped[float] = mapped_column(Float)
    last_price: Mapped[float] = mapped_column(Float)
    min_price: Mapped[float] = mapped_column(Float)
    last_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    # accepted | gated (scorer hard gate) | rejected (text filter) | unclassified (no filter result)
    status: Mapped[str] = mapped_column(String(STATUS_LEN))

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Listing(id={self.id!r}, key={self.listing_key!r}, last_price={self.last_price!r}, status={self.status!r})"


class PriceSnapshot(Base):
    """Append-only price observation of a listing (accepted or rejected)."""

    __tablename__ = "price_snapshots"
    __table_args__ = (
        Index("ix_price_snapshots_key_class_observed", "product_key", "condition_class", "observed_at"),
        Index("ix_price_snapshots_listing_observed", "listing_id", "observed_at"),
    )

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    listing_id: Mapped[str] = mapped_column(String(ID_LEN), ForeignKey("listings.id", ondelete="CASCADE"))
    product_key: Mapped[str | None] = mapped_column(String(PRODUCT_KEY_LEN), nullable=True)
    condition_class: Mapped[str] = mapped_column(String(CLASS_LEN))  # Condition.market_class
    source: Mapped[str] = mapped_column(String(SOURCE_LEN))
    price: Mapped[float] = mapped_column(Float)
    shipping: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_price: Mapped[float] = mapped_column(Float)
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime(), index=True)
    accepted: Mapped[bool] = mapped_column(Boolean)
    reject_code: Mapped[str | None] = mapped_column(String(REJECT_CODE_LEN), nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    risk: Mapped[float | None] = mapped_column(Float, nullable=True)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"PriceSnapshot(listing_id={self.listing_id!r}, total={self.total_price!r}, at={self.observed_at!r})"


class AlertRow(Base):
    """A dispatched (or attempted) alert with its delivery outcome and full JSON payload."""

    __tablename__ = "alerts"

    # Alert.alert_id: a uuid4 hex by default, but the contract puts no length limit on it.
    id: Mapped[str] = mapped_column(Text, primary_key=True)
    listing_id: Mapped[str] = mapped_column(String(ID_LEN), index=True)
    product_key: Mapped[str] = mapped_column(String(PRODUCT_KEY_LEN), index=True)
    severity: Mapped[str] = mapped_column(String(SEVERITY_LEN))
    score: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    market_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    discount_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_price_error: Mapped[bool] = mapped_column(Boolean, default=False)
    is_update: Mapped[bool] = mapped_column(Boolean, default=False)
    previous_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    targets: Mapped[list[str]] = mapped_column(JSONDoc)  # dispatch targets attempted
    results: Mapped[list[dict[str, Any]]] = mapped_column(JSONDoc)  # DispatchResult dumps
    suppressed_reason: Mapped[str | None] = mapped_column(String(SHORT_TEXT_LEN), nullable=True)
    ok: Mapped[bool] = mapped_column(Boolean)  # at least one target delivered
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), index=True)
    pipeline_ms: Mapped[float] = mapped_column(Float, default=0.0)
    ingest_lag_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONDoc)  # Alert.model_dump(mode="json")

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"AlertRow(id={self.id!r}, product_key={self.product_key!r}, severity={self.severity!r}, ok={self.ok!r})"


__all__ = [
    "AlertRow",
    "Base",
    "Listing",
    "PriceSnapshot",
    "UTCDateTime",
    "clip",
    "db_text",
    "ensure_utc",
]
