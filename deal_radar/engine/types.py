"""Shared data contracts that flow through the DealRadar pipeline.

Every stage of the system communicates through the models defined here:

    source  --RawListing-->  normalizer  --DealItem-->  text_filter  --FilterResult-->
    anomaly (+ vision)  --ScoreResult/VisionResult-->  dedup  --DedupDecision-->
    router  --Alert-->  dispatchers  --DispatchResult-->

Keeping the contracts in one dependency-free module lets ingestors, the engine and
the dispatchers evolve independently and lets collector nodes serialise
``RawListing`` objects onto the Redis stream without importing engine code.
"""

from __future__ import annotations

import enum
import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def utcnow() -> datetime:
    """Timezone-aware UTC ``now`` used for every timestamp in the system."""
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- enums


class SourceKind(str, enum.Enum):
    """Coarse class of a source; drives default trust, condition and vision policy."""

    RETAIL = "retail"  # first-party retailer price/inventory endpoints (Best Buy, Shopify...)
    MARKETPLACE = "marketplace"  # structured national marketplaces with seller data (eBay)
    AGGREGATOR = "aggregator"  # community deal feeds (Slickdeals, r/buildapcsales)
    LOCAL = "local"  # peer-to-peer local listings (FB Marketplace, OfferUp, Craigslist, r/hardwareswap)


class Condition(str, enum.Enum):
    NEW = "new"
    OPEN_BOX = "open_box"
    REFURBISHED = "refurbished"
    USED = "used"
    FOR_PARTS = "for_parts"
    UNKNOWN = "unknown"

    @property
    def market_class(self) -> str:
        """Bucket used to keep separate price histories per condition class."""
        if self is Condition.NEW:
            return "new"
        if self in (Condition.OPEN_BOX, Condition.REFURBISHED):
            return "refurb"
        if self is Condition.FOR_PARTS:
            return "parts"
        return "used"


class Severity(str, enum.Enum):
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return _SEVERITY_RANK[self]

    def at_least(self, other: "Severity") -> bool:
        return self.rank >= other.rank


_SEVERITY_RANK = {Severity.MEDIUM: 1, Severity.HIGH: 2, Severity.CRITICAL: 3}


class VisionVerdict(str, enum.Enum):
    GENUINE = "genuine"  # the expected item is visibly present and intact
    BOX_ONLY = "box_only"  # packaging without the product
    PARTS_ONLY = "parts_only"  # shroud/cooler without PCB, panel without electronics, etc.
    DAMAGED = "damaged"  # cracked screen, burnt connector, snapped PCB...
    SCREENSHOT = "screenshot"  # photo of a screen / screenshot of a listing or spec sheet
    RECEIPT = "receipt"  # receipt / invoice / document instead of the item
    STOCK_PHOTO = "stock_photo"  # manufacturer marketing render, no proof of possession
    UNRELATED = "unrelated"  # picture does not show the advertised product class
    UNCERTAIN = "uncertain"  # model could not decide
    SKIPPED = "skipped"  # vision not applicable / not run
    ERROR = "error"  # backend failure

    @property
    def is_negative(self) -> bool:
        return self in _NEGATIVE_VERDICTS


_NEGATIVE_VERDICTS = frozenset(
    {
        VisionVerdict.BOX_ONLY,
        VisionVerdict.PARTS_ONLY,
        VisionVerdict.DAMAGED,
        VisionVerdict.SCREENSHOT,
        VisionVerdict.RECEIPT,
        VisionVerdict.STOCK_PHOTO,
        VisionVerdict.UNRELATED,
    }
)


class DedupStatus(str, enum.Enum):
    NEW = "new"  # first time this listing is alerted
    PRICE_DROP = "price_drop"  # already alerted, price fell enough to re-alert
    DUPLICATE = "duplicate"  # already alerted at this (or a lower) price
    CROSS_SOURCE_DUPLICATE = "cross_source_duplicate"  # same deal already alerted via another source


# --------------------------------------------------------------------------- sub-models


class SellerInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    feedback_score: int | None = None  # eBay feedback count, Reddit trade count, ...
    feedback_pct: float | None = None  # 0-100
    account_age_days: int | None = None
    is_business: bool | None = None


class Location(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str | None = None  # free-form "Austin, TX"
    city: str | None = None
    region: str | None = None  # state / province
    postal_code: str | None = None
    country: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    distance_miles: float | None = None


# --------------------------------------------------------------------------- listings


class RawListing(BaseModel):
    """Lightly-structured listing exactly as an ingestor observed it.

    Prices may still be strings ("$1,199 OBO"); the normalizer owns parsing so that
    every source shares one battle-tested price/condition parser.
    """

    model_config = ConfigDict(extra="forbid")

    source: str  # ingestor name, e.g. "ebay", "reddit", "fb_marketplace"
    source_kind: SourceKind
    source_id: str  # stable id within the source (itemId, t3_ fullname, sku...)
    url: str
    title: str
    description: str = ""
    price: float | str | None = None
    currency: str = "USD"
    shipping: float | str | None = None  # None = unknown, 0 = free
    list_price: float | str | None = None  # "was"/MSRP/strike-through price if the source exposes it
    condition: str | None = None  # raw condition text or id ("Used", "1000", "Open-Box Excellent")
    seller: SellerInfo | None = None
    location: Location | None = None
    image_urls: list[str] = Field(default_factory=list)
    posted_at: datetime | None = None  # when the seller/retailer published it (if known)
    in_stock: bool | None = None
    quantity: int | None = None
    retailer: str | None = None  # store name for retail/aggregator deals ("Best Buy")
    sku: str | None = None
    outbound_url: str | None = None  # for aggregators: link to the actual store page
    query: str | None = None  # search term that surfaced the listing
    profile_hint: str | None = None  # profile id the query was issued for (a hint, never trusted)
    extra: dict[str, Any] = Field(default_factory=dict)
    received_at: datetime = Field(default_factory=utcnow)  # collector receive time
    node_id: str | None = None  # collector node that observed it

    @property
    def listing_key(self) -> str:
        return f"{self.source}:{self.source_id}"


class DealItem(BaseModel):
    """Canonical, fully-typed listing used by every engine stage."""

    model_config = ConfigDict(extra="forbid")

    source: str
    source_kind: SourceKind
    source_id: str
    url: str
    title: str
    description: str = ""
    price: float  # item price in ``currency`` (rounded to cents)
    currency: str = "USD"
    shipping: float | None = None
    total_price: float  # price + shipping (shipping treated as 0 when unknown)
    list_price: float | None = None
    condition: Condition = Condition.UNKNOWN
    seller: SellerInfo | None = None
    location: Location | None = None
    image_urls: list[str] = Field(default_factory=list)
    posted_at: datetime | None = None
    in_stock: bool | None = None
    quantity: int | None = None
    retailer: str | None = None
    sku: str | None = None
    outbound_url: str | None = None
    query: str | None = None
    profile_hint: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)
    received_at: datetime = Field(default_factory=utcnow)
    normalized_at: datetime = Field(default_factory=utcnow)
    node_id: str | None = None

    @property
    def listing_key(self) -> str:
        return f"{self.source}:{self.source_id}"

    @property
    def fingerprint(self) -> str:
        """Stable 20-hex-char id of the listing (used as DB primary key)."""
        return hashlib.sha1(self.listing_key.encode("utf-8")).hexdigest()[:20]

    @property
    def primary_image(self) -> str | None:
        return self.image_urls[0] if self.image_urls else None

    @property
    def text(self) -> str:
        """Title + description, the haystack for text rules."""
        if self.description:
            return f"{self.title}\n{self.description}"
        return self.title

    @property
    def best_url(self) -> str:
        """Where a human should click: the store page when known, else the listing."""
        return self.outbound_url or self.url


# --------------------------------------------------------------------------- stage results


class RiskSignal(BaseModel):
    """Independent piece of scam/quality evidence, combined by noisy-OR in the scorer."""

    model_config = ConfigDict(extra="forbid")

    code: str  # machine code, e.g. "payment_red_flag", "bait_price", "vision_box_only"
    probability: float = Field(ge=0.0, le=1.0)
    detail: str = ""
    origin: str = "text"  # text | price | seller | listing | vision


class FilterResult(BaseModel):
    """Outcome of deterministic text classification for one item."""

    model_config = ConfigDict(extra="forbid")

    accepted: bool
    profile_id: str | None = None
    variant_id: str | None = None
    category: str | None = None
    match_strength: float = 0.0  # 0..1, how unambiguous the product match is
    reject_code: str | None = None  # e.g. "box_only", "rental", "no_profile_match"
    reject_detail: str = ""  # the matched text that triggered the rejection
    risk_signals: list[RiskSignal] = Field(default_factory=list)
    matched_terms: list[str] = Field(default_factory=list)
    elapsed_us: float = 0.0

    @property
    def product_key(self) -> str | None:
        if not self.profile_id:
            return None
        return f"{self.profile_id}:{self.variant_id}" if self.variant_id else self.profile_id


class VisionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    verdict: VisionVerdict
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    model: str | None = None
    latency_ms: float = 0.0
    images_checked: int = 0
    cached: bool = False
    details: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None

    @property
    def is_negative(self) -> bool:
        return self.verdict.is_negative


class ScoreResult(BaseModel):
    """Full, explainable anomaly score for one item."""

    model_config = ConfigDict(extra="forbid")

    score: float = Field(ge=0.0, le=100.0)
    severity: Severity | None = None  # None => below alerting threshold
    rejected: bool = False  # hard gate tripped (ceiling, risk >= reject_risk...)
    reject_reason: str | None = None
    market_price: float | None = None
    market_basis: str = "none"  # "history" | "config" | "blend" | "none"
    discount_pct: float | None = None  # (M - P) / M
    robust_z: float | None = None
    iqr_position: float | None = None
    history_n: int = 0
    confidence: float = 0.0
    risk: float = 0.0
    components: dict[str, float] = Field(default_factory=dict)
    risk_signals: list[RiskSignal] = Field(default_factory=list)
    is_price_error: bool = False
    explain: list[str] = Field(default_factory=list)


class DedupDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: DedupStatus
    previous_price: float | None = None
    keys: list[str] = Field(default_factory=list)
    rollback: dict[str, Any] = Field(default_factory=dict)  # opaque state to undo a failed claim

    @property
    def should_alert(self) -> bool:
        return self.status in (DedupStatus.NEW, DedupStatus.PRICE_DROP)


class Alert(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alert_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    item: DealItem
    profile_id: str
    profile_name: str
    variant_id: str | None = None
    category: str
    severity: Severity
    score: ScoreResult
    vision: VisionResult | None = None
    dedup: DedupDecision
    created_at: datetime = Field(default_factory=utcnow)
    pipeline_ms: float = 0.0  # internal processing time (excludes source + dispatch network)
    ingest_lag_ms: float | None = None  # posted_at -> alert creation, when posted_at is known
    mention: bool = False  # set by the router for routes that ping humans

    @property
    def is_update(self) -> bool:
        return self.dedup.status is DedupStatus.PRICE_DROP

    @property
    def product_key(self) -> str:
        return f"{self.profile_id}:{self.variant_id}" if self.variant_id else self.profile_id


class DispatchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target: str  # "discord:gpu", "telegram:main", "websocket", "console"
    ok: bool
    status: int | None = None
    latency_ms: float = 0.0
    attempts: int = 1
    message_id: str | None = None
    error: str | None = None


class DispatchReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alert_id: str
    results: list[DispatchResult] = Field(default_factory=list)
    suppressed_reason: str | None = None  # route-level suppression (quiet hours, flood guard)

    @property
    def any_ok(self) -> bool:
        return any(r.ok for r in self.results)

    @property
    def all_failed(self) -> bool:
        return bool(self.results) and not self.any_ok


__all__ = [
    "Alert",
    "Condition",
    "DealItem",
    "DedupDecision",
    "DedupStatus",
    "DispatchReport",
    "DispatchResult",
    "FilterResult",
    "Location",
    "RawListing",
    "RiskSignal",
    "ScoreResult",
    "SellerInfo",
    "Severity",
    "SourceKind",
    "VisionResult",
    "VisionVerdict",
    "utcnow",
]
