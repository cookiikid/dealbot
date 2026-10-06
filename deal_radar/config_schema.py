"""Pydantic v2 schema for ``config.yaml`` — the single source of truth for DealRadar.

Fail-fast philosophy:

* every model forbids unknown keys, so a typo (``poll_intervall``) is an error, not a
  silently ignored setting;
* every regular expression is compiled at load time and reported with its YAML path;
* cross-references (route targets, profile ids in sources, bus/redis coupling) are
  validated in :class:`AppConfig`'s model validator;
* secrets are ``SecretStr`` and are injected from the environment via
  ``${VAR}`` / ``${VAR:-default}`` / ``${VAR:?message}`` interpolation, so the YAML
  can be committed without credentials.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any, Literal, Union
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)

from deal_radar.engine.types import Condition, Severity, SourceKind

# --------------------------------------------------------------------------- helpers


def _empty_to_none(value: Any) -> Any:
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def _csv_list(value: Any) -> Any:
    """Allow ``roles: ${APP_ROLES:-collector,processor}`` style env-driven lists."""
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return value


OptSecret = Annotated[SecretStr | None, BeforeValidator(_empty_to_none)]
OptStr = Annotated[str | None, BeforeValidator(_empty_to_none)]
OptFloat = Annotated[float | None, BeforeValidator(_empty_to_none)]
OptInt = Annotated[int | None, BeforeValidator(_empty_to_none)]

_SLUG = re.compile(r"^[a-z0-9][a-z0-9_]*$")
_HHMM = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


def _compile_all(patterns: list[str], where: str) -> list[str]:
    for pattern in patterns:
        try:
            re.compile(pattern, re.IGNORECASE)
        except re.error as exc:
            raise ValueError(f"invalid regex in {where}: {pattern!r}: {exc}") from exc
    return patterns


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_default=True, populate_by_name=True)


# --------------------------------------------------------------------------- app / infra


class AppSection(Strict):
    name: str = "dealradar"
    node_id: str = "node-1"
    roles: Annotated[list[Literal["collector", "processor"]], BeforeValidator(_csv_list)] = Field(
        default_factory=lambda: ["collector", "processor"]
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_json: bool = True
    timezone: str = "UTC"
    dry_run: bool = False  # route alerts to the console only

    @field_validator("timezone")
    @classmethod
    def _tz(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"unknown timezone {value!r}") from exc
        return value

    @field_validator("roles")
    @classmethod
    def _roles(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("at least one role is required")
        return sorted(set(value))


class HttpServerSection(Strict):
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = Field(default=8080, ge=1, le=65535)
    auth_token: OptSecret = None  # required for /status, /alerts and /ws when set


class StorageSection(Strict):
    database_url: str = "sqlite+aiosqlite:///data/dealradar.db"
    database_echo: bool = False
    database_pool_size: int = Field(default=5, ge=1, le=100)
    redis_url: OptStr = None  # None => single-node in-memory mode
    redis_key_prefix: str = "dr:"
    redis_max_connections: int = Field(default=50, ge=2, le=10_000)
    redis_socket_timeout_seconds: float = Field(default=2.0, gt=0)
    snapshot_batch_size: int = Field(default=200, ge=1, le=10_000)
    snapshot_flush_seconds: float = Field(default=2.0, gt=0)
    record_rejected: bool = True
    retention_days: int = Field(default=120, ge=1)

    @field_validator("database_url")
    @classmethod
    def _db(cls, value: str) -> str:
        if not (value.startswith("sqlite+aiosqlite://") or value.startswith("postgresql+asyncpg://")):
            raise ValueError("database_url must use sqlite+aiosqlite:// or postgresql+asyncpg://")
        return value

    @field_validator("redis_url")
    @classmethod
    def _redis(cls, value: str | None) -> str | None:
        if value is not None and not value.startswith(("redis://", "rediss://", "unix://")):
            raise ValueError("redis_url must start with redis://, rediss:// or unix://")
        return value


class BusSection(Strict):
    backend: Literal["memory", "redis"] = "memory"
    stream_key: str = "stream:raw"
    group: str = "processors"
    maxlen: int = Field(default=100_000, ge=1000)
    block_ms: int = Field(default=1000, ge=10, le=60_000)
    batch_size: int = Field(default=64, ge=1, le=10_000)
    claim_idle_ms: int = Field(default=60_000, ge=1000)
    queue_maxsize: int = Field(default=10_000, ge=10)
    workers: int = Field(default=8, ge=1, le=256)


class RetrySection(Strict):
    max_attempts: int = Field(default=4, ge=1, le=20)
    base_delay_seconds: float = Field(default=0.4, ge=0)
    max_delay_seconds: float = Field(default=15.0, ge=0)
    max_total_seconds: float = Field(default=30.0, gt=0)


class HostLimitSection(Strict):
    rate_per_second: float = Field(gt=0)
    burst: float = Field(default=1.0, ge=1)


class NetworkSection(Strict):
    timeout_seconds: float = Field(default=10.0, gt=0)
    connect_timeout_seconds: float = Field(default=3.0, gt=0)
    max_connections: int = Field(default=200, ge=1)
    max_connections_per_host: int = Field(default=10, ge=1)
    dns_cache_ttl_seconds: int = Field(default=300, ge=0)
    keepalive_seconds: float = Field(default=60.0, ge=0)
    trust_env: bool = True  # honour HTTP(S)_PROXY / NO_PROXY
    proxy: OptStr = None
    ca_bundle: OptStr = None
    chrome_major_version: OptInt = None  # None => estimated from the release train
    accept_language: str = "en-US,en;q=0.9"
    identity_rotation_minutes: float = Field(default=45.0, gt=0)
    retry: RetrySection = Field(default_factory=RetrySection)
    default_host_limit: HostLimitSection | None = None
    host_limits: dict[str, HostLimitSection] = Field(default_factory=dict)


# --------------------------------------------------------------------------- sources


class SourceCommon(Strict):
    enabled: bool = False
    poll_interval_seconds: float = Field(default=30.0, gt=0)
    jitter_pct: float = Field(default=0.15, ge=0, lt=0.9)
    poll_timeout_seconds: float = Field(default=60.0, gt=0)
    max_consecutive_failures: int = Field(default=6, ge=1)
    cooldown_seconds: float = Field(default=300.0, gt=0)
    reliability: float = Field(default=0.9, ge=0, le=1)  # c_src in the scoring model
    nodes: Annotated[list[str], BeforeValidator(_csv_list)] = Field(default_factory=list)  # restrict to node ids (empty = any)
    profiles: list[str] = Field(default_factory=list)  # restrict searched profiles (empty = all)
    max_item_age_minutes: OptFloat = None
    lease_ttl_seconds: OptFloat = None  # enable Redis active/passive fail-over


class EbayQuery(Strict):
    """One explicit, coalesced Browse search.

    eBay's ``q`` supports OR groups — ``rtx (5090, 4090, 3090)`` means "rtx" AND any of
    the three — so a single call can cover several profiles. Every result still goes
    through the text filter, which assigns the real profile. Fewer calls per poll means
    a shorter quota-derived interval (see docs/ARCHITECTURE.md §2.2).
    """

    q: str = Field(min_length=1, max_length=100)
    category_id: OptStr = None
    price_min: OptFloat = Field(default=None, ge=0)
    price_max: OptFloat = Field(default=None, gt=0)
    condition_ids: list[int] = Field(default_factory=list)
    profile_hint: OptStr = None


class EbaySource(SourceCommon):
    reliability: float = Field(default=0.95, ge=0, le=1)
    poll_interval_seconds: float = Field(default=60.0, gt=0)
    client_id: OptSecret = None
    client_secret: OptSecret = None
    environment: Literal["production", "sandbox"] = "production"
    marketplace_id: str = "EBAY_US"
    delivery_country: str = "US"
    delivery_postal_code: OptStr = None
    item_location_country: OptStr = "US"
    affiliate_campaign_id: OptStr = None
    daily_call_budget: int = Field(default=5000, ge=1)  # Browse API default quota
    budget_safety_factor: float = Field(default=0.85, gt=0, le=1)
    limit: int = Field(default=100, ge=1, le=200)
    buying_options: list[Literal["FIXED_PRICE", "BEST_OFFER", "AUCTION"]] = Field(
        default_factory=lambda: ["FIXED_PRICE", "BEST_OFFER"]
    )
    max_concurrency: int = Field(default=4, ge=1, le=32)
    queries: list[EbayQuery] = Field(default_factory=list)  # non-empty => replaces per-profile term searches

    @model_validator(mode="after")
    def _creds(self) -> "EbaySource":
        if self.enabled and (self.client_id is None or self.client_secret is None):
            raise ValueError("sources.ebay.enabled requires client_id and client_secret (EBAY_CLIENT_ID / EBAY_CLIENT_SECRET)")
        return self


class SubredditSpec(Strict):
    name: str
    mode: Literal["deals", "swap"] = "deals"  # parse style: r/buildapcsales vs r/hardwareswap
    limit: int = Field(default=25, ge=1, le=100)

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        value = value.strip().removeprefix("r/").removeprefix("/r/")
        if not re.fullmatch(r"[A-Za-z0-9_]{2,21}", value):
            raise ValueError(f"invalid subreddit name {value!r}")
        return value


class RedditSource(SourceCommon):
    poll_interval_seconds: float = Field(default=6.0, gt=0)
    reliability: float = Field(default=0.9, ge=0, le=1)
    max_item_age_minutes: OptFloat = 240.0
    client_id: OptSecret = None  # OAuth app (recommended: 100 QPM, not IP-blocked)
    client_secret: OptSecret = None
    username: OptStr = None  # used in the User-Agent "(by /u/<username>)"
    user_agent: OptStr = None  # full override
    subreddits: list[SubredditSpec] = Field(
        default_factory=lambda: [SubredditSpec(name="buildapcsales"), SubredditSpec(name="hardwareswap", mode="swap")]
    )
    hardwareswap_locations: list[str] = Field(default_factory=list)  # e.g. ["USA-TX", "USA-CA"]; empty = all
    skip_flairs: list[str] = Field(default_factory=lambda: ["Expired", "Closed", "Buying", "BUYING", "CLOSED"])

    @model_validator(mode="after")
    def _creds(self) -> "RedditSource":
        # Unauthenticated .json has been blocked since 2026-05-28 (verified); enabling the
        # source without OAuth credentials can only produce a permanently blocked poller.
        if self.enabled and self.client_id is None:
            raise ValueError("sources.reddit.enabled requires OAuth credentials (REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET)")
        return self


class FeedSpec(Strict):
    name: str
    url: str
    # Per-feed cadence: e.g. the Hot Deals forum every ~30 s, frontpage/popular every few
    # minutes. None => every poll of the source.
    interval_seconds: OptFloat = Field(default=None, gt=0)


class SlickdealsSource(SourceCommon):
    poll_interval_seconds: float = Field(default=45.0, gt=0)
    reliability: float = Field(default=0.88, ge=0, le=1)
    max_item_age_minutes: OptFloat = 360.0
    feeds: list[FeedSpec] = Field(default_factory=list)
    search_feeds_from_profiles: bool = True
    search_feed_template: str = (
        "https://slickdeals.net/newsearch.php?mode=frontpage&searcharea=deals&searchin=first&rss=1&q={query}"
    )


class FieldMap(Strict):
    """Dotted JSON paths (``a.b[0].c``, ``items[*]`` handled by ``items_path``)."""

    id: str
    title: str
    price: str
    list_price: OptStr = None
    in_stock: OptStr = None
    url: OptStr = None
    image: OptStr = None
    condition: OptStr = None


class _EndpointCommon(Strict):
    name: str
    retailer: str
    currency: str = "USD"  # Shopify/generic JSON rarely state their currency
    profile_hint: OptStr = None
    poll_interval_seconds: OptFloat = None  # per-endpoint override
    cache_bust: bool = False
    cache_bust_param: str = "_"
    conditional: bool = True
    browser_identity: bool = True
    headers: dict[str, str] = Field(default_factory=dict)


class BestBuyEndpoint(_EndpointCommon):
    adapter: Literal["bestbuy"] = "bestbuy"
    retailer: str = "Best Buy"
    api_key: OptSecret = None
    skus: list[str] = Field(default_factory=list)
    batch_size: int = Field(default=100, ge=1, le=100)
    browser_identity: bool = False


class ShopifyEndpoint(_EndpointCommon):
    adapter: Literal["shopify"] = "shopify"
    store_url: str
    handles: list[str] = Field(default_factory=list)  # poll specific products
    collection: OptStr = None  # or poll a whole collection (products.json)
    max_pages: int = Field(default=2, ge=1, le=20)


class TargetEndpoint(_EndpointCommon):
    adapter: Literal["target_redsky"] = "target_redsky"
    retailer: str = "Target"
    api_key: OptSecret = None  # RedSky web key
    tcins: list[str] = Field(default_factory=list)
    store_id: OptStr = None
    zip_code: OptStr = None


class NeweggEndpoint(_EndpointCommon):
    adapter: Literal["newegg"] = "newegg"
    retailer: str = "Newegg"
    item_numbers: list[str] = Field(default_factory=list)


class GenericJsonEndpoint(_EndpointCommon):
    adapter: Literal["json"] = "json"
    url: str
    method: Literal["GET", "POST"] = "GET"
    params: dict[str, str] = Field(default_factory=dict)
    body: dict[str, Any] | None = None
    items_path: str = ""  # dotted path to the list of products ("" = root is the list)
    fields: FieldMap
    url_template: OptStr = None  # e.g. "https://store.example/p/{id}" when url field absent
    price_divisor: float = Field(default=1.0, gt=0)  # 100 for APIs that return cents
    condition: Condition = Condition.NEW


RetailEndpoint = Annotated[
    Union[BestBuyEndpoint, ShopifyEndpoint, TargetEndpoint, NeweggEndpoint, GenericJsonEndpoint],
    Field(discriminator="adapter"),
]


class RetailSource(SourceCommon):
    poll_interval_seconds: float = Field(default=20.0, gt=0)
    reliability: float = Field(default=1.0, ge=0, le=1)
    endpoints: list[RetailEndpoint] = Field(default_factory=list)
    max_concurrency: int = Field(default=6, ge=1, le=64)

    @model_validator(mode="after")
    def _unique(self) -> "RetailSource":
        names = [e.name for e in self.endpoints]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"duplicate retail endpoint names: {sorted(dupes)}")
        for e in self.endpoints:
            if self.enabled and isinstance(e, BestBuyEndpoint) and e.api_key is None:
                raise ValueError(f"retail endpoint {e.name!r} (bestbuy) requires api_key (BESTBUY_API_KEY)")
            if self.enabled and isinstance(e, TargetEndpoint) and e.api_key is None:
                raise ValueError(f"retail endpoint {e.name!r} (target_redsky) requires api_key")
        return self


class GeoPin(Strict):
    """Where a browser-based marketplace session pretends to be (must match the IP!)."""

    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    city_slug: OptStr = None  # facebook.com/marketplace/<city_slug>/search ; numeric location ids work too
    radius_km: int = Field(default=40, ge=1, le=805)
    timezone_id: str = "America/New_York"
    locale: str = "en-US"
    postal_code: OptStr = None

    @field_validator("timezone_id")
    @classmethod
    def _tz(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"unknown timezone {value!r}") from exc
        return value


class BrowserSection(Strict):
    headless: bool = True
    storage_state_path: str = "data/fb_storage_state.json"
    user_data_dir: OptStr = None  # persistent profile dir (alternative to storage_state)
    executable_path: OptStr = None
    proxy: OptStr = None  # residential proxy if not running on a home connection
    viewport_width: int = Field(default=1366, ge=800)
    viewport_height: int = Field(default=900, ge=600)
    navigation_timeout_seconds: float = Field(default=45.0, gt=0)
    min_action_delay_seconds: float = Field(default=1.5, ge=0)
    max_action_delay_seconds: float = Field(default=4.5, ge=0)

    @model_validator(mode="after")
    def _delays(self) -> "BrowserSection":
        if self.max_action_delay_seconds < self.min_action_delay_seconds:
            raise ValueError("max_action_delay_seconds must be >= min_action_delay_seconds")
        return self


class FbMarketplaceSource(SourceCommon):
    poll_interval_seconds: float = Field(default=240.0, gt=0)
    # Searches are deliberately slow and sequential (≈10-15 s each plus human pauses);
    # the ingestor rotates a cursor across polls when a sweep does not fit.
    poll_timeout_seconds: float = Field(default=600.0, gt=0)
    reliability: float = Field(default=0.7, ge=0, le=1)
    cooldown_seconds: float = Field(default=1800.0, gt=0)
    max_item_age_minutes: OptFloat = 1440.0
    location: GeoPin | None = None
    browser: BrowserSection = Field(default_factory=BrowserSection)
    days_since_listed: Literal[1, 7, 30] = 1
    max_listings_per_query: int = Field(default=40, ge=1, le=200)
    scrolls_per_query: int = Field(default=1, ge=0, le=10)
    checkpoint_pause_minutes: float = Field(default=180.0, gt=0)

    @model_validator(mode="after")
    def _loc(self) -> "FbMarketplaceSource":
        if self.enabled and self.location is None:
            raise ValueError("sources.fb_marketplace.enabled requires a location block")
        return self


class OfferUpSource(SourceCommon):
    poll_interval_seconds: float = Field(default=180.0, gt=0)
    poll_timeout_seconds: float = Field(default=120.0, gt=0)  # ~20 terms at the 0.3 rps host budget
    reliability: float = Field(default=0.7, ge=0, le=1)
    max_item_age_minutes: OptFloat = 1440.0
    latitude: OptFloat = None
    longitude: OptFloat = None
    radius_miles: int = Field(default=30, ge=1, le=500)
    zip_code: OptStr = None
    max_listings_per_query: int = Field(default=50, ge=1, le=200)

    @model_validator(mode="after")
    def _loc(self) -> "OfferUpSource":
        if self.enabled and (self.latitude is None or self.longitude is None) and self.zip_code is None:
            raise ValueError("sources.offerup.enabled requires latitude/longitude or zip_code")
        return self


class CraigslistSource(SourceCommon):
    poll_interval_seconds: float = Field(default=300.0, gt=0)
    poll_timeout_seconds: float = Field(default=120.0, gt=0)
    reliability: float = Field(default=0.65, ge=0, le=1)
    max_item_age_minutes: OptFloat = 1440.0
    sites: list[str] = Field(default_factory=list)  # e.g. ["sfbay", "losangeles"]
    category: str = "sss"  # for sale - all
    postal_code: OptStr = None
    search_distance_miles: OptInt = None

    @model_validator(mode="after")
    def _sites(self) -> "CraigslistSource":
        if self.enabled and not self.sites:
            raise ValueError("sources.craigslist.enabled requires at least one site")
        return self


class SourcesSection(Strict):
    ebay: EbaySource = Field(default_factory=EbaySource)
    reddit: RedditSource = Field(default_factory=RedditSource)
    slickdeals: SlickdealsSource = Field(default_factory=SlickdealsSource)
    retail: RetailSource = Field(default_factory=RetailSource)
    fb_marketplace: FbMarketplaceSource = Field(default_factory=FbMarketplaceSource)
    offerup: OfferUpSource = Field(default_factory=OfferUpSource)
    craigslist: CraigslistSource = Field(default_factory=CraigslistSource)

    def items(self) -> list[tuple[str, SourceCommon]]:
        return [(name, getattr(self, name)) for name in type(self).model_fields]


# --------------------------------------------------------------------------- filters


class RuleGroup(Strict):
    """A named family of regexes.

    ``reject`` groups hard-reject on any match; ``risk`` groups add a
    :class:`RiskSignal` with ``probability``. ``negatable`` groups are evaluated on text
    where negated phrases ("no cracks", "never mined") have been masked first.
    """

    action: Literal["reject", "risk"]
    patterns: list[str]
    probability: float = Field(default=0.5, ge=0, le=1)
    negatable: bool = False
    field: Literal["title", "text"] = "text"
    categories: list[str] = Field(default_factory=list)  # empty = all product categories
    source_kinds: list[SourceKind] = Field(default_factory=list)  # empty = all source kinds
    description: str = ""

    @model_validator(mode="after")
    def _patterns(self) -> "RuleGroup":
        if not self.patterns:
            raise ValueError("rule group needs at least one pattern")
        _compile_all(self.patterns, "filters.rules")
        return self


class FiltersSection(Strict):
    negation_terms: list[str] = Field(
        default_factory=lambda: ["no", "not", "never", "zero", "without", "nothing", "isn't", "wasn't", "free of", "none"]
    )
    negation_window_words: int = Field(default=3, ge=0, le=6)
    min_title_length: int = Field(default=6, ge=0)
    rules: dict[str, RuleGroup] = Field(default_factory=dict)
    allowed_currencies: list[str] = Field(default_factory=lambda: ["USD"])

    @field_validator("rules")
    @classmethod
    def _names(cls, value: dict[str, RuleGroup]) -> dict[str, RuleGroup]:
        for name in value:
            if not _SLUG.match(name):
                raise ValueError(f"rule group name {name!r} must be a lowercase slug")
        return value


# --------------------------------------------------------------------------- scoring


class ScoringWeights(Strict):
    discount: float = Field(default=0.45, ge=0, le=1)
    statistical: float = Field(default=0.30, ge=0, le=1)
    target: float = Field(default=0.25, ge=0, le=1)

    @model_validator(mode="after")
    def _sum(self) -> "ScoringWeights":
        total = self.discount + self.statistical + self.target
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"scoring weights must sum to 1.0 (got {total:.4f})")
        return self


class SeverityThresholds(Strict):
    critical: float = Field(default=85, ge=0, le=100)
    high: float = Field(default=70, ge=0, le=100)
    medium: float = Field(default=55, ge=0, le=100)

    @model_validator(mode="after")
    def _order(self) -> "SeverityThresholds":
        if not (self.medium <= self.high <= self.critical):
            raise ValueError("severity thresholds must satisfy medium <= high <= critical")
        return self


def _default_risk_probabilities() -> dict[str, float]:
    return {
        "bait_price": 0.95,  # P below the profile floor
        "placeholder_price": 0.6,  # $1, $1234, $9999...
        "extreme_discount_low_trust": 0.5,  # >55% off on a low-reliability source
        "title_price_mismatch": 0.45,  # title advertises a very different price
        "low_feedback": 0.35,  # seller feedback count below threshold
        "poor_feedback_pct": 0.3,  # seller positive feedback % below threshold
        "no_image": 0.15,  # local listing without a photo
        "vision_unverified": 0.05,  # vision required but unavailable
        "vision_box_only": 0.95,
        "vision_parts_only": 0.95,
        "vision_damaged": 0.9,
        "vision_unrelated": 0.8,
        "vision_screenshot": 0.7,
        "vision_receipt": 0.7,
        "vision_stock_photo": 0.35,
    }


class ScoringSection(Strict):
    weights: ScoringWeights = Field(default_factory=ScoringWeights)
    discount_saturation: float = Field(default=0.45, gt=0, le=1)  # Δ at which D saturates to 1
    z_low: float = Field(default=1.0, ge=0)
    z_high: float = Field(default=4.0, gt=0)
    confidence_floor: float = Field(default=0.4, ge=0, le=1)  # λ
    risk_exponent: float = Field(default=2.0, ge=1, le=6)  # γ
    reject_risk: float = Field(default=0.85, gt=0, le=1)
    min_samples_for_stats: int = Field(default=6, ge=2)
    reference_prior_strength: float = Field(default=8.0, gt=0)  # n0
    history_half_life_days: float = Field(default=14.0, gt=0)
    history_window_days: float = Field(default=60.0, gt=0)
    history_max_samples: int = Field(default=400, ge=10, le=100_000)
    history_max_risk: float = Field(default=0.5, ge=0, le=1)  # items riskier than this never enter history
    vision_trust_boost: float = Field(default=0.5, ge=0, le=1)  # closes this share of (1 - c_src) on GENUINE
    price_error_discount: float = Field(default=0.5, gt=0, lt=1)
    price_error_min_reliability: float = Field(default=0.9, ge=0, le=1)
    price_error_max_risk: float = Field(default=0.3, ge=0, le=1)
    extreme_discount_threshold: float = Field(default=0.55, gt=0, lt=1)
    low_trust_reliability: float = Field(default=0.8, ge=0, le=1)
    min_seller_feedback: int = Field(default=5, ge=0)
    min_seller_feedback_pct: float = Field(default=97.0, ge=0, le=100)
    # Values sellers type when they do not want to show a price ("$1, DM me", "$1234").
    # Deliberately excludes real price points such as 99 / 100 / 999 / 1999.
    placeholder_prices: list[float] = Field(
        default_factory=lambda: [0.01, 1, 2, 11, 111, 123, 1111, 1234, 9999, 11111, 12345, 99999, 123456]
    )
    severity: SeverityThresholds = Field(default_factory=SeverityThresholds)
    source_history_weights: dict[str, float] = Field(default_factory=dict)
    risk_probabilities: dict[str, float] = Field(default_factory=_default_risk_probabilities)

    @model_validator(mode="after")
    def _check(self) -> "ScoringSection":
        if self.z_high <= self.z_low:
            raise ValueError("scoring.z_high must be greater than scoring.z_low")
        merged = _default_risk_probabilities()
        for key, value in self.risk_probabilities.items():
            if not 0 <= value <= 1:
                raise ValueError(f"scoring.risk_probabilities.{key} must be within [0, 1]")
            merged[key] = value
        self.risk_probabilities = merged
        for key, value in self.source_history_weights.items():
            if value < 0:
                raise ValueError(f"scoring.source_history_weights.{key} must be >= 0")
        return self


# --------------------------------------------------------------------------- vision / dedup


class VisionSection(Strict):
    enabled: bool = False
    backend: Literal["ollama", "openai"] = "ollama"
    base_url: str = "http://localhost:11434"
    model: str = "qwen3-vl:4b-instruct"
    escalation_model: OptStr = None  # bigger model for UNCERTAIN verdicts (two-stage cascade)
    api_key: OptSecret = None  # OpenAI-compatible servers (vLLM/LM Studio) that require one
    timeout_seconds: float = Field(default=8.0, gt=0)
    max_concurrency: int = Field(default=2, ge=1, le=16)
    max_images: int = Field(default=2, ge=1, le=6)
    max_image_bytes: int = Field(default=8_000_000, ge=10_000)
    resize_max_side: int = Field(default=512, ge=224, le=2048)
    jpeg_quality: int = Field(default=85, ge=40, le=100)
    keep_alive: str = "30m"
    num_predict: int = Field(default=160, ge=16, le=2048)
    apply_to_kinds: list[SourceKind] = Field(default_factory=lambda: [SourceKind.LOCAL])
    apply_to_sources: list[str] = Field(default_factory=list)  # extra source names beyond kinds
    min_prelim_score: float = Field(default=40.0, ge=0, le=100)
    negative_min_confidence: float = Field(default=0.6, ge=0, le=1)
    on_error: Literal["allow", "reject"] = "allow"
    cache_ttl_seconds: int = Field(default=86_400, ge=0)

    @field_validator("base_url")
    @classmethod
    def _url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("vision.base_url must be an http(s) URL")
        return value.rstrip("/")


class DedupSection(Strict):
    listing_ttl_hours: float = Field(default=72.0, gt=0)
    cluster_ttl_hours: float = Field(default=12.0, gt=0)
    min_drop_pct: float = Field(default=0.05, ge=0, lt=1)
    min_drop_abs: float = Field(default=10.0, ge=0)
    price_bucket_pct: float = Field(default=0.02, gt=0, lt=0.5)
    cross_source: bool = True


# --------------------------------------------------------------------------- dispatch


class DiscordTarget(Strict):
    webhook_url: OptSecret = None  # unset => target disabled (skipped with a warning)
    username: str = "DealRadar"
    avatar_url: OptStr = None
    mention_role_id: OptStr = None  # role pinged for alerts on routes with mention: true
    thread_id: OptStr = None
    link_buttons: bool = True

    @property
    def configured(self) -> bool:
        return self.webhook_url is not None


class TelegramChat(Strict):
    chat_id: OptStr = None  # unset => target disabled
    message_thread_id: OptInt = None
    silent_below: Severity = Severity.HIGH  # disable_notification for lower severities
    send_photos: bool = True

    @property
    def configured(self) -> bool:
        return self.chat_id is not None


class DiscordSection(Strict):
    webhooks: dict[str, DiscordTarget] = Field(default_factory=dict)


class TelegramSection(Strict):
    bot_token: OptSecret = None
    api_base: str = "https://api.telegram.org"
    chats: dict[str, TelegramChat] = Field(default_factory=dict)


class WebSocketSection(Strict):
    enabled: bool = True
    path: str = "/ws"
    max_clients: int = Field(default=50, ge=1)
    recent_buffer: int = Field(default=100, ge=0, le=10_000)


class QuietHours(Strict):
    start: str  # "23:00" local (app.timezone)
    end: str  # "07:00"
    min_severity: Severity = Severity.CRITICAL  # only these get through during quiet hours

    @field_validator("start", "end")
    @classmethod
    def _hhmm(cls, value: str) -> str:
        if not _HHMM.match(value):
            raise ValueError(f"expected HH:MM, got {value!r}")
        return value


class RouteRule(Strict):
    name: str
    min_severity: Severity = Severity.MEDIUM
    profiles: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    price_error_only: bool = False
    targets: list[str]
    mention: bool = False  # ping mention_role_id / bypass silent mode
    max_per_minute: OptInt = None  # flood guard for this route (critical alerts always pass)
    quiet_hours: QuietHours | None = None

    @field_validator("targets")
    @classmethod
    def _targets(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("route needs at least one target")
        for target in value:
            if target in ("console", "websocket"):
                continue
            kind, _, name = target.partition(":")
            if kind not in ("discord", "telegram") or not name:
                raise ValueError(f"invalid target {target!r}; use discord:<name>, telegram:<name>, websocket or console")
        return value


class DispatchSection(Strict):
    timeout_seconds: float = Field(default=6.0, gt=0)
    discord: DiscordSection = Field(default_factory=DiscordSection)
    telegram: TelegramSection = Field(default_factory=TelegramSection)
    websocket: WebSocketSection = Field(default_factory=WebSocketSection)
    routes: list[RouteRule] = Field(default_factory=list)
    system_targets: list[str] = Field(default_factory=lambda: ["console"])  # operator notices


# --------------------------------------------------------------------------- profiles


class MatchRules(Strict):
    field: Literal["title", "text"] = "title"  # match on the title only, or title + description
    any: list[str] = Field(default_factory=list)  # at least one must match (if non-empty)
    all: list[str] = Field(default_factory=list)  # every one must match
    none: list[str] = Field(default_factory=list)  # none may match

    @model_validator(mode="after")
    def _compile(self) -> "MatchRules":
        if not self.any and not self.all:
            raise ValueError("match needs at least one 'any' or 'all' pattern")
        for group in ("any", "all", "none"):
            _compile_all(getattr(self, group), f"profile.match.{group}")
        return self


class PriceBandOverride(Strict):
    reference_new: OptFloat = Field(default=None, gt=0)
    reference_used: OptFloat = Field(default=None, gt=0)
    reference_refurb: OptFloat = Field(default=None, gt=0)
    floor: OptFloat = Field(default=None, gt=0)
    target: OptFloat = Field(default=None, gt=0)
    ceiling: OptFloat = Field(default=None, gt=0)


class PriceBand(Strict):
    """Static anchors. ``floor`` < price is bait, ``target`` is a great deal, ``ceiling`` is the max ever alerted."""

    currency: str = "USD"
    reference_new: OptFloat = Field(default=None, gt=0)
    reference_used: OptFloat = Field(default=None, gt=0)
    reference_refurb: OptFloat = Field(default=None, gt=0)
    floor: float = Field(gt=0)
    target: float = Field(gt=0)
    ceiling: float = Field(gt=0)

    @model_validator(mode="after")
    def _order(self) -> "PriceBand":
        if not (self.floor < self.target <= self.ceiling):
            raise ValueError(f"price band must satisfy floor < target <= ceiling (got {self.floor}, {self.target}, {self.ceiling})")
        if self.reference_new is None and self.reference_used is None and self.reference_refurb is None:
            raise ValueError("price band needs at least one reference price (reference_new / reference_used)")
        return self

    def reference_for(self, market_class: str) -> float | None:
        """Configured reference for a condition class, falling back sensibly."""
        if market_class == "new":
            return self.reference_new or (self.reference_refurb / 0.9 if self.reference_refurb else None) or (
                self.reference_used / 0.8 if self.reference_used else None
            )
        if market_class == "refurb":
            if self.reference_refurb:
                return self.reference_refurb
            if self.reference_new and self.reference_used:
                return (self.reference_new + self.reference_used) / 2
            return (self.reference_new * 0.9 if self.reference_new else None) or self.reference_used
        return self.reference_used or (self.reference_refurb * 0.9 if self.reference_refurb else None) or (
            self.reference_new * 0.8 if self.reference_new else None
        )

    def merged(self, override: PriceBandOverride | None) -> "PriceBand":
        if override is None:
            return self
        data = self.model_dump()
        data.update({k: v for k, v in override.model_dump().items() if v is not None})
        return PriceBand.model_validate(data)


class Variant(Strict):
    id: str
    match: list[str]  # any of these must match for the variant to apply
    field: Literal["title", "text"] = "title"
    price: PriceBandOverride | None = None

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        if not _SLUG.match(value):
            raise ValueError(f"variant id {value!r} must be a lowercase slug")
        return value

    @field_validator("match")
    @classmethod
    def _match(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("variant needs at least one match pattern")
        return _compile_all(value, "profile.variants.match")


class ProfileSearch(Strict):
    terms: list[str] = Field(default_factory=list)  # free-text queries for search-capable sources
    sources: list[str] = Field(default_factory=list)  # restrict to these sources (empty = all)
    ebay_category_ids: list[str] = Field(default_factory=list)
    ebay_condition_ids: list[int] = Field(default_factory=list)
    price_min: OptFloat = None  # default: price.floor
    price_max: OptFloat = None  # default: price.ceiling


class Profile(Strict):
    id: str
    name: str
    category: str
    enabled: bool = True
    priority: int = 0  # higher wins when several profiles match the same listing
    match: MatchRules
    variants: list[Variant] = Field(default_factory=list)
    variant_required: bool = False  # reject listings whose variant (size, capacity) is unknown
    price: PriceBand
    conditions: list[Condition] = Field(
        default_factory=lambda: [Condition.NEW, Condition.OPEN_BOX, Condition.REFURBISHED, Condition.USED, Condition.UNKNOWN]
    )
    min_score: OptFloat = Field(default=None, ge=0, le=100)
    search: ProfileSearch = Field(default_factory=ProfileSearch)
    vision: Literal["auto", "required", "off"] = "auto"
    vision_hint: OptStr = None  # e.g. "a desktop graphics card with fans and a PCB"
    tags: list[str] = Field(default_factory=list)

    @field_validator("id", "category")
    @classmethod
    def _slug(cls, value: str) -> str:
        if not _SLUG.match(value):
            raise ValueError(f"{value!r} must be a lowercase slug ([a-z0-9_])")
        return value

    @model_validator(mode="after")
    def _variants(self) -> "Profile":
        ids = [v.id for v in self.variants]
        if len(ids) != len(set(ids)):
            raise ValueError(f"profile {self.id}: duplicate variant ids")
        if self.variant_required and not self.variants:
            raise ValueError(f"profile {self.id}: variant_required needs variants")
        for v in self.variants:
            self.price.merged(v.price)  # validates the merged band (floor < target <= ceiling)
        return self

    def band_for(self, variant_id: str | None) -> PriceBand:
        if variant_id:
            for v in self.variants:
                if v.id == variant_id:
                    return self.price.merged(v.price)
        return self.price


# --------------------------------------------------------------------------- root


class AppConfig(Strict):
    version: Literal[1] = 1
    app: AppSection = Field(default_factory=AppSection)
    http_server: HttpServerSection = Field(default_factory=HttpServerSection)
    storage: StorageSection = Field(default_factory=StorageSection)
    bus: BusSection = Field(default_factory=BusSection)
    network: NetworkSection = Field(default_factory=NetworkSection)
    sources: SourcesSection = Field(default_factory=SourcesSection)
    filters: FiltersSection = Field(default_factory=FiltersSection)
    scoring: ScoringSection = Field(default_factory=ScoringSection)
    vision: VisionSection = Field(default_factory=VisionSection)
    dedup: DedupSection = Field(default_factory=DedupSection)
    dispatch: DispatchSection = Field(default_factory=DispatchSection)
    profiles: list[Profile] = Field(default_factory=list)

    @model_validator(mode="after")
    def _cross_refs(self) -> "AppConfig":
        ids = [p.id for p in self.profiles]
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        if dupes:
            raise ValueError(f"duplicate profile ids: {dupes}")
        known = set(ids)
        source_names = {name for name, _ in self.sources.items()}
        for name, src in self.sources.items():
            unknown = [p for p in src.profiles if p not in known]
            if unknown:
                raise ValueError(f"sources.{name}.profiles references unknown profile(s) {unknown}")
        for profile in self.profiles:
            bad = [s for s in profile.search.sources if s not in source_names]
            if bad:
                raise ValueError(f"profile {profile.id}: search.sources has unknown source(s) {bad}")
        if self.bus.backend == "redis" and not self.storage.redis_url:
            raise ValueError("bus.backend=redis requires storage.redis_url")
        if "collector" not in self.app.roles and "processor" in self.app.roles and self.bus.backend != "redis":
            raise ValueError("a processor-only node needs bus.backend=redis to receive listings")
        if "processor" not in self.app.roles and self.bus.backend != "redis":
            raise ValueError("a collector-only node needs bus.backend=redis to publish listings")
        targets = self.known_targets()
        for route in self.dispatch.routes:
            for target in route.targets:
                if target not in targets:
                    raise ValueError(f"route {route.name!r} references undefined target {target!r}")
            unknown = [p for p in route.profiles if p not in known]
            if unknown:
                raise ValueError(f"route {route.name!r} references unknown profile(s) {unknown}")
            bad = [s for s in route.sources if s not in source_names]
            if bad:
                raise ValueError(f"route {route.name!r} references unknown source(s) {bad}")
        for target in self.dispatch.system_targets:
            if target not in targets:
                raise ValueError(f"dispatch.system_targets references undefined target {target!r}")
        for key in self.scoring.source_history_weights:
            if key not in source_names:
                raise ValueError(f"scoring.source_history_weights has unknown source {key!r}")
        return self

    def known_targets(self) -> set[str]:
        targets = {"console"}
        if self.dispatch.websocket.enabled:
            targets.add("websocket")
        targets.update(f"discord:{name}" for name in self.dispatch.discord.webhooks)
        targets.update(f"telegram:{name}" for name in self.dispatch.telegram.chats)
        return targets

    def profile(self, profile_id: str) -> Profile:
        for p in self.profiles:
            if p.id == profile_id:
                return p
        raise KeyError(profile_id)

    def enabled_profiles(self) -> list[Profile]:
        return [p for p in self.profiles if p.enabled]

    def source_enabled_here(self, name: str) -> bool:
        src = getattr(self.sources, name)
        return bool(src.enabled) and "collector" in self.app.roles and (not src.nodes or self.app.node_id in src.nodes)


# --------------------------------------------------------------------------- loading

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?:(:-|:\?)([^}]*))?\}")


class ConfigError(Exception):
    """Raised for unreadable files, bad YAML, missing env vars or schema violations."""


def interpolate_env(value: Any, env: Mapping[str, str], path: str = "") -> Any:
    """Recursively expand ``${VAR}``, ``${VAR:-default}`` and ``${VAR:?error}`` in strings.

    A string that consists of exactly one placeholder becomes the raw env value (so
    YAML scalars like ``enabled: ${EBAY_ENABLED:-false}`` are then coerced by pydantic).
    """
    if isinstance(value, dict):
        return {k: interpolate_env(v, env, f"{path}.{k}" if path else str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [interpolate_env(v, env, f"{path}[{i}]") for i, v in enumerate(value)]
    if not isinstance(value, str) or "${" not in value:
        return value

    def _sub(match: re.Match[str]) -> str:
        name, op, arg = match.group(1), match.group(2), match.group(3)
        current = env.get(name)
        if current not in (None, ""):
            return current  # type: ignore[return-value]
        if op == ":-":
            return arg or ""
        if op == ":?":
            raise ConfigError(f"{path}: required environment variable {name} is not set ({arg or 'no message'})")
        if current is None:
            raise ConfigError(f"{path}: environment variable {name} is not set (use ${{{name}:-default}} to make it optional)")
        return ""

    return _ENV_PATTERN.sub(_sub, value)


def load_dotenv(path: str | Path, env: dict[str, str] | None = None, *, override: bool = False) -> dict[str, str]:
    """Minimal ``.env`` reader (KEY=VALUE, optional quotes, ``#`` comments)."""
    target = env if env is not None else os.environ
    p = Path(path)
    if not p.is_file():
        return dict(target)
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        if override or key not in target:
            target[key] = value
    return dict(target)


def load_config(path: str | Path, *, env: Mapping[str, str] | None = None) -> AppConfig:
    """Read, interpolate and validate a config file. Raises :class:`ConfigError`."""
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read config {p}: {exc}") from exc
    try:
        data = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {p}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{p}: top level must be a mapping")
    data = interpolate_env(data, env if env is not None else os.environ)
    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(format_validation_error(exc, p)) from exc


def format_validation_error(exc: ValidationError, path: str | Path = "config") -> str:
    lines = [f"{path}: {exc.error_count()} configuration error(s):"]
    for err in exc.errors():
        loc = ".".join(str(part) for part in err["loc"])
        lines.append(f"  - {loc or '<root>'}: {err['msg']}")
    return "\n".join(lines)


__all__ = [
    "AppConfig",
    "BestBuyEndpoint",
    "BrowserSection",
    "ConfigError",
    "CraigslistSource",
    "DedupSection",
    "DiscordTarget",
    "DispatchSection",
    "EbayQuery",
    "EbaySource",
    "FbMarketplaceSource",
    "FieldMap",
    "FiltersSection",
    "GenericJsonEndpoint",
    "GeoPin",
    "MatchRules",
    "NeweggEndpoint",
    "OfferUpSource",
    "PriceBand",
    "Profile",
    "ProfileSearch",
    "RedditSource",
    "RetailSource",
    "RouteRule",
    "RuleGroup",
    "ScoringSection",
    "ShopifyEndpoint",
    "SlickdealsSource",
    "SourceCommon",
    "StorageSection",
    "TargetEndpoint",
    "TelegramChat",
    "Variant",
    "VisionSection",
    "interpolate_env",
    "load_config",
    "load_dotenv",
]
