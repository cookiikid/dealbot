"""eBay ingestor built on the official Browse API (OAuth2 application token, REST/JSON).

Why poll the Browse API
-----------------------
* eBay decommissioned the Finding API (including ``findCompletedItems``) and the
  Shopping API on 2025-02-04. ``GET /buy/browse/v1/item_summary/search`` replaced them
  and is the only open, supported way to read active listings.
* eBay has no push channel for "a new listing matches my search". The Notification
  API's ``LISTING`` topic covers only the caller's *own* listings.
  ``ITEM_PRICE_REVISION``/``ITEM_AVAILABILITY`` are eBay Partner Network topics for
  items you already know. The Feed API is Limited Release and runs 2-48 h behind.
  Polling ``sort=newlyListed`` (newest ``itemOriginDate`` first) therefore gives the
  lowest latency.

One poll issues one request per (profile, search term) pair::

    GET {api}/buy/browse/v1/item_summary/search
        ?q=<term>&category_ids=<ids>&sort=newlyListed&limit=<cfg.limit>
        &fieldgroups=MATCHING_ITEMS,EXTENDED            (adds shortDescription + itemLocation.city)
        &filter=price:[lo..hi],priceCurrency:USD,buyingOptions:{FIXED_PRICE|BEST_OFFER},
                conditionIds:{1000|3000},itemLocationCountry:US,deliveryCountry:US
    Authorization: Bearer <application token>
    X-EBAY-C-MARKETPLACE-ID: EBAY_US
    X-EBAY-C-ENDUSERCTX: contextualLocation=country%3DUS%2Czip%3D19406,affiliateCampaignId=<ePN id>

In ``filter``, commas separate filters and ``|`` separates values inside ``{}``. Both
belong to eBay's filter grammar, so the whole value is percent-encoded as one query
parameter. The price range is widened to cover every variant band of the profile, so
a 3090 Ti listing is not cut off by the base 3090 ceiling. The pages are newest-first
with ``limit`` up to 200, so one page per query covers the gap between polls. When a
page is full and even its oldest item is newer than the previous poll,
``ebay_page_saturated_total`` flags that ``limit`` should go up.

Quota math
----------
The default Browse quota is **5,000 calls/day per application keyset**. Every node
using the keyset shares it, and so does ``getItem``. It resets at midnight
America/Los_Angeles (observed empirically). With ``Q`` = (profile, term) pairs::

    calls_per_poll         = Q
    allowed_polls_per_day  = daily_call_budget * budget_safety_factor / Q
    interval               = max(poll_interval_seconds, 86_400 / allowed_polls_per_day)  ± jitter_pct

Example: the shipped config has 21 (profile, term) pairs, so 5000 x 0.85 / 21 = 202
polls/day, or one poll every ~427 s. Faster detection on eBay needs fewer, broader
queries (eBay's ``(a, b)`` OR-syntax in ``q``) or a quota increase through eBay's
free Application Growth Check.

A daily **ledger** (kept in Redis when ``storage.redis_url`` is set, so every node and
restart counts against the same total) checks the plan against what was actually
spent. The interval is never shorter than
``seconds_until_reset * Q / (budget * safety - used)``. Once the safety budget is
used up the source sleeps until the reset. A hard guard never starts a poll that
would exceed ``daily_call_budget``. Retries are capped at 2 per request (eBay's
Growth Check rule for infrastructure errors).

HTTP 429 with errorId 2001 is eBay's unpublished *short-burst* throttle. It can fire
while daily quota remains. On a 429 the queries not yet started in that cycle are
skipped (they would be throttled too), partial results are kept, and the next
interval is stretched exponentially. If no query succeeded, the cycle raises
:class:`EbayRateLimited` (a ``SourceError``) so the base loop backs off. A failure
of one query never discards the others' results. Only when *all* queries fail does
the poll raise.

Authentication
--------------
Client-credentials grant: ``POST {api}/identity/v1/oauth2/token`` with
``Authorization: Basic base64(client_id:client_secret)`` and the form fields
``grant_type=client_credentials`` and ``scope=https://api.ebay.com/oauth/api_scope``.
Tokens live 7,200 s and minting is limited to 1,000/day, so :class:`EbayTokenManager`
does the following:

* caches the token and refreshes it 5 minutes before it expires;
* is single-flight, so an ``asyncio.Lock`` makes concurrent queries trigger one mint;
* optionally shares the token through Redis so a crash-looping fleet cannot burn the
  mint quota;
* keeps using the still-valid old token if a *proactive* refresh fails;
* is invalidated on a 401, and the search is retried once with a fresh token;
* raises ``SourceAuthError`` for ``invalid_client``, ``invalid_scope``, etc.

Compliance
----------
* **Marketplace Account Deletion.** eBay does not activate a production keyset until
  the developer subscribes to Marketplace Account Deletion/Closure notifications or
  obtains an exemption. Subscribing needs a public HTTPS endpoint that answers
  ``GET ?challenge_code=X`` with ``{"challengeResponse": sha256_hex(X + verificationToken
  + endpointUrl)}`` and acknowledges signed POST notifications. DealRadar persists
  eBay-derived rows (listing snapshots, seller names), so it needs the endpoint, not
  the exemption. Operators must purge the named user's rows when a notification
  arrives. That endpoint is deployment infrastructure and is not part of this
  ingestor.
* **API License Agreement.** eBay prohibits using API data to collect statistical
  data about eBay or to derive average selling prices or GMV for eBay categories.
  DealRadar only compares *individual* listings against operator-configured
  reference prices, for the operator's personal use, and publishes no aggregates.
  eBay asking prices enter the local price history only with weight
  ``scoring.source_history_weights.ebay`` (0.8 as shipped). Set it to ``0`` to keep
  eBay data out of the history entirely.

Parsing notes
-------------
* Money values and ``conditionId`` arrive as JSON *strings* and are parsed with
  ``Decimal``.
* AUCTION-only listings are skipped, because a bid is not a buy price. Auctions that
  still offer Buy It Now keep their BIN ``price``.
* Shipping is the cheapest known ``shippingOptions[].shippingCost`` (free = 0). It is
  ``None`` when eBay cannot quote it, for example CALCULATED shipping without a
  ``contextualLocation`` zip.
* ``posted_at`` is the newer of ``itemCreationDate`` and ``itemOriginDate``, so a
  relisted item counts as fresh.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time as dtime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

import aiohttp
from pydantic import SecretStr, ValidationError

from deal_radar.config_schema import EbaySource, Profile
from deal_radar.core.backoff import BackoffPolicy, RetryExhausted
from deal_radar.core.http import RETRYABLE_STATUSES, HttpClient, HttpResponse, HttpStatusError, json_dumps, json_loads
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import Location, RawListing, SellerInfo, SourceKind, utcnow
from deal_radar.sources.base import BaseIngestor, IngestorContext, SourceAuthError, SourceError

if TYPE_CHECKING:  # pragma: no cover
    from redis.asyncio import Redis

log = get_logger("sources.ebay")

# --------------------------------------------------------------------------- constants

PRODUCTION_API = "https://api.ebay.com"
SANDBOX_API = "https://api.sandbox.ebay.com"
TOKEN_PATH = "/identity/v1/oauth2/token"
SEARCH_PATH = "/buy/browse/v1/item_summary/search"
APP_SCOPE = "https://api.ebay.com/oauth/api_scope"

TOKEN_REFRESH_MARGIN_SECONDS = 300.0  # refresh this long before expiry
DEFAULT_TOKEN_TTL_SECONDS = 7200  # eBay application tokens live 2 h
MAX_ATTEMPTS_PER_REQUEST = 3  # 1 try + 2 retries (eBay Growth Check rule)
MAX_QUERY_CHARS = 100  # eBay truncates/rejects longer ``q``
SORT = "newlyListed"
FIELDGROUPS = "MATCHING_ITEMS,EXTENDED"
SECONDS_PER_DAY = 86_400.0
QUOTA_TIMEZONE = ZoneInfo("America/Los_Angeles")  # Browse quota resets at Pacific midnight
THROTTLE_BASE_SECONDS = 30.0  # first stretch after a burst throttle; doubles per strike
LEDGER_KEY_TTL_SECONDS = 2 * 86_400

_AUTH_ERRORS = frozenset({"invalid_client", "invalid_grant", "invalid_scope", "unauthorized_client", "invalid_request"})
_BUY_NOW_OPTIONS = frozenset({"FIXED_PRICE", "BEST_OFFER", "CLASSIFIED_AD"})
_ACCEPT_LANGUAGE = {
    "EBAY_US": "en-US",
    "EBAY_MOTORS_US": "en-US",
    "EBAY_CA": "en-CA",
    "EBAY_GB": "en-GB",
    "EBAY_AU": "en-AU",
    "EBAY_IE": "en-IE",
    "EBAY_DE": "de-DE",
    "EBAY_AT": "de-AT",
    "EBAY_CH": "de-CH",
    "EBAY_FR": "fr-FR",
    "EBAY_IT": "it-IT",
    "EBAY_ES": "es-ES",
    "EBAY_NL": "nl-NL",
}
# i.ebayimg.com/images/g/<image key>/s-l1600.jpg and the /thumbs/ variant of the same photo.
_EBAY_IMAGE = re.compile(r"/images/g/(?P<key>[^/]+)/s-l(?P<size>\d+)\.", re.IGNORECASE)


# --------------------------------------------------------------------------- errors


class EbayRateLimited(SourceError):
    """HTTP 429 / errorId 2001: eBay's short-burst throttle (or daily quota) tripped."""


class EbayRequestError(SourceError):
    """eBay rejected the request itself (HTTP 400/404/409: bad filter, category, ...)."""


# --------------------------------------------------------------------------- endpoints


@dataclass(frozen=True, slots=True)
class EbayEndpoints:
    """Base URLs for one eBay environment (overridable for tests and proxies)."""

    api_base: str

    @classmethod
    def for_environment(cls, environment: str) -> "EbayEndpoints":
        return cls(SANDBOX_API if environment == "sandbox" else PRODUCTION_API)

    @property
    def token_url(self) -> str:
        return self.api_base.rstrip("/") + TOKEN_PATH

    @property
    def search_url(self) -> str:
        return self.api_base.rstrip("/") + SEARCH_PATH


def api_policy(base: BackoffPolicy) -> BackoffPolicy:
    """The shared retry policy, capped at eBay's "at most 2 retries" rule."""
    return replace(base, max_attempts=max(1, min(base.max_attempts, MAX_ATTEMPTS_PER_REQUEST)))


def keyset_id(client_id: SecretStr | None) -> str:
    """Short, non-reversible id of the application keyset (Redis key namespace)."""
    if client_id is None:
        return "anonymous"
    return hashlib.sha1(client_id.get_secret_value().encode("utf-8")).hexdigest()[:12]


def ebay_errors(body: str | bytes | None) -> list[dict[str, Any]]:
    """``errors[]`` (or ``warnings[]``) of an eBay error body; [] when not JSON."""
    if not body:
        return []
    try:
        data = json_loads(body)
    except ValueError:
        return []
    if not isinstance(data, Mapping):
        return []
    errors = data.get("errors") or data.get("warnings") or []
    return [e for e in errors if isinstance(e, Mapping)] if isinstance(errors, list) else []


def describe_errors(errors: Sequence[Mapping[str, Any]]) -> str:
    parts = []
    for err in errors[:3]:
        message = str(err.get("message") or err.get("longMessage") or "").strip()
        parts.append(f"{err.get('errorId', '?')} {message}".strip())
    return "; ".join(parts)


# --------------------------------------------------------------------------- token manager


@dataclass(slots=True)
class _Token:
    value: str
    refresh_at: float  # monotonic: proactively refresh from here on
    expires_at: float  # monotonic: hard expiry


class EbayTokenManager:
    """Cached, single-flight OAuth2 client-credentials application token.

    ``get_token`` returns a cached token until ``refresh_margin`` seconds before its
    expiry. Concurrent callers share one refresh. Callers that waited on a refresh
    that failed re-raise that failure instead of minting again, so a down token
    endpoint costs one request per wave. With ``redis``, freshly minted tokens are
    shared under ``redis_key`` (TTL = lifetime - margin), so a fleet or a
    restart-looping worker reuses one token instead of burning eBay's 1,000/day mint
    limit.
    """

    def __init__(
        self,
        http: HttpClient,
        token_url: str,
        client_id: SecretStr | None,
        client_secret: SecretStr | None,
        *,
        scope: str = APP_SCOPE,
        refresh_margin: float = TOKEN_REFRESH_MARGIN_SECONDS,
        policy: BackoffPolicy | None = None,
        metrics: Metrics | None = None,
        redis: "Redis | None" = None,
        redis_key: str | None = None,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self.http = http
        self.token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self.scope = scope
        self.refresh_margin = refresh_margin
        self.policy = policy or api_policy(http.settings.retry)
        self.redis = redis
        self.redis_key = redis_key
        self._clock = clock
        self._wall = wall_clock
        self._token: _Token | None = None
        self._lock = asyncio.Lock()
        self._completed = 0  # refresh attempts that have finished (success or failure)
        self._last_error: Exception | None = None
        m = metrics or http.metrics
        self._m_requests = m.counter("ebay_token_requests_total", "eBay OAuth token mint requests", ("outcome",))
        self._m_ttl = m.gauge("ebay_token_ttl_seconds", "Lifetime of the current eBay application token")

    # ------------------------------------------------------------------ public

    async def get_token(self) -> str:
        token = self._fresh()
        if token is not None:
            return token
        seen = self._completed
        async with self._lock:
            token = self._fresh()
            if token is not None:
                return token
            if self._completed != seen and self._last_error is not None:
                raise self._last_error  # we queued behind a refresh that just failed
            try:
                await self._refresh()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._completed += 1
                self._last_error = exc
                stale = self._token
                now = self._clock()
                if stale is not None and now < stale.expires_at - 5.0:
                    # Proactive refresh failed but the old token still works: keep it,
                    # and retry the refresh in a little while instead of every call.
                    stale.refresh_at = min(stale.expires_at - 5.0, now + 30.0)
                    log.warning("eBay token refresh failed; reusing current token", extra={"error": str(exc)})
                    return stale.value
                raise
            self._completed += 1
            self._last_error = None
            assert self._token is not None
            return self._token.value

    async def invalidate(self, token: str | None = None) -> None:
        """Drop ``token`` (or whatever is cached) after eBay answered 401 with it."""
        if self._token is not None and (token is None or self._token.value == token):
            self._token = None
        if self.redis is None or self.redis_key is None:
            return
        try:
            shared = await self.redis.get(self.redis_key)
            if shared is not None and (token is None or _shared_token_value(shared) == token):
                await self.redis.delete(self.redis_key)
        except Exception as exc:  # noqa: BLE001 - Redis is an optimisation only
            log.debug("could not invalidate shared eBay token", extra={"error": repr(exc)})

    def clear(self) -> None:
        self._token = None

    # ------------------------------------------------------------------ internals

    def _fresh(self) -> str | None:
        if self._token is not None and self._clock() < self._token.refresh_at:
            return self._token.value
        return None

    def _install(self, value: str, lifetime: float) -> None:
        now = self._clock()
        margin = min(self.refresh_margin, lifetime / 2.0)
        self._token = _Token(value=value, refresh_at=now + lifetime - margin, expires_at=now + lifetime)
        self._m_ttl.set(lifetime)

    async def _refresh(self) -> None:
        if await self._load_shared():
            return
        value, lifetime = await self._mint()
        self._install(value, lifetime)
        await self._store_shared(value, lifetime)

    async def _mint(self) -> tuple[str, float]:
        if self._client_id is None or self._client_secret is None:
            raise SourceAuthError("eBay client_id/client_secret are not configured (EBAY_CLIENT_ID / EBAY_CLIENT_SECRET)")
        raw = f"{self._client_id.get_secret_value()}:{self._client_secret.get_secret_value()}"
        basic = base64.b64encode(raw.encode("utf-8")).decode("ascii")
        body = urlencode({"grant_type": "client_credentials", "scope": self.scope})
        try:
            resp = await self.http.request(
                "POST",
                self.token_url,
                data=body,
                headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
                accept="application/json",
                policy=self.policy,
            )
        except HttpStatusError as exc:
            error, description = _oauth_error(exc.body)
            if exc.status == 401 or error in _AUTH_ERRORS:
                self._m_requests.inc(outcome="auth_error")
                detail = f"{error}: {description}" if error else f"HTTP {exc.status}"
                raise SourceAuthError(f"eBay rejected the client credentials ({detail})") from exc
            self._m_requests.inc(outcome="error")
            raise SourceError(f"eBay token endpoint returned HTTP {exc.status}") from exc
        except (RetryExhausted, aiohttp.ClientError, asyncio.TimeoutError) as exc:
            self._m_requests.inc(outcome="error")
            raise SourceError(f"eBay token endpoint unreachable: {exc!r}") from exc
        data = resp.data if isinstance(resp.data, Mapping) else {}
        value = data.get("access_token")
        if not isinstance(value, str) or not value:
            self._m_requests.inc(outcome="error")
            raise SourceError("eBay token response has no access_token")
        lifetime = _positive_float(data.get("expires_in")) or float(DEFAULT_TOKEN_TTL_SECONDS)
        self._m_requests.inc(outcome="ok")
        log.info("minted eBay application token", extra={"expires_in_s": lifetime})
        return value, lifetime

    async def _load_shared(self) -> bool:
        if self.redis is None or self.redis_key is None:
            return False
        try:
            raw = await self.redis.get(self.redis_key)
        except Exception as exc:  # noqa: BLE001
            log.debug("shared eBay token unavailable", extra={"error": repr(exc)})
            return False
        if raw is None:
            return False
        try:
            data = json_loads(raw)
            value = str(data["t"])
            remaining = float(data["exp"]) - self._wall()
        except (ValueError, KeyError, TypeError):
            return False
        if not value or remaining <= self.refresh_margin:
            return False
        self._install(value, remaining)
        self._m_requests.inc(outcome="shared")
        return True

    async def _store_shared(self, value: str, lifetime: float) -> None:
        if self.redis is None or self.redis_key is None:
            return
        ttl_ms = int((lifetime - min(self.refresh_margin, lifetime / 2.0)) * 1000)
        if ttl_ms <= 0:
            return
        try:
            payload = json_dumps({"t": value, "exp": self._wall() + lifetime})
            await self.redis.set(self.redis_key, payload, px=ttl_ms)
        except Exception as exc:  # noqa: BLE001
            log.debug("could not share eBay token", extra={"error": repr(exc)})


def _shared_token_value(raw: Any) -> str | None:
    try:
        return str(json_loads(raw)["t"])
    except (ValueError, KeyError, TypeError):
        return None


def _oauth_error(body: str) -> tuple[str | None, str]:
    try:
        data = json_loads(body) if body else None
    except ValueError:
        return None, ""
    if not isinstance(data, Mapping):
        return None, ""
    error = data.get("error")
    return (str(error) if error else None), str(data.get("error_description") or "")


# --------------------------------------------------------------------------- quota ledger


class QuotaLedger:
    """Calls spent today against the keyset's daily budget (Pacific-midnight reset).

    Without Redis it counts this process only. With Redis, ``INCRBY`` on a per-day
    key makes the count fleet-wide and survive restarts.
    """

    def __init__(
        self,
        budget: int,
        *,
        redis: "Redis | None" = None,
        key_prefix: str = "ebay:calls:",
        tz: ZoneInfo = QUOTA_TIMEZONE,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.budget = budget
        self.redis = redis
        self.key_prefix = key_prefix
        self.tz = tz
        self._clock = clock
        self._day: date | None = None
        self._local = 0
        self._shared = 0

    def _roll(self, now: datetime) -> date:
        day = now.astimezone(self.tz).date()
        if day != self._day:
            self._day = day
            self._local = 0
            self._shared = 0
        return day

    def now(self) -> datetime:
        return self._clock()

    def day(self, now: datetime | None = None) -> date:
        """Quota day (Pacific calendar date) of ``now``."""
        return self._roll(now or self._clock())

    def used(self, now: datetime | None = None) -> int:
        self._roll(now or self._clock())
        return max(self._local, self._shared)

    def seconds_until_reset(self, now: datetime | None = None) -> float:
        now = now or self._clock()
        local = now.astimezone(self.tz)
        reset = datetime.combine(local.date() + timedelta(days=1), dtime(0), tzinfo=self.tz)
        return max(0.0, (reset - now).total_seconds())

    async def record(self, calls: int) -> int:
        """Add ``calls`` (may be 0 to just sync with Redis); returns calls used today."""
        now = self._clock()
        day = self._roll(now)
        self._local += max(0, calls)
        if self.redis is not None:
            key = f"{self.key_prefix}{day.isoformat()}"
            try:
                async with self.redis.pipeline(transaction=True) as pipe:
                    pipe.incrby(key, max(0, calls))
                    pipe.expire(key, LEDGER_KEY_TTL_SECONDS)
                    total, _ = await pipe.execute()
                self._shared = int(total)
            except Exception as exc:  # noqa: BLE001 - fall back to the local count
                log.debug("eBay quota ledger: Redis unavailable", extra={"error": repr(exc)})
        return self.used(now)


# --------------------------------------------------------------------------- request building


@dataclass(frozen=True, slots=True)
class EbayQuery:
    profile_id: str
    term: str
    url: str
    filter: str
    category_ids: tuple[str, ...] = ()

    @property
    def key(self) -> str:
        return f"{self.profile_id}:{self.term}"


def _fmt_amount(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}"


def _dedupe(values: Sequence[Any]) -> list[Any]:
    seen: set[Any] = set()
    out = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def price_bounds(profile: Profile) -> tuple[float | None, float | None]:
    """Search price range: explicit ``search.price_min/max`` or the widest band of all variants."""
    bands = [profile.price, *(profile.band_for(v.id) for v in profile.variants)]
    lo = profile.search.price_min if profile.search.price_min is not None else min(b.floor for b in bands)
    hi = profile.search.price_max if profile.search.price_max is not None else max(b.ceiling for b in bands)
    if lo is not None and hi is not None and lo > hi:
        lo, hi = hi, lo
    return lo, hi


def build_filter(
    *,
    price_min: float | None,
    price_max: float | None,
    currency: str = "USD",
    buying_options: Sequence[str] = ("FIXED_PRICE",),
    condition_ids: Sequence[int | str] = (),
    item_location_country: str | None = None,
    delivery_country: str | None = None,
) -> str:
    """Browse ``filter`` value (unencoded): ``price:[lo..hi],priceCurrency:USD,buyingOptions:{A|B},...``.

    ``price`` must be paired with ``priceCurrency``; a min-only range is ``[lo]`` and a
    max-only range ``[..hi]`` per eBay's filter reference.
    """
    parts: list[str] = []
    lo = _fmt_amount(price_min) if price_min is not None and price_min > 0 else ""
    hi = _fmt_amount(price_max) if price_max is not None and price_max > 0 else ""
    if lo and hi:
        parts.append(f"price:[{lo}..{hi}]")
    elif lo:
        parts.append(f"price:[{lo}]")
    elif hi:
        parts.append(f"price:[..{hi}]")
    if lo or hi:
        parts.append(f"priceCurrency:{currency.upper()}")
    options = _dedupe([o.strip().upper() for o in buying_options if o and o.strip()])
    if options:
        parts.append("buyingOptions:{" + "|".join(options) + "}")
    conditions = _dedupe([str(int(c)) for c in condition_ids])
    if conditions:
        parts.append("conditionIds:{" + "|".join(conditions) + "}")
    if item_location_country and item_location_country.strip():
        parts.append(f"itemLocationCountry:{item_location_country.strip().upper()}")
    if delivery_country and delivery_country.strip():
        parts.append(f"deliveryCountry:{delivery_country.strip().upper()}")
    return ",".join(parts)


def build_search_url(
    search_url: str,
    *,
    q: str,
    category_ids: Sequence[str] = (),
    filter_expr: str = "",
    limit: int = 50,
    sort: str = SORT,
    fieldgroups: str | None = FIELDGROUPS,
) -> str:
    """Fully percent-encoded search URL (every value encoded with ``safe=""``)."""
    params: list[tuple[str, str]] = [("q", q)]
    if category_ids:
        params.append(("category_ids", ",".join(category_ids)))
    if filter_expr:
        params.append(("filter", filter_expr))
    params.append(("sort", sort))
    params.append(("limit", str(int(limit))))
    if fieldgroups:
        params.append(("fieldgroups", fieldgroups))
    return search_url + "?" + "&".join(f"{k}={quote(v, safe='')}" for k, v in params)


def build_enduserctx(cfg: EbaySource) -> str | None:
    """``X-EBAY-C-ENDUSERCTX`` value, or None when there is nothing to send.

    eBay requires the zip together with the country for countries that use postal
    codes, so ``contextualLocation`` is only sent when ``delivery_postal_code`` is set.
    The inner ``country=..,zip=..`` pairs are URL-encoded as eBay documents.
    """
    parts: list[str] = []
    zip_code = (cfg.delivery_postal_code or "").strip()
    country = (cfg.delivery_country or "").strip().upper()
    if zip_code and country:
        parts.append("contextualLocation=" + quote(f"country={country},zip={zip_code}", safe=""))
    campaign = (cfg.affiliate_campaign_id or "").strip()
    if campaign:
        parts.append("affiliateCampaignId=" + quote(campaign, safe=""))
    return ",".join(parts) or None


def build_headers(cfg: EbaySource, token: str) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": cfg.marketplace_id,
        "Accept-Language": _ACCEPT_LANGUAGE.get(cfg.marketplace_id.upper(), "en-US"),
        "Accept-Encoding": "gzip",  # the only compression eBay's REST APIs support
    }
    ctx = build_enduserctx(cfg)
    if ctx:
        headers["X-EBAY-C-ENDUSERCTX"] = ctx
    return headers


def build_queries(cfg: EbaySource, profiles: Sequence[Profile], search_url: str) -> list[EbayQuery]:
    """One query per (profile, distinct term)."""
    queries: list[EbayQuery] = []
    for profile in profiles:
        lo, hi = price_bounds(profile)
        filter_expr = build_filter(
            price_min=lo,
            price_max=hi,
            currency=profile.price.currency,
            buying_options=cfg.buying_options,
            condition_ids=profile.search.ebay_condition_ids,
            item_location_country=cfg.item_location_country,
            delivery_country=cfg.delivery_country,
        )
        categories = tuple(_dedupe([c.strip() for c in profile.search.ebay_category_ids if c and c.strip()]))
        if len(categories) > 1:
            log.warning(
                "eBay documents only one category id per search request; extra ids may be ignored",
                extra={"profile": profile.id, "category_ids": list(categories)},
            )
        for raw_term in _dedupe([t.strip() for t in profile.search.terms if t and t.strip()]):
            term = raw_term
            if len(term) > MAX_QUERY_CHARS:
                log.warning("eBay query truncated to 100 characters", extra={"profile": profile.id, "term": term})
                term = term[:MAX_QUERY_CHARS].rstrip()
            url = build_search_url(search_url, q=term, category_ids=categories, filter_expr=filter_expr, limit=cfg.limit)
            queries.append(EbayQuery(profile.id, term, url, filter_expr, categories))
    return queries


# --------------------------------------------------------------------------- parsing


def _text(value: Any) -> str | None:
    if isinstance(value, str):
        value = value.strip()
        return value or None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        dec = Decimal(str(value).strip().replace(",", ""))
    except (InvalidOperation, ValueError):
        return None
    if not dec.is_finite():
        return None
    return float(dec)


def _positive_float(value: Any) -> float | None:
    number = _number(value)
    return number if number is not None and number > 0 else None


def _int(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _amount(container: Any) -> tuple[float | None, str | None]:
    if not isinstance(container, Mapping):
        return None, None
    currency = _text(container.get("currency"))
    return _number(container.get("value")), currency.upper() if currency else None


def parse_timestamp(value: Any) -> datetime | None:
    """eBay ``yyyy-MM-ddThh:mm:ss.sssZ`` → aware UTC datetime (None if unparsable)."""
    text = _text(value)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_shipping(options: Any, currency: str | None) -> tuple[float | None, str | None]:
    """Cheapest known shipping cost and its cost type; (None, None) when unknown.

    Free shipping arrives as a FIXED option costing 0.00 and yields 0.0. CALCULATED
    options only carry a cost when the request had a ``contextualLocation`` zip.
    Options priced in another currency than the item are ignored.
    """
    if not isinstance(options, list):
        return None, None
    best: float | None = None
    best_type: str | None = None
    for option in options:
        if not isinstance(option, Mapping):
            continue
        cost_type = (_text(option.get("shippingCostType")) or "").upper() or None
        value, cost_currency = _amount(option.get("shippingCost"))
        if value is None and cost_type == "FREE":
            value = 0.0
        if value is None or value < 0:
            continue
        if cost_currency and currency and cost_currency != currency.upper():
            continue
        if best is None or value < best:
            best, best_type = value, cost_type
    return best, best_type


def collect_images(d: Mapping[str, Any]) -> list[str]:
    """Primary image first, then thumbnails and additional images.

    Search results often return the same photo twice: a small ``/thumbs/`` copy and a
    large one. Copies are merged by eBay image key, keeping the position of the first
    copy and the largest ``s-lNNN`` size.
    """
    candidates: list[Any] = []
    primary = d.get("image")
    if isinstance(primary, Mapping):
        candidates.append(primary.get("imageUrl"))
    for group in ("thumbnailImages", "additionalImages"):
        images = d.get(group)
        if isinstance(images, list):
            candidates.extend(img.get("imageUrl") for img in images if isinstance(img, Mapping))
    order: list[str] = []
    best: dict[str, tuple[int, str]] = {}
    for url in candidates:
        url = _text(url)
        if url is None or not url.lower().startswith(("http://", "https://")):
            continue
        match = _EBAY_IMAGE.search(url)
        key, size = (match.group("key"), int(match.group("size"))) if match else (url, 0)
        if key not in best:
            order.append(key)
            best[key] = (size, url)
        elif size > best[key][0]:
            best[key] = (size, url)
    return [best[key][1] for key in order]


def _seller(d: Mapping[str, Any]) -> SellerInfo | None:
    seller = d.get("seller")
    if not isinstance(seller, Mapping):
        return None
    pct = _number(seller.get("feedbackPercentage"))
    if pct is not None:
        pct = min(100.0, max(0.0, pct))
    account_type = (_text(seller.get("sellerAccountType")) or "").upper()
    info = SellerInfo(
        name=_text(seller.get("username")),
        feedback_score=_int(seller.get("feedbackScore")),
        feedback_pct=pct,
        is_business=(account_type == "BUSINESS") if account_type else None,
    )
    if info.name is None and info.feedback_score is None and info.feedback_pct is None:
        return None
    return info


def _location(d: Mapping[str, Any]) -> Location | None:
    loc = d.get("itemLocation")
    if not isinstance(loc, Mapping):
        return None
    city = _text(loc.get("city"))
    region = _text(loc.get("stateOrProvince"))
    postal = _text(loc.get("postalCode"))
    country = _text(loc.get("country"))
    if not any((city, region, postal, country)):
        return None
    text = ", ".join(p for p in (city, region, postal, country) if p)
    return Location(text=text, city=city, region=region, postal_code=postal, country=country)


def _parse_item(
    d: Any, *, query: str | None, profile_hint: str | None, prefer_affiliate: bool
) -> RawListing | str:
    """RawListing, or the skip reason (``malformed``/``auction_only``/``no_price``/``no_url``)."""
    if not isinstance(d, Mapping):
        return "malformed"
    item_id = _text(d.get("itemId"))
    title = _text(d.get("title"))
    if item_id is None or title is None:
        return "malformed"

    raw_options = d.get("buyingOptions")
    options = [str(o).upper() for o in raw_options if isinstance(o, str)] if isinstance(raw_options, list) else []
    if "AUCTION" in options and not _BUY_NOW_OPTIONS.intersection(options):
        return "auction_only"  # a bid is not a buy price

    price_obj = d.get("price")
    price, currency = _amount(price_obj)
    if price is None:
        return "no_price"
    currency = currency or "USD"

    affiliate_url = _text(d.get("itemAffiliateWebUrl"))
    web_url = _text(d.get("itemWebUrl"))
    legacy_id = _text(d.get("legacyItemId"))
    if web_url is None and legacy_id is not None:
        web_url = f"https://www.ebay.com/itm/{legacy_id}"
    use_affiliate = bool(prefer_affiliate and affiliate_url)
    url = affiliate_url if use_affiliate else web_url
    if url is None:
        return "no_url"

    shipping, shipping_type = parse_shipping(d.get("shippingOptions"), currency)
    condition_id = _text(d.get("conditionId"))
    condition_text = _text(d.get("condition"))
    created = parse_timestamp(d.get("itemCreationDate"))
    origin = parse_timestamp(d.get("itemOriginDate"))
    posted_at = max((t for t in (created, origin) if t is not None), default=None)

    marketing = d.get("marketingPrice")
    list_price: float | None = None
    extra: dict[str, Any] = {"buying_options": options or ["FIXED_PRICE"]}
    if isinstance(marketing, Mapping):
        original, original_currency = _amount(marketing.get("originalPrice"))
        if original is not None and original > 0 and (original_currency in (None, currency)):
            list_price = original
        discount = _number(marketing.get("discountPercentage"))
        if discount is not None:
            extra["discount_pct"] = discount

    if legacy_id:
        extra["legacy_item_id"] = legacy_id
    if condition_text:
        extra["condition_text"] = condition_text
    if shipping_type:
        extra["shipping_cost_type"] = shipping_type
    if created is not None:
        extra["item_creation_date"] = created.isoformat()
    if origin is not None:
        extra["item_origin_date"] = origin.isoformat()
    end = parse_timestamp(d.get("itemEndDate"))
    if end is not None:
        extra["item_end_date"] = end.isoformat()
    if "AUCTION" in options:
        bid, _ = _amount(d.get("currentBidPrice"))
        if bid is not None:
            extra["current_bid"] = bid
        bids = _int(d.get("bidCount"))
        if bids is not None:
            extra["bid_count"] = bids
    leaf = d.get("leafCategoryIds")
    categories = d.get("categories")
    if isinstance(leaf, list) and leaf:
        extra["category_ids"] = [str(c) for c in leaf if _text(c)]
    elif isinstance(categories, list):
        extra["category_ids"] = [
            str(c.get("categoryId")) for c in categories if isinstance(c, Mapping) and _text(c.get("categoryId"))
        ]
    for src, dst in (("epid", "epid"), ("itemGroupType", "item_group_type"), ("listingMarketplaceId", "marketplace_id")):
        value = _text(d.get(src))
        if value:
            extra[dst] = value
    if isinstance(d.get("topRatedBuyingExperience"), bool):
        extra["top_rated"] = d["topRatedBuyingExperience"]
    converted_from = _text(price_obj.get("convertedFromCurrency")) if isinstance(price_obj, Mapping) else None
    if converted_from:
        extra["converted_from_currency"] = converted_from
    if use_affiliate and web_url:
        extra["item_web_url"] = web_url

    return RawListing(
        source="ebay",
        source_kind=SourceKind.MARKETPLACE,
        source_id=item_id,
        url=url,
        title=title,
        description=_text(d.get("shortDescription")) or "",
        price=price,
        currency=currency,
        shipping=shipping,
        list_price=list_price,
        condition=condition_id or condition_text,
        seller=_seller(d),
        location=_location(d),
        image_urls=collect_images(d),
        posted_at=posted_at,
        query=query,
        profile_hint=profile_hint,
        extra=extra,
    )


def parse_item_summary(
    d: dict[str, Any],
    *,
    query: str | None = None,
    profile_hint: str | None = None,
    prefer_affiliate: bool = False,
) -> RawListing | None:
    """Parse one Browse ``ItemSummary``; None for AUCTION-only, unpriced or malformed items."""
    try:
        result = _parse_item(d, query=query, profile_hint=profile_hint, prefer_affiliate=prefer_affiliate)
    except (ValidationError, TypeError, ValueError, AttributeError):
        return None
    return result if isinstance(result, RawListing) else None


@dataclass(slots=True)
class SearchPage:
    listings: list[RawListing] = field(default_factory=list)
    skipped: dict[str, int] = field(default_factory=dict)
    item_count: int = 0  # raw itemSummaries on the page
    total: int | None = None
    next_url: str | None = None
    warnings: list[dict[str, Any]] = field(default_factory=list)
    oldest_origin: datetime | None = None


def parse_search_page(
    data: Any,
    *,
    query: str | None = None,
    profile_hint: str | None = None,
    prefer_affiliate: bool = False,
) -> SearchPage:
    """Parse a ``SearchPagedCollection``; malformed items are skipped and counted."""
    page = SearchPage()
    if not isinstance(data, Mapping):
        return page
    page.total = _int(data.get("total"))
    page.next_url = _text(data.get("next"))
    warnings = data.get("warnings")
    if isinstance(warnings, list):
        page.warnings = [w for w in warnings if isinstance(w, Mapping)]
    summaries = data.get("itemSummaries")
    if not isinstance(summaries, list):
        return page
    page.item_count = len(summaries)
    for d in summaries:
        try:
            result = _parse_item(d, query=query, profile_hint=profile_hint, prefer_affiliate=prefer_affiliate)
        except (ValidationError, TypeError, ValueError, AttributeError) as exc:
            log.debug("skipping malformed eBay item", extra={"error": repr(exc)})
            result = "malformed"
        if isinstance(d, Mapping):
            origin = parse_timestamp(d.get("itemOriginDate"))
            if origin is not None and (page.oldest_origin is None or origin < page.oldest_origin):
                page.oldest_origin = origin
        if isinstance(result, RawListing):
            page.listings.append(result)
        else:
            page.skipped[result] = page.skipped.get(result, 0) + 1
    return page


# --------------------------------------------------------------------------- ingestor


@dataclass(slots=True)
class _PollState:
    stop_reason: str | None = None  # "throttled" | "auth": skip queries not yet started
    calls: int = 0


@dataclass(slots=True)
class _QueryOutcome:
    query: EbayQuery
    page: SearchPage | None = None
    error: SourceError | None = None
    skipped: bool = False


def classify_error(exc: BaseException) -> SourceError:
    """Map transport/HTTP failures onto the source error hierarchy."""
    if isinstance(exc, SourceError):
        return exc
    if isinstance(exc, HttpStatusError):
        errors = ebay_errors(exc.body)
        detail = describe_errors(errors) or f"HTTP {exc.status}"
        if exc.status == 429:
            err: SourceError = EbayRateLimited(f"eBay throttled the request (HTTP 429: {detail})")
        elif exc.status == 401:
            err = SourceAuthError(f"eBay rejected the access token (HTTP 401: {detail})")
        elif exc.status == 403:
            err = SourceAuthError(
                f"eBay denied access (HTTP 403: {detail}); check the keyset is activated "
                "(Marketplace Account Deletion compliance) and allowed to use the Browse API"
            )
        elif exc.status in (400, 404, 409):
            err = EbayRequestError(f"eBay rejected the search request (HTTP {exc.status}: {detail})")
        else:
            err = SourceError(f"eBay returned HTTP {exc.status}: {detail}")
        err.__cause__ = exc
        return err
    if isinstance(exc, RetryExhausted):
        err = SourceError(f"eBay request failed after {exc.attempts} attempt(s): {exc.last_exc!r}")
    elif isinstance(exc, (aiohttp.ClientError, asyncio.TimeoutError)):
        err = SourceError(f"eBay request failed: {exc!r}")
    else:
        err = SourceError(f"eBay query failed: {exc!r}")
    err.__cause__ = exc
    return err


class EbayIngestor(BaseIngestor):
    """Quota-aware poller of the Browse API ``item_summary/search`` endpoint."""

    name: ClassVar[str] = "ebay"
    kind: ClassVar[SourceKind] = SourceKind.MARKETPLACE
    cfg: EbaySource

    def __init__(self, cfg: EbaySource, ctx: IngestorContext, *, endpoints: EbayEndpoints | None = None) -> None:
        super().__init__(cfg, ctx)
        self.cfg = cfg
        self.endpoints = endpoints or EbayEndpoints.for_environment(cfg.environment)
        self.policy = api_policy(ctx.http.settings.retry)
        keyset = keyset_id(cfg.client_id)
        prefix = ctx.config.storage.redis_key_prefix
        self.tokens = EbayTokenManager(
            ctx.http,
            self.endpoints.token_url,
            cfg.client_id,
            cfg.client_secret,
            policy=self.policy,
            metrics=ctx.metrics,
            redis=ctx.redis,
            redis_key=f"{prefix}ebay:token:{cfg.environment}:{keyset}",
        )
        self.ledger = QuotaLedger(
            cfg.daily_call_budget, redis=ctx.redis, key_prefix=f"{prefix}ebay:calls:{cfg.environment}:{keyset}:"
        )
        self._queries: list[EbayQuery] | None = None
        self._monotonic: Callable[[], float] = time.monotonic
        self._throttle_strikes = 0
        self._throttled_until = 0.0
        self._last_query_success: dict[str, datetime] = {}
        self._warned: set[str] = set()
        self._exhausted_day: date | None = None
        m = ctx.metrics
        self._m_queries = m.counter("ebay_queries_total", "eBay search requests by outcome", ("profile", "outcome"))
        self._m_skipped = m.counter("ebay_items_skipped_total", "eBay item summaries not turned into listings", ("reason",))
        self._m_saturated = m.counter("ebay_page_saturated_total", "Full eBay pages newer than the previous poll", ("profile",))
        self._m_used = m.gauge("ebay_quota_used", "eBay Browse calls used today (keyset ledger)")
        self._m_interval = m.gauge("ebay_poll_interval_seconds", "Quota-derived eBay poll interval")
        self._m_calls_per_poll = m.gauge("ebay_calls_per_poll", "eBay search calls per poll cycle")

    # ------------------------------------------------------------------ queries & quota

    @property
    def queries(self) -> list[EbayQuery]:
        if self._queries is None:
            self._queries = build_queries(self.cfg, self.search_profiles(), self.endpoints.search_url)
            self._m_calls_per_poll.set(len(self._queries))
        return self._queries

    def calls_per_poll(self) -> int:
        return max(1, len(self.queries))

    def quota_interval(self) -> float:
        """Unjittered steady-state interval that keeps a day of polls inside the safety budget."""
        allowed_polls_per_day = self.cfg.daily_call_budget * self.cfg.budget_safety_factor / self.calls_per_poll()
        return max(float(self.cfg.poll_interval_seconds), SECONDS_PER_DAY / allowed_polls_per_day)

    def ledger_interval(self, now: datetime | None = None) -> float:
        """Interval implied by what is actually left of today's safety budget."""
        cpp = self.calls_per_poll()
        usable = self.cfg.daily_call_budget * self.cfg.budget_safety_factor - self.ledger.used(now)
        until_reset = self.ledger.seconds_until_reset(now)
        if usable < cpp:
            return until_reset
        return until_reset * cpp / usable

    def _budget_exhausted(self, now: datetime | None = None) -> bool:
        usable = self.cfg.daily_call_budget * self.cfg.budget_safety_factor - self.ledger.used(now)
        return usable < self.calls_per_poll()

    def next_interval(self) -> float:
        now = self.ledger.now()
        jitter = float(self.cfg.jitter_pct)
        quota = self.quota_interval()
        if self._budget_exhausted(now):
            delay = self.ledger.seconds_until_reset(now) + 1.0 + self.ctx.rng.uniform(0.0, jitter * quota)
        else:
            base = max(quota, self.ledger_interval(now))
            delay = base * (1.0 + self.ctx.rng.uniform(-jitter, jitter))
        delay = max(delay, self._throttled_until - self._monotonic())
        self._m_interval.set(round(delay, 3))
        return max(0.05, delay)

    # ------------------------------------------------------------------ lifecycle

    async def setup(self) -> None:
        if self.cfg.client_id is None or self.cfg.client_secret is None:
            raise SourceAuthError("sources.ebay requires client_id and client_secret (EBAY_CLIENT_ID / EBAY_CLIENT_SECRET)")
        queries = self.queries
        if not queries:
            self.log.warning("no enabled profile has search terms for eBay; nothing to poll", extra={"source": self.name})
        self.log.info(
            "eBay polling plan",
            extra={
                "source": self.name,
                "environment": self.cfg.environment,
                "queries": len(queries),
                "daily_call_budget": self.cfg.daily_call_budget,
                "interval_s": round(self.quota_interval(), 1),
            },
        )

    async def teardown(self) -> None:
        self.tokens.clear()

    # ------------------------------------------------------------------ polling

    async def poll(self) -> list[RawListing]:
        queries = self.queries
        if not queries:
            return []
        used = self.ledger.used()
        if used + len(queries) > self.cfg.daily_call_budget:
            day = self.ledger.day()
            if self._exhausted_day != day:
                self._exhausted_day = day
                self.log.warning(
                    "eBay daily call budget exhausted; pausing until the quota resets",
                    extra={"source": self.name, "used": used, "budget": self.cfg.daily_call_budget},
                )
            return []

        state = _PollState()
        semaphore = asyncio.Semaphore(self.cfg.max_concurrency)
        try:
            async with asyncio.TaskGroup() as group:
                tasks = [group.create_task(self._run_query(q, semaphore, state)) for q in queries]
        finally:
            self._m_used.set(await self.ledger.record(state.calls))
        outcomes = [t.result() for t in tasks]

        succeeded = [o for o in outcomes if o.page is not None]
        failed = [o for o in outcomes if o.error is not None]
        skipped = [o for o in outcomes if o.skipped]
        throttled = any(isinstance(o.error, EbayRateLimited) for o in failed)
        self._update_throttle(throttled)

        if not succeeded:
            errors = [o.error for o in failed if o.error is not None]
            auth = next((e for e in errors if isinstance(e, SourceAuthError)), None)
            if auth is not None:
                raise auth
            if throttled:
                raise EbayRateLimited(
                    f"eBay throttled the poll (HTTP 429 / errorId 2001): {len(failed)} failed, {len(skipped)} skipped"
                )
            first = errors[0] if errors else SourceError("no eBay query completed")
            raise SourceError(f"all {len(queries)} eBay queries failed; first error: {first}") from first

        listings: list[RawListing] = []
        seen: set[str] = set()
        duplicates = 0
        for outcome in succeeded:
            assert outcome.page is not None
            for raw in outcome.page.listings:
                if raw.source_id in seen:
                    duplicates += 1
                    continue
                seen.add(raw.source_id)
                listings.append(raw)
        if duplicates:
            self._m_skipped.inc(duplicates, reason="duplicate")
        if failed or skipped:
            self.log.warning(
                "eBay poll partially failed",
                extra={
                    "source": self.name,
                    "ok": len(succeeded),
                    "failed": len(failed),
                    "skipped": len(skipped),
                    "listings": len(listings),
                },
            )
        return listings

    def _update_throttle(self, throttled: bool) -> None:
        if not throttled:
            self._throttle_strikes = 0
            self._throttled_until = 0.0
            return
        self._throttle_strikes += 1
        penalty = min(float(self.cfg.cooldown_seconds), THROTTLE_BASE_SECONDS * 2 ** (self._throttle_strikes - 1))
        self._throttled_until = self._monotonic() + penalty
        self.log.warning(
            "eBay burst throttle hit; stretching the poll interval", extra={"source": self.name, "penalty_s": penalty}
        )

    async def _run_query(self, query: EbayQuery, semaphore: asyncio.Semaphore, state: _PollState) -> _QueryOutcome:
        async with semaphore:
            if state.stop_reason is not None:
                self._m_queries.inc(profile=query.profile_id, outcome="skipped")
                return _QueryOutcome(query, skipped=True)
            started_at = utcnow()
            try:
                data = await self._search(query, state)
                page = parse_search_page(
                    data,
                    query=query.term,
                    profile_hint=query.profile_id,
                    prefer_affiliate=bool(self.cfg.affiliate_campaign_id),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - one failing query must not sink the poll
                error = classify_error(exc)
                if isinstance(error, EbayRateLimited):
                    state.stop_reason = "throttled"
                    outcome = "throttled"
                elif isinstance(error, SourceAuthError):
                    state.stop_reason = "auth"
                    outcome = "auth_error"
                else:
                    outcome = "error"
                self._m_queries.inc(profile=query.profile_id, outcome=outcome)
                self.log.warning(
                    "eBay query failed",
                    extra={"source": self.name, "profile": query.profile_id, "term": query.term, "error": str(error)},
                )
                return _QueryOutcome(query, error=error)
        self._m_queries.inc(profile=query.profile_id, outcome="ok")
        self._after_page(query, page, started_at)
        return _QueryOutcome(query, page=page)

    def _after_page(self, query: EbayQuery, page: SearchPage, started_at: datetime) -> None:
        for reason, count in page.skipped.items():
            self._m_skipped.inc(count, reason=reason)
        for warning in page.warnings:
            marker = f"{query.key}:{warning.get('errorId')}"
            if marker not in self._warned:
                self._warned.add(marker)
                self.log.warning(
                    "eBay returned a search warning",
                    extra={
                        "source": self.name,
                        "profile": query.profile_id,
                        "term": query.term,
                        "warning": describe_errors([warning]),
                    },
                )
        previous = self._last_query_success.get(query.key)
        if (
            previous is not None
            and page.item_count >= self.cfg.limit
            and page.next_url
            and page.oldest_origin is not None
            and page.oldest_origin > previous
        ):
            self._m_saturated.inc(profile=query.profile_id)
            self.log.warning(
                "eBay result page saturated; listings may have been missed (raise sources.ebay.limit)",
                extra={"source": self.name, "profile": query.profile_id, "term": query.term, "limit": self.cfg.limit},
            )
        self._last_query_success[query.key] = started_at

    async def _search(self, query: EbayQuery, state: _PollState) -> Any:
        token = await self.tokens.get_token()
        try:
            response = await self._get(query.url, token, state)
        except HttpStatusError as exc:
            if exc.status != 401:
                raise
            self.log.info("eBay rejected the access token; minting a new one", extra={"source": self.name})
            await self.tokens.invalidate(token)
            token = await self.tokens.get_token()
            try:
                response = await self._get(query.url, token, state)
            except HttpStatusError as retry_exc:
                if retry_exc.status == 401:
                    detail = describe_errors(ebay_errors(retry_exc.body)) or "HTTP 401"
                    raise SourceAuthError(f"eBay rejected a freshly minted application token ({detail})") from retry_exc
                raise
        return response.data

    async def _get(self, url: str, token: str, state: _PollState) -> HttpResponse:
        try:
            response = await self.ctx.http.request(
                "GET",
                url,
                headers=build_headers(self.cfg, token),
                accept="application/json",
                policy=self.policy,
            )
        except HttpStatusError as exc:
            state.calls += self.policy.max_attempts if exc.status in RETRYABLE_STATUSES else 1
            raise
        except RetryExhausted as exc:
            state.calls += exc.attempts
            raise
        except Exception:
            state.calls += 1
            raise
        state.calls += response.attempts
        return response


__all__ = [
    "APP_SCOPE",
    "EbayEndpoints",
    "EbayIngestor",
    "EbayQuery",
    "EbayRateLimited",
    "EbayRequestError",
    "EbayTokenManager",
    "QuotaLedger",
    "SearchPage",
    "api_policy",
    "build_enduserctx",
    "build_filter",
    "build_headers",
    "build_queries",
    "build_search_url",
    "classify_error",
    "collect_images",
    "describe_errors",
    "ebay_errors",
    "keyset_id",
    "parse_item_summary",
    "parse_search_page",
    "parse_shipping",
    "parse_timestamp",
    "price_bounds",
]
