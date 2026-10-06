"""OfferUp ingestor: server-rendered search feed first, the web app's GraphQL as fallback.

OfferUp has no public API. Two read paths are used by its own web app and are
documented by several open-source clients (validated 2025-2026):

1. **Search page SSR** (primary) – ``GET https://offerup.com/search?q=...`` is a
   Next.js (pages router) page whose ``<script id="__NEXT_DATA__">`` JSON embeds the
   first page of the search feed at ``props.pageProps.searchFeedResponse.looseTiles``.
   One navigation-shaped GET per query, no client tokens, no browser. Query params
   honoured by the page: ``q``, ``sort`` (``-posted`` = newest first), ``radius``
   (5/10/20/30/50 miles), ``price_min``/``price_max``.
2. **GraphQL** (fallback) – ``POST https://offerup.com/api/graphql`` with operation
   ``GetModularFeed`` and ``searchParams`` ``[{key, value}]`` (string values; keys
   ``q``, ``sort``, ``radius``, ``price_min``, ``price_max``, ``lat``, ``lon``,
   ``zipcode``, ``limit``). Unknown keys are silently ignored upstream, so only keys
   confirmed by the server's own filter echo are sent. Results live in
   ``modularFeed.looseTiles[].listing`` and ``modularFeed.modules[].grid.tiles[].listing``.
   Requests carry the headers the web app mints client-side (``x-ou-operation-name``,
   a sticky ``x-ou-d-token`` device id ``web-<56 hex>``, ``ou-session-id``
   ``<token>@<epoch ms>``, ``x-request-id``); no login or server-issued token is used.
   The endpoint is token-gated for some anonymous clients: a bare 401/403 *without* a
   challenge wall therefore parks GraphQL for :data:`BLOCK_COOLDOWN_SECONDS` instead of
   pausing the whole source (a real IP block also walls the SSR page, which then raises
   :class:`SourceBlocked`).

Location: OfferUp geolocates anonymous visitors by IP and keeps the choice in an
``ou.location`` cookie (URL-encoded JSON ``{latitude, longitude, zipCode, source}``).
We send that cookie with every request so results are scoped to the configured
place even when the collector's IP geolocates elsewhere; a ZIP-only config is
resolved once to coordinates via the ``GeocodeLocation`` GraphQL operation. Any
``ou.location`` the server may have set in the shared cookie jar is dropped first,
otherwise aiohttp would let the jar value override our header.

Parsing is deliberately *shape-agnostic*: rather than trusting one JSON path, the
parser walks the payload for listing-like objects (``listingId`` + ``title``), skips
ad tiles (``tileType`` ``AD_*`` / ``...Ad`` typenames: promoted listings are not
necessarily local), de-duplicates by id and isolates malformed items (counted per
reason). A Next.js path rename therefore degrades nothing; a missing payload falls
back to the other strategy.

Politeness: requests go through the shared :class:`HttpClient` (host token bucket
``offerup.com`` in config, coherent sticky browser identity, retries). A poll runs
queries sequentially inside a time budget (80 % of ``poll_timeout_seconds``) and
rotates its starting query between polls, so a slow host never makes the poll time
out and every query still gets its turn. Block walls (HTTP 403/429 on the page,
Cloudflare / PerimeterX / hCaptcha challenge pages, also when served as 503) raise
:class:`SourceBlocked` – never retried, never solved; listings already gathered in
that poll are returned and the block is raised at the start of the next poll so the
base loop applies its cooldown. The ZIP geocode is an optimisation: its failures
(including a gated GraphQL) only back off geocoding; the ZIP cookie still scopes
the page search.

Known gaps (upstream does not expose them in the feed): posting time, description,
seller data and ``state``/``isRemoved`` are only on item-detail objects, which are
deliberately not fetched (one extra request per listing would multiply load ~50x);
when such fields do appear (Apollo state), ``isRemoved: true`` maps to
``in_stock=False`` so the scorer gates the listing as unavailable.
"""

from __future__ import annotations

import asyncio
import html as html_lib
import json
import math
import re
import secrets
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import quote

from deal_radar.core.http import HttpResponse, HttpStatusError, json_loads
from deal_radar.core.logs import get_logger
from deal_radar.engine.types import Location, RawListing, SellerInfo, SourceKind
from deal_radar.sources.base import BaseIngestor, IngestorContext, SourceBlocked, SourceError

if TYPE_CHECKING:  # pragma: no cover
    from deal_radar.config_schema import OfferUpSource, Profile

log = get_logger("sources.offerup")

OFFERUP_ORIGIN = "https://offerup.com"
ITEM_URL_TEMPLATE = OFFERUP_ORIGIN + "/item/detail/{listing_id}"
RADIUS_CHOICES: tuple[int, ...] = (5, 10, 20, 30, 50)  # values OfferUp's DISTANCE filter publishes
SORT_NEWEST = "-posted"
MAX_BODY_BYTES = 8 * 1024 * 1024
BUDGET_FRACTION = 0.8  # share of poll_timeout_seconds a poll may spend issuing queries
PRIMARY_RETRY_SECONDS = 1800.0  # after switching to the fallback, re-try the primary this often
GEOCODE_RETRY_SECONDS = 3600.0
BLOCK_COOLDOWN_SECONDS = 1800.0  # minimum pause after a 403/challenge wall (429 uses the configured cooldown)
STRATEGY_PAGE = "page"
STRATEGY_GRAPHQL = "graphql"

FEED_QUERY = """query GetModularFeed($searchParams: [SearchParam], $debug: Boolean = false) {
  modularFeed(params: $searchParams, debug: $debug) {
    pageCursor
    looseTiles {
      __typename
      ... on ModularFeedTileListing {
        tileId
        tileType
        listing {
          ...feedListing
        }
      }
    }
    modules {
      __typename
      ... on ModularFeedModuleGrid {
        grid {
          tiles {
            __typename
            ... on ModularFeedTileListing {
              tileId
              tileType
              listing {
                ...feedListing
              }
            }
          }
        }
      }
    }
    __typename
  }
}

fragment feedListing on ModularFeedListing {
  listingId
  conditionText
  flags
  image {
    height
    url
    width
  }
  isFirmPrice
  locationName
  price
  title
  vehicleMiles
}
"""

GEOCODE_QUERY = """query GeocodeLocation($input: GeocodeLocationInput!) {
  geocodeLocation(input: $input) {
    location {
      city
      latitude
      longitude
      state
      zipCode
    }
  }
}
"""

_NEXT_DATA_RE = re.compile(r"<script[^>]*\bid=[\"']__NEXT_DATA__[\"'][^>]*>(.*?)</script>", re.S | re.I)
# Only consulted when the expected payload is missing: a normal page may legitimately
# mention "captcha" (login modal scripts), a challenge page never carries __NEXT_DATA__.
_BLOCK_RE = re.compile(
    r"cf-chl|challenge-platform|cf-turnstile|cf-browser-verification|checking your browser"
    r"|just a moment\.\.\.|attention required!? \| cloudflare|px-captcha|captcha-delivery"
    r"|access to this page has been denied|verify you are (?:a )?human|<title>\s*access denied\s*</title>"
    r"|\bh-?captcha\b|\bcaptcha\b",  # word-bounded: a page that merely loads reCAPTCHA is not a wall
    re.I,
)
_LISTING_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
_PLAIN_PRICE_RE = re.compile(r"^\$?\s*(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?$")
# Promoted (SellerAd), display ads and job tiles are not local for-sale listings.
_AD_TYPENAME_RE = re.compile(r"Tile\w*Ad$|TileJob$")
_FEED_KEYS = ("searchFeedResponse", "initialSearchFeedResponse", "feedData")
_POSTED_KEYS = ("postDate", "postedDate", "listingDate", "createdDate", "listedAt", "createdAt")
_SKIP_SCAN_BYTES = 200_000


class OfferUpParseError(SourceError):
    """The response did not contain a recognisable search feed."""


class OfferUpGraphQLUnavailable(SourceError):
    """GraphQL answered 401/403 without a challenge wall (token gating, not an IP block)."""


# --------------------------------------------------------------------------- pure helpers


def snap_radius(miles: float | int | None) -> int:
    """Round a radius up to the nearest value OfferUp honours (5/10/20/30/50 miles)."""
    if miles is None or not math.isfinite(float(miles)) or miles <= 0:
        return 30
    for choice in RADIUS_CHOICES:
        if miles <= choice:
            return choice
    return RADIUS_CHOICES[-1]


def location_cookie(latitude: float | None, longitude: float | None, zip_code: str | None = None) -> str | None:
    """Value of the ``ou.location`` cookie for a search centre (``None`` = use IP geo)."""
    payload: dict[str, Any] = {}
    if latitude is not None and longitude is not None:
        payload["latitude"] = round(float(latitude), 6)
        payload["longitude"] = round(float(longitude), 6)
    if zip_code:
        payload["zipCode"] = str(zip_code).strip()
    if not payload:
        return None
    payload["source"] = "user"
    return quote(json.dumps(payload, separators=(",", ":")), safe="")


def block_cooldown(reason: str, configured: float) -> float | None:
    """Cooldown for a block: rate limits use the configured pause, walls at least 30 min."""
    return None if reason == "http_429" else max(configured, BLOCK_COOLDOWN_SECONDS)


def detect_block(status: int, body: str | None) -> str | None:
    """Return a short reason when a response is a block wall / challenge, else ``None``."""
    if status in (403, 429):
        return f"http_{status}"
    if body:
        match = _BLOCK_RE.search(body[:_SKIP_SCAN_BYTES])
        if match:
            return f"challenge:{match.group(0).strip().lower()[:40]}"
    return None


def extract_next_data(page: str) -> dict[str, Any] | None:
    """Decode the ``__NEXT_DATA__`` JSON embedded in a Next.js page (``None`` if absent/invalid)."""
    match = _NEXT_DATA_RE.search(page)
    if match is None:
        return None
    try:
        data = json_loads(match.group(1).strip())
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def parse_timestamp(value: Any) -> datetime | None:
    """Epoch seconds/milliseconds (number or digit string) or ISO-8601 -> aware UTC datetime."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            value = int(text)
        else:
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc) if 2005 <= parsed.year <= 2100 else None
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            return None
        seconds = float(value) / 1000.0 if value > 1e11 else float(value)
        if not 1.1e9 <= seconds <= 4.1e9:  # 2004 .. 2099
            return None
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    return None


def coerce_price(value: Any) -> float | str | None:
    """Plain numbers become floats; anything else is passed through for the normalizer."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(float(value)) and value >= 0 else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        match = _PLAIN_PRICE_RE.match(text)
        if match:
            return float(match.group(1).replace(",", "") + (match.group(2) or ""))
        return text
    return None


def split_place(name: str | None) -> tuple[str | None, str | None]:
    """``"Brooklyn, NY"`` -> ``("Brooklyn", "NY")``."""
    if not name:
        return None, None
    city, sep, region = name.rpartition(",")
    if not sep:
        return name.strip() or None, None
    return city.strip() or None, region.strip() or None


def _clean(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    return " ".join(html_lib.unescape(text).split())


def _is_ad_tile(node: Mapping[str, Any]) -> bool:
    tile_type = node.get("tileType")
    if isinstance(tile_type, str) and tile_type.upper().startswith("AD"):
        return True
    typename = node.get("__typename")
    return isinstance(typename, str) and bool(_AD_TYPENAME_RE.search(typename))


def _is_listing_like(node: Mapping[str, Any]) -> bool:
    listing_id = node.get("listingId")
    title = node.get("title")
    return (
        isinstance(listing_id, (str, int))
        and not isinstance(listing_id, bool)
        and str(listing_id).strip() != ""
        and isinstance(title, str)
    )


def iter_listing_objects(node: Any, *, max_depth: int = 40) -> Iterator[Mapping[str, Any]]:
    """Yield every listing-like dict under ``node`` (depth-first), skipping ad tiles."""
    stack: list[tuple[Any, int]] = [(node, 0)]
    while stack:
        current, depth = stack.pop()
        if depth > max_depth:
            continue
        if isinstance(current, Mapping):
            if _is_ad_tile(current):
                continue
            if _is_listing_like(current):
                yield current
                continue
            children = list(current.values())
        elif isinstance(current, list):
            children = current
        else:
            continue
        # Reverse so the walk preserves document order (feed rank) despite the LIFO stack.
        for child in reversed(children):
            if isinstance(child, (Mapping, list)):
                stack.append((child, depth + 1))


def _image_urls(obj: Mapping[str, Any]) -> list[str]:
    urls: list[str] = []

    def add(candidate: Any) -> None:
        if isinstance(candidate, Mapping):
            candidate = candidate.get("url")
        if isinstance(candidate, str) and candidate.startswith("//"):
            candidate = "https:" + candidate
        if isinstance(candidate, str) and candidate.startswith(("http://", "https://")) and candidate not in urls:
            urls.append(candidate)

    add(obj.get("image"))
    add(obj.get("imageUrl"))
    photos = obj.get("photos")
    if isinstance(photos, list):
        for photo in photos:
            if isinstance(photo, Mapping):
                for key in ("detailFull", "detail", "medium", "list", "url"):
                    if photo.get(key):
                        add(photo.get(key))
                        break
            else:
                add(photo)
    return urls


def _location(obj: Mapping[str, Any]) -> Location | None:
    details = obj.get("locationDetails")
    details = details if isinstance(details, Mapping) else {}
    name = obj.get("locationName") or details.get("locationName")
    name = _clean(name) or None
    lat = details.get("latitude")
    lon = details.get("longitude")
    postal = details.get("zipcode") or details.get("zipCode")
    if name is None and lat is None and postal is None:
        return None
    city, region = split_place(name)
    return Location(
        text=name,
        city=city,
        region=region,
        postal_code=str(postal) if postal else None,
        country="US",
        latitude=float(lat) if isinstance(lat, (int, float)) and not isinstance(lat, bool) else None,
        longitude=float(lon) if isinstance(lon, (int, float)) and not isinstance(lon, bool) else None,
    )


def _seller(obj: Mapping[str, Any]) -> SellerInfo | None:
    owner = obj.get("owner")
    profile = owner.get("profile") if isinstance(owner, Mapping) else None
    if not isinstance(profile, Mapping):
        name = _clean(obj.get("sellerName")) or None
        return SellerInfo(name=name) if name else None
    rating = profile.get("ratingSummary")
    count = rating.get("count") if isinstance(rating, Mapping) else None
    business = profile.get("isBusinessAccount")
    name = _clean(profile.get("name")) or None
    if name is None and count is None:
        return None
    return SellerInfo(
        name=name,
        feedback_score=int(count) if isinstance(count, (int, float)) and not isinstance(count, bool) else None,
        is_business=business if isinstance(business, bool) else None,
    )


def parse_listing(
    obj: Mapping[str, Any],
    *,
    query: str | None = None,
    profile_hint: str | None = None,
    strategy: str | None = None,
) -> RawListing | None:
    """Map one OfferUp listing object to a :class:`RawListing` (``None`` if unusable)."""
    listing_id = str(obj.get("listingId") or "").strip()
    if not _LISTING_ID_RE.match(listing_id):
        return None
    title = _clean(obj.get("title"))
    if not title:
        return None
    price = coerce_price(obj.get("price"))
    if price is None:
        price = coerce_price(obj.get("formattedPrice"))
    condition = obj.get("conditionText")
    if not isinstance(condition, str) or not condition.strip():
        raw_condition = obj.get("condition")
        condition = raw_condition if isinstance(raw_condition, str) else None
    posted_at = None
    for key in _POSTED_KEYS:
        posted_at = parse_timestamp(obj.get(key))
        if posted_at is not None:
            break
    flags = [f for f in obj.get("flags") or [] if isinstance(f, str)] if isinstance(obj.get("flags"), list) else []
    extra: dict[str, Any] = {}
    if flags:
        extra["flags"] = flags
    if isinstance(obj.get("isFirmPrice"), bool):
        extra["firm_price"] = obj["isFirmPrice"]
    if obj.get("vehicleMiles") not in (None, ""):
        extra["vehicle_miles"] = str(obj["vehicleMiles"])
    if isinstance(obj.get("price"), str):
        extra["price_text"] = obj["price"]
    if strategy:
        extra["via"] = strategy
    state = obj.get("state")
    if isinstance(state, str) and state.strip():
        extra["state"] = state.strip()
    removed = obj.get("isRemoved")
    description = _clean(obj.get("description"))
    original = coerce_price(obj.get("originalPrice"))
    return RawListing(
        source="offerup",
        source_kind=SourceKind.LOCAL,
        source_id=listing_id,
        url=ITEM_URL_TEMPLATE.format(listing_id=listing_id),
        title=title,
        description=description,
        price=price,
        currency="USD",
        list_price=original if isinstance(original, float) else None,
        condition=condition.strip() if isinstance(condition, str) and condition.strip() else None,
        seller=_seller(obj),
        location=_location(obj),
        image_urls=_image_urls(obj),
        posted_at=posted_at,
        in_stock=False if removed is True else None,
        query=query,
        profile_hint=profile_hint,
        extra=extra,
    )


def _collect(
    root: Any,
    *,
    query: str | None,
    profile_hint: str | None,
    limit: int | None,
    stats: dict[str, int] | None,
    strategy: str,
) -> list[RawListing]:
    out: list[RawListing] = []
    seen: set[str] = set()
    for obj in iter_listing_objects(root):
        try:
            raw = parse_listing(obj, query=query, profile_hint=profile_hint, strategy=strategy)
        except (ValueError, TypeError) as exc:  # pydantic ValidationError is a ValueError
            log.debug("offerup item rejected", extra={"error": repr(exc)[:200]})
            raw = None
            reason = "invalid"
        else:
            reason = "unusable"
        if raw is None:
            if stats is not None:
                stats[reason] = stats.get(reason, 0) + 1
            continue
        if raw.source_id in seen:
            continue
        seen.add(raw.source_id)
        out.append(raw)
        if limit is not None and len(out) >= limit:
            break
    return out


def parse_search_page(
    page: str,
    *,
    query: str | None = None,
    profile_hint: str | None = None,
    limit: int | None = None,
    stats: dict[str, int] | None = None,
) -> list[RawListing]:
    """Listings from an OfferUp search page's ``__NEXT_DATA__``.

    Raises :class:`OfferUpParseError` when the page carries no ``__NEXT_DATA__`` or no
    search feed at all (block page, degraded page, layout change). A feed with zero
    tiles is a legitimate empty result and returns ``[]``.
    """
    data = extract_next_data(page)
    if data is None:
        raise OfferUpParseError("no __NEXT_DATA__ in search page")
    props = data.get("props")
    page_props = props.get("pageProps") if isinstance(props, Mapping) else None
    if not isinstance(page_props, Mapping):
        raise OfferUpParseError("__NEXT_DATA__ has no props.pageProps")
    feed = next((page_props[k] for k in _FEED_KEYS if isinstance(page_props.get(k), Mapping)), None)
    listings = _collect(
        feed if feed is not None else page_props,
        query=query,
        profile_hint=profile_hint,
        limit=limit,
        stats=stats,
        strategy=STRATEGY_PAGE,
    )
    if feed is None and not listings:
        raise OfferUpParseError("no search feed in __NEXT_DATA__")
    return listings


def parse_graphql_feed(
    payload: Any,
    *,
    query: str | None = None,
    profile_hint: str | None = None,
    limit: int | None = None,
    stats: dict[str, int] | None = None,
) -> list[RawListing]:
    """Listings from a ``GetModularFeed`` GraphQL response."""
    if not isinstance(payload, Mapping):
        raise OfferUpParseError(f"unexpected GraphQL payload type {type(payload).__name__}")
    data = payload.get("data")
    feed = data.get("modularFeed") if isinstance(data, Mapping) else None
    if not isinstance(feed, Mapping):
        errors = payload.get("errors")
        messages = []
        if isinstance(errors, list):
            messages = [str(e.get("message", e))[:120] for e in errors if isinstance(e, Mapping)]
        raise OfferUpParseError(f"GraphQL response without modularFeed: {messages or 'no data'}")
    return _collect(feed, query=query, profile_hint=profile_hint, limit=limit, stats=stats, strategy=STRATEGY_GRAPHQL)


def parse_geocode(payload: Any) -> tuple[float, float] | None:
    """``(latitude, longitude)`` from a ``GeocodeLocation`` response."""
    if not isinstance(payload, Mapping):
        return None
    data = payload.get("data")
    geo = data.get("geocodeLocation") if isinstance(data, Mapping) else None
    loc = geo.get("location") if isinstance(geo, Mapping) else None
    if not isinstance(loc, Mapping):
        return None
    try:
        lat = float(loc["latitude"])
        lon = float(loc["longitude"])
    except (KeyError, TypeError, ValueError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    return lat, lon


@dataclass(frozen=True, slots=True)
class SearchTask:
    """One upstream query: a search term of a profile with its price window."""

    profile_id: str
    term: str
    price_min: int | None
    price_max: int | None


def build_tasks(profiles: list["Profile"]) -> list[SearchTask]:
    """One task per (profile, term); price window = search override or the profile band."""
    tasks: list[SearchTask] = []
    for profile in profiles:
        low = profile.search.price_min if profile.search.price_min is not None else profile.price.floor
        high = profile.search.price_max if profile.search.price_max is not None else profile.price.ceiling
        for term in profile.search.terms:
            term = " ".join(term.split())
            if term:
                tasks.append(
                    SearchTask(
                        profile_id=profile.id,
                        term=term,
                        price_min=int(math.floor(low)) if low is not None else None,
                        price_max=int(math.ceil(high)) if high is not None else None,
                    )
                )
    return tasks


def page_params(task: SearchTask, radius_miles: int) -> dict[str, str]:
    params = {"q": task.term, "sort": SORT_NEWEST, "radius": str(snap_radius(radius_miles))}
    if task.price_min is not None:
        params["price_min"] = str(task.price_min)
    if task.price_max is not None:
        params["price_max"] = str(task.price_max)
    return params


def graphql_search_params(
    task: SearchTask,
    *,
    radius_miles: int,
    limit: int,
    coordinates: tuple[float, float] | None,
    session_id: str,
    zip_code: str | None = None,
) -> list[dict[str, str]]:
    params = [
        {"key": "q", "value": task.term},
        {"key": "platform", "value": "web"},
        {"key": "sort", "value": SORT_NEWEST},
        {"key": "radius", "value": str(snap_radius(radius_miles))},
        {"key": "limit", "value": str(limit)},
        {"key": "searchSessionId", "value": session_id},
    ]
    if coordinates is not None:
        params.append({"key": "lat", "value": f"{coordinates[0]:.6f}"})
        params.append({"key": "lon", "value": f"{coordinates[1]:.6f}"})
    zip_code = (zip_code or "").strip()[:5]
    if zip_code.isdigit() and len(zip_code) == 5:
        params.append({"key": "zipcode", "value": zip_code})
    if task.price_min is not None:
        params.append({"key": "price_min", "value": str(task.price_min)})
    if task.price_max is not None:
        params.append({"key": "price_max", "value": str(task.price_max)})
    return params


# --------------------------------------------------------------------------- ingestor


class OfferUpIngestor(BaseIngestor):
    """Polls OfferUp keyword searches around the configured location."""

    name: ClassVar[str] = "offerup"
    kind: ClassVar[SourceKind] = SourceKind.LOCAL
    cfg: "OfferUpSource"

    def __init__(
        self,
        cfg: "OfferUpSource",
        ctx: IngestorContext,
        *,
        base_url: str = OFFERUP_ORIGIN,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__(cfg, ctx)
        self.base_url = base_url.rstrip("/")
        self._clock = clock
        self._cursor = 0
        self._deferred_block: SourceBlocked | None = None
        self._preferred = STRATEGY_PAGE
        self._fallback_since: float | None = None
        self._coordinates: tuple[float, float] | None = (
            (float(cfg.latitude), float(cfg.longitude)) if cfg.latitude is not None and cfg.longitude is not None else None
        )
        self._geocode_retry_at = 0.0
        self._graphql_parked_until = 0.0
        self._request_timeout = float(ctx.http.settings.timeout_seconds)
        self._session_id = str(uuid.uuid4())
        # Client-minted identifiers the web app sends with every GraphQL call: one anonymous
        # device token per process (sticky, like a browser profile) and a session id.
        self._device_token = "web-" + secrets.token_hex(28)
        self._ou_session = f"{self._device_token}@{int(time.time() * 1000)}"
        m = ctx.metrics
        self._m_queries = m.counter("offerup_queries_total", "OfferUp search requests", ("strategy", "outcome"))
        self._m_skipped = m.counter("offerup_items_skipped_total", "Malformed OfferUp feed items", ("reason",))
        self._m_budget = m.counter("offerup_budget_exhausted_total", "Polls that hit the time budget before all queries ran")

    # ------------------------------------------------------------------ poll

    async def poll(self) -> list[RawListing]:
        if self._deferred_block is not None:
            blocked, self._deferred_block = self._deferred_block, None
            raise blocked
        tasks = build_tasks(self.search_profiles())
        if not tasks:
            return []
        started = self._clock()
        budget = max(1.0, float(self.cfg.poll_timeout_seconds) * BUDGET_FRACTION)
        start = self._cursor % len(tasks)
        ordered = tasks[start:] + tasks[:start]
        results: dict[str, RawListing] = {}
        attempted = failed = 0
        last_error: BaseException | None = None
        for task in ordered:
            remaining = budget - (self._clock() - started)
            if remaining <= 0:
                self._m_budget.inc()
                self.log.info(
                    "poll budget exhausted; remaining queries run next poll",
                    extra={"source": self.name, "done": attempted, "total": len(tasks)},
                )
                break
            attempted += 1
            try:
                listings = await asyncio.wait_for(self._search(task), timeout=remaining)
            except asyncio.CancelledError:
                raise
            except SourceBlocked as exc:
                if results:
                    self._deferred_block = exc
                    self.log.warning(
                        "blocked mid-poll; returning partial results", extra={"source": self.name, "error": str(exc)}
                    )
                    break
                raise
            except asyncio.TimeoutError as exc:
                if budget - (self._clock() - started) <= 0:
                    self._m_budget.inc()
                    self.log.info("query cut by poll budget", extra={"source": self.name, "query": task.term})
                    break
                failed += 1  # a timeout from inside the query, not the budget
                last_error = exc
                self.log.warning("offerup query timed out", extra={"source": self.name, "query": task.term})
                continue
            except Exception as exc:  # noqa: BLE001 - one failing query must not sink the poll
                failed += 1
                last_error = exc
                self.log.warning(
                    "offerup query failed", extra={"source": self.name, "query": task.term, "error": repr(exc)[:300]}
                )
                continue
            for raw in listings:
                results.setdefault(raw.source_id, raw)
        self._cursor = (start + max(1, attempted)) % len(tasks)
        if attempted and failed == attempted and last_error is not None:
            raise SourceError(f"all {attempted} offerup queries failed; last: {last_error!r}") from last_error
        return list(results.values())

    # ------------------------------------------------------------------ strategies

    def _strategy_order(self) -> list[str]:
        now = self._clock()
        if (
            self._preferred != STRATEGY_PAGE
            and self._fallback_since is not None
            and now - self._fallback_since >= PRIMARY_RETRY_SECONDS
        ):
            self._preferred, self._fallback_since = STRATEGY_PAGE, None
        other = STRATEGY_GRAPHQL if self._preferred == STRATEGY_PAGE else STRATEGY_PAGE
        order = [self._preferred, other]
        if now < self._graphql_parked_until:
            order.remove(STRATEGY_GRAPHQL)
        return order

    async def _search(self, task: SearchTask) -> list[RawListing]:
        await self._ensure_coordinates()
        last_error: BaseException | None = None
        for strategy in self._strategy_order():
            try:
                if strategy == STRATEGY_PAGE:
                    listings = await self._search_page(task)
                else:
                    listings = await self._search_graphql(task)
            except (SourceBlocked, asyncio.CancelledError):
                raise
            except OfferUpGraphQLUnavailable as exc:
                last_error = exc
                self._graphql_parked_until = self._clock() + BLOCK_COOLDOWN_SECONDS
                self._m_queries.inc(strategy=strategy, outcome="gated")
                self.log.warning(
                    "offerup graphql gated; parking it",
                    extra={"source": self.name, "error": str(exc)[:300], "parked_s": BLOCK_COOLDOWN_SECONDS},
                )
                continue
            except Exception as exc:  # noqa: BLE001 - try the other strategy
                last_error = exc
                self._m_queries.inc(strategy=strategy, outcome="error")
                self.log.debug(
                    "offerup strategy failed", extra={"strategy": strategy, "query": task.term, "error": repr(exc)[:300]}
                )
                continue
            self._m_queries.inc(strategy=strategy, outcome="ok")
            if strategy != self._preferred:
                self.log.info("offerup switching strategy", extra={"source": self.name, "strategy": strategy})
                self._preferred = strategy
                self._fallback_since = self._clock() if strategy != STRATEGY_PAGE else None
            return listings
        raise SourceError(f"offerup search {task.term!r} failed: {last_error!r}") from last_error

    def _common_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        lat, lon = self._coordinates if self._coordinates is not None else (None, None)
        cookie = location_cookie(lat, lon, self.cfg.zip_code)
        if cookie is not None:
            headers["Cookie"] = f"ou.location={cookie}"
            self._forget_jar_location()
        return headers

    def _forget_jar_location(self) -> None:
        """Drop a server-set ``ou.location`` from the shared jar so our header wins."""
        jar = getattr(self.ctx.http.session, "cookie_jar", None)
        clear = getattr(jar, "clear", None)
        if clear is None:
            return
        try:
            clear(lambda morsel: morsel.key == "ou.location")
        except TypeError:  # pragma: no cover - DummyCookieJar / very old aiohttp
            return

    async def _search_page(self, task: SearchTask) -> list[RawListing]:
        resp = await self._call(
            "GET",
            f"{self.base_url}/search",
            params=page_params(task, self.cfg.radius_miles),
            headers=self._common_headers(),
            accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        )
        body = resp.data if isinstance(resp.data, str) else ""
        stats: dict[str, int] = {}
        try:
            listings = await asyncio.to_thread(
                parse_search_page,
                body,
                query=task.term,
                profile_hint=task.profile_id,
                limit=self.cfg.max_listings_per_query,
                stats=stats,
            )
        except OfferUpParseError:
            reason = detect_block(resp.status, body)
            if reason is not None:
                raise self._blocked(f"offerup search page blocked ({reason})", reason) from None
            raise
        self._record_skips(stats)
        return listings

    async def _search_graphql(self, task: SearchTask) -> list[RawListing]:
        payload = {
            "operationName": "GetModularFeed",
            "variables": {
                "debug": False,
                "searchParams": graphql_search_params(
                    task,
                    radius_miles=self.cfg.radius_miles,
                    limit=self.cfg.max_listings_per_query,
                    coordinates=self._coordinates,
                    session_id=self._session_id,
                    zip_code=self.cfg.zip_code,
                ),
            },
            "query": FEED_QUERY,
        }
        data = await self._graphql(payload, referer=f"{OFFERUP_ORIGIN}/search?q={quote(task.term)}")
        stats: dict[str, int] = {}
        listings = await asyncio.to_thread(
            parse_graphql_feed,
            data,
            query=task.term,
            profile_hint=task.profile_id,
            limit=self.cfg.max_listings_per_query,
            stats=stats,
        )
        self._record_skips(stats)
        return listings

    async def _graphql(self, payload: Mapping[str, Any], *, referer: str) -> Any:
        headers = {
            **self._common_headers(),
            "Origin": OFFERUP_ORIGIN,
            "Referer": referer,
            "Sec-Fetch-Site": "same-origin",
            "x-ou-operation-name": str(payload.get("operationName") or ""),
            "x-ou-d-token": self._device_token,
            "ou-session-id": self._ou_session,
            "x-request-id": str(uuid.uuid4()),
        }
        resp = await self._call(
            "POST", f"{self.base_url}/api/graphql", payload=payload, headers=headers, accept="*/*", api=True
        )
        body = resp.data if isinstance(resp.data, str) else ""
        try:
            return json_loads(body)
        except ValueError:
            reason = detect_block(resp.status, body)
            if reason is not None:
                raise self._blocked(f"offerup graphql blocked ({reason})", reason) from None
            raise OfferUpParseError(f"non-JSON GraphQL response ({len(body)} bytes)") from None

    async def _call(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        payload: Any = None,
        headers: Mapping[str, str] | None = None,
        accept: str,
        api: bool = False,
    ) -> HttpResponse:
        """One request; ``api=True`` marks a fetch()-style GraphQL call (CORS fetch metadata)."""
        try:
            return await self.ctx.http.request(
                method,
                url,
                params=params,
                json=payload,
                headers=headers,
                browser_identity=True,
                fetch_mode="cors" if api else "navigate",
                accept=accept,
                parse="text",
                max_bytes=MAX_BODY_BYTES,
                # Explicit per-request timeout: HttpClient passes ``timeout=None`` to aiohttp
                # when none is given, which disables the session's network.timeout_seconds.
                timeout=self._request_timeout,
            )
        except HttpStatusError as exc:
            body = exc.body if exc.status in (401, 403, 429, 503) else None  # walls are often 403/503
            if api and exc.status in (401, 403) and not _BLOCK_RE.search((body or "")[:_SKIP_SCAN_BYTES]):
                raise OfferUpGraphQLUnavailable(f"offerup graphql answered {exc.status} without a challenge") from exc
            reason = detect_block(exc.status, body)
            if reason is not None:
                raise self._blocked(f"offerup {method} {url.rsplit('/', 1)[-1]} blocked ({reason})", reason) from exc
            raise

    def _blocked(self, message: str, reason: str) -> SourceBlocked:
        return SourceBlocked(message, cooldown_seconds=block_cooldown(reason, self.cfg.cooldown_seconds))

    async def _ensure_coordinates(self) -> None:
        """Resolve a ZIP-only config to coordinates once (best effort, retried hourly)."""
        now = self._clock()
        if self._coordinates is not None or not self.cfg.zip_code or now < max(self._geocode_retry_at, self._graphql_parked_until):
            return
        payload = {
            "operationName": "GeocodeLocation",
            "variables": {"input": {"zipcode": str(self.cfg.zip_code).strip()[:5]}},
            "query": GEOCODE_QUERY,
        }
        try:
            coords = parse_geocode(await self._graphql(payload, referer=f"{OFFERUP_ORIGIN}/"))
        except (SourceBlocked, asyncio.CancelledError):
            raise
        except Exception as exc:  # noqa: BLE001 - ZIP cookie still scopes the page search
            coords = None
            if isinstance(exc, OfferUpGraphQLUnavailable):  # same endpoint as the search fallback
                self._graphql_parked_until = self._clock() + BLOCK_COOLDOWN_SECONDS
            self.log.warning("offerup geocode failed", extra={"source": self.name, "error": repr(exc)[:300]})
        if coords is None:
            self._geocode_retry_at = self._clock() + GEOCODE_RETRY_SECONDS
            return
        self._coordinates = coords
        self.log.info("offerup location resolved", extra={"source": self.name, "lat": coords[0], "lon": coords[1]})

    def _record_skips(self, stats: Mapping[str, int]) -> None:
        for reason, count in stats.items():
            if count:
                self._m_skipped.inc(count, reason=reason)


__all__ = [
    "FEED_QUERY",
    "GEOCODE_QUERY",
    "ITEM_URL_TEMPLATE",
    "OFFERUP_ORIGIN",
    "OfferUpGraphQLUnavailable",
    "OfferUpIngestor",
    "OfferUpParseError",
    "RADIUS_CHOICES",
    "SearchTask",
    "block_cooldown",
    "build_tasks",
    "coerce_price",
    "detect_block",
    "extract_next_data",
    "graphql_search_params",
    "iter_listing_objects",
    "location_cookie",
    "page_params",
    "parse_geocode",
    "parse_graphql_feed",
    "parse_listing",
    "parse_search_page",
    "parse_timestamp",
    "snap_radius",
    "split_place",
]
