"""Craigslist ingestor: the search UI's JSON API first, the static result list as fallback.

Craigslist's search pages are a thin JS client over an unauthenticated JSON API
(several open-source clients decode it the same way, validated 2025-2026):

``GET https://sapi.craigslist.org/web/v8/postings/search/full``
    ``batch=<areaId>-0-360-0-0&cc=US&lang=en&searchPath=<cat>&query=<q>&sort=date``
    plus optional ``min_price``/``max_price``, ``srchType=T`` (title-only match),
    ``postal``+``search_distance`` or ``lat``+``lon``+``search_distance``.

The response is compact: ``data.items`` is a list of positional arrays and
``data.decode`` holds the shared tables::

    item[0]  posting-id offset      -> postingId = decode.minPostingId + item[0]
    item[1]  posted-time offset (s) -> epoch     = decode.minPostedDate + item[1]
    item[2]  numeric category id    -> abbreviation via reference.craigslist.org/Categories
    item[3]  price (int; -1/0 = none given)
    item[4]  "locIdx[:descIdx[:hoodIdx]]~lat~lon"  (indexes into decode.locations /
             decode.locationDescriptions / decode.neighborhoods; locations[i] is
             [areaId, hostname, subareaAbbr?])
    tagged   [4, "3:<imgId>", ...] images, [6, "<slug>"] URL slug, [10, "$1,350"] price text,
             [13, token] opaque; the title is the last plain string of the array.

Images are served as ``https://images.craigslist.org/<imgId>_600x450.jpg`` (the
``N:`` prefix of an image ref is dropped; 300x300 / 1200x900 sizes also exist).
Posting URLs are ``https://<host>.craigslist.org/<subarea>/<cat>/d/<slug>/<id>.html``.

Design decisions:

* **Area ids** (needed for ``batch``) and **category abbreviations** (needed for
  canonical URLs) come from ``reference.craigslist.org/Areas`` and ``/Categories``,
  fetched once per process (failures retried hourly). If areas are unavailable the
  static HTML search page is used for that site; its markup embeds ``"areaId":N``,
  which is learned so the next query can use the JSON API.
* **Geo scoping**: the API scopes by the caller's IP unless told otherwise, so we
  always send either the configured ``postal_code`` (+``search_distance_miles``) or the
  site's centre coordinates with a radius.
* **Fallback** to the classic static result list (``li.cl-static-search-result`` with
  ``.title``/``.price``/``.location``) when the JSON API errors. It lacks posting
  times and images, so it is only a fallback; the preference flips back to the API
  after :data:`PRIMARY_RETRY_SECONDS`.
* **Politeness**: everything goes through the shared :class:`HttpClient` (host token
  buckets ``craigslist.org`` in config, sticky browser identity, retries). Queries run
  sequentially inside 80 % of ``poll_timeout_seconds`` with a rotating start, and a
  block wall (HTTP 403/429, "This IP has been automatically blocked") raises
  :class:`SourceBlocked` – partial results of the poll are returned first and the block
  surfaces on the next poll so the base loop applies its cooldown.
* Price ``0``/``-1`` means "no price given" on Craigslist far more often than "free"
  for hardware, so it maps to ``None`` and the normalizer falls back to the title price.
"""

from __future__ import annotations

import asyncio
import html as html_lib
import math
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urljoin, urlsplit

from deal_radar.core.http import HttpResponse, HttpStatusError, json_loads
from deal_radar.core.logs import get_logger
from deal_radar.engine.types import Location, RawListing, SourceKind
from deal_radar.sources.base import BaseIngestor, IngestorContext, SourceBlocked, SourceError

if TYPE_CHECKING:  # pragma: no cover
    from deal_radar.config_schema import CraigslistSource, Profile

log = get_logger("sources.craigslist")

SAPI_ORIGIN = "https://sapi.craigslist.org"
SAPI_SEARCH_PATH = "/web/v8/postings/search/full"
REFERENCE_ORIGIN = "https://reference.craigslist.org"
SITE_URL_TEMPLATE = "https://{site}.craigslist.org"
IMAGE_URL_TEMPLATE = "https://images.craigslist.org/{image_id}_600x450.jpg"
BATCH_SIZE = 360  # the web UI's page size (API maximum)
DEFAULT_SITE_RADIUS_MILES = 60  # radius around a site's centre when no postal code is configured
MAX_IMAGES = 8
MAX_BODY_BYTES = 8 * 1024 * 1024
BUDGET_FRACTION = 0.8
PRIMARY_RETRY_SECONDS = 1800.0
REFERENCE_RETRY_SECONDS = 3600.0
MODE_SAPI = "sapi"
MODE_HTML = "html"

TAG_IMAGES = 4
TAG_SLUG = 6
TAG_PRICE_TEXT = 10

_SITE_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
_IMAGE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{6,128}$")
_SLUG_RE = re.compile(r"^[A-Za-z0-9-]{1,200}$")
_CATEGORY_RE = re.compile(r"^[a-z]{2,6}$")
_POSTING_ID_RE = re.compile(r"/(\d{6,15})\.html(?:[?#].*)?$")
_AREA_ID_RE = re.compile(r'"areaId"\s*:\s*(\d+)')
_PLAIN_PRICE_RE = re.compile(r"^\$?\s*(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?$")
_BLOCK_RE = re.compile(
    r"ip has been automatically blocked|your request has been blocked|blocks-[a-z0-9.]*@craigslist"
    r"|<title>\s*access denied\s*</title>|captcha",
    re.I,
)
_COUNTRY_CURRENCY = {
    "US": "USD", "CA": "CAD", "GB": "GBP", "UK": "GBP", "AU": "AUD", "NZ": "NZD",
    "IE": "EUR", "DE": "EUR", "FR": "EUR", "ES": "EUR", "IT": "EUR", "NL": "EUR", "BE": "EUR",
    "AT": "EUR", "PT": "EUR", "FI": "EUR", "GR": "EUR",
}


class CraigslistParseError(SourceError):
    """The response did not have the expected structure."""


# --------------------------------------------------------------------------- reference data


@dataclass(frozen=True, slots=True)
class AreaInfo:
    area_id: int
    hostname: str
    country: str | None = None
    latitude: float | None = None
    longitude: float | None = None

    @property
    def currency(self) -> str:
        return _COUNTRY_CURRENCY.get((self.country or "US").upper(), "USD")

    @property
    def country_code(self) -> str:
        code = (self.country or "US").upper()
        return code if len(code) == 2 and code.isalpha() else "US"


def _pick(entry: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in entry and entry[key] not in (None, ""):
            return entry[key]
    return None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def normalize_site(site: str) -> str | None:
    """``"newyork"`` / ``"newyork.craigslist.org"`` / ``"https://newyork.craigslist.org/"`` -> ``"newyork"``."""
    text = (site or "").strip().lower()
    if "://" in text:
        text = urlsplit(text).hostname or ""
    text = text.strip("/")
    if text.endswith(".craigslist.org"):
        text = text[: -len(".craigslist.org")]
    return text if _SITE_RE.match(text) else None


def parse_areas(payload: Any) -> dict[str, AreaInfo]:
    """``reference.craigslist.org/Areas`` -> {hostname/abbreviation: AreaInfo}."""
    out: dict[str, AreaInfo] = {}
    if not isinstance(payload, list):
        return out
    by_abbreviation: dict[str, AreaInfo] = {}
    for entry in payload:
        if not isinstance(entry, Mapping):
            continue
        area_id = _as_int(_pick(entry, "AreaID", "AreaId", "areaId", "areaID"))
        hostname = _pick(entry, "Hostname", "hostname")
        if area_id is None or not isinstance(hostname, str) or not hostname.strip():
            continue
        country = _pick(entry, "Country", "country")
        info = AreaInfo(
            area_id=area_id,
            hostname=hostname.strip().lower(),
            country=country.strip() if isinstance(country, str) else None,
            latitude=_as_float(_pick(entry, "Latitude", "latitude", "lat")),
            longitude=_as_float(_pick(entry, "Longitude", "longitude", "lng", "lon")),
        )
        out[info.hostname] = info
        abbreviation = _pick(entry, "Abbreviation", "abbreviation")
        if isinstance(abbreviation, str) and abbreviation.strip():
            by_abbreviation.setdefault(abbreviation.strip().lower(), info)
    for key, info in by_abbreviation.items():
        out.setdefault(key, info)  # hostnames win over abbreviations
    return out


def parse_categories(payload: Any) -> dict[int, str]:
    """``reference.craigslist.org/Categories`` -> {categoryId: abbreviation}."""
    out: dict[int, str] = {}
    if not isinstance(payload, list):
        return out
    for entry in payload:
        if not isinstance(entry, Mapping):
            continue
        cat_id = _as_int(_pick(entry, "CategoryID", "CategoryId", "categoryId"))
        abbreviation = _pick(entry, "Abbreviation", "abbreviation")
        if cat_id is not None and isinstance(abbreviation, str) and _CATEGORY_RE.match(abbreviation.strip().lower()):
            out[cat_id] = abbreviation.strip().lower()
    return out


def extract_area_id(page: str) -> int | None:
    """Numeric area id embedded in a Craigslist page (``"areaId":3``)."""
    match = _AREA_ID_RE.search(page or "")
    return int(match.group(1)) if match else None


# --------------------------------------------------------------------------- pure helpers


def detect_block(status: int, body: str | None) -> str | None:
    """Short reason when a response is a block wall, else ``None``."""
    if status in (403, 429):
        return f"http_{status}"
    if body:
        match = _BLOCK_RE.search(body[:200_000])
        if match:
            return f"wall:{match.group(0).strip().lower()[:40]}"
    return None


def image_url(ref: Any) -> str | None:
    """``"3:00a0a_jx892ZIFraf_0CI0qt"`` -> ``https://images.craigslist.org/00a0a_..._600x450.jpg``."""
    if not isinstance(ref, str):
        return None
    image_id = ref.split(":", 1)[1] if ":" in ref else ref
    image_id = image_id.strip()
    return IMAGE_URL_TEMPLATE.format(image_id=image_id) if _IMAGE_ID_RE.match(image_id) else None


def posting_url(host: str, subarea: str | None, category: str, slug: str | None, posting_id: int | str) -> str:
    """Canonical posting URL (Craigslist resolves postings by id, the path mirrors its own links)."""
    path = [p for p in (subarea, category) if p]
    if slug:
        path += ["d", slug]
    return f"https://{host}.craigslist.org/{'/'.join(path)}/{posting_id}.html"


def coerce_price(value: Any) -> float | str | None:
    """Positive numbers -> float, ``0``/``-1``/blank -> ``None``, other text passed through."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(float(value)) and value > 0 else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        match = _PLAIN_PRICE_RE.match(text)
        if match:
            amount = float(match.group(1).replace(",", "") + (match.group(2) or ""))
            return amount if amount > 0 else None
        return text
    return None


def _epoch(value: Any) -> datetime | None:
    seconds = _as_float(value)
    if seconds is None:
        return None
    if seconds > 1e11:  # milliseconds
        seconds /= 1000.0
    if not 1.1e9 <= seconds <= 4.1e9:
        return None
    moment = datetime.fromtimestamp(seconds, tz=timezone.utc)
    if moment > datetime.now(timezone.utc) + timedelta(days=1):
        return None
    return moment


def _clean(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    return " ".join(html_lib.unescape(text).split())


def _table(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _at(table: Sequence[Any], index: int | None) -> Any:
    if index is None or index < 0 or index >= len(table):
        return None
    return table[index]


@dataclass(frozen=True, slots=True)
class _Decode:
    min_posting_id: int
    min_posted_date: int
    locations: list[Any]
    descriptions: list[Any]
    neighborhoods: list[Any]

    @classmethod
    def from_payload(cls, raw: Any) -> "_Decode":
        raw = raw if isinstance(raw, Mapping) else {}  # zero-result responses carry "decode": 0
        return cls(
            min_posting_id=_as_int(raw.get("minPostingId")) or 0,
            min_posted_date=_as_int(raw.get("minPostedDate")) or 0,
            locations=_table(raw.get("locations")),
            descriptions=_table(raw.get("locationDescriptions")),
            neighborhoods=_table(raw.get("neighborhoods")),
        )


@dataclass(frozen=True, slots=True)
class DecodeContext:
    """Per-request facts the item decoder needs besides the payload."""

    site: str
    category: str  # search path, used when a category id is unknown
    categories: Mapping[int, str]
    currency: str = "USD"
    query: str | None = None
    profile_hint: str | None = None


def parse_geo(value: Any) -> tuple[int | None, int | None, int | None, float | None, float | None]:
    """``"1:2:3~40.71~-73.99"`` -> (locIdx, descIdx, hoodIdx, lat, lon). Empty index slots are 0."""
    if not isinstance(value, str) or not value:
        return None, None, None, None, None
    parts = value.split("~")
    indexes: list[int | None] = []
    for chunk in parts[0].split(":")[:3]:
        chunk = chunk.strip()
        indexes.append(int(chunk) if chunk.isdigit() else 0 if chunk == "" else None)
    while len(indexes) < 3:
        indexes.append(None)
    lat = _as_float(parts[1]) if len(parts) > 1 else None
    lon = _as_float(parts[2]) if len(parts) > 2 else None
    if lat is not None and not -90 <= lat <= 90:
        lat = None
    if lon is not None and not -180 <= lon <= 180:
        lon = None
    return indexes[0], indexes[1], indexes[2], lat, lon


def _build_listing(
    ctx: DecodeContext,
    *,
    posting_id: int,
    title: str,
    price: float | str | None,
    host: str | None,
    subarea: str | None,
    category: str | None,
    slug: str | None,
    posted_at: datetime | None,
    images: list[str],
    location: Location | None,
    extra: dict[str, Any],
) -> RawListing:
    host = host if host and _SITE_RE.match(host) else ctx.site
    subarea = subarea if subarea and _CATEGORY_RE.match(subarea) else None
    category = category if category and _CATEGORY_RE.match(category) else ctx.category
    slug = slug if slug and _SLUG_RE.match(slug) else None
    extra = {"site": host, **({"subarea": subarea} if subarea else {}), "category": category, **extra}
    return RawListing(
        source="craigslist",
        source_kind=SourceKind.LOCAL,
        source_id=str(posting_id),
        url=posting_url(host, subarea, category, slug, posting_id),
        title=title,
        price=price,
        currency=ctx.currency,
        location=location,
        image_urls=images[:MAX_IMAGES],
        posted_at=posted_at,
        query=ctx.query,
        profile_hint=ctx.profile_hint,
        extra=extra,
    )


def decode_compact_item(item: Any, decode: _Decode, ctx: DecodeContext) -> RawListing | None:
    """Decode one positional-array item of ``search/full`` (``None`` when unusable)."""
    if not isinstance(item, list) or len(item) < 6:
        return None
    offset = _as_int(item[0])
    if offset is None or offset < 0:
        return None
    posting_id = decode.min_posting_id + offset if decode.min_posting_id else offset
    if posting_id < 100_000:
        return None
    title_index = next((i for i in range(len(item) - 1, 4, -1) if isinstance(item[i], str)), None)
    if title_index is None:
        return None
    title = _clean(item[title_index])
    if not title:
        return None
    tags: dict[int, list[Any]] = {}
    for element in item[5:title_index]:
        if isinstance(element, list) and element and isinstance(element[0], int) and not isinstance(element[0], bool):
            tags.setdefault(element[0], element[1:])
    date_offset = _as_int(item[1])
    posted_at = _epoch(decode.min_posted_date + date_offset) if decode.min_posted_date and date_offset is not None else None
    category_id = _as_int(item[2])
    price_text = next((v for v in tags.get(TAG_PRICE_TEXT, []) if isinstance(v, str)), None)
    price = coerce_price(item[3])
    if price is None and price_text:
        price = coerce_price(price_text)
    loc_idx, desc_idx, hood_idx, lat, lon = parse_geo(item[4])
    area = _at(decode.locations, loc_idx)
    host = area[1] if isinstance(area, list) and len(area) > 1 and isinstance(area[1], str) else None
    subarea = area[2] if isinstance(area, list) and len(area) > 2 and isinstance(area[2], str) else None
    description = _at(decode.descriptions, desc_idx)
    hood = _at(decode.neighborhoods, hood_idx)
    description = _clean(description) or None
    hood = _clean(hood) or None
    location = None
    if description or hood or lat is not None:
        location = Location(text=description or hood, latitude=lat, longitude=lon)
    slug = next((v for v in tags.get(TAG_SLUG, []) if isinstance(v, str)), None)
    images = [u for u in (image_url(ref) for ref in tags.get(TAG_IMAGES, [])) if u]
    extra: dict[str, Any] = {"via": MODE_SAPI}
    if category_id is not None:
        extra["category_id"] = category_id
    if price_text:
        extra["price_text"] = price_text
    if hood:
        extra["neighborhood"] = hood
    return _build_listing(
        ctx,
        posting_id=posting_id,
        title=title,
        price=price,
        host=host,
        subarea=subarea,
        category=ctx.categories.get(category_id) if category_id is not None else None,
        slug=slug,
        posted_at=posted_at,
        images=images,
        location=location,
        extra=extra,
    )


def decode_object_item(item: Any, ctx: DecodeContext) -> RawListing | None:
    """Decode a dict-shaped item (the non-``full`` search endpoint returns these)."""
    if not isinstance(item, Mapping):
        return None
    posting_id = _as_int(item.get("postingId") or item.get("id"))
    title = _clean(item.get("title"))
    if posting_id is None or posting_id < 100_000 or not title:
        return None
    loc = item.get("location")
    loc = loc if isinstance(loc, Mapping) else {}
    price_text = item.get("priceString") if isinstance(item.get("priceString"), str) else None
    price = coerce_price(item.get("price"))
    if price is None and price_text:
        price = coerce_price(price_text)
    description = _clean(loc.get("description")) or None
    lat = _as_float(loc.get("lat") if loc.get("lat") is not None else item.get("lat"))
    lon = _as_float(loc.get("lon") if loc.get("lon") is not None else item.get("lon"))
    images_raw = item.get("images")
    images = [u for u in (image_url(r) for r in images_raw) if u] if isinstance(images_raw, list) else []
    extra: dict[str, Any] = {"via": MODE_SAPI}
    if price_text:
        extra["price_text"] = price_text
    return _build_listing(
        ctx,
        posting_id=posting_id,
        title=title,
        price=price,
        host=loc.get("hostname") if isinstance(loc.get("hostname"), str) else None,
        subarea=loc.get("subareaAbbr") if isinstance(loc.get("subareaAbbr"), str) else None,
        category=item.get("categoryAbbr") if isinstance(item.get("categoryAbbr"), str) else None,
        slug=item.get("seo") if isinstance(item.get("seo"), str) else None,
        posted_at=_epoch(item.get("postedDate")),
        images=images,
        location=Location(text=description, latitude=lat, longitude=lon) if (description or lat is not None) else None,
        extra=extra,
    )


def decode_search_response(payload: Any, ctx: DecodeContext, *, stats: dict[str, int] | None = None) -> list[RawListing]:
    """Listings from a ``sapi`` search response (compact arrays or objects)."""
    if not isinstance(payload, Mapping):
        raise CraigslistParseError(f"unexpected payload type {type(payload).__name__}")
    data = payload.get("data")
    errors = payload.get("errors")
    if not isinstance(data, Mapping):
        raise CraigslistParseError(f"response without data (errors: {str(errors)[:200]})")
    items = data.get("items")
    if items is None:
        if errors:
            raise CraigslistParseError(f"search API errors: {str(errors)[:200]}")
        return []
    if not isinstance(items, list):
        raise CraigslistParseError("data.items is not a list")
    decode = _Decode.from_payload(data.get("decode"))
    out: list[RawListing] = []
    seen: set[str] = set()
    for item in items:
        try:
            raw = decode_compact_item(item, decode, ctx) if isinstance(item, list) else decode_object_item(item, ctx)
            reason = "unusable"
        except (ValueError, TypeError, IndexError) as exc:  # pydantic ValidationError is a ValueError
            log.debug("craigslist item rejected", extra={"error": repr(exc)[:200]})
            raw, reason = None, "invalid"
        if raw is None:
            if stats is not None:
                stats[reason] = stats.get(reason, 0) + 1
            continue
        if raw.source_id not in seen:
            seen.add(raw.source_id)
            out.append(raw)
    return out


class _StaticResultsParser(HTMLParser):
    """Collects ``li.cl-static-search-result`` rows (title/price/location/link)."""

    _FIELDS = ("title", "price", "location")

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[dict[str, str]] = []
        self._row: dict[str, str] | None = None
        self._field: str | None = None
        self._field_depth = 0
        self._div_depth = 0
        self._buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {k: v or "" for k, v in attrs}
        classes = attributes.get("class", "").split()
        if tag == "li" and "cl-static-search-result" in classes:
            self._finish_row()
            self._row = {"title_attr": attributes.get("title", "")}
            self._div_depth = 0
            return
        if self._row is None:
            return
        if tag == "a" and "href" not in self._row and attributes.get("href"):
            self._row["href"] = attributes["href"]
        elif tag == "div":
            self._div_depth += 1
            if self._field is None:
                field = next((f for f in self._FIELDS if f in classes), None)
                if field is not None:
                    self._field, self._field_depth, self._buffer = field, self._div_depth, []

    def handle_endtag(self, tag: str) -> None:
        if self._row is None:
            return
        if tag == "div":
            if self._field is not None and self._div_depth == self._field_depth:
                self._row.setdefault(self._field, " ".join("".join(self._buffer).split()))
                self._field = None
            self._div_depth = max(0, self._div_depth - 1)
        elif tag == "li":
            self._finish_row()

    def handle_data(self, data: str) -> None:
        if self._field is not None:
            self._buffer.append(data)

    def close(self) -> None:
        super().close()
        self._finish_row()

    def _finish_row(self) -> None:
        if self._row is not None:
            if self._field is not None:
                self._row.setdefault(self._field, " ".join("".join(self._buffer).split()))
            self.rows.append(self._row)
        self._row, self._field = None, None


def parse_search_html(
    page: str,
    ctx: DecodeContext,
    *,
    base_url: str | None = None,
    stats: dict[str, int] | None = None,
) -> list[RawListing]:
    """Listings from the static (no-JS) search result list of a Craigslist site."""
    parser = _StaticResultsParser()
    parser.feed(page or "")
    parser.close()
    base = base_url or SITE_URL_TEMPLATE.format(site=ctx.site)
    out: list[RawListing] = []
    seen: set[str] = set()
    for row in parser.rows:
        href = row.get("href", "")
        url = urljoin(base + "/", href) if href else ""
        match = _POSTING_ID_RE.search(urlsplit(url).path) if url else None
        title = _clean(row.get("title") or row.get("title_attr"))
        if not match or not title or not url.startswith(("http://", "https://")):
            if stats is not None:
                stats["unusable"] = stats.get("unusable", 0) + 1
            continue
        posting_id = match.group(1)
        if posting_id in seen:
            continue
        seen.add(posting_id)
        place = _clean(row.get("location")) or None
        price_text = _clean(row.get("price")) or None
        host = (urlsplit(url).hostname or "").removesuffix(".craigslist.org")
        extra: dict[str, Any] = {"via": MODE_HTML, "site": host if _SITE_RE.match(host) else ctx.site}
        if price_text:
            extra["price_text"] = price_text
        try:
            raw = RawListing(
                source="craigslist",
                source_kind=SourceKind.LOCAL,
                source_id=posting_id,
                url=url,
                title=title,
                price=coerce_price(price_text),
                currency=ctx.currency,
                location=Location(text=place) if place else None,
                query=ctx.query,
                profile_hint=ctx.profile_hint,
                extra=extra,
            )
        except ValueError as exc:
            log.debug("craigslist html row rejected", extra={"error": repr(exc)[:200]})
            if stats is not None:
                stats["invalid"] = stats.get("invalid", 0) + 1
            continue
        out.append(raw)
    if not out and "cl-static-search-result" not in (page or ""):
        raise CraigslistParseError("no static search results markup in page")
    return out


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


def sapi_params(
    task: SearchTask,
    *,
    area: AreaInfo,
    category: str,
    postal_code: str | None,
    distance_miles: int | None,
) -> dict[str, str]:
    params = {
        "batch": f"{area.area_id}-0-{BATCH_SIZE}-0-0",
        "cc": area.country_code,
        "lang": "en",
        "searchPath": category,
        "query": task.term,
        "sort": "date",
        "srchType": "T",
    }
    if task.price_min is not None and task.price_min > 0:
        params["min_price"] = str(task.price_min)
    if task.price_max is not None:
        params["max_price"] = str(task.price_max)
    if postal_code:
        params["postal"] = postal_code
        if distance_miles:
            params["search_distance"] = str(distance_miles)
    elif area.latitude is not None and area.longitude is not None:
        params["lat"] = f"{area.latitude:.5f}"
        params["lon"] = f"{area.longitude:.5f}"
        params["search_distance"] = str(distance_miles or DEFAULT_SITE_RADIUS_MILES)
    return params


def html_params(task: SearchTask, *, postal_code: str | None, distance_miles: int | None) -> dict[str, str]:
    params = {"query": task.term, "sort": "date", "srchType": "T"}
    if task.price_min is not None and task.price_min > 0:
        params["min_price"] = str(task.price_min)
    if task.price_max is not None:
        params["max_price"] = str(task.price_max)
    if postal_code:
        params["postal"] = postal_code
        if distance_miles:
            params["search_distance"] = str(distance_miles)
    return params


# --------------------------------------------------------------------------- ingestor


class CraigslistIngestor(BaseIngestor):
    """Polls Craigslist keyword searches for each configured site."""

    name: ClassVar[str] = "craigslist"
    kind: ClassVar[SourceKind] = SourceKind.LOCAL
    cfg: "CraigslistSource"

    def __init__(
        self,
        cfg: "CraigslistSource",
        ctx: IngestorContext,
        *,
        sapi_origin: str = SAPI_ORIGIN,
        reference_origin: str = REFERENCE_ORIGIN,
        site_url_template: str = SITE_URL_TEMPLATE,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__(cfg, ctx)
        self.sapi_origin = sapi_origin.rstrip("/")
        self.reference_origin = reference_origin.rstrip("/")
        self.site_url_template = site_url_template
        self._clock = clock
        self.sites: list[str] = []
        for raw_site in cfg.sites:
            site = normalize_site(raw_site)
            if site is None:
                self.log.warning("ignoring invalid craigslist site", extra={"source": self.name, "site": raw_site})
            elif site not in self.sites:
                self.sites.append(site)
        self.category = (cfg.category or "sss").strip().lower()
        if not _CATEGORY_RE.match(self.category):
            self.log.warning("invalid craigslist category; using sss", extra={"source": self.name, "category": cfg.category})
            self.category = "sss"
        self.postal_code = (cfg.postal_code or "").strip() or None
        self._areas: dict[str, AreaInfo] = {}
        self._categories: dict[int, str] = {}
        self._areas_loaded = False
        self._categories_loaded = False
        self._areas_retry_at = 0.0
        self._categories_retry_at = 0.0
        self._cursor = 0
        self._deferred_block: SourceBlocked | None = None
        self._preferred = MODE_SAPI
        self._fallback_since: float | None = None
        m = ctx.metrics
        self._m_queries = m.counter("craigslist_queries_total", "Craigslist search requests", ("mode", "outcome"))
        self._m_skipped = m.counter("craigslist_items_skipped_total", "Malformed Craigslist result items", ("reason",))
        self._m_budget = m.counter("craigslist_budget_exhausted_total", "Polls that hit the time budget before all queries ran")

    # ------------------------------------------------------------------ poll

    async def poll(self) -> list[RawListing]:
        if self._deferred_block is not None:
            blocked, self._deferred_block = self._deferred_block, None
            raise blocked
        jobs = [(task, site) for task in build_tasks(self.search_profiles()) for site in self.sites]
        if not jobs:
            return []
        started = self._clock()
        budget = max(1.0, float(self.cfg.poll_timeout_seconds) * BUDGET_FRACTION)
        await self._ensure_reference(timeout=budget * 0.4)
        start = self._cursor % len(jobs)
        ordered = jobs[start:] + jobs[:start]
        results: dict[str, RawListing] = {}
        attempted = failed = 0
        last_error: BaseException | None = None
        for task, site in ordered:
            remaining = budget - (self._clock() - started)
            if remaining <= 0:
                self._m_budget.inc()
                self.log.info(
                    "poll budget exhausted; remaining queries run next poll",
                    extra={"source": self.name, "done": attempted, "total": len(jobs)},
                )
                break
            attempted += 1
            try:
                listings = await asyncio.wait_for(self._search(site, task), timeout=remaining)
            except asyncio.CancelledError:
                raise
            except SourceBlocked as exc:
                if results:
                    self._deferred_block = exc
                    self.log.warning("blocked mid-poll; returning partial results", extra={"source": self.name, "error": str(exc)})
                    break
                raise
            except asyncio.TimeoutError as exc:
                if budget - (self._clock() - started) <= 0:
                    self._m_budget.inc()
                    self.log.info("query cut by poll budget", extra={"source": self.name, "query": task.term, "site": site})
                    break
                failed += 1
                last_error = exc
                self.log.warning("craigslist query timed out", extra={"source": self.name, "query": task.term, "site": site})
                continue
            except Exception as exc:  # noqa: BLE001 - one failing query must not sink the poll
                failed += 1
                last_error = exc
                self.log.warning(
                    "craigslist query failed",
                    extra={"source": self.name, "query": task.term, "site": site, "error": repr(exc)[:300]},
                )
                continue
            for raw in listings:
                results.setdefault(raw.source_id, raw)
        self._cursor = (start + max(1, attempted)) % len(jobs)
        if attempted and failed == attempted and last_error is not None:
            raise SourceError(f"all {attempted} craigslist queries failed; last: {last_error!r}") from last_error
        return list(results.values())

    # ------------------------------------------------------------------ reference data

    async def _ensure_reference(self, *, timeout: float) -> None:
        try:
            await asyncio.wait_for(self._load_reference(), timeout=max(1.0, timeout))
        except asyncio.TimeoutError:
            now = self._clock()
            if not self._areas_loaded:
                self._areas_retry_at = now + REFERENCE_RETRY_SECONDS
            if not self._categories_loaded:
                self._categories_retry_at = now + REFERENCE_RETRY_SECONDS
            self.log.warning("craigslist reference data timed out", extra={"source": self.name})

    async def _load_reference(self) -> None:
        missing = [s for s in self.sites if s not in self._areas]
        if self._categories_loaded and (self._areas_loaded or not missing):
            return
        now = self._clock()
        if not self._categories_loaded and now >= self._categories_retry_at:
            categories = await self._fetch_reference("Categories", parse_categories)
            if categories:
                self._categories.update(categories)
                self._categories_loaded = True
            else:
                self._categories_retry_at = now + REFERENCE_RETRY_SECONDS
        if missing and not self._areas_loaded and now >= self._areas_retry_at:
            areas = await self._fetch_reference("Areas", parse_areas)
            if areas:
                for site in self.sites:
                    if site in areas:
                        self._areas[site] = areas[site]
                    else:
                        self.log.warning("craigslist site not found in area list", extra={"source": self.name, "site": site})
                self._areas_loaded = True
            else:
                self._areas_retry_at = now + REFERENCE_RETRY_SECONDS

    async def _fetch_reference(self, name: str, parser: Callable[[Any], dict[Any, Any]]) -> dict[Any, Any]:
        try:
            resp = await self._call("GET", f"{self.reference_origin}/{name}", accept="application/json")
            payload = json_loads(resp.data if isinstance(resp.data, str) else "")
            return await asyncio.to_thread(parser, payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - reference data is an optimisation, never fatal
            self.log.warning("craigslist reference fetch failed", extra={"source": self.name, "table": name, "error": repr(exc)[:300]})
            return {}

    # ------------------------------------------------------------------ search modes

    def _mode_order(self, site: str) -> list[str]:
        if site not in self._areas:
            return [MODE_HTML]  # the JSON API needs the numeric area id
        if (
            self._preferred != MODE_SAPI
            and self._fallback_since is not None
            and self._clock() - self._fallback_since >= PRIMARY_RETRY_SECONDS
        ):
            self._preferred, self._fallback_since = MODE_SAPI, None
        return [MODE_SAPI, MODE_HTML] if self._preferred == MODE_SAPI else [MODE_HTML, MODE_SAPI]

    async def _search(self, site: str, task: SearchTask) -> list[RawListing]:
        modes = self._mode_order(site)
        last_error: BaseException | None = None
        for mode in modes:
            try:
                if mode == MODE_SAPI:
                    listings = await self._search_sapi(site, self._areas[site], task)
                else:
                    listings = await self._search_html(site, task)
            except (SourceBlocked, asyncio.CancelledError):
                raise
            except Exception as exc:  # noqa: BLE001 - try the other mode
                last_error = exc
                self._m_queries.inc(mode=mode, outcome="error")
                self.log.debug("craigslist mode failed", extra={"mode": mode, "query": task.term, "site": site, "error": repr(exc)[:300]})
                continue
            self._m_queries.inc(mode=mode, outcome="ok")
            if len(modes) > 1 and mode != self._preferred:
                self.log.info("craigslist switching search mode", extra={"source": self.name, "mode": mode})
                self._preferred = mode
                self._fallback_since = self._clock() if mode != MODE_SAPI else None
            return listings
        raise SourceError(f"craigslist search {task.term!r} on {site} failed: {last_error!r}") from last_error

    def _context(self, site: str, task: SearchTask) -> DecodeContext:
        area = self._areas.get(site)
        return DecodeContext(
            site=site,
            category=self.category,
            categories=self._categories,
            currency=area.currency if area is not None else "USD",
            query=task.term,
            profile_hint=task.profile_id,
        )

    def _site_base(self, site: str) -> str:
        return self.site_url_template.format(site=site).rstrip("/")

    async def _search_sapi(self, site: str, area: AreaInfo, task: SearchTask) -> list[RawListing]:
        site_base = self._site_base(site)
        parts = urlsplit(site_base)
        origin = f"{parts.scheme}://{parts.netloc}"
        headers = {
            "Origin": origin,
            "Referer": f"{site_base}/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }
        params = sapi_params(
            task,
            area=area,
            category=self.category,
            postal_code=self.postal_code,
            distance_miles=self.cfg.search_distance_miles,
        )
        resp = await self._call(
            "GET",
            f"{self.sapi_origin}{SAPI_SEARCH_PATH}",
            params=params,
            headers=headers,
            accept="application/json, text/plain, */*",
        )
        body = resp.data if isinstance(resp.data, str) else ""
        try:
            payload = json_loads(body)
        except ValueError:
            reason = detect_block(resp.status, body)
            if reason is not None:
                raise SourceBlocked(f"craigslist search API blocked ({reason})") from None
            raise CraigslistParseError(f"non-JSON search API response ({len(body)} bytes)") from None
        stats: dict[str, int] = {}
        listings = await asyncio.to_thread(decode_search_response, payload, self._context(site, task), stats=stats)
        self._record_skips(stats)
        return listings

    async def _search_html(self, site: str, task: SearchTask) -> list[RawListing]:
        site_base = self._site_base(site)
        resp = await self._call(
            "GET",
            f"{site_base}/search/{self.category}",
            params=html_params(task, postal_code=self.postal_code, distance_miles=self.cfg.search_distance_miles),
            accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        )
        body = resp.data if isinstance(resp.data, str) else ""
        if site not in self._areas:
            area_id = extract_area_id(body)
            if area_id is not None:
                self._areas[site] = AreaInfo(area_id=area_id, hostname=site)
                self.log.info("craigslist area id learned from page", extra={"source": self.name, "site": site, "area_id": area_id})
        stats: dict[str, int] = {}
        try:
            listings = await asyncio.to_thread(parse_search_html, body, self._context(site, task), base_url=site_base, stats=stats)
        except CraigslistParseError:
            reason = detect_block(resp.status, body)
            if reason is not None:
                raise SourceBlocked(f"craigslist search page blocked ({reason})") from None
            raise
        self._record_skips(stats)
        return listings

    async def _call(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        accept: str,
    ) -> HttpResponse:
        try:
            return await self.ctx.http.request(
                method,
                url,
                params=params,
                headers=headers,
                browser_identity=True,
                accept=accept,
                parse="text",
                max_bytes=MAX_BODY_BYTES,
            )
        except HttpStatusError as exc:
            reason = detect_block(exc.status, exc.body if exc.status in (401, 403, 429) else None)
            if reason is not None:
                raise SourceBlocked(f"craigslist {urlsplit(url).hostname} blocked ({reason})") from exc
            raise

    def _record_skips(self, stats: Mapping[str, int]) -> None:
        for reason, count in stats.items():
            if count:
                self._m_skipped.inc(count, reason=reason)


__all__ = [
    "AreaInfo",
    "CraigslistIngestor",
    "CraigslistParseError",
    "DecodeContext",
    "IMAGE_URL_TEMPLATE",
    "SAPI_ORIGIN",
    "SAPI_SEARCH_PATH",
    "SearchTask",
    "build_tasks",
    "coerce_price",
    "decode_compact_item",
    "decode_object_item",
    "decode_search_response",
    "detect_block",
    "extract_area_id",
    "html_params",
    "image_url",
    "normalize_site",
    "parse_areas",
    "parse_categories",
    "parse_geo",
    "parse_search_html",
    "posting_url",
    "sapi_params",
]
