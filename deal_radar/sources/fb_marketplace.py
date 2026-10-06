"""Facebook Marketplace ingestor: a real, logged-in Chromium session read like a person would.

Facebook has no public Marketplace API and is the most bot-sensitive source DealRadar
reads, so this ingestor is built around restraint rather than throughput:

* **One browser, one page, for the life of the process.** Spawning a fresh browser
  per poll is slow (1-2 s of startup) and looks like a new device every few minutes.
  The page is reused for every query; ``storage_state`` (cookies) is saved back after
  each successful poll so refreshed session cookies survive restarts.
* **The operator's own session.** ``--login`` opens a *headed* browser where the
  operator signs in by hand (2FA included); the session is stored with 0600
  permissions. Nothing here types credentials, solves CAPTCHAs or works around a
  checkpoint: a login wall, checkpoint or "temporarily blocked" page raises
  :class:`SourceBlocked` with a long cooldown and the base loop notifies the operator.
* **Sequential, paced queries.** Search terms run one after another with human-like
  pauses and wheel scrolling. A poll is time-boxed to ``poll_timeout_seconds``;
  terms that did not fit continue next poll from a rotating cursor, so every term is
  covered over a few polls without ever bursting.

Data extraction (most robust first, merged and de-duplicated by listing id):

1. **Server-rendered JSON** — the first page of results is embedded in the HTML as
   ``<script type="application/json">`` Relay preloader payloads.
2. **GraphQL responses** — scrolling triggers ``POST /api/graphql/`` pagination
   queries, captured with ``page.on("response")``. Bodies may carry the ``for (;;);``
   anti-JSON-hijacking prefix and several newline-delimited JSON documents
   (``@defer``/``@stream`` chunks), see :func:`iter_json_documents`.
3. **DOM fallback** — when no JSON was found, item cards (``a[href*="/marketplace/item/"]``)
   are read with a single ``page.evaluate``.

Facebook reshuffles GraphQL paths regularly, so listings are found by walking every
document generically for objects that look like a listing (``marketplace_listing_title``
plus ``listing_price``/``id``) rather than by one hard-coded path. The commonly
observed path is ``data.marketplace_search.feed_units.edges[].node.listing`` with
fields ``id``, ``marketplace_listing_title``, ``listing_price{amount, formatted_amount}``,
``strikethrough_price``, ``primary_listing_photo.image.uri``,
``location.reverse_geocode{city, state, city_page.display_name}``, ``is_sold``,
``is_pending``, ``delivery_types``, ``marketplace_listing_seller{name}`` (field names as
used by several independent open-source Marketplace tools; not an official contract).
Search results rarely carry a timestamp, so ``posted_at`` is only set when a
``creation_time`` is present.

URL parameters (``minPrice``/``maxPrice`` in whole currency units, ``daysSinceListed``
∈ {1, 7, 30}, ``sortBy=creation_time_descend``, ``exact=false``) are the ones Facebook's
own UI produces; newest-first also returns the most complete result set. Location comes
from the path (``city_slug`` or a numeric location id). No ``radius`` parameter is sent:
for logged-in sessions Facebook ignores it and applies the radius saved on the account
(Marketplace → Location). The ingestor reads the applied ``filter_radius_km`` from the
page once and warns when it disagrees with ``location.radius_km``, and warns when an
unrecognised slug was redirected to the account's default location.

Recommended production setup (most to least important): the operator's home
connection, a long-lived profile (``browser.user_data_dir``; fresh profiles get
stricter treatment), and headed real Chrome (``executable_path`` to Google Chrome,
``headless: false`` under Xvfb). Headless Chromium with :mod:`deal_radar.sources.stealth`
works but is the weakest of these.

CLI (run on a machine with a display, ideally the home connection the collector uses)::

    python -m deal_radar.sources.fb_marketplace --config deal_radar/config.yaml --login
    python -m deal_radar.sources.fb_marketplace --config deal_radar/config.yaml --check
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import re
import sys
import time
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import quote, urlencode, urlsplit

from pydantic import ValidationError

from deal_radar.config_schema import AppConfig, ConfigError, FbMarketplaceSource, GeoPin, load_config, load_dotenv
from deal_radar.core.http import json_loads
from deal_radar.core.logs import configure_logging
from deal_radar.engine.types import Location, RawListing, SellerInfo, SourceKind
from deal_radar.sources.base import BaseIngestor, IngestorContext, SourceAuthError, SourceBlocked, SourceError
from deal_radar.sources.stealth import (
    PlaywrightError,
    human_pause,
    human_scroll,
    new_stealth_context,
    save_storage_state,
)

if TYPE_CHECKING:  # pragma: no cover
    from playwright.async_api import Browser, BrowserContext, Page, Playwright, Response

    from deal_radar.config_schema import Profile

FB_BASE_URL = "https://www.facebook.com"
SOURCE_NAME = "fb_marketplace"
GRAPHQL_PATH = "/api/graphql"
ITEM_LINK_SELECTOR = 'a[href*="/marketplace/item/"]'
LISTING_MARKER = "marketplace_listing_title"
#: Substrings that make a JSON payload worth parsing (listings, or an empty/withheld feed).
PAYLOAD_MARKERS: tuple[str, ...] = (LISTING_MARKER, '"feed_units"')
XSSI_PREFIX = "for (;;);"
MAX_PAYLOAD_BYTES = 16 * 1024 * 1024

#: Relative difference between the account's saved radius and ``radius_km`` worth a warning.
RADIUS_MISMATCH_TOLERANCE = 0.25

#: Fraction of ``poll_timeout_seconds`` a poll may use before it stops starting queries.
POLL_BUDGET_FRACTION = 0.8
#: Initial guess for one query's duration (navigation + waits + scrolls), seconds.
DEFAULT_QUERY_ESTIMATE_S = 12.0

LOGIN_COMMAND = "python -m deal_radar.sources.fb_marketplace --config <config.yaml> --login"
#: CLI: seconds to let Facebook finish setting cookies after login / page load.
LOGIN_SETTLE_SECONDS = 3.0
CHECK_SETTLE_SECONDS = 2.0
#: CLI: seconds between cookie checks while waiting for the operator to log in.
LOGIN_POLL_SECONDS = 2.0

_ID_RE = re.compile(r"^[0-9A-Za-z_-]{1,64}$")
_DECIMAL_RE = re.compile(r"^\s*\d+(?:\.\d+)?\s*$")
_SOLD_TITLE_RE = re.compile(r"^\s*(?:\[\s*sold\s*\]|\(\s*sold\s*\)|sold\s*[-–—:!|]|sold\s*$)", re.IGNORECASE)
_PRICE_LINE_RE = re.compile(
    r"^(?:free|(?:[A-Z]{1,2})?[$€£¥₹₱]\s?\d[\d,.\s]*|\d[\d,.\s]*\s?(?:[$€£¥₹₱]|zł|kr|USD|CAD|EUR|GBP|AUD))$",
    re.IGNORECASE,
)
_STATUS_LINES = frozenset({"sold", "pending", "sold out"})
_CURRENCY_TOKENS: tuple[tuple[str, str], ...] = (
    ("CA$", "CAD"),
    ("C$", "CAD"),
    ("AU$", "AUD"),
    ("A$", "AUD"),
    ("NZ$", "NZD"),
    ("MX$", "MXN"),
    ("HK$", "HKD"),
    ("US$", "USD"),
    ("R$", "BRL"),
    ("€", "EUR"),
    ("£", "GBP"),
    ("¥", "JPY"),
    ("₹", "INR"),
    ("₱", "PHP"),
    ("zł", "PLN"),
    ("$", "USD"),
)

_BLOCK_TEXT: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("temporarily_blocked", re.compile(r"you(?:[’']re| are| have been) temporarily blocked", re.IGNORECASE)),
    ("rate_limited", re.compile(r"misusing this feature by going too fast|you[’']re going too fast", re.IGNORECASE)),
    (
        "account_restricted",
        re.compile(r"we suspended your account|your account has been (?:disabled|suspended|locked)", re.IGNORECASE),
    ),
)
_BLOCK_HINTS = {
    "checkpoint": "Facebook wants the account verified: open facebook.com in your normal browser, "
    f"complete the check, then refresh the session with `{LOGIN_COMMAND}`",
    "login_required": f"the saved session is no longer logged in; refresh it with `{LOGIN_COMMAND}`",
    "temporarily_blocked": "Facebook rate-limited this account; polling pauses (consider fewer search terms "
    "or a longer poll interval)",
    "rate_limited": "Facebook says requests are too fast; polling pauses (consider a longer poll interval)",
    "account_restricted": "the account is restricted; check facebook.com in your normal browser",
    "ineligible": "this account cannot use Marketplace (Page profile or unsupported region)",
    "consent_required": "Facebook asks for Marketplace data-use consent (EU): accept it yourself in a normal "
    f"browser session or in the `{LOGIN_COMMAND}` window",
}

# Single round-trip probes run inside the page.
_PROBE_JS = """() => {
  const heads = Array.from(document.querySelectorAll('h1, h2, [role="heading"], [role="alert"]'))
    .slice(0, 25).map((el) => (el.innerText || '').trim()).filter(Boolean).join('\\n');
  const dialog = Array.from(document.querySelectorAll('[role="dialog"]'))
    .map((el) => (el.innerText || '').slice(0, 600)).join('\\n');
  const form = document.querySelector(
    'form#login_form, form[action*="/login"] input[name="pass"], input[name="pass"][type="password"]');
  return { title: document.title || '', text: (heads + '\\n' + dialog).slice(0, 4000), loginForm: !!form };
}"""
_RADIUS_JS = """() => {
  for (const s of document.querySelectorAll('script')) {
    const m = (s.textContent || '').match(/"filter_radius_km"\\s*:\\s*([0-9.]+)/);
    if (m) return parseFloat(m[1]);
  }
  return null;
}"""
_EMBEDDED_JSON_JS = """(markers) => Array.from(document.querySelectorAll('script[type="application/json"]'))
  .map((s) => s.textContent || '').filter((t) => markers.some((m) => t.includes(m)))"""
_DOM_CARDS_JS = """([selector, limit]) => {
  const out = [];
  const seen = new Set();
  for (const a of document.querySelectorAll(selector)) {
    const m = (a.getAttribute('href') || '').match(/\\/marketplace\\/item\\/(\\d+)/);
    if (!m || seen.has(m[1])) continue;
    seen.add(m[1]);
    const img = a.querySelector('img');
    out.push({
      id: m[1],
      lines: (a.innerText || '').split('\\n').map((s) => s.trim()).filter(Boolean),
      img: img ? (img.currentSrc || img.src || null) : null,
      alt: img ? (img.getAttribute('alt') || null) : null,
    });
    if (out.length >= limit) break;
  }
  return out;
}"""


# --------------------------------------------------------------------------- URLs


def listing_url(listing_id: str) -> str:
    """Canonical public URL of a listing (always facebook.com, whatever was crawled)."""
    return f"{FB_BASE_URL}/marketplace/item/{listing_id}/"


def build_search_url(
    base_url: str,
    *,
    location_slug: str | None,
    query: str,
    min_price: float | None,
    max_price: float | None,
    days_since_listed: int,
) -> str:
    """Marketplace search URL, newest first, with the same parameters the UI emits."""
    path = f"/marketplace/{quote(location_slug.strip('/'), safe='')}/search" if location_slug else "/marketplace/search"
    params: list[tuple[str, str]] = []
    if min_price is not None and min_price > 0:
        params.append(("minPrice", str(math.floor(min_price))))
    if max_price is not None and max_price > 0:
        params.append(("maxPrice", str(math.ceil(max_price))))
    params.append(("daysSinceListed", str(int(days_since_listed))))
    params.append(("sortBy", "creation_time_descend"))
    params.append(("query", query))
    params.append(("exact", "false"))
    return f"{base_url.rstrip('/')}{path}?{urlencode(params, quote_via=quote)}"


def detect_block(url: str, *, title: str = "", text: str = "", has_login_form: bool = False) -> str | None:
    """Classify a page as a block/login wall; ``None`` when it looks like normal content.

    URL checks only look at the *path* (``?next=/login`` in a query string is not a wall).
    Text checks only run on headings, alerts and dialogs captured by the page probe so
    listing titles cannot trigger them.
    """
    path = urlsplit(url).path.lower()
    if "/checkpoint" in path or "/two_step_verification" in path:
        return "checkpoint"
    if path.startswith(("/login", "/r.php")) or "/login.php" in path:
        return "login_required"
    if path.startswith("/marketplace/ineligible"):
        return "ineligible"
    if path.startswith("/privacy/consent"):
        return "consent_required"
    haystack = f"{title}\n{text}"
    for reason, pattern in _BLOCK_TEXT:
        if pattern.search(haystack):
            return reason
    if has_login_form:
        return "login_required"
    return None


# --------------------------------------------------------------------------- payload parsing


def _strip_xssi(text: str) -> str:
    while text.startswith(XSSI_PREFIX):
        text = text[len(XSSI_PREFIX):].lstrip()
    return text


def _raw_decode_all(line: str) -> Iterator[Any]:
    """Decode back-to-back JSON values on one line (``{...}{...}``); stop at garbage."""
    decoder = json.JSONDecoder()
    index, end = 0, len(line)
    while index < end:
        while index < end and line[index] in " \t\r\n":
            index += 1
        if line.startswith(XSSI_PREFIX, index):
            index += len(XSSI_PREFIX)
            continue
        if index >= end or line[index] not in "{[":
            return
        try:
            value, index = decoder.raw_decode(line, index)
        except ValueError:
            return
        yield value


def iter_json_documents(text: str) -> Iterator[Any]:
    """Yield every JSON document in a Facebook response body.

    Handles the ``for (;;);`` prefix (also repeated per line), a single document,
    newline-delimited multi-document bodies (streamed ``@defer`` chunks), several
    documents concatenated on one line, and skips garbage/HTML/truncated lines.
    """
    body = _strip_xssi(text.lstrip("﻿ \t\r\n"))
    if not body:
        return
    try:
        yield json_loads(body)
        return
    except ValueError:
        pass
    for raw_line in body.splitlines():
        line = _strip_xssi(raw_line.strip())
        if not line or line[0] not in "{[":
            continue
        try:
            yield json_loads(line)
        except ValueError:
            yield from _raw_decode_all(line)


def _is_listing_node(node: Mapping[str, Any]) -> bool:
    title = node.get(LISTING_MARKER)
    return isinstance(title, str) and ("listing_price" in node or "id" in node)


def iter_listing_nodes(document: Any) -> Iterator[dict[str, Any]]:
    """Depth-first (document order) walk yielding listing-shaped objects.

    Iterative, so arbitrarily deep Relay payloads cannot hit the recursion limit; a
    listing's own children are not searched (they never contain other listings).
    """
    stack: list[Any] = [document]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if _is_listing_node(node):
                yield node
                continue
            children: Iterable[Any] = node.values()
        elif isinstance(node, list):
            children = node
        else:
            continue
        stack.extend(reversed([child for child in children if isinstance(child, (dict, list))]))


def _cursor_match_count(page_info: Any) -> int:
    """Matches announced by a feed cursor (``end_cursor`` is a JSON string with c2c/b2c ``it``)."""
    cursor = page_info.get("end_cursor") if isinstance(page_info, Mapping) else None
    if not isinstance(cursor, str) or not cursor.lstrip().startswith("{"):
        return 0
    try:
        data = json.loads(cursor)
    except ValueError:
        return 0
    total = 0
    for key in ("c2c", "b2c"):
        part = data.get(key) if isinstance(data, dict) else None
        count = part.get("it") if isinstance(part, Mapping) else None
        if isinstance(count, int) and not isinstance(count, bool) and count > 0:
            total += count
    return total


def count_withheld_feeds(document: Any) -> int:
    """Search feeds that return no edges although their cursor reports matches.

    Observed when Facebook withholds results from a client it distrusts (a soft block):
    an empty page is then not an empty search.
    """
    withheld = 0
    stack: list[Any] = [document]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            feed = node.get("feed_units")
            if isinstance(feed, Mapping) and isinstance(feed.get("edges"), list) and not feed["edges"]:
                if _cursor_match_count(feed.get("page_info")) > 0:
                    withheld += 1
            stack.extend(value for value in node.values() if isinstance(value, (dict, list)))
        elif isinstance(node, list):
            stack.extend(value for value in node if isinstance(value, (dict, list)))
    return withheld


def currency_from_text(text: str | None) -> str | None:
    """ISO currency for a formatted price ("CA$1,200" -> CAD, "350 €" -> EUR)."""
    if not text:
        return None
    for token, code in _CURRENCY_TOKENS:
        if token in text:
            return code
    return None


def _to_amount(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    numeric = isinstance(value, (int, float)) or (isinstance(value, str) and _DECIMAL_RE.match(value) is not None)
    if not numeric:
        return None
    number = float(value)
    if math.isnan(number) or math.isinf(number) or number < 0:
        return None
    return number


def _price_of(obj: Any) -> tuple[float | str | None, str | None]:
    """(price, currency) from a ``listing_price``-like object."""
    if not isinstance(obj, Mapping):
        return None, None
    formatted = obj.get("formatted_amount")
    if not isinstance(formatted, str) or not formatted.strip():
        formatted = obj.get("formatted_amount_zeros_stripped")
    formatted = formatted.strip() if isinstance(formatted, str) and formatted.strip() else None
    currency = obj.get("currency")
    currency = currency.upper() if isinstance(currency, str) and re.fullmatch(r"[A-Za-z]{3}", currency) else None
    if currency is None:
        currency = currency_from_text(formatted)
    price: float | str | None = _to_amount(obj.get("amount"))
    if price is None:
        price = formatted
    return price, currency


def _clean(text: Any, max_len: int | None = None) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = " ".join(text.split())
    return cleaned[:max_len] if max_len else cleaned


def _text_field(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        inner = value.get("text")
        return inner.strip() if isinstance(inner, str) else ""
    return ""


def _http_url(value: Any) -> str | None:
    return value if isinstance(value, str) and value.startswith(("https://", "http://")) else None


def _images_of(node: Mapping[str, Any]) -> list[str]:
    urls: list[str] = []

    def add(photo: Any) -> None:
        if not isinstance(photo, Mapping):
            return
        image = photo.get("image")
        url = _http_url(image.get("uri")) if isinstance(image, Mapping) else _http_url(photo.get("uri"))
        if url and url not in urls:
            urls.append(url)

    add(node.get("primary_listing_photo"))
    for key in ("listing_photos", "photos"):
        photos = node.get(key)
        if isinstance(photos, list):
            for photo in photos:
                add(photo)
    return urls


def _location_of(node: Mapping[str, Any]) -> Location | None:
    city = region = display = None
    latitude = longitude = None
    loc = node.get("location")
    if isinstance(loc, Mapping):
        geo = loc.get("reverse_geocode")
        if isinstance(geo, Mapping):
            city = _clean(geo.get("city")) or None
            region = _clean(geo.get("state")) or None
            page = geo.get("city_page")
            if isinstance(page, Mapping):
                display = _clean(page.get("display_name")) or None
        lat, lon = _to_float(loc.get("latitude")), _to_float(loc.get("longitude"))
        if lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
            latitude, longitude = lat, lon
    text = display or ", ".join(part for part in (city, region) if part) or _text_field(node.get("location_text")) or None
    if not any((text, city, region, latitude is not None)):
        return None
    return Location(text=text, city=city, region=region, latitude=latitude, longitude=longitude)


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) or math.isinf(number) else number


def _timestamp(value: Any) -> datetime | None:
    seconds = _to_float(value)
    if seconds is None or seconds <= 0:
        return None
    if seconds > 1e12:  # milliseconds
        seconds /= 1000.0
    if seconds < 1_100_000_000 or seconds > time.time() + 86_400:  # before 2004 / in the future
        return None
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _listing_from_node(
    node: Mapping[str, Any], *, query: str | None, profile_hint: str | None, via: str
) -> tuple[RawListing | None, str | None]:
    """Build a RawListing from a listing node; returns (listing, skip_reason)."""
    raw_id = node.get("id")
    if isinstance(raw_id, bool) or not isinstance(raw_id, (str, int)):
        return None, "no_id"
    listing_id = str(raw_id).strip()
    if not _ID_RE.match(listing_id):
        return None, "bad_id"
    title = _clean(node.get(LISTING_MARKER), 500) or _clean(node.get("custom_title"), 500)
    if not title:
        return None, "no_title"
    if node.get("is_sold") is True or _SOLD_TITLE_RE.match(title):
        return None, "sold"
    if node.get("is_pending") is True:
        return None, "pending"

    price, currency = _price_of(node.get("listing_price"))
    list_price, list_currency = _price_of(node.get("strikethrough_price"))
    currency = currency or list_currency
    seller_name = None
    extra: dict[str, Any] = {"via": via}
    seller = node.get("marketplace_listing_seller")
    if isinstance(seller, Mapping):
        seller_name = _clean(seller.get("name"), 200) or None
        if isinstance(seller.get("id"), (str, int)) and not isinstance(seller.get("id"), bool):
            extra["seller_id"] = str(seller["id"])
    delivery = node.get("delivery_types")
    if isinstance(delivery, list):
        extra["delivery_types"] = [d for d in delivery if isinstance(d, str)]
    category = node.get("marketplace_listing_category_id")
    if isinstance(category, (str, int)) and not isinstance(category, bool):
        extra["category_id"] = str(category)
    subtitles = node.get("custom_sub_titles_with_rendering_flags")
    if isinstance(subtitles, list):
        texts = [_clean(s.get("subtitle")) for s in subtitles if isinstance(s, Mapping)]
        if any(texts):
            extra["subtitles"] = [t for t in texts if t]
    if isinstance(node.get("is_shipping_offered"), bool):
        extra["is_shipping_offered"] = node["is_shipping_offered"]
    listing_price = node.get("listing_price")
    if isinstance(listing_price, Mapping) and isinstance(listing_price.get("formatted_amount"), str):
        extra["formatted_price"] = listing_price["formatted_amount"]
    condition = node.get("condition_text")
    try:
        listing = RawListing(
            source=SOURCE_NAME,
            source_kind=SourceKind.LOCAL,
            source_id=listing_id,
            url=listing_url(listing_id),
            title=title,
            description=_text_field(node.get("redacted_description")) or _text_field(node.get("description")),
            price=price,
            currency=currency or "USD",
            list_price=list_price,
            condition=condition.strip() if isinstance(condition, str) and condition.strip() else None,
            seller=SellerInfo(name=seller_name) if seller_name else None,
            location=_location_of(node),
            image_urls=_images_of(node),
            posted_at=_timestamp(node.get("creation_time")),
            query=query,
            profile_hint=profile_hint,
            extra=extra,
        )
    except ValidationError:
        return None, "invalid"
    return listing, None


def _merge(existing: RawListing, newer: RawListing) -> None:
    """Fill gaps of a listing seen earlier from a later chunk describing the same id."""
    if existing.price is None and newer.price is not None:
        existing.price = newer.price
        existing.currency = newer.currency
    if existing.list_price is None and newer.list_price is not None:
        existing.list_price = newer.list_price
    for url in newer.image_urls:
        if url not in existing.image_urls:
            existing.image_urls.append(url)
    for attr in ("location", "seller", "posted_at", "condition"):
        if getattr(existing, attr) is None and getattr(newer, attr) is not None:
            setattr(existing, attr, getattr(newer, attr))
    if not existing.description and newer.description:
        existing.description = newer.description
    for key, value in newer.extra.items():
        existing.extra.setdefault(key, value)


class ParseStats(Counter):
    """Skipped nodes per reason (``sold``, ``pending``, ``no_id``...) plus ``documents`` and ``withheld``."""


def parse_payloads(
    payloads: Iterable[tuple[str, str]],
    *,
    query: str | None = None,
    profile_hint: str | None = None,
) -> tuple[list[RawListing], ParseStats]:
    """Parse ``(via, text)`` payloads into unique listings (document order) + skip stats.

    The same id may appear in several payloads/chunks (SSR page and a pagination
    response, a deferred chunk adding the price...): later sightings only fill gaps.
    Ids skipped as sold/pending stay skipped even if another chunk omits the flag.
    """
    stats = ParseStats()
    found: dict[str, RawListing] = {}
    excluded: set[str] = set()
    for via, text in payloads:
        for document in iter_json_documents(text):
            stats["documents"] += 1
            withheld = count_withheld_feeds(document)
            if withheld:
                stats["withheld"] += withheld
            for node in iter_listing_nodes(document):
                listing, reason = _listing_from_node(node, query=query, profile_hint=profile_hint, via=via)
                if listing is None:
                    stats[reason or "invalid"] += 1
                    if reason in ("sold", "pending") and isinstance(node.get("id"), (str, int)):
                        excluded.add(str(node["id"]))
                        found.pop(str(node["id"]), None)
                    continue
                if listing.source_id in excluded:
                    continue
                current = found.get(listing.source_id)
                if current is None:
                    found[listing.source_id] = listing
                else:
                    _merge(current, listing)
    return list(found.values()), stats


def parse_graphql_payload(text: str, *, query: str | None = None, profile_hint: str | None = None) -> list[RawListing]:
    """Listings contained in one GraphQL (or SSR-embedded) response body."""
    listings, _ = parse_payloads([("graphql", text)], query=query, profile_hint=profile_hint)
    return listings


def parse_dom_cards(
    cards: Sequence[Mapping[str, Any]],
    *,
    query: str | None = None,
    profile_hint: str | None = None,
) -> list[RawListing]:
    """Listings from item cards scraped by ``_DOM_CARDS_JS`` (fallback path).

    A card's text lines are typically ``[price, (strike-through price), title,
    location, (subtitle...)]``; the image ``alt`` reads "<title> in <location>".
    """
    out: list[RawListing] = []
    seen: set[str] = set()
    for card in cards:
        listing_id = str(card.get("id") or "")
        if not listing_id.isdigit() or listing_id in seen:
            continue
        lines = [_clean(line) for line in card.get("lines") or [] if isinstance(line, str) and line.strip()]
        if any(line.lower() in _STATUS_LINES for line in lines):
            continue
        prices = [line for line in lines if _PRICE_LINE_RE.match(line)]
        texts = [line for line in lines if not _PRICE_LINE_RE.match(line)]
        alt = _clean(card.get("alt"))
        title = texts[0] if texts else (alt.rsplit(" in ", 1)[0] if alt else "")
        if not title or _SOLD_TITLE_RE.match(title):
            continue
        location_text = texts[1] if len(texts) > 1 else None
        image = _http_url(card.get("img"))
        price = prices[0] if prices else None
        try:
            listing = RawListing(
                source=SOURCE_NAME,
                source_kind=SourceKind.LOCAL,
                source_id=listing_id,
                url=listing_url(listing_id),
                title=title[:500],
                price=price,
                currency=currency_from_text(price) or "USD",
                list_price=prices[1] if len(prices) > 1 else None,
                location=Location(text=location_text) if location_text else None,
                image_urls=[image] if image else [],
                query=query,
                profile_hint=profile_hint,
                extra={"via": "dom", **({"subtitles": texts[2:]} if len(texts) > 2 else {})},
            )
        except ValidationError:
            continue
        seen.add(listing_id)
        out.append(listing)
    return out


# --------------------------------------------------------------------------- session file


def has_session_cookie(cookies: Iterable[Mapping[str, Any]]) -> bool:
    """True when Facebook's logged-in cookies (``c_user`` + ``xs``) are present."""
    names = {
        str(c.get("name"))
        for c in cookies
        if str(c.get("domain", "")).lstrip(".").endswith("facebook.com") and c.get("value")
    }
    return {"c_user", "xs"} <= names


def session_problem(state: Any, now: float | None = None) -> str | None:
    """Why a Playwright storage-state document cannot be a logged-in FB session (or None)."""
    if not isinstance(state, Mapping) or not isinstance(state.get("cookies"), list):
        return "not a Playwright storage-state file"
    cookies = [c for c in state["cookies"] if isinstance(c, Mapping)]
    if not has_session_cookie(cookies):
        return "no Facebook session cookies (c_user/xs) in the storage state"
    current = time.time() if now is None else now
    for cookie in cookies:
        if cookie.get("name") in ("c_user", "xs") and str(cookie.get("domain", "")).lstrip(".").endswith("facebook.com"):
            expires = _to_float(cookie.get("expires"))
            if expires is not None and 0 < expires < current:
                stamp = datetime.fromtimestamp(expires, tz=timezone.utc).isoformat(timespec="minutes")
                return f"Facebook session cookie {cookie.get('name')} expired at {stamp}"
    return None


def _read_state(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


# --------------------------------------------------------------------------- ingestor


class FbMarketplaceIngestor(BaseIngestor):
    """Facebook Marketplace search via one persistent, logged-in Playwright page."""

    name: ClassVar[str] = SOURCE_NAME
    kind: ClassVar[SourceKind] = SourceKind.LOCAL

    #: Max seconds to wait for the first results (cards or a JSON payload) per query.
    results_timeout_seconds: float = 10.0
    #: Max seconds to wait for pagination payloads after each scroll.
    scroll_settle_seconds: float = 3.0
    #: Max seconds to wait for in-flight GraphQL bodies before parsing.
    drain_timeout_seconds: float = 5.0

    def __init__(self, cfg: FbMarketplaceSource, ctx: IngestorContext, *, base_url: str | None = None) -> None:
        super().__init__(cfg, ctx)
        self.cfg: FbMarketplaceSource = cfg
        # Only tests point this elsewhere; listing URLs always use FB_BASE_URL.
        self.base_url = (base_url or FB_BASE_URL).rstrip("/")
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._needs_restart = False
        self._capture: list[str] | None = None
        self._captured = asyncio.Event()
        self._pending: set[asyncio.Task[None]] = set()
        self._cursor = 0
        self._query_estimate_s = DEFAULT_QUERY_ESTIMATE_S
        # True once a poll succeeded, False again after a wall: only a session that
        # demonstrably works is written back over the saved one.
        self._session_ok = False
        self._location_checked = False
        m = ctx.metrics
        self._m_payloads = m.counter("fb_payloads_total", "Facebook JSON payloads captured", ("kind",))
        self._m_skipped = m.counter("fb_listings_skipped_total", "Facebook listing nodes skipped", ("reason",))
        self._m_blocks = m.counter("fb_blocks_total", "Facebook block/login walls detected", ("reason",))
        self._m_queries = m.counter("fb_queries_total", "Facebook searches run", ("outcome",))
        self._m_query_ms = m.histogram("fb_query_ms", "Duration of one Facebook search (ms)")

    # ------------------------------------------------------------------ lifecycle

    def _geo(self) -> GeoPin:
        if self.cfg.location is None:
            raise SourceError("sources.fb_marketplace.location is required (latitude/longitude/city_slug)")
        return self.cfg.location

    async def _check_session_file(self) -> None:
        browser_cfg = self.cfg.browser
        state_path = Path(browser_cfg.storage_state_path).expanduser()
        if not state_path.is_file():
            if browser_cfg.user_data_dir:
                return  # a persistent profile carries its own cookies
            raise SourceAuthError(
                f"no Facebook session at {state_path}: run `{LOGIN_COMMAND}` on a machine with a display "
                "(log in by hand), then copy the file to this node"
            )
        try:
            state = await asyncio.to_thread(_read_state, state_path)
        except (OSError, ValueError) as exc:
            raise SourceAuthError(f"cannot read Facebook session {state_path}: {exc}") from exc
        problem = session_problem(state)
        if problem is not None and not browser_cfg.user_data_dir:
            raise SourceAuthError(f"{problem} ({state_path}); refresh it with `{LOGIN_COMMAND}`")

    async def setup(self) -> None:
        """Start Playwright, launch the hardened browser and open the persistent page."""
        await self._close_browser()  # the base loop may retry setup after a failure
        geo = self._geo()
        await self._check_session_file()
        from playwright.async_api import async_playwright

        playwright = await async_playwright().start()
        try:
            browser, context = await new_stealth_context(playwright, self.cfg.browser, geo)
        except BaseException:
            with contextlib.suppress(Exception):
                await playwright.stop()
            raise
        self._playwright, self._browser, self._context = playwright, browser, context
        try:
            self._page = await self._new_page()
        except BaseException:
            await self._close_browser()
            raise
        self._needs_restart = False
        self.log.info(
            "browser ready",
            extra={
                "source": self.name,
                "headless": self.cfg.browser.headless,
                "version": browser.version if browser is not None else "persistent",
                "location": geo.city_slug,
            },
        )

    async def teardown(self) -> None:
        """Save cookies (if the session worked) and close everything; safe after a failed setup."""
        try:
            if self._context is not None and self._session_ok and not self._needs_restart:
                await self._save_state()
        finally:
            await self._close_browser()

    async def _close_browser(self) -> None:
        for task in list(self._pending):
            task.cancel()
        self._pending.clear()
        page, context, browser, playwright = self._page, self._context, self._browser, self._playwright
        self._page = self._context = self._browser = self._playwright = None
        if page is not None:
            with contextlib.suppress(Exception):
                await page.close()
        if context is not None:
            with contextlib.suppress(Exception):
                await context.close()
        if browser is not None:
            with contextlib.suppress(Exception):
                await browser.close()
        if playwright is not None:
            with contextlib.suppress(Exception):
                await playwright.stop()

    async def _new_page(self) -> "Page":
        assert self._context is not None
        page = await self._context.new_page()
        page.on("response", self._on_response)
        return page

    async def _ensure_page(self) -> "Page":
        browser_dead = self._browser is not None and not self._browser.is_connected()
        if self._context is None or self._needs_restart or browser_dead:
            if self._needs_restart or browser_dead:
                self.log.warning("browser died; restarting it", extra={"source": self.name})
            await self.setup()
        if self._page is None or self._page.is_closed():
            self._page = await self._new_page()
        return self._page

    async def _save_state(self) -> None:
        if self._context is None:
            return
        try:
            await save_storage_state(self._context, self.cfg.browser.storage_state_path)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a failed save must not fail the poll
            self.log.warning("could not save Facebook session", extra={"source": self.name, "error": repr(exc)})

    # ------------------------------------------------------------------ polling

    def _jobs(self) -> list[tuple["Profile", str]]:
        jobs: list[tuple[Profile, str]] = []
        seen: set[str] = set()
        for profile in self.search_profiles():
            for term in profile.search.terms:
                key = " ".join(term.lower().split())
                if key and key not in seen:
                    seen.add(key)
                    jobs.append((profile, " ".join(term.split())))
        return jobs

    async def poll(self) -> list[RawListing]:
        """Run as many searches as fit in the poll budget, continuing round-robin next time."""
        jobs = self._jobs()
        if not jobs:
            return []
        loop = asyncio.get_running_loop()
        started = loop.time()  # browser (re)start time counts against the budget too
        page = await self._ensure_page()
        budget = self.cfg.poll_timeout_seconds * POLL_BUDGET_FRACTION
        browser_cfg = self.cfg.browser
        between = (browser_cfg.min_action_delay_seconds * 2.0, browser_cfg.max_action_delay_seconds * 2.0)
        results: list[RawListing] = []
        seen: set[str] = set()
        completed = failed = 0
        last_error: BaseException | None = None
        first = self._cursor % len(jobs)
        for step in range(len(jobs)):
            index = (first + step) % len(jobs)
            profile, term = jobs[index]
            if step:
                elapsed = loop.time() - started
                if elapsed + between[1] + self._query_estimate_s > budget:
                    self.log.debug(
                        "poll budget reached; continuing next poll",
                        extra={"source": self.name, "done": step, "total": len(jobs), "next_query": term},
                    )
                    break
                await human_pause(self.ctx.rng, *between)
            query_started = loop.time()
            try:
                listings = await self._search(page, profile, term)
            except PlaywrightError as exc:
                failed += 1
                last_error = exc
                self._m_queries.inc(outcome="error")
                self.log.warning("search failed", extra={"source": self.name, "query": term, "error": str(exc)[:300]})
                if page.is_closed() or (self._browser is not None and not self._browser.is_connected()):
                    self._needs_restart = True
                    raise SourceError(f"browser died during search {term!r}: {exc}") from exc
                listings = []
            else:
                completed += 1
                self._m_queries.inc(outcome="ok")
            finally:
                self._cursor = index + 1
            duration = loop.time() - query_started
            self._query_estimate_s = 0.7 * self._query_estimate_s + 0.3 * duration
            self._m_query_ms.observe(duration * 1000.0)
            for raw in listings:
                if raw.source_id not in seen:
                    seen.add(raw.source_id)
                    results.append(raw)
        if completed == 0 and failed:
            raise SourceError(f"all {failed} Facebook searches failed: {last_error}") from last_error
        self._session_ok = True
        await self._save_state()
        return results

    async def _search(self, page: "Page", profile: "Profile", term: str) -> list[RawListing]:
        geo = self._geo()
        band = profile.price
        url = build_search_url(
            self.base_url,
            location_slug=geo.city_slug,
            query=term,
            min_price=profile.search.price_min or band.floor,
            max_price=profile.search.price_max or band.ceiling,
            days_since_listed=self.cfg.days_since_listed,
        )
        limit = self.cfg.max_listings_per_query
        browser_cfg = self.cfg.browser
        sink: list[str] = []
        self._captured.clear()
        self._capture = sink
        try:
            response = await page.goto(url, wait_until="domcontentloaded")
            await self._raise_if_blocked(page, response.status if response is not None else None)
            await self._wait_for_results(page)
            if not self._location_checked:
                await self._check_location(page, geo)
            for _ in range(self.cfg.scrolls_per_query):
                if await page.locator(ITEM_LINK_SELECTOR).count() >= limit:
                    break
                await human_pause(
                    self.ctx.rng, browser_cfg.min_action_delay_seconds / 2, browser_cfg.max_action_delay_seconds / 2
                )
                before = len(sink)
                await human_scroll(
                    page,
                    self.ctx.rng,
                    steps=self.ctx.rng.randint(2, 4),
                    min_pause=browser_cfg.min_action_delay_seconds / 4,
                    max_pause=browser_cfg.max_action_delay_seconds / 4,
                )
                await self._wait_for_more(sink, before)
            await self._drain_pending()
            await self._raise_if_blocked(page, None)  # logged-out walls appear after scrolling
            embedded: list[str] = await page.evaluate(_EMBEDDED_JSON_JS, list(PAYLOAD_MARKERS))
        finally:
            self._capture = None
        self._m_payloads.inc(len(embedded), kind="embedded")
        self._m_payloads.inc(len(sink), kind="graphql")
        payloads = [("embedded", text) for text in embedded] + [("graphql", text) for text in sink]
        listings, stats = await asyncio.to_thread(parse_payloads, payloads, query=term, profile_hint=profile.id)
        for reason, count in stats.items():
            if reason not in ("documents", "withheld"):
                self._m_skipped.inc(count, reason=reason)
        if stats["withheld"] and not listings:
            self._m_blocks.inc(reason="withheld")
            self.log.warning(
                "Facebook reported matches but returned no listings (results withheld; possible soft block)",
                extra={"source": self.name, "query": term},
            )
        if not listings:
            cards = await page.evaluate(_DOM_CARDS_JS, [ITEM_LINK_SELECTOR, limit])
            listings = parse_dom_cards(cards, query=term, profile_hint=profile.id)
            if listings:
                self._m_payloads.inc(kind="dom")
        self.log.debug(
            "search done",
            extra={
                "source": self.name,
                "query": term,
                "listings": len(listings),
                "payloads": len(payloads),
                "skipped": {k: v for k, v in stats.items() if k not in ("documents", "withheld")},
            },
        )
        return listings[:limit]

    async def _raise_if_blocked(self, page: "Page", status: int | None) -> None:
        try:
            probe = await page.evaluate(_PROBE_JS)
        except PlaywrightError:
            probe = {}  # mid-navigation (client redirect): judge by URL alone
        if not isinstance(probe, dict):
            probe = {}
        reason = detect_block(
            page.url,
            title=str(probe.get("title") or ""),
            text=str(probe.get("text") or ""),
            has_login_form=bool(probe.get("loginForm")),
        )
        if reason is None and status in (403, 429):
            reason = "rate_limited"
        if reason is None:
            return
        self._session_ok = False
        self._m_blocks.inc(reason=reason)
        where = urlsplit(page.url).path or "/"
        raise SourceBlocked(
            f"Facebook {reason.replace('_', ' ')} at {where}: {_BLOCK_HINTS.get(reason, 'pausing')}",
            cooldown_seconds=self.cfg.checkpoint_pause_minutes * 60.0,
        )

    async def _check_location(self, page: "Page", geo: GeoPin) -> None:
        """Once per session: warn when Facebook does not search where the config says."""
        self._location_checked = True
        path = urlsplit(page.url).path
        if geo.city_slug and not path.startswith(f"/marketplace/{geo.city_slug.strip('/')}/"):
            self.log.warning(
                "Facebook did not recognise location.city_slug; searches use the account's saved location",
                extra={"source": self.name, "city_slug": geo.city_slug, "landed_on": path,
                       "hint": "use the numeric location id from the Marketplace URL instead"},
            )
        try:
            applied = await page.evaluate(_RADIUS_JS)
        except PlaywrightError:
            return
        if isinstance(applied, (int, float)) and applied > 0:
            if abs(applied - geo.radius_km) > RADIUS_MISMATCH_TOLERANCE * geo.radius_km:
                self.log.warning(
                    "Facebook applies the account's saved search radius, not location.radius_km",
                    extra={"source": self.name, "account_radius_km": applied, "configured_radius_km": geo.radius_km,
                           "hint": "change it in Marketplace > Location with the logged-in account"},
                )

    async def _wait_for_results(self, page: "Page") -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.results_timeout_seconds
        locator = page.locator(ITEM_LINK_SELECTOR)
        while True:
            if self._captured.is_set():
                return True
            with contextlib.suppress(PlaywrightError):
                if await locator.count() > 0:
                    return True
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._captured.wait(), timeout=min(0.25, remaining))

    async def _wait_for_more(self, sink: list[str], before: int) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.scroll_settle_seconds
        while loop.time() < deadline:
            if len(sink) > before and not self._pending:
                return
            await asyncio.sleep(0.1)

    async def _drain_pending(self) -> None:
        if self._pending:
            await asyncio.wait(set(self._pending), timeout=self.drain_timeout_seconds)

    def _on_response(self, response: "Response") -> None:
        sink = self._capture
        if sink is None or GRAPHQL_PATH not in response.url:
            return
        task = asyncio.get_running_loop().create_task(self._read_payload(response, sink))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def _read_payload(self, response: "Response", sink: list[str]) -> None:
        try:
            if response.status != 200:
                return
            body = await response.body()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - body evicted after navigation etc.
            self._m_payloads.inc(kind="unreadable")
            self.log.debug("graphql body unavailable", extra={"source": self.name, "error": str(exc)[:200]})
            return
        if len(body) > MAX_PAYLOAD_BYTES:
            self._m_payloads.inc(kind="oversized")
            return
        text = body.decode("utf-8", errors="replace")
        if any(marker in text for marker in PAYLOAD_MARKERS):
            sink.append(text)
            self._captured.set()


# --------------------------------------------------------------------------- CLI


def _cli_source(config: AppConfig) -> tuple[FbMarketplaceSource, GeoPin]:
    cfg = config.sources.fb_marketplace
    if cfg.location is None:
        raise ConfigError("sources.fb_marketplace.location is required (set HOME_LAT/HOME_LON/FB_CITY_SLUG)")
    return cfg, cfg.location


def _duration(seconds: float) -> str:
    return f"{seconds / 60:.0f} minutes" if seconds >= 90 else f"{seconds:.0f} seconds"


async def _page_probe(page: "Page") -> dict[str, Any]:
    try:
        probe = await page.evaluate(_PROBE_JS)
    except PlaywrightError:
        return {}
    return probe if isinstance(probe, dict) else {}


async def run_login(
    config: AppConfig,
    *,
    timeout_seconds: float = 600.0,
    base_url: str = FB_BASE_URL,
    headless: bool = False,
) -> int:
    """Open a browser window on the login page and save the session once the operator is in.

    The operator types their own credentials and completes any 2FA/security check in
    the window; this function only watches for the session cookies and for the page to
    leave the login/checkpoint flow. The window uses the same device identity
    (:func:`new_stealth_context`) the collector will use, so the session's cookies are
    issued to the fingerprint that will later present them. ``headless`` exists for
    tests only.
    """
    from playwright.async_api import async_playwright

    cfg, geo = _cli_source(config)
    browser_cfg = cfg.browser.model_copy(update={"headless": headless})
    state_path = Path(browser_cfg.storage_state_path).expanduser()
    async with async_playwright() as playwright:
        try:
            browser, context = await new_stealth_context(playwright, browser_cfg, geo)
        except PlaywrightError as exc:
            print(f"Could not open a browser window ({exc}).\nRun this on a desktop session, or under `xvfb-run` with VNC.")
            return 1
        try:
            page = await context.new_page()
            await page.goto(f"{base_url}/login/", wait_until="domcontentloaded")
            print(
                "A browser window is open on facebook.com/login.\n"
                "Log in with YOUR account (complete any 2FA / security check in the window).\n"
                f"Waiting up to {_duration(timeout_seconds)}; the session is saved automatically."
            )
            deadline = time.monotonic() + timeout_seconds
            logged_in = False
            while time.monotonic() < deadline:
                if page.is_closed():
                    print("The browser window was closed before the login finished.")
                    return 1
                cookies = await context.cookies()
                if has_session_cookie(cookies) and detect_block(page.url) not in ("checkpoint", "login_required"):
                    logged_in = True
                    break
                await asyncio.sleep(LOGIN_POLL_SECONDS)
            if not logged_in:
                print("Timed out waiting for the login; nothing was saved.")
                return 1
            await asyncio.sleep(LOGIN_SETTLE_SECONDS)  # let Facebook finish setting the remaining cookies
            with contextlib.suppress(PlaywrightError):
                await page.goto(f"{base_url}/marketplace/", wait_until="domcontentloaded")
                await asyncio.sleep(CHECK_SETTLE_SECONDS)
            saved = await save_storage_state(context, state_path)
            print(f"Logged in. Session saved to {saved} (permissions 0600). Keep this file private.")
            return 0
        finally:
            with contextlib.suppress(Exception):
                await context.close()
            if browser is not None:
                with contextlib.suppress(Exception):
                    await browser.close()


async def run_check(config: AppConfig, *, base_url: str = FB_BASE_URL) -> int:
    """Load the saved session headless, open Marketplace and report whether it is logged in."""
    from playwright.async_api import async_playwright

    cfg, geo = _cli_source(config)
    browser_cfg = cfg.browser.model_copy(update={"headless": True})
    state_path = Path(browser_cfg.storage_state_path).expanduser()
    if not browser_cfg.user_data_dir:
        if not state_path.is_file():
            print(f"NOT logged in: no session file at {state_path}. Run --login first.")
            return 1
        try:
            problem = session_problem(await asyncio.to_thread(_read_state, state_path))
        except (OSError, ValueError) as exc:
            problem = f"unreadable session file: {exc}"
        if problem is not None:
            print(f"NOT logged in: {problem}. Run --login again.")
            return 1
    async with async_playwright() as playwright:
        browser, context = await new_stealth_context(playwright, browser_cfg, geo)
        try:
            page = await context.new_page()
            await page.goto(f"{base_url}/marketplace/", wait_until="domcontentloaded")
            await asyncio.sleep(CHECK_SETTLE_SECONDS)
            probe = await _page_probe(page)
            reason = detect_block(
                page.url,
                title=str(probe.get("title") or ""),
                text=str(probe.get("text") or ""),
                has_login_form=bool(probe.get("loginForm")),
            )
            cookies = await context.cookies()
            if reason is None and has_session_cookie(cookies):
                await save_storage_state(context, state_path)
                print(f"Logged in: Marketplace loaded at {urlsplit(page.url).path}. Session refreshed in {state_path}.")
                return 0
            detail = _BLOCK_HINTS.get(reason or "login_required", "")
            print(f"NOT logged in ({reason or 'no session cookies'}): {detail}")
            return 1
        finally:
            with contextlib.suppress(Exception):
                await context.close()
            if browser is not None:
                with contextlib.suppress(Exception):
                    await browser.close()


def _arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m deal_radar.sources.fb_marketplace",
        description="Create or verify the Facebook session used by the fb_marketplace source.",
    )
    parser.add_argument("--config", default="deal_radar/config.yaml", help="path to config.yaml")
    parser.add_argument("--env-file", default=".env", help="optional .env file loaded before the config")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--login", action="store_true", help="open a browser window and log in by hand")
    mode.add_argument("--check", action="store_true", help="verify the saved session (headless)")
    parser.add_argument("--timeout-minutes", type=float, default=10.0, help="how long --login waits (default 10)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _arg_parser().parse_args(argv)
    configure_logging("INFO", json_output=False)
    if args.env_file and Path(args.env_file).is_file():
        load_dotenv(args.env_file)
    try:
        config = load_config(args.config)
        _cli_source(config)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    if args.login:
        return asyncio.run(run_login(config, timeout_seconds=max(30.0, args.timeout_minutes * 60.0)))
    return asyncio.run(run_check(config))


__all__ = [
    "FB_BASE_URL",
    "FbMarketplaceIngestor",
    "ParseStats",
    "build_search_url",
    "count_withheld_feeds",
    "currency_from_text",
    "detect_block",
    "has_session_cookie",
    "iter_json_documents",
    "iter_listing_nodes",
    "listing_url",
    "main",
    "parse_dom_cards",
    "parse_graphql_payload",
    "parse_payloads",
    "run_check",
    "run_login",
    "session_problem",
]


if __name__ == "__main__":
    raise SystemExit(main())
