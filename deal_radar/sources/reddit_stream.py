"""Reddit ingestor: r/buildapcsales-style deal feeds and r/hardwareswap-style swap posts.

Platform status (verified 2026-10-06)
-------------------------------------
Reddit shut down the unauthenticated ``.json`` endpoints on 2026-05-28 (they now answer
HTTP 403 "You've been blocked by network security"), new Data API apps need manual
approval (no new requests accepted after 2026-10-31), unregistered apps lose access
from 2027-01-12 and public Data API access ends in March 2027. Application-only OAuth
with an approved app is therefore the only supported transport; the unauthenticated
path is kept only so a misconfigured node fails loudly (operator notice + long
cooldown) instead of silently.

Transport
---------
* **Application-only OAuth** when ``client_id`` is configured:
  ``POST https://www.reddit.com/api/v1/access_token`` with HTTP Basic auth
  (``client_id:client_secret``), ``grant_type=client_credentials`` (confidential
  "script"/"web" apps) — or ``grant_type=https://oauth.reddit.com/grants/installed_client``
  plus a stable 25-char ``device_id`` when only a client id is configured ("installed"
  apps authenticate with an empty secret) — and ``scope=read`` (all ``/new`` needs).
  App-only tokens never come with a refresh token and ``expires_in`` differs between
  reports (3600 vs 86400), so the value is read at runtime and the grant is simply
  re-run :data:`TOKEN_REFRESH_MARGIN_SECONDS` before expiry. A 401 — or the HTML
  network-policy 403 Reddit serves for invalid bearer tokens — invalidates the token
  and the request is retried exactly once with a fresh one. Listings come from
  ``https://oauth.reddit.com/r/{sub}/new`` with ``Authorization: bearer <token>``.
* **Unauthenticated fallback**: ``https://www.reddit.com/r/{sub}/new.json``. A 403/429
  block page raises :class:`SourceBlocked` with a multi-hour cooldown; a warning is
  logged once at startup.
* ``raw_json=1`` opts out of Reddit's legacy HTML escaping of ``<``, ``>`` and ``&``
  in JSON bodies, so titles and preview image URLs arrive verbatim. URLs are therefore
  never passed through ``html.unescape`` (it expands semicolon-less HTML5 legacy
  entities: ``&region=us`` would become ``®ion=us``); only ``;``-terminated entities
  such as the ``&amp;`` / ``&#x200B;`` that new Reddit's editor writes into markdown
  are decoded.
* Listing bodies are capped at :data:`MAX_LISTING_BYTES`; bodies of at least
  :data:`PARSE_IN_THREAD_BYTES` are decoded and parsed in a worker thread (a page of
  long swap posts is hundreds of ms of regex work that must not stall the event loop).
* The User-Agent follows Reddit's API rules (``<platform>:<app id>:<version> (by
  /u/<username>)``) and is *never* a browser identity — Reddit forbids lying about
  the User-Agent and blocks spoofed ones.

Rate limits
-----------
The budget is per OAuth client id (100 QPM averaged over 10 minutes; possibly lower
for app-only grants), shared by every node using the app — enable
``lease_ttl_seconds`` so only one node polls. Every response carries
``X-Ratelimit-Used`` / ``X-Ratelimit-Remaining`` (a float string) /
``X-Ratelimit-Reset`` (seconds until the window ends). Subreddits are polled
concurrently, so the most pessimistic ``remaining`` of the current window wins.
:meth:`RedditIngestor.next_interval` (a) sleeps until the reset when fewer requests
remain than one poll cycle needs (minimum :data:`RATE_LIMIT_MIN_REMAINING`), and
otherwise (b) spreads the remaining budget evenly over the rest of the window, never
polling faster than configured. A 429 (often with an empty body and no
``Retry-After``) pauses the source until ``X-Ratelimit-Reset``. Reset values beyond
:data:`RATE_LIMIT_MAX_RESET_SECONDS` (the documented window is 10 minutes) are clamped
so a bogus header cannot stall the source for hours. When several subreddits fail in
one poll, the :class:`SourceBlocked` with the longest cooldown wins.

Freshness without the ``before`` cursor
---------------------------------------
``/new?before=<fullname>`` looks like the obvious incremental cursor, but it is
fragile: when the anchor post is deleted, removed by a moderator or filtered by
AutoModerator (about a quarter of r/hardwareswap posts are removed within minutes),
the anchor vanishes from the listing and Reddit returns an *empty* page — or, as
observed live, a stale slice from the middle of the listing — instead of the new
posts, silently stalling the feed. Every poll therefore fetches the head of ``/new``
(``limit`` ≤ 100, one request per subreddit) and relies on
:class:`~deal_radar.sources.base.ChangeDetector` (only new/changed listings are
emitted, keyed by the ``t3_`` fullname) plus ``max_item_age_minutes`` (restarts never
replay old posts).

Parsing
-------
* **deals** mode (r/buildapcsales): ``"[GPU] Brand Model ... - $1199 ($1599 - $400)"``
  → ``extra["category_tag"]="GPU"`` (falling back to the link flair, which carries the
  category), price = the poster's explicit final price (``"= $269"``, ``"$84.99 AR"``)
  when present, else the first ``$`` amount after the last top-level ``" - "``
  separator (amounts inside parentheses never win and rebate/coupon/shipping amounts
  such as ``"- $50 MIR"`` are skipped), else the first plain amount. ``list_price``
  comes from a parenthetical showing the original price (``($1599 - $400)``,
  ``(reg $399.99)``, ``(829.99-130)``). ``outbound_url`` is the linked retailer page
  (or, for self posts, the first external link in the body) and ``retailer`` is
  derived from its domain. Condition is left to the normalizer ("Open Box"/"Refurb"
  in the title). Expired deals (flair ``"Expired :table_flip:"`` / css ``expired``),
  stickied, removed and NSFW posts are skipped.
* **swap** mode (r/hardwareswap): ``"[USA-CA] [H] RTX 4090 FE, 32GB DDR5 [W] PayPal"``.
  Only *selling* posts are emitted (``[W]`` names a payment method, ``[H]`` does not,
  flair is not BUYING/CLOSED/TRADING — the flair bot can lag, so the title decides
  when it is missing) with ``source_kind=LOCAL``. Title = the ``[H]`` part; prices are
  banned from swap titles, so price = the first ``$`` amount in the body that is not
  struck through (``~~$500~~``) or marked sold (``[$200 Sold for $185 to /u/x]``,
  ``Sold for $400``);
  ``extra["multi_item"]`` when ``[H]`` lists several items; seller trade count from
  the ``"Trades: N"`` user flair; images from direct i.imgur.com / i.redd.it links and
  inline media; the timestamp album link in ``extra["timestamps_url"]``; payment
  methods named in ``[W]`` in ``extra["payment_methods"]`` and the ones the subreddit
  bans (Zelle, Venmo, Cash App, F&F, crypto, gift cards, wire) in
  ``extra["payment_red_flags"]``.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import html
import itertools
import re
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlsplit

from pydantic import SecretStr

from deal_radar.core.backoff import parse_retry_after
from deal_radar.core.http import HttpClient, HttpResponse, HttpStatusError, json_loads
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import Location, RawListing, SellerInfo, SourceKind
from deal_radar.sources.base import BaseIngestor, IngestorContext, SourceAuthError, SourceBlocked, SourceError

if TYPE_CHECKING:  # pragma: no cover
    from deal_radar.config_schema import RedditSource, SubredditSpec

_log = get_logger("sources.reddit")

# --------------------------------------------------------------------------- constants

TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
OAUTH_BASE = "https://oauth.reddit.com"
PUBLIC_BASE = "https://www.reddit.com"
APP_VERSION = "1.0"
CLIENT_CREDENTIALS_GRANT = "client_credentials"
INSTALLED_CLIENT_GRANT = "https://oauth.reddit.com/grants/installed_client"

UNAUTH_QPM = 10.0  # documented budget for logged-out access; floors the interval without headers
RATE_LIMIT_MIN_REMAINING = 2  # wait for the window reset below this many remaining requests
RATE_LIMIT_RESET_MARGIN_SECONDS = 1.0
# The documented window is 10 minutes; a larger X-Ratelimit-Reset is treated as bogus and
# clamped so one bad header cannot silently stall the source for hours.
RATE_LIMIT_MAX_RESET_SECONDS = 900.0
TOKEN_REFRESH_MARGIN_SECONDS = 120.0  # re-run the grant this long before expiry (capped at 10% of lifetime)
TOKEN_SCOPE = "read"  # least privilege: /r/{sub}/new only needs the read scope
DEFAULT_TOKEN_LIFETIME_SECONDS = 3600.0  # used if the token response omits expires_in
BLOCK_COOLDOWN_SECONDS = 1800.0  # HTML network-policy wall in OAuth mode
UNAUTH_BLOCK_COOLDOWN_SECONDS = 6 * 3600.0  # public .json is shut down: a 403/429 there will not heal soon
OAUTH_429_MIN_COOLDOWN_SECONDS = 30.0
OAUTH_429_DEFAULT_COOLDOWN_SECONDS = 60.0  # 429 without X-Ratelimit-Reset / Retry-After
# The OAuth2 wiki words ``expires_in`` as "Unix Epoch Seconds" while every client (and
# every observed response) treats it as a relative lifetime: accept both.
EPOCH_EXPIRES_IN_THRESHOLD = 1_000_000_000.0
# 100 posts x 40k-char selftext (+ selftext_html) stays well below this; it only bounds
# memory if a proxy or error page streams something absurd.
MAX_LISTING_BYTES = 32 * 1024 * 1024
MAX_TOKEN_BYTES = 64 * 1024
# Bodies at least this large are JSON-decoded and parsed in a worker thread: a 100-post
# page of long swap posts costs hundreds of ms of regex work that must not stall the loop.
PARSE_IN_THREAD_BYTES = 256 * 1024
MAX_IMAGES_PER_POST = 20  # the normalizer keeps 8; bounds work on image-dump posts
MAX_PRICES_PER_POST = 20  # distinct amounts kept in extra["prices"]
MAX_AMOUNTS_SCANNED = 200  # stop scanning a swap body after this many $ amounts / image links
# Upstream ``reason`` values of 403/404 subreddit errors used as metric labels; anything
# else is reported as "other" so a new upstream string cannot grow label cardinality.
_INACCESSIBLE_REASONS = frozenset({"private", "banned", "quarantined", "gold_only", "gated", "restricted"})

# 429 is accepted (not retried by the HTTP layer) because a short retry cannot fix an
# exhausted window; 401/403/404 bodies are needed to tell token expiry, private
# subreddits and block walls apart.
_LISTING_STATUSES = (200, 401, 403, 404, 429)

_REDDIT_HOST_SUFFIXES = ("reddit.com", "redd.it", "redditmedia.com", "redditstatic.com")
_IMAGE_HOST_SUFFIXES = ("imgur.com",)
_MULTI_PART_SUFFIXES = frozenset(
    {"co.uk", "org.uk", "com.au", "net.au", "co.nz", "co.jp", "com.br", "com.mx", "co.in", "com.sg", "co.kr", "com.tr"}
)
# Keyed by the registrable domain's first label so amazon.ca / bestbuy.ca map too.
_RETAILER_BY_LABEL: dict[str, str] = {
    "bestbuy": "Best Buy",
    "newegg": "Newegg",
    "amazon": "Amazon",
    "amzn": "Amazon",
    "microcenter": "Micro Center",
    "bhphotovideo": "B&H",
    "walmart": "Walmart",
    "target": "Target",
    "ebay": "eBay",
}
_RETAILER_BY_DOMAIN: dict[str, str] = {"amzn.to": "Amazon", "a.co": "Amazon"}

_COUNTRY_CODES = {"USA": "US", "US": "US", "CAN": "CA", "CANADA": "CA"}
_CURRENCY_BY_COUNTRY = {"US": "USD", "CA": "CAD"}  # swap sellers quote in their local currency

# --------------------------------------------------------------------------- regexes

# "$1,199.99", "$ 549", "$1.2k" (prefix) and "450$" (postfix, common on swap subs).
_MONEY_RE = re.compile(
    r"\$\s?(?P<a>\d{1,3}(?:,\d{3})+|\d+)(?:\.(?P<ac>\d{1,2}))?(?!\d|,\d)(?P<ak>[kK]\b)?"
    r"|(?<![$\d.,])(?P<b>\d{1,3}(?:,\d{3})+|\d+)(?:\.(?P<bc>\d{1,2}))?\$"
)
_TAG_RE = re.compile(r"^\s*\[\s*([^\[\]]{1,40}?)\s*\]")
_SEPARATOR_RE = re.compile(r"\s[-\u2013\u2014]{1,2}(?:\s+|(?=\$))")
_DISCOUNT_AFTER_RE = re.compile(
    r"^\s*(?:off\b|mir\b|rebate|coupon|promo|discount|instant\b|gc\b|gift\s*card|cash\s*back|savings|credit|sc\b"
    r"|ship(?:ping)?\b|s&h\b)",
    re.IGNORECASE,
)
_FINAL_EQ_RE = re.compile(r"=\s*")  # "... - $30 MIR = $269": the poster's computed final price
_AFTER_REBATE_RE = re.compile(r"^\s*(?:AR\b|after\s+(?:mail[-\s]?in\s+)?(?:rebates?|mir)\b)", re.IGNORECASE)
_BARE_SUBTRACTION_RE = re.compile(r"^\s*(\d[\d,]*(?:\.\d{1,2})?)\s*[-\u2013\u2212]\s*(\d[\d,]*(?:\.\d{1,2})?)\b")
_DISCOUNT_BEFORE_RE = re.compile(r"\b(?:save|saving|rebate|coupon|mir|off|promo)\b", re.IGNORECASE)
_LIST_CUE_RE = re.compile(
    r"\b(?:was|reg(?:ular(?:ly)?)?|msrp|list|orig(?:inal(?:ly)?)?|retail|normally|usually)\b", re.IGNORECASE
)
_SUBTRACTION_RE = re.compile(r"\$\s?[\d,.]+[kK]?\s*[-\u2013\u2212]\s*\$")
_PAREN_GROUP_RE = re.compile(r"\(([^()]*)\)")
_ONLY_MONEY_RE = re.compile(r"^\s*\$\s?[\d,.]+[kK]?\s*$")

_SWAP_TITLE_RE = re.compile(
    r"^\s*\[(?P<loc>[^\]]+)\]\s*\[\s*H\s*\](?P<have>.*?)\[\s*W\s*\](?P<want>.*)$",
    re.IGNORECASE | re.DOTALL,
)
_PAYMENT_RE = re.compile(
    r"\b(?:pay\s?pal|pp|local\s+cash|cash(?:\s?app)?|zelle|venmo|money|apple\s?pay|google\s?pay|g&s|f&f)\b",
    re.IGNORECASE,
)
_MONEY_ONLY_HAVE_RE = re.compile(r"^[\s$\d,.kK+]*$")  # "[H] $$$" / "[H] $1500" = buying with cash
_ITEM_SPLIT_RE = re.compile(r"\s*(?:,|;|\s\+\s|\s&\s|\band\b)\s*", re.IGNORECASE)
_TRADES_RE = re.compile(r"\btrades?\s*:\s*(\d+)", re.IGNORECASE)
_STRIKE_RE = re.compile(r"~~.*?~~", re.DOTALL)
# "[$200 Sold for $185 to /u/x]", "$400 - SOLD": amounts of items that are gone.
_SOLD_RE = re.compile(r"\$\s?[\d,.]+[kK]?[\s\[\]()\-\u2013:]*sold\b[^\n|\]]*", re.IGNORECASE)
# "Sold for $400 to /u/x", "sold to /u/x for $400", "SOLD @ $400": the sale price, not an ask.
_SOLD_FOR_RE = re.compile(r"\bsold\s+(?:to\s+/?u/[\w-]+\s+)?(?:for|at|@)\s*\$\s?[\d,.]+[kK]?", re.IGNORECASE)
_REMOVED_TITLE_RE = re.compile(r"^\s*\[\s*removed\b", re.IGNORECASE)  # "[ Removed by moderator ]"
_PAYMENT_TERMS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("paypal", re.compile(r"\bpay\s?pal\b|\bpp\b|\bg&s\b", re.IGNORECASE)),
    ("cash", re.compile(r"\bcash\b(?!\s?app)", re.IGNORECASE)),
    ("zelle", re.compile(r"\bzelle\b", re.IGNORECASE)),
    ("venmo", re.compile(r"\bvenmo\b", re.IGNORECASE)),
    ("cashapp", re.compile(r"\bcash\s?app\b", re.IGNORECASE)),
    ("friends_family", re.compile(r"\bf&f\b|\bfriends\s*(?:and|&)\s*family\b", re.IGNORECASE)),
    ("crypto", re.compile(r"\b(?:crypto|bitcoin|btc|usdt)\b", re.IGNORECASE)),
    ("gift_card", re.compile(r"\bgift\s*cards?\b", re.IGNORECASE)),
    ("wire", re.compile(r"\bwire\b|\bwestern\s+union\b|\bmoneygram\b", re.IGNORECASE)),
)
# r/hardwareswap only allows PayPal Goods & Services and local cash; the rest are bannable.
_DISALLOWED_PAYMENTS = frozenset({"zelle", "venmo", "cashapp", "friends_family", "crypto", "gift_card", "wire"})
# "$400 shipped", "**$400** shipped" (markdown bold), "$400 (shipped)".
_SHIPPED_AFTER_RE = re.compile(r"^[\s,.()*_~]*(?:\+\s*)?(?:shipped|ship(?:ping)?\s+incl|free\s+ship)", re.IGNORECASE)

_URL_RE = re.compile(r"https?://[^\s<>()\[\]\"'|`]+", re.IGNORECASE)
# Only ";"-terminated entities are decoded. html.unescape() also expands HTML5 legacy
# entities without a semicolon, which corrupts verbatim (raw_json=1) URLs:
# "&region=us" -> "®ion=us", "&gtin=" -> ">in=", "&section=" -> "§ion=".
_ENTITY_RE = re.compile(r"&(?:#[0-9]{1,7}|#[xX][0-9A-Fa-f]{1,6}|amp|lt|gt|quot|apos|nbsp);")
# Zero-width characters (new Reddit's editor writes "&#x200B;" spacer paragraphs) never
# belong in a URL; neither does whitespace produced by decoding "&nbsp;".
_URL_JUNK_RE = re.compile("[\u200b\u200c\u200d\u2060\ufeff]")
_URL_STOP_RE = re.compile(r"\s")
_DIRECT_IMAGE_RE = re.compile(
    r"https?://(?:i\.imgur\.com|i\.redd\.it|preview\.redd\.it)/[A-Za-z0-9_\-]+\.(?:jpe?g|png|webp|gif)(?:\?[^\s)\]]*)?",
    re.IGNORECASE,
)
_IMGUR_ALBUM_RE = re.compile(r"https?://(?:www\.|m\.)?imgur\.com/(?:a|gallery)/[A-Za-z0-9\-]+", re.IGNORECASE)
_IMGUR_PAGE_RE = re.compile(r"https?://(?:www\.|m\.)?imgur\.com/(?!a/|gallery/)[A-Za-z0-9]{5,}\b", re.IGNORECASE)
_REDDIT_GALLERY_RE = re.compile(r"https?://(?:www\.)?reddit\.com/gallery/[A-Za-z0-9]+", re.IGNORECASE)


# --------------------------------------------------------------------------- money helpers


@dataclass(frozen=True, slots=True)
class MoneySpan:
    value: float
    start: int
    end: int


def iter_money(text: str) -> Iterator[MoneySpan]:
    """Lazily yield every dollar amount in ``text`` with its character span, in order."""
    for m in _MONEY_RE.finditer(text):
        whole = m.group("a") if m.group("a") is not None else m.group("b")
        cents = m.group("ac") if m.group("a") is not None else m.group("bc")
        try:
            value = float(whole.replace(",", "") + (f".{cents}" if cents else ""))
        except ValueError:
            continue
        if m.group("ak"):
            value *= 1000.0
        yield MoneySpan(round(value, 2), m.start(), m.end())


def find_money(text: str) -> list[MoneySpan]:
    """Every dollar amount in ``text`` with its character span, in order."""
    return list(iter_money(text))


def _depths(text: str) -> list[int]:
    """Bracket nesting depth of every character ((), [] and {} count alike)."""
    depth = 0
    out: list[int] = []
    for ch in text:
        if ch in "([{":
            out.append(depth)
            depth += 1
        elif ch in ")]}":
            depth = max(0, depth - 1)
            out.append(depth)
        else:
            out.append(depth)
    return out


# --------------------------------------------------------------------------- deals titles


@dataclass(frozen=True, slots=True)
class DealTitle:
    category: str | None
    price: float | None
    list_price: float | None
    price_span: tuple[int, int] | None = None


def _discount_after(text: str, span: MoneySpan) -> bool:
    return bool(_DISCOUNT_AFTER_RE.match(text[span.end : span.end + 24]))


def _explicit_final_price(title: str, spans: Sequence[MoneySpan]) -> MoneySpan | None:
    """``"... = $269"`` (last one wins) or ``"$84.99 AR"`` / ``"$84.99 after rebate"``."""
    starts = {s.start: s for s in spans}
    final: MoneySpan | None = None
    for m in _FINAL_EQ_RE.finditer(title):
        if m.end() in starts:
            final = starts[m.end()]
    if final is not None:
        return final
    after_rebate = [s for s in spans if _AFTER_REBATE_RE.match(title[s.end :])]
    return after_rebate[-1] if after_rebate else None


def _separator_price(title: str, top: Sequence[MoneySpan], depth: Sequence[int]) -> MoneySpan | None:
    """First top-level amount after the last ``" - "`` separator that is not a rebate/coupon."""
    separators = [m for m in _SEPARATOR_RE.finditer(title) if depth[m.start()] == 0]
    for sep in reversed(separators):
        candidate = next((s for s in top if s.start >= sep.end()), None)
        if candidate is None:
            continue
        if _discount_after(title, candidate) or _DISCOUNT_BEFORE_RE.search(title[sep.end() : candidate.start]):
            continue
        return candidate
    return None


def parse_deal_title(title: str) -> DealTitle:
    """Parse r/buildapcsales' ``"[TAG] Product - $price (breakdown)"`` convention."""
    tag = _TAG_RE.match(title)
    category = (tag.group(1).strip() or None) if tag else None
    spans = find_money(title)
    if not spans:
        return DealTitle(category, None, None)
    depth = _depths(title)
    top = [s for s in spans if depth[s.start] == 0]
    chosen = (
        _explicit_final_price(title, spans)
        or _separator_price(title, top, depth)
        or next((s for s in top if not _discount_after(title, s)), None)
        or next((s for s in spans if not _discount_after(title, s)), None)
        or spans[0]
    )
    return DealTitle(category, chosen.value, _list_price(title, chosen), (chosen.start, chosen.end))


def _list_price(title: str, price: MoneySpan) -> float | None:
    """Original price from a parenthetical at/after the price: ``($1599 - $400)``, ``(reg $1599)``, ``(829.99-130)``."""
    for group in _PAREN_GROUP_RE.finditer(title):
        if group.end() <= price.start:
            continue  # breakdowns precede nothing; only groups at or after the price count
        inner = group.group(1)
        amounts = find_money(inner)
        if not amounts:
            bare = _BARE_SUBTRACTION_RE.match(inner)
            if bare:
                original, saving = (float(x.replace(",", "")) for x in bare.groups())
                if saving < original and original > price.value:
                    return round(original, 2)
            continue
        if amounts[0].value <= price.value:
            continue
        if _SUBTRACTION_RE.search(inner) or _LIST_CUE_RE.search(inner) or _ONLY_MONEY_RE.match(inner):
            return amounts[0].value
    return None


# --------------------------------------------------------------------------- url helpers


def registrable_domain(host: str) -> str:
    """Best-effort eTLD+1 (``www.bestbuy.com`` → ``bestbuy.com``, ``amazon.co.uk`` kept)."""
    host = host.lower().strip(".")
    labels = [p for p in host.split(".") if p]
    if len(labels) >= 3 and ".".join(labels[-2:]) in _MULTI_PART_SUFFIXES:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def _host(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""


def _host_matches(host: str, suffixes: Sequence[str]) -> bool:
    return any(host == s or host.endswith("." + s) for s in suffixes)


def retailer_for_url(url: str | None) -> str | None:
    """Store name for an outbound deal link, else the registrable domain."""
    if not url:
        return None
    host = _host(url)
    if not host:
        return None
    domain = registrable_domain(host)
    if domain in _RETAILER_BY_DOMAIN:
        return _RETAILER_BY_DOMAIN[domain]
    label = domain.split(".", 1)[0]
    return _RETAILER_BY_LABEL.get(label, domain or None)


def _is_external(url: str) -> bool:
    host = _host(url)
    return bool(host) and not _host_matches(host, _REDDIT_HOST_SUFFIXES) and not _host_matches(host, _IMAGE_HOST_SUFFIXES)


def unescape_entities(text: str) -> str:
    """Decode ``;``-terminated HTML entities only (``&amp;``, ``&#x200B;``), never ``&region``."""
    if "&" not in text:
        return text
    return _ENTITY_RE.sub(lambda m: html.unescape(m.group(0)), text)


def _clean_url(url: str) -> str:
    """Normalise a URL lifted from JSON or markdown.

    With ``raw_json=1`` URLs arrive verbatim; ``&amp;``-style escapes still show up in
    markdown written by new Reddit's editor (and in payloads fetched without
    ``raw_json``), so terminated entities are decoded, zero-width spacers dropped, the
    URL cut at whitespace and trailing markdown/sentence punctuation stripped.
    """
    url = _URL_JUNK_RE.sub("", unescape_entities(url))
    stop = _URL_STOP_RE.search(url)
    if stop is not None:
        url = url[: stop.start()]
    return url.rstrip(".,;:!?*_~")


def _first_external_link(text: str) -> str | None:
    for m in _URL_RE.finditer(text or ""):
        url = _clean_url(m.group(0))
        if _is_external(url):
            return url
    return None


def _dedupe(urls: Iterable[str], limit: int = MAX_IMAGES_PER_POST) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for url in urls:
        if url and url not in seen and url.startswith(("http://", "https://")):
            seen.add(url)
            out.append(url)
            if len(out) >= limit:
                break
    return out


# --------------------------------------------------------------------------- post helpers


class PostSkipped(Exception):
    """A post that is valid but must not be emitted (expired, buying, filtered...)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def iter_posts(payload: Any) -> Iterator[Any]:
    """Yield each child's ``data`` of a Reddit ``Listing``; raise ``SourceError`` on a bad envelope."""
    if not isinstance(payload, Mapping):
        raise SourceError(f"unexpected Reddit payload type {type(payload).__name__}")
    if payload.get("kind") not in (None, "Listing"):
        raise SourceError(f"unexpected Reddit payload kind {payload.get('kind')!r}")
    data = payload.get("data")
    children = data.get("children") if isinstance(data, Mapping) else None
    if not isinstance(children, list):
        raise SourceError("Reddit listing has no data.children array")
    for child in children:
        if not isinstance(child, Mapping):
            yield child
            continue
        if child.get("kind") not in (None, "t3"):
            continue  # comments / subreddits (e.g. search redirect for a missing sub)
        yield child.get("data")


def _str(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _posted_at(post: Mapping[str, Any]) -> datetime | None:
    created = post.get("created_utc")
    if isinstance(created, bool) or not isinstance(created, (int, float, str)):
        return None
    try:
        return datetime.fromtimestamp(float(created), tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def _permalink_url(post: Mapping[str, Any]) -> str:
    permalink = _str(post.get("permalink"))
    if permalink.startswith("/"):
        return PUBLIC_BASE + permalink
    if permalink.startswith("http"):
        return permalink
    post_id = _str(post.get("id"))
    if not post_id:
        raise ValueError("post has neither permalink nor id")
    return f"{PUBLIC_BASE}/comments/{post_id}/"


def _fullname(post: Mapping[str, Any]) -> str:
    name = _str(post.get("name"))
    if name:
        return name
    post_id = _str(post.get("id"))
    if not post_id:
        raise ValueError("post has neither name nor id")
    return f"t3_{post_id}"


def _flair(post: Mapping[str, Any]) -> str:
    return _str(post.get("link_flair_text")).strip()


def _flair_matches(post: Mapping[str, Any], needles: Sequence[str]) -> str | None:
    """Return the matching needle if the post flair text/css class contains it (case-insensitive)."""
    haystacks = [_flair(post).lower(), _str(post.get("link_flair_css_class")).lower()]
    for needle in needles:
        n = needle.strip().lower()
        if n and any(n in h for h in haystacks if h):
            return needle
    return None


def common_skip_reason(post: Mapping[str, Any], skip_flairs: Sequence[str] = ()) -> str | None:
    """Reasons that apply to every subreddit: pinned/removed/NSFW/expired/skip-flair posts."""
    if post.get("stickied") or post.get("pinned"):
        return "stickied"
    if (
        post.get("removed_by_category")
        or _str(post.get("selftext")).strip() in ("[removed]", "[deleted]")
        or _REMOVED_TITLE_RE.match(_str(post.get("title")))
    ):
        return "removed"
    if post.get("over_18"):
        return "nsfw"
    if _flair_matches(post, ("expired",)):
        return "expired"
    if _flair_matches(post, skip_flairs):
        return "flair"
    return None


def _preview_images(post: Mapping[str, Any]) -> list[str]:
    out: list[str] = []
    preview = post.get("preview")
    images = preview.get("images") if isinstance(preview, Mapping) else None
    if isinstance(images, list) and images and isinstance(images[0], Mapping):
        source = images[0].get("source")
        if isinstance(source, Mapping) and isinstance(source.get("url"), str):
            out.append(_clean_url(source["url"]))
    return out


def _media_metadata_images(post: Mapping[str, Any]) -> list[str]:
    """Gallery / inline images (``media_metadata[id].s.u``), in gallery order when known."""
    meta = post.get("media_metadata")
    if not isinstance(meta, Mapping):
        return []
    order: list[str] = []
    gallery = post.get("gallery_data")
    items = gallery.get("items") if isinstance(gallery, Mapping) else None
    if isinstance(items, list):
        order = [i["media_id"] for i in items if isinstance(i, Mapping) and isinstance(i.get("media_id"), str)]
    order += [k for k in meta if k not in order]
    out: list[str] = []
    for key in order:
        entry = meta.get(key)
        if not isinstance(entry, Mapping) or entry.get("status", "valid") != "valid":
            continue
        if entry.get("e", "Image") != "Image":
            continue
        source = entry.get("s")
        if isinstance(source, Mapping) and isinstance(source.get("u"), str):
            out.append(_clean_url(source["u"]))
    return out


def _direct_image(url: str) -> bool:
    return bool(_DIRECT_IMAGE_RE.fullmatch(url))


# --------------------------------------------------------------------------- deals posts


def parse_deal_post(post: Mapping[str, Any], *, subreddit: str, skip_flairs: Sequence[str] = ()) -> RawListing:
    """Map one r/buildapcsales-style post to a :class:`RawListing` (raises :class:`PostSkipped`)."""
    reason = common_skip_reason(post, skip_flairs)
    if reason:
        raise PostSkipped(reason)
    title = _str(post.get("title")).strip()
    if not title:
        raise ValueError("post without title")
    parsed = parse_deal_title(title)
    is_self = bool(post.get("is_self"))
    selftext = _str(post.get("selftext"))
    link = _str(post.get("url_overridden_by_dest")) or _str(post.get("url"))
    outbound: str | None = None
    if not is_self and link and _is_external(_clean_url(link)):
        outbound = _clean_url(link)
    elif is_self:
        outbound = _first_external_link(selftext)
    images = _preview_images(post) + _media_metadata_images(post)
    if not is_self and link and _direct_image(_clean_url(link)):
        images.insert(0, _clean_url(link))
    extra: dict[str, Any] = {
        "subreddit": _str(post.get("subreddit")) or subreddit,
        "mode": "deals",
        "category_tag": parsed.category or _flair(post) or None,
        "flair": _flair(post) or None,
        "author": _str(post.get("author")) or None,
        "domain": _str(post.get("domain")) or None,
        "is_self": is_self,
        "score": post.get("score"),
        "upvote_ratio": post.get("upvote_ratio"),
        "num_comments": post.get("num_comments"),
    }
    return RawListing(
        source=RedditIngestor.name,
        source_kind=SourceKind.AGGREGATOR,
        source_id=_fullname(post),
        url=_permalink_url(post),
        title=title,
        description=selftext,  # link posts may carry a body too (coupon codes, notes)
        price=parsed.price,
        list_price=parsed.list_price,
        image_urls=_dedupe(images),
        posted_at=_posted_at(post),
        retailer=retailer_for_url(outbound),
        outbound_url=outbound,
        extra={k: v for k, v in extra.items() if v is not None},
    )


# --------------------------------------------------------------------------- swap posts


@dataclass(frozen=True, slots=True)
class SwapTitle:
    location: str
    have: str
    want: str


def parse_swap_title(title: str) -> SwapTitle | None:
    """Split ``"[USA-CA] [H] item, item [W] PayPal"``; None if it does not follow the format."""
    m = _SWAP_TITLE_RE.match(title or "")
    if not m:
        return None
    return SwapTitle(
        location=m.group("loc").strip(),
        have=m.group("have").strip(" \t,;:-"),  # "[H]: item" / "[W]: PayPal" are common
        want=m.group("want").strip(" \t,;:-"),
    )


def swap_intent(swap: SwapTitle, flair: str | None) -> str:
    """Classify a swap post as ``selling`` | ``buying`` | ``trading`` | ``closed``."""
    flair_l = (flair or "").strip().lower()
    for state in ("closed", "buying", "trading"):
        if state in flair_l:
            return state
    if _PAYMENT_RE.search(swap.have) or (swap.have and _MONEY_ONLY_HAVE_RE.match(swap.have)):
        return "buying"
    if _PAYMENT_RE.search(swap.want) or "$" in swap.want:
        return "selling"
    return "trading"


def _location_keys(location_tag: str) -> list[str]:
    """``"USA-NY, NJ"`` → ``["USA-NY,NJ", "USA-NY", "USA-NJ"]`` (one key per listed state)."""
    tag = re.sub(r"\s+", "", location_tag).upper()
    keys = [tag]
    parts = [p for p in re.split(r"[-,/]", tag) if p]
    if len(parts) >= 2:
        keys += [f"{parts[0]}-{p}" for p in parts[1:] if re.fullmatch(r"[A-Z]{2}", p)]
    return keys


def location_allowed(location_tag: str, allowed: Sequence[str]) -> bool:
    """Prefix match of the title's location tag(s) against ``hardwareswap_locations``."""
    if not allowed:
        return True
    keys = _location_keys(location_tag)
    prefixes = [re.sub(r"\s+", "", p).upper() for p in allowed if p.strip()]
    return any(key.startswith(prefix) for key in keys for prefix in prefixes)


def parse_location_tag(tag: str) -> Location:
    """``"USA-CA"`` → country US, region CA (city / ZIP when a third part is present)."""
    parts = [p.strip() for p in re.split(r"\s*[-,/]\s*", tag.strip()) if p.strip()]
    country = _COUNTRY_CODES.get(parts[0].upper()) if parts else None
    region = city = postal = None
    if country and len(parts) > 1 and re.fullmatch(r"[A-Za-z]{2,3}", parts[1]):
        region = parts[1].upper()
        for rest in parts[2:]:
            if re.fullmatch(r"\d{5}", rest):
                postal = rest
            elif re.fullmatch(r"[A-Za-z]{2}", rest):
                continue  # additional state ("USA-NY, NJ"); Location holds a single region
            elif city is None and re.search(r"[A-Za-z]", rest):
                city = rest
    return Location(text=tag.strip(), country=country, region=region, city=city, postal_code=postal)


def _swap_price(selftext: str, have: str) -> tuple[float | None, float | None, list[float]]:
    """(price, shipping, distinct amounts) of a swap post.

    Struck-through ``~~$x~~`` amounts and sold markers (``[$200 Sold for $185 to /u/x]``,
    ``Sold for $400``) are removed first. At most :data:`MAX_PRICES_PER_POST` distinct
    amounts are collected and at most :data:`MAX_AMOUNTS_SCANNED` examined, so a huge
    price table costs bounded work.
    """
    body = _SOLD_FOR_RE.sub(" ", _SOLD_RE.sub(" ", _STRIKE_RE.sub(" ", selftext or "")))
    for text in (body, have):
        first: MoneySpan | None = None
        distinct: list[float] = []
        for span in itertools.islice(iter_money(text), MAX_AMOUNTS_SCANNED):
            if first is None:
                first = span
            if span.value not in distinct:
                distinct.append(span.value)
                if len(distinct) >= MAX_PRICES_PER_POST:
                    break
        if first is not None:
            shipping = 0.0 if _SHIPPED_AFTER_RE.match(text[first.end : first.end + 30]) else None
            return first.value, shipping, distinct
    return None, None, []


def payment_methods(text: str) -> list[str]:
    """Normalised payment methods named in ``text`` (e.g. the ``[W]`` part), in a fixed order."""
    return [name for name, pattern in _PAYMENT_TERMS if pattern.search(text)]


def _have_items(have: str) -> list[str]:
    return [chunk for chunk in _ITEM_SPLIT_RE.split(have) if len(re.sub(r"[^A-Za-z0-9]", "", chunk)) >= 2]


def _swap_images(post: Mapping[str, Any], selftext: str) -> list[str]:
    link = _clean_url(_str(post.get("url_overridden_by_dest")) or _str(post.get("url")))
    direct = [link] if link and _direct_image(link) else []
    # Lazy and bounded: _dedupe stops at the cap, islice bounds repeated links.
    body = (_clean_url(m.group(0)) for m in itertools.islice(_DIRECT_IMAGE_RE.finditer(selftext), MAX_AMOUNTS_SCANNED))
    return _dedupe(itertools.chain(direct, body, _media_metadata_images(post), _preview_images(post)))


def _timestamps_url(post: Mapping[str, Any], selftext: str) -> str | None:
    for pattern in (_IMGUR_ALBUM_RE, _IMGUR_PAGE_RE, _REDDIT_GALLERY_RE):
        m = pattern.search(selftext)
        if m:
            return _clean_url(m.group(0))
    link = _str(post.get("url_overridden_by_dest"))
    if link and (_IMGUR_ALBUM_RE.match(link) or _REDDIT_GALLERY_RE.match(link)):
        return _clean_url(link)
    return None


def parse_swap_post(
    post: Mapping[str, Any],
    *,
    subreddit: str,
    skip_flairs: Sequence[str] = (),
    locations: Sequence[str] = (),
) -> RawListing:
    """Map one r/hardwareswap-style *selling* post to a LOCAL :class:`RawListing`."""
    reason = common_skip_reason(post, skip_flairs)
    if reason:
        raise PostSkipped(reason)
    title = _str(post.get("title")).strip()
    if not title:
        raise ValueError("post without title")
    swap = parse_swap_title(title)
    if swap is None:
        raise PostSkipped("unparseable_title")
    flair = _flair(post)
    intent = swap_intent(swap, flair)
    if intent != "selling":
        raise PostSkipped(intent)
    if not swap.have:
        raise PostSkipped("unparseable_title")
    if not location_allowed(swap.location, locations):
        raise PostSkipped("location")
    author = _str(post.get("author"))
    if not author or author == "[deleted]":
        raise PostSkipped("deleted")
    selftext = _str(post.get("selftext"))
    price, shipping, amounts = _swap_price(selftext, swap.have)
    location = parse_location_tag(swap.location)
    methods = payment_methods(swap.want)
    trades_match = _TRADES_RE.search(_str(post.get("author_flair_text")))
    extra: dict[str, Any] = {
        "subreddit": _str(post.get("subreddit")) or subreddit,
        "mode": "swap",
        "full_title": title,
        "location_tag": swap.location,
        "want": swap.want,
        "payment_methods": methods,
        "payment_red_flags": [m for m in methods if m in _DISALLOWED_PAYMENTS],
        "multi_item": len(_have_items(swap.have)) >= 2,
        "prices": amounts,
        "flair": flair or None,
        "author_flair": _str(post.get("author_flair_text")) or None,
        "num_comments": post.get("num_comments"),
    }
    timestamps = _timestamps_url(post, selftext)
    if timestamps:
        extra["timestamps_url"] = timestamps
    return RawListing(
        source=RedditIngestor.name,
        source_kind=SourceKind.LOCAL,
        source_id=_fullname(post),
        url=_permalink_url(post),
        title=swap.have,
        description=selftext,
        price=price,
        currency=_CURRENCY_BY_COUNTRY.get(location.country or "", "USD"),
        shipping=shipping,
        seller=SellerInfo(name=author, feedback_score=int(trades_match.group(1)) if trades_match else None),
        location=location,
        image_urls=_swap_images(post, selftext),
        posted_at=_posted_at(post),
        extra={k: v for k, v in extra.items() if v is not None},
    )


# --------------------------------------------------------------------------- listing


@dataclass
class ParseOutcome:
    listings: list[RawListing] = field(default_factory=list)
    skipped: dict[str, int] = field(default_factory=dict)
    errors: int = 0

    def skip(self, reason: str) -> None:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1


def parse_listing(
    payload: Any,
    *,
    subreddit: str,
    mode: str = "deals",
    skip_flairs: Sequence[str] = (),
    locations: Sequence[str] = (),
) -> ParseOutcome:
    """Parse a whole ``/r/{sub}/new`` listing; malformed posts are counted, never fatal."""
    outcome = ParseOutcome()
    for post in iter_posts(payload):
        try:
            if not isinstance(post, Mapping):
                raise TypeError(f"post data is {type(post).__name__}, not an object")
            if mode == "swap":
                listing = parse_swap_post(post, subreddit=subreddit, skip_flairs=skip_flairs, locations=locations)
            else:
                listing = parse_deal_post(post, subreddit=subreddit, skip_flairs=skip_flairs)
        except PostSkipped as skipped:
            outcome.skip(skipped.reason)
            continue
        except (KeyError, TypeError, ValueError, AttributeError) as exc:  # pydantic ValidationError is a ValueError
            outcome.errors += 1
            _log_parse_error(subreddit, post, exc)
            continue
        outcome.listings.append(listing)
    return outcome


def _log_parse_error(subreddit: str, post: Any, exc: BaseException) -> None:
    post_id = post.get("name") if isinstance(post, Mapping) else None
    _log.debug(
        "skipping malformed reddit post", extra={"subreddit": subreddit, "post": post_id, "error": repr(exc)[:300]}
    )


# --------------------------------------------------------------------------- user agent / oauth


@dataclass(frozen=True, slots=True)
class RedditEndpoints:
    """Upstream URLs (overridable so tests can point the ingestor at a local server)."""

    token_url: str = TOKEN_URL
    oauth_base: str = OAUTH_BASE
    public_base: str = PUBLIC_BASE


def build_user_agent(cfg: "RedditSource") -> str:
    """Reddit API rules: ``<platform>:<app id>:<version> (by /u/<username>)``, never a browser UA."""
    if cfg.user_agent:
        return cfg.user_agent
    return f"python:dealradar:{APP_VERSION} (by /u/{cfg.username or 'dealradar'})"


class RedditOAuth:
    """Cached application-only OAuth token with single-flight refresh.

    App-only tokens have no refresh token; "refresh" means requesting a new one, which
    happens ``min(TOKEN_REFRESH_MARGIN_SECONDS, 10% of lifetime)`` before expiry, or
    after a 401 via :meth:`invalidate`. Concurrent pollers share one in-flight request.
    """

    def __init__(
        self,
        http: HttpClient,
        client_id: str | SecretStr,
        client_secret: str | SecretStr | None,
        *,
        user_agent: str,
        device_id: str,
        token_url: str = TOKEN_URL,
        block_cooldown: float = BLOCK_COOLDOWN_SECONDS,
        metrics: Metrics | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._http = http
        # Credentials stay wrapped until the Basic header is built (never in repr/vars/logs).
        self._client_id = client_id if isinstance(client_id, SecretStr) else SecretStr(client_id)
        if isinstance(client_secret, str):
            client_secret = SecretStr(client_secret) if client_secret else None
        self._client_secret: SecretStr | None = client_secret
        self._user_agent = user_agent
        self._device_id = device_id
        self._token_url = token_url
        self._block_cooldown = block_cooldown
        self._clock = clock
        self._lock = asyncio.Lock()
        self._token: str | None = None
        self._refresh_at = 0.0
        self.refreshes = 0
        self._m_token = (metrics or Metrics()).counter("reddit_token_requests_total", "Reddit OAuth token requests", ("outcome",))

    @property
    def grant_type(self) -> str:
        return CLIENT_CREDENTIALS_GRANT if self._client_secret is not None else INSTALLED_CLIENT_GRANT

    def _current(self) -> str | None:
        if self._token is not None and self._clock() < self._refresh_at:
            return self._token
        return None

    async def token(self) -> str:
        current = self._current()
        if current is not None:
            return current
        async with self._lock:
            current = self._current()  # another task refreshed while we waited
            if current is not None:
                return current
            return await self._refresh()

    def invalidate(self, token: str) -> None:
        """Drop ``token`` after a 401 (no-op if a concurrent task already replaced it)."""
        if self._token == token:
            self._token = None
            self._refresh_at = 0.0

    def _basic_auth(self) -> str:
        # Installed apps authenticate with an empty password.
        secret = self._client_secret.get_secret_value() if self._client_secret is not None else ""
        pair = f"{self._client_id.get_secret_value()}:{secret}"
        return "Basic " + base64.b64encode(pair.encode("utf-8")).decode("ascii")

    async def _refresh(self) -> str:
        form = {"grant_type": self.grant_type, "scope": TOKEN_SCOPE}
        if self._client_secret is None:
            form["device_id"] = self._device_id
        try:
            resp = await self._http.request(
                "POST",
                self._token_url,
                data=form,
                headers={"Authorization": self._basic_auth(), "User-Agent": self._user_agent},
                accept="application/json",
                expected=(200,),
                parse="bytes",
                max_bytes=MAX_TOKEN_BYTES,
            )
        except HttpStatusError as exc:
            self._m_token.inc(outcome=f"http_{exc.status}")
            if exc.status in (403, 429) and _looks_like_html_text(exc.body):
                raise SourceBlocked(
                    f"reddit token endpoint blocked (HTTP {exc.status})", cooldown_seconds=self._block_cooldown
                ) from exc
            if exc.status in (400, 401, 403):
                raise SourceAuthError(f"reddit rejected the OAuth client credentials (HTTP {exc.status})") from exc
            raise
        body = bytes(resp.data) if isinstance(resp.data, (bytes, bytearray)) else b""
        if _looks_like_html(resp.headers, body):
            self._m_token.inc(outcome="html")
            raise SourceBlocked("reddit token endpoint returned an HTML page", cooldown_seconds=self._block_cooldown)
        try:
            payload = json_loads(body)
        except ValueError as exc:
            self._m_token.inc(outcome="malformed")
            raise SourceError("reddit token endpoint returned malformed JSON") from exc
        token = payload.get("access_token") if isinstance(payload, Mapping) else None
        if not isinstance(token, str) or not token:
            # Reddit answers some bad requests with HTTP 200 + {"error": "unsupported_grant_type"}.
            error = payload.get("error") if isinstance(payload, Mapping) else None
            self._m_token.inc(outcome="error")
            raise SourceAuthError(f"reddit token endpoint returned no access_token (error={error!r})")
        token_type = str(payload.get("token_type", "bearer")).lower()
        if token_type != "bearer":
            _log.warning("unexpected reddit token_type", extra={"source": "reddit", "token_type": token_type})
        lifetime = _token_lifetime(payload.get("expires_in"))
        margin = min(TOKEN_REFRESH_MARGIN_SECONDS, lifetime * 0.1)
        self._token = token
        self._refresh_at = self._clock() + lifetime - margin
        self.refreshes += 1
        self._m_token.inc(outcome="ok")
        _log.info(
            "reddit oauth token acquired",
            extra={"source": "reddit", "grant": self.grant_type, "expires_in_s": round(lifetime, 1)},
        )
        return token


def _token_lifetime(expires_in: Any, *, now: Callable[[], float] = time.time) -> float:
    """Seconds the token stays valid: relative ``expires_in`` (3600/86400 observed), or an
    absolute Unix timestamp as the OAuth2 wiki words it; defaults when missing/garbage."""
    if isinstance(expires_in, bool):
        return DEFAULT_TOKEN_LIFETIME_SECONDS
    try:
        value = float(expires_in)
    except (TypeError, ValueError):
        return DEFAULT_TOKEN_LIFETIME_SECONDS
    if value != value or value in (float("inf"), float("-inf")) or value == 0:
        return DEFAULT_TOKEN_LIFETIME_SECONDS
    if value >= EPOCH_EXPIRES_IN_THRESHOLD:
        value -= now()
    return max(1.0, value)


def _looks_like_html_text(text: str) -> bool:
    return text.lstrip()[:1] == "<"


def _looks_like_html(headers: Mapping[str, str], body: bytes) -> bool:
    ctype = (_header(headers, "Content-Type") or "").lower()
    if "html" in ctype:
        return True
    return body.lstrip()[:1] == b"<"


def _header(headers: Mapping[str, str], name: str) -> str | None:
    value = headers.get(name)
    if value is not None:
        return value
    lname = name.lower()
    for key, val in headers.items():
        if key.lower() == lname:
            return val
    return None


def _float_header(headers: Mapping[str, str], name: str) -> float | None:
    raw = _header(headers, name)
    if raw is None:
        return None
    try:
        value = float(raw.strip())
    except ValueError:
        return None
    if value != value or value < 0:  # NaN / negative
        return None
    return value


def _reset_header(headers: Mapping[str, str]) -> float | None:
    """``X-Ratelimit-Reset`` in seconds, clamped to :data:`RATE_LIMIT_MAX_RESET_SECONDS`."""
    value = _float_header(headers, "X-Ratelimit-Reset")
    return None if value is None else min(value, RATE_LIMIT_MAX_RESET_SECONDS)


# --------------------------------------------------------------------------- rate limits


@dataclass
class RateLimitState:
    """Most pessimistic view of Reddit's current rate-limit window."""

    remaining: float | None = None
    used: float | None = None
    reset_at: float | None = None  # monotonic deadline of the window

    def observe(self, *, remaining: float | None, used: float | None, reset_seconds: float | None, now: float) -> None:
        if remaining is None and reset_seconds is None:
            return
        new_reset = now + reset_seconds if reset_seconds is not None else self.reset_at
        new_window = (
            self.reset_at is None
            or now >= self.reset_at
            or (new_reset is not None and new_reset > self.reset_at + 2.0)
        )
        if new_window or self.remaining is None:
            self.remaining = remaining
            self.used = used
            self.reset_at = new_reset
            return
        if remaining is not None and remaining < self.remaining:
            self.remaining = remaining
            self.used = used if used is not None else self.used
            if new_reset is not None:
                self.reset_at = new_reset

    def seconds_until_reset(self, now: float) -> float | None:
        if self.reset_at is None:
            return None
        return max(0.0, self.reset_at - now)


# --------------------------------------------------------------------------- ingestor


class RedditIngestor(BaseIngestor):
    """Polls ``/r/{sub}/new`` for every configured subreddit concurrently."""

    name: ClassVar[str] = "reddit"
    kind: ClassVar[SourceKind] = SourceKind.AGGREGATOR

    def __init__(
        self,
        cfg: "RedditSource",
        ctx: IngestorContext,
        *,
        clock: Callable[[], float] = time.monotonic,
        endpoints: RedditEndpoints | None = None,
    ) -> None:
        super().__init__(cfg, ctx)
        self.cfg: RedditSource = cfg
        self._clock = clock
        self.endpoints = endpoints or RedditEndpoints()
        self.user_agent = build_user_agent(cfg)
        self.rate_limit = RateLimitState()
        self._unauth_warned = False
        m = ctx.metrics
        self.m_posts = m.counter("reddit_posts_total", "Reddit posts by parse outcome", ("subreddit", "outcome"))
        self.m_requests = m.counter("reddit_requests_total", "Reddit listing requests", ("subreddit", "status"))
        self.m_remaining = m.gauge("reddit_ratelimit_remaining", "X-Ratelimit-Remaining of the last Reddit response")
        self.oauth: RedditOAuth | None = None
        client_id = cfg.client_id.get_secret_value().strip() if cfg.client_id is not None else ""
        self.block_cooldown = max(BLOCK_COOLDOWN_SECONDS if client_id else UNAUTH_BLOCK_COOLDOWN_SECONDS, cfg.cooldown_seconds)
        if client_id:
            secret = cfg.client_secret.get_secret_value().strip() if cfg.client_secret is not None else ""
            device_id = hashlib.sha256(f"{ctx.node_id}:{client_id}".encode("utf-8")).hexdigest()[:25]
            self.oauth = RedditOAuth(
                ctx.http,
                client_id,
                secret or None,
                user_agent=self.user_agent,
                device_id=device_id,
                token_url=self.endpoints.token_url,
                block_cooldown=self.block_cooldown,
                metrics=ctx.metrics,
                clock=clock,
            )

    @property
    def authenticated(self) -> bool:
        return self.oauth is not None

    # ------------------------------------------------------------------ hooks

    async def setup(self) -> None:
        # The token is fetched lazily on the first poll so a transient token-endpoint
        # failure at boot is retried by the normal poll backoff, not the setup loop.
        self._warn_unauthenticated_once()
        self.log.info(
            "reddit source configured",
            extra={
                "source": self.name,
                "mode": "oauth" if self.oauth else "public",
                "subreddits": [s.name for s in self.cfg.subreddits],
                "user_agent": self.user_agent,
            },
        )

    def next_interval(self) -> float:
        base = super().next_interval()
        per_poll = max(1, len(self.cfg.subreddits))
        floor = 0.0 if self.oauth else 60.0 * per_poll / UNAUTH_QPM
        now = self._clock()
        rl = self.rate_limit
        until_reset = rl.seconds_until_reset(now)
        if rl.remaining is None or until_reset is None or until_reset <= 0:
            return max(base, floor)
        if rl.remaining < max(RATE_LIMIT_MIN_REMAINING, per_poll):
            return max(base, until_reset + RATE_LIMIT_RESET_MARGIN_SECONDS + self.ctx.rng.uniform(0.0, 1.0))
        paced = until_reset * per_poll / rl.remaining  # spread the remaining budget over the window
        return max(base, floor, paced)

    # ------------------------------------------------------------------ polling

    async def poll(self) -> list[RawListing]:
        self._warn_unauthenticated_once()
        now = self._clock()
        until_reset = self.rate_limit.seconds_until_reset(now)
        if self.rate_limit.remaining is not None and self.rate_limit.remaining < 1 and until_reset:
            self.log.debug("reddit rate-limit window exhausted; skipping poll", extra={"source": self.name, "reset_in_s": round(until_reset, 1)})
            return []
        specs = list(self.cfg.subreddits)
        results = await asyncio.gather(*(self._poll_subreddit(spec) for spec in specs), return_exceptions=True)
        listings: list[RawListing] = []
        failures: list[tuple[str, BaseException]] = []
        for spec, result in zip(specs, results):
            if isinstance(result, BaseException):
                if not isinstance(result, Exception):  # CancelledError / KeyboardInterrupt
                    raise result
                failures.append((spec.name, result))
                continue
            listings.extend(result)
        if failures:
            # A block anywhere pauses the whole source: honour the longest cooldown seen
            # (a 30 s 429 on one subreddit must not mask a 30 min block wall on another).
            blocked = [exc for _, exc in failures if isinstance(exc, SourceBlocked)]
            if blocked:
                raise max(blocked, key=lambda exc: exc.cooldown_seconds or 0.0)
            for _, exc in failures:
                if isinstance(exc, SourceAuthError):
                    raise exc
            if len(failures) == len(specs):
                raise failures[0][1]
            for sub, exc in failures:
                self.log.warning("subreddit poll failed", extra={"source": self.name, "subreddit": sub, "error": repr(exc)[:300]})
        return listings

    async def _poll_subreddit(self, spec: "SubredditSpec") -> list[RawListing]:
        body = await self._fetch_listing(spec)
        if body is None:
            return []
        if len(body) >= PARSE_IN_THREAD_BYTES:
            outcome = await asyncio.to_thread(self._parse_body, spec, body)
        else:
            outcome = self._parse_body(spec, body)
        for listing in outcome.listings:
            listing.query = f"r/{spec.name}"
        if outcome.listings:
            self.m_posts.inc(len(outcome.listings), subreddit=spec.name, outcome="parsed")
        for reason, count in outcome.skipped.items():
            self.m_posts.inc(count, subreddit=spec.name, outcome=f"skip_{reason}")
        if outcome.errors:
            self.m_posts.inc(outcome.errors, subreddit=spec.name, outcome="malformed")
        return outcome.listings

    def _parse_body(self, spec: "SubredditSpec", body: bytes) -> ParseOutcome:
        """Decode + parse one listing body (pure; may run in a worker thread)."""
        try:
            payload = json_loads(body)
        except ValueError as exc:
            raise SourceError(f"r/{spec.name}: malformed JSON listing") from exc
        return parse_listing(
            payload,
            subreddit=spec.name,
            mode=spec.mode,
            skip_flairs=self.cfg.skip_flairs,
            locations=self.cfg.hardwareswap_locations if spec.mode == "swap" else (),
        )

    def listing_url(self, spec: "SubredditSpec") -> str:
        if self.oauth is not None:
            return f"{self.endpoints.oauth_base}/r/{spec.name}/new"
        return f"{self.endpoints.public_base}/r/{spec.name}/new.json"

    async def _fetch_listing(self, spec: "SubredditSpec") -> bytes | None:
        """GET one listing; returns the JSON body, or None when the subreddit is inaccessible."""
        url = self.listing_url(spec)
        params = {"limit": str(spec.limit), "raw_json": "1"}
        refreshed = False
        while True:
            headers = {"User-Agent": self.user_agent}
            token: str | None = None
            if self.oauth is not None:
                token = await self.oauth.token()
                headers["Authorization"] = f"bearer {token}"
            resp = await self.ctx.http.request(
                "GET",
                url,
                params=params,
                headers=headers,
                accept="application/json",
                expected=_LISTING_STATUSES,
                parse="bytes",
                browser_identity=False,
                max_bytes=MAX_LISTING_BYTES,
            )
            self._observe_rate_limit(resp)
            self.m_requests.inc(subreddit=spec.name, status=resp.status)
            body = bytes(resp.data) if isinstance(resp.data, (bytes, bytearray)) else b""
            if resp.status == 200:
                if _looks_like_html(resp.headers, body):
                    raise SourceBlocked(f"r/{spec.name}: Reddit served an HTML page instead of JSON", cooldown_seconds=self.block_cooldown)
                return body
            if resp.status == 401:
                if self.oauth is not None and token is not None and not refreshed:
                    self.oauth.invalidate(token)
                    refreshed = True
                    self.log.info("reddit token rejected; refreshing once", extra={"source": self.name, "subreddit": spec.name})
                    continue
                if self.oauth is not None:
                    raise SourceAuthError(f"r/{spec.name}: Reddit rejected a freshly issued OAuth token (HTTP 401)")
                raise SourceBlocked(f"r/{spec.name}: Reddit requires authentication (HTTP 401)", cooldown_seconds=self.block_cooldown)
            if resp.status == 429:
                raise SourceBlocked(f"r/{spec.name}: Reddit rate limit exceeded (HTTP 429)", cooldown_seconds=self._cooldown_429(resp))
            if (
                resp.status == 403
                and self.oauth is not None
                and token is not None
                and not refreshed
                and _looks_like_html(resp.headers, body)
            ):
                # Reddit answers an invalid/expired bearer with its HTML network-policy page
                # rather than a 401 from some networks: one fresh token before giving up.
                self.oauth.invalidate(token)
                refreshed = True
                self.log.info("reddit 403 block page with OAuth; retrying once with a fresh token", extra={"source": self.name, "subreddit": spec.name})
                continue
            return self._inaccessible(spec, resp, body)

    def _inaccessible(self, spec: "SubredditSpec", resp: HttpResponse, body: bytes) -> None:
        """403/404: a JSON body with ``reason`` is a private/banned subreddit; anything else is a block."""
        reason: Any = None
        if not _looks_like_html(resp.headers, body):
            try:
                payload = json_loads(body) if body.strip() else None
            except ValueError:
                payload = None
            if isinstance(payload, Mapping):
                reason = payload.get("reason")
        if reason:
            label = reason if isinstance(reason, str) and reason in _INACCESSIBLE_REASONS else "other"
            self.log.warning(
                "subreddit not accessible; skipping",
                extra={"source": self.name, "subreddit": spec.name, "status": resp.status, "reason": str(reason)[:100]},
            )
            self.m_posts.inc(subreddit=spec.name, outcome=f"inaccessible_{label}")
            return None
        if resp.status == 404 and not _looks_like_html(resp.headers, body):
            self.log.warning("subreddit not found; skipping", extra={"source": self.name, "subreddit": spec.name})
            self.m_posts.inc(subreddit=spec.name, outcome="inaccessible_not_found")
            return None
        mode = "OAuth" if self.oauth else "unauthenticated"
        raise SourceBlocked(
            f"r/{spec.name}: Reddit blocked the {mode} request (HTTP {resp.status})",
            cooldown_seconds=self.block_cooldown,
        )

    def _cooldown_429(self, resp: HttpResponse) -> float:
        hint = max(
            _reset_header(resp.headers) or 0.0,
            min(parse_retry_after(_header(resp.headers, "Retry-After")) or 0.0, RATE_LIMIT_MAX_RESET_SECONDS),
        )
        if self.oauth is None:
            return max(self.block_cooldown, hint)
        if hint <= 0:
            return OAUTH_429_DEFAULT_COOLDOWN_SECONDS
        return max(OAUTH_429_MIN_COOLDOWN_SECONDS, hint + RATE_LIMIT_RESET_MARGIN_SECONDS)

    def _observe_rate_limit(self, resp: HttpResponse) -> None:
        remaining = _float_header(resp.headers, "X-Ratelimit-Remaining")
        reset = _reset_header(resp.headers)
        used = _float_header(resp.headers, "X-Ratelimit-Used")
        self.rate_limit.observe(remaining=remaining, used=used, reset_seconds=reset, now=self._clock())
        if remaining is not None:
            self.m_remaining.set(remaining)

    def _warn_unauthenticated_once(self) -> None:
        if self.oauth is not None or self._unauth_warned:
            return
        self._unauth_warned = True
        self.log.warning(
            "reddit OAuth credentials not configured: falling back to the public .json endpoints, which Reddit "
            "shut down on 2026-05-28 (expect HTTP 403 blocks and long cooldowns); set REDDIT_CLIENT_ID / "
            "REDDIT_CLIENT_SECRET for an approved Data API app",
            extra={"source": self.name, "cooldown_s": self.block_cooldown},
        )


__all__ = [
    "DealTitle",
    "MoneySpan",
    "ParseOutcome",
    "PostSkipped",
    "RateLimitState",
    "RedditEndpoints",
    "RedditIngestor",
    "RedditOAuth",
    "SwapTitle",
    "build_user_agent",
    "common_skip_reason",
    "find_money",
    "iter_money",
    "iter_posts",
    "location_allowed",
    "parse_deal_post",
    "parse_deal_title",
    "parse_listing",
    "parse_location_tag",
    "parse_swap_post",
    "parse_swap_title",
    "payment_methods",
    "registrable_domain",
    "retailer_for_url",
    "swap_intent",
    "unescape_entities",
]
