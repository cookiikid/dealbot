"""Slickdeals ingestor: RSS 2.0 feeds (frontpage, Hot Deals forum, keyword searches).

Upstream behaviour (verified live 2026-10-06; see the research notes)
----------------------------------------------------------------------
* Feeds are plain RSS 2.0 (``text/xml``) served through Cloudflare, generated per
  request (``cf-cache-status: DYNAMIC``) and hard-capped at 25 items; paging/count
  parameters are ignored, so a poll only ever sees the newest 25 entries of a feed.
* The **Hot Deals forum** feed (``newsearch.php?searchin=first&forumchoice[]=9&rss=1``)
  is the lowest-latency feed: new threads appear within seconds of creation and
  ``pubDate`` is the thread creation time. The **frontpage** feed's ``pubDate`` is the
  time a deal was *promoted* (3-12 h after creation) — a quality signal, not a speed
  signal. **Keyword search** feeds are fuzzy and over-inclusive (they also match body
  text), which is fine: the text filter re-classifies every listing anyway.
* ``<link>`` is the thread URL plus ``utm_*`` parameters, never the merchant URL.
  ``<guid>`` is ``thread-<id>`` for recent items and a full permalink for older ones,
  so the thread id is always taken from ``/f/<id>`` in the link. That makes the same
  thread seen through several feeds map to the same ``source_id``; duplicates inside
  one poll are merged (feed list, frontpage/popular flags, missing fields).
* ``content:encoded`` carries the useful structure: a 300x300 thumbnail ``<img>``, a
  live ``Thumb Score: +N`` line and ``slickdeals.net/click`` outlinks with
  ``data-store-slug`` / ``data-product-exitWebsite`` / ``data-aps-asin`` attributes.
  feedparser's HTML sanitizer strips ``data-*`` attributes, so feeds are parsed with
  ``sanitize_html=False`` (descriptions are re-cleaned by the normalizer).
* Slickdeals currently sends no ``ETag``/``Last-Modified`` and ignores conditional
  headers. Requests are still conditional: it costs nothing, and a 304 (should they
  start honouring validators) means "no change" for that feed.
* Robots/ToS: ``robots.txt`` disallows ``/newsearch.php?*rss=*`` and the ToS restrict
  automated access. Run a single leased poller (``lease_ttl_seconds``), keep the
  volume low and alerts private.

Request volume
--------------
Configured feeds are fetched every poll. Profile search feeds (one per distinct search
term, built from ``search_feed_template`` with ``quote_plus``) rotate through
:data:`SEARCH_FEEDS_PER_POLL` per cycle: with ~20 terms and a 45 s interval each term is
refreshed every ~4 min (matching the research recommendation of "a few targeted feeds
every few minutes") instead of multiplying the request rate twenty-fold. Fetches run
concurrently, bounded by :data:`MAX_CONCURRENT_FEEDS`.

Blocks
------
Cloudflare Bot Management fronts the site. A feed answering 403/429/503, carrying a
``cf-mitigated`` header, or returning an HTML page instead of XML is *blocked*. One
bad feed is logged, counted and skipped. When no feed succeeded and at least one was
blocked, :class:`SourceBlocked` is raised with a cooldown that starts at
``cooldown_seconds`` and doubles for every consecutive blocked poll (capped at
:data:`BLOCK_COOLDOWN_MAX_SECONDS`, ``Retry-After`` honoured). When every feed failed
for other reasons (network, malformed XML, 404) a :class:`SourceError` lets the base
loop apply its normal backoff.

Parsing
-------
feedparser runs in a worker thread and is always handed a ``BytesIO``: given raw
``bytes``/``str`` it first tries to ``open()`` the argument as a *file name*, so a
hostile body such as ``b"/etc/passwd"`` would be read from local disk.

Extraction from free-text titles (validated by the research team against 456 live
titles, 454 matched the structured price):

* **price** — an editorial prefix (``"[Prime] $13.27* | ..."``, ``"$899: ..."``) wins;
  otherwise every ``$`` amount is a candidate except those introduced by
  save/extra/reg./was/list/MSRP/under/``+`` ... or followed by off/%/credit/gift
  card/rebate/cash back ...; candidates followed by a shipping/store/terminator
  context score higher, ties go to the right-most. Frontpage titles round prices
  (``$1150`` for ``$1149.99``), so a whole-dollar title price is refined with a body
  amount less than $1 away. Without a title price, the editorial body phrase
  ``"... for $X"`` is used. Entries with no price at all are **dropped**: handing them
  to the normalizer would let its description fallback read ``"Save $50"`` as the
  price of a GPU and fabricate a price error.
* **list price** — ``Reg./was/List Price/MSRP/Orig./retails for $X`` in the title, then
  the body text, then struck-through (``<s>/<del>``) body amounts; kept only when it is
  above the price.
* **retailer** — ``data-store-slug`` > ``data-product-exitWebsite`` > body
  ``"<a>Store</a> has ..."`` > title ``" at/@/from/via Store"`` > ``"Micro Center: ..."``
  title prefix > ``-at-<store>`` link slug; common stores are normalised
  (``best-buy`` / ``bestbuy.com`` / ``Best Buy`` -> ``Best Buy``).
* **shipping** — free shipping phrases (``Free S&H``, ``FS``, ``Shipping is free``,
  threshold-aware ``free shipping on orders $35+``) -> 0, ``+ $4.99 shipping`` -> 4.99.
* **outbound_url** — the first direct non-Slickdeals store link in the body, else an
  absolute store URL embedded in a ``/click`` redirect's query string, else
  ``https://www.amazon.com/dp/<ASIN>`` from ``data-aps-asin``. ``url`` stays the
  thread link (minus ``utm_*``).
* **spec tokens** (``extra``) — ``vram_gb`` (explicit ``32GB GDDR7``, the size next to
  the GPU model, or a desktop lookup table; system RAM such as ``64GB DDR5`` is
  ignored), ``refresh_hz``, ``size_in`` (also from LG/Samsung model codes) and
  ``panel`` in {OLED, QD-OLED, Mini-LED}.

The change signature ignores the title: the frontpage editorial title and the forum
title of one thread differ, and would otherwise flip-flop between polls.
"""

from __future__ import annotations

import asyncio
import calendar
import enum
import hashlib
import html
import io
import re
import time
from collections import Counter
from collections.abc import Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import parse_qsl, quote_plus, urlencode, urlsplit, urlunsplit

import feedparser

from deal_radar.core.backoff import parse_retry_after
from deal_radar.core.http import HttpStatusError
from deal_radar.core.logs import get_logger
from deal_radar.engine.types import RawListing, SourceKind
from deal_radar.sources.base import BaseIngestor, IngestorContext, SourceBlocked, SourceError

if TYPE_CHECKING:  # pragma: no cover
    from deal_radar.config_schema import Profile, SlickdealsSource

_log = get_logger("sources.slickdeals")

# --------------------------------------------------------------------------- constants

SOURCE_NAME = "slickdeals"
SLICKDEALS_BASE = "https://slickdeals.net"
FEED_ACCEPT = "application/rss+xml, application/xml;q=0.9, */*;q=0.8"
QUERY_PLACEHOLDER = "{query}"

MAX_FEED_BYTES = 4 * 1024 * 1024  # real feeds are ~80 KB raw (25 items)
MAX_CONCURRENT_FEEDS = 4
SEARCH_FEEDS_PER_POLL = 4  # profile search feeds fetched per cycle (round-robin)

BLOCK_STATUSES = frozenset({403, 429, 503})
# Block statuses are "expected" so the HTTP layer hands them back instead of retrying a
# Cloudflare wall several times within one poll.
_FETCH_STATUSES = (200, *sorted(BLOCK_STATUSES))
BLOCK_ESCALATION_STEPS = 4  # the block cooldown doubles up to 2**4 x cooldown_seconds
BLOCK_COOLDOWN_MAX_SECONDS = 3600.0

MAX_IMAGES = 4
MAX_DESCRIPTION_CHARS = 4000
MAX_STORE_NAME_CHARS = 40
MAX_LIST_PRICE_RATIO = 20.0  # a "was" price 20x the deal price is noise, not a list price

FRONTPAGE_CATEGORY = "frontpage deals"
POPULAR_CATEGORY = "popular deals"

_SLICKDEALS_HOSTS = ("slickdeals.net", "slickdealscdn.com")
# Links in deal posts that are never the store page (reviews, price trackers, socials).
_NON_STORE_HOSTS = (
    "youtube.com",
    "youtu.be",
    "imgur.com",
    "reddit.com",
    "redd.it",
    "twitter.com",
    "x.com",
    "facebook.com",
    "instagram.com",
    "tiktok.com",
    "camelcamelcamel.com",
    "keepa.com",
    "rtings.com",
    "google.com",
    "bit.ly",
)
_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".bmp", ".svg")

# Keys are lower-case alphanumerics only, so "Best Buy", "best-buy" and "bestbuy.com"
# (after the TLD is dropped) all hit "bestbuy".
_STORE_ALIASES: dict[str, str] = {
    "amazon": "Amazon",
    "amazoncom": "Amazon",
    "bestbuy": "Best Buy",
    "bby": "Best Buy",
    "newegg": "Newegg",
    "neweggbusiness": "Newegg Business",
    "walmart": "Walmart",
    "microcenter": "Micro Center",
    "bh": "B&H Photo",
    "bhphoto": "B&H Photo",
    "bhphotovideo": "B&H Photo",
    "bandh": "B&H Photo",
    "adorama": "Adorama",
    "dell": "Dell",
    "delltechnologies": "Dell",
    "hp": "HP",
    "hpstore": "HP",
    "lenovo": "Lenovo",
    "samsung": "Samsung",
    "samsungepp": "Samsung",
    "samsungeppedu": "Samsung",
    "lg": "LG",
    "lgelectronics": "LG",
    "target": "Target",
    "costco": "Costco",
    "samsclub": "Sam's Club",
    "bjs": "BJ's",
    "bjswholesale": "BJ's",
    "ebay": "eBay",
    "antonline": "Antonline",
    "woot": "Woot",
    "staples": "Staples",
    "officedepot": "Office Depot",
    "officedepotofficemax": "Office Depot",
    "gamestop": "GameStop",
    "apple": "Apple",
    "abt": "Abt",
    "crutchfield": "Crutchfield",
    "monoprice": "Monoprice",
    "nvidia": "NVIDIA",
    "homedepot": "Home Depot",
    "thehomedepot": "Home Depot",
    "lowes": "Lowe's",
    "kohls": "Kohl's",
    "macys": "Macy's",
    "steam": "Steam",
    "asus": "ASUS",
    "acer": "Acer",
    "msi": "MSI",
    "microsoft": "Microsoft",
    "microsoftstore": "Microsoft",
    "sony": "Sony",
    "googlestore": "Google Store",
    "qvc": "QVC",
    "meh": "Meh",
}
_DOMAIN_SUFFIXES = ("com", "net", "us")

# --------------------------------------------------------------------------- regexes

_AMOUNT = r"(?P<amt>\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?|\d+(?:\.\d{1,2})?)(?![\d,]?\d)"
_MONEY = r"\$\s?" + _AMOUNT

_PRICE_RE = re.compile(r"(?<![\w$])(?P<neg>-\s?)?" + _MONEY + r"(?P<star>\*)?")
_EDITORIAL_PREFIX_RE = re.compile(
    r"^(?:\[[^\]]*\]\s*)*(?:\([^)$|]{0,40}\)\s*)?\$\s?" + _AMOUNT + r"\*?\s*(?:or\s+less\s*)?(?:\||:|-\s|–\s?|—\s?)",
    re.I,
)
# Applied to the text right before a candidate amount.
_NOT_PRICE_BEFORE_RE = re.compile(
    r"(?:\b(?:save|saving|savings\s+of|extra|get|spend|on|over|orders?\s+(?:of|over)|min(?:imum)?(?:\s+purchase)?|"
    r"or\s+more|up\s+to|under|below|less\s+than|reg(?:ular(?:ly)?)?\.?|was|list(?:\s+price)?|msrp|retails?(?:\s+price)?|"
    r"orig(?:inal(?:ly)?|\.)?(?:\s+price)?|normally|compare\s+at|typically|usually|street\s+price|valued?\s+at)"
    r"\s*[:\-]?\s*|(?:\bw/|\bwith)\s*|\+\s*)$",
    re.I,
)
# Applied to the text right after a candidate amount.
_NOT_PRICE_AFTER_RE = re.compile(
    r"^\s*(?:(?:[A-Za-z&'.]+\s+){0,2}(?:credit|GC\b|gift\s*cards?|e-?gift|reward\s+(?:cards?|points?|dollars?))|"
    r"\+\s*(?:$|[),;|]|orders?\b|purchases?\b)|or\s+more|off\b|%|back\b|cash\s*back|rebate|rewards?\b|bonus|promo\b|"
    r"coupon(?!\s+price)|in\s+(?:credit|rewards|points)|minimum|min\b|orders?\b|(?:w/|with)\s+\w+\s+(?:card|credit))",
    re.I,
)
# A real price is usually followed by shipping, the store, a separator or the end.
_PRICE_TERMINAL_RE = re.compile(
    r"^\s*\*?\s*(?:\+\s*(?:free|fs\b|f/s|tax|s\s*&\s*h|s/h|ship|\$)|&\s*more|or\s+less|shipped\b|ac\b|after\b|w/|with\b|"
    r"\(|\[|@|at\s|from\s|via\s|\||,|-\s|–|—|$|\.\s*$|ymmv|in[\s-]store|free\s+ship|fs\b|f/s)",
    re.I,
)
_BODY_FOR_PRICE_RE = re.compile(r"\bfor\s+(?:only\s+|just\s+)?" + _MONEY, re.I)

_LIST_PRICE_RE = re.compile(
    r"(?:\b(?:list(?:\s+price)?|reg(?:ular(?:ly)?)?\.?|was|msrp|retails?(?:\s+(?:price|for))?|"
    r"orig(?:inal(?:ly)?|\.)?(?:\s+price)?|normally|compare\s+at|typically|usually|street\s+price)"
    r"\s*(?:price)?\s*(?:is|of|at|for)?\s*[:\-]?\s*)" + _MONEY,
    re.I,
)
_STRIKE_PRICE_RE = re.compile(r"<(?:s|strike|del)\b[^>]*>\s*(?:<[^>]+>\s*)*" + _MONEY, re.I)

_FREE_SHIPPING_RE = re.compile(
    r"\bfree\s+(?:s\s*&\s*h|s/h|shipping|ship|delivery|2-day\s+shipping|next-day\s+delivery)\b|"
    r"\bships?\s+free\b|\bshipping\s+is\s+free\b|\bfree\s+standard\s+shipping\b",
    re.I,
)
_FS_TOKEN_RE = re.compile(r"(?<![\w/])(?:FS|F/S)\b")  # case-sensitive: "fs" inside words is not shipping
_FREE_SHIPPING_THRESHOLD_RE = re.compile(
    r"\bfree\s+(?:standard\s+)?shipping\s+(?:on|with|for)\s+(?:orders?|purchases?)\s+(?:of\s+|over\s+)?" + _MONEY,
    re.I,
)
_PAID_SHIPPING_RE = re.compile(
    r"\+\s*" + _MONEY + r"\s*(?:shipping|ship|s\s*&\s*h|s/h|delivery)\b|\bshipping\s*(?:is|:|costs?)\s*" + _MONEY.replace("amt", "amt2"),
    re.I,
)
_IN_STORE_RE = re.compile(r"\bin[\s-]store(?:\s+(?:only|pickup))?\b|\bB&M\b", re.I)

_STORE_TITLE_RE = re.compile(
    r"(?:\s(?:at|At|AT|from|From|via|Via)\s+|\s?@\s?)"
    r"(?P<store>[A-Z][\w&'.!+\-]*(?:\s+(?:[A-Z0-9&][\w&'.!+\-]*|and|of|by|&)){0,3}?)"
    r"\s*(?=\(|\[|\+|,|\||-\s|\.\s*$|$|\s+(?:w/|with|for|on|via|after|YMMV|B&M|\$|\(|\[))"
)
_STORE_PREFIX_RE = re.compile(
    r"^(?:\[[^\]]*\]\s*)*(?P<store>Woot!?|Micro\s?Center|Best\s?Buy|Newegg|Amazon|Walmart|Costco|Sam'?s\s+Club|"
    r"B&H(?:\s+Photo(?:\s+Video)?)?|Adorama|Dell|HP|Lenovo|Samsung(?:\s+EPP(?:/EDU)?)?|LG|Target|eBay|Antonline|"
    r"Monoprice|Abt|Crutchfield|Staples|Office\s?Depot|BJ'?s|GameStop|Apple|Steam)\b[^:$]{0,25}:\s",
    re.I,
)
_BODY_STORE_HAS_RE = re.compile(
    r">\s*([^<>]{2,40}?)\s*</a>\s*(?:<span[^>]*>.*?</span>\s*)?(?:has|is\s+offering|is\s+having|offers)\b",
    re.I | re.S,
)
_LINK_STORE_SLUG_RE = re.compile(r"-(?:at|from)-([a-z0-9-]+?)/?$")

_ANCHOR_RE = re.compile(r"<a\b([^>]*)>", re.I)
_IMG_RE = re.compile(r"<img\b([^>]*)>", re.I)
_ATTR_RE = re.compile(r"([\w:.-]+)\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s>\"']+))")
_SKIP_IMAGE_RE = re.compile(
    r"/(?:smilies|smiley|emoji|emoticons?|icons?)/|(?:pixel|spacer|blank|tracking)\.(?:gif|png)|\.gif(?:$|\?)",
    re.I,
)
_ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")
_AMAZON_DOMAIN_RE = re.compile(r"^(?:www\.)?(amazon\.[a-z]{2,3}(?:\.[a-z]{2})?)$", re.I)

_THREAD_PATH_RE = re.compile(r"/f/(\d{4,12})(?=[-/]|$)")
_GUID_THREAD_RE = re.compile(r"^thread-(\d{4,12})$", re.I)
_THUMB_SCORE_RE = re.compile(r"Thumb\s+Score:\s*([+-]?\d+)", re.I)
_TITLE_TAG_RE = re.compile(r"\[([^\[\]]{1,40})\]\s*")

_SCRIPT_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1\s*>", re.I | re.S)
_INLINE_TAG_RE = re.compile(r"</?(?:a|b|i|u|s|em|strong|span|font|strike|del|ins|sup|sub|small|big|mark|abbr)\b[^>]*>", re.I)
_BLOCK_TAG_RE = re.compile(r"<\s*/?\s*(?:br|p|div|li|ul|ol|h\d|tr|table|blockquote|hr)\b[^>]*>", re.I)
_TAG_RE = re.compile(r"<[^>]+>")
_BBCODE_RE = re.compile(
    r"\[/?(?:list|\*|b|i|u|s|url|img|quote|color|size|font|center|left|right|spoiler|indent)(?:=[^\]]*)?\]",
    re.I,
)
_HSPACE_RE = re.compile(r"[ \t   ]+")

# --- spec tokens
_GPU_RE = re.compile(
    r"\bRTX\s*(?:™\s*)?(?P<pro>PRO\s+)?(?P<model>A?\d{4})"
    r"(?P<suffix>\s*(?:Ti\s*SUPER|Ti|SUPER|Ada(?:\s+Generation)?|Blackwell))?\b",
    re.I,
)
_VRAM_EXPLICIT_RE = re.compile(r"\b(?P<gb>\d{1,3})\s?GB\s?(?:GDDR\d[XW]?|HBM\d?e?|VRAM)\b", re.I)
_VRAM_NEAR_GPU_RE = re.compile(
    r"\b(?P<gb>\d{1,3})\s?G(?:B)?\b(?!\s?(?:LP)?DDR\d|\s?(?:RAM|SSD|Memory|Unified|eMMC|NVMe|HDD|Storage))",
    re.I,
)
_GPU_CONTEXT_STOP_RE = re.compile(r"[,;|/()$]")
_LAPTOP_RE = re.compile(r"\b(?:laptop|notebook)\b", re.I)
_PLAUSIBLE_VRAM_GB = frozenset({4, 6, 8, 10, 11, 12, 16, 20, 24, 32, 48, 80, 96})
# Desktop cards only; laptop GPUs of the same name ship with less memory.
_GPU_VRAM_GB: dict[str, int] = {
    "5090": 32,
    "5080": 16,
    "4090": 24,
    "4080": 16,
    "4080 SUPER": 16,
    "3090": 24,
    "3090 TI": 24,
    "A6000": 48,
    "A5000": 24,
    "A4000": 16,
    "6000 ADA": 48,
    "PRO 6000 BLACKWELL": 96,
    "PRO 6000": 96,
}
_REFRESH_RE = re.compile(r"\b(?P<hz>\d{2,3})\s?Hz\b", re.I)
_SIZE_RE = re.compile(
    r"(?<![\d.])(?P<inch>[1-9]\d(?:\.\d)?)\s?(?:\"|”|″|''|-?\s?inch(?:es)?\b|-?\s?in\.(?=\s|$)|\s?class\b)",
    re.I,
)
_LG_MODEL_RE = re.compile(r"\bOLED(?P<size>\d{2})(?P<series>[BCGMZ])(?P<year>\d)[A-Z]{0,4}\b")
_SAMSUNG_MODEL_RE = re.compile(r"\bQN(?P<size>\d{2})[A-Z]{1,3}\d{2,3}[A-Z]")
_PANEL_RE = re.compile(r"\b(?P<panel>QD[\s-]?OLED|Mini[\s-]?LED|W[\s-]?OLED|OLED)\b", re.I)


# --------------------------------------------------------------------------- feeds


@dataclass(frozen=True, slots=True)
class FeedTarget:
    """One feed URL polled by the ingestor."""

    name: str
    url: str
    query: str | None = None  # search term, for profile search feeds
    profile_hint: str | None = None

    @property
    def is_search(self) -> bool:
        return self.query is not None


class FeedOutcome(str, enum.Enum):
    OK = "ok"
    NOT_MODIFIED = "not_modified"
    BLOCKED = "blocked"
    ERROR = "error"


@dataclass(slots=True)
class FeedResult:
    feed: FeedTarget
    outcome: FeedOutcome
    listings: list[RawListing] = field(default_factory=list)
    detail: str = ""
    retry_after: float | None = None


@dataclass(slots=True)
class ParsedFeed:
    """Outcome of parsing one feed body (produced in a worker thread)."""

    listings: list[RawListing] = field(default_factory=list)
    skipped: Counter[str] = field(default_factory=Counter)
    errors: int = 0
    malformed: bool = False
    detail: str = ""


def build_search_feed_url(template: str, query: str) -> str:
    """Fill ``{query}`` in ``template`` with the ``quote_plus``-encoded search term."""
    if QUERY_PLACEHOLDER not in template:
        raise ValueError(f"search feed template must contain {QUERY_PLACEHOLDER}: {template!r}")
    return template.replace(QUERY_PLACEHOLDER, quote_plus(" ".join(query.split())))


def search_feed_targets(profiles: Iterable["Profile"], template: str) -> list[FeedTarget]:
    """One search feed per distinct (case-insensitive) profile search term, in config order."""
    targets: list[FeedTarget] = []
    seen: set[str] = set()
    for profile in profiles:
        for term in profile.search.terms:
            query = " ".join(term.split())
            key = query.casefold()
            if not query or key in seen:
                continue
            seen.add(key)
            targets.append(
                FeedTarget(
                    name=f"search:{query}",
                    url=build_search_feed_url(template, query),
                    query=query,
                    profile_hint=profile.id,
                )
            )
    return targets


# --------------------------------------------------------------------------- URL helpers


def _host(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""


def _host_in(host: str, suffixes: Sequence[str]) -> bool:
    return any(host == suffix or host.endswith("." + suffix) for suffix in suffixes)


def _is_http_url(url: str) -> bool:
    try:
        parts = urlsplit(url)
    except ValueError:
        return False
    return parts.scheme in ("http", "https") and bool(parts.hostname)


def is_slickdeals_url(url: str) -> bool:
    return _host_in(_host(url), _SLICKDEALS_HOSTS)


def strip_tracking(url: str) -> str:
    """Drop ``utm_*`` parameters (Slickdeals tags every feed link with ``utm_source=rss``)."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    if not parts.query:
        return url
    kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment))


def thread_id_from_url(url: str | None) -> str | None:
    """Thread id from a Slickdeals thread URL (``/f/20105466-slug`` -> ``"20105466"``)."""
    if not url or not is_slickdeals_url(url):
        return None
    try:
        path = urlsplit(url).path
    except ValueError:
        return None
    match = _THREAD_PATH_RE.search(path)
    return match.group(1) if match else None


def entry_source_id(link: str | None, guid: str | None) -> str | None:
    """Stable id: thread id from the link, else from the guid, else the guid, else a link hash."""
    thread_id = thread_id_from_url(link)
    if thread_id:
        return thread_id
    guid = (guid or "").strip()
    if guid:
        match = _GUID_THREAD_RE.match(guid)
        if match:
            return match.group(1)
        thread_id = thread_id_from_url(guid)
        if thread_id:
            return thread_id
        if len(guid) <= 80 and not any(ch.isspace() for ch in guid):
            return guid
        return hashlib.sha1(guid.encode("utf-8")).hexdigest()[:20]
    if link and _is_http_url(link):
        return hashlib.sha1(strip_tracking(link).encode("utf-8")).hexdigest()[:20]
    return None


def _query_value(url: str, key: str) -> str | None:
    try:
        query = urlsplit(url).query
    except ValueError:
        return None
    for k, v in parse_qsl(query, keep_blank_values=True):
        if k == key:
            return v
    return None


# --------------------------------------------------------------------------- block detection


_XML_PREFIX_RE = re.compile(rb"^\s*(?:<\?xml|<rss\b|<feed\b|<rdf:RDF\b)", re.I)
_HTML_PREFIXES = (b"<!doctype html", b"<html")
_HTML_MARKERS = (b"<html", b"<head>", b"<head ", b"<body")


def looks_like_feed(body: bytes) -> bool:
    return bool(_XML_PREFIX_RE.match(body[:512].lstrip(b"\xef\xbb\xbf")))


def looks_like_html(body: bytes, content_type: str | None = None) -> bool:
    """True for HTML documents (Cloudflare challenges, error pages) served instead of XML."""
    if looks_like_feed(body):
        return False
    head = body[:4096].lstrip(b"\xef\xbb\xbf \t\r\n").lower()
    if head.startswith(_HTML_PREFIXES) or any(marker in head for marker in _HTML_MARKERS):
        return True
    return bool(content_type and "html" in content_type.lower())


def is_block_response(headers: Mapping[str, str], body: bytes) -> bool:
    """Cloudflare marks challenge responses with ``cf-mitigated``; otherwise sniff for HTML."""
    if _header(headers, "cf-mitigated"):
        return True
    return looks_like_html(body, _header(headers, "Content-Type"))


def _header(headers: Mapping[str, str], name: str) -> str | None:
    value = headers.get(name)
    if value is None:
        lowered = name.lower()
        for key, candidate in headers.items():
            if key.lower() == lowered:
                return candidate
    return value


# --------------------------------------------------------------------------- text helpers


def _str(value: Any) -> str:
    return value if isinstance(value, str) else ""


def clean_title(value: Any) -> str:
    return " ".join(html.unescape(_str(value)).split())


def html_to_text(fragment: str) -> str:
    """Readable plain text from post HTML (block tags become newlines, BBCode removed)."""
    if not fragment:
        return ""
    text = _SCRIPT_RE.sub(" ", fragment)
    text = _INLINE_TAG_RE.sub("", text)
    text = _BLOCK_TAG_RE.sub("\n", text)
    text = _TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = _BBCODE_RE.sub(" ", text)
    lines = (_HSPACE_RE.sub(" ", line).strip() for line in text.splitlines())
    return "\n".join(line for line in lines if line)


def _amount(match: re.Match[str], group: str = "amt") -> float | None:
    raw = match.group(group)
    if not raw:
        return None
    try:
        value = float(raw.replace(",", ""))
    except ValueError:
        return None
    return value if value > 0 else None


def _attrs(tag_inner: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for m in _ATTR_RE.finditer(tag_inner):
        value = m.group(2) if m.group(2) is not None else m.group(3) if m.group(3) is not None else m.group(4) or ""
        attrs.setdefault(m.group(1).lower(), html.unescape(value).strip())
    return attrs


def _anchors(body_html: str) -> list[dict[str, str]]:
    return [_attrs(m.group(1)) for m in _ANCHOR_RE.finditer(body_html or "")]


# --------------------------------------------------------------------------- price extraction


@dataclass(frozen=True, slots=True)
class _PriceCandidate:
    value: float
    start: int
    has_cents: bool
    score: int


def _price_candidates(text: str) -> list[_PriceCandidate]:
    candidates: list[_PriceCandidate] = []
    for m in _PRICE_RE.finditer(text):
        if m.group("neg"):
            continue  # "-$50" is a discount
        value = _amount(m)
        if value is None:
            continue
        before = text[max(0, m.start() - 30) : m.start()]
        after = text[m.end() : m.end() + 40]
        if _NOT_PRICE_BEFORE_RE.search(before) or _NOT_PRICE_AFTER_RE.match(after):
            continue
        score = 0
        if _PRICE_TERMINAL_RE.match(after):
            score += 2
        if not after.strip(" *.!"):
            score += 1
        candidates.append(_PriceCandidate(value, m.start(), "." in m.group("amt"), score))
    return candidates


def _title_price(title: str) -> _PriceCandidate | None:
    editorial = _EDITORIAL_PREFIX_RE.match(title)
    if editorial:
        value = _amount(editorial)
        if value is not None:
            return _PriceCandidate(value, editorial.start("amt"), "." in editorial.group("amt"), 3)
    candidates = _price_candidates(title)
    if not candidates:
        return None
    return max(candidates, key=lambda c: (c.score, c.start))


def extract_price(title: str) -> float | None:
    """Deal price from a free-text Slickdeals title (None if the title carries none)."""
    best = _title_price(clean_title(title))
    return best.value if best is not None else None


def extract_body_price(body_text: str) -> float | None:
    """Editorial body phrase ``"<Store> has <Product> for $X"`` (first 600 chars only)."""
    head = body_text[:600]
    for m in _BODY_FOR_PRICE_RE.finditer(head):
        if _NOT_PRICE_AFTER_RE.match(head[m.end() : m.end() + 40]):
            continue
        value = _amount(m)
        if value is not None:
            return value
    return None


def refine_price(title_price: float, title_has_cents: bool, body_text: str) -> float:
    """Frontpage titles round (``$1150`` for ``$1149.99``): prefer a body amount < $1 away."""
    if title_has_cents:
        return title_price
    for candidate in _price_candidates(body_text[:1500]):
        if candidate.has_cents and 0 < abs(candidate.value - title_price) < 1.0:
            return candidate.value
    return title_price


def resolve_price(title: str, body_text: str) -> float | None:
    best = _title_price(clean_title(title))
    if best is not None:
        return round(refine_price(best.value, best.has_cents, body_text), 2)
    return extract_body_price(body_text)


def extract_list_price(title: str, body_text: str = "", body_html: str = "", price: float | None = None) -> float | None:
    """"Was"/MSRP/list price from the title, then the body text, then struck-through amounts."""

    def candidates() -> Iterable[float | None]:
        for text in (clean_title(title), body_text):
            for m in _LIST_PRICE_RE.finditer(text):
                yield _amount(m)
        for m in _STRIKE_PRICE_RE.finditer(body_html or ""):
            yield _amount(m)

    for value in candidates():
        if value is None or value < 1.0:
            continue
        if price is not None and not (price < value <= price * MAX_LIST_PRICE_RATIO):
            continue
        return round(value, 2)
    return None


def extract_shipping(title: str, body_text: str = "", price: float | None = None) -> float | None:
    """0.0 for free shipping, the amount for ``+ $X shipping``, None when unknown."""
    title = clean_title(title)
    if _FREE_SHIPPING_RE.search(title) or _FS_TOKEN_RE.search(title):
        return 0.0
    paid = _paid_shipping(title)
    if paid is not None:
        return paid
    if body_text:
        threshold = _FREE_SHIPPING_THRESHOLD_RE.search(body_text)
        if threshold is not None:
            minimum = _amount(threshold)
            if minimum is not None and price is not None and price >= minimum:
                return 0.0
            if minimum is not None:
                return _paid_shipping(body_text)
        if _FREE_SHIPPING_RE.search(body_text):
            return 0.0
        return _paid_shipping(body_text)
    return None


def _paid_shipping(text: str) -> float | None:
    m = _PAID_SHIPPING_RE.search(text)
    if m is None:
        return None
    value = _amount(m, "amt") if m.group("amt") else _amount(m, "amt2")
    return round(value, 2) if value is not None else None


# --------------------------------------------------------------------------- retailer / links / images


def _store_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def lookup_store(name: str | None) -> str | None:
    """Canonical name of a *known* store (slug, domain or display name), else None."""
    if not name:
        return None
    key = _store_key(html.unescape(name))
    if key.startswith("www"):
        key = key[3:]
    if not key:
        return None
    hit = _STORE_ALIASES.get(key)
    if hit:
        return hit
    for suffix in _DOMAIN_SUFFIXES:
        if key.endswith(suffix) and len(key) > len(suffix):
            hit = _STORE_ALIASES.get(key[: -len(suffix)])
            if hit:
                return hit
    return None


def normalize_store(name: str | None) -> str | None:
    """Canonical store name; unknown stores are cleaned up (slugs title-cased)."""
    if not name:
        return None
    cleaned = " ".join(html.unescape(name).split()).strip(" .,:;|-–—*")
    if not cleaned or len(cleaned) > MAX_STORE_NAME_CHARS or not cleaned[0].isalnum():
        return None
    known = lookup_store(cleaned)
    if known:
        return known
    parts = [p for p in re.split(r"\s*(?:&|/|,|\band\b|\bor\b)\s*", cleaned) if p]
    if len(parts) > 1:
        for part in parts:
            known = lookup_store(part)
            if known:
                return known
    cleaned = cleaned.rstrip("!")
    if cleaned.islower() and " " not in cleaned:
        cleaned = cleaned.replace("-", " ").replace("_", " ").title()
    return cleaned or None


def _title_store(title: str) -> str | None:
    found: str | None = None
    for m in _STORE_TITLE_RE.finditer(title):
        candidate = normalize_store(m.group("store"))
        if candidate:
            found = candidate  # trailing " at Store" is the convention: keep the last one
    if found:
        return found
    prefix = _STORE_PREFIX_RE.match(title)
    if prefix:
        return normalize_store(prefix.group("store"))
    return None


def extract_retailer(title: str, body_html: str = "", link: str | None = None) -> str | None:
    """Store name by precedence: data-store-slug > exit website > body "X has" > title > link slug."""
    anchors = _anchors(body_html)
    for attrs in anchors:
        store = normalize_store(attrs.get("data-store-slug"))
        if store:
            return store
    for attrs in anchors:
        store = lookup_store(attrs.get("data-product-exitwebsite"))
        if store:
            return store
    if body_html:
        m = _BODY_STORE_HAS_RE.search(body_html)
        if m:
            store = normalize_store(html_to_text(m.group(1)))
            if store:
                return store
    store = _title_store(clean_title(title))
    if store:
        return store
    if link:
        try:
            last_segment = urlsplit(link).path.rstrip("/").rsplit("/", 1)[-1]
        except ValueError:
            last_segment = ""
        m = _LINK_STORE_SLUG_RE.search(last_segment)
        if m:
            return lookup_store(m.group(1))
    return None


def _is_store_link(url: str) -> bool:
    if not _is_http_url(url):
        return False
    host = _host(url)
    if _host_in(host, _SLICKDEALS_HOSTS) or _host_in(host, _NON_STORE_HOSTS):
        return False
    try:
        path = urlsplit(url).path.lower()
    except ValueError:
        return False
    return not path.endswith(_IMAGE_EXTENSIONS)


def _embedded_target(click_url: str) -> str | None:
    """Absolute store URL carried in a Slickdeals ``/click`` redirect's query string, if any."""
    try:
        query = urlsplit(click_url).query
    except ValueError:
        return None
    for _, value in parse_qsl(query, keep_blank_values=False):
        value = value.strip()
        if value.lower().startswith(("http://", "https://")) and _is_store_link(value):
            return value
    return None


def extract_outbound_url(body_html: str) -> str | None:
    """Direct store link > store URL embedded in a /click redirect > Amazon ``/dp/<ASIN>``."""
    asin: str | None = None
    exit_site: str | None = None
    for attrs in _anchors(body_html):
        href = attrs.get("href", "")
        if href.startswith("//"):
            href = "https:" + href
        if _is_http_url(href):
            if is_slickdeals_url(href):
                target = _embedded_target(href)
                if target:
                    return target
            elif _is_store_link(href):
                return href
        candidate = (attrs.get("data-aps-asin") or "").strip().upper()
        if asin is None and _ASIN_RE.match(candidate):
            asin = candidate
            exit_site = attrs.get("data-product-exitwebsite") or exit_site
    if asin:
        m = _AMAZON_DOMAIN_RE.match(exit_site or "")
        domain = m.group(1).lower() if m else "amazon.com"
        return f"https://www.{domain}/dp/{asin}"
    return None


def _usable_image(url: Any) -> str | None:
    if not isinstance(url, str):
        return None
    url = html.unescape(url).strip()
    if url.startswith("//"):
        url = "https:" + url
    if not _is_http_url(url) or _SKIP_IMAGE_RE.search(url):
        return None
    return url


def extract_images(entry: Mapping[str, Any], body_html: str = "") -> list[str]:
    """media:thumbnail / media:content / image enclosures, then ``<img>`` tags in the body."""
    found: list[Any] = []
    for item in _as_list(entry.get("media_thumbnail")):
        if isinstance(item, Mapping):
            found.append(item.get("url"))
    for item in _as_list(entry.get("media_content")):
        if isinstance(item, Mapping):
            kind = f"{item.get('medium') or ''} {item.get('type') or ''}".lower()
            if not kind.strip() or "image" in kind:
                found.append(item.get("url"))
    for item in _as_list(entry.get("enclosures")):
        if isinstance(item, Mapping) and str(item.get("type") or "").lower().startswith("image/"):
            found.append(item.get("href") or item.get("url"))
    for m in _IMG_RE.finditer(body_html or ""):
        attrs = _attrs(m.group(1))
        if attrs.get("width") == "1" or attrs.get("height") == "1":
            continue
        found.append(attrs.get("src") or attrs.get("data-src"))
    images: list[str] = []
    for raw in found:
        url = _usable_image(raw)
        if url and url not in images:
            images.append(url)
        if len(images) >= MAX_IMAGES:
            break
    return images


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


# --------------------------------------------------------------------------- spec tokens


def extract_specs(title: str) -> dict[str, Any]:
    """Spec tokens from a title: vram_gb, gpu_model, refresh_hz, size_in, panel."""
    title = clean_title(title)
    specs: dict[str, Any] = {}
    gpu = _GPU_RE.search(title)
    if gpu:
        suffix = " ".join((gpu.group("suffix") or "").split()).upper().replace(" GENERATION", "")
        model = gpu.group("model").upper()
        key = " ".join(part for part in ("PRO" if gpu.group("pro") else "", model, suffix) if part)
        specs["gpu_model"] = f"RTX {key}"
        vram = _vram_near(title, gpu.end())
        if vram is None:
            explicit = _VRAM_EXPLICIT_RE.search(title)
            vram = int(explicit.group("gb")) if explicit else None
        if vram is None and not _LAPTOP_RE.search(title):
            vram = _GPU_VRAM_GB.get(key)
        if vram is not None:
            specs["vram_gb"] = vram
    else:
        explicit = _VRAM_EXPLICIT_RE.search(title)
        if explicit:
            specs["vram_gb"] = int(explicit.group("gb"))
    rates = [int(m.group("hz")) for m in _REFRESH_RE.finditer(title)]
    rates = [hz for hz in rates if 24 <= hz <= 1000]
    if rates:
        specs["refresh_hz"] = max(rates)
    size = _size_inches(title)
    if size is not None:
        specs["size_in"] = size
    panel = _PANEL_RE.search(title)
    if panel:
        token = panel.group("panel").lower()
        specs["panel"] = "QD-OLED" if token.startswith("qd") else "Mini-LED" if token.startswith("mini") else "OLED"
    elif _LG_MODEL_RE.search(title):
        specs["panel"] = "OLED"
    return specs


def _vram_near(title: str, gpu_end: int) -> int | None:
    window = title[gpu_end : gpu_end + 40]
    stop = _GPU_CONTEXT_STOP_RE.search(window)
    if stop:
        window = window[: stop.start()]
    for m in _VRAM_NEAR_GPU_RE.finditer(window):
        value = int(m.group("gb"))
        if value in _PLAUSIBLE_VRAM_GB:
            return value
    return None


def _size_inches(title: str) -> float | int | None:
    for m in _SIZE_RE.finditer(title):
        value = float(m.group("inch"))
        if 10 <= value <= 120:
            return int(value) if value.is_integer() else value
    for regex in (_LG_MODEL_RE, _SAMSUNG_MODEL_RE):
        m = regex.search(title)
        if m:
            value = int(m.group("size"))
            if 10 <= value <= 120:
                return value
    return None


def title_tags(title: str) -> list[str]:
    """Leading bracket tags: ``"[Prime, SnS] $13.27 | ..."`` -> ``["Prime", "SnS"]``."""
    tags: list[str] = []
    pos = 0
    title = clean_title(title)
    while True:
        m = _TITLE_TAG_RE.match(title, pos)
        if m is None:
            return tags
        tags.extend(part.strip() for part in m.group(1).split(",") if part.strip())
        pos = m.end()


# --------------------------------------------------------------------------- entries


class EntrySkipped(Exception):
    """An entry that cannot become a listing (counted per reason, never fatal)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def entry_posted_at(entry: Mapping[str, Any]) -> datetime | None:
    """``published_parsed``/``updated_parsed`` (feedparser normalises them to UTC)."""
    for key in ("published_parsed", "updated_parsed"):
        value = entry.get(key)
        if not value:
            continue
        try:
            return datetime.fromtimestamp(calendar.timegm(value), tz=timezone.utc)
        except (TypeError, ValueError, OverflowError):
            continue
    return None


def _entry_body_html(entry: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for item in _as_list(entry.get("content")):
        if isinstance(item, Mapping):
            value = _str(item.get("value"))
            if value:
                parts.append(value)
    if parts:
        return "\n".join(parts)
    return _str(entry.get("summary"))


def _entry_category(entry: Mapping[str, Any]) -> str | None:
    for tag in _as_list(entry.get("tags")):
        if isinstance(tag, Mapping):
            term = _str(tag.get("term")).strip()
            if term:
                return term
    return None


def _thread_url(link: str, guid: str, source_id: str) -> str | None:
    for candidate in (link, guid):
        if candidate and _is_http_url(candidate):
            return strip_tracking(candidate)
    if source_id.isdigit():
        return f"{SLICKDEALS_BASE}/f/{source_id}"
    return None


def build_listing(
    entry: Mapping[str, Any],
    feed_name: str,
    *,
    query: str | None = None,
    profile_hint: str | None = None,
) -> RawListing:
    """Convert one feedparser entry into a :class:`RawListing` or raise :class:`EntrySkipped`."""
    title = clean_title(entry.get("title"))
    if not title:
        raise EntrySkipped("no_title")
    link = _str(entry.get("link")).strip()
    guid = _str(entry.get("id")).strip()
    source_id = entry_source_id(link, guid)
    if source_id is None:
        raise EntrySkipped("no_id")
    url = _thread_url(link, guid, source_id)
    if url is None:
        raise EntrySkipped("no_url")

    body_html = _entry_body_html(entry)
    body_text = html_to_text(body_html)
    price = resolve_price(title, body_text)
    if price is None:
        raise EntrySkipped("no_price")

    thumb = _THUMB_SCORE_RE.search(body_text)
    description = _THUMB_SCORE_RE.sub("", body_text, count=1).strip()
    category = _entry_category(entry)
    category_key = (category or "").casefold()
    anchors = _anchors(body_html)

    extra: dict[str, Any] = {"feeds": [feed_name]}
    if source_id.isdigit():
        extra["thread_id"] = source_id
    if category:
        extra["feed_category"] = category
    extra["frontpage"] = category_key == FRONTPAGE_CATEGORY or _query_value(link, "utm_content") == "fp"
    extra["popular"] = category_key == POPULAR_CATEGORY
    if thumb:
        extra["thumb_score"] = int(thumb.group(1))
    for attr, key in (("data-store-slug", "store_slug"), ("data-store-id", "store_id"), ("data-aps-asin", "asin")):
        value = next((a[attr] for a in anchors if a.get(attr)), None)
        if value:
            extra[key] = value
    author = _str(entry.get("author")).strip()
    if author:
        extra["author"] = author
    tags = title_tags(title)
    if tags:
        extra["title_tags"] = tags
    if _IN_STORE_RE.search(title):
        extra["in_store_only"] = True
    extra.update(extract_specs(title))

    return RawListing(
        source=SOURCE_NAME,
        source_kind=SourceKind.AGGREGATOR,
        source_id=source_id,
        url=url,
        title=title,
        description=description[:MAX_DESCRIPTION_CHARS],
        price=price,
        currency="USD",
        shipping=extract_shipping(title, body_text, price),
        list_price=extract_list_price(title, body_text, body_html, price),
        image_urls=extract_images(entry, body_html),
        posted_at=entry_posted_at(entry),
        retailer=extract_retailer(title, body_html, link),
        outbound_url=extract_outbound_url(body_html),
        query=query,
        profile_hint=profile_hint,
        extra=extra,
    )


def parse_entry(
    entry: Mapping[str, Any],
    feed_name: str,
    *,
    query: str | None = None,
    profile_hint: str | None = None,
) -> RawListing | None:
    """:func:`build_listing`, returning None for entries without title, id or price."""
    try:
        return build_listing(entry, feed_name, query=query, profile_hint=profile_hint)
    except EntrySkipped:
        return None


def parse_feed(data: bytes, content_type: str | None = None) -> Any:
    """feedparser over a ``BytesIO`` (never raw bytes: feedparser tries those as a file name)."""
    headers = {"content-type": content_type} if content_type else None
    return feedparser.parse(io.BytesIO(data), sanitize_html=False, response_headers=headers)


def parse_feed_listings(data: bytes, feed: FeedTarget, content_type: str | None = None) -> ParsedFeed:
    """Parse a feed body into listings; CPU-bound, run it in a worker thread."""
    parsed = parse_feed(data, content_type)
    entries = list(parsed.get("entries") or [])
    if not entries and parsed.get("bozo"):
        return ParsedFeed(malformed=True, detail=repr(parsed.get("bozo_exception"))[:200])
    result = ParsedFeed()
    for entry in entries:
        try:
            result.listings.append(build_listing(entry, feed.name, query=feed.query, profile_hint=feed.profile_hint))
        except EntrySkipped as exc:
            result.skipped[exc.reason] += 1
        except Exception as exc:  # noqa: BLE001 - one malformed entry must not drop the feed
            result.errors += 1
            _log.debug("slickdeals entry failed to parse", extra={"feed": feed.name, "error": repr(exc)[:300]})
    return result


def merge_duplicates(listings: Sequence[RawListing]) -> list[RawListing]:
    """Collapse listings of the same thread seen through several feeds (first one wins)."""
    merged: dict[str, RawListing] = {}
    for raw in listings:
        primary = merged.get(raw.source_id)
        if primary is None:
            merged[raw.source_id] = raw
        else:
            _merge_into(primary, raw)
    return list(merged.values())


def _merge_into(primary: RawListing, other: RawListing) -> None:
    feeds = list(primary.extra.get("feeds") or [])
    for name in other.extra.get("feeds") or []:
        if name not in feeds:
            feeds.append(name)
    primary.extra["feeds"] = feeds
    for flag in ("frontpage", "popular"):
        primary.extra[flag] = bool(primary.extra.get(flag) or other.extra.get(flag))
    scores = [s for s in (primary.extra.get("thumb_score"), other.extra.get("thumb_score")) if isinstance(s, int)]
    if scores:
        primary.extra["thumb_score"] = max(scores)
    for key, value in other.extra.items():
        primary.extra.setdefault(key, value)
    for attr in ("shipping", "list_price", "retailer", "outbound_url", "posted_at", "query", "profile_hint"):
        if getattr(primary, attr) is None and getattr(other, attr) is not None:
            setattr(primary, attr, getattr(other, attr))
    for url in other.image_urls:
        if url not in primary.image_urls and len(primary.image_urls) < MAX_IMAGES:
            primary.image_urls.append(url)
    if not primary.description and other.description:
        primary.description = other.description


# --------------------------------------------------------------------------- ingestor


class SlickdealsIngestor(BaseIngestor):
    """Polls the configured Slickdeals RSS feeds plus rotating profile search feeds."""

    name: ClassVar[str] = "slickdeals"
    kind: ClassVar[SourceKind] = SourceKind.AGGREGATOR

    def __init__(self, cfg: "SlickdealsSource", ctx: IngestorContext) -> None:
        super().__init__(cfg, ctx)
        self.cfg: SlickdealsSource = cfg
        self.static_feeds: list[FeedTarget] = [FeedTarget(name=f.name, url=f.url) for f in cfg.feeds]
        self.search_feeds: list[FeedTarget] = []
        if cfg.search_feeds_from_profiles:
            try:
                static_urls = {f.url for f in self.static_feeds}
                self.search_feeds = [
                    t for t in search_feed_targets(self.search_profiles(), cfg.search_feed_template) if t.url not in static_urls
                ]
            except ValueError as exc:
                self.log.error("slickdeals search feeds disabled", extra={"source": self.name, "error": str(exc)})
        self._search_cursor = 0
        self._consecutive_blocks = 0
        self._warned_no_feeds = False
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_FEEDS)
        m = ctx.metrics
        self.m_fetch = m.counter("slickdeals_feed_fetch_total", "Slickdeals feed fetches by outcome", ("feed", "outcome"))
        self.m_entries = m.counter("slickdeals_entries_total", "Slickdeals feed entries by parse outcome", ("feed", "outcome"))
        self.m_parse_ms = m.histogram("slickdeals_feed_parse_ms", "Slickdeals feed parse time (ms)", ("feed",))

    # ------------------------------------------------------------------ hooks

    async def setup(self) -> None:
        self.log.info(
            "slickdeals source configured",
            extra={
                "source": self.name,
                "feeds": [f.name for f in self.static_feeds],
                "search_feeds": len(self.search_feeds),
                "search_feeds_per_poll": min(len(self.search_feeds), SEARCH_FEEDS_PER_POLL),
            },
        )

    def signature(self, raw: RawListing) -> Hashable:
        # The frontpage editorial title and the forum title of one thread differ; keying
        # the change detector on them would re-emit the thread whenever a feed drops out.
        return (str(raw.price), str(raw.shipping), raw.in_stock)

    # ------------------------------------------------------------------ polling

    def feeds_for_poll(self) -> list[FeedTarget]:
        """Configured feeds + the next round-robin slice of profile search feeds."""
        feeds = list(self.static_feeds)
        total = len(self.search_feeds)
        if total:
            per_poll = min(total, SEARCH_FEEDS_PER_POLL)
            start = self._search_cursor % total
            feeds.extend(self.search_feeds[(start + i) % total] for i in range(per_poll))
            self._search_cursor = (start + per_poll) % total
        return feeds

    async def poll(self) -> list[RawListing]:
        feeds = self.feeds_for_poll()
        if not feeds:
            if not self._warned_no_feeds:
                self._warned_no_feeds = True
                self.log.warning("slickdeals enabled without feeds or search terms", extra={"source": self.name})
            return []
        gathered = await asyncio.gather(*(self._poll_feed(feed) for feed in feeds), return_exceptions=True)
        results: list[FeedResult] = []
        for feed, item in zip(feeds, gathered):
            if isinstance(item, BaseException):
                if not isinstance(item, Exception):  # CancelledError / KeyboardInterrupt
                    raise item
                item = FeedResult(feed, FeedOutcome.ERROR, detail=repr(item)[:300])
            results.append(item)

        listings: list[RawListing] = []
        failed: list[FeedResult] = []
        succeeded = 0
        for result in results:
            self.m_fetch.inc(feed=result.feed.name, outcome=result.outcome.value)
            if result.outcome in (FeedOutcome.OK, FeedOutcome.NOT_MODIFIED):
                succeeded += 1
                listings.extend(result.listings)
            else:
                failed.append(result)

        if not succeeded:
            blocked = [r for r in failed if r.outcome is FeedOutcome.BLOCKED]
            summary = "; ".join(f"{r.feed.name}: {r.detail}" for r in failed)[:500]
            if blocked:
                retry_after = max((r.retry_after or 0.0 for r in blocked), default=0.0)
                cooldown = self._block_cooldown(retry_after)
                raise SourceBlocked(f"all {len(failed)} Slickdeals feeds failed ({len(blocked)} blocked): {summary}", cooldown_seconds=cooldown)
            raise SourceError(f"all {len(failed)} Slickdeals feeds failed: {summary}")

        self._consecutive_blocks = 0
        for result in failed:
            self.log.warning(
                "slickdeals feed skipped",
                extra={"source": self.name, "feed": result.feed.name, "outcome": result.outcome.value, "error": result.detail},
            )
        return merge_duplicates(listings)

    def _block_cooldown(self, retry_after: float) -> float:
        """``cooldown_seconds`` doubling per consecutive blocked poll, capped; Retry-After honoured."""
        self._consecutive_blocks += 1
        base = float(self.cfg.cooldown_seconds)
        escalated = base * 2 ** min(self._consecutive_blocks - 1, BLOCK_ESCALATION_STEPS)
        cap = max(BLOCK_COOLDOWN_MAX_SECONDS, base)
        return max(base, min(max(escalated, retry_after), cap))

    async def _poll_feed(self, feed: FeedTarget) -> FeedResult:
        """Fetch + parse one feed. Never raises (except cancellation)."""
        async with self._semaphore:
            try:
                resp = await self.ctx.http.request(
                    "GET",
                    feed.url,
                    parse="bytes",
                    accept=FEED_ACCEPT,
                    browser_identity=True,
                    conditional=True,
                    expected=_FETCH_STATUSES,
                    max_bytes=MAX_FEED_BYTES,
                )
            except asyncio.CancelledError:
                raise
            except HttpStatusError as exc:
                if exc.status in BLOCK_STATUSES:
                    return FeedResult(
                        feed, FeedOutcome.BLOCKED, detail=f"HTTP {exc.status}", retry_after=parse_retry_after(exc.headers.get("Retry-After"))
                    )
                return FeedResult(feed, FeedOutcome.ERROR, detail=f"HTTP {exc.status}")
            except Exception as exc:  # noqa: BLE001 - network/size failures only skip this feed
                return FeedResult(feed, FeedOutcome.ERROR, detail=repr(exc)[:300])

        if resp.not_modified:
            return FeedResult(feed, FeedOutcome.NOT_MODIFIED)
        body = bytes(resp.data) if isinstance(resp.data, (bytes, bytearray)) else b""
        content_type = _header(resp.headers, "Content-Type")
        if resp.status in BLOCK_STATUSES or is_block_response(resp.headers, body):
            self._forget_validators(feed)
            return FeedResult(
                feed,
                FeedOutcome.BLOCKED,
                detail=f"HTTP {resp.status} {'challenge/HTML page' if resp.status == 200 else 'block'}",
                retry_after=parse_retry_after(_header(resp.headers, "Retry-After")),
            )

        started = time.perf_counter()
        try:
            parsed = await asyncio.to_thread(parse_feed_listings, body, feed, content_type)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - feedparser is lenient, but never trust it fully
            self._forget_validators(feed)
            return FeedResult(feed, FeedOutcome.ERROR, detail=f"parse failed: {exc!r}"[:300])
        self.m_parse_ms.observe((time.perf_counter() - started) * 1000.0, feed=feed.name)

        if parsed.malformed:
            self._forget_validators(feed)
            self.m_entries.inc(feed=feed.name, outcome="malformed_feed")
            if looks_like_html(body, content_type):
                return FeedResult(feed, FeedOutcome.BLOCKED, detail="HTML instead of RSS")
            return FeedResult(feed, FeedOutcome.ERROR, detail=f"malformed feed: {parsed.detail}")

        if parsed.listings:
            self.m_entries.inc(len(parsed.listings), feed=feed.name, outcome="parsed")
        for reason, count in parsed.skipped.items():
            self.m_entries.inc(count, feed=feed.name, outcome=f"skip_{reason}")
        if parsed.errors:
            self.m_entries.inc(parsed.errors, feed=feed.name, outcome="error")
            self.log.debug("slickdeals entries failed to parse", extra={"source": self.name, "feed": feed.name, "count": parsed.errors})
        return FeedResult(feed, FeedOutcome.OK, listings=parsed.listings)

    def _forget_validators(self, feed: FeedTarget) -> None:
        # Never revalidate against a challenge/broken body: a later 304 would mean
        # "unchanged since the block page" and hide the real feed.
        self.ctx.http.conditional.store(feed.url, {})


__all__ = [
    "BLOCK_COOLDOWN_MAX_SECONDS",
    "BLOCK_STATUSES",
    "EntrySkipped",
    "FEED_ACCEPT",
    "FeedOutcome",
    "FeedResult",
    "FeedTarget",
    "MAX_CONCURRENT_FEEDS",
    "ParsedFeed",
    "SEARCH_FEEDS_PER_POLL",
    "SlickdealsIngestor",
    "build_listing",
    "build_search_feed_url",
    "clean_title",
    "entry_posted_at",
    "entry_source_id",
    "extract_body_price",
    "extract_images",
    "extract_list_price",
    "extract_outbound_url",
    "extract_price",
    "extract_retailer",
    "extract_shipping",
    "extract_specs",
    "html_to_text",
    "is_block_response",
    "is_slickdeals_url",
    "looks_like_feed",
    "looks_like_html",
    "lookup_store",
    "merge_duplicates",
    "normalize_store",
    "parse_entry",
    "parse_feed",
    "parse_feed_listings",
    "refine_price",
    "resolve_price",
    "search_feed_targets",
    "strip_tracking",
    "thread_id_from_url",
    "title_tags",
]
