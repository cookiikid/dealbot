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

Request volume and per-feed cadence
-----------------------------------
A configured feed without ``interval_seconds`` is fetched on every poll (the Hot Deals
forum feed: new threads appear 0-22 s after creation, so it drives the source's
``poll_interval_seconds``). A feed with ``interval_seconds`` (frontpage/popular: their
``pubDate`` is the promotion time, hours after creation) is fetched only once it is due;
a poll in which nothing is due is a quiet, empty success, and :meth:`next_interval`
never sleeps past the moment the next such feed falls due, so a jittered poll interval
cannot stretch a feed's cadence. The schedule uses a monotonic clock
(:attr:`SlickdealsIngestor.clock`, injectable for tests); a cancelled poll gives its
feeds back. Profile search feeds (one per distinct search term, built from
``search_feed_template`` with ``quote_plus``; off in the shipped config because
``robots.txt`` disallows keyword-search RSS) rotate through :data:`SEARCH_FEEDS_PER_POLL` per cycle
instead of multiplying the request rate. Fetches run concurrently, bounded by
:data:`MAX_CONCURRENT_FEEDS`. Feeds hold exactly the newest 25 items.

Blocks
------
Cloudflare Bot Management fronts the site. A feed answering 403/429/503, carrying a
``cf-mitigated`` header, or returning an HTML page instead of XML is *blocked*. One
bad feed is logged, counted and skipped. When no feed succeeded and at least one was
blocked, :class:`SourceBlocked` is raised with a cooldown that starts at
``cooldown_seconds`` and doubles for every consecutive blocked poll (capped at
:data:`BLOCK_COOLDOWN_MAX_SECONDS`, ``Retry-After`` honoured). When every feed failed
for other reasons (network, malformed XML, 404) a :class:`SourceError` lets the base
loop apply its normal backoff. Block statuses are "expected" by the request, so the HTTP
layer does not see them as blocks: every blocked feed burns the host's browser identity
itself (``ctx.http.identities.burn``) so the next request presents a different one.

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
  card/rebate/cash back ..., and a ``-$X``/``- $X`` right after another amount (a
  discount: ``"$1,099.99 -$100 w/ code"``). Elsewhere the dash is the live separator
  convention (``"... Gaming Monitor - $679.00"``, ``"... S3225QC -$559.99 @ Amazon"``).
  Candidates followed by a shipping/store/terminator
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
  (``best-buy`` / ``bestbuy.com`` / ``Best Buy`` -> ``Best Buy``; live multi-word slugs
  such as ``hp-small-medium-business`` by their leading store name), and a third-party
  seller written ``"<seller> via Amazon"`` resolves to the marketplace.
* **shipping** — free shipping phrases (``Free S&H``, ``FS``, ``Shipping is free``) -> 0
  and ``+ $4.99 shipping`` -> 4.99. A free phrase followed by a threshold (``Free
  Shipping w/ Prime or on $35+``, ``free shipping on orders over $35``) only counts when
  the price reaches it; below it the cost is unknown (None) — never the threshold.
* **outbound_url** — the first direct non-Slickdeals store link in the body, else an
  absolute store URL embedded in a ``/click`` redirect's query string, else
  ``https://www.amazon.com/dp/<ASIN>`` from ``data-aps-asin``. It must agree with the
  retailer: links of another known store are skipped and the ASIN is only used when the
  retailer is Amazon (multi-store posts often list Amazon second). ``url`` stays the
  thread link (minus ``utm_*``).
* **spec tokens** (``extra``) — ``vram_gb`` (explicit ``32GB GDDR7``, the size next to
  the GPU model, or a desktop lookup table; system RAM such as ``64GB DDR5`` is
  ignored), ``refresh_hz``, ``size_in`` (also from LG/Samsung model codes) and
  ``panel`` in {OLED, QD-OLED, Mini-LED}.

Every regex that runs over post HTML is linear-time: tag/attribute scans stop at the
next ``<`` and nested repetitions are bounded, and unclosed ``<script>`` blocks are cut
procedurally, so a hostile 4 MB body cannot stall the parser thread.

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
from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
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
# A feed with ``interval_seconds`` counts as due this much early, so a poll that wakes a
# hair before the due time (timer resolution) does not push the feed a whole cycle back.
FEED_DUE_TOLERANCE_SECONDS = 1.0
MIN_POLL_WAIT_SECONDS = 0.05

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
    "costcowholesale": "Costco",
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
    "dickssportinggoods": "Dick's Sporting Goods",
    "dicks": "Dick's Sporting Goods",
    "originpc": "Origin PC",
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

_PRICE_RE = re.compile(r"(?<![\w$])" + _MONEY + r"(?P<star>\*)?")
# A dash right before a candidate ("- $679.00", "-$559.99") ...
_DASH_BEFORE_RE = re.compile(r"[-–]\s?$")
# ... is a discount when it directly follows another amount ("$1,099.99 -$100 w/ code").
_AMOUNT_BEFORE_RE = re.compile(r"\$\s?\d[\d,]*(?:\.\d{1,2})?\*?\s*$")
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
# Bounded: at most four wrapper tags between <s>/<del> and the amount ("<s><b>$1,399</b>").
_STRIKE_PRICE_RE = re.compile(r"<(?:s|strike|del)\b[^<>]{0,500}>\s*(?:<[^<>]{1,500}>\s*){0,4}" + _MONEY, re.I)

_FREE_SHIPPING_RE = re.compile(
    r"\bfree\s+(?:s\s*&\s*h|s/h|shipping|ship|delivery|2-day\s+shipping|next-day\s+delivery)\b|"
    r"\bships?\s+free\b|\bshipping\s+is\s+free\b|\bfree\s+standard\s+shipping\b",
    re.I,
)
_FS_TOKEN_RE = re.compile(r"(?<![\w/])(?:FS|F/S)\b")  # case-sensitive: "fs" inside words is not shipping
# Matched right after a free-shipping phrase, within the same clause: "w/ Prime or on $35+",
# " on orders over $35", " with Prime or on $35+ orders". The amount is a threshold, not a cost.
_SHIPPING_THRESHOLD_RE = re.compile(
    r"[^.!?;\n$]{0,40}?\b(?:on|over|above|for|with|w/)\s+(?:(?:all\s+)?(?:orders?|purchases?)\s+)?"
    r"(?:of\s+|over\s+|above\s+)?" + _MONEY,
    re.I,
)
_PAID_SHIPPING_RES = (
    re.compile(r"\+\s*" + _MONEY + r"\s*(?:shipping|ship|s\s*&\s*h|s/h|delivery)\b", re.I),  # "+ $4.99 shipping"
    re.compile(r"\bshipping\s*(?:is|:|costs?)\s*" + _MONEY, re.I),  # "Shipping is $9.99"
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
# Body "<a>Store</a> [<span>[<a>store.com</a>]</span>] has ...": matched procedurally (see
# _body_store_has) so a long run of unclosed <span>s cannot make the search quadratic.
_ANCHOR_TEXT_RE = re.compile(r">([^<>]{2,80})</a\s*>", re.I)
_SPAN_OPEN_RE = re.compile(r"\s*<span\b[^<>]{0,500}>", re.I)
_SPAN_CLOSE_RE = re.compile(r"</span\s*>", re.I)
_SPAN_SCAN_CHARS = 3000  # the live "[<a ...>bhphotovideo.com</a>]" span is ~700 chars
_STORE_VERB_RE = re.compile(r"\s*(?:has|is\s+offering|is\s+having|offers)\b", re.I)
_LINK_STORE_SLUG_RE = re.compile(r"-(?:at|from)-([a-z0-9-]{1,60}?)/?$")
_VIA_SELLER_RE = re.compile(r"\s+via\s+", re.I)
_STORE_TOKEN_SPLIT_RE = re.compile(r"[\s_-]+")

# Tag scans stop at the next "<" so that "<a <a <a ..." stays linear.
_ANCHOR_RE = re.compile(r"<a\b([^<>]*)>", re.I)
_IMG_RE = re.compile(r"<img\b([^<>]*)>", re.I)
# Names only start at a boundary and an unclosed quote runs to the end of the tag: both keep
# attribute parsing linear in the tag length.
_ATTR_RE = re.compile(r"(?<![\w:.-])([\w:.-]+)\s*=\s*(?:\"([^\"]*)\"?|'([^']*)'?|([^\s>\"']+))")
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

_SCRIPT_OPEN_RE = re.compile(r"<(script|style)\b[^<>]{0,1000}>", re.I)
_SCRIPT_CLOSE_RES = {name: re.compile(rf"</{name}\s*>", re.I) for name in ("script", "style")}
_INLINE_TAG_RE = re.compile(r"</?(?:a|b|i|u|s|em|strong|span|font|strike|del|ins|sup|sub|small|big|mark|abbr)\b[^<>]*>", re.I)
_BLOCK_TAG_RE = re.compile(r"<\s{0,8}/?\s{0,8}(?:br|p|div|li|ul|ol|h\d|tr|table|blockquote|hr)\b[^<>]*>", re.I)
_TAG_RE = re.compile(r"<[^<>]+>")
_BBCODE_RE = re.compile(
    r"\[/?(?:list|\*|b|i|u|s|url|img|quote|color|size|font|center|left|right|spoiler|indent)(?:=[^\[\]]*)?\]",
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
    interval_seconds: float | None = None  # per-feed cadence; None = every poll

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


def _strip_scripts(fragment: str) -> str:
    """Drop ``<script>``/``<style>`` blocks; an unclosed one swallows the rest (as in browsers).

    Procedural rather than a lazy ``.*?`` regex, which rescans the remainder of the
    document from every unclosed opening tag (quadratic on ``"<script>" * N``).
    """
    out: list[str] = []
    pos = 0
    while True:
        opening = _SCRIPT_OPEN_RE.search(fragment, pos)
        if opening is None:
            out.append(fragment[pos:])
            break
        out.append(fragment[pos : opening.start()])
        closing = _SCRIPT_CLOSE_RES[opening.group(1).lower()].search(fragment, opening.end())
        if closing is None:
            break
        out.append(" ")
        pos = closing.end()
    return "".join(out)


def html_to_text(fragment: str) -> str:
    """Readable plain text from post HTML (block tags become newlines, BBCode removed)."""
    if not fragment:
        return ""
    text = _strip_scripts(fragment)
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
        value = _amount(m)
        if value is None:
            continue
        before = text[max(0, m.start() - 30) : m.start()]
        dash = _DASH_BEFORE_RE.search(before)
        if dash is not None and _AMOUNT_BEFORE_RE.search(before[: dash.start()]):
            continue  # "$1,099.99 -$100 w/ code": a discount, not the price
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
    """0.0 for free shipping, the amount for ``+ $X shipping``, None when unknown.

    ``"Free Shipping w/ Prime or on $35+"`` is free only for a price of at least $35;
    below the threshold the shipping cost is unknown (None), never the $35 itself.
    """
    title = clean_title(title)
    title_free = _free_shipping(title, price, fs_token=True)
    if title_free:
        return 0.0
    paid = _paid_shipping(title)
    if paid is not None:
        return paid
    if not body_text:
        return None
    if title_free is None and _free_shipping(body_text, price):
        return 0.0
    # A threshold the price misses (title or body): only an explicit cost is usable.
    return _paid_shipping(body_text)


def _free_shipping(text: str, price: float | None, *, fs_token: bool = False) -> bool | None:
    """True: ships free at ``price``. False: free only above a threshold the price misses
    (or the price is unknown). None: no free-shipping phrase at all."""
    matches = list(_FREE_SHIPPING_RE.finditer(text))
    if fs_token:
        matches.extend(_FS_TOKEN_RE.finditer(text))
    verdict: bool | None = None
    for m in matches:
        threshold = _SHIPPING_THRESHOLD_RE.match(text, m.end())
        if threshold is None:
            return True
        minimum = _amount(threshold)
        if minimum is not None and price is not None and price >= minimum:
            return True
        verdict = False
    return verdict


def _paid_shipping(text: str) -> float | None:
    for regex in _PAID_SHIPPING_RES:
        m = regex.search(text)
        if m is not None:
            value = _amount(m)
            if value is not None:
                return round(value, 2)
    return None


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


# Canonical names keyed like aliases ("hp" -> "HP", "costco" -> "Costco"): the only keys a
# multi-word slug may start with ("hp-small-medium-business"). Abbreviated aliases such as
# "bh" are deliberately absent ("bh-cosmetics" is not B&H Photo).
_CANONICAL_STORE_KEYS: dict[str, str] = {_store_key(v): v for v in _STORE_ALIASES.values()}


def _store_by_leading_name(name: str) -> str | None:
    """Known store whose own name starts a multi-word name/slug (longest prefix wins)."""
    tokens = [_store_key(t) for t in _STORE_TOKEN_SPLIT_RE.split(name)]
    tokens = [t for t in tokens if t]
    for size in range(len(tokens) - 1, 0, -1):
        hit = _CANONICAL_STORE_KEYS.get("".join(tokens[:size]))
        if hit:
            return hit
    return None


def normalize_store(name: str | None) -> str | None:
    """Canonical store name; unknown stores are cleaned up (slugs title-cased).

    ``"<seller> via Amazon"`` (a third-party marketplace seller) resolves to the
    marketplace, and live multi-word slugs to the store they start with
    (``hp-small-medium-business`` -> ``HP``, ``costco-wholesale`` -> ``Costco``).
    """
    if not name:
        return None
    cleaned = " ".join(html.unescape(name).split()).strip(" .,:;|-–—*")
    via = _VIA_SELLER_RE.split(cleaned)
    if len(via) > 1:
        cleaned = via[-1].strip(" .,:;|-–—*")
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
    known = _store_by_leading_name(cleaned)
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
        name = _body_store_has(body_html)
        if name:
            store = normalize_store(html_to_text(name))
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


def _body_store_has(body_html: str) -> str | None:
    """Anchor text of the editorial ``"<a>Store</a> has ..."`` lead-in (the link may be
    followed by a ``<span class="externallink">[<a>store.com</a>]</span>``)."""
    for m in _ANCHOR_TEXT_RE.finditer(body_html):
        name = m.group(1).strip()
        if not 2 <= len(name) <= MAX_STORE_NAME_CHARS:
            continue
        pos = m.end()
        span = _SPAN_OPEN_RE.match(body_html, pos)
        if span is not None:
            closing = _SPAN_CLOSE_RE.search(body_html, span.end(), span.end() + _SPAN_SCAN_CHARS)
            if closing is None:
                continue
            pos = closing.end()
        if _STORE_VERB_RE.match(body_html, pos):
            return name
    return None


def _store_of_url(url: str) -> str | None:
    """Known store behind a URL's host (``www.bestbuy.com``/``smile.amazon.com``), else None."""
    host = _host(url)
    if not host:
        return None
    return lookup_store(host) or lookup_store(".".join(host.split(".")[-2:]))


def _agrees_with_retailer(url: str, retailer: str | None) -> bool:
    if retailer is None:
        return True
    store = _store_of_url(url)
    return store is None or store == retailer


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


def extract_outbound_url(body_html: str, retailer: str | None = None) -> str | None:
    """Direct store link > store URL embedded in a /click redirect > Amazon ``/dp/<ASIN>``.

    With a known ``retailer`` the result must agree with it: links that belong to another
    known store are skipped, and the ASIN (always an Amazon product) is only used when the
    retailer is Amazon. Multi-store posts ("Best Buy & Amazon") would otherwise pair the
    primary store with the secondary store's product page.
    """
    asin: str | None = None
    exit_site: str | None = None
    for attrs in _anchors(body_html):
        href = attrs.get("href", "")
        if href.startswith("//"):
            href = "https:" + href
        if _is_http_url(href):
            if is_slickdeals_url(href):
                target = _embedded_target(href)
            else:
                target = href if _is_store_link(href) else None
            if target and _agrees_with_retailer(target, retailer):
                return target
        candidate = (attrs.get("data-aps-asin") or "").strip().upper()
        if asin is None and _ASIN_RE.match(candidate):
            asin = candidate
            exit_site = attrs.get("data-product-exitwebsite") or exit_site
    if asin and retailer in (None, "Amazon"):
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
        model = regex.search(title)
        if model:
            size = int(model.group("size"))
            if 10 <= size <= 120:
                return size
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
    retailer = extract_retailer(title, body_html, link)

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
        retailer=retailer,
        outbound_url=extract_outbound_url(body_html, retailer),
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
    result = ParsedFeed()
    for entry in parsed.get("entries") or []:
        try:
            result.listings.append(build_listing(entry, feed.name, query=feed.query, profile_hint=feed.profile_hint))
        except EntrySkipped as exc:
            result.skipped[exc.reason] += 1
        except Exception as exc:  # noqa: BLE001 - one malformed entry must not drop the feed
            result.errors += 1
            _log.debug("slickdeals entry failed to parse", extra={"feed": feed.name, "error": repr(exc)[:300]})
    # feedparser's loose fallback turns a truncated/garbage body into empty or title-less
    # entries; a not-well-formed document that produced nothing usable is a broken feed.
    # (Well-formed feeds with zero priced entries are fine.)
    if parsed.get("bozo") and not result.listings:
        result.malformed = True
        result.detail = repr(parsed.get("bozo_exception"))[:200]
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
    for attr in ("shipping", "list_price", "posted_at", "query", "profile_hint"):
        if getattr(primary, attr) is None and getattr(other, attr) is not None:
            setattr(primary, attr, getattr(other, attr))
    # Retailer and outbound URL travel together: never pair one store with another's link.
    if primary.retailer is None and other.retailer is not None:
        primary.retailer = other.retailer
        if primary.outbound_url is None or not _agrees_with_retailer(primary.outbound_url, primary.retailer):
            primary.outbound_url = other.outbound_url
    elif (
        primary.outbound_url is None
        and other.outbound_url is not None
        and other.retailer in (None, primary.retailer)
        and _agrees_with_retailer(other.outbound_url, primary.retailer)
    ):
        primary.outbound_url = other.outbound_url
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
        self.static_feeds: list[FeedTarget] = [
            FeedTarget(name=f.name, url=f.url, interval_seconds=f.interval_seconds) for f in cfg.feeds
        ]
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
        # Per-feed cadence: monotonic time at which each interval feed is next due.
        self.clock: Callable[[], float] = time.monotonic
        self._next_due: dict[FeedTarget, float] = {}
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
                "feed_intervals_s": {f.name: f.interval_seconds for f in self.static_feeds if f.interval_seconds},
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
        """Due configured feeds + the next round-robin slice of profile search feeds.

        Consumes the slots: an interval feed returned here is next due ``interval_seconds``
        from now. Feeds without an interval are due on every call.
        """
        now = self.clock()
        feeds: list[FeedTarget] = []
        for feed in self.static_feeds:
            if feed.interval_seconds is None:
                feeds.append(feed)
                continue
            due_at = self._next_due.get(feed)
            if due_at is None or now >= due_at - FEED_DUE_TOLERANCE_SECONDS:
                feeds.append(feed)
                self._next_due[feed] = now + feed.interval_seconds
        total = len(self.search_feeds)
        if total:
            per_poll = min(total, SEARCH_FEEDS_PER_POLL)
            start = self._search_cursor % total
            feeds.extend(self.search_feeds[(start + i) % total] for i in range(per_poll))
            self._search_cursor = (start + per_poll) % total
        return feeds

    def seconds_until_next_due(self) -> float | None:
        """Seconds until the earliest interval feed is due (0 if one is due now); None if
        no configured feed has an interval."""
        now = self.clock()
        waits = [
            max(0.0, self._next_due.get(feed, now) - now) for feed in self.static_feeds if feed.interval_seconds is not None
        ]
        return min(waits) if waits else None

    def next_interval(self) -> float:
        """The jittered poll interval, but never past the moment the next interval feed is due."""
        interval = super().next_interval()
        until_due = self.seconds_until_next_due()
        if until_due is None:
            return interval
        return max(MIN_POLL_WAIT_SECONDS, min(interval, until_due))

    async def poll(self) -> list[RawListing]:
        if not self.static_feeds and not self.search_feeds:
            if not self._warned_no_feeds:
                self._warned_no_feeds = True
                self.log.warning("slickdeals enabled without feeds or search terms", extra={"source": self.name})
            return []
        schedule, cursor = dict(self._next_due), self._search_cursor
        feeds = self.feeds_for_poll()
        if not feeds:
            # Only interval feeds are configured and none is due yet: a quiet success.
            self.log.debug("no slickdeals feed due", extra={"source": self.name, "next_due_s": self.seconds_until_next_due()})
            return []
        try:
            gathered = await asyncio.gather(*(self._poll_feed(feed) for feed in feeds), return_exceptions=True)
            results: list[FeedResult] = []
            for feed, item in zip(feeds, gathered):
                if isinstance(item, BaseException):
                    if not isinstance(item, Exception):  # CancelledError / KeyboardInterrupt
                        raise item
                    item = FeedResult(feed, FeedOutcome.ERROR, detail=repr(item)[:300])
                results.append(item)
        except BaseException:
            # Nothing was delivered: the consumed feed slots are due again on the next poll.
            self._next_due, self._search_cursor = schedule, cursor
            raise

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
                # Block statuses are "expected" and normally come back as responses; should one
                # surface here, the HTTP layer has already burned the identity for 403/429.
                if exc.status in BLOCK_STATUSES:
                    return FeedResult(feed, FeedOutcome.BLOCKED, detail=f"HTTP {exc.status}", retry_after=exc.retry_after)
                return FeedResult(feed, FeedOutcome.ERROR, detail=f"HTTP {exc.status}")
            except Exception as exc:  # noqa: BLE001 - network/size failures only skip this feed
                return FeedResult(feed, FeedOutcome.ERROR, detail=repr(exc)[:300])

        if resp.not_modified:
            return FeedResult(feed, FeedOutcome.NOT_MODIFIED)
        body = bytes(resp.data) if isinstance(resp.data, (bytes, bytearray)) else b""
        content_type = _header(resp.headers, "Content-Type")
        if resp.status in BLOCK_STATUSES or is_block_response(resp.headers, body):
            return self._blocked(
                feed,
                f"HTTP {resp.status} {'challenge/HTML page' if resp.status == 200 else 'block'}",
                parse_retry_after(_header(resp.headers, "Retry-After")),
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
            self.m_entries.inc(feed=feed.name, outcome="malformed_feed")
            if looks_like_html(body, content_type):
                return self._blocked(feed, "HTML instead of RSS")
            self._forget_validators(feed)
            return FeedResult(feed, FeedOutcome.ERROR, detail=f"malformed feed: {parsed.detail}")

        if parsed.listings:
            self.m_entries.inc(len(parsed.listings), feed=feed.name, outcome="parsed")
        for reason, count in parsed.skipped.items():
            self.m_entries.inc(count, feed=feed.name, outcome=f"skip_{reason}")
        if parsed.errors:
            self.m_entries.inc(parsed.errors, feed=feed.name, outcome="error")
            self.log.debug("slickdeals entries failed to parse", extra={"source": self.name, "feed": feed.name, "count": parsed.errors})
        return FeedResult(feed, FeedOutcome.OK, listings=parsed.listings)

    def _blocked(self, feed: FeedTarget, detail: str, retry_after: float | None = None) -> FeedResult:
        """A block wall: burn the host's browser identity (the request "expected" the block
        status, so the HTTP layer did not) and drop the feed's validators."""
        host = _host(feed.url)
        if host:
            self.ctx.http.identities.burn(host)
        self._forget_validators(feed)
        return FeedResult(feed, FeedOutcome.BLOCKED, detail=detail, retry_after=retry_after)

    def _forget_validators(self, feed: FeedTarget) -> None:
        # Never revalidate against a challenge/broken body: a later 304 would mean
        # "unchanged since the block page" and hide the real feed.
        self.ctx.http.conditional.store(feed.url, {})


__all__ = [
    "BLOCK_COOLDOWN_MAX_SECONDS",
    "BLOCK_STATUSES",
    "EntrySkipped",
    "FEED_ACCEPT",
    "FEED_DUE_TOLERANCE_SECONDS",
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
