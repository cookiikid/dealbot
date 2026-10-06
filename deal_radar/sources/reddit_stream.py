"""Reddit ingestor: r/buildapcsales-style deal feeds and r/hardwareswap-style swap posts.

Transport
---------
* **Application-only OAuth** (recommended) when ``client_id`` is configured:
  ``POST https://www.reddit.com/api/v1/access_token`` with HTTP Basic auth
  (``client_id:client_secret``) and ``grant_type=client_credentials`` (confidential
  "script"/"web" apps) — or ``grant_type=https://oauth.reddit.com/grants/installed_client``
  plus a stable 25-char ``device_id`` when only a client id is configured ("installed"
  apps have no secret). App-only tokens never come with a refresh token, so the token
  is cached and simply re-requested shortly before ``expires_in`` runs out; a 401 on
  a listing call invalidates it and the request is retried exactly once with a fresh
  token. Listings are read from ``https://oauth.reddit.com/r/{sub}/new`` with
  ``Authorization: bearer <token>``.
* **Unauthenticated fallback**: ``https://www.reddit.com/r/{sub}/new.json``. It has a
  much smaller budget and datacenter IPs regularly get an HTML "blocked by network
  security" page with HTTP 403 — that (and a 429) is raised as :class:`SourceBlocked`
  with a long cooldown instead of hammering the wall. A warning is logged once.
* ``raw_json=1`` opts out of Reddit's legacy HTML escaping of ``<``, ``>`` and ``&``
  in JSON bodies, so titles and preview image URLs arrive verbatim.
* The User-Agent follows Reddit's API rules (``<platform>:<app id>:<version> (by
  /u/<username>)``) and is *never* a browser identity — spoofing browsers is
  explicitly forbidden and gets clients blocked faster.

Rate limits
-----------
Every response carries ``X-Ratelimit-Used`` / ``X-Ratelimit-Remaining`` /
``X-Ratelimit-Reset`` (seconds until the window ends). Subreddits are polled
concurrently, so the most pessimistic ``remaining`` seen in the current window wins.
:meth:`RedditIngestor.next_interval` (a) sleeps until the reset when fewer requests
remain than one poll cycle needs (minimum 2), and otherwise (b) spreads the remaining
budget evenly over the rest of the window, never polling faster than configured.
Without headers in unauthenticated mode the interval is floored at the documented
public budget (:data:`UNAUTH_QPM`).

Freshness without the ``before`` cursor
---------------------------------------
``/new?before=<fullname>`` looks like the obvious incremental cursor, but it is
fragile: when the anchor post is deleted, removed by a moderator or filtered by
AutoModerator, Reddit returns an *empty* listing forever (nothing is "before" a
fullname that is no longer in the listing), silently stalling the feed. Instead every
poll fetches the newest ``limit`` posts (≤100, one request per subreddit) and relies on
:class:`~deal_radar.sources.base.ChangeDetector` (only new/changed listings are
emitted) plus ``max_item_age_minutes`` (restarts never replay old posts).

Parsing
-------
* **deals** mode (r/buildapcsales): ``"[GPU] Brand Model ... - $1199 ($1599 - $400)"``
  → ``extra["category_tag"]="GPU"``, price = first ``$`` amount after the last
  top-level ``" - "`` separator (amounts inside parentheses are ignored, so the
  ``($1599 - $400)`` breakdown never wins; rebate/coupon amounts such as ``"- $50
  MIR"`` are skipped), list price from a following parenthetical that shows the
  original price. ``outbound_url`` is the linked retailer page (or, for self posts, the
  first external link in the body), ``retailer`` is derived from its domain. Condition
  is left to the normalizer (it reads "Open Box"/"Refurb" from the title).
* **swap** mode (r/hardwareswap): ``"[USA-CA] [H] RTX 4090 FE, 32GB DDR5 [W] PayPal"``.
  Only *selling* posts are emitted (``[W]`` names a payment method, ``[H]`` does not,
  flair is not BUYING/CLOSED/TRADING) with ``source_kind=LOCAL``. Title = the ``[H]``
  part, price = first ``$`` amount in the body that is not struck through
  (``~~$500~~`` marks sold items/old prices), ``extra["multi_item"]`` when ``[H]``
  lists several items, seller trade count from the ``"Trades: N"`` user flair, images
  from direct i.imgur.com / i.redd.it links and inline media, timestamp album link in
  ``extra["timestamps_url"]``.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import html
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlsplit

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
TOKEN_REFRESH_MARGIN_SECONDS = 300.0  # refresh this long before expiry (capped at 10% of lifetime)
DEFAULT_TOKEN_LIFETIME_SECONDS = 3600.0  # used if the token response omits expires_in
BLOCK_COOLDOWN_SECONDS = 1800.0  # HTML block wall / unauthenticated 429: back off for a long time
OAUTH_429_MIN_COOLDOWN_SECONDS = 30.0

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
_SEPARATOR_RE = re.compile(r"\s[-–—]{1,2}(?:\s+|(?=\$))")
_DISCOUNT_AFTER_RE = re.compile(
    r"^\s*(?:off\b|mir\b|rebate|coupon|promo|discount|instant\b|gc\b|gift\s*card|cash\s*back|savings|credit|sc\b)",
    re.IGNORECASE,
)
_DISCOUNT_BEFORE_RE = re.compile(r"\b(?:save|saving|rebate|coupon|mir|off|promo)\b", re.IGNORECASE)
_LIST_CUE_RE = re.compile(
    r"\b(?:was|reg(?:ular(?:ly)?)?|msrp|list|orig(?:inal(?:ly)?)?|retail|normally|usually)\b", re.IGNORECASE
)
_SUBTRACTION_RE = re.compile(r"\$\s?[\d,.]+[kK]?\s*[-–−]\s*\$")
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
_SHIPPED_AFTER_RE = re.compile(r"^[\s,.)]*(?:\+\s*)?(?:shipped|ship(?:ping)?\s+incl|free\s+ship)", re.IGNORECASE)

_URL_RE = re.compile(r"https?://[^\s<>()\[\]\"'|`]+", re.IGNORECASE)
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


def find_money(text: str) -> list[MoneySpan]:
    """Every dollar amount in ``text`` with its character span, in order."""
    spans: list[MoneySpan] = []
    for m in _MONEY_RE.finditer(text):
        whole = m.group("a") if m.group("a") is not None else m.group("b")
        cents = m.group("ac") if m.group("a") is not None else m.group("bc")
        try:
            value = float(whole.replace(",", "") + (f".{cents}" if cents else ""))
        except ValueError:
            continue
        if m.group("ak"):
            value *= 1000.0
        spans.append(MoneySpan(round(value, 2), m.start(), m.end()))
    return spans


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


def _is_discount_amount(text: str, span: MoneySpan, sep_end: int) -> bool:
    if _DISCOUNT_AFTER_RE.match(text[span.end : span.end + 24]):
        return True
    return bool(_DISCOUNT_BEFORE_RE.search(text[sep_end : span.start]))


def parse_deal_title(title: str) -> DealTitle:
    """Parse r/buildapcsales' ``"[TAG] Product - $price (breakdown)"`` convention."""
    tag = _TAG_RE.match(title)
    category = (tag.group(1).strip() or None) if tag else None
    spans = find_money(title)
    if not spans:
        return DealTitle(category, None, None)
    depth = _depths(title)
    top = [s for s in spans if depth[s.start] == 0]
    separators = [m for m in _SEPARATOR_RE.finditer(title) if depth[m.start()] == 0]

    chosen: MoneySpan | None = None
    fallback: MoneySpan | None = None
    for sep in reversed(separators):
        candidate = next((s for s in top if s.start >= sep.end()), None)
        if candidate is None:
            continue
        if fallback is None:
            fallback = candidate
        if not _is_discount_amount(title, candidate, sep.end()):
            chosen = candidate
            break
    if chosen is None:
        chosen = fallback
    if chosen is None:
        chosen = next((s for s in top if not _DISCOUNT_AFTER_RE.match(title[s.end : s.end + 24])), None)
    if chosen is None:
        chosen = top[0] if top else spans[0]
    return DealTitle(category, chosen.value, _list_price_after(title, chosen), (chosen.start, chosen.end))


def _list_price_after(title: str, price: MoneySpan) -> float | None:
    """Original price from a parenthetical after the price: ``($1599 - $400)``, ``(reg $1599)``."""
    for group in _PAREN_GROUP_RE.finditer(title, price.end):
        inner = group.group(1)
        amounts = find_money(inner)
        if not amounts or amounts[0].value <= price.value:
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


def _clean_url(url: str) -> str:
    return html.unescape(url).rstrip(".,;:!?*_~")


def _first_external_link(text: str) -> str | None:
    for m in _URL_RE.finditer(text or ""):
        url = _clean_url(m.group(0))
        if _is_external(url):
            return url
    return None


def _dedupe(urls: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for url in urls:
        if url and url not in seen and url.startswith(("http://", "https://")):
            seen.add(url)
            out.append(url)
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
    if post.get("removed_by_category") or _str(post.get("selftext")).strip() in ("[removed]", "[deleted]"):
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
        "category_tag": parsed.category,
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
        description=selftext if is_self else "",
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
        have=m.group("have").strip(" \t,;-"),
        want=m.group("want").strip(" \t,;-"),
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


def location_allowed(location_tag: str, allowed: Sequence[str]) -> bool:
    """Prefix match of the title's location tag against ``hardwareswap_locations``."""
    if not allowed:
        return True
    tag = re.sub(r"\s+", "", location_tag).upper()
    return any(tag.startswith(re.sub(r"\s+", "", prefix).upper()) for prefix in allowed if prefix.strip())


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
            elif city is None and re.search(r"[A-Za-z]", rest):
                city = rest
    return Location(text=tag.strip(), country=country, region=region, city=city, postal_code=postal)


def _swap_price(selftext: str, have: str) -> tuple[float | None, float | None, list[float]]:
    """(price, shipping, all amounts) — struck-through ``~~$x~~`` amounts are ignored."""
    body = _STRIKE_RE.sub(" ", selftext or "")
    amounts = find_money(body)
    text = body
    if not amounts:
        amounts = find_money(have)
        text = have
    if not amounts:
        return None, None, []
    first = amounts[0]
    shipping = 0.0 if _SHIPPED_AFTER_RE.match(text[first.end : first.end + 30]) else None
    distinct: list[float] = []
    for span in amounts:
        if span.value not in distinct:
            distinct.append(span.value)
    return first.value, shipping, distinct


def _have_items(have: str) -> list[str]:
    return [chunk for chunk in _ITEM_SPLIT_RE.split(have) if len(re.sub(r"[^A-Za-z0-9]", "", chunk)) >= 2]


def _swap_images(post: Mapping[str, Any], selftext: str) -> list[str]:
    images = [_clean_url(m.group(0)) for m in _DIRECT_IMAGE_RE.finditer(selftext)]
    link = _clean_url(_str(post.get("url_overridden_by_dest")) or _str(post.get("url")))
    if link and _direct_image(link):
        images.insert(0, link)
    images += _media_metadata_images(post)
    images += _preview_images(post)
    return _dedupe(images)


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
    trades_match = _TRADES_RE.search(_str(post.get("author_flair_text")))
    extra: dict[str, Any] = {
        "subreddit": _str(post.get("subreddit")) or subreddit,
        "mode": "swap",
        "full_title": title,
        "location_tag": swap.location,
        "want": swap.want,
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
        client_id: str,
        client_secret: str | None,
        *,
        user_agent: str,
        device_id: str,
        token_url: str = TOKEN_URL,
        block_cooldown: float = BLOCK_COOLDOWN_SECONDS,
        metrics: Metrics | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._http = http
        self._client_id = client_id
        self._client_secret = client_secret
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
        return CLIENT_CREDENTIALS_GRANT if self._client_secret else INSTALLED_CLIENT_GRANT

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

    async def _refresh(self) -> str:
        secret = self._client_secret or ""  # installed apps authenticate with an empty password
        basic = base64.b64encode(f"{self._client_id}:{secret}".encode("utf-8")).decode("ascii")
        form = {"grant_type": self.grant_type}
        if not self._client_secret:
            form["device_id"] = self._device_id
        try:
            resp = await self._http.request(
                "POST",
                self._token_url,
                data=form,
                headers={"Authorization": f"Basic {basic}", "User-Agent": self._user_agent},
                accept="application/json",
                expected=(200,),
                parse="bytes",
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
        try:
            lifetime = float(payload.get("expires_in") or DEFAULT_TOKEN_LIFETIME_SECONDS)
        except (TypeError, ValueError):
            lifetime = DEFAULT_TOKEN_LIFETIME_SECONDS
        lifetime = max(1.0, lifetime)
        margin = min(TOKEN_REFRESH_MARGIN_SECONDS, lifetime * 0.1)
        self._token = token
        self._refresh_at = self._clock() + lifetime - margin
        self.refreshes += 1
        self._m_token.inc(outcome="ok")
        _log.info(
            "reddit oauth token acquired",
            extra={"source": "reddit", "grant": "client_credentials" if self._client_secret else "installed_client", "expires_in_s": lifetime},
        )
        return token


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
        self.block_cooldown = max(BLOCK_COOLDOWN_SECONDS, cfg.cooldown_seconds)
        self._unauth_warned = False
        m = ctx.metrics
        self.m_posts = m.counter("reddit_posts_total", "Reddit posts by parse outcome", ("subreddit", "outcome"))
        self.m_requests = m.counter("reddit_requests_total", "Reddit listing requests", ("subreddit", "status"))
        self.m_remaining = m.gauge("reddit_ratelimit_remaining", "X-Ratelimit-Remaining of the last Reddit response")
        self.oauth: RedditOAuth | None = None
        client_id = cfg.client_id.get_secret_value().strip() if cfg.client_id is not None else ""
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
            for _, exc in failures:
                if isinstance(exc, (SourceBlocked, SourceAuthError)):
                    raise exc
            if len(failures) == len(specs):
                raise failures[0][1]
            for sub, exc in failures:
                self.log.warning("subreddit poll failed", extra={"source": self.name, "subreddit": sub, "error": repr(exc)[:300]})
        return listings

    async def _poll_subreddit(self, spec: "SubredditSpec") -> list[RawListing]:
        payload = await self._fetch_listing(spec)
        if payload is None:
            return []
        outcome = parse_listing(
            payload,
            subreddit=spec.name,
            mode=spec.mode,
            skip_flairs=self.cfg.skip_flairs,
            locations=self.cfg.hardwareswap_locations if spec.mode == "swap" else (),
        )
        for listing in outcome.listings:
            listing.query = f"r/{spec.name}"
        if outcome.listings:
            self.m_posts.inc(len(outcome.listings), subreddit=spec.name, outcome="parsed")
        for reason, count in outcome.skipped.items():
            self.m_posts.inc(count, subreddit=spec.name, outcome=f"skip_{reason}")
        if outcome.errors:
            self.m_posts.inc(outcome.errors, subreddit=spec.name, outcome="malformed")
        return outcome.listings

    def listing_url(self, spec: "SubredditSpec") -> str:
        if self.oauth is not None:
            return f"{self.endpoints.oauth_base}/r/{spec.name}/new"
        return f"{self.endpoints.public_base}/r/{spec.name}/new.json"

    async def _fetch_listing(self, spec: "SubredditSpec") -> Any | None:
        """GET one listing; returns parsed JSON, or None when the subreddit is inaccessible."""
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
            )
            self._observe_rate_limit(resp)
            self.m_requests.inc(subreddit=spec.name, status=resp.status)
            body = resp.data if isinstance(resp.data, (bytes, bytearray)) else b""
            if resp.status == 200:
                if _looks_like_html(resp.headers, bytes(body)):
                    raise SourceBlocked(f"r/{spec.name}: Reddit served an HTML page instead of JSON", cooldown_seconds=self.block_cooldown)
                try:
                    return json_loads(bytes(body))
                except ValueError as exc:
                    raise SourceError(f"r/{spec.name}: malformed JSON listing") from exc
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
            return self._inaccessible(spec, resp, bytes(body))

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
            self.log.warning(
                "subreddit not accessible; skipping",
                extra={"source": self.name, "subreddit": spec.name, "status": resp.status, "reason": str(reason)},
            )
            self.m_posts.inc(subreddit=spec.name, outcome=f"inaccessible_{reason}")
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
            _float_header(resp.headers, "X-Ratelimit-Reset") or 0.0,
            parse_retry_after(_header(resp.headers, "Retry-After")) or 0.0,
        )
        if self.oauth is None:
            return max(self.block_cooldown, hint)
        return max(OAUTH_429_MIN_COOLDOWN_SECONDS, hint + RATE_LIMIT_RESET_MARGIN_SECONDS)

    def _observe_rate_limit(self, resp: HttpResponse) -> None:
        remaining = _float_header(resp.headers, "X-Ratelimit-Remaining")
        reset = _float_header(resp.headers, "X-Ratelimit-Reset")
        used = _float_header(resp.headers, "X-Ratelimit-Used")
        self.rate_limit.observe(remaining=remaining, used=used, reset_seconds=reset, now=self._clock())
        if remaining is not None:
            self.m_remaining.set(remaining)

    def _warn_unauthenticated_once(self) -> None:
        if self.oauth is not None or self._unauth_warned:
            return
        self._unauth_warned = True
        self.log.warning(
            "reddit OAuth credentials not configured: using the public JSON endpoints, which are heavily "
            "rate-limited and frequently 403-blocked from datacenter IPs; set REDDIT_CLIENT_ID/REDDIT_CLIENT_SECRET",
            extra={"source": self.name},
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
    "iter_posts",
    "location_allowed",
    "parse_deal_post",
    "parse_deal_title",
    "parse_listing",
    "parse_location_tag",
    "parse_swap_post",
    "parse_swap_title",
    "registrable_domain",
    "retailer_for_url",
    "swap_intent",
]
