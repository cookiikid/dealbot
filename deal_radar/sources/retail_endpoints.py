"""First-party retailer price/inventory endpoints (Best Buy, Shopify, Target, Newegg, any JSON API).

Pricing errors show up first at the retailer itself, so this is the lowest-latency
source DealRadar has. Every configured endpoint (``sources.retail.endpoints``) gets an
*adapter*. An adapter has an async ``fetch(http) -> list[RawListing]`` that does the
I/O, and it hands the response to a pure, unit-tested ``parse_*`` function.

Scheduling
----------
Each endpoint keeps its own schedule. ``endpoint.poll_interval_seconds`` overrides
``sources.retail.poll_interval_seconds``. A ``poll()`` cycle runs only the endpoints
that are due; the others are skipped this cycle. Each due time is jittered by
``jitter_pct`` when it is scheduled, so endpoints do not phase-lock with each other or
with upstream cache TTLs. :meth:`RetailIngestor.next_interval` returns the time until
the earliest endpoint is due, and never less than one second. Endpoints that come due
within ``DUE_SLACK_SECONDS`` of each other run in the same cycle. In-flight requests
across all endpoints are capped by ``max_concurrency``, using one semaphore.

Failure semantics
-----------------
* One failing endpoint never fails the poll. Its error is logged, counted and backed
  off on *that endpoint's* schedule, and the other endpoints' listings are returned.
* A poll raises only when every due endpoint failed and none returned listings. It
  raises ``SourceBlocked`` if all of them were bot-walled, ``SourceAuthError`` if all
  were credential failures, and ``SourceError`` otherwise.
* Blocks are HTTP 403, HTTP 429, Target's non-standard HTTP 435, an HTML challenge
  page where JSON was expected, and a PerimeterX ``{"appId", "blockScript"}`` JSON
  body. On Best Buy a 403 means a bad key or an exhausted quota (it never sends 429),
  so it is reported as a credential failure instead. Either one puts
  only that endpoint into an exponential cooldown, starting at ``cooldown_seconds``
  and capped at one hour, or honours ``Retry-After`` when the server sends it. The
  request is never retried at once, and the endpoint's remaining sub-requests in that
  cycle are skipped. A bot wall is never read as "out of stock".
* Error messages pass through :func:`redact` before they are raised or logged. Best
  Buy and Target keys travel as query parameters, and ``HttpStatusError`` embeds the
  full request URL.

Latency tactics
---------------
* **Persistent keep-alive connections.** Every request goes through the shared
  :class:`~deal_radar.core.http.HttpClient`, whose one tuned session keeps warm TLS
  connections per host. On a 15 s poll that saves 1-3 RTTs, which is more than any
  parsing optimisation can.
* **Conditional GET** (``endpoint.conditional``, on by default). ``ETag`` and
  ``Last-Modified`` validators are replayed as ``If-None-Match`` /
  ``If-Modified-Since``. A ``304`` means unchanged. The body is empty, nothing is
  parsed, and the listings parsed from the last ``200`` for that exact request are
  served again with a fresh ``received_at``. ``poll()`` therefore still returns
  everything currently visible, and the base change detector drops the unchanged
  ones. Shopify storefront JSON sends a weak ``W/"page_cache:..."`` ETag and answers
  a real 304 (verified live). Newegg sends no validator, and whether
  ``api.bestbuy.com`` does is unverified. Without a validator this costs nothing.
* **Opt-in cache-busting** (``endpoint.cache_bust`` with ``cache_bust_param``). It
  appends a unique nonce query parameter so a CDN whose cache key includes the full
  query string has to miss and fetch from origin. Be honest about its limits:

  - It does **nothing** on CDNs or application caches that ignore, whitelist or
    normalise unknown query parameters. Newegg's ProductRealtime is served from an
    internal cache with a ~60-65 s TTL that ignores random parameters and no-cache
    request headers (observed live).
  - Many retailer JSON responses are not edge-cached at all. Cloudflare does not cache
    JSON by default, and Shopify answers ``cf-cache-status: DYNAMIC`` with the same
    ETag with or without a nonce. On those it only adds a bot fingerprint.
  - Turning every poll into an origin hit raises origin load. Akamai Bot Manager,
    HUMAN and Cloudflare score exactly that, so it gets IPs rate-limited or blocked
    faster than it gets fresher data.

  That is why it is off by default and set per endpoint. Enable it only after
  measuring that the plain URL is a cache HIT (``Age`` > 0, ``x-cache: HIT``) while
  the nonce URL is a MISS *and* returns fresher data. Request-side
  ``Cache-Control: no-cache`` is never sent, because no tested or documented CDN
  honours it for freshness.
* **Batching.** Best Buy takes up to 100 SKUs per call with the ``sku in(...)``
  operator, and the docs recommend ``in()`` to avoid per-second limits. Target's
  ``product_summary_with_fulfillment_v1`` takes a comma-separated ``tcins`` list.
  A Shopify collection page carries up to 250 products.
* **Official APIs first.** The Best Buy Products API is supported and reachable from
  cloud IPs (5 calls/s, 50,000/day; quota exhaustion is a 403, not a 429), while
  bestbuy.com itself resets HTTP/2 streams from datacenter IPs (Akamai). Pace hosts
  with ``network.host_limits``. Polite defaults from the research: api.bestbuy.com
  4 rps, each Shopify store 0.5 rps, www.newegg.com 1 rps.

Adapters
--------
``bestbuy`` (official)
    ``GET https://api.bestbuy.com/v1/products(sku in(1,2,3))?apiKey=..&format=json
    &show=..&pageSize=100``, batched by ``batch_size`` (100 at most). The space in the
    path goes on the wire as ``%20``. ``price=salePrice`` and ``list_price=regularPrice``.
    ``in_stock`` is ``onlineAvailability`` (or ``orderable == "Available"``).
    ``condition`` (new / refurbished / pre-owned) is mapped to the normalizer's words.
    ``posted_at`` is the later of ``priceUpdateDate`` and
    ``onlineAvailabilityUpdateDate``, used only when the value carries a UTC offset.
    The raw strings always go in ``extra``, because the docs do not state a time zone.
    ToS: content may be cached for at most 72 h, and alerts should attribute Best Buy.
``shopify`` (unauthenticated storefront JSON)
    ``handles`` -> ``GET {store}/products/{handle}.js`` (Ajax API; prices are integer
    **cents**, ``variants[].available`` is always present).
    ``collection`` -> ``GET {store}/collections/{c}/products.json?limit=250&page=N``,
    with N up to ``max_pages``. Prices are decimal **strings**, and
    ``variants[].available`` may be missing on some themes, which gives
    ``in_stock=None``. With neither set, the store-wide ``/products.json`` is used.
    ``/products/{handle}.json`` is never used, because its variants have no
    ``available`` field (verified live). Each variant becomes one listing, with
    ``compare_at_price <= 0`` treated as absent. Shopify throttles unsigned bots with
    a 429 Cloudflare managed challenge (HTML, ``cf-mitigated: challenge``, no
    ``Retry-After``). That puts the endpoint into a block cooldown. Store currency is
    not exposed by these endpoints, so ``DEFAULT_CURRENCY`` is assumed.
``target_redsky`` (undocumented; **blocked from cloud IPs**)
    ``GET https://redsky.target.com/redsky_aggregations/v1/web/product_summary_with_fulfillment_v1
    ?key=..&tcins=a,b&store_id=..&zip=..&channel=WEB``. ``tcins`` is plural; the
    singular ``tcin`` is rejected. Prices come from ``price.current_retail`` /
    ``reg_retail``, and stock from the shipping and store-option
    ``availability_status`` fields. From a GCP IP every call returns HTTP 435 with a
    HUMAN/PerimeterX JSON wall. That is detected as a block, never as out of stock.
    The public web key rotates, and aggregations get retired (404/405/410). Run this
    adapter only from a residential collector node, and only where Target's terms
    allow it.
``newegg`` (undocumented; best-effort)
    ``GET https://www.newegg.com/product/api/ProductRealtime?ItemNumber=14-137-866``,
    one request per item. It reads ``MainItem.FinalPrice``, ``Instock``, ``Stock``,
    ``Description.Title``, ``Feature.IsOpenBoxed`` / ``IsRefurbished``,
    ``Seller.SellerName`` (null means sold by Newegg) and the
    ``Image.ImagePathPattern`` template. Field names were verified against a live
    response in October 2026, but the API is internal and can change without notice.
    Its data refreshes about every 60-65 s, so without an explicit
    ``poll_interval_seconds`` this endpoint defaults to ``NEWEGG_MIN_INTERVAL_SECONDS``.
``json`` (generic)
    ``url``/``method``/``params``/``body``/``headers`` describe the request.
    ``items_path`` and the ``fields`` map are dotted paths with list indices
    (``images[0].url``). ``url_template`` fills ``{id}`` when the item has no URL.
    ``price_divisor`` handles APIs that return cents, and ``condition`` sets the
    default condition.

Every listing has ``source="retail"`` and ``source_id=f"{endpoint.name}:{id}"`` (for
Shopify ``id`` is ``f"{store_host}:{variant_id}"``). ``retailer`` and ``profile_hint``
come from the endpoint, and ``extra`` always carries ``{"endpoint", "adapter"}``. The
change signature is ``(price, in_stock)``, so price moves *and* stock flips both
re-emit.
"""

from __future__ import annotations

import abc
import asyncio
import random
import re
import time
from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from typing import Any, ClassVar
from urllib.parse import quote, urljoin, urlsplit

from deal_radar.config_schema import (
    BestBuyEndpoint,
    GenericJsonEndpoint,
    NeweggEndpoint,
    RetailSource,
    ShopifyEndpoint,
    TargetEndpoint,
)
from deal_radar.core.backoff import RetryExhausted, parse_retry_after
from deal_radar.core.http import HttpClient, HttpStatusError, ResponseTooLarge, json_loads
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import Condition, RawListing, SellerInfo, SourceKind, utcnow
from deal_radar.sources.base import BaseIngestor, IngestorContext, SourceAuthError, SourceBlocked, SourceError

AnyEndpoint = BestBuyEndpoint | ShopifyEndpoint | TargetEndpoint | NeweggEndpoint | GenericJsonEndpoint
SkipFn = Callable[[str, str], None]  # (reason, item id or "") -> None

SOURCE_NAME = "retail"
DEFAULT_CURRENCY = "USD"  # the retail endpoint schema has no currency field (see module docstring)

BESTBUY_API_BASE = "https://api.bestbuy.com/v1"
BESTBUY_PAGE_SIZE = 100
BESTBUY_SHOW_FIELDS: tuple[str, ...] = (
    "sku",
    "name",
    "salePrice",
    "regularPrice",
    "onSale",
    "onlineAvailability",
    "onlineAvailabilityUpdateDate",
    "orderable",
    "inStoreAvailability",
    "url",
    "addToCartUrl",
    "image",
    "condition",
    "priceRestriction",
    "priceUpdateDate",
    "itemUpdateDate",
)

SHOPIFY_PAGE_LIMIT = 250

REDSKY_BASE = "https://redsky.target.com/redsky_aggregations/v1/web"
REDSKY_SUMMARY_PATH = "/product_summary_with_fulfillment_v1"
REDSKY_BATCH_SIZE = 24  # undocumented maximum; small batches keep one bad TCIN from hiding many
REDSKY_DIGITAL_STORE_ID = "3991"  # rejected by RedSky ("cannot be digital store")
REDSKY_AVAILABLE = frozenset({"IN_STOCK", "LIMITED_STOCK", "PRE_ORDER_SELLABLE"})

NEWEGG_REALTIME_URL = "https://www.newegg.com/product/api/ProductRealtime"
NEWEGG_MIN_INTERVAL_SECONDS = 60.0  # observed internal cache TTL (~60-65 s); faster polling buys nothing
NEWEGG_IMAGE_PATTERN = "https://c1.neweggimages.com/ProductImageOriginal/{ImageName}"
NEWEGG_IMAGE_SIZE = 1280

MIN_INTERVAL_SECONDS = 1.0
DUE_SLACK_SECONDS = 0.5
BLOCK_COOLDOWN_CAP_SECONDS = 3600.0
BLOCK_STATUSES: tuple[int, ...] = (403, 429, 435)
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
THREAD_PARSE_BYTES = 512 * 1024  # decode bigger bodies off the event loop
MAX_IMAGES = 6

_JSON_ACCEPT = "application/json"


# =========================================================================== errors


class PayloadError(ValueError):
    """The upstream payload is unusable as a whole (wrong shape, API-level error)."""


class RetailEndpointError(SourceError):
    """One endpoint failed. The message is already redacted.

    ``blocked`` marks bot walls and rate limits. ``auth`` marks rejected keys or
    exhausted quotas. ``partial`` holds listings the endpoint produced before it
    failed, which are still worth emitting.
    """

    def __init__(
        self,
        endpoint: str,
        message: str,
        *,
        status: int | None = None,
        blocked: bool = False,
        auth: bool = False,
        retry_after: float | None = None,
        partial: Sequence[RawListing] = (),
    ) -> None:
        super().__init__(f"{endpoint}: {message}")
        self.endpoint = endpoint
        self.detail = message
        self.status = status
        self.blocked = blocked
        self.auth = auth
        self.retry_after = retry_after
        self.partial: list[RawListing] = list(partial)

    def with_partial(self, partial: Sequence[RawListing]) -> RetailEndpointError:
        clone = RetailEndpointError(
            self.endpoint,
            self.detail,
            status=self.status,
            blocked=self.blocked,
            auth=self.auth,
            retry_after=self.retry_after,
            partial=partial,
        )
        return clone


# =========================================================================== redaction

_SECRET_QUERY_RE = re.compile(
    r"(?i)((?:[?&]|%3F|%26)(?:api_?key|key|access_token|token|client_secret|secret)(?:=|%3D))[^&#\s'\"<>]*"
)


def redact(text: str, secrets: Iterable[str | None] = ()) -> str:
    """Remove credential values from ``text`` (URLs, error bodies, reprs).

    Known secret values are replaced wherever they appear, raw or percent-encoded.
    Any ``apiKey=`` / ``key=`` / ``token=`` query value is masked as well, which
    covers keys the caller did not know about.
    """
    out = text
    for secret in secrets:
        if not secret or len(secret) < 4:
            continue
        out = out.replace(secret, "***")
        encoded = quote(secret, safe="")
        if encoded != secret:
            out = out.replace(encoded, "***")
    return _SECRET_QUERY_RE.sub(r"\1***", out)


# =========================================================================== value helpers


def to_float(value: Any, divisor: float = 1.0) -> float | None:
    """Plain numeric value (number or numeric string such as ``"25.00"``) -> float.

    Returns None for booleans, blanks, negatives, NaN and strings that are not plain
    numbers. Currency text like ``"$1,299.99 OBO"`` is left to the normalizer.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = Decimal(str(value))
    elif isinstance(value, str):
        cleaned = value.strip().replace(",", "").lstrip("$").strip()
        if not cleaned:
            return None
        try:
            number = Decimal(cleaned)
        except InvalidOperation:
            return None
    else:
        return None
    if not number.is_finite() or number < 0:
        return None
    if divisor != 1:
        number = number / Decimal(str(divisor))
    return float(round(number, 2))


def price_value(value: Any, divisor: float = 1.0) -> float | str | None:
    """Price for ``RawListing``: a float when numeric, else the raw string for the normalizer."""
    number = to_float(value, divisor)
    if number is not None:
        return number
    if isinstance(value, str) and value.strip() and divisor == 1:
        return value.strip()
    return None


_TRUE_WORDS = frozenset({"true", "yes", "y", "1", "in_stock", "instock", "in stock", "available", "add_to_cart", "limited_stock"})
_FALSE_WORDS = frozenset(
    {"false", "no", "n", "0", "out_of_stock", "outofstock", "out of stock", "sold out", "soldout", "sold_out",
     "unavailable", "not available", "discontinued", "notorderable", "not_orderable"}
)


def to_bool(value: Any) -> bool | None:
    """Lenient stock flag: bools, numbers (> 0) and common availability words."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    if isinstance(value, str):
        word = value.strip().lower()
        if word in _TRUE_WORDS:
            return True
        if word in _FALSE_WORDS:
            return False
    return None


def parse_timestamp(value: Any) -> datetime | None:
    """ISO-8601 timestamp -> aware datetime. Naive values return None (unknown zone)."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def absolute_url(value: Any, base: str | None = None) -> str | None:
    """http(s) URL from absolute, protocol-relative (``//cdn``) or relative values."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.startswith("//"):
        text = "https:" + text
    elif base and not urlsplit(text).scheme:
        text = urljoin(base, text)
    if urlsplit(text).scheme not in ("http", "https"):
        return None
    return text


def _dedupe(urls: Iterable[str | None], limit: int = MAX_IMAGES) -> list[str]:
    out: list[str] = []
    for url in urls:
        if url and url not in out:
            out.append(url)
            if len(out) >= limit:
                break
    return out


def _mapping(value: Any) -> Mapping[str, Any]:
    """``value`` when it is a JSON object, else an empty mapping (shape drift tolerance)."""
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return ""


def _item_id(value: Any) -> str:
    """Stable string id from an int/str id field ("" when unusable)."""
    if isinstance(value, bool) or value is None:
        return ""
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else ""
    if isinstance(value, (int, str)):
        return str(value).strip()
    return ""


_CONDITION_WORDS: dict[Condition, str | None] = {
    Condition.NEW: "New",
    Condition.OPEN_BOX: "Open Box",
    Condition.REFURBISHED: "Refurbished",
    Condition.USED: "Used",
    Condition.FOR_PARTS: "For parts",
    Condition.UNKNOWN: None,
}


def condition_word(raw: Any) -> str | None:
    """Map retailer condition values to words the normalizer understands."""
    text = _text(raw).lower()
    if not text:
        return None
    if "open" in text and "box" in text:
        return "Open Box"
    if "refurb" in text or "renewed" in text or "certified" in text:
        return "Refurbished"
    if "pre-owned" in text or "preowned" in text or "used" in text:
        return "Used"
    if "parts" in text:
        return "For parts"
    if text == "new" or text.startswith("new"):
        return "New"
    return _text(raw)


# =========================================================================== JSON paths

_PATH_SEGMENT_RE = re.compile(r"([^.\[\]]+)?((?:\[-?\d+\])*)")
_PATH_INDEX_RE = re.compile(r"\[(-?\d+)\]")


@lru_cache(maxsize=512)
def compile_path(path: str) -> tuple[str | int, ...]:
    """``"data.items[0].url"`` -> ``("data", "items", 0, "url")``. Raises ValueError if malformed."""
    path = path.strip()
    if not path:
        return ()
    steps: list[str | int] = []
    for segment in path.split("."):
        match = _PATH_SEGMENT_RE.fullmatch(segment)
        if match is None or not segment:
            raise ValueError(f"malformed JSON path {path!r} (segment {segment!r})")
        name, indices = match.group(1), match.group(2)
        if name is None and not indices:
            raise ValueError(f"malformed JSON path {path!r}")
        if name is not None:
            steps.append(name)
        steps.extend(int(i) for i in _PATH_INDEX_RE.findall(indices))
    return tuple(steps)


def resolve_path(obj: Any, path: str | Sequence[str | int]) -> Any:
    """Follow a dotted path (``a.b[0].c``) through dicts and lists; None when missing."""
    steps = compile_path(path) if isinstance(path, str) else path
    current = obj
    for step in steps:
        if current is None:
            return None
        if isinstance(step, int):
            if isinstance(current, list) and -len(current) <= step < len(current):
                current = current[step]
            else:
                return None
        elif isinstance(current, Mapping):
            current = current.get(step)
        elif isinstance(current, list) and step.lstrip("-").isdigit():
            index = int(step)
            current = current[index] if -len(current) <= index < len(current) else None
        else:
            return None
    return current


# =========================================================================== listing builder


def build_listing(
    endpoint: AnyEndpoint,
    *,
    item_id: str,
    url: str,
    title: str,
    price: float | str | None,
    list_price: float | str | None = None,
    in_stock: bool | None = None,
    condition: str | None = None,
    image_urls: Sequence[str] = (),
    sku: str | None = None,
    shipping: float | None = None,
    quantity: int | None = None,
    seller: SellerInfo | None = None,
    posted_at: datetime | None = None,
    currency: str = DEFAULT_CURRENCY,
    extra: Mapping[str, Any] | None = None,
) -> RawListing:
    """Assemble a RawListing with the fields every retail adapter shares."""
    meta: dict[str, Any] = {"endpoint": endpoint.name, "adapter": endpoint.adapter}
    if extra:
        meta.update({k: v for k, v in extra.items() if v is not None})
    return RawListing(
        source=SOURCE_NAME,
        source_kind=SourceKind.RETAIL,
        source_id=f"{endpoint.name}:{item_id}",
        url=url,
        title=title,
        price=price,
        currency=currency,
        shipping=shipping,
        list_price=list_price,
        condition=condition,
        seller=seller,
        image_urls=list(image_urls),
        posted_at=posted_at,
        in_stock=in_stock,
        quantity=quantity,
        retailer=endpoint.retailer,
        sku=sku,
        profile_hint=endpoint.profile_hint,
        extra=meta,
    )


def _noop_skip(reason: str, item: str) -> None:
    return None


# =========================================================================== Best Buy


def bestbuy_products_url(skus: Sequence[str], base: str | None = None) -> str:
    """``{base}/products(sku in(1,2,3))``; the space is percent-encoded by yarl on the wire."""
    return f"{base or BESTBUY_API_BASE}/products(sku in({','.join(skus)}))"


def bestbuy_params(api_key: str) -> dict[str, str]:
    return {
        "apiKey": api_key,
        "format": "json",
        "show": ",".join(BESTBUY_SHOW_FIELDS),
        "pageSize": str(BESTBUY_PAGE_SIZE),
    }


def parse_bestbuy_products(payload: Any, endpoint: BestBuyEndpoint, on_skip: SkipFn = _noop_skip) -> list[RawListing]:
    """Parse a Products API collection response (``{"products": [...], "total": ...}``)."""
    if not isinstance(payload, Mapping):
        raise PayloadError(f"expected a JSON object, got {type(payload).__name__}")
    products = payload.get("products")
    if products is None:
        message = payload.get("errorMessage") or payload.get("error") or "response has no 'products'"
        raise PayloadError(f"Best Buy API: {str(message)[:200]}")
    if not isinstance(products, list):
        raise PayloadError("'products' is not a list")
    out: list[RawListing] = []
    for product in products:
        if not isinstance(product, Mapping):
            on_skip("not_an_object", "")
            continue
        sku = _item_id(product.get("sku"))
        title = _text(product.get("name"))
        if not sku or not title:
            on_skip("missing_id_or_title", sku)
            continue
        price = to_float(product.get("salePrice"))
        if price is None:
            price = to_float(product.get("regularPrice"))
        if price is None:
            on_skip("no_price", sku)
            continue
        online = product.get("onlineAvailability")
        orderable = _text(product.get("orderable"))
        if online is True or orderable.lower() == "available":
            in_stock: bool | None = True
        elif online is False or orderable:
            in_stock = False
        else:
            in_stock = None
        price_updated = product.get("priceUpdateDate")
        stock_updated = product.get("onlineAvailabilityUpdateDate")
        stamps = [ts for ts in (parse_timestamp(price_updated), parse_timestamp(stock_updated)) if ts is not None]
        url = absolute_url(product.get("url")) or f"https://www.bestbuy.com/site/{sku}.p?skuId={sku}"
        out.append(
            build_listing(
                endpoint,
                item_id=sku,
                url=url,
                title=title,
                price=price,
                list_price=to_float(product.get("regularPrice")),
                in_stock=in_stock,
                condition=condition_word(product.get("condition")) or "New",
                image_urls=_dedupe([absolute_url(product.get("image"))]),
                sku=sku,
                posted_at=max(stamps) if stamps else None,
                extra={
                    "on_sale": product.get("onSale") if isinstance(product.get("onSale"), bool) else None,
                    "orderable": orderable or None,
                    "in_store_availability": product.get("inStoreAvailability")
                    if isinstance(product.get("inStoreAvailability"), bool)
                    else None,
                    "price_restriction": _text(product.get("priceRestriction")) or None,
                    "price_update_date": _text(price_updated) or None,
                    "availability_update_date": _text(stock_updated) or None,
                    "item_update_date": _text(product.get("itemUpdateDate")) or None,
                    "add_to_cart_url": absolute_url(product.get("addToCartUrl")),
                },
            )
        )
    return out


# =========================================================================== Shopify


def normalize_store_url(store_url: str) -> str:
    """``example.com/`` or ``https://example.com/shop`` -> ``https://example.com``."""
    text = store_url.strip()
    if "://" not in text:
        text = "https://" + text
    parts = urlsplit(text)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError(f"invalid Shopify store_url {store_url!r}")
    return f"{parts.scheme}://{parts.netloc}"


def _shopify_image(value: Any, base: str) -> str | None:
    if isinstance(value, Mapping):
        value = value.get("src") or value.get("url")
    return absolute_url(value, base)


def _shopify_variants(
    product: Mapping[str, Any],
    endpoint: ShopifyEndpoint,
    store_base: str,
    *,
    cents: bool,
    on_skip: SkipFn,
) -> list[RawListing]:
    product_id = _item_id(product.get("id"))
    product_title = _text(product.get("title"))
    handle = _text(product.get("handle"))
    variants = product.get("variants")
    if not product_title or not isinstance(variants, list):
        on_skip("bad_product", product_id)
        return []
    if handle:
        product_url = f"{store_base}/products/{quote(handle, safe='')}"
    else:
        product_url = absolute_url(product.get("url"), store_base) or store_base
    raw_images = product.get("images")
    product_images = [_shopify_image(img, store_base) for img in raw_images] if isinstance(raw_images, list) else []
    featured = _shopify_image(product.get("featured_image"), store_base)
    store_host = urlsplit(store_base).hostname or store_base
    divisor = 100.0 if cents else 1.0
    out: list[RawListing] = []
    for variant in variants:
        if not isinstance(variant, Mapping):
            on_skip("not_an_object", product_id)
            continue
        variant_id = _item_id(variant.get("id"))
        if not variant_id:
            on_skip("missing_variant_id", product_id)
            continue
        price = to_float(variant.get("price"), divisor)
        if price is None:
            on_skip("no_price", variant_id)
            continue
        compare_at = to_float(variant.get("compare_at_price"), divisor)
        if compare_at is not None and compare_at <= 0:
            compare_at = None
        variant_title = _text(variant.get("title"))
        title = product_title
        if variant_title and variant_title.lower() != "default title":
            title = f"{product_title} - {variant_title}"
        available = variant.get("available")
        quantity = variant.get("inventory_quantity")
        out.append(
            build_listing(
                endpoint,
                item_id=f"{store_host}:{variant_id}",
                url=f"{product_url}?variant={variant_id}",
                title=title,
                price=price,
                list_price=compare_at,
                in_stock=available if isinstance(available, bool) else None,
                image_urls=_dedupe([_shopify_image(variant.get("featured_image"), store_base), featured, *product_images]),
                sku=_text(variant.get("sku")) or None,
                quantity=quantity if isinstance(quantity, int) and not isinstance(quantity, bool) and quantity >= 0 else None,
                posted_at=parse_timestamp(variant.get("updated_at")),
                extra={
                    "store": store_host,
                    "product_id": product_id or None,
                    "variant_id": variant_id,
                    "handle": handle or None,
                    "variant_title": variant_title or None,
                    "vendor": _text(product.get("vendor")) or None,
                    "product_type": _text(product.get("product_type") or product.get("type")) or None,
                },
            )
        )
    return out


def parse_shopify_product_js(
    payload: Any, endpoint: ShopifyEndpoint, store_base: str, on_skip: SkipFn = _noop_skip
) -> list[RawListing]:
    """Parse ``/products/{handle}.js`` (Ajax API: prices in integer cents)."""
    if not isinstance(payload, Mapping):
        raise PayloadError(f"expected a product object, got {type(payload).__name__}")
    return _shopify_variants(payload, endpoint, store_base, cents=True, on_skip=on_skip)


def parse_shopify_products_json(
    payload: Any, endpoint: ShopifyEndpoint, store_base: str, on_skip: SkipFn = _noop_skip
) -> tuple[list[RawListing], int, str | None]:
    """Parse a ``products.json`` page (prices are decimal strings).

    Returns ``(listings, product_count, first_product_id)``. The count drives
    pagination, and the first id guards against a store serving the same page again.
    """
    if not isinstance(payload, Mapping) or not isinstance(payload.get("products"), list):
        raise PayloadError("expected {'products': [...]} from products.json")
    products = payload["products"]
    out: list[RawListing] = []
    for product in products:
        if not isinstance(product, Mapping):
            on_skip("not_an_object", "")
            continue
        out.extend(_shopify_variants(product, endpoint, store_base, cents=False, on_skip=on_skip))
    first_id = _item_id(products[0].get("id")) if products and isinstance(products[0], Mapping) else None
    return out, len(products), first_id or None


# =========================================================================== Target RedSky


def is_bot_wall_json(payload: Any) -> bool:
    """HUMAN/PerimeterX block bodies look like data: ``{appId, blockScript, jsClientSrc, ...}``."""
    if not isinstance(payload, Mapping):
        return False
    if "blockScript" in payload:
        return True
    return "appId" in payload and ("jsClientSrc" in payload or "hostUrl" in payload)


def redsky_params(endpoint: TargetEndpoint, tcins: Sequence[str], api_key: str, visitor_id: str | None = None) -> dict[str, str]:
    params: dict[str, str] = {"key": api_key, "tcins": ",".join(tcins), "channel": "WEB"}
    if endpoint.store_id:
        params.update(
            {
                "store_id": endpoint.store_id,
                "required_store_id": endpoint.store_id,
                "has_required_store_id": "true",
                "scheduled_delivery_store_id": endpoint.store_id,
            }
        )
    if endpoint.zip_code:
        params["zip"] = endpoint.zip_code
    if visitor_id:
        params["visitor_id"] = visitor_id
    return params


def redsky_in_stock(fulfillment: Any) -> bool | None:
    """Shipping or store availability (pickup / in-store-only counts) -> bool, None if unknown."""
    if not isinstance(fulfillment, Mapping):
        return None
    statuses: list[str] = []
    ship = resolve_path(fulfillment, "shipping_options.availability_status")
    if isinstance(ship, str):
        statuses.append(ship.upper())
    store_options = fulfillment.get("store_options")
    if isinstance(store_options, list):
        for option in store_options:
            if not isinstance(option, Mapping):
                continue
            for kind in ("order_pickup", "in_store_only", "ship_to_store"):
                status = resolve_path(option, f"{kind}.availability_status")
                if isinstance(status, str):
                    statuses.append(status.upper())
    if any(status in REDSKY_AVAILABLE for status in statuses):
        return True
    if statuses:
        return False
    return None


def parse_redsky_summaries(payload: Any, endpoint: TargetEndpoint, on_skip: SkipFn = _noop_skip) -> list[RawListing]:
    """Parse ``product_summary_with_fulfillment_v1`` (``data.product_summaries[]``).

    ``pdp_client_v1``'s ``data.product`` is accepted as well, as a one-item list.
    """
    if not isinstance(payload, Mapping):
        raise PayloadError(f"expected a JSON object, got {type(payload).__name__}")
    data = payload.get("data")
    summaries: Any = None
    if isinstance(data, Mapping):
        summaries = data.get("product_summaries")
        if summaries is None and isinstance(data.get("product"), Mapping):
            summaries = [data["product"]]
    if not isinstance(summaries, list):
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            first = errors[0]
            message = first.get("message") if isinstance(first, Mapping) else first
            raise PayloadError(f"RedSky error: {str(message)[:200]}")
        raise PayloadError("RedSky response has no data.product_summaries")
    out: list[RawListing] = []
    for summary in summaries:
        if not isinstance(summary, Mapping):
            on_skip("not_an_object", "")
            continue
        tcin = _item_id(summary.get("tcin"))
        item = _mapping(summary.get("item"))
        title = _text(resolve_path(item, "product_description.title"))
        if not tcin or not title:
            on_skip("missing_id_or_title", tcin)
            continue
        price_block = _mapping(summary.get("price"))
        price: float | str | None = to_float(price_block.get("current_retail"))
        if price is None:
            price = to_float(price_block.get("current_retail_min"))
        if price is None:
            price = price_value(price_block.get("formatted_current_price"))
        if price is None:
            on_skip("no_price", tcin)
            continue
        regular = to_float(price_block.get("reg_retail"))
        if regular is None:
            regular = to_float(price_block.get("reg_retail_max"))
        images_block = resolve_path(item, "enrichment.images")
        images: list[str | None] = []
        if isinstance(images_block, Mapping):
            images.append(absolute_url(images_block.get("primary_image_url")))
            alternates = images_block.get("alternate_image_urls")
            if isinstance(alternates, list):
                images.extend(absolute_url(u) for u in alternates)
        fulfillment = summary.get("fulfillment")
        out.append(
            build_listing(
                endpoint,
                item_id=tcin,
                url=absolute_url(resolve_path(item, "enrichment.buy_url")) or f"https://www.target.com/p/-/A-{tcin}",
                title=title,
                price=price,
                list_price=regular,
                in_stock=redsky_in_stock(fulfillment),
                condition="New",
                image_urls=_dedupe(images),
                sku=tcin,
                extra={
                    "tcin": tcin,
                    "price_type": _text(price_block.get("formatted_current_price_type")) or None,
                    "is_marketplace": resolve_path(item, "fulfillment.is_marketplace")
                    if isinstance(resolve_path(item, "fulfillment.is_marketplace"), bool)
                    else None,
                    "store_id": endpoint.store_id,
                },
            )
        )
    return out


# =========================================================================== Newegg

_NEWEGG_ITEM_RE = re.compile(r"^(\d{2})-(\d{3})-(\d{3})$")


def newegg_product_url(item: str) -> str:
    """``14-137-866`` -> ``https://www.newegg.com/p/N82E16814137866`` (other ids as-is)."""
    match = _NEWEGG_ITEM_RE.match(item)
    code = "N82E168" + "".join(match.groups()) if match else item
    return f"https://www.newegg.com/p/{quote(code, safe='')}"


def _newegg_images(main: Mapping[str, Any]) -> list[str]:
    image = _mapping(main.get("Image"))
    pattern = NEWEGG_IMAGE_PATTERN
    patterns = image.get("ImagePathPattern")
    if isinstance(patterns, list):
        for entry in patterns:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("PathPattern"), str):
                continue
            if entry.get("Size") == NEWEGG_IMAGE_SIZE:
                pattern = entry["PathPattern"]
                break
    names: list[str] = []
    for source in (resolve_path(image, "Normal.ImageNameList"), resolve_path(main, "NewImage.ImageNameList")):
        if isinstance(source, str) and source.strip():
            names.extend(part.strip() for part in source.split(",") if part.strip())
            break
    primary = resolve_path(image, "Normal.ImageName") or image.get("ItemCellImageName")
    if isinstance(primary, str) and primary.strip():
        names.insert(0, primary.strip())
    urls = []
    for name in names:
        if name.startswith(("http://", "https://", "//")):
            urls.append(absolute_url(name))
        else:
            urls.append(absolute_url(pattern.replace("{Size}", str(NEWEGG_IMAGE_SIZE)).replace("{ImageName}", quote(name))))
    return _dedupe(urls)


def parse_newegg_realtime(payload: Any, endpoint: NeweggEndpoint, on_skip: SkipFn = _noop_skip) -> list[RawListing]:
    """Parse a ProductRealtime response (``{"MainItem": {...}, ...}``); best-effort."""
    if not isinstance(payload, Mapping):
        raise PayloadError(f"expected a JSON object, got {type(payload).__name__}")
    main = payload.get("MainItem")
    if not isinstance(main, Mapping):
        on_skip("no_main_item", "")
        return []
    item = _item_id(main.get("Item"))
    title = _text(resolve_path(main, "Description.Title")) or _text(resolve_path(main, "Description.WebDescription"))
    if not item or not title:
        on_skip("missing_id_or_title", item)
        return []
    price = to_float(main.get("FinalPrice")) or to_float(main.get("UnitCost"))
    if not price:  # 0 / missing: price hidden or item data incomplete
        on_skip("no_price", item)
        return []
    regular = max(
        (p for p in (to_float(main.get("OriginalUnitPrice")), to_float(main.get("UnitCost"))) if p is not None),
        default=None,
    )
    list_price = regular if regular is not None and regular > price else None
    instock = main.get("Instock")
    stock = main.get("Stock")
    stock_count = stock if isinstance(stock, int) and not isinstance(stock, bool) else None
    if isinstance(instock, bool):
        in_stock: bool | None = instock
    elif stock_count is not None:
        in_stock = stock_count > 0
    else:
        in_stock = None
    if payload.get("IsForceDeactiveItem") is True:
        in_stock = False
    feature = _mapping(main.get("Feature"))
    if feature.get("IsOpenBoxed") is True:
        condition = "Open Box"
    elif feature.get("IsRefurbished") is True:
        condition = "Refurbished"
    else:
        condition = "New"
    free_shipping = resolve_path(main, "ItemTagFlags.FreeShipping")
    shipping = 0.0 if free_shipping in (1, True, "1") else to_float(main.get("ShippingCharge"))
    seller_name = _text(resolve_path(main, "Seller.SellerName"))
    seller = SellerInfo(name=seller_name, is_business=True) if seller_name else None
    hide = main.get("PriceHideMark")
    map_price = to_float(main.get("MapPrice"))
    out = [
        build_listing(
            endpoint,
            item_id=item,
            url=newegg_product_url(item),
            title=title,
            price=price,
            list_price=list_price,
            in_stock=in_stock,
            condition=condition,
            image_urls=_newegg_images(main),
            sku=item,
            shipping=shipping,
            quantity=stock_count if stock_count is not None and stock_count >= 0 else None,
            seller=seller,
            extra={
                "marketplace_seller": seller_name or None,
                "price_in_cart": hide in ("1", 1, True, "true", "True"),
                "map_price": map_price if map_price else None,
                "lowest_price_30d": to_float(main.get("LowestPrice30Days")) or None,
                "instant_rebate": to_float(main.get("InstantRebateAmount")) or None,
                "limit_quantity": main.get("LimitQuantity") if isinstance(main.get("LimitQuantity"), int) else None,
                "ship_by_newegg": feature.get("ShipByNewegg") if isinstance(feature.get("ShipByNewegg"), bool) else None,
                "promotion": _text(resolve_path(main, "PromotionInfo.DisplayPromotionText")) or None,
                "subcategory": _text(resolve_path(main, "Subcategory.SubcategoryDescription")) or None,
            },
        )
    ]
    return out


# =========================================================================== generic JSON


@dataclass(frozen=True, slots=True)
class CompiledFieldMap:
    """FieldMap with every dotted path compiled once (malformed paths fail at startup)."""

    items: tuple[str | int, ...]
    id: tuple[str | int, ...]
    title: tuple[str | int, ...]
    price: tuple[str | int, ...]
    list_price: tuple[str | int, ...] | None
    in_stock: tuple[str | int, ...] | None
    url: tuple[str | int, ...] | None
    image: tuple[str | int, ...] | None
    condition: tuple[str | int, ...] | None

    @classmethod
    def from_endpoint(cls, endpoint: GenericJsonEndpoint) -> CompiledFieldMap:
        f = endpoint.fields

        def opt(path: str | None) -> tuple[str | int, ...] | None:
            return compile_path(path) if path else None

        return cls(
            items=compile_path(endpoint.items_path),
            id=compile_path(f.id),
            title=compile_path(f.title),
            price=compile_path(f.price),
            list_price=opt(f.list_price),
            in_stock=opt(f.in_stock),
            url=opt(f.url),
            image=opt(f.image),
            condition=opt(f.condition),
        )


def _generic_images(value: Any, base: str) -> list[str]:
    values = value if isinstance(value, list) else [value]
    urls: list[str | None] = []
    for entry in values:
        if isinstance(entry, Mapping):
            entry = entry.get("url") or entry.get("src") or entry.get("href")
        urls.append(absolute_url(entry, base))
    return _dedupe(urls)


def parse_generic_items(
    payload: Any,
    endpoint: GenericJsonEndpoint,
    fields: CompiledFieldMap | None = None,
    on_skip: SkipFn = _noop_skip,
) -> list[RawListing]:
    """Map an arbitrary JSON product list onto RawListings using the endpoint's FieldMap."""
    fm = fields or CompiledFieldMap.from_endpoint(endpoint)
    items = resolve_path(payload, fm.items)
    if isinstance(items, Mapping):
        items = [items]
    if not isinstance(items, list):
        raise PayloadError(f"items_path {endpoint.items_path!r} did not resolve to a list")
    default_condition = _CONDITION_WORDS.get(endpoint.condition)
    out: list[RawListing] = []
    for entry in items:
        if not isinstance(entry, Mapping):
            on_skip("not_an_object", "")
            continue
        item_id = _item_id(resolve_path(entry, fm.id))
        title = _text(resolve_path(entry, fm.title))
        if not item_id or not title:
            on_skip("missing_id_or_title", item_id)
            continue
        price = price_value(resolve_path(entry, fm.price), endpoint.price_divisor)
        if price is None:
            on_skip("no_price", item_id)
            continue
        list_price = price_value(resolve_path(entry, fm.list_price), endpoint.price_divisor) if fm.list_price else None
        url = absolute_url(resolve_path(entry, fm.url), endpoint.url) if fm.url else None
        if url is None and endpoint.url_template:
            url = absolute_url(endpoint.url_template.replace("{id}", quote(item_id, safe="")), endpoint.url)
        url_fallback = url is None
        if url is None:
            url = endpoint.url
        condition = condition_word(resolve_path(entry, fm.condition)) if fm.condition else None
        out.append(
            build_listing(
                endpoint,
                item_id=item_id,
                url=url,
                title=title,
                price=price,
                list_price=list_price,
                in_stock=to_bool(resolve_path(entry, fm.in_stock)) if fm.in_stock else None,
                condition=condition or default_condition,
                image_urls=_generic_images(resolve_path(entry, fm.image), endpoint.url) if fm.image else [],
                sku=item_id,
                extra={"url_fallback": True if url_fallback else None},
            )
        )
    return out


# =========================================================================== adapters


@dataclass(frozen=True, slots=True)
class RequestSpec:
    """One HTTP request an adapter issues. ``key`` identifies it for the 304 replay cache."""

    key: str
    url: str
    params: Mapping[str, str] | None = None
    method: str = "GET"
    json: Any = None


@dataclass(slots=True)
class _Reply:
    data: Any
    not_modified: bool
    status: int


class EndpointAdapter(abc.ABC):
    """Fetch + parse for one configured endpoint. Subclasses define ``requests``/``parse``."""

    adapter: ClassVar[str] = "base"
    accept: ClassVar[str] = _JSON_ACCEPT

    def __init__(
        self,
        endpoint: AnyEndpoint,
        *,
        semaphore: asyncio.Semaphore | None = None,
        metrics: Metrics | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.endpoint = endpoint
        self._sem = semaphore or asyncio.Semaphore(4)
        self._rng = rng or random.Random()
        self.log = get_logger(f"sources.{SOURCE_NAME}.{self.adapter}")
        m = metrics or Metrics()
        self._m_skipped = m.counter("retail_items_skipped_total", "Retail items skipped while parsing", ("endpoint", "reason"))
        self._m_not_modified = m.counter("retail_not_modified_total", "Retail requests answered 304 Not Modified", ("endpoint",))
        self._m_requests = m.counter("retail_requests_total", "Retail endpoint HTTP requests by outcome", ("endpoint", "outcome"))
        self._replay: dict[str, list[RawListing]] = {}

    # ------------------------------------------------------------------ config

    @property
    def name(self) -> str:
        return self.endpoint.name

    def secrets(self) -> list[str]:
        """Credential values that must never appear in logs or errors."""
        return []

    def has_work(self) -> bool:
        return True

    def interval(self, default: float) -> float:
        return float(self.endpoint.poll_interval_seconds or default)

    def redact(self, text: str) -> str:
        return redact(text, self.secrets())

    # ------------------------------------------------------------------ fetch

    @abc.abstractmethod
    def requests(self) -> list[RequestSpec]:
        """Requests for one cycle (batched)."""

    @abc.abstractmethod
    def parse(self, payload: Any, spec: RequestSpec) -> list[RawListing]:
        """Pure payload -> listings for one request."""

    async def fetch(self, http: HttpClient) -> list[RawListing]:
        """Run every request (bounded by the shared semaphore) and parse the results."""
        return await self._gather(http, self.requests(), self.parse)

    async def _gather(
        self,
        http: HttpClient,
        specs: Sequence[RequestSpec],
        parse: Callable[[Any, RequestSpec], list[RawListing]],
    ) -> list[RawListing]:
        """Issue ``specs`` concurrently; keep partial results; stop on the first block."""
        slots: list[list[RawListing]] = [[] for _ in specs]
        errors: list[RetailEndpointError] = []
        succeeded = 0
        halted = False

        async def one(index: int, spec: RequestSpec) -> None:
            nonlocal succeeded, halted
            async with self._sem:
                if halted:
                    return
                try:
                    reply = await self._request(http, spec)
                except RetailEndpointError as exc:
                    if exc.blocked or exc.auth:
                        halted = True  # do not keep hammering a wall
                    errors.append(exc)
                    return
            try:
                slots[index] = self._consume(reply, spec, parse)
            except PayloadError as exc:
                errors.append(RetailEndpointError(self.name, self.redact(f"unusable payload: {exc}"), status=reply.status))
                return
            except Exception as exc:
                self.log.exception("retail parser crashed", extra={"endpoint": self.name, "error": self.redact(repr(exc))})
                errors.append(RetailEndpointError(self.name, self.redact(f"parser error: {exc!r}"), status=reply.status))
                return
            succeeded += 1

        await asyncio.gather(*(one(i, spec) for i, spec in enumerate(specs)))
        listings = [listing for slot in slots for listing in slot]
        return self._settle(listings, errors, succeeded)

    def _consume(
        self, reply: _Reply, spec: RequestSpec, parse: Callable[[Any, RequestSpec], list[RawListing]]
    ) -> list[RawListing]:
        if reply.not_modified:
            self._m_not_modified.inc(endpoint=self.name)
            return self._replayed(spec.key)
        listings = parse(reply.data, spec)
        self._replay[spec.key] = listings
        return listings

    def _replayed(self, key: str) -> list[RawListing]:
        """Listings from the last 200 for ``key``, re-stamped as received now (304 path)."""
        cached = self._replay.get(key)
        if not cached:
            return []
        now = utcnow()
        return [raw.model_copy(update={"received_at": now, "node_id": None}) for raw in cached]

    def _settle(self, listings: list[RawListing], errors: list[RetailEndpointError], succeeded: int) -> list[RawListing]:
        if not errors:
            return listings
        walls = [e for e in errors if e.blocked or e.auth]
        if walls:
            raise walls[0].with_partial(listings)
        if succeeded == 0:
            raise errors[0].with_partial(listings)
        for exc in errors:
            self.log.warning("retail sub-request failed", extra={"endpoint": self.name, "error": str(exc)})
        return listings

    def _on_skip(self, reason: str, item: str) -> None:
        self._m_skipped.inc(endpoint=self.name, reason=reason)
        self.log.debug("retail item skipped", extra={"endpoint": self.name, "reason": reason, "item": item})

    async def _request(self, http: HttpClient, spec: RequestSpec) -> _Reply:
        ep = self.endpoint
        browser = bool(ep.browser_identity)
        try:
            resp = await http.request(
                spec.method,
                spec.url,
                params=spec.params or None,
                json=spec.json,
                headers=dict(ep.headers) or None,
                browser_identity=browser,
                fetch_mode="cors" if browser else "navigate",
                accept=self.accept,
                conditional=bool(ep.conditional) and spec.method == "GET",
                bust_cache=ep.cache_bust_param if ep.cache_bust else None,
                expected=(200, *BLOCK_STATUSES),
                parse="bytes",
                max_bytes=MAX_RESPONSE_BYTES,
            )
        except asyncio.CancelledError:
            raise
        except HttpStatusError as exc:
            self._m_requests.inc(endpoint=self.name, outcome=str(exc.status))
            raise self._status_error(http, spec, exc.status, exc.body, exc.headers) from None
        except RetryExhausted as exc:
            self._m_requests.inc(endpoint=self.name, outcome="retries_exhausted")
            raise RetailEndpointError(self.name, self.redact(f"retries exhausted: {exc.last_exc!r}")) from None
        except ResponseTooLarge as exc:
            self._m_requests.inc(endpoint=self.name, outcome="too_large")
            raise RetailEndpointError(self.name, self.redact(str(exc))) from None
        except Exception as exc:  # noqa: BLE001 - any transport error; the message may embed the URL
            self._m_requests.inc(endpoint=self.name, outcome=type(exc).__name__)
            raise RetailEndpointError(self.name, self.redact(f"request failed: {exc!r}")) from None
        if resp.not_modified:
            self._m_requests.inc(endpoint=self.name, outcome="304")
            return _Reply(None, True, 304)
        body: bytes = resp.data or b""
        if resp.status != 200:
            self._m_requests.inc(endpoint=self.name, outcome=str(resp.status))
            text = body[:2000].decode("utf-8", "replace")
            raise self._status_error(http, spec, resp.status, text, resp.headers)
        content_type = str(resp.headers.get("Content-Type", "")).lower()
        if "html" in content_type or body.lstrip()[:1] == b"<":
            self._m_requests.inc(endpoint=self.name, outcome="html_wall")
            self._burn(http, spec)
            raise RetailEndpointError(
                self.name, "HTML page instead of JSON (bot wall / challenge)", status=resp.status, blocked=True
            )
        try:
            data = await _decode_json(body)
        except ValueError as exc:
            self._m_requests.inc(endpoint=self.name, outcome="bad_json")
            raise RetailEndpointError(self.name, self.redact(f"invalid JSON: {exc}"), status=resp.status) from None
        if is_bot_wall_json(data):
            self._m_requests.inc(endpoint=self.name, outcome="json_wall")
            self._burn(http, spec)
            raise RetailEndpointError(self.name, "PerimeterX/HUMAN block JSON", status=resp.status, blocked=True)
        self._m_requests.inc(endpoint=self.name, outcome="200")
        return _Reply(data, False, resp.status)

    def _burn(self, http: HttpClient, spec: RequestSpec) -> None:
        if self.endpoint.browser_identity:
            http.identities.burn(urlsplit(spec.url).hostname or "")

    def _status_error(
        self, http: HttpClient, spec: RequestSpec, status: int, body: str, headers: Mapping[str, str]
    ) -> RetailEndpointError:
        retry_after = parse_retry_after(headers.get("Retry-After")) if headers else None
        snippet = self.redact(" ".join(body.split())[:200])
        if status in BLOCK_STATUSES:
            self._burn(http, spec)
            if retry_after is not None:
                bucket = http.limiter.for_host(urlsplit(spec.url).hostname or "")
                if bucket is not None:
                    bucket.penalize(retry_after)
            return self._block_error(status, snippet, headers, retry_after)
        if status in (404, 405, 410):
            return RetailEndpointError(self.name, f"HTTP {status}: not found or endpoint retired ({snippet})", status=status)
        return RetailEndpointError(self.name, f"HTTP {status}: {snippet}", status=status, retry_after=retry_after)

    def _block_error(
        self, status: int, snippet: str, headers: Mapping[str, str], retry_after: float | None
    ) -> RetailEndpointError:
        challenge = str(headers.get("cf-mitigated", "")).lower() == "challenge" if headers else False
        reason = {403: "forbidden", 429: "rate limited", 435: "PerimeterX/HUMAN bot wall"}.get(status, "blocked")
        if challenge:
            reason += " (Cloudflare managed challenge)"
        return RetailEndpointError(
            self.name, f"HTTP {status} {reason}: {snippet}", status=status, blocked=True, retry_after=retry_after
        )


async def _decode_json(body: bytes) -> Any:
    if not body.strip():
        raise ValueError("empty body")
    if len(body) > THREAD_PARSE_BYTES:
        return await asyncio.to_thread(json_loads, body)
    return json_loads(body)


def _chunks(values: Sequence[str], size: int) -> list[list[str]]:
    return [list(values[i : i + size]) for i in range(0, len(values), max(1, size))]


def _unique_ids(values: Iterable[str], pattern: re.Pattern[str], log: Any, endpoint: str, what: str) -> list[str]:
    out: list[str] = []
    for value in values:
        text = str(value).strip()
        if not pattern.fullmatch(text):
            log.warning("ignoring invalid id", extra={"endpoint": endpoint, "kind": what, "value": text[:40]})
            continue
        if text not in out:
            out.append(text)
    return out


_DIGITS_RE = re.compile(r"\d{1,12}")


class BestBuyAdapter(EndpointAdapter):
    adapter = "bestbuy"
    endpoint: BestBuyEndpoint

    def __init__(self, endpoint: BestBuyEndpoint, **kwargs: Any) -> None:
        super().__init__(endpoint, **kwargs)
        self.skus = _unique_ids(endpoint.skus, _DIGITS_RE, self.log, endpoint.name, "sku")

    def secrets(self) -> list[str]:
        key = self.endpoint.api_key
        return [key.get_secret_value()] if key is not None else []

    def has_work(self) -> bool:
        return bool(self.skus)

    def requests(self) -> list[RequestSpec]:
        key = self.endpoint.api_key
        if key is None:
            raise RetailEndpointError(self.name, "api_key is not configured (BESTBUY_API_KEY)", auth=True)
        params = bestbuy_params(key.get_secret_value())
        return [
            RequestSpec(key="skus:" + ",".join(batch), url=bestbuy_products_url(batch), params=params)
            for batch in _chunks(self.skus, min(self.endpoint.batch_size, BESTBUY_PAGE_SIZE))
        ]

    def parse(self, payload: Any, spec: RequestSpec) -> list[RawListing]:
        return parse_bestbuy_products(payload, self.endpoint, self._on_skip)

    def _block_error(
        self, status: int, snippet: str, headers: Mapping[str, str], retry_after: float | None
    ) -> RetailEndpointError:
        if status == 403:  # Best Buy: invalid key OR daily/per-second quota exhausted (no 429)
            return RetailEndpointError(
                self.name, f"HTTP 403 API key rejected or call quota exhausted: {snippet}", status=403, auth=True
            )
        return super()._block_error(status, snippet, headers, retry_after)


class ShopifyAdapter(EndpointAdapter):
    adapter = "shopify"
    accept = "application/json, text/javascript, */*; q=0.01"
    endpoint: ShopifyEndpoint

    def __init__(self, endpoint: ShopifyEndpoint, **kwargs: Any) -> None:
        super().__init__(endpoint, **kwargs)
        self.store_base = normalize_store_url(endpoint.store_url)
        self.handles = [h.strip().strip("/") for h in dict.fromkeys(endpoint.handles) if h and h.strip().strip("/")]
        self._page_meta: dict[str, tuple[int, str | None]] = {}

    def requests(self) -> list[RequestSpec]:
        return [
            RequestSpec(key=f"handle:{handle}", url=f"{self.store_base}/products/{quote(handle, safe='')}.js")
            for handle in self.handles
        ]

    def parse(self, payload: Any, spec: RequestSpec) -> list[RawListing]:
        return parse_shopify_product_js(payload, self.endpoint, self.store_base, self._on_skip)

    def listing_path(self) -> str | None:
        """products.json path to paginate, or None when only handles are polled."""
        if self.endpoint.collection:
            return f"/collections/{quote(self.endpoint.collection.strip(), safe='')}/products.json"
        if not self.handles:
            return "/products.json"
        return None

    async def fetch(self, http: HttpClient) -> list[RawListing]:
        jobs: list[Any] = []
        if self.handles:
            jobs.append(self._gather(http, self.requests(), self.parse))
        path = self.listing_path()
        if path is not None:
            jobs.append(self._paginate(http, path))
        results = await asyncio.gather(*jobs, return_exceptions=True)
        listings: list[RawListing] = []
        errors: list[RetailEndpointError] = []
        for result in results:
            if isinstance(result, RetailEndpointError):
                listings.extend(result.partial)
                errors.append(result)
            elif isinstance(result, BaseException):
                raise result
            else:
                listings.extend(result)
        if not errors:
            return listings
        walls = [e for e in errors if e.blocked or e.auth]
        if walls or not listings:
            raise (walls or errors)[0].with_partial(listings)
        for exc in errors:
            self.log.warning("shopify partial failure", extra={"endpoint": self.name, "error": str(exc)})
        return listings

    async def _paginate(self, http: HttpClient, path: str) -> list[RawListing]:
        """Walk products.json pages sequentially until a short/empty/repeated page."""
        listings: list[RawListing] = []
        seen_first: set[str] = set()
        for page in range(1, self.endpoint.max_pages + 1):
            spec = RequestSpec(
                key=f"{path}?page={page}",
                url=self.store_base + path,
                params={"limit": str(SHOPIFY_PAGE_LIMIT), "page": str(page)},
            )
            try:
                async with self._sem:
                    reply = await self._request(http, spec)
            except RetailEndpointError as exc:
                raise exc.with_partial(listings) from None
            if reply.not_modified:
                self._m_not_modified.inc(endpoint=self.name)
                meta = self._page_meta.get(spec.key)
                if meta is None:  # validator without a parsed page (earlier parse failed): stop here
                    break
                page_listings, count, first_id = self._replayed(spec.key), meta[0], meta[1]
            else:
                try:
                    page_listings, count, first_id = parse_shopify_products_json(
                        reply.data, self.endpoint, self.store_base, self._on_skip
                    )
                except PayloadError as exc:
                    raise RetailEndpointError(
                        self.name, self.redact(f"unusable payload: {exc}"), status=reply.status, partial=listings
                    ) from None
                self._replay[spec.key] = page_listings
                self._page_meta[spec.key] = (count, first_id)
            if first_id is not None and first_id in seen_first:
                break  # store ignores ?page= and served page 1 again
            if first_id is not None:
                seen_first.add(first_id)
            listings.extend(page_listings)
            if count < SHOPIFY_PAGE_LIMIT:
                break
        return listings


_TCIN_RE = re.compile(r"\d{5,12}")


class TargetAdapter(EndpointAdapter):
    adapter = "target_redsky"
    endpoint: TargetEndpoint

    def __init__(self, endpoint: TargetEndpoint, **kwargs: Any) -> None:
        super().__init__(endpoint, **kwargs)
        self.tcins = _unique_ids(endpoint.tcins, _TCIN_RE, self.log, endpoint.name, "tcin")
        # Browsers send a 32-hex visitor id cookie value; keep one stable id per process.
        self.visitor_id = f"{self._rng.getrandbits(128):032X}"
        if endpoint.store_id == REDSKY_DIGITAL_STORE_ID:
            self.log.warning(
                "store_id 3991 is Target's digital store and is rejected by RedSky; use a physical store id",
                extra={"endpoint": endpoint.name},
            )

    def secrets(self) -> list[str]:
        key = self.endpoint.api_key
        return [key.get_secret_value()] if key is not None else []

    def has_work(self) -> bool:
        return bool(self.tcins)

    def requests(self) -> list[RequestSpec]:
        key = self.endpoint.api_key
        if key is None:
            raise RetailEndpointError(self.name, "api_key (RedSky web key) is not configured", auth=True)
        url = REDSKY_BASE + REDSKY_SUMMARY_PATH
        return [
            RequestSpec(
                key="tcins:" + ",".join(batch),
                url=url,
                params=redsky_params(self.endpoint, batch, key.get_secret_value(), self.visitor_id),
            )
            for batch in _chunks(self.tcins, REDSKY_BATCH_SIZE)
        ]

    def parse(self, payload: Any, spec: RequestSpec) -> list[RawListing]:
        return parse_redsky_summaries(payload, self.endpoint, self._on_skip)

    def _status_error(
        self, http: HttpClient, spec: RequestSpec, status: int, body: str, headers: Mapping[str, str]
    ) -> RetailEndpointError:
        if status in (404, 405, 410):
            return RetailEndpointError(
                self.name,
                f"HTTP {status}: RedSky aggregation retired or key rotated; rediscover from a live target.com page",
                status=status,
            )
        return super()._status_error(http, spec, status, body, headers)


_NEWEGG_ID_RE = re.compile(r"[0-9A-Za-z][0-9A-Za-z-]{4,40}")


class NeweggAdapter(EndpointAdapter):
    adapter = "newegg"
    accept = "application/json, text/plain, */*"
    endpoint: NeweggEndpoint

    def __init__(self, endpoint: NeweggEndpoint, **kwargs: Any) -> None:
        super().__init__(endpoint, **kwargs)
        self.items = _unique_ids(endpoint.item_numbers, _NEWEGG_ID_RE, self.log, endpoint.name, "item_number")
        if endpoint.poll_interval_seconds is not None and endpoint.poll_interval_seconds < NEWEGG_MIN_INTERVAL_SECONDS:
            self.log.warning(
                "Newegg ProductRealtime refreshes about every 60 s; a faster interval only adds block risk",
                extra={"endpoint": endpoint.name, "interval_s": endpoint.poll_interval_seconds},
            )

    def has_work(self) -> bool:
        return bool(self.items)

    def interval(self, default: float) -> float:
        if self.endpoint.poll_interval_seconds is not None:
            return float(self.endpoint.poll_interval_seconds)
        return max(float(default), NEWEGG_MIN_INTERVAL_SECONDS)

    def requests(self) -> list[RequestSpec]:
        return [
            RequestSpec(key=f"item:{item}", url=NEWEGG_REALTIME_URL, params={"ItemNumber": item}) for item in self.items
        ]

    def parse(self, payload: Any, spec: RequestSpec) -> list[RawListing]:
        return parse_newegg_realtime(payload, self.endpoint, self._on_skip)


class GenericJsonAdapter(EndpointAdapter):
    adapter = "json"
    endpoint: GenericJsonEndpoint

    def __init__(self, endpoint: GenericJsonEndpoint, **kwargs: Any) -> None:
        super().__init__(endpoint, **kwargs)
        try:
            self.fields = CompiledFieldMap.from_endpoint(endpoint)
        except ValueError as exc:
            raise ValueError(f"retail endpoint {endpoint.name!r}: {exc}") from exc
        if urlsplit(endpoint.url).scheme not in ("http", "https"):
            raise ValueError(f"retail endpoint {endpoint.name!r}: url must be http(s)")

    def requests(self) -> list[RequestSpec]:
        ep = self.endpoint
        return [RequestSpec(key="request", url=ep.url, params=dict(ep.params) or None, method=ep.method, json=ep.body)]

    def parse(self, payload: Any, spec: RequestSpec) -> list[RawListing]:
        return parse_generic_items(payload, self.endpoint, self.fields, self._on_skip)


ADAPTERS: dict[str, type[EndpointAdapter]] = {
    "bestbuy": BestBuyAdapter,
    "shopify": ShopifyAdapter,
    "target_redsky": TargetAdapter,
    "newegg": NeweggAdapter,
    "json": GenericJsonAdapter,
}


def build_adapter(endpoint: AnyEndpoint, **kwargs: Any) -> EndpointAdapter:
    try:
        cls = ADAPTERS[endpoint.adapter]
    except KeyError as exc:
        raise ValueError(f"unknown retail adapter {endpoint.adapter!r}") from exc
    return cls(endpoint, **kwargs)  # type: ignore[arg-type]


# =========================================================================== ingestor


@dataclass(slots=True)
class EndpointState:
    """Per-endpoint schedule and health."""

    adapter: EndpointAdapter
    interval: float
    next_due: float = 0.0  # clock() value; 0 = due at the first poll
    consecutive_failures: int = 0
    consecutive_blocks: int = 0
    polls: int = 0
    failures: int = 0
    items: int = 0
    last_ms: float | None = None
    last_error: str | None = None
    last_success: float | None = None
    state: str = "idle"  # idle | ok | error | blocked

    @property
    def name(self) -> str:
        return self.adapter.name


@dataclass(slots=True)
class _Outcome:
    listings: list[RawListing] = field(default_factory=list)
    error: RetailEndpointError | None = None
    notice: tuple[str, str] | None = None  # operator notice for a fresh block / credential failure


class RetailIngestor(BaseIngestor):
    """Polls every configured retail endpoint on its own schedule (see module docstring)."""

    name = "retail"
    kind = SourceKind.RETAIL
    cfg: RetailSource

    def __init__(self, cfg: RetailSource, ctx: IngestorContext, *, clock: Callable[[], float] = time.monotonic) -> None:
        super().__init__(cfg, ctx)
        self._clock = clock
        self._sem = asyncio.Semaphore(cfg.max_concurrency)
        m = ctx.metrics
        self._m_endpoint = m.counter("retail_endpoint_polls_total", "Retail endpoint polls by outcome", ("endpoint", "outcome"))
        self._m_endpoint_ms = m.histogram("retail_endpoint_ms", "Retail endpoint fetch+parse time (ms)", ("endpoint",))
        self._m_endpoint_items = m.counter("retail_endpoint_items_total", "Listings returned per retail endpoint", ("endpoint",))
        self.endpoints: list[EndpointState] = []
        for endpoint in cfg.endpoints:
            adapter = build_adapter(endpoint, semaphore=self._sem, metrics=m, rng=ctx.rng)
            if not adapter.has_work():
                self.log.warning(
                    "retail endpoint has nothing to poll (no valid skus/handles/tcins/items); skipping",
                    extra={"endpoint": endpoint.name, "adapter": endpoint.adapter},
                )
                continue
            self.endpoints.append(EndpointState(adapter, adapter.interval(cfg.poll_interval_seconds)))

    # ------------------------------------------------------------------ BaseIngestor hooks

    def signature(self, raw: RawListing) -> Hashable:
        """Price moves and stock flips both count as changes."""
        return (str(raw.price), raw.in_stock)

    def next_interval(self) -> float:
        """Seconds until the earliest endpoint is due (each due time is already jittered)."""
        if not self.endpoints:
            return super().next_interval()
        earliest = min(state.next_due for state in self.endpoints)
        return max(MIN_INTERVAL_SECONDS, earliest - self._clock())

    async def poll(self) -> list[RawListing]:
        now = self._clock()
        due = [state for state in self.endpoints if state.next_due <= now + DUE_SLACK_SECONDS]
        if not due:
            return []
        outcomes = await asyncio.gather(*(self._run_endpoint(state) for state in due))
        listings = [listing for outcome in outcomes for listing in outcome.listings]
        errors = [outcome.error for outcome in outcomes if outcome.error is not None]
        failed_all = bool(errors) and len(errors) == len(due) and not listings
        if failed_all and all(e.blocked for e in errors):
            # The base loop pauses the whole source and sends its own "blocked" notice.
            summary = "; ".join(str(e) for e in errors)[:600]
            raise SourceBlocked(f"all {len(due)} due retail endpoint(s) blocked: {summary}", cooldown_seconds=self.next_interval())
        for outcome in outcomes:
            if outcome.notice is not None:
                await self._notify(*outcome.notice)
        if failed_all:
            summary = "; ".join(str(e) for e in errors)[:600]
            if all(e.auth for e in errors):
                raise SourceAuthError(f"all {len(due)} due retail endpoint(s) rejected credentials: {summary}")
            raise SourceError(f"all {len(due)} due retail endpoint(s) failed: {summary}")
        return listings

    # ------------------------------------------------------------------ per endpoint

    async def _run_endpoint(self, state: EndpointState) -> _Outcome:
        adapter = state.adapter
        started = self._clock()
        perf = time.perf_counter()
        error: RetailEndpointError | None = None
        try:
            listings = await adapter.fetch(self.ctx.http)
        except asyncio.CancelledError:
            raise
        except RetailEndpointError as exc:
            error = exc
            listings = exc.partial
        except Exception as exc:
            self.log.exception("retail endpoint crashed", extra={"endpoint": state.name, "error": adapter.redact(repr(exc))})
            error = RetailEndpointError(state.name, adapter.redact(f"unexpected error: {exc!r}"))
            listings = []
        elapsed_ms = (time.perf_counter() - perf) * 1000.0
        state.polls += 1
        state.last_ms = round(elapsed_ms, 2)
        state.items = len(listings)
        self._m_endpoint_ms.observe(elapsed_ms, endpoint=state.name)
        self._m_endpoint_items.inc(len(listings), endpoint=state.name)
        notice: tuple[str, str] | None = None
        if error is None:
            self._on_success(state, started)
        else:
            notice = self._on_failure(state, started, error)
        return _Outcome(list(listings), error, notice)

    def _jittered(self, interval: float) -> float:
        jitter = float(self.cfg.jitter_pct)
        return max(MIN_INTERVAL_SECONDS, interval * (1.0 + self.ctx.rng.uniform(-jitter, jitter)))

    def _on_success(self, state: EndpointState, started: float) -> None:
        state.consecutive_failures = 0
        state.consecutive_blocks = 0
        state.last_error = None
        state.last_success = started
        state.state = "ok"
        state.next_due = started + self._jittered(state.interval)
        self._m_endpoint.inc(endpoint=state.name, outcome="ok")

    def _on_failure(self, state: EndpointState, started: float, error: RetailEndpointError) -> tuple[str, str] | None:
        """Back the endpoint off; return an operator notice when it just got blocked."""
        notice: tuple[str, str] | None = None
        state.failures += 1
        state.last_error = str(error)[:500]
        base = self._jittered(state.interval)
        if error.blocked or error.auth:
            state.consecutive_blocks += 1
            state.state = "blocked"
            if error.retry_after is not None:
                delay = max(base, error.retry_after)
            else:
                cap = max(self.cfg.cooldown_seconds, BLOCK_COOLDOWN_CAP_SECONDS)
                delay = min(cap, self.cfg.cooldown_seconds * 2 ** (state.consecutive_blocks - 1))
                delay = max(base, delay)
            outcome = "auth" if error.auth else "blocked"
            self.log.warning(
                "retail endpoint blocked; cooling down",
                extra={"endpoint": state.name, "error": str(error), "cooldown_s": round(delay, 1), "partial": len(error.partial)},
            )
            if state.consecutive_blocks == 1:
                what = "rejected credentials" if error.auth else "blocked"
                notice = (f"retail endpoint {state.name} {what}", f"{error} - pausing it {delay:.0f}s")
        else:
            state.consecutive_failures += 1
            state.state = "error"
            cap = max(state.interval, self.cfg.cooldown_seconds)
            delay = max(base, min(cap, state.interval * 2 ** (state.consecutive_failures - 1)))
            if error.retry_after is not None:  # e.g. 503 + Retry-After after the in-request retries
                delay = max(delay, error.retry_after)
            outcome = "error"
            self.log.warning(
                "retail endpoint failed",
                extra={"endpoint": state.name, "error": str(error), "retry_in_s": round(delay, 1), "partial": len(error.partial)},
            )
        state.next_due = started + delay
        self._m_endpoint.inc(endpoint=state.name, outcome=outcome)
        return notice

    # ------------------------------------------------------------------ introspection

    def endpoint_status(self) -> list[dict[str, Any]]:
        """Per-endpoint health for /status pages and the CLI."""
        now = self._clock()
        return [
            {
                "endpoint": state.name,
                "adapter": state.adapter.adapter,
                "state": state.state,
                "interval_s": round(state.interval, 2),
                "due_in_s": round(max(0.0, state.next_due - now), 2),
                "polls": state.polls,
                "failures": state.failures,
                "consecutive_failures": state.consecutive_failures,
                "consecutive_blocks": state.consecutive_blocks,
                "last_items": state.items,
                "last_ms": state.last_ms,
                "last_success_age_s": round(now - state.last_success, 1) if state.last_success is not None else None,
                "last_error": state.last_error,
            }
            for state in self.endpoints
        ]


__all__ = [
    "ADAPTERS",
    "BESTBUY_API_BASE",
    "BESTBUY_SHOW_FIELDS",
    "NEWEGG_MIN_INTERVAL_SECONDS",
    "NEWEGG_REALTIME_URL",
    "REDSKY_BASE",
    "REDSKY_SUMMARY_PATH",
    "BestBuyAdapter",
    "CompiledFieldMap",
    "EndpointAdapter",
    "EndpointState",
    "GenericJsonAdapter",
    "NeweggAdapter",
    "PayloadError",
    "RequestSpec",
    "RetailEndpointError",
    "RetailIngestor",
    "ShopifyAdapter",
    "TargetAdapter",
    "absolute_url",
    "bestbuy_params",
    "bestbuy_products_url",
    "build_adapter",
    "build_listing",
    "compile_path",
    "condition_word",
    "is_bot_wall_json",
    "newegg_product_url",
    "normalize_store_url",
    "parse_bestbuy_products",
    "parse_generic_items",
    "parse_newegg_realtime",
    "parse_redsky_summaries",
    "parse_shopify_product_js",
    "parse_shopify_products_json",
    "parse_timestamp",
    "redact",
    "redsky_in_stock",
    "redsky_params",
    "resolve_path",
    "to_bool",
    "to_float",
]
