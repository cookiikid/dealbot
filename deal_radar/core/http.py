"""Shared outbound HTTP layer used by every ingestor, the vision client and dispatchers.

Design goals (see docs/ARCHITECTURE.md §3):

* **Connection reuse** – one tuned ``aiohttp`` session per process: keep-alive
  pools per host, cached DNS, happy-eyeballs; a warm TLS connection removes 1-3 RTTs
  from every poll, which matters far more than micro-optimising parsing.
* **Coherent browser identities** – a User-Agent alone is a weak disguise and a
  *rotating* UA from one IP is itself a bot signal. Each :class:`HeaderProfile`
  bundles a UA with the matching client hints (``sec-ch-ua*``), ``Accept`` and
  ``Accept-Language`` headers, and :class:`IdentityRotator` keeps one identity
  sticky per host, rotating only on a timer or after a block (403/429).
* **Conditional requests** – ``ETag``/``Last-Modified`` validators are remembered
  per URL so unchanged feeds cost a 304 with an empty body.
* **Opt-in cache busting** – a nonce query parameter for endpoints whose CDN keys on
  the full query string. It is off by default: it increases origin load and only
  helps on CDNs that do not normalise query strings.
* **Politeness + resilience** – per-host token buckets, retries with full-jitter
  backoff, and server ``Retry-After`` hints applied to the whole host bucket.
"""

from __future__ import annotations

import asyncio
import random
import re
import ssl
import time
from collections import OrderedDict
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import aiohttp
from multidict import CIMultiDict

from deal_radar.core.backoff import BackoffPolicy, RetryableError, RetryExhausted, parse_retry_after, retry_async
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.core.ratelimit import HostLimit, RateLimiterRegistry

try:  # orjson is ~5x faster than json for the large payloads eBay/Reddit return
    import orjson

    def _loads(data: bytes) -> Any:
        return orjson.loads(data)

    def _dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode("utf-8")

except ImportError:  # pragma: no cover - orjson is a declared dependency
    import json

    def _loads(data: bytes) -> Any:
        return json.loads(data)

    def _dumps(obj: Any) -> str:
        return json.dumps(obj, separators=(",", ":"))


try:
    from aiohttp.compression_utils import HAS_BROTLI, HAS_ZSTD
except ImportError:  # pragma: no cover - older aiohttp
    HAS_BROTLI = False
    HAS_ZSTD = False

log = get_logger("http")

# Only advertise encodings we can actually decode; advertising "br" without a brotli
# decoder installed turns every response into a decode error.
ACCEPT_ENCODING = ", ".join(["gzip", "deflate", *(["br"] if HAS_BROTLI else []), *(["zstd"] if HAS_ZSTD else [])])

TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    RetryableError,
    asyncio.TimeoutError,
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
)

RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524})


# --------------------------------------------------------------------------- identities


_GREASE_CHARS = (" ", "(", ":", "-", ".", "/", ")", ";", "=", "?", "_")
_GREASE_VERSIONS = ("8", "99", "24")
_GREASE_ORDERS = ((0, 1, 2), (0, 2, 1), (1, 0, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0))


def chromium_sec_ch_ua(major: int, brand: str = "Google Chrome") -> str:
    """Reproduce Chromium's GREASE'd ``sec-ch-ua`` brand list for a major version.

    Mirrors ``GenerateBrandVersionList`` in Chromium's user-agent utils so the header
    is byte-identical to what a real browser of that version sends (e.g. Chrome 131 →
    ``"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"``).
    """
    grease = f"Not{_GREASE_CHARS[major % 11]}A{_GREASE_CHARS[(major + 1) % 11]}Brand"
    entries = [(grease, _GREASE_VERSIONS[major % 3]), ("Chromium", str(major)), (brand, str(major))]
    order = _GREASE_ORDERS[major % 6]
    slots: list[tuple[str, str]] = [("", "")] * 3
    for entry, position in zip(entries, order):
        slots[position] = entry
    return ", ".join(f'"{name}";v="{ver}"' for name, ver in slots)


def estimate_chrome_major(today: date | None = None) -> int:
    """Estimate the current stable Chrome major from its 4-week release train.

    Anchored on Chrome 138 (2025-06-24) and lagging one version behind the newest
    stable, which matches the bulk of real-world traffic. Override with
    ``network.chrome_major_version`` when precision matters.
    """
    today = today or date.today()
    anchor = date(2025, 6, 24)
    elapsed = (today - anchor).days
    return max(120, 138 + elapsed // 28 - 1)


@dataclass(frozen=True, slots=True)
class HeaderProfile:
    name: str
    user_agent: str
    headers: Mapping[str, str]

    def build(
        self,
        *,
        accept: str | None = None,
        extra: Mapping[str, str] | None = None,
        fetch_mode: str = "navigate",
    ) -> dict[str, str]:
        """Headers for one request.

        ``fetch_mode="navigate"`` mirrors a top-level page load. ``"cors"`` mirrors a
        ``fetch()``/XHR issued by a page: real browsers then send ``Sec-Fetch-Dest: empty``
        and ``Sec-Fetch-Mode: cors`` and never the navigation-only ``Sec-Fetch-User`` /
        ``Upgrade-Insecure-Requests`` headers — sending those on a JSON API call is a
        cheap bot tell.
        """
        merged = {"User-Agent": self.user_agent, **self.headers, "Accept-Encoding": ACCEPT_ENCODING}
        if fetch_mode != "navigate":
            merged.pop("Sec-Fetch-User", None)
            merged.pop("Upgrade-Insecure-Requests", None)
            if "Sec-Fetch-Dest" in merged:  # browsers without Fetch Metadata (none here) keep nothing
                merged["Sec-Fetch-Dest"] = "empty"
                merged["Sec-Fetch-Mode"] = "cors" if fetch_mode == "cors" else fetch_mode
                merged["Sec-Fetch-Site"] = "same-origin"
        if accept is not None:
            merged["Accept"] = accept
        if extra:
            merged.update(extra)
        return merged


def build_header_profiles(chrome_major: int | None = None, accept_language: str = "en-US,en;q=0.9") -> tuple[HeaderProfile, ...]:
    """Coherent desktop browser identities for the given Chrome generation."""
    major = chrome_major or estimate_chrome_major()
    firefox_major = major + 2  # Firefox and Chrome share a 4-week cadence, offset by ~2
    today = date.today()
    safari_major = today.year - 2000 + (1 if today.month >= 9 else 0)  # Safari 26 shipped Sept 2025
    html_accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    chrome_common = {
        "Accept": html_accept,
        "Accept-Language": accept_language,
        "sec-ch-ua-mobile": "?0",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    return (
        HeaderProfile(
            "chrome-windows",
            f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36",
            {**chrome_common, "sec-ch-ua": chromium_sec_ch_ua(major), "sec-ch-ua-platform": '"Windows"'},
        ),
        HeaderProfile(
            "chrome-macos",
            f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36",
            {**chrome_common, "sec-ch-ua": chromium_sec_ch_ua(major), "sec-ch-ua-platform": '"macOS"'},
        ),
        HeaderProfile(
            "edge-windows",
            f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36 Edg/{major}.0.0.0",
            {**chrome_common, "sec-ch-ua": chromium_sec_ch_ua(major, "Microsoft Edge"), "sec-ch-ua-platform": '"Windows"'},
        ),
        HeaderProfile(
            "firefox-windows",
            f"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{firefox_major}.0) Gecko/20100101 Firefox/{firefox_major}.0",
            {
                "Accept": html_accept,
                "Accept-Language": "en-US,en;q=0.5",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            },
        ),
        HeaderProfile(
            "safari-macos",
            f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{safari_major}.0 Safari/605.1.15",
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            },
        ),
    )


class IdentityRotator:
    """Sticky-per-host identity selection with timed rotation and block-driven burn."""

    def __init__(
        self,
        profiles: tuple[HeaderProfile, ...],
        *,
        rotation_seconds: float = 45 * 60,
        rng: random.Random | None = None,
        clock: Any = time.monotonic,
    ) -> None:
        if not profiles:
            raise ValueError("at least one header profile is required")
        self.profiles = profiles
        self.rotation_seconds = rotation_seconds
        self._rng = rng or random.Random()
        self._clock = clock
        self._assigned: dict[str, tuple[HeaderProfile, float]] = {}

    def for_host(self, host: str) -> HeaderProfile:
        host = host.lower()
        current = self._assigned.get(host)
        now = self._clock()
        if current is not None and now - current[1] < self.rotation_seconds:
            return current[0]
        exclude = current[0] if current is not None and len(self.profiles) > 1 else None
        choices = [p for p in self.profiles if p is not exclude]
        profile = self._rng.choice(choices)
        self._assigned[host] = (profile, now)
        return profile

    def burn(self, host: str) -> None:
        """Forget the identity for ``host`` (call after a 403/429 block page)."""
        current = self._assigned.get(host.lower())
        if current is not None:
            # Expire it immediately; next call picks a *different* profile.
            self._assigned[host.lower()] = (current[0], self._clock() - self.rotation_seconds - 1)


# --------------------------------------------------------------------------- conditional cache


@dataclass(slots=True)
class _Validators:
    etag: str | None
    last_modified: str | None


class ConditionalCache:
    """LRU of HTTP validators so repeat polls can send If-None-Match/If-Modified-Since."""

    def __init__(self, max_entries: int = 4096) -> None:
        self.max_entries = max_entries
        self._entries: OrderedDict[str, _Validators] = OrderedDict()

    def headers_for(self, key: str) -> dict[str, str]:
        entry = self._entries.get(key)
        if entry is None:
            return {}
        self._entries.move_to_end(key)
        headers: dict[str, str] = {}
        if entry.etag:
            headers["If-None-Match"] = entry.etag
        if entry.last_modified:
            headers["If-Modified-Since"] = entry.last_modified
        return headers

    def store(self, key: str, headers: Mapping[str, str]) -> None:
        lowered = {k.lower(): v for k, v in headers.items()}
        etag = lowered.get("etag")
        last_modified = lowered.get("last-modified")
        if not etag and not last_modified:
            self._entries.pop(key, None)
            return
        self._entries[key] = _Validators(etag, last_modified)
        self._entries.move_to_end(key)
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)

    def __len__(self) -> int:
        return len(self._entries)


def cache_bust(url: str, param: str = "_", value: str | None = None) -> str:
    """Return ``url`` with a unique nonce query parameter (replacing any previous one)."""
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != param]
    query.append((param, value if value is not None else str(time.time_ns())))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


# --------------------------------------------------------------------------- client


_SECRET_PARAMS = frozenset(
    {"apikey", "api_key", "key", "token", "access_token", "client_secret", "secret", "sig", "signature", "password", "auth"}
)
_DISCORD_WEBHOOK = re.compile(r"(/api(?:/v\d+)?/webhooks/\d+/)[^/?#]+")
_TELEGRAM_BOT = re.compile(r"(/bot)\d+:[A-Za-z0-9_-]+")


def redact_url(url: str) -> str:
    """Strip credentials from a URL before it reaches an exception message or a log line.

    Masks secret-looking query parameters (``apiKey``, ``token``...), userinfo, Discord
    webhook tokens (``/webhooks/<id>/<token>``) and Telegram bot tokens (``/bot<id>:<tok>``).
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<unparseable url>"
    netloc = parts.netloc.rsplit("@", 1)[-1]
    path = _TELEGRAM_BOT.sub(r"\1<redacted>", _DISCORD_WEBHOOK.sub(r"\1<redacted>", parts.path))
    query = urlencode(
        [(k, "<redacted>" if k.lower() in _SECRET_PARAMS else v) for k, v in parse_qsl(parts.query, keep_blank_values=True)],
        safe="<>",
    )
    return urlunsplit((parts.scheme, netloc, path, query, ""))


_CHALLENGE_MARKERS = re.compile(
    r"just a moment|cf-chl|challenge-platform|attention required|h?captcha|px-captcha|perimeterx"
    r"|access denied|request blocked|you(?:'|&#39;)?ve been blocked|blocked by network security",
    re.IGNORECASE,
)


def is_challenge_page(status: int, headers: Mapping[str, str], body: str) -> bool:
    """True for bot-management walls (Cloudflare, Akamai, HUMAN/PerimeterX, DataDome...).

    A challenge is not transient: retrying it immediately only deepens the block, so the
    client raises :class:`HttpStatusError` at once and the source backs off instead.
    """
    if not (status in (403, 429, 503) or 430 <= status < 500):  # 435 = HUMAN/PerimeterX
        return False
    lowered = {k.lower(): v for k, v in headers.items()}
    if lowered.get("cf-mitigated", "").lower() == "challenge":
        return True
    content_type = lowered.get("content-type", "").lower()
    if "html" not in content_type and "json" not in content_type and content_type:
        return False
    return bool(_CHALLENGE_MARKERS.search(body[:20_000]))


class HttpStatusError(Exception):
    """Unexpected HTTP status (non-retryable, or retries exhausted on a retryable one).

    ``url`` is always credential-redacted; ``body`` (first 64 KB) and case-insensitive
    ``headers`` are preserved so callers can read e.g. Discord's JSON ``retry_after``
    or ``X-RateLimit-*`` headers. ``retry_after`` carries the parsed ``Retry-After``.
    """

    def __init__(
        self,
        status: int,
        url: str,
        body: str = "",
        headers: Mapping[str, str] | None = None,
        *,
        retry_after: float | None = None,
    ) -> None:
        safe_url = redact_url(url)
        super().__init__(f"HTTP {status} for {safe_url}: {body[:300]}")
        self.status = status
        self.url = safe_url
        self.body = body
        self.headers: Mapping[str, str] = CIMultiDict(headers or {})
        self.retry_after = retry_after


class RetryableHttpError(RetryableError):
    """A retryable status (429/5xx) that still carries the response for the final error."""

    def __init__(self, status: int, url: str, body: str, headers: Mapping[str, str], retry_after: float | None) -> None:
        super().__init__(f"HTTP {status} from {urlsplit(url).hostname or '?'}", retry_after=retry_after, status=status)
        self.url = url
        self.body = body
        self.headers = headers

    def to_status_error(self) -> HttpStatusError:
        return HttpStatusError(self.status or 0, self.url, self.body, self.headers, retry_after=self.retry_after)


@dataclass(slots=True)
class HttpResponse:
    status: int
    url: str
    headers: Mapping[str, str]  # case-insensitive (CIMultiDict)
    data: Any = None  # parsed JSON / text / bytes depending on ``parse``
    not_modified: bool = False
    elapsed_ms: float = 0.0
    attempts: int = 1
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class NetworkSettings:
    """Subset of the ``network`` config section needed to build the HTTP layer."""

    timeout_seconds: float = 10.0
    connect_timeout_seconds: float = 3.0
    max_connections: int = 200
    max_connections_per_host: int = 10
    dns_cache_ttl_seconds: int = 300
    keepalive_seconds: float = 60.0
    trust_env: bool = True
    proxy: str | None = None
    ca_bundle: str | None = None
    retry: BackoffPolicy = BackoffPolicy()
    default_host_limit: HostLimit | None = None
    host_limits: Mapping[str, HostLimit] = field(default_factory=dict)
    chrome_major_version: int | None = None
    accept_language: str = "en-US,en;q=0.9"
    identity_rotation_seconds: float = 45 * 60


def create_session(settings: NetworkSettings) -> aiohttp.ClientSession:
    """Build the process-wide tuned ``aiohttp.ClientSession``."""
    ssl_ctx: ssl.SSLContext | bool = True
    if settings.ca_bundle:
        ssl_ctx = ssl.create_default_context(cafile=settings.ca_bundle)
    connector = aiohttp.TCPConnector(
        limit=settings.max_connections,
        limit_per_host=settings.max_connections_per_host,
        ttl_dns_cache=settings.dns_cache_ttl_seconds,
        use_dns_cache=True,
        keepalive_timeout=settings.keepalive_seconds,
        ssl=ssl_ctx,
    )
    timeout = aiohttp.ClientTimeout(
        total=settings.timeout_seconds,
        connect=settings.connect_timeout_seconds,
        sock_connect=settings.connect_timeout_seconds,
        sock_read=settings.timeout_seconds,
    )
    return aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        trust_env=settings.trust_env,
        json_serialize=_dumps,
        raise_for_status=False,
        auto_decompress=True,
    )


class HttpClient:
    """High-level request helper shared by all components.

    ``request`` applies, in order: host rate limit → identity headers → conditional
    validators → optional cache-bust → send → status classification → parse. Transient
    failures are retried per the configured :class:`BackoffPolicy`.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        settings: NetworkSettings,
        *,
        metrics: Metrics | None = None,
        limiter: RateLimiterRegistry | None = None,
        identities: IdentityRotator | None = None,
        conditional: ConditionalCache | None = None,
    ) -> None:
        self.session = session
        self.settings = settings
        self.metrics = metrics or Metrics()
        self.limiter = limiter or RateLimiterRegistry(settings.default_host_limit, settings.host_limits)
        self.identities = identities or IdentityRotator(
            build_header_profiles(settings.chrome_major_version, settings.accept_language),
            rotation_seconds=settings.identity_rotation_seconds,
        )
        self.conditional = conditional or ConditionalCache()
        self._requests = self.metrics.counter("http_requests_total", "Outbound HTTP requests", ("host", "status"))
        self._latency = self.metrics.histogram("http_request_ms", "Outbound HTTP latency (ms)", ("host",))
        self._retries = self.metrics.counter("http_retries_total", "Outbound HTTP retries", ("host",))

    @classmethod
    def create(cls, settings: NetworkSettings, *, metrics: Metrics | None = None) -> "HttpClient":
        return cls(create_session(settings), settings, metrics=metrics)

    async def close(self) -> None:
        if not self.session.closed:
            await self.session.close()

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        json: Any = None,
        data: Any = None,
        browser_identity: bool = False,
        fetch_mode: str = "navigate",
        accept: str | None = "application/json",
        conditional: bool = False,
        bust_cache: str | None = None,
        expected: Collection[int] = (200,),
        parse: str = "json",
        retry: bool = True,
        policy: BackoffPolicy | None = None,
        timeout: float | None = None,
        rate_limit: bool = True,
        max_bytes: int | None = None,
        proxy: str | None = None,
    ) -> HttpResponse:
        """Perform a request and return a parsed :class:`HttpResponse`.

        Raises :class:`HttpStatusError` on unexpected/non-retryable statuses and
        :class:`deal_radar.core.backoff.RetryExhausted` when retries run out.
        """
        if parse not in ("json", "text", "bytes", "none"):
            raise ValueError(f"unknown parse mode {parse!r}")
        host = urlsplit(url).hostname or ""
        cond_key = url + ("?" + urlencode(sorted((str(k), str(v)) for k, v in params.items())) if params else "")
        attempts = 0

        async def _once() -> HttpResponse:
            nonlocal attempts
            attempts += 1
            bucket = self.limiter.for_host(host) if rate_limit else None
            if bucket is not None:
                await bucket.acquire()
            req_headers: dict[str, str] = {}
            if browser_identity:
                req_headers.update(self.identities.for_host(host).build(accept=accept, fetch_mode=fetch_mode))
            else:
                req_headers["Accept-Encoding"] = ACCEPT_ENCODING
                if accept is not None:
                    req_headers["Accept"] = accept
            if conditional:
                req_headers.update(self.conditional.headers_for(cond_key))
            if headers:
                req_headers.update(headers)
            target = cache_bust(url, bust_cache) if bust_cache else url
            extra_kwargs: dict[str, Any] = {}
            if timeout:
                # Only override when asked: passing timeout=None to aiohttp would *disable*
                # the session's default timeout instead of inheriting it.
                extra_kwargs["timeout"] = aiohttp.ClientTimeout(
                    total=timeout,
                    connect=min(timeout, self.settings.connect_timeout_seconds),
                    sock_read=timeout,
                )
            started = time.perf_counter()
            status = 0
            try:
                async with self.session.request(
                    method,
                    target,
                    params=params,
                    headers=req_headers,
                    json=json,
                    data=data,
                    proxy=proxy or self.settings.proxy,
                    **extra_kwargs,
                ) as resp:
                    status = resp.status
                    # Case-insensitive copy: servers send "Etag", "etag", "x-ratelimit-remaining"...
                    resp_headers: CIMultiDict[str] = CIMultiDict(resp.headers)
                    if status == 304 and conditional:
                        return HttpResponse(304, str(resp.url), resp_headers, None, True, _ms(started), attempts)
                    if status in expected:
                        body = await _read_limited(resp, max_bytes)
                        if conditional and 200 <= status < 300:
                            # Only real content updates validators: a 403/429/435 block page
                            # that a caller chose to "expect" must not wipe a good ETag.
                            self.conditional.store(cond_key, resp_headers)
                        return HttpResponse(status, str(resp.url), resp_headers, _parse(body, parse, resp), False, _ms(started), attempts)
                    text = (await _read_limited(resp, 64_000)).decode("utf-8", "replace")
                    retry_after = parse_retry_after(resp_headers.get("Retry-After"))
                    if status in (403, 429):
                        self.identities.burn(host)
                    if status in RETRYABLE_STATUSES and not is_challenge_page(status, resp_headers, text):
                        if retry_after is not None and bucket is not None:
                            bucket.penalize(retry_after)
                        raise RetryableHttpError(status, str(resp.url), text, resp_headers, retry_after)
                    raise HttpStatusError(status, str(resp.url), text, resp_headers, retry_after=retry_after)
            finally:
                elapsed = _ms(started)
                self._latency.observe(elapsed, host=host)
                self._requests.inc(host=host, status=status or "error")

        def _on_retry(attempt: int, exc: BaseException, delay: float) -> None:
            self._retries.inc(host=host)
            log.debug("retrying request", extra={"host": host, "attempt": attempt, "delay_s": round(delay, 3), "error": repr(exc)})

        if not retry:
            try:
                return await _once()
            except RetryableHttpError as exc:
                raise exc.to_status_error() from None
        try:
            return await retry_async(_once, policy=policy or self.settings.retry, retry_on=TRANSIENT_EXCEPTIONS, on_retry=_on_retry)
        except RetryExhausted as exc:
            last = exc.last_exc
            if isinstance(last, RetryableHttpError):
                raise last.to_status_error() from None
            raise

    async def get_json(self, url: str, **kwargs: Any) -> HttpResponse:
        return await self.request("GET", url, parse="json", **kwargs)

    async def get_text(self, url: str, **kwargs: Any) -> HttpResponse:
        kwargs.setdefault("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        return await self.request("GET", url, parse="text", **kwargs)

    async def get_bytes(self, url: str, **kwargs: Any) -> HttpResponse:
        kwargs.setdefault("accept", "*/*")
        return await self.request("GET", url, parse="bytes", **kwargs)

    async def post_json(self, url: str, payload: Any, **kwargs: Any) -> HttpResponse:
        return await self.request("POST", url, json=payload, parse=kwargs.pop("parse", "json"), **kwargs)


class ResponseTooLarge(Exception):
    pass


async def _read_limited(resp: aiohttp.ClientResponse, max_bytes: int | None) -> bytes:
    if max_bytes is None:
        return await resp.read()
    declared = resp.content_length
    if declared is not None and declared > max_bytes:
        raise ResponseTooLarge(f"response of {declared} bytes exceeds limit {max_bytes}")
    chunks: list[bytes] = []
    total = 0
    async for chunk in resp.content.iter_chunked(65536):
        total += len(chunk)
        if total > max_bytes:
            raise ResponseTooLarge(f"response exceeds limit {max_bytes}")
        chunks.append(chunk)
    return b"".join(chunks)


def _parse(body: bytes, mode: str, resp: aiohttp.ClientResponse) -> Any:
    if mode == "json":
        if not body.strip():
            return None
        return _loads(body)
    if mode == "text":
        return body.decode(resp.get_encoding() if resp.charset else "utf-8", "replace")
    if mode == "bytes":
        return body
    return None


def _ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 3)


def json_loads(data: bytes | str) -> Any:
    return _loads(data.encode("utf-8") if isinstance(data, str) else data)


def json_dumps(obj: Any) -> str:
    return _dumps(obj)


__all__ = [
    "ACCEPT_ENCODING",
    "ConditionalCache",
    "HeaderProfile",
    "HttpClient",
    "HttpResponse",
    "HttpStatusError",
    "IdentityRotator",
    "NetworkSettings",
    "RETRYABLE_STATUSES",
    "ResponseTooLarge",
    "RetryableHttpError",
    "TRANSIENT_EXCEPTIONS",
    "build_header_profiles",
    "cache_bust",
    "chromium_sec_ch_ua",
    "create_session",
    "estimate_chrome_major",
    "is_challenge_page",
    "json_dumps",
    "json_loads",
    "redact_url",
]
