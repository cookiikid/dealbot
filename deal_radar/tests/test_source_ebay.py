"""Tests for the eBay Browse API ingestor (sources/ebay_api.py).

All HTTP is offline: ``aioresponses`` patches ``aiohttp.ClientSession`` for most tests,
and one test runs an ``aiohttp`` TestServer to check the bytes that actually go on
the wire. The fixture payload under ``fixtures/ebay/`` mirrors a recorded
``item_summary/search`` response (string money values, thumbs vs full-size images,
AUCTION-only and auction+BIN items, calculated shipping without a quote, a malformed
summary).
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import random
import re
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs

import aiohttp
import aioresponses.core as aioresponses_core
import fakeredis
import pytest
from aiohttp import ClientResponse, web
from aiohttp.test_utils import TestServer
from aioresponses import CallbackResult, aioresponses
from multidict import CIMultiDict, CIMultiDictProxy
from pydantic import SecretStr
from yarl import URL

from deal_radar.config_schema import AppConfig, EbaySource, load_config
from deal_radar.core.backoff import BackoffPolicy
from deal_radar.core.http import HttpClient, HttpStatusError, NetworkSettings
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import RawListing, SourceKind
from deal_radar.sources.base import IngestorContext, SourceAuthError, SourceBlocked, SourceError
from deal_radar.sources.ebay_api import (
    APP_SCOPE,
    SEARCH_PATH,
    TOKEN_PATH,
    EbayEndpoints,
    EbayIngestor,
    EbayRateLimited,
    EbayRequestError,
    EbayResponseError,
    EbayTokenManager,
    QuotaLedger,
    build_enduserctx,
    build_filter,
    build_headers,
    build_search_url,
    classify_error,
    collect_images,
    decode_search_body,
    looks_blocked,
    parse_item_summary,
    parse_search_page,
    parse_shipping,
    price_bounds,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ebay" / "search_rtx4090.json"
CONFIG_PATH = Path(__file__).parents[1] / "config.yaml"
API = "https://api.ebay.com"
TOKEN_URL = API + TOKEN_PATH
SEARCH_RE = re.compile(r"^https://api\.ebay\.com/buy/browse/v1/item_summary/search\?.*$")
RTX4090_FILTER = (
    "price:[450..1950],priceCurrency:USD,buyingOptions:{FIXED_PRICE|BEST_OFFER},"
    "conditionIds:{1000|3000},itemLocationCountry:US,deliveryCountry:US"
)
NOON_PDT = datetime(2026, 10, 6, 19, 0, tzinfo=timezone.utc)  # 12:00 America/Los_Angeles
INVALID_CLIENT = {"error": "invalid_client", "error_description": "client authentication failed"}


# --------------------------------------------------------------------------- helpers


class _CompatClientResponse(ClientResponse):
    """aioresponses 0.7.9 predates aiohttp 3.14's required ``stream_writer`` argument."""

    def __init__(self, method: str, url: URL, **kwargs: Any) -> None:
        if _NEEDS_STREAM_WRITER:
            kwargs.setdefault("stream_writer", SimpleNamespace(output_size=0))
        super().__init__(method, url, **kwargs)


_NEEDS_STREAM_WRITER = "stream_writer" in inspect.signature(ClientResponse.__init__).parameters


@pytest.fixture(autouse=True)
def _aioresponses_compat(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(aioresponses_core, "ClientResponse", _CompatClientResponse)


def fixture() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def token_payload(token: str, expires_in: int = 7200) -> dict[str, Any]:
    return {"access_token": token, "expires_in": expires_in, "token_type": "Application Access Token"}


def make_http(**settings: Any) -> HttpClient:
    return HttpClient.create(NetworkSettings(retry=BackoffPolicy(max_attempts=2, base_delay=0, max_delay=0), **settings))


@pytest.fixture
async def http():
    client = make_http()
    yield client
    await client.close()


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t


def gpu_profile(
    pid: str,
    terms: list[str],
    *,
    floor: float = 450,
    target: float = 1400,
    ceiling: float = 1950,
    condition_ids: tuple[int, ...] = (1000, 3000),
    category: str = "27386",
    **extra: Any,
) -> dict[str, Any]:
    return {
        "id": pid,
        "name": pid.replace("_", " ").upper(),
        "category": "gpu",
        "match": {"any": [rf"\b{pid}\b"]},
        "price": {"reference_new": ceiling, "reference_used": target, "floor": floor, "target": target, "ceiling": ceiling},
        "search": {"terms": terms, "ebay_category_ids": [category], "ebay_condition_ids": list(condition_ids)},
        **extra,
    }


def make_config(profiles: list[dict[str, Any]] | None = None, **ebay: Any) -> AppConfig:
    ebay_cfg: dict[str, Any] = {
        "enabled": True,
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "delivery_postal_code": "19406",
        "jitter_pct": 0.0,
        **ebay,
    }
    return AppConfig.model_validate(
        {"sources": {"ebay": ebay_cfg}, "profiles": profiles if profiles is not None else [gpu_profile("rtx_4090", ["rtx 4090"])]}
    )


def make_ingestor(
    config: AppConfig,
    http: HttpClient,
    *,
    redis: Any = None,
    endpoints: EbayEndpoints | None = None,
    ledger_now: datetime | None = NOON_PDT,
) -> EbayIngestor:
    ctx = IngestorContext(http=http, metrics=Metrics(), config=config, node_id="test", redis=redis, rng=random.Random(7))
    ingestor = EbayIngestor(config.sources.ebay, ctx, endpoints=endpoints)
    if ledger_now is not None:
        ingestor.ledger = QuotaLedger(config.sources.ebay.daily_call_budget, clock=lambda: ledger_now)
    return ingestor


def requests_for(m: aioresponses, method: str) -> list[tuple[URL, Any]]:
    out = []
    for (verb, url), calls in m.requests.items():
        if verb == method:
            out.extend((url, call) for call in calls)
    return out


def ebay_error(error_id: int, message: str, domain: str = "API_BROWSE") -> dict[str, Any]:
    return {"errors": [{"errorId": error_id, "domain": domain, "category": "REQUEST", "message": message}]}


# --------------------------------------------------------------------------- token manager


async def test_token_fetch_uses_basic_auth_and_form_body_and_is_cached(http: HttpClient) -> None:
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid-PRD-123"), SecretStr("PRD-secret"), clock=FakeClock())
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("v^1.1#i^1#tok-1"))
        assert await tm.get_token() == "v^1.1#i^1#tok-1"
        assert await tm.get_token() == "v^1.1#i^1#tok-1"
    posts = requests_for(m, "POST")
    assert len(posts) == 1
    kwargs = posts[0][1].kwargs
    assert kwargs["headers"]["Authorization"] == "Basic " + base64.b64encode(b"cid-PRD-123:PRD-secret").decode()
    assert kwargs["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert kwargs["data"] == "grant_type=client_credentials&scope=https%3A%2F%2Fapi.ebay.com%2Foauth%2Fapi_scope"
    assert parse_qs(kwargs["data"]) == {"grant_type": ["client_credentials"], "scope": [APP_SCOPE]}


async def test_token_refreshed_proactively_five_minutes_before_expiry(http: HttpClient) -> None:
    clock = FakeClock()
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), clock=clock)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok-1"))
        m.post(TOKEN_URL, payload=token_payload("tok-2"))
        assert await tm.get_token() == "tok-1"
        clock.t += 7200 - 300 - 1  # just before the refresh point
        assert await tm.get_token() == "tok-1"
        assert len(requests_for(m, "POST")) == 1
        clock.t += 2  # inside the 5 minute margin, token still technically valid
        assert await tm.get_token() == "tok-2"
        assert len(requests_for(m, "POST")) == 2


async def test_short_lived_token_refreshes_at_half_life(http: HttpClient) -> None:
    clock = FakeClock()
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), clock=clock)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("short-1", expires_in=400))
        m.post(TOKEN_URL, payload=token_payload("short-2", expires_in=400))
        assert await tm.get_token() == "short-1"
        clock.t += 199
        assert await tm.get_token() == "short-1"
        clock.t += 2
        assert await tm.get_token() == "short-2"


async def test_concurrent_callers_trigger_a_single_refresh(http: HttpClient) -> None:
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), clock=FakeClock())

    async def slow_token(url: URL, **kwargs: Any) -> CallbackResult:
        await asyncio.sleep(0.02)  # keep the refresh in flight while the others queue up
        return CallbackResult(payload=token_payload("shared-token"))

    with aioresponses() as m:
        m.post(TOKEN_URL, callback=slow_token, repeat=True)
        tokens = await asyncio.gather(*(tm.get_token() for _ in range(25)))
    assert set(tokens) == {"shared-token"}
    assert len(requests_for(m, "POST")) == 1


async def test_waiters_share_a_failed_refresh_instead_of_retrying(http: HttpClient) -> None:
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), clock=FakeClock())

    async def rejected(url: URL, **kwargs: Any) -> CallbackResult:
        await asyncio.sleep(0.02)
        return CallbackResult(status=401, payload=INVALID_CLIENT)

    with aioresponses() as m:
        m.post(TOKEN_URL, callback=rejected, repeat=True)
        results = await asyncio.gather(*(tm.get_token() for _ in range(10)), return_exceptions=True)
    assert all(isinstance(r, SourceAuthError) for r in results)
    assert len(requests_for(m, "POST")) == 1


async def test_invalid_client_raises_auth_error_without_leaking_secret(http: HttpClient) -> None:
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("super-secret-value"), clock=FakeClock())
    with aioresponses() as m:
        m.post(TOKEN_URL, status=401, payload=INVALID_CLIENT)
        with pytest.raises(SourceAuthError) as excinfo:
            await tm.get_token()
    assert "invalid_client" in str(excinfo.value)
    assert "super-secret-value" not in str(excinfo.value)


async def test_invalid_scope_is_an_auth_error(http: HttpClient) -> None:
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), clock=FakeClock())
    with aioresponses() as m:
        m.post(TOKEN_URL, status=400, payload={"error": "invalid_scope", "error_description": "The requested scope is invalid"})
        with pytest.raises(SourceAuthError, match="invalid_scope"):
            await tm.get_token()


async def test_token_endpoint_outage_is_recoverable_source_error(http: HttpClient) -> None:
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), clock=FakeClock())
    with aioresponses() as m:
        m.post(TOKEN_URL, status=503, body="Service Unavailable", repeat=True)
        with pytest.raises(SourceError) as excinfo:
            await tm.get_token()
    assert not isinstance(excinfo.value, SourceAuthError)
    assert len(requests_for(m, "POST")) == 2  # one retry from the shared policy


async def test_missing_credentials_raise_auth_error(http: HttpClient) -> None:
    tm = EbayTokenManager(http, TOKEN_URL, None, None, clock=FakeClock())
    with aioresponses() as m:
        with pytest.raises(SourceAuthError):
            await tm.get_token()
    assert not m.requests


async def test_failed_proactive_refresh_keeps_still_valid_token(http: HttpClient) -> None:
    clock = FakeClock()
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), clock=clock)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok-1"))
        m.post(TOKEN_URL, status=503, body="down", repeat=True)
        assert await tm.get_token() == "tok-1"
        clock.t += 7000  # inside the refresh margin, 200 s before hard expiry
        assert await tm.get_token() == "tok-1"
        assert len(requests_for(m, "POST")) == 3  # 1 mint + 2 failed refresh attempts
        clock.t += 10
        assert await tm.get_token() == "tok-1"  # refresh retry is deferred, no new request
        assert len(requests_for(m, "POST")) == 3
        clock.t += 300  # past hard expiry: the failure must surface now
        with pytest.raises(SourceError):
            await tm.get_token()


async def test_invalidate_only_drops_the_rejected_token(http: HttpClient) -> None:
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), clock=FakeClock())
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok-1"))
        m.post(TOKEN_URL, payload=token_payload("tok-2"))
        assert await tm.get_token() == "tok-1"
        await tm.invalidate("some-older-token")
        assert await tm.get_token() == "tok-1"
        await tm.invalidate("tok-1")
        assert await tm.get_token() == "tok-2"


async def test_token_is_shared_through_redis(http: HttpClient) -> None:
    redis = fakeredis.FakeAsyncRedis()
    key = "dr:ebay:token:production:abc"
    wall = FakeClock(1_800_000_000.0)
    first = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), redis=redis, redis_key=key, wall_clock=wall)
    second = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), redis=redis, redis_key=key, wall_clock=wall)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("fleet-token"))
        assert await first.get_token() == "fleet-token"
        assert await second.get_token() == "fleet-token"
        assert len(requests_for(m, "POST")) == 1
    ttl_ms = await redis.pttl(key)
    assert 6_890_000 < ttl_ms <= 6_900_000  # lifetime minus the 5 minute refresh margin
    await second.invalidate("fleet-token")
    assert await redis.get(key) is None


# --------------------------------------------------------------------------- request construction


def test_build_filter_exact_syntax() -> None:
    assert build_filter(
        price_min=600,
        price_max=2600,
        currency="USD",
        buying_options=["FIXED_PRICE", "BEST_OFFER"],
        condition_ids=[1000, 1500, 3000, 1000],
        item_location_country="us",
        delivery_country="US",
    ) == (
        "price:[600..2600],priceCurrency:USD,buyingOptions:{FIXED_PRICE|BEST_OFFER},"
        "conditionIds:{1000|1500|3000},itemLocationCountry:US,deliveryCountry:US"
    )
    assert build_filter(price_min=None, price_max=99.5, buying_options=["FIXED_PRICE"]) == (
        "price:[..99.50],priceCurrency:USD,buyingOptions:{FIXED_PRICE}"
    )
    assert build_filter(price_min=10, price_max=None, buying_options=[]) == "price:[10],priceCurrency:USD"
    assert build_filter(price_min=0, price_max=None, buying_options=[], delivery_country="US") == "deliveryCountry:US"


def test_build_search_url_encodes_filter_grammar_as_one_value() -> None:
    url = build_search_url(
        API + SEARCH_PATH,
        q="rtx 4090 c++",
        category_ids=["27386"],
        filter_expr="price:[450..1950],priceCurrency:USD,buyingOptions:{FIXED_PRICE|BEST_OFFER}",
        limit=100,
    )
    assert url == (
        "https://api.ebay.com/buy/browse/v1/item_summary/search?q=rtx%204090%20c%2B%2B&category_ids=27386"
        "&filter=price%3A%5B450..1950%5D%2CpriceCurrency%3AUSD%2CbuyingOptions%3A%7BFIXED_PRICE%7CBEST_OFFER%7D"
        "&sort=newlyListed&limit=100&fieldgroups=MATCHING_ITEMS%2CEXTENDED"
    )
    parsed = URL(url)
    assert parsed.query["q"] == "rtx 4090 c++"  # '+' survives, spaces are not '+'
    assert parsed.query["filter"] == "price:[450..1950],priceCurrency:USD,buyingOptions:{FIXED_PRICE|BEST_OFFER}"


def test_headers_include_marketplace_and_encoded_enduser_context() -> None:
    cfg = EbaySource(delivery_postal_code="19406", affiliate_campaign_id="5338123456")
    assert build_headers(cfg, "tok") == {
        "Authorization": "Bearer tok",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
        "X-EBAY-C-ENDUSERCTX": "contextualLocation=country%3DUS%2Czip%3D19406,affiliateCampaignId=5338123456",
        "Accept-Language": "en-US",
        "Accept-Encoding": "gzip",
    }
    # eBay requires the zip with the country, so no zip means no contextualLocation at all.
    assert build_enduserctx(EbaySource(affiliate_campaign_id="5338123456")) == "affiliateCampaignId=5338123456"
    assert build_enduserctx(EbaySource()) is None
    assert "X-EBAY-C-ENDUSERCTX" not in build_headers(EbaySource(), "tok")
    assert build_headers(EbaySource(marketplace_id="EBAY_GB"), "tok")["Accept-Language"] == "en-GB"


def test_price_bounds_cover_all_variant_bands_and_respect_overrides() -> None:
    profile = gpu_profile(
        "rtx_3090",
        ["rtx 3090"],
        floor=220,
        target=650,
        ceiling=950,
        variants=[{"id": "ti", "match": [r"\b3090\s*ti\b"], "price": {"floor": 250, "target": 700, "ceiling": 1050}}],
    )
    config = make_config([profile])
    assert price_bounds(config.profiles[0]) == (220, 1050)
    override = gpu_profile("rtx_4090", ["rtx 4090"])
    override["search"].update({"price_min": 900, "price_max": 1500})
    assert price_bounds(make_config([override]).profiles[0]) == (900, 1500)


async def test_poll_sends_expected_query_and_headers(http: HttpClient) -> None:
    config = make_config(affiliate_campaign_id="5338123456", limit=8)
    ingestor = make_ingestor(config, http)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok-1"))
        m.get(SEARCH_RE, payload=fixture())
        listings = await ingestor.poll()
    gets = requests_for(m, "GET")
    assert len(gets) == 1
    url, call = gets[0]
    assert url.path == SEARCH_PATH
    assert dict(url.query) == {
        "q": "rtx 4090",
        "category_ids": "27386",
        "filter": RTX4090_FILTER,
        "sort": "newlyListed",
        "limit": "8",
        "fieldgroups": "MATCHING_ITEMS,EXTENDED",
    }
    headers = call.kwargs["headers"]
    assert headers["Authorization"] == "Bearer tok-1"
    assert headers["X-EBAY-C-MARKETPLACE-ID"] == "EBAY_US"
    assert headers["X-EBAY-C-ENDUSERCTX"] == "contextualLocation=country%3DUS%2Czip%3D19406,affiliateCampaignId=5338123456"
    assert headers["Accept-Language"] == "en-US"
    assert headers["Accept"] == "application/json"
    assert len(listings) == 4
    by_id = {raw.source_id: raw for raw in listings}
    assert by_id["v1|388765432101|0"].url.startswith("https://www.ebay.com/itm/388765432101?mkevt=1")  # affiliate URL
    assert all(raw.query == "rtx 4090" and raw.profile_hint == "rtx_4090" for raw in listings)
    assert ingestor.ledger.used() == 1


async def test_wire_level_encoding_against_a_real_server() -> None:
    captured: dict[str, Any] = {}

    async def token(request: web.Request) -> web.Response:
        captured["token_body"] = await request.text()
        captured["token_auth"] = request.headers["Authorization"]
        captured["token_ct"] = request.headers["Content-Type"]
        return web.json_response(token_payload("wire-token"))

    async def search(request: web.Request) -> web.Response:
        captured["raw_query"] = request.rel_url.raw_query_string
        captured["query"] = dict(request.query)
        captured["headers"] = dict(request.headers)
        return web.json_response(fixture())

    app = web.Application()
    app.router.add_post(TOKEN_PATH, token)
    app.router.add_get(SEARCH_PATH, search)
    config = make_config(limit=8)
    async with TestServer(app) as server:
        client = make_http(trust_env=False)
        try:
            endpoints = EbayEndpoints(str(server.make_url("/")).rstrip("/"))
            ingestor = make_ingestor(config, client, endpoints=endpoints)
            listings = await ingestor.poll()
        finally:
            await client.close()
    assert len(listings) == 4
    assert parse_qs(captured["token_body"]) == {"grant_type": ["client_credentials"], "scope": [APP_SCOPE]}
    assert captured["token_auth"] == "Basic " + base64.b64encode(b"test-client-id:test-client-secret").decode()
    assert captured["token_ct"] == "application/x-www-form-urlencoded"
    assert captured["query"]["filter"] == RTX4090_FILTER
    assert captured["query"]["q"] == "rtx 4090"
    raw = captured["raw_query"]
    assert "q=rtx%204090" in raw
    for literal in ("{", "}", "|", "[", "]", " ", "+"):
        assert literal not in raw, f"{literal!r} must be percent-encoded on the wire: {raw}"
    assert "%7BFIXED_PRICE%7CBEST_OFFER%7D" in raw
    assert "%5B450..1950%5D" in raw
    assert captured["headers"]["Authorization"] == "Bearer wire-token"
    assert captured["headers"]["X-EBAY-C-ENDUSERCTX"] == "contextualLocation=country%3DUS%2Czip%3D19406"
    assert captured["headers"]["X-EBAY-C-MARKETPLACE-ID"] == "EBAY_US"


# --------------------------------------------------------------------------- parsing


def test_parse_search_page_realistic_payload() -> None:
    page = parse_search_page(fixture(), query="rtx 4090", profile_hint="rtx_4090")
    assert page.item_count == 8
    assert page.total == 1187
    assert page.next_url is not None and "offset=8" in page.next_url
    assert page.skipped == {"auction_only": 1, "no_price": 1, "malformed": 2}
    assert page.oldest_origin == datetime(2026, 9, 28, 9, 12, tzinfo=timezone.utc)
    by_id = {raw.source_id: raw for raw in page.listings}
    assert set(by_id) == {"v1|226512345678|0", "v1|388765432101|0", "v1|405566778899|0", "v1|296677889900|0"}

    fe = by_id["v1|226512345678|0"]  # FIXED_PRICE + BEST_OFFER, free shipping
    assert isinstance(fe, RawListing)
    assert fe.source == "ebay" and fe.source_kind is SourceKind.MARKETPLACE
    assert fe.listing_key == "ebay:v1|226512345678|0"
    assert fe.price == 1349.99 and fe.currency == "USD"
    assert fe.shipping == 0.0
    assert fe.condition == "3000"
    assert fe.extra["condition_text"] == "Used"
    assert fe.extra["buying_options"] == ["FIXED_PRICE", "BEST_OFFER"]
    assert fe.url.startswith("https://www.ebay.com/itm/226512345678?")
    assert fe.image_urls == [
        "https://i.ebayimg.com/images/g/7pIAAOSwJk1lRj1v/s-l1600.jpg",  # full-size copy replaces the thumb
        "https://i.ebayimg.com/thumbs/images/g/Qa8AAOSwbXNlRj1x/s-l225.jpg",
        "https://i.ebayimg.com/thumbs/images/g/zZ0AAOSw3vRlRj1z/s-l225.jpg",
    ]
    assert fe.seller is not None
    assert (fe.seller.name, fe.seller.feedback_score, fe.seller.feedback_pct) == ("gpu_flipper_tx", 1532, 99.6)
    assert fe.posted_at == datetime(2026, 10, 6, 14, 2, 11, tzinfo=timezone.utc)
    assert fe.location is not None
    location = (fe.location.city, fe.location.region, fe.location.postal_code, fe.location.country)
    assert location == ("Austin", "Texas", "787**", "US")
    assert fe.location.text == "Austin, Texas, 787**, US"
    assert fe.description.startswith("Card works perfectly")
    assert fe.list_price is None
    assert fe.query == "rtx 4090" and fe.profile_hint == "rtx_4090"

    new = by_id["v1|388765432101|0"]  # new, FIXED + CALCULATED shipping, marketing price
    assert new.price == 1799.0
    assert new.shipping == 24.35  # cheapest option
    assert new.extra["shipping_cost_type"] == "CALCULATED"
    assert new.condition == "1000"
    assert new.list_price == 1999.99
    assert new.extra["discount_pct"] == 10.0
    assert new.url == "https://www.ebay.com/itm/388765432101?hash=item5a8b1c2d3e:g:kLMAAOSwq1dlRk2a"  # no affiliate configured
    assert new.extra["epid"] == "18064393842" and new.extra["top_rated"] is True
    assert new.seller is not None and new.seller.feedback_pct == 100.0

    bin_auction = by_id["v1|405566778899|0"]  # auction that still has Buy It Now
    assert bin_auction.price == 1450.0
    assert bin_auction.shipping == 12.5
    assert bin_auction.extra["current_bid"] == 900.0 and bin_auction.extra["bid_count"] == 0
    assert bin_auction.extra["buying_options"] == ["AUCTION", "FIXED_PRICE"]

    relist = by_id["v1|296677889900|0"]  # no image, calculated shipping without a quote, relisted
    assert relist.image_urls == []
    assert relist.shipping is None
    assert relist.condition == "Used"  # no conditionId -> condition text
    assert relist.seller is not None and relist.seller.feedback_score == 3
    assert relist.posted_at == datetime(2026, 10, 6, 13, 47, 30, tzinfo=timezone.utc)  # creation is newer than origin
    assert relist.extra["item_origin_date"] == "2026-09-28T09:12:00+00:00"

    for raw in page.listings:  # must survive the Redis stream round trip
        assert RawListing.model_validate_json(raw.model_dump_json()) == raw


def test_parse_item_summary_affiliate_preference_and_skips() -> None:
    items = fixture()["itemSummaries"]
    affiliate = parse_item_summary(items[1], query="rtx 4090", profile_hint="rtx_4090", prefer_affiliate=True)
    assert affiliate is not None
    assert affiliate.url.startswith("https://www.ebay.com/itm/388765432101?mkevt=1&mkcid=1")
    assert affiliate.extra["item_web_url"] == "https://www.ebay.com/itm/388765432101?hash=item5a8b1c2d3e:g:kLMAAOSwq1dlRk2a"
    plain = parse_item_summary(items[0], query="rtx 4090", profile_hint="rtx_4090", prefer_affiliate=True)
    assert plain is not None and plain.url.startswith("https://www.ebay.com/itm/226512345678?")  # no affiliate URL returned
    assert parse_item_summary(items[2], query="q", profile_hint="p") is None  # AUCTION only
    assert parse_item_summary(items[5], query="q", profile_hint="p") is None  # no price
    assert parse_item_summary(items[6], query="q", profile_hint="p") is None  # no itemId
    bad_price = {"itemId": "v1|1|0", "title": "x", "price": {"value": "abc"}}
    assert parse_item_summary(bad_price, query=None, profile_hint=None) is None
    legacy_only = parse_item_summary(
        {"itemId": "v1|42|0", "legacyItemId": "42", "title": "RTX 4090", "price": {"value": "1000.00", "currency": "USD"}},
        query=None,
        profile_hint=None,
    )
    assert legacy_only is not None and legacy_only.url == "https://www.ebay.com/itm/42"


def test_parse_shipping_rules() -> None:
    free = [{"shippingCostType": "FIXED", "shippingCost": {"value": "0.00", "currency": "USD"}}]
    assert parse_shipping(free, "USD") == (0.0, "FIXED")
    assert parse_shipping([{"shippingCostType": "CALCULATED"}], "USD") == (None, None)
    assert parse_shipping(None, "USD") == (None, None)
    assert parse_shipping(
        [
            {"shippingCostType": "FIXED", "shippingCost": {"value": "9.99", "currency": "GBP"}},  # other currency ignored
            {"shippingCostType": "FIXED", "shippingCost": {"value": "19.99", "currency": "USD"}},
        ],
        "USD",
    ) == (19.99, "FIXED")


def test_collect_images_skips_non_http_and_dedupes() -> None:
    images = collect_images(
        {
            "image": {"imageUrl": "https://i.ebayimg.com/thumbs/images/g/AAA/s-l225.jpg"},
            "thumbnailImages": [{"imageUrl": "https://i.ebayimg.com/images/g/AAA/s-l1600.jpg"}, {"imageUrl": "ftp://x/y.jpg"}],
            "additionalImages": [
                {"imageUrl": "https://cdn.example.com/a.jpg"},
                {"imageUrl": "https://cdn.example.com/a.jpg"},
                "bad",
            ],
        }
    )
    assert images == ["https://i.ebayimg.com/images/g/AAA/s-l1600.jpg", "https://cdn.example.com/a.jpg"]


# --------------------------------------------------------------------------- 401 retry


async def test_401_invalidates_token_and_retries_once(http: HttpClient) -> None:
    ingestor = make_ingestor(make_config(), http)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok-old"))
        m.post(TOKEN_URL, payload=token_payload("tok-new"))
        m.get(SEARCH_RE, status=401, payload=ebay_error(1001, "Invalid access token", domain="OAuth"))
        m.get(SEARCH_RE, payload=fixture())
        listings = await ingestor.poll()
    assert len(listings) == 4
    gets = requests_for(m, "GET")
    assert [call.kwargs["headers"]["Authorization"] for _, call in gets] == ["Bearer tok-old", "Bearer tok-new"]
    assert len(requests_for(m, "POST")) == 2
    assert ingestor.ledger.used() == 2


async def test_second_401_with_fresh_token_is_auth_error(http: HttpClient) -> None:
    ingestor = make_ingestor(make_config(), http)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok-1"))
        m.post(TOKEN_URL, payload=token_payload("tok-2"))
        m.get(SEARCH_RE, status=401, payload=ebay_error(1001, "Invalid access token", domain="OAuth"), repeat=True)
        with pytest.raises(SourceAuthError, match="freshly minted"):
            await ingestor.poll()
    assert len(requests_for(m, "GET")) == 2


async def test_concurrent_401s_mint_only_one_replacement_token(http: HttpClient) -> None:
    profiles = [gpu_profile(f"gpu_{i}", [f"gpu {i}"]) for i in range(4)]
    ingestor = make_ingestor(make_config(profiles, max_concurrency=4), http)
    seen: list[str] = []

    async def search(url: URL, **kwargs: Any) -> CallbackResult:
        auth = kwargs["headers"]["Authorization"]
        seen.append(auth)
        await asyncio.sleep(0.01)
        if auth == "Bearer tok-old":
            return CallbackResult(status=401, payload=ebay_error(1001, "Invalid access token", domain="OAuth"))
        return CallbackResult(payload={"total": 0, "itemSummaries": []})

    async def mint(url: URL, **kwargs: Any) -> CallbackResult:
        await asyncio.sleep(0.01)
        return CallbackResult(payload=token_payload("tok-new"))

    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok-old"))
        m.post(TOKEN_URL, callback=mint, repeat=True)
        m.get(SEARCH_RE, callback=search, repeat=True)
        assert await ingestor.poll() == []
    assert seen.count("Bearer tok-old") == 4 and seen.count("Bearer tok-new") == 4
    assert len(requests_for(m, "POST")) == 2


# --------------------------------------------------------------------------- partial failure & throttling


def _route_by_query(responses: dict[str, CallbackResult | None]):
    def callback(url: URL, **kwargs: Any) -> CallbackResult:
        result = responses[url.query["q"]]
        return result if result is not None else CallbackResult(payload=fixture())

    return callback


async def test_failing_query_does_not_discard_other_results(http: HttpClient) -> None:
    profiles = [gpu_profile("rtx_4090", ["rtx 4090"]), gpu_profile("rtx_3090", ["rtx 3090"])]
    ingestor = make_ingestor(make_config(profiles, max_concurrency=1), http)
    responses = {"rtx 4090": None, "rtx 3090": CallbackResult(status=500, payload=ebay_error(12000, "Internal error"))}
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, callback=_route_by_query(responses), repeat=True)
        listings = await ingestor.poll()
    assert len(listings) == 4
    queries = ingestor.ctx.metrics.counter("ebay_queries_total", "", ("profile", "outcome"))
    assert queries.value(profile="rtx_4090", outcome="ok") == 1
    assert queries.value(profile="rtx_3090", outcome="error") == 1
    assert ingestor.ledger.used() == 3  # 1 success + 2 attempts on the 500


async def test_all_queries_failing_raises_source_error(http: HttpClient) -> None:
    profiles = [gpu_profile("rtx_4090", ["rtx 4090"]), gpu_profile("rtx_3090", ["rtx 3090"])]
    ingestor = make_ingestor(make_config(profiles), http)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        too_large = ebay_error(12023, "This keyword search results in a response that is too large to return.")
        m.get(SEARCH_RE, status=400, payload=too_large, repeat=True)
        with pytest.raises(SourceError, match="all 2 eBay queries failed") as excinfo:
            await ingestor.poll()
    assert "12023" in str(excinfo.value)
    assert not isinstance(excinfo.value, (SourceAuthError, EbayRateLimited))


async def test_throttle_on_every_query_raises_rate_limited_and_skips_the_rest(http: HttpClient) -> None:
    profiles = [gpu_profile("rtx_4090", ["rtx 4090"]), gpu_profile("rtx_3090", ["rtx 3090"])]
    config = make_config(profiles, max_concurrency=1, poll_interval_seconds=1, daily_call_budget=1_000_000)
    ingestor = make_ingestor(config, http)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, status=429, payload=ebay_error(2001, "Too many requests", domain="ACCESS"), repeat=True)
        with pytest.raises(EbayRateLimited):
            await ingestor.poll()
    gets = requests_for(m, "GET")
    assert {url.query["q"] for url, _ in gets} == {"rtx 4090"}  # second query skipped
    assert len(gets) == 2  # the throttled request plus its single retry
    queries = ingestor.ctx.metrics.counter("ebay_queries_total", "", ("profile", "outcome"))
    assert queries.value(profile="rtx_3090", outcome="skipped") == 1
    assert 29.0 < ingestor.next_interval() <= 30.0  # throttle penalty dominates the 1 s interval


async def test_partial_throttle_keeps_results_and_stretches_interval(http: HttpClient) -> None:
    profiles = [gpu_profile(f"rtx_{n}", [f"rtx {n}"]) for n in (4090, 3090, 5090)]
    config = make_config(profiles, max_concurrency=1, poll_interval_seconds=1, daily_call_budget=1_000_000)
    ingestor = make_ingestor(config, http)
    throttled = CallbackResult(status=429, payload=ebay_error(2001, "Too many requests", domain="ACCESS"))
    responses = {"rtx 4090": None, "rtx 3090": throttled, "rtx 5090": None}
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, callback=_route_by_query(responses), repeat=True)
        listings = await ingestor.poll()
        assert len(listings) == 4
        assert {url.query["q"] for url, _ in requests_for(m, "GET")} == {"rtx 4090", "rtx 3090"}
        assert 29.0 < ingestor.next_interval() <= 30.0
        responses["rtx 3090"] = None
        await ingestor.poll()  # a clean poll clears the penalty
    assert ingestor.next_interval() == pytest.approx(1.0)


async def test_duplicate_items_across_queries_are_emitted_once(http: HttpClient) -> None:
    profiles = [gpu_profile("prebuilt_a", ["4090 gaming pc"]), gpu_profile("prebuilt_b", ["5090 gaming pc"])]
    ingestor = make_ingestor(make_config(profiles, max_concurrency=1), http)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, payload=fixture(), repeat=True)
        listings = await ingestor.poll()
    assert len(listings) == 4
    assert {raw.profile_hint for raw in listings} == {"prebuilt_a"}
    skipped = ingestor.ctx.metrics.counter("ebay_items_skipped_total", "", ("reason",))
    assert skipped.value(reason="duplicate") == 4
    assert skipped.value(reason="auction_only") == 2


async def test_full_page_newer_than_last_poll_is_flagged_as_saturated(http: HttpClient) -> None:
    ingestor = make_ingestor(make_config(limit=8), http)
    query = ingestor.queries[0]
    ingestor._last_query_success[query.key] = datetime(2026, 9, 1, tzinfo=timezone.utc)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, payload=fixture(), repeat=True)
        await ingestor.poll()
        saturated = ingestor.ctx.metrics.counter("ebay_page_saturated_total", "", ("profile",))
        assert saturated.value(profile="rtx_4090") == 1
        await ingestor.poll()  # previous poll is now newer than the oldest item -> no flag
    assert saturated.value(profile="rtx_4090") == 1


def test_classify_error_maps_ebay_statuses() -> None:
    url = API + SEARCH_PATH
    denied = classify_error(HttpStatusError(403, url, json.dumps(ebay_error(1100, "Access denied", domain="ACCESS"))))
    assert isinstance(denied, SourceAuthError) and "1100" in str(denied)
    bad = classify_error(HttpStatusError(400, url, json.dumps(ebay_error(12521, "malformed encoding"))))
    assert isinstance(bad, EbayRequestError) and "12521" in str(bad)
    assert isinstance(classify_error(HttpStatusError(429, url, "HTTP 429 from api.ebay.com")), EbayRateLimited)
    server = classify_error(HttpStatusError(503, url, "<html>"))
    assert type(server) is SourceError and "503" in str(server)
    assert isinstance(classify_error(asyncio.TimeoutError()), SourceError)


# --------------------------------------------------------------------------- quota scheduling


def _profiles_with_terms(count: int) -> list[dict[str, Any]]:
    # 5 (profile, term) pairs spread over 3 profiles; duplicate terms are counted once.
    terms = [["rtx 4090", "4090 founders"], ["rtx 3090", "rtx 3090 ti", "rtx 3090"], ["rtx 5090"]]
    return [gpu_profile(f"gpu_{i}", t) for i, t in enumerate(terms)][:count]


async def test_quota_interval_math(http: HttpClient) -> None:
    config = make_config(_profiles_with_terms(3), daily_call_budget=5000, budget_safety_factor=0.85, poll_interval_seconds=45)
    ingestor = make_ingestor(config, http)
    assert ingestor.calls_per_poll() == 5
    allowed_polls_per_day = 5000 * 0.85 / 5  # 850
    assert ingestor.quota_interval() == pytest.approx(86_400 / allowed_polls_per_day)  # 101.65 s
    assert ingestor.next_interval() == pytest.approx(101.647, abs=1e-3)  # jitter_pct = 0

    single = make_ingestor(make_config(daily_call_budget=5000, budget_safety_factor=0.85, poll_interval_seconds=45), http)
    assert single.calls_per_poll() == 1
    assert single.quota_interval() == 45.0  # 86400 / 4250 = 20.3 s < configured floor

    jittered = make_ingestor(make_config(_profiles_with_terms(3), jitter_pct=0.2), http)
    base = jittered.quota_interval()
    samples = [jittered.next_interval() for _ in range(500)]
    assert all(0.8 * base - 1e-9 <= s <= 1.2 * base + 1e-9 for s in samples)
    assert sum(samples) / len(samples) == pytest.approx(base, rel=0.03)  # jitter keeps the daily average


async def test_quota_interval_for_shipped_config(http: HttpClient) -> None:
    env = {"EBAY_ENABLED": "true", "EBAY_CLIENT_ID": "id", "EBAY_CLIENT_SECRET": "secret", "HOME_ZIP": "19406"}
    config = load_config(CONFIG_PATH, env=env)
    ingestor = make_ingestor(config, http)
    ebay = config.sources.ebay
    if ebay.queries:  # coalesced searches replace the per-profile terms
        distinct = {(q.q.strip(), q.category_id, q.price_min, q.price_max, tuple(q.condition_ids)) for q in ebay.queries}
        expected_calls = len(distinct)
    else:
        expected_calls = sum(
            len({t.strip() for t in p.search.terms if t.strip()})
            for p in config.profiles
            if p.enabled and p.search.terms and (not p.search.sources or "ebay" in p.search.sources)
        )
    assert expected_calls > 0
    assert ingestor.calls_per_poll() == expected_calls
    expected = max(ebay.poll_interval_seconds, 86_400 * expected_calls / (ebay.daily_call_budget * ebay.budget_safety_factor))
    assert ingestor.quota_interval() == pytest.approx(expected)
    assert all(q.filter.endswith("itemLocationCountry:US,deliveryCountry:US") for q in ingestor.queries)
    assert all("buyingOptions:{FIXED_PRICE|BEST_OFFER}" in q.filter for q in ingestor.queries)
    assert all("priceCurrency:USD" in q.filter for q in ingestor.queries if "price:[" in q.filter)
    assert all(len(q.term) <= 100 and len(q.category_ids) <= 1 for q in ingestor.queries)


async def test_ledger_stretches_interval_when_usage_runs_ahead_of_plan(http: HttpClient) -> None:
    config = make_config(_profiles_with_terms(3), daily_call_budget=5000, budget_safety_factor=0.85, poll_interval_seconds=45)
    ingestor = make_ingestor(config, http)
    assert ingestor.ledger.seconds_until_reset() == 12 * 3600  # Pacific midnight reset
    await ingestor.ledger.record(4000)  # 4250 usable - 4000 used = 250 calls left for 12 h
    assert ingestor.ledger_interval() == pytest.approx(43_200 * 5 / 250)  # 864 s
    assert ingestor.next_interval() == pytest.approx(864.0)

    await ingestor.ledger.record(248)  # only 2 usable calls left < 5 per poll -> wait for reset
    assert ingestor.next_interval() == pytest.approx(43_200 + 1.0)


async def test_poll_is_skipped_when_daily_budget_is_exhausted(http: HttpClient) -> None:
    config = make_config(_profiles_with_terms(3), daily_call_budget=5000)
    ingestor = make_ingestor(config, http)
    await ingestor.ledger.record(4998)
    with aioresponses() as m:
        assert await ingestor.poll() == []
    assert not m.requests


async def test_ledger_rolls_over_at_pacific_midnight_and_is_shared_via_redis() -> None:
    now = [datetime(2026, 10, 7, 6, 59, tzinfo=timezone.utc)]  # 23:59 PDT
    redis = fakeredis.FakeAsyncRedis()
    first = QuotaLedger(5000, redis=redis, key_prefix="dr:ebay:calls:k:", clock=lambda: now[0])
    second = QuotaLedger(5000, redis=redis, key_prefix="dr:ebay:calls:k:", clock=lambda: now[0])
    assert await first.record(3) == 3
    assert await second.record(4) == 7  # fleet-wide total
    assert await redis.get("dr:ebay:calls:k:2026-10-06") == b"7"
    assert 0 < await redis.ttl("dr:ebay:calls:k:2026-10-06") <= 2 * 86_400
    assert first.seconds_until_reset() == 60.0
    now[0] += timedelta(minutes=2)  # 00:01 PDT: new quota day
    assert first.used() == 0
    assert await first.record(0) == 0
    winter = QuotaLedger(5000, clock=lambda: datetime(2026, 12, 1, 20, 0, tzinfo=timezone.utc))  # 12:00 PST
    assert winter.seconds_until_reset() == 12 * 3600


# --------------------------------------------------------------------------- lifecycle


async def test_ingestor_identity_and_setup_requires_credentials(http: HttpClient) -> None:
    assert EbayIngestor.name == "ebay"
    assert EbayIngestor.kind is SourceKind.MARKETPLACE
    config = AppConfig.model_validate({"profiles": [gpu_profile("rtx_4090", ["rtx 4090"])]})  # ebay disabled, no creds
    ingestor = make_ingestor(config, http)
    with pytest.raises(SourceAuthError):
        await ingestor.setup()
    assert EbayEndpoints.for_environment("sandbox").search_url == "https://api.sandbox.ebay.com" + SEARCH_PATH
    assert EbayEndpoints.for_environment("sandbox").token_url == "https://api.sandbox.ebay.com" + TOKEN_PATH
    sandbox = make_ingestor(make_config(environment="sandbox"), http)
    assert sandbox.tokens.token_url == "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
    assert sandbox.queries[0].url.startswith("https://api.sandbox.ebay.com/buy/browse/v1/item_summary/search?")


async def test_run_once_emits_only_new_or_changed_listings(http: HttpClient) -> None:
    ingestor = make_ingestor(make_config(), http)
    await ingestor.setup()
    changed = fixture()
    changed["itemSummaries"][0]["price"]["value"] = "1299.99"
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, payload=fixture())
        m.get(SEARCH_RE, payload=fixture())
        m.get(SEARCH_RE, payload=changed)
        first = await ingestor.run_once()
        second = await ingestor.run_once()
        third = await ingestor.run_once()
    assert len(first) == 4 and all(raw.node_id == "test" for raw in first)
    assert second == []
    assert [(raw.source_id, raw.price) for raw in third] == [("v1|226512345678|0", 1299.99)]
    assert len(requests_for(m, "POST")) == 1  # token reused across polls
    await ingestor.teardown()


# --------------------------------------------------------------------------- adversarial regressions

AKAMAI_403 = (
    "<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD><BODY>\n<H1>Access Denied</H1>\n \n"
    "You don't have permission to access \"http&#58;&#47;&#47;api&#46;ebay&#46;com&#47;buy&#47;browse&#47;v1&#47;"
    "item&#95;summary&#47;search&#63;\" on this server.<P>\n"
    "Reference&#32;&#35;18&#46;5c3e1002&#46;1759730000&#46;1a2b3c4d\n</BODY>\n</HTML>\n"
)


async def test_configured_coalesced_queries_replace_profile_terms(http: HttpClient) -> None:
    profiles = [
        gpu_profile("rtx_4090", ["rtx 4090", "4090 founders"]),
        gpu_profile("rtx_5090", ["rtx 5090"], floor=600, target=1900, ceiling=2600),
    ]
    queries = [
        {"q": "rtx (4090, 5090)", "category_id": "27386", "price_min": 450, "price_max": 2600, "condition_ids": [1000, 3000]},
        {"q": "rtx 4090 founders edition", "profile_hint": "rtx_4090"},  # unset fields inherit the hinted profile
        {"q": "geforce rtx"},  # nothing to inherit: no price/condition/category filter
    ]
    ingestor = make_ingestor(make_config(profiles, limit=8, queries=queries), http)
    assert ingestor.calls_per_poll() == 3  # the 3 profile terms are replaced, not added
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, payload=fixture(), repeat=True)
        listings = await ingestor.poll()
    by_q = {url.query["q"]: url for url, _ in requests_for(m, "GET")}
    assert set(by_q) == {"rtx (4090, 5090)", "rtx 4090 founders edition", "geforce rtx"}
    tail = "buyingOptions:{FIXED_PRICE|BEST_OFFER},conditionIds:{1000|3000},itemLocationCountry:US,deliveryCountry:US"
    assert by_q["rtx (4090, 5090)"].query["filter"] == "price:[450..2600],priceCurrency:USD," + tail
    assert by_q["rtx (4090, 5090)"].query["category_ids"] == "27386"
    assert by_q["rtx 4090 founders edition"].query["filter"] == RTX4090_FILTER
    assert by_q["rtx 4090 founders edition"].query["category_ids"] == "27386"
    bare = "buyingOptions:{FIXED_PRICE|BEST_OFFER},itemLocationCountry:US,deliveryCountry:US"
    assert by_q["geforce rtx"].query["filter"] == bare
    assert "category_ids" not in by_q["geforce rtx"].query
    assert len(listings) == 4  # same items from every query, emitted once
    assert all(raw.query == "rtx (4090, 5090)" and raw.profile_hint is None for raw in listings)
    queries_metric = ingestor.ctx.metrics.counter("ebay_queries_total", "", ("profile", "outcome"))
    assert queries_metric.value(profile="coalesced", outcome="ok") == 2
    assert queries_metric.value(profile="rtx_4090", outcome="ok") == 1


async def test_multiple_category_ids_never_combine_with_fieldgroups(http: HttpClient) -> None:
    # eBay answers 409 errorId 12020 when fieldgroups is sent with more than one category id.
    url = URL(build_search_url(API + SEARCH_PATH, q="rtx 4090", category_ids=["27386", "175673"], limit=50))
    assert url.query["category_ids"] == "27386,175673"
    assert "fieldgroups" not in url.query
    profile = gpu_profile("rtx_4090", ["rtx 4090"])
    profile["search"]["ebay_category_ids"] = ["27386", "175673"]
    ingestor = make_ingestor(make_config([profile]), http)
    assert "fieldgroups" not in URL(ingestor.queries[0].url).query
    single = make_ingestor(make_config(), http)
    assert URL(single.queries[0].url).query["fieldgroups"] == "MATCHING_ITEMS,EXTENDED"


async def test_token_outage_costs_one_mint_per_poll_not_one_per_wave(http: HttpClient) -> None:
    profiles = [gpu_profile(f"gpu_{i}", [f"gpu {i}"]) for i in range(8)]
    ingestor = make_ingestor(make_config(profiles, max_concurrency=2), http)
    with aioresponses() as m:
        m.post(TOKEN_URL, status=503, body="Service Unavailable", repeat=True)
        with pytest.raises(SourceError) as excinfo:
            await ingestor.poll()
    assert not isinstance(excinfo.value, SourceAuthError)
    assert len(requests_for(m, "POST")) == 2  # one mint (+ its single retry) for the whole poll
    assert not requests_for(m, "GET")
    queries = ingestor.ctx.metrics.counter("ebay_queries_total", "", ("profile", "outcome"))
    assert sum(queries.value(profile=f"gpu_{i}", outcome="skipped") for i in range(8)) == 6


async def test_edge_block_page_raises_source_blocked(http: HttpClient) -> None:
    url = API + SEARCH_PATH
    blocked = classify_error(HttpStatusError(403, url, AKAMAI_403, {"Content-Type": "text/html"}))
    assert isinstance(blocked, SourceBlocked)
    denied = classify_error(HttpStatusError(403, url, json.dumps(ebay_error(1100, "Access denied", domain="ACCESS"))))
    assert isinstance(denied, SourceAuthError) and not isinstance(denied, SourceBlocked)

    profiles = [gpu_profile("rtx_4090", ["rtx 4090"]), gpu_profile("rtx_3090", ["rtx 3090"])]
    ingestor = make_ingestor(make_config(profiles, max_concurrency=1), http)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, status=403, body=AKAMAI_403, content_type="text/html", repeat=True)
        with pytest.raises(SourceBlocked):
            await ingestor.poll()
    assert len(requests_for(m, "GET")) == 1  # the wall stops the remaining queries


async def test_unexpected_success_body_is_a_query_failure_not_an_empty_success(http: HttpClient) -> None:
    profiles = [gpu_profile("rtx_4090", ["rtx 4090"]), gpu_profile("rtx_3090", ["rtx 3090"])]
    ingestor = make_ingestor(make_config(profiles, max_concurrency=1), http)
    responses = {"rtx 4090": None, "rtx 3090": CallbackResult(body="[]", content_type="application/json")}
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, callback=_route_by_query(responses), repeat=True)
        assert len(await ingestor.poll()) == 4
    queries = ingestor.ctx.metrics.counter("ebay_queries_total", "", ("profile", "outcome"))
    assert queries.value(profile="rtx_3090", outcome="error") == 1

    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, body="", content_type="application/json", repeat=True)  # empty 200 body
        with pytest.raises(SourceError, match="all 2 eBay queries failed"):
            await ingestor.poll()


async def test_slow_query_does_not_discard_finished_results(http: HttpClient) -> None:
    profiles = [gpu_profile("rtx_4090", ["rtx 4090"]), gpu_profile("rtx_3090", ["rtx 3090"])]
    ingestor = make_ingestor(make_config(profiles, max_concurrency=2, poll_timeout_seconds=0.6), http)

    async def search(url: URL, **kwargs: Any) -> CallbackResult:
        if url.query["q"] == "rtx 3090":
            await asyncio.sleep(30)  # hung upstream: must be cut by the poll's own deadline
        return CallbackResult(payload=fixture())

    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, callback=search, repeat=True)
        fresh = await ingestor.run_once()
    assert len(fresh) == 4
    queries = ingestor.ctx.metrics.counter("ebay_queries_total", "", ("profile", "outcome"))
    assert queries.value(profile="rtx_3090", outcome="timeout") == 1
    assert ingestor.ledger.used() == 2  # the abandoned in-flight request still counts against the quota


def test_non_finite_amounts_are_rejected() -> None:
    base = {"itemId": "v1|1|0", "title": "RTX 4090", "itemWebUrl": "https://www.ebay.com/itm/1"}
    assert parse_item_summary({**base, "price": {"value": "1e400", "currency": "USD"}}) is None
    assert parse_item_summary({**base, "price": {"value": "NaN", "currency": "USD"}}) is None
    shipping = [{"shippingCostType": "FIXED", "shippingCost": {"value": "1e999", "currency": "USD"}}]
    raw = parse_item_summary({**base, "price": {"value": "100.00", "currency": "USD"}, "shippingOptions": shipping})
    assert raw is not None and raw.shipping is None
    assert RawListing.model_validate_json(raw.model_dump_json()) == raw


def test_buying_options_are_not_invented_when_absent() -> None:
    raw = parse_item_summary(
        {"itemId": "v1|1|0", "title": "RTX 4090", "itemWebUrl": "https://www.ebay.com/itm/1", "price": {"value": "1000.00"}}
    )
    assert raw is not None and "buying_options" not in raw.extra


def test_auction_with_bin_whose_price_is_only_the_current_bid_is_skipped() -> None:
    item = {
        "itemId": "v1|7|0",
        "title": "RTX 4090 auction",
        "itemWebUrl": "https://www.ebay.com/itm/7",
        "buyingOptions": ["AUCTION", "FIXED_PRICE"],
        "price": {"value": "900.00", "currency": "USD"},
        "currentBidPrice": {"value": "900.00", "currency": "USD"},
        "bidCount": 0,
    }
    assert parse_item_summary(item) is None  # eBay requires BIN > start price, so this price is the bid
    page = parse_search_page({"itemSummaries": [item]})
    assert page.skipped == {"auction_bid_price": 1}


async def test_token_endpoint_wall_vs_json_access_denied(http: HttpClient) -> None:
    access_denied = json.dumps(ebay_error(1100, "Access denied", domain="ACCESS"))
    assert not looks_blocked(access_denied)  # eBay's own JSON 403 also says "Access denied"
    assert looks_blocked(AKAMAI_403) and looks_blocked("<html><body>Pardon Our Interruption</body></html>")
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("cid"), SecretStr("sec"), clock=FakeClock())
    with aioresponses() as m:
        m.post(TOKEN_URL, status=403, body=access_denied, content_type="application/json")
        with pytest.raises(SourceError) as excinfo:
            await tm.get_token()
    assert not isinstance(excinfo.value, SourceBlocked)

    ingestor = make_ingestor(make_config([gpu_profile(f"gpu_{i}", [f"gpu {i}"]) for i in range(3)]), http)
    with aioresponses() as m:
        m.post(TOKEN_URL, status=403, body=AKAMAI_403, content_type="text/html", repeat=True)
        with pytest.raises(SourceBlocked):
            await ingestor.poll()
    assert len(requests_for(m, "POST")) == 1 and not requests_for(m, "GET")


def test_decode_search_body_distinguishes_walls_from_bad_replies() -> None:
    assert decode_search_body(b'{"total": 0, "limit": 50}') == {"total": 0, "limit": 50}  # no itemSummaries = no hits
    with pytest.raises(SourceBlocked):
        decode_search_body(b"<html><title>Pardon Our Interruption...</title></html>", {"Content-Type": "text/html"})
    for body in (b"<html><body>502 proxy error</body></html>", b"null", b"[1, 2]", b"   ", b'{"itemSummaries": [tr'):
        with pytest.raises(EbayResponseError):
            decode_search_body(body, {"Content-Type": "text/html"})


async def test_cancelled_poll_leaves_no_orphan_tasks_and_books_the_calls(http: HttpClient) -> None:
    profiles = [gpu_profile(f"gpu_{i}", [f"gpu {i}"]) for i in range(4)]
    ingestor = make_ingestor(make_config(profiles, max_concurrency=2), http)
    in_flight = asyncio.Event()

    async def hang(url: URL, **kwargs: Any) -> CallbackResult:
        in_flight.set()
        await asyncio.sleep(30)
        return CallbackResult(payload=fixture())

    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, callback=hang, repeat=True)
        poll = asyncio.create_task(ingestor.poll())
        await asyncio.wait_for(in_flight.wait(), 5)
        await asyncio.sleep(0.01)
        poll.cancel()
        with pytest.raises(asyncio.CancelledError):
            await poll
    leftovers = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    assert leftovers == []
    assert ingestor.ledger.used() == 2  # the two in-flight searches are booked against the quota


async def test_unicode_query_is_utf8_percent_encoded_on_the_wire() -> None:
    captured: dict[str, str] = {}

    async def token(request: web.Request) -> web.Response:
        return web.json_response(token_payload("t"))

    async def search(request: web.Request) -> web.Response:
        captured["raw"] = request.rel_url.raw_query_string
        captured["q"] = request.query["q"]
        return web.json_response({"total": 0, "limit": 8, "offset": 0})

    app = web.Application()
    app.router.add_post(TOKEN_PATH, token)
    app.router.add_get(SEARCH_PATH, search)
    term = "Café RTX 4090 – 24GB ＲＴＸ"
    async with TestServer(app) as server:
        client = make_http(trust_env=False)
        try:
            endpoints = EbayEndpoints(str(server.make_url("/")).rstrip("/"))
            ingestor = make_ingestor(make_config([gpu_profile("rtx_4090", [term])], limit=8), client, endpoints=endpoints)
            assert await ingestor.poll() == []  # a JSON object without itemSummaries is a valid empty result
        finally:
            await client.close()
    assert captured["q"] == term
    assert "q=Caf%C3%A9%20RTX%204090%20%E2%80%93%2024GB%20%EF%BC%B2%EF%BC%B4%EF%BC%B8" in captured["raw"]


def _redirect_loop(url: str, authorization: str) -> aiohttp.TooManyRedirects:
    headers = CIMultiDictProxy(CIMultiDict({"Authorization": authorization}))
    return aiohttp.TooManyRedirects(aiohttp.RequestInfo(URL(url), "GET", headers, URL(url)), (), status=302, message="Found")


async def test_transport_errors_never_leak_credentials(http: HttpClient) -> None:
    basic = "Basic " + base64.b64encode(b"test-client-id:test-client-secret").decode()
    tm = EbayTokenManager(http, TOKEN_URL, SecretStr("test-client-id"), SecretStr("test-client-secret"), clock=FakeClock())
    with aioresponses() as m:
        m.post(TOKEN_URL, exception=_redirect_loop(TOKEN_URL, basic))
        with pytest.raises(SourceError) as excinfo:
            await tm.get_token()
    assert basic.split()[1] not in str(excinfo.value) and "test-client-secret" not in str(excinfo.value)

    ingestor = make_ingestor(make_config(), http)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("v^1.1#i^1#secret-app-token"))
        m.get(SEARCH_RE, exception=_redirect_loop(API + SEARCH_PATH, "Bearer v^1.1#i^1#secret-app-token"))
        with pytest.raises(SourceError) as excinfo:
            await ingestor.poll()
    assert "secret-app-token" not in str(excinfo.value)
    assert "secret-app-token" not in (ingestor.health.last_error or "")


async def test_large_pages_are_parsed_off_the_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    # A real server: aioresponses cannot feed bodies above aiohttp's stream high-water mark.
    import deal_radar.sources.ebay_api as ebay_api

    threads: list[int] = []
    real_parse = ebay_api.parse_search_page

    def spy(*args: Any, **kwargs: Any) -> Any:
        threads.append(threading.get_ident())
        return real_parse(*args, **kwargs)

    monkeypatch.setattr(ebay_api, "parse_search_page", spy)
    valid = [item for item in fixture()["itemSummaries"] if isinstance(item, dict) and "price" in item and "itemId" in item]
    big = {"total": 900, "limit": 200, "itemSummaries": []}
    for n in range(200):
        big["itemSummaries"].append(dict(valid[n % len(valid)], itemId=f"v1|{900000 + n}|0"))

    async def token(request: web.Request) -> web.Response:
        return web.json_response(token_payload("t"))

    async def search(request: web.Request) -> web.Response:
        return web.json_response(big if request.query["q"] == "rtx 4090" else fixture())

    app = web.Application()
    app.router.add_post(TOKEN_PATH, token)
    app.router.add_get(SEARCH_PATH, search)
    profiles = [gpu_profile("rtx_4090", ["rtx 4090"]), gpu_profile("rtx_3090", ["rtx 3090"])]
    async with TestServer(app) as server:
        client = make_http(trust_env=False)
        try:
            endpoints = EbayEndpoints(str(server.make_url("/")).rstrip("/"))
            ingestor = make_ingestor(make_config(profiles, max_concurrency=1, limit=200), client, endpoints=endpoints)
            listings = await ingestor.poll()
        finally:
            await client.close()
    assert len(listings) == 160 + 4  # 4 of every 5 clones are buyable (one is AUCTION-only); the small page adds 4
    loop_thread = threading.get_ident()
    assert sorted(t == loop_thread for t in threads) == [False, True]  # big page in a worker, small page inline


class _BrokenRedis:
    """Every command fails, like a Redis that went away mid-run."""

    def __getattr__(self, name: str) -> Any:
        def fail(*args: Any, **kwargs: Any) -> Any:
            raise ConnectionError(f"redis down ({name})")

        return fail


async def test_redis_outage_degrades_to_local_token_and_ledger(http: HttpClient) -> None:
    ingestor = make_ingestor(make_config(), http, redis=_BrokenRedis())
    ingestor.ledger = QuotaLedger(5000, redis=_BrokenRedis(), clock=lambda: NOON_PDT)
    with aioresponses() as m:
        m.post(TOKEN_URL, payload=token_payload("tok"))
        m.get(SEARCH_RE, payload=fixture())
        assert len(await ingestor.poll()) == 4
    assert ingestor.ledger.used() == 1


async def test_budget_guard_sees_fleet_usage_in_redis_before_the_first_poll(http: HttpClient) -> None:
    redis = fakeredis.FakeAsyncRedis()
    config = make_config(_profiles_with_terms(3), daily_call_budget=5000)
    ingestor = make_ingestor(config, http, redis=redis)
    ingestor.ledger = QuotaLedger(5000, redis=redis, key_prefix="dr:ebay:calls:k:", clock=lambda: NOON_PDT)
    await redis.set("dr:ebay:calls:k:2026-10-06", 4998)  # other nodes spent the day's budget
    with aioresponses() as m:
        assert await ingestor.poll() == []
    assert not m.requests
