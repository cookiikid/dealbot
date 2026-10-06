"""Tests for the Reddit ingestor (r/buildapcsales deals + r/hardwareswap swap posts).

Network I/O is served by a local ``aiohttp`` :class:`TestServer` that impersonates the
three Reddit endpoints (token, ``oauth.reddit.com`` and public ``www.reddit.com``
JSON); the ingestor is pointed at it through :class:`RedditEndpoints`. (``aioresponses``
0.7.x cannot build responses for aiohttp 3.14, whose ``ClientResponse`` requires a
``stream_writer``.) Listing payloads are recorded-style ``/r/{sub}/new`` responses
stored under ``tests/fixtures/reddit/``.
"""

from __future__ import annotations

import base64
import copy
import json
import random
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from deal_radar.config_schema import AppConfig, RedditSource, SubredditSpec, load_config
from deal_radar.core.backoff import BackoffPolicy
from deal_radar.core.http import HttpClient, NetworkSettings
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import SourceKind
from deal_radar.sources.base import IngestorContext, SourceAuthError, SourceBlocked, SourceError
from deal_radar.sources.reddit_stream import (
    BLOCK_COOLDOWN_SECONDS,
    INSTALLED_CLIENT_GRANT,
    OAUTH_BASE,
    PUBLIC_BASE,
    TOKEN_REFRESH_MARGIN_SECONDS,
    TOKEN_URL,
    UNAUTH_BLOCK_COOLDOWN_SECONDS,
    PostSkipped,
    RateLimitState,
    RedditEndpoints,
    RedditIngestor,
    build_user_agent,
    find_money,
    iter_posts,
    location_allowed,
    parse_deal_post,
    parse_deal_title,
    parse_listing,
    parse_location_tag,
    parse_swap_post,
    parse_swap_title,
    payment_methods,
    registrable_domain,
    retailer_for_url,
    swap_intent,
)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "reddit"
SKIP_FLAIRS = ["Expired", "Closed", "Buying", "BUYING", "CLOSED"]

TOKEN_PATH = "/api/v1/access_token"
PUBLIC_BAPCS = "/www/r/buildapcsales/new.json"
PUBLIC_HWS = "/www/r/hardwareswap/new.json"
OAUTH_BAPCS = "/oauth/r/buildapcsales/new"
OAUTH_HWS = "/oauth/r/hardwareswap/new"
OAUTH_ENV = {"REDDIT_CLIENT_ID": "cid-123", "REDDIT_CLIENT_SECRET": "s3cr3t", "REDDIT_USERNAME": "radar_bot"}
RL_OK = {"x-ratelimit-used": "4", "x-ratelimit-remaining": "996.0", "x-ratelimit-reset": "412"}

# Body of the real 403 page Reddit serves to blocked (datacenter) clients.
BLOCK_PAGE = (
    "<!DOCTYPE html><html><head><title>Blocked</title></head><body class=theme-beta><div>"
    "You've been blocked by network security. If you think you've been blocked by mistake, "
    "file a ticket below and we'll look into it.</div></body></html>"
)


# --------------------------------------------------------------------------- fixtures & fake server


def fixture(name: str, *, newest_age_s: float | None = None) -> dict[str, Any]:
    """Load a recorded listing; optionally shift timestamps so the newest post is ``newest_age_s`` old."""
    payload = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
    if newest_age_s is not None:
        children = payload["data"]["children"]
        newest = max(c["data"]["created_utc"] for c in children if not c["data"].get("stickied"))
        shift = (time.time() - newest_age_s) - newest
        for child in children:
            child["data"]["created_utc"] += shift
            child["data"]["created"] += shift
    return payload


def post(name: str, post_id: str) -> dict[str, Any]:
    for child in fixture(name)["data"]["children"]:
        if child["data"]["id"] == post_id:
            return copy.deepcopy(child["data"])
    raise KeyError(post_id)


@dataclass
class Seen:
    method: str
    path: str
    query: dict[str, str]
    headers: dict[str, str]
    form: dict[str, str]


Responder = Callable[[Seen], web.StreamResponse]


def reply(
    status: int = 200,
    payload: Any = None,
    *,
    body: str | bytes | None = None,
    content_type: str = "application/json",
    headers: dict[str, str] | None = None,
) -> Responder:
    def _make(_seen: Seen) -> web.StreamResponse:
        if payload is not None:
            return web.json_response(payload, status=status, headers=headers)
        if isinstance(body, bytes):
            return web.Response(status=status, body=body, content_type=content_type, headers=headers)
        return web.Response(status=status, text=body or "", content_type=content_type, headers=headers)

    return _make


class FakeReddit:
    """Records every request; serves queued responders per (method, path), repeating the last one."""

    def __init__(self) -> None:
        self.seen: list[Seen] = []
        self._routes: dict[tuple[str, str], list[Responder]] = {}
        self.base = ""

    def reset(self) -> None:
        self.seen.clear()
        self._routes.clear()

    def on(self, method: str, path: str, *responders: Responder) -> None:
        self._routes.setdefault((method, path), []).extend(responders)

    def calls(self, method: str, path: str) -> list[Seen]:
        return [s for s in self.seen if s.method == method and s.path == path]

    @property
    def endpoints(self) -> RedditEndpoints:
        return RedditEndpoints(token_url=self.base + TOKEN_PATH, oauth_base=self.base + "/oauth", public_base=self.base + "/www")

    async def handle(self, request: web.Request) -> web.StreamResponse:
        form = {k: str(v) for k, v in (await request.post()).items()} if request.method == "POST" else {}
        seen = Seen(request.method, request.path, dict(request.query), dict(request.headers), form)
        self.seen.append(seen)
        queue = self._routes.get((request.method, request.path))
        if not queue:
            return web.Response(status=418, text=f"unrouted {request.method} {request.path}")
        responder = queue.pop(0) if len(queue) > 1 else queue[0]
        return responder(seen)


@pytest.fixture
async def reddit() -> AsyncIterator[FakeReddit]:
    fake = FakeReddit()
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", fake.handle)
    server = TestServer(app)
    await server.start_server()
    fake.base = str(server.make_url("")).rstrip("/")
    yield fake
    await server.close()


@pytest.fixture
async def http() -> AsyncIterator[HttpClient]:
    client = HttpClient.create(
        NetworkSettings(trust_env=False, retry=BackoffPolicy(max_attempts=2, base_delay=0, max_delay=0))
    )
    yield client
    await client.close()


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def make_ingestor(
    http: HttpClient,
    fake: FakeReddit | None = None,
    *,
    env: dict[str, str] | None = None,
    clock: Callable[[], float] | None = None,
    **overrides: Any,
) -> RedditIngestor:
    config: AppConfig = load_config(CONFIG_PATH, env=env or {})
    cfg = config.sources.reddit
    if overrides:
        cfg = RedditSource.model_validate({**cfg.model_dump(), **overrides})
    ctx = IngestorContext(http=http, metrics=Metrics(), config=config, node_id="test", rng=random.Random(7))
    kwargs: dict[str, Any] = {}
    if clock is not None:
        kwargs["clock"] = clock
    if fake is not None:
        kwargs["endpoints"] = fake.endpoints
    return RedditIngestor(cfg, ctx, **kwargs)


def token_payload(token: str = "tok-1", expires_in: int = 86400) -> dict[str, Any]:
    return {"access_token": token, "token_type": "bearer", "expires_in": expires_in, "scope": "*"}


def listing_reply(name: str, *, headers: dict[str, str] | None = None, newest_age_s: float | None = None) -> Responder:
    return reply(200, fixture(name, newest_age_s=newest_age_s), headers=headers)


# --------------------------------------------------------------------------- pure parsing: money & titles


def test_find_money_formats() -> None:
    values = [s.value for s in find_money("$1,199.99 or $ 549, 450$ shipped, $1.2k, $500, $480")]
    assert values == [1199.99, 549.0, 450.0, 1200.0, 500.0, 480.0]
    assert find_money("no prices here, DDR5-6000 CL30") == []


@pytest.mark.parametrize(
    ("title", "category", "price", "list_price"),
    [
        ("[GPU] ASUS TUF Gaming GeForce RTX 4090 OC 24GB - $1199.99 ($1599.99 - $400)", "GPU", 1199.99, 1599.99),
        ("[GPU] ASUS TUF RTX 4090 OC - $1199 ($1599 - $400)", "GPU", 1199.0, 1599.0),
        ("[SSD] Samsung 990 Pro 2TB - $149.99 - Amazon", "SSD", 149.99, None),
        ('[Monitor] Dell S2721DGF - 27" 1440p 165Hz - $279.99', "Monitor", 279.99, None),
        ("[GPU] Zotac RTX 4070 Twin Edge - $499.99 - $50 MIR", "GPU", 499.99, None),
        ("[RAM] G.Skill 64GB (2x32GB) DDR5-6000 - $1,049.50 (was $1,299.00)", "RAM", 1049.5, 1299.0),
        ("[Monitor] LG 27GP850-B - $279.99 (reg $399.99)", "Monitor", 279.99, 399.99),
        ("[CPU] Ryzen 5 7600 -$179 ($229-$50)", "CPU", 179.0, 229.0),
        ("[PSU] Corsair RM850x - $99.99 ($139.99 - $20 code - $20 MIR)", "PSU", 99.99, 139.99),
        ("[Case] Lian Li O11 Dynamic $129.99 shipped", "Case", 129.99, None),
        ("[Prebuilt] Some Gaming PC - $1.2k", "Prebuilt", 1200.0, None),
        ("[Cooler] Thermalright PA120 - $35.90 ($5 off coupon)", "Cooler", 35.9, None),
        ("[Game] Free on Epic - Some Game", "Game", None, None),
        # Live r/buildapcsales titles (2026-10):
        ("[PSU] $109.99 - Corsair RM1000e (2025) 1000W ATX 3.1", "PSU", 109.99, None),
        ("[Prebuilt] Acer Nitro Desktop ($1700), AMD Ryzen 9 9900X, RTX 5070 Ti", "Prebuilt", 1700.0, None),
        ("[GPU] ASUS Prime Gaming Radeon RX 9070 XT OC Edition - $699.99 (829.99-130) - Central Computers In-store",
         "GPU", 699.99, 829.99),
        ("[Laptop] HP ZBook Fury G1i 18 (Ultra 9 285HX, RTX PRO 5000) $8999( $26794-$17795)", "Laptop", 8999.0, 26794.0),
        ("[PSU] MSI MPG A1000GS | $94.99 (w/ code: FTTF7873) - $10 Rebate = $84.99 AR (Newegg)", "PSU", 84.99, None),
        ('[TV] LG 48" B6 OLED 4K 120Hz Smart TV - $699.99 @ Best Buy', "TV", 699.99, None),
        ("[GPU] PNY RTX 5070 - $299 - $30MIR = $269", "GPU", 269.0, None),
        ("[GPU] Gigabyte RTX 5080 - $1199 ($1599 - $400 = $1199)", "GPU", 1199.0, 1599.0),
        ("[SSD] WD SN850X 4TB - $249.99 after rebate (reg $329.99)", "SSD", 249.99, 329.99),
        ("No tag RTX 4080 Super - $899", None, 899.0, None),
    ],
)
def test_parse_deal_title(title: str, category: str | None, price: float | None, list_price: float | None) -> None:
    parsed = parse_deal_title(title)
    assert parsed.category == category
    assert parsed.price == price
    assert parsed.list_price == list_price


@pytest.mark.parametrize(
    ("url", "retailer"),
    [
        ("https://www.bestbuy.com/site/x/6524436.p?skuId=6524436", "Best Buy"),
        ("https://www.newegg.com/p/N82E16814137771", "Newegg"),
        ("https://smile.amazon.com/dp/B0BG94PS2F", "Amazon"),
        ("https://www.amazon.co.uk/dp/B0BG94PS2F", "Amazon"),
        ("https://amzn.to/3xYzAbC", "Amazon"),
        ("https://www.microcenter.com/product/659253/x", "Micro Center"),
        ("https://www.bhphotovideo.com/c/product/1234-REG/x.html", "B&H"),
        ("https://www.walmart.com/ip/123", "Walmart"),
        ("https://www.target.com/p/x/-/A-1", "Target"),
        ("https://www.ebay.com/itm/1234567890", "eBay"),
        ("https://www.adorama.com/x.html", "adorama.com"),
        ("https://shop.example.co.uk/item", "example.co.uk"),
        (None, None),
        ("not a url", None),
    ],
)
def test_retailer_for_url(url: str | None, retailer: str | None) -> None:
    assert retailer_for_url(url) == retailer


def test_registrable_domain() -> None:
    assert registrable_domain("www.bestbuy.com") == "bestbuy.com"
    assert registrable_domain("deals.store.example.com.au") == "example.com.au"
    assert registrable_domain("localhost") == "localhost"


# --------------------------------------------------------------------------- pure parsing: buildapcsales


def test_parse_buildapcsales_listing() -> None:
    outcome = parse_listing(fixture("buildapcsales_new.json"), subreddit="buildapcsales", mode="deals", skip_flairs=SKIP_FLAIRS)
    assert outcome.errors == 0
    assert outcome.skipped == {"stickied": 1, "expired": 1, "removed": 1}
    by_id = {raw.source_id: raw for raw in outcome.listings}
    assert list(by_id) == ["t3_1nyk2gq", "t3_1nyjz7d", "t3_1nygq2c", "t3_1nyfm8k"]

    gpu = by_id["t3_1nyk2gq"]
    assert gpu.source == "reddit"
    assert gpu.source_kind is SourceKind.AGGREGATOR
    assert gpu.url.startswith("https://www.reddit.com/r/buildapcsales/comments/1nyk2gq/")
    assert gpu.title == "[GPU] ASUS TUF Gaming GeForce RTX 4090 OC 24GB - $1199.99 ($1599.99 - $400)"
    assert gpu.price == 1199.99
    assert gpu.list_price == 1599.99
    assert gpu.currency == "USD"
    assert gpu.condition is None  # left to the normalizer
    assert gpu.retailer == "Best Buy"
    assert gpu.outbound_url == (
        "https://www.bestbuy.com/site/asus-tuf-gaming-nvidia-geforce-rtx-4090-oc-edition-24gb-gddr6x/6524436.p?skuId=6524436"
    )
    assert gpu.image_urls == ["https://external-preview.redd.it/aSuS4090tUf.jpeg?auto=webp&s=8f2a1c0d9e"]
    assert gpu.posted_at == datetime.fromtimestamp(post("buildapcsales_new.json", "1nyk2gq")["created_utc"], tz=timezone.utc)
    assert gpu.posted_at.tzinfo is not None
    assert gpu.description == ""
    assert gpu.extra["category_tag"] == "GPU"
    assert gpu.extra["mode"] == "deals"
    assert gpu.extra["subreddit"] == "buildapcsales"
    assert gpu.extra["domain"] == "bestbuy.com"
    assert gpu.listing_key == "reddit:t3_1nyk2gq"

    monitor = by_id["t3_1nyjz7d"]  # self post: retailer link lives in the body
    assert monitor.extra["is_self"] is True
    assert monitor.outbound_url == "https://www.amazon.com/dp/B093MBYSTQ?th=1&tag=foo-20"
    assert monitor.retailer == "Amazon"
    assert monitor.description.startswith("Lightning deal on Amazon")
    assert monitor.price == 279.99 and monitor.list_price == 399.99
    assert monitor.image_urls == []

    cpu = by_id["t3_1nygq2c"]
    assert cpu.retailer == "Micro Center"
    assert cpu.price == 339.99
    assert "Open Box" in cpu.title and cpu.condition is None

    ram = by_id["t3_1nyfm8k"]
    assert ram.retailer == "Amazon" and ram.outbound_url == "https://amzn.to/3xYzAbC"


def test_deal_post_serialises_for_the_bus() -> None:
    raw = parse_deal_post(post("buildapcsales_new.json", "1nyk2gq"), subreddit="buildapcsales")
    assert json.loads(raw.model_dump_json())["extra"]["category_tag"] == "GPU"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ({"stickied": True}, "stickied"),
        ({"pinned": True}, "stickied"),
        ({"removed_by_category": "moderator"}, "removed"),
        ({"removed_by_category": "deleted"}, "removed"),
        ({"over_18": True}, "nsfw"),
        ({"link_flair_text": "Expired: Price Increased"}, "expired"),
        ({"link_flair_text": "Expired :table_flip:", "link_flair_css_class": "expired"}, "expired"),
        ({"title": "[ Removed by moderator ]"}, "removed"),
        ({"is_self": True, "selftext": "[removed]"}, "removed"),
        ({"link_flair_text": None, "link_flair_css_class": "expired"}, "expired"),
        ({"link_flair_text": "Out of Stock"}, "flair"),
    ],
)
def test_deal_post_skip_rules(mutation: dict[str, Any], reason: str) -> None:
    data = post("buildapcsales_new.json", "1nyk2gq")
    data.update(mutation)
    with pytest.raises(PostSkipped) as info:
        parse_deal_post(data, subreddit="buildapcsales", skip_flairs=["Out of Stock"])
    assert info.value.reason == reason


def test_deal_link_to_reddit_or_imgur_is_not_outbound() -> None:
    data = post("buildapcsales_new.json", "1nyk2gq")
    data.update(url="https://www.reddit.com/gallery/1nyk2gq", url_overridden_by_dest="https://www.reddit.com/gallery/1nyk2gq",
                domain="reddit.com", preview=None)
    data["media_metadata"] = {
        "b": {"status": "valid", "e": "Image", "s": {"u": "https://preview.redd.it/b.jpg?width=640&s=2"}},
        "a": {"status": "valid", "e": "Image", "s": {"u": "https://preview.redd.it/a.jpg?width=640&s=1"}},
        "v": {"status": "valid", "e": "RedditVideo", "dashUrl": "https://v.redd.it/x/DASHPlaylist.mpd"},
    }
    data["gallery_data"] = {"items": [{"media_id": "a", "id": 1}, {"media_id": "b", "id": 2}]}
    raw = parse_deal_post(data, subreddit="buildapcsales")
    assert raw.outbound_url is None and raw.retailer is None
    assert raw.image_urls == ["https://preview.redd.it/a.jpg?width=640&s=1", "https://preview.redd.it/b.jpg?width=640&s=2"]

    data.update(url="https://i.imgur.com/Zz9Yy8x.jpg", url_overridden_by_dest="https://i.imgur.com/Zz9Yy8x.jpg")
    raw = parse_deal_post(data, subreddit="buildapcsales")
    assert raw.outbound_url is None
    assert raw.image_urls[0] == "https://i.imgur.com/Zz9Yy8x.jpg"


def test_malformed_posts_are_counted_not_fatal() -> None:
    good = {"kind": "t3", "data": post("buildapcsales_new.json", "1nyk2gq")}
    no_title = {"kind": "t3", "data": {**post("buildapcsales_new.json", "1nyfm8k"), "title": None}}
    payload = {
        "kind": "Listing",
        "data": {"after": None, "children": [{"kind": "t3", "data": None}, no_title, {"kind": "t1", "data": {}}, good, "junk"]},
    }
    outcome = parse_listing(payload, subreddit="buildapcsales")
    assert [raw.source_id for raw in outcome.listings] == ["t3_1nyk2gq"]
    assert outcome.errors == 3


@pytest.mark.parametrize("payload", [None, [], {"kind": "Listing", "data": {}}, {"kind": "t1", "data": {"children": []}}])
def test_bad_envelope_raises(payload: Any) -> None:
    with pytest.raises(SourceError):
        list(iter_posts(payload))


# --------------------------------------------------------------------------- pure parsing: hardwareswap


def test_parse_hardwareswap_listing() -> None:
    outcome = parse_listing(fixture("hardwareswap_new.json"), subreddit="hardwareswap", mode="swap", skip_flairs=SKIP_FLAIRS)
    assert outcome.errors == 0
    # BUYING/CLOSED flairs hit skip_flairs; [H] PayPal without flair is still recognised as buying.
    assert outcome.skipped == {"stickied": 1, "flair": 2, "trading": 1, "buying": 1, "removed": 1}
    by_id = {raw.source_id: raw for raw in outcome.listings}
    assert list(by_id) == ["t3_1nyk9a2", "t3_1nyk1e6", "t3_1nyjxg8"]

    first = by_id["t3_1nyk9a2"]
    assert first.source_kind is SourceKind.LOCAL
    assert first.title == "RTX 4090 Founders Edition, 32GB (2x16GB) G.Skill DDR5-6000"
    assert first.description.startswith("Timestamps: https://imgur.com/a/Xk3LmQp")
    assert first.price == 1550.0  # the struck-through ~~$1700~~ is ignored
    assert first.shipping == 0.0  # "$1550 shipped"
    assert first.currency == "USD"
    assert first.extra["multi_item"] is True
    assert first.extra["prices"] == [1550.0, 85.0]
    assert first.extra["timestamps_url"] == "https://imgur.com/a/Xk3LmQp"
    assert first.extra["location_tag"] == "USA-CA"
    assert first.extra["want"] == "PayPal, Local Cash"
    assert first.image_urls == ["https://i.imgur.com/AbC123x.jpeg"]
    assert first.seller is not None and first.seller.name == "pcbuilder_ca" and first.seller.feedback_score == 23
    assert first.location is not None
    assert (first.location.text, first.location.country, first.location.region) == ("USA-CA", "US", "CA")
    assert first.outbound_url is None and first.retailer is None
    assert first.url.startswith("https://www.reddit.com/r/hardwareswap/comments/1nyk9a2/")

    evga = by_id["t3_1nyk1e6"]
    assert evga.extra["multi_item"] is False
    assert evga.price == 420.0 and evga.shipping == 0.0
    assert evga.extra["timestamps_url"] == "https://imgur.com/gallery/evga-3080-timestamps-q8Zr2Lm"
    assert evga.seller is not None and evga.seller.feedback_score == 4

    cpu = by_id["t3_1nyjxg8"]  # inline image uploaded to the self post (media_metadata)
    assert cpu.image_urls == ["https://preview.redd.it/8x2kq1abcdz1.jpeg?width=4032&format=pjpg&auto=webp&s=0a1b2c"]
    assert "timestamps_url" not in cpu.extra
    assert cpu.price == 250.0


@pytest.mark.parametrize(
    ("locations", "expected"),
    [
        ([], {"t3_1nyk9a2", "t3_1nyk1e6", "t3_1nyjxg8"}),
        (["USA-TX"], {"t3_1nyk1e6"}),
        (["usa-ca", "USA-IL"], {"t3_1nyk9a2", "t3_1nyjxg8"}),
        (["USA"], {"t3_1nyk9a2", "t3_1nyk1e6", "t3_1nyjxg8"}),
        (["CAN"], set()),
    ],
)
def test_hardwareswap_location_filter(locations: list[str], expected: set[str]) -> None:
    outcome = parse_listing(
        fixture("hardwareswap_new.json"), subreddit="hardwareswap", mode="swap", skip_flairs=SKIP_FLAIRS, locations=locations
    )
    assert {raw.source_id for raw in outcome.listings} == expected
    if len(expected) < 3:
        assert outcome.skipped.get("location", 0) == 3 - len(expected)


@pytest.mark.parametrize(
    ("title", "flair", "intent"),
    [
        ("[USA-CA] [H] RTX 4090 FE [W] PayPal, Local Cash", "SELLING", "selling"),
        ("[USA-CA] [H] RTX 4090 FE [W] PayPal", None, "selling"),
        ("[USA-CA] [H] RTX 4090 FE [W] Venmo", None, "selling"),
        ("[USA-CA] [H] RTX 4090 FE [W] $$$", None, "selling"),
        ("[USA-CA][H]RTX 4090 FE[W]Zelle", None, "selling"),
        ("[USA-TX] [H] PayPal [W] RTX 4080 Super", None, "buying"),
        ("[USA-TX] [H] $$$ [W] RTX 4080 Super", None, "buying"),
        ("[USA-TX] [H] Local Cash, PayPal [W] Steam Deck", "SELLING", "buying"),
        ("[USA-NY] [H] RX 7900 XTX [W] RTX 4080, PayPal", "TRADING", "trading"),
        ("[USA-NY] [H] RX 7900 XTX [W] RTX 4080", None, "trading"),
        ("[USA-FL] [H] Ryzen 9 7950X [W] PayPal", "CLOSED", "closed"),
        ("[USA-FL] [H] Ryzen 9 7950X [W] PayPal", "Buying", "buying"),
    ],
)
def test_swap_intent(title: str, flair: str | None, intent: str) -> None:
    swap = parse_swap_title(title)
    assert swap is not None
    assert swap_intent(swap, flair) == intent


def test_parse_swap_title_rejects_other_formats() -> None:
    assert parse_swap_title("Confirmed Trade Thread - October 2026") is None
    assert parse_swap_title("[META] Rules update [H] nothing") is None
    swap = parse_swap_title("[USA-CA] [H] RTX 4090 FE, [W] PayPal ")
    assert swap is not None and swap.have == "RTX 4090 FE" and swap.want == "PayPal"


@pytest.mark.parametrize(
    ("tag", "country", "region", "city", "postal"),
    [
        ("USA-CA", "US", "CA", None, None),
        ("USA-NY-NYC", "US", "NY", "NYC", None),
        ("USA-CA, San Diego", "US", "CA", "San Diego", None),
        ("USA-CA-94043", "US", "CA", None, "94043"),
        ("CAN-ON", "CA", "ON", None, None),
        ("EU-DE", None, None, None, None),
    ],
)
def test_parse_location_tag(tag: str, country: str | None, region: str | None, city: str | None, postal: str | None) -> None:
    loc = parse_location_tag(tag)
    assert (loc.text, loc.country, loc.region, loc.city, loc.postal_code) == (tag, country, region, city, postal)


def test_location_allowed_prefix_match() -> None:
    assert location_allowed("USA-TX", [])
    assert location_allowed("USA - TX", ["usa-tx"])
    assert location_allowed("USA-TX-Austin", ["USA-TX"])
    assert not location_allowed("USA-TN", ["USA-TX"])
    assert not location_allowed("USA-TX", ["   "])


def test_swap_post_price_fallbacks_and_currency() -> None:
    data = post("hardwareswap_new.json", "1nyk1e6")
    data.update(
        title="[CAN-ON] [H] Steam Deck OLED 1TB [W] PayPal, Local Cash",
        selftext="Mint condition, comes with case. Asking 650$ + shipping. https://imgur.com/Ab12Cd3 for timestamps",
        author_flair_text=None,
    )
    raw = parse_swap_post(data, subreddit="hardwareswap")
    assert raw.price == 650.0 and raw.shipping is None
    assert raw.currency == "CAD"
    assert raw.seller is not None and raw.seller.feedback_score is None
    assert raw.extra["timestamps_url"] == "https://imgur.com/Ab12Cd3"

    data.update(title="[USA-CA] [H] RTX 3070 FE $300 shipped [W] PayPal", selftext="See title. ~~$350~~")
    raw = parse_swap_post(data, subreddit="hardwareswap")
    assert raw.price == 300.0 and raw.shipping == 0.0 and raw.currency == "USD"

    data.update(selftext="Make me an offer.", title="[USA-CA] [H] RTX 3070 FE [W] PayPal")
    raw = parse_swap_post(data, subreddit="hardwareswap")
    assert raw.price is None and raw.extra["prices"] == []


def test_swap_post_skips() -> None:
    data = post("hardwareswap_new.json", "1nyk1e6")
    data["author"] = "[deleted]"
    with pytest.raises(PostSkipped) as info:
        parse_swap_post(data, subreddit="hardwareswap")
    assert info.value.reason == "deleted"
    data = post("hardwareswap_new.json", "1nyk1e6")
    data["title"] = "[META] Please read the rules"
    with pytest.raises(PostSkipped) as info:
        parse_swap_post(data, subreddit="hardwareswap")
    assert info.value.reason == "unparseable_title"


def test_user_agent_follows_reddit_rules() -> None:
    assert build_user_agent(RedditSource()) == "python:dealradar:1.0 (by /u/dealradar)"
    assert build_user_agent(RedditSource(username="radar_bot")) == "python:dealradar:1.0 (by /u/radar_bot)"
    assert build_user_agent(RedditSource(user_agent="linux:my.app:2.0 (by /u/me)")) == "linux:my.app:2.0 (by /u/me)"


def test_deal_category_falls_back_to_link_flair() -> None:
    data = post("buildapcsales_new.json", "1nyk2gq")
    data.update(title="ASUS TUF RTX 4090 OC - $1199.99", link_flair_text="GPU", link_flair_css_class="gpu")
    raw = parse_deal_post(data, subreddit="buildapcsales")
    assert raw.extra["category_tag"] == "GPU" and raw.price == 1199.99


def test_swap_sold_markers_are_not_prices() -> None:
    data = post("hardwareswap_new.json", "1nyk9a2")
    data["selftext"] = (
        "Timestamps: https://imgur.com/a/Xk3LmQp\n\n| Item | Price |\n|:-|:-|\n"
        "| RTX 4090 FE | [$1600 Sold for $1550 to /u/buyer123] |\n"
        "| G.Skill 32GB DDR5-6000 | $85 shipped |\n"
    )
    raw = parse_swap_post(data, subreddit="hardwareswap")
    assert raw.price == 85.0 and raw.shipping == 0.0
    assert raw.extra["prices"] == [85.0]


def test_swap_payment_methods_and_red_flags() -> None:
    raw = parse_listing(fixture("hardwareswap_new.json"), subreddit="hardwareswap", mode="swap", skip_flairs=SKIP_FLAIRS)
    by_id = {r.source_id: r for r in raw.listings}
    assert by_id["t3_1nyk9a2"].extra["payment_methods"] == ["paypal", "cash"]
    assert by_id["t3_1nyk9a2"].extra["payment_red_flags"] == []
    assert by_id["t3_1nyk1e6"].extra["payment_methods"] == ["paypal", "cash", "zelle"]
    assert by_id["t3_1nyk1e6"].extra["payment_red_flags"] == ["zelle"]
    assert payment_methods("Venmo, CashApp, PayPal F&F, BTC") == ["paypal", "venmo", "cashapp", "friends_family", "crypto"]


def test_multi_state_location_tags() -> None:
    assert location_allowed("USA-NY, NJ", ["USA-NJ"])
    assert location_allowed("USA-NY, NJ", ["USA-NY"])
    assert not location_allowed("USA-NY, NJ", ["USA-CT"])
    loc = parse_location_tag("USA-NY, NJ")
    assert (loc.country, loc.region, loc.city) == ("US", "NY", None)


# --------------------------------------------------------------------------- polling: unauthenticated


async def test_poll_unauthenticated_reads_both_subreddits(
    http: HttpClient, reddit: FakeReddit, caplog: pytest.LogCaptureFixture
) -> None:
    ing = make_ingestor(http, reddit)
    assert not ing.authenticated
    reddit.on("GET", PUBLIC_BAPCS, listing_reply("buildapcsales_new.json"))
    reddit.on("GET", PUBLIC_HWS, listing_reply("hardwareswap_new.json"))
    with caplog.at_level("WARNING", logger="deal_radar.sources.reddit"):
        listings = await ing.poll()
        await ing.setup()  # the unauthenticated warning is emitted once only
    assert len(listings) == 7
    kinds = {raw.source_id: raw.source_kind for raw in listings}
    assert kinds["t3_1nyk2gq"] is SourceKind.AGGREGATOR
    assert kinds["t3_1nyk9a2"] is SourceKind.LOCAL
    assert {raw.query for raw in listings} == {"r/buildapcsales", "r/hardwareswap"}
    assert sorted(s.path for s in reddit.seen) == sorted([PUBLIC_BAPCS, PUBLIC_HWS])
    for seen in reddit.seen:
        assert seen.query == {"limit": "25", "raw_json": "1"}
        assert seen.headers["User-Agent"] == "python:dealradar:1.0 (by /u/dealradar)"
        assert "Mozilla" not in seen.headers["User-Agent"] and "sec-ch-ua" not in seen.headers
        assert "Authorization" not in seen.headers
        assert seen.headers["Accept"] == "application/json"
    warnings = [r for r in caplog.records if "OAuth credentials not configured" in r.getMessage()]
    assert len(warnings) == 1


def test_default_endpoints_are_reddit() -> None:
    assert RedditEndpoints() == RedditEndpoints(TOKEN_URL, OAUTH_BASE, PUBLIC_BASE)
    assert TOKEN_URL == "https://www.reddit.com/api/v1/access_token"
    assert OAUTH_BASE == "https://oauth.reddit.com" and PUBLIC_BASE == "https://www.reddit.com"


async def test_unauthenticated_403_block_page_raises_source_blocked(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit)
    reddit.on("GET", PUBLIC_BAPCS, reply(403, body=BLOCK_PAGE, content_type="text/html"))
    reddit.on("GET", PUBLIC_HWS, reply(403, body=BLOCK_PAGE, content_type="text/html"))
    with pytest.raises(SourceBlocked) as info:
        await ing.poll()
    assert info.value.cooldown_seconds is not None and info.value.cooldown_seconds >= UNAUTH_BLOCK_COOLDOWN_SECONDS
    assert "403" in str(info.value)
    assert len(reddit.seen) == 2  # a block wall is not retried


async def test_unauthenticated_429_raises_source_blocked_with_long_cooldown(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit)
    reddit.on("GET", PUBLIC_BAPCS, reply(429, body="<html><body>Too Many Requests</body></html>",
                                         content_type="text/html", headers={"Retry-After": "7"}))
    reddit.on("GET", PUBLIC_HWS, listing_reply("hardwareswap_new.json"))
    with pytest.raises(SourceBlocked) as info:
        await ing.poll()
    assert info.value.cooldown_seconds is not None and info.value.cooldown_seconds >= BLOCK_COOLDOWN_SECONDS


async def test_blocked_poll_puts_loop_into_cooldown(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit)
    reddit.on("GET", PUBLIC_BAPCS, reply(403, body=BLOCK_PAGE, content_type="text/html"))
    reddit.on("GET", PUBLIC_HWS, reply(403, body=BLOCK_PAGE, content_type="text/html"))
    emitted: list[Any] = []

    async def emit(raw: Any) -> None:
        emitted.append(raw)

    delay = await ing._cycle(emit)
    assert delay >= BLOCK_COOLDOWN_SECONDS
    assert ing.health.state == "open" and emitted == []


async def test_html_instead_of_json_is_a_block(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, subreddits=[{"name": "buildapcsales"}])
    reddit.on("GET", PUBLIC_BAPCS, reply(200, body=BLOCK_PAGE, content_type="text/html"))
    with pytest.raises(SourceBlocked):
        await ing.poll()


async def test_private_subreddit_is_skipped_not_fatal(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit)
    reddit.on("GET", PUBLIC_BAPCS, listing_reply("buildapcsales_new.json"))
    reddit.on("GET", PUBLIC_HWS, reply(403, {"reason": "private", "message": "Forbidden", "error": 403}))
    listings = await ing.poll()
    assert {raw.extra["subreddit"] for raw in listings} == {"buildapcsales"}
    assert ing.m_posts.value(subreddit="hardwareswap", outcome="inaccessible_private") == 1


async def test_partial_failure_returns_healthy_subreddits(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit)
    reddit.on("GET", PUBLIC_BAPCS, listing_reply("buildapcsales_new.json"))
    reddit.on("GET", PUBLIC_HWS, reply(503, body="upstream unavailable", content_type="text/plain"))
    listings = await ing.poll()
    assert len(listings) == 4 and all(raw.extra["mode"] == "deals" for raw in listings)
    assert len(reddit.calls("GET", PUBLIC_HWS)) == 2  # transient 5xx retried by the HTTP layer


async def test_all_subreddits_failing_raises(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit)
    reddit.on("GET", PUBLIC_BAPCS, reply(503, body="x", content_type="text/plain"))
    reddit.on("GET", PUBLIC_HWS, reply(503, body="x", content_type="text/plain"))
    with pytest.raises(Exception) as info:
        await ing.poll()
    assert not isinstance(info.value, SourceBlocked)


async def test_malformed_json_listing_raises_source_error(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, subreddits=[{"name": "buildapcsales"}])
    reddit.on("GET", PUBLIC_BAPCS, reply(200, body=b'{"kind": "Listing", "data": {'))
    with pytest.raises(SourceError):
        await ing.poll()


async def test_run_once_emits_new_posts_once_and_ignores_stale(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit)
    reddit.on("GET", PUBLIC_BAPCS, listing_reply("buildapcsales_new.json", newest_age_s=60))
    reddit.on("GET", PUBLIC_HWS, listing_reply("hardwareswap_new.json", newest_age_s=60))
    first = await ing.run_once()
    second = await ing.run_once()
    assert len(first) == 7 and all(raw.node_id == "test" for raw in first)
    assert second == []  # unchanged listing -> nothing re-emitted (no `before` cursor needed)

    reddit.reset()
    reddit.on("GET", PUBLIC_BAPCS, listing_reply("buildapcsales_new.json", newest_age_s=300 * 60))
    reddit.on("GET", PUBLIC_HWS, listing_reply("hardwareswap_new.json", newest_age_s=300 * 60))
    stale = make_ingestor(http, reddit)
    assert await stale.run_once() == []  # older than max_item_age_minutes=240


async def test_price_edit_on_swap_post_is_re_emitted(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, subreddits=[{"name": "hardwareswap", "mode": "swap"}])
    payload = fixture("hardwareswap_new.json", newest_age_s=60)
    edited = copy.deepcopy(payload)
    for child in edited["data"]["children"]:
        if child["data"]["id"] == "1nyk1e6":
            child["data"]["selftext"] = child["data"]["selftext"].replace("$420 shipped", "~~$420~~ $380 shipped")
    reddit.on("GET", PUBLIC_HWS, reply(200, payload), reply(200, edited))
    assert len(await ing.run_once()) == 3
    again = await ing.run_once()
    assert [(raw.source_id, raw.price) for raw in again] == [("t3_1nyk1e6", 380.0)]


# --------------------------------------------------------------------------- polling: OAuth


async def test_oauth_token_flow_and_caching(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, env=OAUTH_ENV)
    assert ing.authenticated and ing.oauth is not None
    reddit.on("POST", TOKEN_PATH, reply(200, token_payload("tok-1")))
    reddit.on("GET", OAUTH_BAPCS, listing_reply("buildapcsales_new.json", headers=RL_OK))
    reddit.on("GET", OAUTH_HWS, listing_reply("hardwareswap_new.json", headers=RL_OK))
    assert len(await ing.poll()) == 7
    assert len(await ing.poll()) == 7  # token reused, not re-requested
    token_calls = reddit.calls("POST", TOKEN_PATH)
    assert len(token_calls) == 1
    req = token_calls[0]
    assert req.headers["Authorization"] == "Basic " + base64.b64encode(b"cid-123:s3cr3t").decode("ascii")
    assert req.headers["User-Agent"] == "python:dealradar:1.0 (by /u/radar_bot)"
    assert req.form == {"grant_type": "client_credentials", "scope": "read"}
    listing_calls = [s for s in reddit.seen if s.method == "GET"]
    assert len(listing_calls) == 4
    assert all(s.headers["Authorization"] == "bearer tok-1" for s in listing_calls)
    assert all(s.query == {"limit": "25", "raw_json": "1"} for s in listing_calls)
    assert all(s.headers["User-Agent"] == "python:dealradar:1.0 (by /u/radar_bot)" for s in listing_calls)
    assert ing.oauth.refreshes == 1


def _auth_gate(name: str, bad_tokens: set[str]) -> Responder:
    """401 for tokens in ``bad_tokens``, the recorded listing otherwise."""

    def _make(seen: Seen) -> web.StreamResponse:
        if seen.headers.get("Authorization", "").removeprefix("bearer ") in bad_tokens:
            return web.json_response({"message": "Unauthorized", "error": 401}, status=401)
        return web.json_response(fixture(name), headers=RL_OK)

    return _make


async def test_oauth_401_refreshes_token_once(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, env=OAUTH_ENV, subreddits=[{"name": "buildapcsales"}])
    reddit.on("POST", TOKEN_PATH, reply(200, token_payload("tok-1")), reply(200, token_payload("tok-2")))
    reddit.on("GET", OAUTH_BAPCS, _auth_gate("buildapcsales_new.json", {"tok-1"}))
    listings = await ing.poll()
    assert len(listings) == 4
    assert len(reddit.calls("POST", TOKEN_PATH)) == 2
    assert [s.headers["Authorization"] for s in reddit.calls("GET", OAUTH_BAPCS)] == ["bearer tok-1", "bearer tok-2"]


async def test_oauth_persistent_401_raises_auth_error(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, env=OAUTH_ENV, subreddits=[{"name": "buildapcsales"}])
    reddit.on("POST", TOKEN_PATH, reply(200, token_payload("tok-1")), reply(200, token_payload("tok-2")))
    reddit.on("GET", OAUTH_BAPCS, reply(401, {"message": "Unauthorized", "error": 401}))
    with pytest.raises(SourceAuthError):
        await ing.poll()
    assert len(reddit.calls("GET", OAUTH_BAPCS)) == 2  # original + exactly one retry


async def test_oauth_concurrent_401_share_one_refresh(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, env=OAUTH_ENV)
    reddit.on(
        "POST", TOKEN_PATH,
        reply(200, token_payload("tok-1")), reply(200, token_payload("tok-2")), reply(200, token_payload("tok-3")),
    )
    reddit.on("GET", OAUTH_BAPCS, _auth_gate("buildapcsales_new.json", {"tok-1"}))
    reddit.on("GET", OAUTH_HWS, _auth_gate("hardwareswap_new.json", {"tok-1"}))
    assert len(await ing.poll()) == 7
    assert len(reddit.calls("POST", TOKEN_PATH)) == 2


async def test_oauth_token_refreshed_before_expiry(http: HttpClient, reddit: FakeReddit) -> None:
    clock = FakeClock()
    ing = make_ingestor(http, reddit, env=OAUTH_ENV, clock=clock, subreddits=[{"name": "buildapcsales"}])
    reddit.on("POST", TOKEN_PATH, reply(200, token_payload("tok-1", 3600)), reply(200, token_payload("tok-2", 3600)))
    reddit.on("GET", OAUTH_BAPCS, listing_reply("buildapcsales_new.json", headers=RL_OK))
    await ing.poll()
    clock.now += 3600 - TOKEN_REFRESH_MARGIN_SECONDS - 1  # still inside the lifetime minus the refresh margin
    await ing.poll()
    assert len(reddit.calls("POST", TOKEN_PATH)) == 1
    clock.now += 2  # inside the refresh margin -> proactively re-request
    await ing.poll()
    assert len(reddit.calls("POST", TOKEN_PATH)) == 2
    auths = [s.headers["Authorization"] for s in reddit.calls("GET", OAUTH_BAPCS)]
    assert auths == ["bearer tok-1", "bearer tok-1", "bearer tok-2"]


async def test_oauth_bad_credentials_raise_auth_error(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, env=OAUTH_ENV, subreddits=[{"name": "buildapcsales"}])
    reddit.on("POST", TOKEN_PATH, reply(401, {"message": "Unauthorized", "error": 401}), reply(200, {"error": "unsupported_grant_type"}))
    with pytest.raises(SourceAuthError):
        await ing.poll()
    with pytest.raises(SourceAuthError):  # Reddit also answers some bad requests with 200 + {"error": ...}
        await ing.poll()
    assert reddit.calls("GET", OAUTH_BAPCS) == []


async def test_oauth_token_endpoint_block_page(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, env=OAUTH_ENV, subreddits=[{"name": "buildapcsales"}])
    reddit.on("POST", TOKEN_PATH, reply(403, body=BLOCK_PAGE, content_type="text/html"))
    with pytest.raises(SourceBlocked):
        await ing.poll()


async def test_installed_app_grant_when_no_secret(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, env={"REDDIT_CLIENT_ID": "installed-id"}, subreddits=[{"name": "buildapcsales"}])
    assert ing.oauth is not None and ing.oauth.grant_type == INSTALLED_CLIENT_GRANT
    reddit.on("POST", TOKEN_PATH, reply(200, token_payload("tok-i")))
    reddit.on("GET", OAUTH_BAPCS, listing_reply("buildapcsales_new.json", headers=RL_OK))
    assert len(await ing.poll()) == 4
    req = reddit.calls("POST", TOKEN_PATH)[0]
    assert req.form["grant_type"] == INSTALLED_CLIENT_GRANT
    assert req.form["scope"] == "read"
    assert 20 <= len(req.form["device_id"]) <= 30
    assert req.headers["Authorization"] == "Basic " + base64.b64encode(b"installed-id:").decode("ascii")


async def test_oauth_403_block_page_retries_with_fresh_token(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, env=OAUTH_ENV, subreddits=[{"name": "buildapcsales"}])
    pardner = "<html><body><h1>whoa there, pardner!</h1>Your request has been blocked due to a network policy.</body></html>"

    def gate(seen: Seen) -> web.StreamResponse:
        if seen.headers.get("Authorization") == "bearer tok-1":
            return web.Response(status=403, text=pardner, content_type="text/html")
        return web.json_response(fixture("buildapcsales_new.json"), headers=RL_OK)

    reddit.on("POST", TOKEN_PATH, reply(200, token_payload("tok-1")), reply(200, token_payload("tok-2")))
    reddit.on("GET", OAUTH_BAPCS, gate)
    assert len(await ing.poll()) == 4
    assert [s.headers["Authorization"] for s in reddit.calls("GET", OAUTH_BAPCS)] == ["bearer tok-1", "bearer tok-2"]

    reddit.reset()
    blocked = make_ingestor(http, reddit, env=OAUTH_ENV, subreddits=[{"name": "buildapcsales"}])
    reddit.on("POST", TOKEN_PATH, reply(200, token_payload("tok-3")))
    reddit.on("GET", OAUTH_BAPCS, reply(403, body=pardner, content_type="text/html"))
    with pytest.raises(SourceBlocked) as info:
        await blocked.poll()
    assert info.value.cooldown_seconds is not None and info.value.cooldown_seconds >= BLOCK_COOLDOWN_SECONDS
    assert len(reddit.calls("GET", OAUTH_BAPCS)) == 2


async def test_oauth_429_without_headers_uses_default_cooldown(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, env=OAUTH_ENV, subreddits=[{"name": "buildapcsales"}])
    reddit.on("POST", TOKEN_PATH, reply(200, token_payload()))
    reddit.on("GET", OAUTH_BAPCS, reply(429, body=b"", content_type="text/plain"))
    with pytest.raises(SourceBlocked) as info:
        await ing.poll()
    assert info.value.cooldown_seconds == 60.0


async def test_oauth_429_raises_blocked_with_reset_cooldown(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, env=OAUTH_ENV, subreddits=[{"name": "buildapcsales"}])
    reddit.on("POST", TOKEN_PATH, reply(200, token_payload()))
    reddit.on("GET", OAUTH_BAPCS, reply(429, {"message": "Too Many Requests", "error": 429},
                                        headers={"x-ratelimit-remaining": "0", "x-ratelimit-used": "1000",
                                                 "x-ratelimit-reset": "95"}))
    with pytest.raises(SourceBlocked) as info:
        await ing.poll()
    assert info.value.cooldown_seconds == pytest.approx(96.0)
    assert len(reddit.calls("GET", OAUTH_BAPCS)) == 1  # 429 is not hammered with retries


# --------------------------------------------------------------------------- rate limits


async def test_rate_limit_headers_stretch_next_interval(http: HttpClient, reddit: FakeReddit) -> None:
    clock = FakeClock()
    ing = make_ingestor(http, reddit, env=OAUTH_ENV, clock=clock)
    base = ing.cfg.poll_interval_seconds
    exhausted = {"x-ratelimit-used": "999", "x-ratelimit-remaining": "1.0", "x-ratelimit-reset": "120"}
    reddit.on("POST", TOKEN_PATH, reply(200, token_payload()))
    reddit.on("GET", OAUTH_BAPCS, listing_reply("buildapcsales_new.json", headers=RL_OK),
              listing_reply("buildapcsales_new.json", headers=exhausted))
    reddit.on("GET", OAUTH_HWS, listing_reply("hardwareswap_new.json", headers=RL_OK),
              listing_reply("hardwareswap_new.json", headers={**exhausted, "x-ratelimit-remaining": "2.0"}))
    await ing.poll()
    healthy = [ing.next_interval() for _ in range(20)]
    assert all(base * 0.85 - 1e-9 <= v <= base * 1.15 + 1e-9 for v in healthy)

    await ing.poll()
    assert ing.rate_limit.remaining == 1.0  # the most pessimistic value of the window wins
    assert ing.m_remaining.value() in (1.0, 2.0)
    stretched = ing.next_interval()
    assert 120.0 < stretched <= 122.0

    clock.now += 60  # half the window elapsed -> wait only for the rest
    assert 60.0 < ing.next_interval() <= 62.0

    clock.now += 61  # window over -> back to the normal cadence
    assert ing.next_interval() <= base * 1.15 + 1e-9


async def test_rate_limit_paces_remaining_budget(http: HttpClient) -> None:
    clock = FakeClock()
    ing = make_ingestor(http, env=OAUTH_ENV, clock=clock)
    ing.rate_limit.observe(remaining=10.0, used=990.0, reset_seconds=300.0, now=clock.now)
    # 10 requests left for 300 s with 2 requests per poll -> one poll every 60 s.
    assert ing.next_interval() == pytest.approx(60.0)


async def test_unauthenticated_interval_floor(http: HttpClient) -> None:
    ing = make_ingestor(http)
    # 2 subreddits at the 10 QPM public budget -> at least 12 s between polls.
    assert all(ing.next_interval() >= 12.0 for _ in range(10))


async def test_exhausted_window_skips_poll_without_requests(http: HttpClient, reddit: FakeReddit) -> None:
    clock = FakeClock()
    ing = make_ingestor(http, reddit, env=OAUTH_ENV, clock=clock)
    ing.rate_limit.observe(remaining=0.0, used=1000.0, reset_seconds=30.0, now=clock.now)
    assert await ing.poll() == []
    assert reddit.seen == []


def test_rate_limit_state_windows() -> None:
    rl = RateLimitState()
    rl.observe(remaining=10.0, used=990.0, reset_seconds=100.0, now=0.0)
    rl.observe(remaining=12.0, used=988.0, reset_seconds=99.5, now=0.5)  # older response, same window
    assert rl.remaining == 10.0
    rl.observe(remaining=8.0, used=992.0, reset_seconds=99.0, now=1.0)
    assert rl.remaining == 8.0
    rl.observe(remaining=995.0, used=5.0, reset_seconds=600.0, now=101.0)  # new window
    assert rl.remaining == 995.0 and rl.seconds_until_reset(101.0) == pytest.approx(600.0)
    rl.observe(remaining=None, used=None, reset_seconds=None, now=102.0)  # no headers: unchanged
    assert rl.remaining == 995.0


# --------------------------------------------------------------------------- wiring


async def test_registry_and_config_wiring(http: HttpClient) -> None:
    from deal_radar.sources.registry import load_ingestor_class

    assert load_ingestor_class("reddit") is RedditIngestor
    ing = make_ingestor(http, hardwareswap_locations=["USA-TX"])
    assert ing.name == "reddit" and ing.kind is SourceKind.AGGREGATOR
    assert [s.mode for s in ing.cfg.subreddits] == ["deals", "swap"]
    assert ing.listing_url(SubredditSpec(name="buildapcsales")) == "https://www.reddit.com/r/buildapcsales/new.json"
    oauth = make_ingestor(http, env=OAUTH_ENV)
    assert oauth.listing_url(SubredditSpec(name="hardwareswap", mode="swap")) == "https://oauth.reddit.com/r/hardwareswap/new"
    assert oauth.user_agent == "python:dealradar:1.0 (by /u/radar_bot)"


async def test_location_filter_applies_only_to_swap_subreddits(http: HttpClient, reddit: FakeReddit) -> None:
    ing = make_ingestor(http, reddit, hardwareswap_locations=["USA-TX"])
    reddit.on("GET", PUBLIC_BAPCS, listing_reply("buildapcsales_new.json"))
    reddit.on("GET", PUBLIC_HWS, listing_reply("hardwareswap_new.json"))
    listings = await ing.poll()
    swap = [raw for raw in listings if raw.source_kind is SourceKind.LOCAL]
    deals = [raw for raw in listings if raw.source_kind is SourceKind.AGGREGATOR]
    assert [raw.source_id for raw in swap] == ["t3_1nyk1e6"]
    assert len(deals) == 4
