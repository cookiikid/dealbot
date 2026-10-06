"""Tests for the OfferUp and Craigslist ingestors (offline: fixtures + a local aiohttp server).

aioresponses 0.7.x cannot build responses for aiohttp 3.14 (``ClientResponse`` gained a
required ``stream_writer`` argument), so upstreams are emulated with an aiohttp
``TestServer`` and the ingestors are pointed at it through their origin overrides –
the same approach ``test_core.py`` uses for the HTTP client. Fixtures mirror the real
upstream payload shapes documented in the module docstrings.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from deal_radar.config_schema import AppConfig, CraigslistSource, OfferUpSource, load_config
from deal_radar.core.backoff import BackoffPolicy
from deal_radar.core.http import HttpClient, NetworkSettings
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import SourceKind
from deal_radar.sources import craigslist as cl
from deal_radar.sources import offerup as ou
from deal_radar.sources.base import IngestorContext, SourceBlocked, SourceError

# --------------------------------------------------------------------------- fixtures: payloads

OU_LISTING_FE = {
    "__typename": "ModularFeedListing",
    "listingId": "8f1c2a3b-4d5e-3f60-9a1b-2c3d4e5f6a7b",
    "conditionText": "Used",
    "flags": ["LOCAL_PICKUP"],
    "image": {"__typename": "Image", "height": 250, "url": "https://images.offerup.com/AbC123=/250x250/7c1d.jpg", "width": 250},
    "isFirmPrice": False,
    "locationName": "Brooklyn, NY",
    "price": "1450",
    "title": "NVIDIA RTX 4090 Founders Edition",
    "vehicleMiles": None,
}
OU_LISTING_TUF = {
    "__typename": "ModularFeedListing",
    "listingId": "1234567890",
    "conditionText": None,
    "flags": ["SHIPPING", "LOCAL_PICKUP"],
    "image": {"height": 250, "url": "https://images.offerup.com/XyZ=/250x250/a1b2.jpg", "width": 250},
    "isFirmPrice": True,
    "locationName": "Jersey City, NJ",
    "price": "1,350.00",
    "title": "ASUS TUF rtx 4090 OC &amp; box",
    "vehicleMiles": None,
}


def ou_feed(*listings: dict[str, Any], with_ads: bool = True) -> dict[str, Any]:
    tiles: list[dict[str, Any]] = []
    if with_ads:
        tiles.append({"__typename": "ModularFeedTileBanner", "tileId": "b-1", "tileType": "BANNER", "title": "Results near you"})
        tiles.append(
            {
                "__typename": "ModularFeedTileGoogleDisplayAd",
                "tileId": "ad-1",
                "tileType": "AD_3P_GOOGLE_DISPLAY",
                "googleDisplayAd": {"ouAdId": "x", "adNetwork": "GOOGLE", "contentUrl": "https://offerup.com/"},
            }
        )
        tiles.append(
            {
                "__typename": "ModularFeedTileSellerAd",
                "tileId": "ad-2",
                "tileType": "AD_1P",
                "listing": {"listingId": "999000", "title": "Promoted RTX 4090 (ships nationwide)", "price": "1999"},
                "sellerAd": {"ouAdId": "y"},
            }
        )
    for i, listing in enumerate(listings):
        tiles.append({"__typename": "ModularFeedTileListing", "tileId": f"t-{i}", "tileType": "LISTING", "listing": listing})
    return {
        "__typename": "ModularFeedResponse",
        "looseTiles": tiles,
        "nextPageCursor": "eyJvZmZzZXQiOjUwfQ==",
        "searchData": {"requestId": "req-1", "searchSessionId": "sess-1"},
    }


def ou_page(feed: dict[str, Any] | None, *, page_props: dict[str, Any] | None = None) -> str:
    props = page_props if page_props is not None else {"searchFeedResponse": feed}
    next_data = {
        "props": {"pageProps": props, "__N_SSP": True},
        "page": "/search",
        "query": {"q": "rtx 4090"},
        "buildId": "2026.22.0-abc",
    }
    return (
        "<!DOCTYPE html><html lang=\"en\"><head><meta charSet=\"utf-8\"/><title>rtx 4090 for sale | OfferUp</title>"
        # Real pages load reCAPTCHA for the login modal: must not be mistaken for a challenge.
        "<script src=\"https://www.google.com/recaptcha/api.js?render=explicit\" async></script></head>"
        "<body><div id=\"__next\"><main>Results</main></div>"
        f"<script id=\"__NEXT_DATA__\" type=\"application/json\">{json.dumps(next_data)}</script></body></html>"
    )


OU_GEOCODE = {
    "data": {
        "geocodeLocation": {
            "location": {"city": "Brooklyn", "latitude": 40.68, "longitude": -73.94, "state": "NY", "zipCode": "11216"}
        }
    }
}

CLOUDFLARE_PAGE = (
    "<!DOCTYPE html><html lang=\"en-US\"><head><title>Just a moment...</title></head><body>"
    "<div class=\"main-wrapper\"><noscript>Enable JavaScript and cookies to continue</noscript></div>"
    "<script src=\"/cdn-cgi/challenge-platform/h/b/orchestrate/chl_page/v1?ray=8c1\"></script></body></html>"
)

CL_AREAS = [
    {
        "Abbreviation": "nyc",
        "AreaID": 3,
        "Country": "US",
        "Description": "new york city",
        "Hostname": "newyork",
        "Latitude": 40.7143,
        "Longitude": -74.0059,
        "Region": "NY",
        "ShortDescription": "new york",
        "SubAreas": [{"Abbreviation": "mnh", "Description": "manhattan", "ShortDescription": "manhattan", "SubAreaID": 1}],
        "Timezone": "America/New_York",
    },
    {
        "Abbreviation": "sfo",
        "AreaID": 1,
        "Country": "US",
        "Description": "SF bay area",
        "Hostname": "sfbay",
        "Latitude": 37.7749,
        "Longitude": -122.4194,
        "Region": "CA",
        "ShortDescription": "SF bay area",
        "SubAreas": [],
        "Timezone": "America/Los_Angeles",
    },
    {"Abbreviation": "tor", "AreaID": 25, "Country": "CA", "Hostname": "toronto", "Latitude": 43.65, "Longitude": -79.38},
    {"Hostname": "broken"},
]
CL_CATEGORIES = [
    {"Abbreviation": "sop", "CategoryID": 7, "Description": "computer parts - by owner", "Type": "S"},
    {"Abbreviation": "sdp", "CategoryID": 8, "Description": "computer parts - by dealer", "Type": "S"},
    {"Abbreviation": "ele", "CategoryID": 96, "Description": "electronics - by owner", "Type": "S"},
    {"Abbreviation": "", "CategoryID": 99, "Description": "bad", "Type": "S"},
]

CL_MIN_POSTING_ID = 7_880_000_000
CL_MIN_POSTED = 1_759_600_000


def cl_sapi_payload() -> dict[str, Any]:
    return {
        "apiVersion": 8,
        "data": {
            "apiVersion": 8,
            "areas": {"3": {"name": "new york"}},
            "cacheId": "f3a2b1",
            "cacheTs": 1_759_700_000,
            "canonicalUrl": "https://newyork.craigslist.org/search/sss?query=rtx%204090",
            "categoryAbbr": "sss",
            "decode": {
                "locationDescriptions": ["Brooklyn", "Manhattan", "Jersey City"],
                "locations": [[3, "newyork", "brk"], [3, "newyork", "mnh"], [3, "newyork", "jsy"], 0],
                "maxPostedDate": 1_759_700_000,
                "minDate": 1_759_000_000,
                "minPostedDate": CL_MIN_POSTED,
                "minPostingId": CL_MIN_POSTING_ID,
                "neighborhoods": ["Park Slope", "Midtown"],
            },
            "items": [
                [
                    123_456,
                    99_000,
                    7,
                    1400,
                    "0:0:0~40.6710~-73.9814",
                    "a1b2c3",
                    [13, "fK7wQ2mZp9LsXy3Rt8VbNa"],
                    [4, "3:00a0a_jx892ZIFraf_0CI0qt", "3:00b0b_kq77ABCdef_0CI0qt", "3:bad id!"],
                    [6, "brooklyn-nvidia-rtx-4090-founders"],
                    [10, "$1,400"],
                    "NVIDIA RTX 4090 Founders Edition",
                ],
                [
                    123_457, 98_000, 12_345, -1, "1:1:1~40.7549~-73.9840", "d4e5f6",
                    [6, "manhattan-rtx-4090-gaming-pc"], "RTX 4090 Gaming PC - $3200",
                ],
                [
                    123_458, 50, 7, 0, "2:2~40.7178~-74.0431", "0a0b0c", -2, [4, "3:00c0c_abcdefgh_0t20CI"],
                    [6, "jersey-city-asus-tuf-4090"], [10, "$1,250"], "ASUS TUF 4090 OC",
                ],
                ["oops"],
                [123_459, 10, 7, 900, "0~40.7~-74.0", [6, "no-title"], 12_345],
                [123_456, 99_000, 7, 1400, "0:0:0~40.6710~-73.9814", "a1b2c3", "NVIDIA RTX 4090 Founders Edition (dupe)"],
            ],
            "location": {"url": "newyork.craigslist.org"},
            "totalResultCount": 6,
        },
        "errors": [],
    }


CL_HTML = """<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>new york for sale "rtx 4090" - craigslist</title>
<script>window.cl = {"areaId":3,"hostname":"newyork","lang":"en"};</script></head>
<body class="no-js">
<ol class="cl-static-search-results">
  <li class="cl-static-header"><a href="https://newyork.craigslist.org/search/sss">see also</a></li>
  <li class="cl-static-search-result" title="NVIDIA RTX 4090 FE">
    <a href="https://newyork.craigslist.org/brk/sop/d/brooklyn-nvidia-rtx-4090-fe/7880123456.html">
      <div class="title">NVIDIA RTX 4090 FE</div>
      <div class="details">
        <div class="price">$1,400</div>
        <div class="location">
          Brooklyn
        </div>
      </div>
    </a>
  </li>
  <li class="cl-static-search-result" title="RTX 4090 &amp; i9 build">
    <a href="/mnh/sys/d/new-york-rtx-4090-i9-build/7880123457.html">
      <div class="title">RTX 4090 &amp; i9 build</div>
      <div class="details"><div class="location">Manhattan</div></div>
    </a>
  </li>
  <li class="cl-static-search-result" title="no link"><div class="title">no link</div></li>
  <li class="cl-static-search-result" title="dupe">
    <a href="https://newyork.craigslist.org/brk/sop/d/brooklyn-nvidia-rtx-4090-fe/7880123456.html">
      <div class="title">dupe</div></a>
  </li>
</ol>
</body></html>"""

CL_BLOCK_PAGE = (
    "<html><body><p>This IP has been automatically blocked.</p>"
    "<p>If you have questions, please email: blocks-b1@craigslist.org</p></body></html>"
)


# --------------------------------------------------------------------------- fixtures: config / server


def make_config() -> AppConfig:
    band_gpu = {"reference_new": 2199, "reference_used": 1750, "floor": 450, "target": 1400, "ceiling": 1950}
    band_deck = {"reference_new": 549, "reference_used": 420, "floor": 150.5, "target": 380, "ceiling": 480.2}
    return AppConfig.model_validate(
        {
            "profiles": [
                {
                    "id": "rtx_4090",
                    "name": "NVIDIA GeForce RTX 4090",
                    "category": "gpu",
                    "match": {"any": [r"\b4090\b"]},
                    "price": band_gpu,
                    "search": {"terms": ["rtx 4090"]},
                },
                {
                    "id": "steam_deck",
                    "name": "Steam Deck OLED",
                    "category": "handheld",
                    "match": {"any": [r"steam\s*deck"]},
                    "price": band_deck,
                    "search": {"terms": ["steam deck oled", "  steam   deck 1tb "], "price_min": 200},
                },
                {
                    "id": "disabled_one",
                    "name": "Disabled",
                    "category": "gpu",
                    "enabled": False,
                    "match": {"any": ["x"]},
                    "price": band_gpu,
                    "search": {"terms": ["never searched"]},
                },
                {
                    "id": "ebay_only",
                    "name": "eBay only",
                    "category": "gpu",
                    "match": {"any": ["y"]},
                    "price": band_gpu,
                    "search": {"terms": ["ebay only term"], "sources": ["ebay"]},
                },
            ]
        }
    )


class StepClock:
    """Monotonic fake clock that advances ``step`` seconds on every read."""

    def __init__(self, step: float) -> None:
        self.now = 0.0
        self.step = step

    def __call__(self) -> float:
        self.now += self.step
        return self.now


Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


class Upstream:
    """Tiny programmable fake upstream: routes (method, path) -> handler, records requests."""

    def __init__(self) -> None:
        self.routes: dict[tuple[str, str], Handler] = {}
        self.requests: list[dict[str, Any]] = []
        self.server: TestServer | None = None

    def route(self, method: str, path: str, handler: Handler) -> None:
        self.routes[(method, path)] = handler

    def url(self, path: str = "") -> str:
        assert self.server is not None
        return str(self.server.make_url(path)).rstrip("/")

    def calls(self, method: str | None = None, path: str | None = None) -> list[dict[str, Any]]:
        return [r for r in self.requests if (method is None or r["method"] == method) and (path is None or r["path"] == path)]

    async def dispatch(self, request: web.Request) -> web.StreamResponse:
        body = await request.text()
        self.requests.append(
            {
                "method": request.method,
                "path": request.path,
                "query": dict(request.query),
                "headers": dict(request.headers),
                "json": json.loads(body) if body and request.content_type == "application/json" else None,
            }
        )
        handler = self.routes.get((request.method, request.path))
        if handler is None:
            return web.Response(status=404, text="not found")
        return await handler(request)


def text(body: str, status: int = 200, content_type: str = "text/html") -> Handler:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(status=status, text=body, content_type=content_type)

    return handler


def jsonr(payload: Any, status: int = 200) -> Handler:
    async def handler(request: web.Request) -> web.Response:
        return web.json_response(payload, status=status)

    return handler


@pytest.fixture
async def upstream():
    up = Upstream()
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", up.dispatch)
    up.server = TestServer(app)
    await up.server.start_server()
    yield up
    await up.server.close()


@pytest.fixture
async def ctx():
    http = HttpClient.create(NetworkSettings(retry=BackoffPolicy(max_attempts=1, base_delay=0, max_delay=0), trust_env=False))
    yield IngestorContext(http=http, metrics=Metrics(), config=make_config(), node_id="test-node")
    await http.close()


def offerup(ctx: IngestorContext, upstream: Upstream, **overrides: Any) -> ou.OfferUpIngestor:
    params: dict[str, Any] = {
        "enabled": True,
        "latitude": 40.6782,
        "longitude": -73.9442,
        "radius_miles": 25,
        "poll_timeout_seconds": 30,
    }
    clock = overrides.pop("clock", None)
    params.update(overrides)
    kwargs: dict[str, Any] = {"base_url": upstream.url()}
    if clock is not None:
        kwargs["clock"] = clock
    return ou.OfferUpIngestor(OfferUpSource(**params), ctx, **kwargs)


def craigslist(ctx: IngestorContext, upstream: Upstream, **overrides: Any) -> cl.CraigslistIngestor:
    params: dict[str, Any] = {"enabled": True, "sites": ["newyork"], "search_distance_miles": 40, "poll_timeout_seconds": 30}
    clock = overrides.pop("clock", None)
    params.update(overrides)
    kwargs: dict[str, Any] = {
        "sapi_origin": upstream.url(),
        "reference_origin": upstream.url("/ref"),
        "site_url_template": upstream.url() + "/site/{site}",
    }
    if clock is not None:
        kwargs["clock"] = clock
    return cl.CraigslistIngestor(CraigslistSource(**params), ctx, **kwargs)


# =========================================================================== OfferUp: pure parsing


def test_offerup_snap_radius_rounds_up_to_supported_values() -> None:
    assert [ou.snap_radius(v) for v in (1, 5, 6, 25, 30, 31, 500)] == [5, 5, 10, 30, 30, 50, 50]
    assert ou.snap_radius(None) == 30 and ou.snap_radius(0) == 30


def test_offerup_location_cookie_is_url_encoded_json() -> None:
    value = ou.location_cookie(40.6782, -73.9442, "11216")
    assert value is not None and ";" not in value and "," not in value and '"' not in value
    decoded = json.loads(unquote(value))
    assert decoded == {"latitude": 40.6782, "longitude": -73.9442, "zipCode": "11216", "source": "user"}
    assert ou.location_cookie(None, None, None) is None
    assert json.loads(unquote(ou.location_cookie(None, None, "10001") or "")) == {"zipCode": "10001", "source": "user"}


def test_offerup_parse_search_page_extracts_listings_and_skips_ads() -> None:
    stats: dict[str, int] = {}
    feed = ou_feed(
        OU_LISTING_FE,
        {"listingId": "bad id with spaces", "title": "unusable id"},
        {"listingId": "777", "title": "   "},
        OU_LISTING_TUF,
        dict(OU_LISTING_FE, price="1400"),  # same id again (reposted tile) -> first wins
    )
    listings = ou.parse_search_page(ou_page(feed), query="rtx 4090", profile_hint="rtx_4090", stats=stats)
    assert [r.source_id for r in listings] == [OU_LISTING_FE["listingId"], "1234567890"]
    first, second = listings
    assert first.source == "offerup" and first.source_kind is SourceKind.LOCAL
    assert first.url == f"https://offerup.com/item/detail/{OU_LISTING_FE['listingId']}"
    assert first.title == "NVIDIA RTX 4090 Founders Edition"
    assert first.price == 1450.0 and first.currency == "USD"
    assert first.condition == "Used"
    assert first.image_urls == ["https://images.offerup.com/AbC123=/250x250/7c1d.jpg"]
    assert first.location is not None and first.location.text == "Brooklyn, NY"
    assert (first.location.city, first.location.region) == ("Brooklyn", "NY")
    assert first.query == "rtx 4090" and first.profile_hint == "rtx_4090"
    assert first.extra == {"flags": ["LOCAL_PICKUP"], "firm_price": False, "price_text": "1450", "via": "page"}
    assert first.posted_at is None and first.seller is None
    assert second.title == "ASUS TUF rtx 4090 OC & box"
    assert second.price == 1350.0 and second.condition is None
    assert second.extra["firm_price"] is True and second.extra["flags"] == ["SHIPPING", "LOCAL_PICKUP"]
    assert "999000" not in {r.source_id for r in listings}  # promoted AD_1P tile skipped
    assert stats == {"unusable": 2}


def test_offerup_parse_search_page_limit_and_empty_feed() -> None:
    page = ou_page(ou_feed(OU_LISTING_FE, OU_LISTING_TUF))
    assert len(ou.parse_search_page(page, limit=1)) == 1
    assert ou.parse_search_page(ou_page(ou_feed(with_ads=False))) == []


def test_offerup_parse_search_page_walks_apollo_state_when_feed_path_moves() -> None:
    apollo = {
        "initialApolloState": {
            "ROOT_QUERY": {
                "__typename": "Query",
                'modularFeed({"params":[]})': {"looseTiles": [{"__ref": "ModularFeedTileListing:t1"}]},
            },
            "ModularFeedTileListing:t1": {"__typename": "ModularFeedTileListing", "listing": {"__ref": "ModularFeedListing:555"}},
            "ModularFeedListing:555": {
                "__typename": "ModularFeedListing",
                "listingId": "555",
                "title": "MSI RTX 4090 Suprim Liquid",
                "price": "1500",
                "locationName": "Queens, NY",
                "postDate": "2026-10-05T18:30:00.000Z",
                "owner": {"profile": {"name": "Sam", "ratingSummary": {"average": 4.8, "count": 37}, "isBusinessAccount": False}},
                "photos": [
                    {"detailFull": {"url": "https://images.offerup.com/full/1.jpg"}},
                    {"list": {"url": "https://images.offerup.com/list/2.jpg"}},
                ],
            },
        }
    }
    listings = ou.parse_search_page(ou_page(None, page_props=apollo))
    assert [r.source_id for r in listings] == ["555"]
    raw = listings[0]
    assert raw.posted_at == datetime(2026, 10, 5, 18, 30, tzinfo=timezone.utc)
    assert raw.seller is not None and raw.seller.name == "Sam" and raw.seller.feedback_score == 37
    assert raw.seller.is_business is False and raw.seller.feedback_pct is None
    assert raw.image_urls == ["https://images.offerup.com/full/1.jpg", "https://images.offerup.com/list/2.jpg"]


def test_offerup_parse_search_page_raises_on_missing_payload() -> None:
    with pytest.raises(ou.OfferUpParseError):
        ou.parse_search_page("<html><body>maintenance</body></html>")
    with pytest.raises(ou.OfferUpParseError):
        ou.parse_search_page('<script id="__NEXT_DATA__" type="application/json">{not json</script>')
    with pytest.raises(ou.OfferUpParseError):
        ou.parse_search_page(ou_page(None, page_props={"somethingElse": {"a": 1}}))


def test_offerup_parse_graphql_feed_and_errors() -> None:
    payload = {"data": {"modularFeed": ou_feed(OU_LISTING_TUF)}}
    listings = ou.parse_graphql_feed(payload, query="rtx 4090", profile_hint="rtx_4090")
    assert [r.source_id for r in listings] == ["1234567890"]
    assert listings[0].extra["via"] == "graphql"
    with pytest.raises(ou.OfferUpParseError, match="PersistedQueryNotFound"):
        ou.parse_graphql_feed({"errors": [{"message": "PersistedQueryNotFound"}], "data": None})
    with pytest.raises(ou.OfferUpParseError):
        ou.parse_graphql_feed(["not", "a", "mapping"])


def test_offerup_parse_geocode() -> None:
    assert ou.parse_geocode(OU_GEOCODE) == (40.68, -73.94)
    assert ou.parse_geocode({"data": {"geocodeLocation": None}}) is None
    assert ou.parse_geocode({"data": {"geocodeLocation": {"location": {"latitude": "x", "longitude": 1}}}}) is None
    assert ou.parse_geocode({"data": {"geocodeLocation": {"location": {"latitude": 99.0, "longitude": 1}}}}) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1450", 1450.0),
        ("1,350.00", 1350.0),
        ("$95", 95.0),
        (" 12.50 ", 12.5),
        (1200, 1200.0),
        ("0", 0.0),
        ("Make Offer", "Make Offer"),
        ("", None),
        (None, None),
        (True, None),
        (-5, None),
        (float("nan"), None),
    ],
)
def test_offerup_coerce_price(value: Any, expected: Any) -> None:
    assert ou.coerce_price(value) == expected


def test_offerup_parse_timestamp_variants() -> None:
    ts = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)
    assert ou.parse_timestamp(int(ts.timestamp())) == ts
    assert ou.parse_timestamp(int(ts.timestamp() * 1000)) == ts
    assert ou.parse_timestamp(str(int(ts.timestamp()))) == ts
    assert ou.parse_timestamp("2026-10-05T12:00:00Z") == ts
    assert ou.parse_timestamp("2026-10-05T08:00:00-04:00") == ts
    for bad in (None, "", "yesterday", 42, True, float("inf"), "1900-01-01T00:00:00Z"):
        assert ou.parse_timestamp(bad) is None


def test_offerup_detect_block() -> None:
    assert ou.detect_block(403, "") == "http_403"
    assert ou.detect_block(429, None) == "http_429"
    assert ou.detect_block(200, CLOUDFLARE_PAGE) is not None
    assert ou.detect_block(200, "<html><body>Please solve this CAPTCHA</body></html>") is not None
    assert ou.detect_block(200, "<html><body>No results</body></html>") is None
    # Pages that merely load reCAPTCHA (login modal) are not walls.
    recaptcha_page = '<script src="https://www.google.com/recaptcha/api.js"></script><div class="g-recaptcha">'
    assert ou.detect_block(200, recaptcha_page) is None
    assert ou.block_cooldown("http_429", 300.0) is None
    assert ou.block_cooldown("http_403", 300.0) == 1800.0 and ou.block_cooldown("challenge:cf-chl", 7200.0) == 7200.0


def test_offerup_build_tasks_price_windows() -> None:
    config = make_config()
    tasks = ou.build_tasks([p for p in config.profiles if p.enabled])
    by_term = {t.term: t for t in tasks}
    assert by_term["rtx 4090"] == ou.SearchTask("rtx_4090", "rtx 4090", 450, 1950)
    assert by_term["steam deck oled"] == ou.SearchTask("steam_deck", "steam deck oled", 200, 481)  # override min, ceil max
    assert "steam deck 1tb" in by_term  # whitespace normalised


def test_offerup_query_params() -> None:
    task = ou.SearchTask("rtx_4090", "rtx 4090", 450, 1950)
    expected = {"q": "rtx 4090", "sort": "-posted", "radius": "30", "price_min": "450", "price_max": "1950"}
    assert ou.page_params(task, 25) == expected
    params = ou.graphql_search_params(task, radius_miles=50, limit=40, coordinates=(40.5, -73.25), session_id="s")
    as_dict = {p["key"]: p["value"] for p in params}
    assert as_dict == {
        "q": "rtx 4090",
        "platform": "web",
        "sort": "-posted",
        "radius": "50",
        "limit": "40",
        "searchSessionId": "s",
        "lat": "40.500000",
        "lon": "-73.250000",
        "price_min": "450",
        "price_max": "1950",
    }
    assert all(isinstance(p["value"], str) for p in params)


# =========================================================================== OfferUp: polling


async def test_offerup_poll_uses_page_with_location_cookie_and_dedupes(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text(ou_page(ou_feed(OU_LISTING_FE, OU_LISTING_TUF))))
    ing = offerup(ctx, upstream)
    listings = await ing.poll()
    calls = upstream.calls("GET", "/search")
    assert [c["query"]["q"] for c in calls] == ["rtx 4090", "steam deck oled", "steam deck 1tb"]
    assert calls[0]["query"] == {"q": "rtx 4090", "sort": "-posted", "radius": "30", "price_min": "450", "price_max": "1950"}
    assert calls[1]["query"]["price_min"] == "200" and calls[1]["query"]["price_max"] == "481"
    cookie = calls[0]["headers"]["Cookie"]
    assert cookie.startswith("ou.location=")
    assert json.loads(unquote(cookie.split("=", 1)[1]))["latitude"] == 40.6782
    assert calls[0]["headers"]["User-Agent"].startswith("Mozilla/5.0")
    assert not upstream.calls("POST")
    # The same two listings come back for every query: deduplicated, first query wins attribution.
    assert sorted(r.source_id for r in listings) == sorted([OU_LISTING_FE["listingId"], "1234567890"])
    assert {r.profile_hint for r in listings} == {"rtx_4090"}
    assert ctx.metrics.counter("offerup_queries_total", "", ("strategy", "outcome")).value(strategy="page", outcome="ok") == 3


async def test_offerup_falls_back_to_graphql_and_keeps_preference(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text("<html><body><div id='__next'></div></body></html>"))  # degraded SSR
    upstream.route("POST", "/api/graphql", jsonr({"data": {"modularFeed": ou_feed(OU_LISTING_TUF)}}))
    ing = offerup(ctx, upstream, profiles=["rtx_4090"])
    listings = await ing.poll()
    assert [r.source_id for r in listings] == ["1234567890"]
    assert listings[0].extra["via"] == "graphql"
    post = upstream.calls("POST", "/api/graphql")[0]
    assert post["json"]["operationName"] == "GetModularFeed"
    assert "modularFeed(params: $searchParams" in post["json"]["query"]
    params = {p["key"]: p["value"] for p in post["json"]["variables"]["searchParams"]}
    assert params["q"] == "rtx 4090" and params["sort"] == "-posted" and params["radius"] == "30"
    assert params["lat"] == "40.678200" and params["lon"] == "-73.944200"
    assert params["price_min"] == "450" and params["price_max"] == "1950" and params["limit"] == "50"
    assert post["headers"]["Sec-Fetch-Mode"] == "cors" and post["headers"]["Origin"] == "https://offerup.com"
    assert "ou.location=" in post["headers"]["Cookie"]
    # Second poll starts with the strategy that worked: no wasted page request.
    before = len(upstream.calls("GET", "/search"))
    await ing.poll()
    assert len(upstream.calls("GET", "/search")) == before
    assert len(upstream.calls("POST", "/api/graphql")) == 2


async def test_offerup_retries_primary_strategy_after_fallback_window(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text("<html></html>"))
    upstream.route("POST", "/api/graphql", jsonr({"data": {"modularFeed": ou_feed(OU_LISTING_TUF)}}))
    clock = StepClock(0.001)
    ing = offerup(ctx, upstream, profiles=["rtx_4090"], clock=clock)
    await ing.poll()
    assert ing._preferred == ou.STRATEGY_GRAPHQL
    clock.now += ou.PRIMARY_RETRY_SECONDS + 1
    upstream.route("GET", "/search", text(ou_page(ou_feed(OU_LISTING_FE))))
    listings = await ing.poll()
    assert [r.extra["via"] for r in listings] == ["page"]
    assert ing._preferred == ou.STRATEGY_PAGE


async def test_offerup_block_status_raises_source_blocked(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text("Forbidden", status=403))
    ing = offerup(ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceBlocked, match="http_403") as caught:
        await ing.poll()
    assert caught.value.cooldown_seconds == ou.BLOCK_COOLDOWN_SECONDS
    assert not upstream.calls("POST")  # a block is never "worked around" via the other endpoint


async def test_offerup_challenge_page_raises_source_blocked(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text(CLOUDFLARE_PAGE))
    ing = offerup(ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceBlocked, match="challenge"):
        await ing.poll()


async def test_offerup_block_mid_poll_returns_partial_then_raises(upstream: Upstream, ctx: IngestorContext) -> None:
    state = {"n": 0}

    async def flaky(request: web.Request) -> web.Response:
        state["n"] += 1
        if state["n"] == 1:
            return web.Response(text=ou_page(ou_feed(OU_LISTING_FE)), content_type="text/html")
        return web.Response(status=429, text="Too Many Requests")

    upstream.route("GET", "/search", flaky)
    ing = offerup(ctx, upstream)
    listings = await ing.poll()
    assert [r.source_id for r in listings] == [OU_LISTING_FE["listingId"]]
    assert len(upstream.calls()) == 2  # stopped issuing queries after the block
    with pytest.raises(SourceBlocked) as caught:
        await ing.poll()
    assert caught.value.cooldown_seconds is None  # 429 -> the source's configured cooldown
    assert len(upstream.calls()) == 2  # the deferred block is raised without touching upstream


async def test_offerup_query_failures_are_isolated(upstream: Upstream, ctx: IngestorContext) -> None:
    async def search(request: web.Request) -> web.Response:
        if request.query["q"] == "steam deck oled":
            return web.Response(status=404, text="gone")
        return web.Response(text=ou_page(ou_feed(OU_LISTING_FE)), content_type="text/html")

    upstream.route("GET", "/search", search)
    upstream.route("POST", "/api/graphql", jsonr({"errors": [{"message": "boom"}]}, status=400))
    ing = offerup(ctx, upstream)
    listings = await ing.poll()
    assert [r.source_id for r in listings] == [OU_LISTING_FE["listingId"]]
    assert len(upstream.calls("GET", "/search")) == 3


async def test_offerup_all_queries_failing_raises_source_error(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text("oops", status=404))
    upstream.route("POST", "/api/graphql", text("<html>bad gateway page</html>", status=200))
    ing = offerup(ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceError, match="all 1 offerup queries failed"):
        await ing.poll()


async def test_offerup_zip_only_config_geocodes_once(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("POST", "/api/graphql", jsonr(OU_GEOCODE))
    upstream.route("GET", "/search", text(ou_page(ou_feed(OU_LISTING_FE))))
    ing = offerup(ctx, upstream, latitude=None, longitude=None, zip_code="11216", profiles=["rtx_4090"])
    await ing.poll()
    await ing.poll()
    posts = upstream.calls("POST", "/api/graphql")
    assert len(posts) == 1
    assert posts[0]["json"]["operationName"] == "GeocodeLocation"
    assert posts[0]["json"]["variables"] == {"input": {"zipcode": "11216"}}
    cookie = json.loads(unquote(upstream.calls("GET", "/search")[0]["headers"]["Cookie"].split("=", 1)[1]))
    assert cookie == {"latitude": 40.68, "longitude": -73.94, "zipCode": "11216", "source": "user"}


async def test_offerup_geocode_failure_falls_back_to_zip_cookie(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("POST", "/api/graphql", text("upstream error", status=500))
    upstream.route("GET", "/search", text(ou_page(ou_feed(OU_LISTING_FE))))
    ing = offerup(ctx, upstream, latitude=None, longitude=None, zip_code="11216", profiles=["rtx_4090"])
    listings = await ing.poll()
    assert len(listings) == 1
    cookie = json.loads(unquote(upstream.calls("GET", "/search")[0]["headers"]["Cookie"].split("=", 1)[1]))
    assert cookie == {"zipCode": "11216", "source": "user"}
    await ing.poll()
    assert len(upstream.calls("POST", "/api/graphql")) == 1  # retried only after the back-off window


async def test_offerup_budget_rotates_queries_between_polls(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text(ou_page(ou_feed(OU_LISTING_FE))))
    # poll_timeout 10s -> budget 8s; every clock read advances 5s, so one query fits per poll.
    ing = offerup(ctx, upstream, poll_timeout_seconds=10, clock=StepClock(5.0))
    for _ in range(3):
        await ing.poll()
    assert [c["query"]["q"] for c in upstream.calls("GET", "/search")] == ["rtx 4090", "steam deck oled", "steam deck 1tb"]
    assert ctx.metrics.counter("offerup_budget_exhausted_total").value() == 3


async def test_offerup_run_once_applies_change_detection(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text(ou_page(ou_feed(OU_LISTING_FE, OU_LISTING_TUF))))
    ing = offerup(ctx, upstream, profiles=["rtx_4090"])
    first = await ing.run_once()
    second = await ing.run_once()
    assert len(first) == 2 and second == []
    assert all(r.node_id == "test-node" for r in first)


async def test_offerup_search_profiles_with_shipped_config(ctx: IngestorContext) -> None:
    config = load_config(Path(__file__).parents[1] / "config.yaml", env={})
    shipped = IngestorContext(http=ctx.http, metrics=Metrics(), config=config, node_id="n")
    ing = ou.OfferUpIngestor(OfferUpSource(enabled=True, zip_code="10001"), shipped)
    tasks = ou.build_tasks(ing.search_profiles())
    assert tasks and all(t.term and t.price_max and t.price_min is not None for t in tasks)
    assert all(t.price_min <= t.price_max for t in tasks if t.price_min is not None and t.price_max is not None)


# =========================================================================== Craigslist: pure parsing


def test_craigslist_normalize_site() -> None:
    assert cl.normalize_site("newyork") == "newyork"
    assert cl.normalize_site(" NewYork.craigslist.org ") == "newyork"
    assert cl.normalize_site("https://sfbay.craigslist.org/") == "sfbay"
    assert cl.normalize_site("") is None and cl.normalize_site("bad site!") is None


def test_craigslist_reference_tables() -> None:
    areas = cl.parse_areas(CL_AREAS)
    assert areas["newyork"] == cl.AreaInfo(3, "newyork", "US", 40.7143, -74.0059)
    assert areas["nyc"].area_id == 3 and areas["sfbay"].area_id == 1
    assert "broken" not in areas
    assert areas["toronto"].currency == "CAD" and areas["toronto"].country_code == "CA"
    assert areas["newyork"].currency == "USD"
    assert cl.parse_areas({"not": "a list"}) == {}
    assert cl.parse_categories(CL_CATEGORIES) == {7: "sop", 8: "sdp", 96: "ele"}


def test_craigslist_parse_geo() -> None:
    assert cl.parse_geo("1:2:3~40.71~-73.99") == (1, 2, 3, 40.71, -73.99)
    assert cl.parse_geo("2:2~40.72~-74.04") == (2, 2, None, 40.72, -74.04)
    assert cl.parse_geo(":3:1~1~2") == (0, 3, 1, 1.0, 2.0)
    assert cl.parse_geo("0") == (0, None, None, None, None)
    assert cl.parse_geo("0~999~1") == (0, None, None, None, 1.0)
    assert cl.parse_geo(None) == (None, None, None, None, None)


def _cl_ctx(**overrides: Any) -> cl.DecodeContext:
    params: dict[str, Any] = {
        "site": "newyork",
        "category": "sss",
        "categories": cl.parse_categories(CL_CATEGORIES),
        "query": "rtx 4090",
        "profile_hint": "rtx_4090",
    }
    params.update(overrides)
    return cl.DecodeContext(**params)


def test_craigslist_decode_compact_items() -> None:
    stats: dict[str, int] = {}
    listings = cl.decode_search_response(cl_sapi_payload(), _cl_ctx(), stats=stats)
    assert [r.source_id for r in listings] == [str(CL_MIN_POSTING_ID + n) for n in (123_456, 123_457, 123_458)]
    fe, pc, tuf = listings
    assert fe.source == "craigslist" and fe.source_kind is SourceKind.LOCAL
    fe_id = CL_MIN_POSTING_ID + 123_456
    assert fe.source_id == str(fe_id)
    assert fe.url == "https://www.craigslist.org/view/d/brooklyn-nvidia-rtx-4090-founders/fK7wQ2mZp9LsXy3Rt8VbNa"
    assert fe.title == "NVIDIA RTX 4090 Founders Edition"
    assert fe.price == 1400.0 and fe.currency == "USD"
    assert fe.posted_at == datetime.fromtimestamp(CL_MIN_POSTED + 99_000, tz=timezone.utc)
    assert fe.image_urls == [
        "https://images.craigslist.org/00a0a_jx892ZIFraf_0CI0qt_600x450.jpg",
        "https://images.craigslist.org/00b0b_kq77ABCdef_0CI0qt_600x450.jpg",
    ]
    assert fe.location is not None and fe.location.text == "Brooklyn"
    assert (fe.location.latitude, fe.location.longitude) == (40.671, -73.9814)
    assert fe.query == "rtx 4090" and fe.profile_hint == "rtx_4090"
    assert fe.extra == {
        "site": "newyork",
        "subarea": "brk",
        "category": "sop",
        "via": "sapi",
        "posting_id": fe_id,
        "category_id": 7,
        "price_text": "$1,400",
        "neighborhood": "Park Slope",
        "posting_key": "fK7wQ2mZp9LsXy3Rt8VbNa",
    }
    # unknown category id -> configured search path; "-1" price -> None (normalizer uses the title price)
    assert pc.url == f"https://newyork.craigslist.org/mnh/sss/d/manhattan-rtx-4090-gaming-pc/{CL_MIN_POSTING_ID + 123_457}.html"
    assert pc.price is None and pc.image_urls == [] and pc.extra["neighborhood"] == "Midtown"
    # "0" price falls back to the [10, "$1,250"] price label; the -2 dedupe flag is ignored
    assert tuf.price == 1250.0 and tuf.location is not None and tuf.location.text == "Jersey City"
    assert tuf.url.startswith("https://newyork.craigslist.org/jsy/sop/d/jersey-city-asus-tuf-4090/")
    assert stats == {"unusable": 2}


def test_craigslist_decode_object_items_and_empty_results() -> None:
    payload = {
        "data": {
            "items": [
                {
                    "postingId": 7_880_555_000,
                    "title": "Steam Deck OLED 1TB",
                    "price": 450,
                    "priceString": "$450",
                    "postedDate": 1_759_650_000,
                    "categoryAbbr": "vgm",
                    "seo": "brooklyn-steam-deck-oled-1tb",
                    "images": ["3:00d0d_abcdefghij_0CI0qt"],
                    "location": {"hostname": "newyork", "subareaAbbr": "brk", "description": "Brooklyn"},
                },
                {"postingId": "nope", "title": "x"},
            ]
        }
    }
    listings = cl.decode_search_response(payload, _cl_ctx())
    assert len(listings) == 1
    raw = listings[0]
    assert raw.url == "https://newyork.craigslist.org/brk/vgm/d/brooklyn-steam-deck-oled-1tb/7880555000.html"
    assert raw.price == 450.0 and raw.posted_at == datetime.fromtimestamp(1_759_650_000, tz=timezone.utc)
    assert raw.image_urls == ["https://images.craigslist.org/00d0d_abcdefghij_0CI0qt_600x450.jpg"]
    assert cl.decode_search_response({"data": {"items": [], "decode": 0, "totalResultCount": 0}, "errors": []}, _cl_ctx()) == []
    assert cl.decode_search_response({"data": {"totalResultCount": 0}}, _cl_ctx()) == []


def test_craigslist_decode_errors_raise() -> None:
    with pytest.raises(cl.CraigslistParseError):
        cl.decode_search_response({"errors": [{"message": "bad batch"}]}, _cl_ctx())
    with pytest.raises(cl.CraigslistParseError):
        cl.decode_search_response({"data": {"errors": "x"}, "errors": ["bad"]}, _cl_ctx())
    with pytest.raises(cl.CraigslistParseError):
        cl.decode_search_response({"data": {"items": "nope"}}, _cl_ctx())
    with pytest.raises(cl.CraigslistParseError):
        cl.decode_search_response([1, 2], _cl_ctx())


def test_craigslist_parse_search_html() -> None:
    stats: dict[str, int] = {}
    listings = cl.parse_search_html(CL_HTML, _cl_ctx(), base_url="https://newyork.craigslist.org", stats=stats)
    assert [r.source_id for r in listings] == ["7880123456", "7880123457"]
    fe, build = listings
    assert fe.url == "https://newyork.craigslist.org/brk/sop/d/brooklyn-nvidia-rtx-4090-fe/7880123456.html"
    assert fe.title == "NVIDIA RTX 4090 FE" and fe.price == 1400.0
    assert fe.location is not None and fe.location.text == "Brooklyn"
    assert fe.extra == {"via": "html", "site": "newyork", "price_text": "$1,400"}
    assert build.url == "https://newyork.craigslist.org/mnh/sys/d/new-york-rtx-4090-i9-build/7880123457.html"
    assert build.title == "RTX 4090 & i9 build" and build.price is None
    assert stats == {"unusable": 1}
    assert cl.parse_search_html('<ol class="cl-static-search-results"></ol>', _cl_ctx()) == []
    with pytest.raises(cl.CraigslistParseError):
        cl.parse_search_html("<html><body>something else</body></html>", _cl_ctx())


def test_craigslist_helpers() -> None:
    assert cl.extract_area_id(CL_HTML) == 3 and cl.extract_area_id("<html></html>") is None
    assert cl.image_url("3:00a0a_jx892ZIFraf_0CI0qt") == "https://images.craigslist.org/00a0a_jx892ZIFraf_0CI0qt_600x450.jpg"
    assert cl.image_url("3:../../etc") is None and cl.image_url(42) is None
    assert cl.posting_url("sfbay", None, "sop", None, 7_880_000_001) == "https://sfbay.craigslist.org/sop/7880000001.html"
    assert cl.coerce_price(0) is None and cl.coerce_price(-1) is None and cl.coerce_price("$0") is None
    assert cl.coerce_price("$1,250") == 1250.0 and cl.coerce_price("1200 obo") == "1200 obo"
    assert cl.detect_block(200, CL_BLOCK_PAGE) is not None and cl.detect_block(200, CL_HTML) is None
    assert cl.detect_block(403, "") == "http_403"
    assert cl.detect_block(403, CL_BLOCK_PAGE) == "wall:ip has been automatically blocked"
    assert cl.block_cooldown("wall:ip has been automatically blocked", 300.0) == cl.IP_BLOCK_COOLDOWN_SECONDS
    assert cl.block_cooldown("http_403", 300.0) == cl.BLOCK_COOLDOWN_SECONDS and cl.block_cooldown("http_429", 300.0) is None


def test_craigslist_query_params() -> None:
    task = cl.SearchTask("rtx_4090", "rtx 4090", 450, 1950)
    area = cl.AreaInfo(3, "newyork", "US", 40.7143, -74.0059)
    base = {
        "batch": "3-0-360-0-0",
        "cc": "US",
        "lang": "en",
        "searchPath": "sss",
        "query": "rtx 4090",
        "sort": "date",
        "srchType": "T",
    }
    with_postal = cl.sapi_params(task, area=area, category="sss", postal_code="11216", distance_miles=40)
    assert with_postal == {**base, "min_price": "450", "max_price": "1950", "postal": "11216", "search_distance": "40"}
    centred = cl.sapi_params(task, area=area, category="sss", postal_code=None, distance_miles=None)
    assert centred["lat"] == "40.71430" and centred["lon"] == "-74.00590" and centred["search_distance"] == "60"
    unknown_geo = cl.sapi_params(
        cl.SearchTask("p", "q", 0, None), area=cl.AreaInfo(3, "newyork"), category="sya", postal_code=None, distance_miles=10
    )
    assert "lat" not in unknown_geo and "min_price" not in unknown_geo and "max_price" not in unknown_geo
    assert cl.html_params(task, postal_code=None, distance_miles=40) == {
        "query": "rtx 4090",
        "sort": "date",
        "srchType": "T",
        "min_price": "450",
        "max_price": "1950",
    }


# =========================================================================== Craigslist: polling


def _cl_reference_routes(upstream: Upstream) -> None:
    upstream.route("GET", "/ref/Areas", jsonr(CL_AREAS))
    upstream.route("GET", "/ref/Categories", jsonr(CL_CATEGORIES))


async def test_craigslist_poll_uses_reference_and_search_api(upstream: Upstream, ctx: IngestorContext) -> None:
    _cl_reference_routes(upstream)
    upstream.route("GET", cl.SAPI_SEARCH_PATH, jsonr(cl_sapi_payload()))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    listings = await ing.poll()
    assert len(listings) == 3
    assert listings[0].url.startswith("https://www.craigslist.org/view/d/brooklyn-nvidia-rtx-4090-founders/")
    assert listings[2].url.startswith("https://newyork.craigslist.org/jsy/sop/d/")  # no token -> legacy form
    search = upstream.calls("GET", cl.SAPI_SEARCH_PATH)
    assert len(search) == 1
    assert search[0]["query"] == {
        "batch": "3-0-360-0-0",
        "cc": "US",
        "lang": "en",
        "searchPath": "sss",
        "query": "rtx 4090",
        "sort": "date",
        "srchType": "T",
        "min_price": "450",
        "max_price": "1950",
        "lat": "40.71430",
        "lon": "-74.00590",
        "search_distance": "40",
    }
    headers = search[0]["headers"]
    assert headers["Origin"] == upstream.url() and headers["Referer"] == upstream.url() + "/site/newyork/"
    assert headers["Sec-Fetch-Mode"] == "cors" and headers["User-Agent"].startswith("Mozilla/5.0")
    # Reference tables are fetched once per process.
    await ing.poll()
    assert len(upstream.calls("GET", "/ref/Areas")) == 1 and len(upstream.calls("GET", "/ref/Categories")) == 1


async def test_craigslist_postal_code_scopes_search(upstream: Upstream, ctx: IngestorContext) -> None:
    _cl_reference_routes(upstream)
    upstream.route("GET", cl.SAPI_SEARCH_PATH, jsonr(cl_sapi_payload()))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"], postal_code="11216", search_distance_miles=25)
    await ing.poll()
    query = upstream.calls("GET", cl.SAPI_SEARCH_PATH)[0]["query"]
    assert query["postal"] == "11216" and query["search_distance"] == "25" and "lat" not in query


async def test_craigslist_learns_area_from_html_when_reference_is_down(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/ref/Areas", text("unavailable", status=500))
    upstream.route("GET", "/ref/Categories", text("unavailable", status=500))
    upstream.route("GET", "/site/newyork/search/sss", text(CL_HTML))
    upstream.route("GET", cl.SAPI_SEARCH_PATH, jsonr(cl_sapi_payload()))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    first = await ing.poll()
    assert [r.extra["via"] for r in first] == ["html", "html"]
    html_call = upstream.calls("GET", "/site/newyork/search/sss")[0]
    assert html_call["query"] == {"query": "rtx 4090", "sort": "date", "srchType": "T", "min_price": "450", "max_price": "1950"}
    assert not upstream.calls("GET", cl.SAPI_SEARCH_PATH)
    second = await ing.poll()  # area id 3 learned from the page -> JSON API from now on
    assert {r.extra["via"] for r in second} == {"sapi"}
    sapi = upstream.calls("GET", cl.SAPI_SEARCH_PATH)[0]["query"]
    assert sapi["batch"] == "3-0-360-0-0" and "lat" not in sapi
    # categories unknown -> configured search path in legacy URLs; reference not re-fetched inside the retry window
    assert second[1].url.startswith("https://newyork.craigslist.org/mnh/sss/d/")
    assert len(upstream.calls("GET", "/ref/Areas")) == 1


async def test_craigslist_falls_back_to_html_when_api_fails(upstream: Upstream, ctx: IngestorContext) -> None:
    _cl_reference_routes(upstream)
    upstream.route("GET", cl.SAPI_SEARCH_PATH, text("<html>oops</html>", status=200))
    upstream.route("GET", "/site/newyork/search/sss", text(CL_HTML))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    listings = await ing.poll()
    assert {r.extra["via"] for r in listings} == {"html"}
    assert ing._preferred == cl.MODE_HTML
    counter = ctx.metrics.counter("craigslist_queries_total", "", ("mode", "outcome"))
    assert counter.value(mode="sapi", outcome="error") == 1 and counter.value(mode="html", outcome="ok") == 1


async def test_craigslist_block_raises_source_blocked(upstream: Upstream, ctx: IngestorContext) -> None:
    _cl_reference_routes(upstream)
    upstream.route("GET", cl.SAPI_SEARCH_PATH, text(CL_BLOCK_PAGE, status=403))
    upstream.route("GET", "/site/newyork/search/sss", text(CL_HTML))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceBlocked, match="automatically blocked") as caught:
        await ing.poll()
    assert caught.value.cooldown_seconds == cl.IP_BLOCK_COOLDOWN_SECONDS
    assert not upstream.calls("GET", "/site/newyork/search/sss")  # never route around a block


async def test_craigslist_block_wall_on_html_page(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/ref/Areas", text("unavailable", status=500))
    upstream.route("GET", "/ref/Categories", jsonr(CL_CATEGORIES))
    upstream.route("GET", "/site/newyork/search/sss", text(CL_BLOCK_PAGE))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceBlocked, match="wall"):
        await ing.poll()


async def test_craigslist_multi_site_dedupes_and_isolates_failures(upstream: Upstream, ctx: IngestorContext) -> None:
    _cl_reference_routes(upstream)

    async def sapi(request: web.Request) -> web.Response:
        if request.query["batch"].startswith("1-"):
            return web.Response(status=404, text="no such area")
        return web.json_response(cl_sapi_payload())

    upstream.route("GET", cl.SAPI_SEARCH_PATH, sapi)
    upstream.route("GET", "/site/sfbay/search/sss", text("gone", status=404))
    ing = craigslist(ctx, upstream, sites=["newyork", "https://sfbay.craigslist.org", "newyork.craigslist.org", "bad site"])
    assert ing.sites == ["newyork", "sfbay"]
    listings = await ing.poll()
    # 3 terms x 2 sites; sfbay fails every time but newyork results survive (deduplicated across terms).
    assert len(upstream.calls("GET", cl.SAPI_SEARCH_PATH)) == 6
    assert len(listings) == 3


async def test_craigslist_budget_rotation(upstream: Upstream, ctx: IngestorContext) -> None:
    clock = StepClock(0.0)  # time only moves when the fake search API "takes" 9 s per request

    async def slow_sapi(request: web.Request) -> web.Response:
        clock.now += 9.0
        return web.json_response(cl_sapi_payload())

    _cl_reference_routes(upstream)
    upstream.route("GET", cl.SAPI_SEARCH_PATH, slow_sapi)
    ing = craigslist(ctx, upstream, poll_timeout_seconds=10, clock=clock)  # budget 8 s -> one query per poll
    for _ in range(3):
        await ing.poll()
    queries = [c["query"]["query"] for c in upstream.calls("GET", cl.SAPI_SEARCH_PATH)]
    assert queries == ["rtx 4090", "steam deck oled", "steam deck 1tb"]
    assert ctx.metrics.counter("craigslist_budget_exhausted_total").value() == 3


async def test_craigslist_all_queries_failing_raises_source_error(upstream: Upstream, ctx: IngestorContext) -> None:
    _cl_reference_routes(upstream)
    upstream.route("GET", cl.SAPI_SEARCH_PATH, text("err", status=404))
    upstream.route("GET", "/site/newyork/search/sss", text("err", status=404))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceError, match="all 1 craigslist queries failed"):
        await ing.poll()


def test_registry_resolves_local_sources() -> None:
    from deal_radar.sources.registry import load_ingestor_class

    assert load_ingestor_class("offerup") is ou.OfferUpIngestor
    assert load_ingestor_class("craigslist") is cl.CraigslistIngestor
    assert ou.OfferUpIngestor.kind is SourceKind.LOCAL and cl.CraigslistIngestor.kind is SourceKind.LOCAL


# =========================================================================== adversarial-review regressions
#
# Shapes below come from 2026 recordings of craigslist.org (static result list with
# ``www.craigslist.org/view/d/<slug>/<token>`` links, ``[13, token]`` sapi tag) and from
# OfferUp web-app GraphQL clients (``x-ou-operation-name`` / ``x-ou-d-token`` headers,
# ``isRemoved``/``state`` on listing objects).

CL_TOKEN = "sznH9vDet1Yzc5uUyjGWXA"

CL_HTML_2026 = """<!DOCTYPE html>
<html><head><title>for sale "rtx 4090" near Brooklyn, NY 11216 - craigslist</title>
<script>window.cl.init('https://www.craigslist.org/static/www/', '', 'www', 'search',
{'initialCategoryAbbr': "sss",
'location': {"radius":40,"region":"NY","type":"postal","lat":40.68,"lon":-73.94,"postal":"11216","city":"Brooklyn","country":"US","areaId":3,"v":1}}, 0);</script>
</head><body>
<ol class="cl-static-search-results">
    <li class="cl-static-hub-links"><div>see also</div>
        <p><a href="https://www.craigslist.org/search/city/brooklyn-ny?hub=computers&amp;postal=11216">computers</a></p>
    </li>
    <li class="cl-static-search-result" title="NVIDIA RTX 4090 FE">
        <a href="https://www.craigslist.org/view/d/brooklyn-nvidia-rtx-4090-fe/sznH9vDet1Yzc5uUyjGWXA">
            <div class="title">NVIDIA RTX 4090 FE</div>
            <div class="details">
                <div class="price">$1,400</div>
                <div class="location">Brooklyn</div>
            </div>
        </a>
    </li>
    <li class="cl-static-search-result" title="RTX 4090 gaming PC">
        <a href="/view/d/manhattan-rtx-4090-gaming-pc/11QDBnUSTTgQuQ5AXSoAWa">
            <div class="title">RTX 4090 gaming PC</div>
            <div class="details"><div class="price">$3,200</div></div>
        </a>
    </li>
</ol>
</body></html>"""


def cl_sapi_payload_2026() -> dict[str, Any]:
    payload = cl_sapi_payload()
    items = payload["data"]["items"]
    items[0] = [
        123_456, 99_000, 7, 1400, "0:0:0~40.6710~-73.9814", "a1b2c3",
        [13, CL_TOKEN], [4, "3:00a0a_jx892ZIFraf_0CI0qt"], [6, "brooklyn-nvidia-rtx-4090-fe"], [10, "$1,400"],
        "NVIDIA RTX 4090 FE",
    ]
    return payload


def test_craigslist_compact_item_uses_view_url_when_posting_key_present() -> None:
    listings = cl.decode_search_response(cl_sapi_payload_2026(), _cl_ctx())
    fe = listings[0]
    assert fe.source_id == str(CL_MIN_POSTING_ID + 123_456)  # numeric posting id stays the identity
    assert fe.url == f"https://www.craigslist.org/view/d/brooklyn-nvidia-rtx-4090-fe/{CL_TOKEN}"
    assert fe.extra["posting_key"] == CL_TOKEN
    # Items without a usable [13, key] keep the classic host/subarea/category URL.
    assert listings[1].url.startswith("https://newyork.craigslist.org/mnh/sss/d/manhattan-rtx-4090-gaming-pc/")
    assert "posting_key" not in listings[1].extra


def test_craigslist_compact_item_reads_tags_after_the_title() -> None:
    item = [
        123_460, 5, 7, 900, "0:0~40.7~-74.0", "x9y8z7", [6, "brooklyn-rtx-4090"], "RTX 4090 blower",
        [4, "3:00e0e_abcdefgh12_0CI0qt"], [10, "$900"],
    ]
    decode = cl._Decode.from_payload(cl_sapi_payload()["data"]["decode"])
    raw = cl.decode_compact_item(item, decode, _cl_ctx())
    assert raw is not None and raw.title == "RTX 4090 blower"
    assert raw.image_urls == ["https://images.craigslist.org/00e0e_abcdefgh12_0CI0qt_600x450.jpg"]
    assert raw.extra["price_text"] == "$900"


def test_craigslist_parse_search_html_2026_view_links() -> None:
    stats: dict[str, int] = {}
    listings = cl.parse_search_html(CL_HTML_2026, _cl_ctx(), base_url="https://www.craigslist.org/search/sss", stats=stats)
    assert [r.source_id for r in listings] == [CL_TOKEN, "11QDBnUSTTgQuQ5AXSoAWa"]
    fe, pc = listings
    assert fe.url == f"https://www.craigslist.org/view/d/brooklyn-nvidia-rtx-4090-fe/{CL_TOKEN}"
    assert fe.title == "NVIDIA RTX 4090 FE" and fe.price == 1400.0
    assert fe.extra["posting_key"] == CL_TOKEN and fe.extra["site"] == "newyork"  # never "www"
    assert pc.url == "https://www.craigslist.org/view/d/manhattan-rtx-4090-gaming-pc/11QDBnUSTTgQuQ5AXSoAWa"
    assert stats == {}
    assert cl.extract_area_id(CL_HTML_2026) == 3


def test_craigslist_parse_search_html_raises_when_every_row_is_unparseable() -> None:
    # Result rows exist but none can be mapped (link scheme drifted): that is a parse
    # failure, not an empty result, so the mode logic must not mark HTML as "working".
    drifted = CL_HTML_2026.replace("/view/d/", "/posting/")
    with pytest.raises(cl.CraigslistParseError):
        cl.parse_search_html(drifted, _cl_ctx())


async def test_craigslist_listing_identity_is_stable_across_modes(upstream: Upstream, ctx: IngestorContext) -> None:
    # Reference data down: first poll runs on the static page (token links), learns the
    # area id, and the next poll uses the JSON API (numeric ids). The same posting must
    # keep one listing key, otherwise every listing would be re-alerted after the switch.
    upstream.route("GET", "/ref/Areas", text("unavailable", status=500))
    upstream.route("GET", "/ref/Categories", text("unavailable", status=500))
    upstream.route("GET", "/site/newyork/search/sss", text(CL_HTML_2026))
    upstream.route("GET", cl.SAPI_SEARCH_PATH, jsonr(cl_sapi_payload_2026()))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    first = await ing.run_once()
    assert {r.extra["via"] for r in first} == {"html"}
    assert CL_TOKEN in {r.source_id for r in first}
    second = await ing.poll()
    assert {r.extra["via"] for r in second} == {"sapi"}
    by_key = {r.extra.get("posting_key"): r for r in second}
    assert by_key[CL_TOKEN].source_id == CL_TOKEN  # first identity wins
    fresh = ing.select_changed(second)
    assert CL_TOKEN not in {r.source_id for r in fresh}  # unchanged listing not re-emitted


async def test_craigslist_html_mode_reuses_numeric_ids_learned_from_api(upstream: Upstream, ctx: IngestorContext) -> None:
    _cl_reference_routes(upstream)
    state = {"api_up": True}

    async def sapi(request: web.Request) -> web.Response:
        if state["api_up"]:
            return web.json_response(cl_sapi_payload_2026())
        return web.Response(text="<html>maintenance</html>", content_type="text/html")

    upstream.route("GET", cl.SAPI_SEARCH_PATH, sapi)
    upstream.route("GET", "/site/newyork/search/sss", text(CL_HTML_2026))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    api = await ing.poll()
    numeric = next(r.source_id for r in api if r.extra.get("posting_key") == CL_TOKEN)
    state["api_up"] = False
    html = await ing.poll()
    assert {r.extra["via"] for r in html} == {"html"}
    assert next(r.source_id for r in html if r.extra.get("posting_key") == CL_TOKEN) == numeric


async def test_craigslist_html_relative_links_resolve_against_final_url(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/ref/Areas", text("unavailable", status=500))
    upstream.route("GET", "/ref/Categories", text("unavailable", status=500))

    async def redirect(request: web.Request) -> web.StreamResponse:
        raise web.HTTPFound(location=f"/www/search/city/brooklyn-ny?{request.query_string}")

    upstream.route("GET", "/site/newyork/search/sss", redirect)
    upstream.route("GET", "/www/search/city/brooklyn-ny", text(CL_HTML_2026))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    listings = await ing.poll()
    pc = next(r for r in listings if r.source_id == "11QDBnUSTTgQuQ5AXSoAWa")
    assert pc.url == upstream.url("/view/d/manhattan-rtx-4090-gaming-pc/11QDBnUSTTgQuQ5AXSoAWa")


async def test_craigslist_cancellation_mid_request_propagates_and_next_poll_recovers(
    upstream: Upstream, ctx: IngestorContext
) -> None:
    import asyncio

    _cl_reference_routes(upstream)
    gate = {"slow": True}

    async def sapi(request: web.Request) -> web.Response:
        if gate["slow"]:
            await asyncio.sleep(5)
        return web.json_response(cl_sapi_payload())

    upstream.route("GET", cl.SAPI_SEARCH_PATH, sapi)
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    task = asyncio.create_task(ing.poll())
    while not upstream.calls("GET", cl.SAPI_SEARCH_PATH):
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    gate["slow"] = False
    assert len(await ing.poll()) == 3


async def test_offerup_graphql_403_on_geocode_does_not_block_the_source(upstream: Upstream, ctx: IngestorContext) -> None:
    # OfferUp's GraphQL is token-gated for anonymous clients; a bare 403 there is not an
    # IP block. The ZIP cookie still scopes the SSR search page, which works.
    upstream.route("POST", "/api/graphql", jsonr({"errors": [{"message": "Forbidden"}]}, status=403))
    upstream.route("GET", "/search", text(ou_page(ou_feed(OU_LISTING_FE))))
    ing = offerup(ctx, upstream, latitude=None, longitude=None, zip_code="11216", profiles=["rtx_4090"])
    listings = await ing.poll()
    assert [r.source_id for r in listings] == [OU_LISTING_FE["listingId"]]
    await ing.poll()
    assert len(upstream.calls("POST", "/api/graphql")) == 1  # geocoding backs off instead of retrying every poll


async def test_offerup_gated_graphql_fallback_is_not_reported_as_block(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text("<html><body><div id='__next'></div></body></html>"))  # degraded SSR
    upstream.route("POST", "/api/graphql", jsonr({"errors": [{"message": "Forbidden"}]}, status=403))
    ing = offerup(ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceError) as caught:
        await ing.poll()
    assert not isinstance(caught.value, SourceBlocked)
    # The gated endpoint is parked: the next poll does not hit it again.
    with pytest.raises(SourceError):
        await ing.poll()
    assert len(upstream.calls("POST", "/api/graphql")) == 1
    assert len(upstream.calls("GET", "/search")) == 2


async def test_offerup_graphql_challenge_wall_is_still_a_block(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text("<html><body><div id='__next'></div></body></html>"))
    upstream.route("POST", "/api/graphql", text(CLOUDFLARE_PAGE, status=403))
    ing = offerup(ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceBlocked):
        await ing.poll()


async def test_offerup_graphql_sends_web_app_headers(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text("<html></html>"))
    upstream.route("POST", "/api/graphql", jsonr({"data": {"modularFeed": ou_feed(OU_LISTING_TUF)}}))
    ing = offerup(ctx, upstream, profiles=["rtx_4090"])
    await ing.poll()
    await ing.poll()
    first, second = ({k.lower(): v for k, v in c["headers"].items()} for c in upstream.calls("POST", "/api/graphql"))
    for headers in (first, second):
        assert headers["x-ou-operation-name"] == "GetModularFeed"
        assert headers["x-ou-d-token"].startswith("web-") and len(headers["x-ou-d-token"]) == 4 + 56
        assert headers["ou-session-id"].startswith(headers["x-ou-d-token"] + "@")
        assert len(headers["x-request-id"]) == 36
        # fetch()-style call: CORS fetch metadata, no navigation-only headers.
        assert headers["sec-fetch-mode"] == "cors" and headers["sec-fetch-dest"] == "empty"
        assert "sec-fetch-user" not in headers and "upgrade-insecure-requests" not in headers
    # Device token is sticky per ingestor (like a browser profile); request ids are not.
    assert first["x-ou-d-token"] == second["x-ou-d-token"]
    assert first["x-request-id"] != second["x-request-id"]


def test_offerup_removed_listing_is_marked_unavailable() -> None:
    removed = dict(OU_LISTING_TUF, isRemoved=True, state="SOLD")
    raw = ou.parse_listing(removed)
    assert raw is not None and raw.in_stock is False and raw.extra["state"] == "SOLD"
    live = ou.parse_listing(dict(OU_LISTING_TUF, isRemoved=False))
    assert live is not None and live.in_stock is None


def test_offerup_protocol_relative_image_urls_are_kept() -> None:
    raw = ou.parse_listing(dict(OU_LISTING_TUF, image={"url": "//images.offerup.com/XyZ=/250x250/a1b2.jpg"}))
    assert raw is not None and raw.image_urls == ["https://images.offerup.com/XyZ=/250x250/a1b2.jpg"]


async def test_offerup_cancellation_mid_request_propagates_and_next_poll_recovers(
    upstream: Upstream, ctx: IngestorContext
) -> None:
    import asyncio

    gate = {"slow": True}

    async def search(request: web.Request) -> web.Response:
        if gate["slow"]:
            await asyncio.sleep(5)
        return web.Response(text=ou_page(ou_feed(OU_LISTING_FE)), content_type="text/html")

    upstream.route("GET", "/search", search)
    ing = offerup(ctx, upstream, profiles=["rtx_4090"])
    task = asyncio.create_task(ing.poll())
    while not upstream.calls("GET", "/search"):
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    gate["slow"] = False
    assert len(await ing.poll()) == 1


HCAPTCHA_PAGE = (
    "<html><head><title>craigslist | verification</title>"
    '<script src="https://js.hcaptcha.com/1/api.js" async defer></script></head>'
    '<body><form><div class="h-captcha" data-sitekey="x"></div></form></body></html>'
)


async def test_craigslist_api_calls_use_cors_fetch_metadata(upstream: Upstream, ctx: IngestorContext) -> None:
    _cl_reference_routes(upstream)
    upstream.route("GET", cl.SAPI_SEARCH_PATH, jsonr(cl_sapi_payload()))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    await ing.poll()
    for call in (*upstream.calls("GET", cl.SAPI_SEARCH_PATH), *upstream.calls("GET", "/ref/Areas")):
        headers = {k.lower(): v for k, v in call["headers"].items()}
        assert headers["sec-fetch-mode"] == "cors" and headers["sec-fetch-dest"] == "empty"
        assert "sec-fetch-user" not in headers and "upgrade-insecure-requests" not in headers
    assert {k.lower(): v for k, v in upstream.calls("GET", cl.SAPI_SEARCH_PATH)[0]["headers"].items()}[
        "sec-fetch-site"
    ] == "same-site"


async def test_craigslist_hcaptcha_wall_served_as_503_is_a_block(upstream: Upstream, ctx: IngestorContext) -> None:
    _cl_reference_routes(upstream)
    upstream.route("GET", cl.SAPI_SEARCH_PATH, text(HCAPTCHA_PAGE, status=503))
    upstream.route("GET", "/site/newyork/search/sss", text(CL_HTML))
    ing = craigslist(ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceBlocked, match="captcha") as caught:
        await ing.poll()
    assert caught.value.cooldown_seconds == cl.IP_BLOCK_COOLDOWN_SECONDS
    assert not upstream.calls("GET", "/site/newyork/search/sss")  # never routed around


async def test_offerup_challenge_served_as_503_is_a_block_not_a_fallback(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("GET", "/search", text(CLOUDFLARE_PAGE, status=503))
    upstream.route("POST", "/api/graphql", jsonr({"data": {"modularFeed": ou_feed(OU_LISTING_TUF)}}))
    ing = offerup(ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceBlocked, match="challenge"):
        await ing.poll()
    assert not upstream.calls("POST")


def test_offerup_hcaptcha_detected_and_job_tiles_skipped() -> None:
    assert ou.detect_block(200, HCAPTCHA_PAGE) is not None
    feed = ou_feed(OU_LISTING_FE)
    feed["looseTiles"].append(
        {"__typename": "ModularFeedTileJob", "tileId": "j-1", "tileType": "JOB",
         "job": {"listingId": "job-1", "title": "Warehouse associate"}}
    )
    listings = ou.parse_graphql_feed({"data": {"modularFeed": feed}})
    assert [r.source_id for r in listings] == [OU_LISTING_FE["listingId"]]


def test_offerup_graphql_feed_reads_module_grid_tiles() -> None:
    assert "modules {" in ou.FEED_QUERY and "ModularFeedModuleGrid" in ou.FEED_QUERY
    feed = {
        "looseTiles": [],
        "modules": [
            {
                "__typename": "ModularFeedModuleGrid",
                "grid": {"tiles": [{"__typename": "ModularFeedTileListing", "tileType": "LISTING", "listing": OU_LISTING_TUF}]},
            }
        ],
        "pageCursor": None,
    }
    assert [r.source_id for r in ou.parse_graphql_feed({"data": {"modularFeed": feed}})] == ["1234567890"]


def test_offerup_graphql_params_include_zipcode_when_configured() -> None:
    task = ou.SearchTask("rtx_4090", "rtx 4090", 450, 1950)
    params = {p["key"]: p["value"] for p in ou.graphql_search_params(
        task, radius_miles=30, limit=50, coordinates=None, session_id="s", zip_code=" 11216-1234 "
    )}
    assert params["zipcode"] == "11216" and "lat" not in params
    bad = {p["key"] for p in ou.graphql_search_params(task, radius_miles=30, limit=50, coordinates=None, session_id="s", zip_code="NW1")}
    assert "zipcode" not in bad


def _garbage(rng: Any, depth: int = 0) -> Any:
    """Deterministic pseudo-random JSON-ish value (mixed types, nesting, extreme numbers)."""
    choice = rng.randrange(12 if depth < 4 else 7)
    if choice == 0:
        return None
    if choice == 1:
        return rng.choice([True, False])
    if choice == 2:
        return rng.choice([0, -1, -2, 1, 2**63, -(2**63), 7_880_000_000, 1_759_600_000])
    if choice == 3:
        return rng.choice([0.0, float("nan"), float("inf"), -1.5, 1e300])
    if choice == 4:
        return rng.choice(["", " ", "0", "-1", "abc", "$1,200", "0:0:0~x~y", "3:..", "<script>", "9" * 40, ":::~~~"])
    if choice == 5:
        return rng.choice(["listingId", "title", "price", "looseTiles", "AD_1P", "Tile Ad"])
    if choice == 6:
        return {}
    if choice in (7, 8):
        return [_garbage(rng, depth + 1) for _ in range(rng.randrange(8))]
    keys = ["listingId", "title", "price", "image", "photos", "owner", "locationName", "locationDetails", "tileType",
            "__typename", "listing", "postDate", "flags", "isRemoved", "state", "items", "decode", "data", "minPostingId",
            "minPostedDate", "locations", "locationDescriptions", "neighborhoods", "postingId", "location", "images", "seo"]
    return {rng.choice(keys): _garbage(rng, depth + 1) for _ in range(rng.randrange(1, 7))}


def test_pure_parsers_survive_malformed_payloads() -> None:
    import random

    rng = random.Random(20261006)
    ctx_ = _cl_ctx()
    for _ in range(1500):
        blob = _garbage(rng)
        # OfferUp
        try:
            ou.parse_graphql_feed({"data": {"modularFeed": blob}} if rng.random() < 0.7 else blob)
        except ou.OfferUpParseError:
            pass
        page = ou_page(None, page_props={"searchFeedResponse": blob} if rng.random() < 0.5 else {"x": blob})
        try:
            ou.parse_search_page(page)
        except ou.OfferUpParseError:
            pass
        ou.parse_geocode(blob)
        if isinstance(blob, dict):
            ou.parse_listing(blob)
        # Craigslist
        payload = {"data": {"items": blob if isinstance(blob, list) else [blob], "decode": _garbage(rng)}}
        try:
            cl.decode_search_response(payload if rng.random() < 0.8 else blob, ctx_)
        except cl.CraigslistParseError:
            pass
        item = [rng.randrange(-5, 10**6), _garbage(rng), _garbage(rng), _garbage(rng), _garbage(rng)]
        item += [_garbage(rng) for _ in range(rng.randrange(6))]
        cl.decode_compact_item(item, cl._Decode.from_payload(_garbage(rng)), ctx_)
        cl.parse_geo(_garbage(rng))
        cl.parse_areas(blob)
        cl.parse_categories(blob)


@pytest.fixture
async def fast_timeout_ctx():
    settings = NetworkSettings(
        retry=BackoffPolicy(max_attempts=1, base_delay=0, max_delay=0), trust_env=False, timeout_seconds=0.3
    )
    http = HttpClient.create(settings)
    yield IngestorContext(http=http, metrics=Metrics(), config=make_config(), node_id="test-node")
    await http.close()


async def test_craigslist_request_timeout_falls_back_then_fails_cleanly(
    upstream: Upstream, fast_timeout_ctx: IngestorContext
) -> None:
    import asyncio

    _cl_reference_routes(upstream)
    state = {"html_slow": False}

    async def slow_sapi(request: web.Request) -> web.Response:
        await asyncio.sleep(2)
        return web.json_response(cl_sapi_payload())

    async def html(request: web.Request) -> web.Response:
        if state["html_slow"]:
            await asyncio.sleep(2)
        return web.Response(text=CL_HTML_2026, content_type="text/html")

    upstream.route("GET", cl.SAPI_SEARCH_PATH, slow_sapi)
    upstream.route("GET", "/site/newyork/search/sss", html)
    ing = craigslist(fast_timeout_ctx, upstream, profiles=["rtx_4090"])
    listings = await ing.poll()  # API timed out (not a block) -> static page served the query
    assert {r.extra["via"] for r in listings} == {"html"}
    state["html_slow"] = True
    with pytest.raises(SourceError, match="all 1 craigslist queries failed") as caught:
        await ing.poll()
    assert not isinstance(caught.value, SourceBlocked)


async def test_offerup_request_timeout_is_a_query_failure_not_a_block(
    upstream: Upstream, fast_timeout_ctx: IngestorContext
) -> None:
    import asyncio

    async def slow(request: web.Request) -> web.Response:
        await asyncio.sleep(2)
        return web.Response(text="late", content_type="text/html")

    upstream.route("GET", "/search", slow)
    upstream.route("POST", "/api/graphql", slow)
    ing = offerup(fast_timeout_ctx, upstream, profiles=["rtx_4090"])
    with pytest.raises(SourceError) as caught:
        await ing.poll()
    assert not isinstance(caught.value, SourceBlocked)


async def test_offerup_gated_geocode_also_parks_graphql_fallback(upstream: Upstream, ctx: IngestorContext) -> None:
    upstream.route("POST", "/api/graphql", jsonr({"errors": [{"message": "Forbidden"}]}, status=403))
    upstream.route("GET", "/search", text("<html><body><div id='__next'></div></body></html>"))  # degraded SSR
    ing = offerup(ctx, upstream, latitude=None, longitude=None, zip_code="11216", profiles=["rtx_4090"])
    with pytest.raises(SourceError) as caught:
        await ing.poll()
    assert not isinstance(caught.value, SourceBlocked)
    posts = upstream.calls("POST", "/api/graphql")
    assert [p["json"]["operationName"] for p in posts] == ["GeocodeLocation"]  # no second gated call in the same poll
