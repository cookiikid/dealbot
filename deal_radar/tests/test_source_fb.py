"""Tests for the Facebook Marketplace source and the shared Playwright stealth helpers.

Pure parsing/URL/config helpers are tested offline against fixtures that mirror the
shapes Facebook serves (Relay GraphQL with the ``for (;;);`` prefix, ``@defer``
chunks, server-rendered preloader JSON). Browser tests drive a real headless
Chromium against a local aiohttp "marketplace" and skip cleanly when no Chromium
can be launched. No test talks to facebook.com.
"""

from __future__ import annotations

import asyncio
import glob
import json
import os
import random
import stat
import time
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from deal_radar.config_schema import AppConfig, BrowserSection, FbMarketplaceSource, GeoPin
from deal_radar.core.http import HttpClient, NetworkSettings
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import SourceKind
from deal_radar.sources import fb_marketplace as fb
from deal_radar.sources import stealth
from deal_radar.sources.base import IngestorContext, SourceAuthError, SourceBlocked, SourceError

FIXTURES = Path(__file__).parent / "fixtures" / "fb"
GRAPHQL_PAGE1 = (FIXTURES / "search_graphql.txt").read_text(encoding="utf-8")
GRAPHQL_PAGE2 = (FIXTURES / "search_graphql_page2.txt").read_text(encoding="utf-8")
SSR_SCRIPT = (FIXTURES / "search_ssr_script.json").read_text(encoding="utf-8")


# --------------------------------------------------------------------------- helpers


def _geo(**overrides: Any) -> GeoPin:
    base = {
        "latitude": 40.7128,
        "longitude": -74.006,
        "city_slug": "nyc",
        "radius_km": 60,
        "timezone_id": "America/New_York",
        "locale": "en-US",
    }
    base.update(overrides)
    return GeoPin(**base)


def _app_config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "profiles": [
                {
                    "id": "rtx3090",
                    "name": "RTX 3090",
                    "category": "gpu",
                    "match": {"any": [r"\b3090\b"]},
                    "price": {"reference_used": 750, "floor": 300, "target": 600, "ceiling": 900},
                    "search": {"terms": ["rtx 3090"]},
                },
                {
                    "id": "steamdeck",
                    "name": "Steam Deck OLED",
                    "category": "handheld",
                    "match": {"any": ["steam deck"]},
                    "price": {"reference_used": 480, "floor": 200, "target": 380, "ceiling": 600},
                    "search": {"terms": ["steam deck oled"], "price_min": 250},
                },
                {
                    "id": "multi",
                    "name": "Multi term",
                    "category": "misc",
                    "match": {"any": ["alpha|beta|gamma"]},
                    "price": {"reference_used": 100, "floor": 10, "target": 50, "ceiling": 200},
                    "search": {"terms": ["alpha", "beta", "ALPHA", "gamma"]},
                },
            ]
        }
    )


def _write_state(path: Path, *, expires: float | None = None, cookies: bool = True) -> None:
    exp = expires if expires is not None else time.time() + 30 * 86400
    jar = (
        [
            {"name": "c_user", "value": "100012345678901", "domain": ".facebook.com", "path": "/", "expires": exp,
             "httpOnly": False, "secure": True, "sameSite": "None"},
            {"name": "xs", "value": "33%3AabcDEF%3A2%3A1759700000%3A-1%3A-1", "domain": ".facebook.com", "path": "/",
             "expires": exp, "httpOnly": True, "secure": True, "sameSite": "None"},
            {"name": "datr", "value": "AbCdEfGhIjKlMnOp", "domain": ".facebook.com", "path": "/", "expires": exp,
             "httpOnly": True, "secure": True, "sameSite": "None"},
        ]
        if cookies
        else []
    )
    path.write_text(json.dumps({"cookies": jar, "origins": []}), encoding="utf-8")


def _find_executable(build: str) -> str | None:
    if build == "full" and os.environ.get("DEALRADAR_TEST_CHROMIUM"):
        return os.environ["DEALRADAR_TEST_CHROMIUM"]
    root = os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "/opt/pw-browsers"
    pattern = {
        "full": "chromium-*/chrome-linux*/chrome",
        "shell": "chromium_headless_shell-*/chrome-linux*/*headless_shell",
    }[build]
    matches = sorted(glob.glob(os.path.join(root, pattern)))
    return matches[-1] if matches else None


_LAUNCHABLE: dict[str, str | None] = {}


async def _require_chromium(build: str = "full") -> str | None:
    """Executable path for ``build`` (None = Playwright default) or skip the test."""
    if build in _LAUNCHABLE:
        if _LAUNCHABLE[build] == "__unavailable__":
            pytest.skip(f"no launchable Chromium ({build})")
        return _LAUNCHABLE[build]
    try:
        from playwright.async_api import async_playwright
    except ImportError:  # pragma: no cover
        pytest.skip("playwright not installed")
    candidates = [_find_executable(build)] if build == "shell" else [_find_executable("full"), _find_executable("shell"), None]
    for exe in dict.fromkeys(candidates):
        if exe is None and build == "shell":
            continue
        try:
            async with async_playwright() as pw:
                browser = await stealth.launch_browser(pw, BrowserSection(headless=True, executable_path=exe))
                await browser.close()
        except Exception:  # noqa: BLE001, S112 - any launch problem means "try the next build, else skip"
            continue
        _LAUNCHABLE[build] = exe
        return exe
    _LAUNCHABLE[build] = "__unavailable__"
    pytest.skip(f"no launchable Chromium ({build})")


@pytest.fixture
async def http():
    client = HttpClient.create(NetworkSettings(trust_env=False))
    yield client
    await client.close()


def _ctx(http: HttpClient, config: AppConfig, metrics: Metrics | None = None) -> IngestorContext:
    return IngestorContext(http=http, metrics=metrics or Metrics(), config=config, node_id="test-node", rng=random.Random(7))


def _fb_cfg(tmp_path: Path, exe: str | None, **overrides: Any) -> FbMarketplaceSource:
    data: dict[str, Any] = {
        "enabled": True,
        "location": _geo(),
        "browser": BrowserSection(
            headless=True,
            executable_path=exe,
            storage_state_path=str(tmp_path / "fb_state.json"),
            min_action_delay_seconds=0.0,
            max_action_delay_seconds=0.02,
            navigation_timeout_seconds=20,
        ),
        "scrolls_per_query": 1,
        "max_listings_per_query": 40,
        "poll_timeout_seconds": 90,
        "checkpoint_pause_minutes": 180,
    }
    data.update(overrides)
    return FbMarketplaceSource(**data)


# --------------------------------------------------------------------------- JSON framing


def test_iter_json_documents_handles_prefix_ndjson_and_garbage() -> None:
    body = (
        '﻿for (;;);{"a":1}\n'
        "\n"
        'for (;;);{"b":2}{"c":3}\n'
        "<html>not json</html>\n"
        '{"truncated": [1, 2\n'
        '[{"d":4}]\n'
    )
    assert list(fb.iter_json_documents(body)) == [{"a": 1}, {"b": 2}, {"c": 3}, [{"d": 4}]]


def test_iter_json_documents_single_document_and_empty() -> None:
    assert list(fb.iter_json_documents('for (;;); {"x": {"y": [1, 2]}}')) == [{"x": {"y": [1, 2]}}]
    pretty = json.dumps({"x": {"marketplace_listing_title": "t", "id": "1"}}, indent=2)
    assert list(fb.iter_json_documents(pretty)) == [{"x": {"marketplace_listing_title": "t", "id": "1"}}]
    assert list(fb.iter_json_documents("")) == []
    assert list(fb.iter_json_documents("for (;;);")) == []
    assert list(fb.iter_json_documents("<!DOCTYPE html><html></html>")) == []


def test_iter_listing_nodes_walks_generically_in_document_order() -> None:
    doc = {
        "data": {
            "viewer": {
                "marketplace_feed_stories": {"edges": [{"node": {"listing": {"id": "1", "marketplace_listing_title": "A"}}}]}
            },
            "other": [{"deep": [{"x": {"id": "2", "marketplace_listing_title": "B", "listing_price": None}}]}],
            "not_a_listing": {"marketplace_listing_title": "no id or price"},
        }
    }
    assert [n["id"] for n in fb.iter_listing_nodes(doc)] == ["1", "2"]


def test_iter_listing_nodes_survives_very_deep_payloads() -> None:
    node: Any = {"id": "42", "marketplace_listing_title": "deep"}
    for _ in range(5000):
        node = [node]
    assert [n["id"] for n in fb.iter_listing_nodes({"data": node})] == ["42"]


# --------------------------------------------------------------------------- GraphQL parsing


def test_parse_graphql_payload_fixture() -> None:
    listings = fb.parse_graphql_payload(GRAPHQL_PAGE1, query="rtx 3090", profile_hint="rtx3090")
    by_id = {item.source_id: item for item in listings}
    # sold (flag + "SOLD -" title) and pending listings are skipped; order follows the feed.
    assert [item.source_id for item in listings] == [
        "1111111111111111",
        "4444444444444444",
        "5555555555555555",
        "6666666666666666",
        "7777777777777777",
    ]

    first = by_id["1111111111111111"]
    assert first.source == "fb_marketplace"
    assert first.source_kind is SourceKind.LOCAL
    assert first.url == "https://www.facebook.com/marketplace/item/1111111111111111/"
    assert first.title == "RTX 3090 Founders Edition 24GB"
    assert first.price == 650.0 and first.currency == "USD"
    assert first.list_price == 800.0
    assert first.image_urls and first.image_urls[0].startswith("https://scontent-")
    assert first.location is not None
    assert (first.location.text, first.location.city, first.location.region) == ("Brooklyn, New York", "Brooklyn", "NY")
    assert first.seller is not None and first.seller.name == "Alex"
    assert first.posted_at is not None and int(first.posted_at.timestamp()) == 1759700000
    assert first.query == "rtx 3090" and first.profile_hint == "rtx3090"
    assert first.extra["delivery_types"] == ["IN_PERSON"]
    assert first.extra["seller_id"] == "100012345678901"
    assert first.extra["subtitles"] == ["Used - Like New"]
    assert first.extra["category_id"] == "1792291877663080"
    assert first.extra["via"] == "graphql"

    no_photo = by_id["4444444444444444"]
    assert no_photo.image_urls == []
    assert no_photo.price == "$1,150"  # no numeric amount: formatted string for the normalizer
    assert no_photo.title == "RTX 3090 Ti with box"  # whitespace collapsed
    assert no_photo.location is not None and no_photo.location.text == "Newark, NJ"
    assert no_photo.extra["delivery_types"] == ["IN_PERSON", "SHIPPING"]
    assert no_photo.posted_at is None

    deferred = by_id["5555555555555555"]  # listing_price null in the feed, filled by the @defer chunk
    assert deferred.price == 900.0
    assert len(deferred.image_urls) == 1

    free = by_id["6666666666666666"]
    assert free.price == 0.0

    cad = by_id["7777777777777777"]
    assert cad.price == "CA$1,200" and cad.currency == "CAD"


def test_parse_payloads_merges_pages_and_reports_skips() -> None:
    listings, stats = fb.parse_payloads([("graphql", GRAPHQL_PAGE1), ("graphql", GRAPHQL_PAGE2)], query="q")
    ids = [item.source_id for item in listings]
    assert ids.count("1111111111111111") == 1
    assert ids[-1] == "1212121212121212"
    assert stats["sold"] == 2 and stats["pending"] == 1
    assert stats["documents"] == 4  # page1 doc + defer chunk + final extensions + page2


def test_parse_payloads_embedded_preloader() -> None:
    listings, stats = fb.parse_payloads([("embedded", SSR_SCRIPT)], query="steam deck oled", profile_hint="steamdeck")
    assert [item.source_id for item in listings] == ["3131313131313131"]
    assert listings[0].extra["via"] == "embedded"
    assert listings[0].price == 480.0
    assert stats["pending"] == 1


def test_sold_listing_stays_excluded_when_seen_again_without_flag() -> None:
    sold = {"id": "123456", "marketplace_listing_title": "RTX 3090", "listing_price": {"amount": "500"}, "is_sold": True}
    again = {"id": "123456", "marketplace_listing_title": "RTX 3090", "listing_price": {"amount": "500"}}
    before = {"id": "654321", "marketplace_listing_title": "RTX 3080", "listing_price": {"amount": "400"}}
    listings, _ = fb.parse_payloads([("graphql", json.dumps([again, before])), ("graphql", json.dumps({"x": sold}))])
    assert [item.source_id for item in listings] == ["654321"]


@pytest.mark.parametrize(
    ("node", "reason"),
    [
        ({"id": True, "marketplace_listing_title": "x", "listing_price": {}}, "no_id"),
        ({"id": None, "marketplace_listing_title": "x", "listing_price": {}}, "no_id"),
        ({"id": "../../etc", "marketplace_listing_title": "x"}, "bad_id"),
        ({"id": "99", "marketplace_listing_title": "   "}, "no_title"),
        ({"id": "99", "marketplace_listing_title": "[SOLD] RTX 4090"}, "sold"),
        ({"id": "99", "marketplace_listing_title": "RTX 4090", "is_pending": True}, "pending"),
    ],
)
def test_malformed_or_unavailable_nodes_are_skipped(node: dict[str, Any], reason: str) -> None:
    listings, stats = fb.parse_payloads([("graphql", json.dumps({"data": {"n": node}}))])
    assert listings == []
    assert stats[reason] == 1


def test_price_variants_and_defensive_fields() -> None:
    nodes = [
        {"id": "1", "marketplace_listing_title": "A", "listing_price": {"amount": 1250, "formatted_amount": "$1,250"}},
        {"id": "2", "marketplace_listing_title": "B", "listing_price": {"amount": "abc", "formatted_amount": "350 €"}},
        {"id": "3", "marketplace_listing_title": "C", "listing_price": "garbage", "location": "nowhere",
         "primary_listing_photo": 7},
        {"id": "4", "marketplace_listing_title": "D", "listing_price": {"amount": "-5"}, "creation_time": "1759700000"},
        {"id": "5", "marketplace_listing_title": "E", "listing_price": {"amount": "10", "currency": "eur"}, "creation_time": 1},
        {"id": "6", "marketplace_listing_title": "F", "listing_price": {"formatted_amount_zeros_stripped": "£40"},
         "redacted_description": {"text": " Works great "}, "location_text": {"text": "Leeds"},
         "listing_photos": [{"image": {"uri": "https://scontent.example/1.jpg"}}, {"image": {"uri": "javascript:alert(1)"}}],
         "location": {"latitude": 53.8, "longitude": -1.55}},
    ]
    listings = {item.source_id: item for item in fb.parse_graphql_payload(json.dumps({"data": nodes}))}
    assert listings["1"].price == 1250.0 and listings["1"].currency == "USD"
    assert listings["2"].price == "350 €" and listings["2"].currency == "EUR"
    assert listings["3"].price is None and listings["3"].location is None and listings["3"].image_urls == []
    assert listings["4"].price is None  # negative amount rejected, no formatted fallback
    assert listings["4"].posted_at is not None
    assert listings["5"].currency == "EUR" and listings["5"].posted_at is None  # implausible timestamp ignored
    six = listings["6"]
    assert six.price == "£40" and six.currency == "GBP"
    assert six.description == "Works great"
    assert six.image_urls == ["https://scontent.example/1.jpg"]
    assert six.location is not None and six.location.text == "Leeds"
    assert (six.location.latitude, six.location.longitude) == (53.8, -1.55)


def test_withheld_results_are_counted() -> None:
    def feed(edges: list[Any], cursor: str | None) -> dict[str, Any]:
        return {"data": {"marketplace_search": {"feed_units": {"edges": edges, "page_info": {"end_cursor": cursor}}}}}

    withheld = feed([], json.dumps({"pg": 0, "b2c": {"br": "", "it": 0}, "c2c": {"br": "AbqX", "it": 12}}))
    assert fb.count_withheld_feeds(withheld) == 1
    assert fb.count_withheld_feeds(feed([], json.dumps({"c2c": {"it": 0}, "b2c": {"it": 0}}))) == 0  # truly empty
    assert fb.count_withheld_feeds(feed([], "AQHRn0cX2vC1QWp6")) == 0  # opaque cursor
    assert fb.count_withheld_feeds(feed([{"node": {}}], json.dumps({"c2c": {"it": 3}}))) == 0
    _, stats = fb.parse_payloads([("graphql", json.dumps(withheld))])
    assert stats["withheld"] == 1


@pytest.mark.parametrize(
    ("text", "code"),
    [
        ("$650", "USD"), ("CA$1,200", "CAD"), ("A$90", "AUD"), ("350 €", "EUR"), ("£40", "GBP"), ("R$ 900", "BRL"),
        ("Free", None), (None, None),
    ],
)
def test_currency_from_text(text: str | None, code: str | None) -> None:
    assert fb.currency_from_text(text) == code


# --------------------------------------------------------------------------- DOM fallback


def test_parse_dom_cards() -> None:
    cards = [
        {"id": "4242424242424242", "lines": ["$450", "$520", "Steam Deck OLED 1TB", "Queens, NY"], "img": "https://scontent.x/1.jpg",
         "alt": "Steam Deck OLED 1TB in Queens, NY"},
        {"id": "4343434343434343", "lines": ["Free", "Old gaming PC", "Bronx, NY", "Pickup only"], "img": None, "alt": None},
        {"id": "4444444444444444", "lines": ["$300", "Steam Deck LCD", "Newark, NJ", "Pending"], "img": None, "alt": None},
        {"id": "4545454545454545", "lines": ["$1,000", "1080 GTX for sale"], "img": "data:image/gif;base64,AA", "alt": None},
        {"id": "4646464646464646", "lines": ["$80"], "img": None, "alt": "Joy-Con pair in Hoboken, NJ"},
        {"id": "4242424242424242", "lines": ["$1", "duplicate"], "img": None, "alt": None},
        {"id": "not-a-number", "lines": ["$5", "bogus"], "img": None, "alt": None},
        {"id": "4747474747474747", "lines": ["$10", "SOLD - Steam Deck"], "img": None, "alt": None},
        {"id": "4848484848484848", "lines": "not-a-list", "img": None, "alt": None},
    ]
    listings = fb.parse_dom_cards(cards, query="steam deck", profile_hint="steamdeck")
    by_id = {item.source_id: item for item in listings}
    assert list(by_id) == ["4242424242424242", "4343434343434343", "4545454545454545", "4646464646464646"]
    first = by_id["4242424242424242"]
    assert (first.price, first.list_price, first.title) == ("$450", "$520", "Steam Deck OLED 1TB")
    assert first.location is not None and first.location.text == "Queens, NY"
    assert first.image_urls == ["https://scontent.x/1.jpg"]
    assert first.extra["via"] == "dom" and first.query == "steam deck"
    assert by_id["4343434343434343"].price == "Free"
    assert by_id["4343434343434343"].extra["subtitles"] == ["Pickup only"]
    assert by_id["4545454545454545"].title == "1080 GTX for sale" and by_id["4545454545454545"].image_urls == []
    assert by_id["4646464646464646"].title == "Joy-Con pair"


# --------------------------------------------------------------------------- URLs and walls


def test_build_search_url() -> None:
    url = fb.build_search_url(
        "https://www.facebook.com/",
        location_slug="nyc",
        query="rtx 3090 & fe",
        min_price=299.5,
        max_price=900.2,
        days_since_listed=1,
    )
    assert url == (
        "https://www.facebook.com/marketplace/nyc/search?minPrice=299&maxPrice=901&daysSinceListed=1"
        "&sortBy=creation_time_descend&query=rtx%203090%20%26%20fe&exact=false"
    )
    numeric = fb.build_search_url(
        "https://www.facebook.com", location_slug="108424279189115", query="x", min_price=1, max_price=2, days_since_listed=30
    )
    assert numeric.startswith("https://www.facebook.com/marketplace/108424279189115/search?minPrice=1&maxPrice=2&daysSinceListed=30")
    bare = fb.build_search_url(
        "http://127.0.0.1:1", location_slug=None, query="x", min_price=None, max_price=0, days_since_listed=7
    )
    assert bare == "http://127.0.0.1:1/marketplace/search?daysSinceListed=7&sortBy=creation_time_descend&query=x&exact=false"


@pytest.mark.parametrize(
    ("url", "kwargs", "expected"),
    [
        ("https://www.facebook.com/marketplace/nyc/search?query=x", {}, None),
        ("https://www.facebook.com/marketplace/nyc/search?next=/login/", {}, None),
        ("https://www.facebook.com/checkpoint/1501092823525282/?next=%2Fmarketplace", {}, "checkpoint"),
        ("https://www.facebook.com/two_step_verification/authentication/", {}, "checkpoint"),
        ("https://www.facebook.com/login/?next=https%3A%2F%2Fwww.facebook.com%2Fmarketplace", {}, "login_required"),
        ("https://www.facebook.com/login.php?skip_api_login=1", {}, "login_required"),
        ("https://www.facebook.com/marketplace/ineligible/", {}, "ineligible"),
        ("https://www.facebook.com/privacy/consent/?flow=fb_dma_marketplace", {}, "consent_required"),
        ("https://www.facebook.com/marketplace/nyc/search", {"has_login_form": True}, "login_required"),
        ("https://www.facebook.com/marketplace/nyc/search", {"text": "You’re Temporarily Blocked"}, "temporarily_blocked"),
        ("https://www.facebook.com/marketplace/nyc/search", {"title": "You're temporarily blocked"}, "temporarily_blocked"),
        ("https://www.facebook.com/marketplace/nyc/search",
         {"text": "It looks like you were misusing this feature by going too fast."}, "rate_limited"),
        ("https://www.facebook.com/marketplace/nyc/search", {"text": "We suspended your account"}, "account_restricted"),
        ("https://www.facebook.com/marketplace/nyc/search", {"text": "Blocked fan for sale\nTemporarily unavailable"}, None),
    ],
)
def test_detect_block(url: str, kwargs: dict[str, Any], expected: str | None) -> None:
    assert fb.detect_block(url, **kwargs) == expected


# --------------------------------------------------------------------------- session file


def test_session_cookie_helpers(tmp_path: Path) -> None:
    state_path = tmp_path / "s.json"
    _write_state(state_path)
    state = json.loads(state_path.read_text())
    assert fb.has_session_cookie(state["cookies"])
    assert fb.session_problem(state) is None
    assert "no Facebook session cookies" in (fb.session_problem({"cookies": [], "origins": []}) or "")
    assert fb.session_problem([1, 2]) == "not a Playwright storage-state file"
    _write_state(state_path, expires=1_700_000_000)
    assert "expired" in (fb.session_problem(json.loads(state_path.read_text())) or "")
    other_site = [
        {"name": "c_user", "value": "1", "domain": ".example.com"},
        {"name": "xs", "value": "1", "domain": ".example.com"},
    ]
    assert not fb.has_session_cookie(other_site)
    lookalike = [{"name": name, "value": "1", "domain": "notfacebook.com"} for name in ("c_user", "xs")]
    assert not fb.has_session_cookie(lookalike)
    bare = [
        {"name": "c_user", "value": "1", "domain": "facebook.com"},
        {"name": "xs", "value": "1", "domain": "www.facebook.com"},
    ]
    assert fb.has_session_cookie(bare)


@pytest.mark.parametrize("variant", ["missing", "no_cookies", "expired", "corrupt"])
async def test_setup_requires_a_valid_session_file(tmp_path: Path, http: HttpClient, variant: str) -> None:
    cfg = _fb_cfg(tmp_path, None)
    state_path = Path(cfg.browser.storage_state_path)
    if variant == "no_cookies":
        _write_state(state_path, cookies=False)
    elif variant == "expired":
        _write_state(state_path, expires=1_700_000_000)
    elif variant == "corrupt":
        state_path.write_text("{not json", encoding="utf-8")
    ingestor = fb.FbMarketplaceIngestor(cfg, _ctx(http, _app_config()))
    with pytest.raises(SourceAuthError) as excinfo:
        await ingestor.setup()
    if variant == "missing":
        assert "--login" in str(excinfo.value)
    await ingestor.teardown()  # safe after a failed setup


# --------------------------------------------------------------------------- stealth helpers


def test_build_context_options_is_one_consistent_device(tmp_path: Path) -> None:
    geo = _geo(latitude=41.8781, longitude=-87.6298, timezone_id="America/Chicago", locale="en-US")
    cfg = BrowserSection(storage_state_path=str(tmp_path / "state.json"), viewport_width=1366, viewport_height=900)
    options = stealth.build_context_options(cfg, geo, None)
    assert options["viewport"] == {"width": 1366, "height": 900}
    screen = options["screen"]
    assert screen["width"] >= 1366 and screen["height"] >= 900 + stealth.BROWSER_UI_HEIGHT
    assert (screen["width"], screen["height"]) in stealth.COMMON_SCREENS
    assert options["timezone_id"] == "America/Chicago"
    assert options["geolocation"] == {"latitude": 41.8781, "longitude": -87.6298, "accuracy": stealth.GEO_ACCURACY_METERS}
    assert options["permissions"] == ["geolocation"]
    assert options["color_scheme"] == "light" and options["device_scale_factor"] == 1
    assert options["is_mobile"] is False and options["has_touch"] is False
    # Language lives on the browser process (see launch_options), never per context.
    assert "locale" not in options and "extra_http_headers" not in options
    assert "user_agent" not in options and "storage_state" not in options

    Path(cfg.storage_state_path).write_text('{"cookies": [], "origins": []}')
    ua = stealth.user_agent_for("141.0.7390.37", "Linux")
    options = stealth.build_context_options(cfg, geo, ua)
    assert options["storage_state"] == str(Path(cfg.storage_state_path))
    assert options["user_agent"] == ua


def test_launch_options_language_and_hardening() -> None:
    cfg = BrowserSection(headless=True, proxy="http://user:p%40ss@10.0.0.2:3128")
    options = stealth.launch_options(cfg, locale="de_DE")
    args = options["args"]
    assert "--disable-blink-features=AutomationControlled" in args
    assert "--lang=de-DE" in args and "--accept-lang=de-DE,de" in args
    assert options["env"]["LANG"] == "de_DE.UTF-8" and options["env"]["LANGUAGE"] == "de_DE:de"
    assert "LC_ALL" not in options["env"]
    assert options["ignore_default_args"] == ["--enable-automation"]
    assert options["channel"] == "chromium" and "executable_path" not in options
    assert options["proxy"] == {"server": "http://10.0.0.2:3128", "username": "user", "password": "p@ss"}

    headed = stealth.launch_options(BrowserSection(headless=False, executable_path="/usr/bin/google-chrome"))
    assert headed["headless"] is False and headed["executable_path"] == "/usr/bin/google-chrome"
    assert "channel" not in headed and "env" not in headed and "proxy" not in headed


def test_identity_helpers() -> None:
    assert stealth.accept_language("en-US") == "en-US,en;q=0.9"
    assert stealth.accept_language("en_GB") == "en-GB,en;q=0.9"
    assert stealth.accept_language("fr") == "fr"
    assert stealth.accept_languages("pt-BR") == ["pt-BR", "pt"]
    assert stealth.screen_for_viewport(1366, 900) == (1680, 1050)
    assert stealth.screen_for_viewport(1280, 600) == (1366, 768)
    assert stealth.screen_for_viewport(5000, 3000) == (5000, 3000 + stealth.BROWSER_UI_HEIGHT)
    linux = stealth.user_agent_for("141.0.7390.37", "Linux")
    assert linux == "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    assert "Macintosh; Intel Mac OS X 10_15_7" in stealth.user_agent_for("140.0.1", "Darwin")
    assert "Windows NT 10.0; Win64; x64" in stealth.user_agent_for("140", "Windows")
    assert "HeadlessChrome" not in linux
    with pytest.raises(ValueError):
        stealth.user_agent_for("HeadlessChrome/141")
    assert stealth.parse_proxy("socks5://[::1]:1080") == {"server": "socks5://[::1]:1080"}
    assert stealth.parse_proxy("proxy.local:8080") == {"server": "http://proxy.local:8080"}
    with pytest.raises(ValueError):
        stealth.parse_proxy("http://")


class _FakeContext:
    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state

    async def storage_state(self) -> dict[str, Any]:
        return self.state


async def test_save_storage_state_is_atomic_and_private(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "nested" / "dir" / "state.json"
    state = {"cookies": [{"name": "c_user", "value": "1"}], "origins": []}
    saved = await stealth.save_storage_state(_FakeContext(state), target)
    assert saved == target
    assert json.loads(target.read_text()) == state
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert stat.S_IMODE(target.parent.stat().st_mode) == 0o700

    target.chmod(0o644)  # a pre-existing world-readable file is replaced by a private one
    await stealth.save_storage_state(_FakeContext({"cookies": [], "origins": []}), target)
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert json.loads(target.read_text()) == {"cookies": [], "origins": []}

    def boom(src: str, dst: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(stealth.os, "replace", boom)
    with pytest.raises(OSError):
        await stealth.save_storage_state(_FakeContext(state), target)
    assert json.loads(target.read_text()) == {"cookies": [], "origins": []}  # old file intact
    assert [p.name for p in target.parent.iterdir()] == ["state.json"]  # temp file cleaned up


async def test_human_pause_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    monkeypatch.setattr(stealth.asyncio, "sleep", fake_sleep)
    rng = random.Random(3)
    delays = [await stealth.human_pause(rng, 1.5, 4.5) for _ in range(200)]
    assert all(1.5 <= d <= 4.5 for d in delays)
    assert slept == delays
    assert sum(delays) / len(delays) < 3.0  # right-skewed: mean below the midpoint
    assert await stealth.human_pause(rng, 2.0, 1.0) <= 2.0  # swapped bounds are tolerated
    slept.clear()
    assert await stealth.human_pause(rng, 0, 0) == 0.0 and slept == []


class _FakeMouse:
    def __init__(self) -> None:
        self.moves: list[tuple[float, float, int]] = []
        self.wheels: list[tuple[float, float]] = []

    async def move(self, x: float, y: float, steps: int = 1) -> None:
        self.moves.append((x, y, steps))

    async def wheel(self, dx: float, dy: float) -> None:
        self.wheels.append((dx, dy))


class _FakePage:
    def __init__(self) -> None:
        self.mouse = _FakeMouse()
        self.viewport_size = {"width": 1366, "height": 900}


async def test_human_scroll_uses_uneven_wheel_ticks(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_sleep(delay: float) -> None:
        return None

    monkeypatch.setattr(stealth.asyncio, "sleep", fake_sleep)
    page = _FakePage()
    total = await stealth.human_scroll(page, random.Random(11), steps=5)  # type: ignore[arg-type]
    x, y, steps = page.mouse.moves[0]
    assert 0.3 * 1366 <= x <= 0.7 * 1366 and 0.35 * 900 <= y <= 0.65 * 900 and steps >= 4
    assert 5 * 2 <= len(page.mouse.wheels) <= 5 * 4
    assert all(dx == 0 for dx, _ in page.mouse.wheels)
    assert len({round(dy, 3) for _, dy in page.mouse.wheels}) > 1
    assert abs(sum(dy for _, dy in page.mouse.wheels) - total) < 1e-6
    assert total > 0


# --------------------------------------------------------------------------- real browser: stealth


_STEALTH_PROBE_JS = """async () => {
  const ts = Function.prototype.toString;
  const frame = document.createElement('iframe');
  document.body.appendChild(frame);
  const child = frame.contentWindow;
  let webgl = null;
  try {
    const gl = document.createElement('canvas').getContext('webgl');
    const ext = gl && gl.getExtension('WEBGL_debug_renderer_info');
    if (ext) webgl = [gl.getParameter(ext.UNMASKED_VENDOR_WEBGL), gl.getParameter(ext.UNMASKED_RENDERER_WEBGL)];
  } catch (err) { webgl = null; }
  let stack = '';
  try { ts.call(5); } catch (err) { stack = String(err.stack); }
  let brandError = null;
  try { Object.getOwnPropertyDescriptor(Navigator.prototype, 'languages').get.call({}); } catch (err) { brandError = err.name; }
  const notif = (await navigator.permissions.query({ name: 'notifications' })).state;
  await fetch('/echo');
  return {
    ua: navigator.userAgent,
    webdriver: navigator.webdriver,
    plugins: navigator.plugins.length,
    pluginNames: Array.from(navigator.plugins).map((p) => p.name),
    mimeTypes: navigator.mimeTypes.length,
    pdfMime: navigator.mimeTypes.namedItem('application/pdf') ? navigator.mimeTypes.namedItem('application/pdf').type : null,
    language: navigator.language,
    languages: Array.from(navigator.languages),
    chrome: typeof window.chrome,
    runtime: !!(window.chrome && window.chrome.runtime),
    languagesSource: ts.call(Object.getOwnPropertyDescriptor(Navigator.prototype, 'languages').get),
    toStringSource: ts.call(ts),
    getParameterSource: ts.call(WebGLRenderingContext.prototype.getParameter),
    getParameterLength: WebGLRenderingContext.prototype.getParameter.length,
    querySource: ts.call(Permissions.prototype.query),
    getterHasPrototype: 'prototype' in Object.getOwnPropertyDescriptor(Navigator.prototype, 'languages').get,
    notificationPermission: Notification.permission,
    notificationQuery: notif,
    hardwareConcurrency: navigator.hardwareConcurrency,
    webgl,
    stack,
    brandError,
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    outer: [window.outerWidth, window.outerHeight, window.innerWidth, window.innerHeight, screen.width, screen.height],
    child: { webdriver: child.navigator.webdriver, languages: Array.from(child.navigator.languages), chrome: typeof child.chrome,
             plugins: child.navigator.plugins.length },
  };
}"""


@pytest.mark.parametrize("build", ["full", "shell"])
async def test_stealth_init_script_in_real_chromium(build: str, tmp_path: Path) -> None:
    exe = await _require_chromium(build)
    from playwright.async_api import async_playwright

    headers: list[tuple[str, str | None]] = []

    async def page(request: web.Request) -> web.Response:
        headers.append((request.path, request.headers.get("Accept-Language")))
        return web.Response(text="<!doctype html><html><head><title>t</title></head><body><p>ok</p></body></html>",
                            content_type="text/html")

    app = web.Application()
    app.router.add_get("/", page)
    app.router.add_get("/echo", page)
    server = TestServer(app, host="127.0.0.1")
    await server.start_server()
    cfg = BrowserSection(headless=True, executable_path=exe, storage_state_path=str(tmp_path / "absent.json"))
    geo = _geo(timezone_id="America/Chicago", locale="en-US")
    try:
        async with async_playwright() as pw:
            browser, context = await stealth.new_stealth_context(pw, cfg, geo)
            try:
                await context.add_init_script(stealth.STEALTH_INIT_SCRIPT)  # a second copy must be harmless
                tab = await context.new_page()
                await tab.goto(str(server.make_url("/")))
                info = await tab.evaluate(_STEALTH_PROBE_JS)
            finally:
                await context.close()
                if browser is not None:
                    await browser.close()
    finally:
        await server.close()

    assert info["webdriver"] in (False, None)
    assert "HeadlessChrome" not in info["ua"]
    assert info["plugins"] > 0 and info["mimeTypes"] > 0
    assert "PDF Viewer" in info["pluginNames"] and info["pdfMime"] == "application/pdf"
    assert info["language"] == "en-US" and info["languages"] == ["en-US", "en"]
    assert info["chrome"] == "object" and info["runtime"] is True
    assert info["languagesSource"] == "function get languages() { [native code] }"
    assert info["toStringSource"] == "function toString() { [native code] }"
    assert info["getParameterSource"] == "function getParameter() { [native code] }"
    assert info["getParameterLength"] == 1
    assert info["querySource"] == "function query() { [native code] }"
    assert info["getterHasPrototype"] is False
    expected_query = "prompt" if info["notificationPermission"] == "default" else info["notificationPermission"]
    assert info["notificationQuery"] == expected_query
    assert info["hardwareConcurrency"] >= 2
    if info["webgl"] is not None:
        assert not any("swiftshader" in str(v).lower() for v in info["webgl"])
    assert "callNative" not in info["stack"] and "Object.apply" not in info["stack"]
    assert info["brandError"] == "TypeError"
    assert info["timezone"] == "America/Chicago"
    outer_w, outer_h, inner_w, inner_h, screen_w, screen_h = info["outer"]
    assert outer_w >= inner_w and outer_h > inner_h and screen_w >= outer_w and screen_h >= outer_h
    assert info["child"] == {"webdriver": info["webdriver"], "languages": ["en-US", "en"], "chrome": "object",
                             "plugins": info["plugins"]}
    # One Accept-Language for the document and its own XHR/fetch, matching navigator.languages.
    nav_header = dict(headers)["/"]
    assert nav_header == dict(headers)["/echo"]
    assert nav_header is not None and nav_header.startswith("en-US,en")


# --------------------------------------------------------------------------- real browser: poll()


_HTML_HEAD = "<!doctype html><html><head><meta charset='utf-8'><title>Marketplace</title></head><body>"
_HTML_TAIL = "<div style='height: 4000px'></div></body></html>"
_GRAPHQL_PAGE_JS = """<script>
  const post = (cursor) => fetch('/api/graphql/', {
    method: 'POST',
    headers: {'content-type': 'application/x-www-form-urlencoded'},
    body: 'fb_api_req_friendly_name=CometMarketplaceSearchContentPaginationQuery&cursor=' + cursor,
  });
  document.addEventListener('DOMContentLoaded', () => post(0));
  let paged = false;
  window.addEventListener('wheel', () => { if (!paged) { paged = true; post(1); } });
</script>"""


class FakeMarketplace:
    """Local stand-in for facebook.com Marketplace search pages (no real FB traffic)."""

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.searches: list[dict[str, Any]] = []
        self.graphql_bodies: list[str] = []

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/marketplace/{slug}/search", self.search)
        app.router.add_post("/api/graphql/", self.graphql)
        app.router.add_get("/checkpoint/{rest:.*}", self.checkpoint)
        app.router.add_get("/img/{name}", self.image)
        return app

    async def search(self, request: web.Request) -> web.Response:
        self.searches.append(
            {
                "slug": request.match_info["slug"],
                "query": dict(request.query),
                "accept_language": request.headers.get("Accept-Language"),
            }
        )
        mode = self.mode
        if mode == "checkpoint":
            raise web.HTTPFound("/checkpoint/1501092823525282/?next=%2Fmarketplace%2F")
        if request.match_info["slug"] not in ("nyc", "category"):
            # What Facebook does with an unknown slug: search around the account's saved location.
            raise web.HTTPFound(f"/marketplace/category/search?{request.query_string}")
        radius = "<script type='application/json'>{\"browse_request_params\":{\"filter_radius_km\":250}}</script>"
        if mode == "graphql":
            body = _GRAPHQL_PAGE_JS + "<div role='main'>Results</div>"
        elif mode == "ssr":
            body = (
                f"<script type='application/json' data-content-len='{len(SSR_SCRIPT)}' data-sjs>{SSR_SCRIPT}</script>"
                "<script type='application/json' data-sjs>"
                "{\"require\":[[\"Bootloader\",\"markComponentsAsImmediate\",null,[[]]]]}</script>"
                "<a href='/marketplace/item/3131313131313131/?ref=search'><div>$480</div><div>Steam Deck OLED 1TB</div></a>"
            )
        elif mode == "withheld":
            empty = {"data": {"marketplace_search": {"feed_units": {
                "edges": [], "page_info": {"end_cursor": json.dumps({"c2c": {"br": "Abq", "it": 12}}), "has_next_page": True}}}}}
            body = f"<script type='application/json' data-sjs>{json.dumps(empty)}</script>"
        elif mode == "dom":
            body = (
                "<div role='main'>"
                "<a href='/marketplace/item/4242424242424242/?ref=search&amp;referral_code=null'>"
                "<div><img src='/img/4242.jpg' alt='Steam Deck OLED 1TB in Queens, NY'></div>"
                "<div>$450</div><div>$520</div><div>Steam Deck OLED 1TB</div><div>Queens, NY</div></a>"
                "<a href='/marketplace/item/4343434343434343/'>"
                "<div>$399</div><div>Steam Deck OLED 512GB</div><div>Bronx, NY</div></a>"
                f"<a href='/marketplace/item/4444444444444444/'><div>$1</div><div>{request.query.get('query', '')}</div></a>"
                "</div>"
            )
        elif mode == "login":
            body = (
                "<div role='dialog'><h2>See more on Facebook</h2>"
                "<form id='login_form' action='/login/device-based/regular/login/'>"
                "<input name='email'><input type='password' name='pass'></form></div>"
            )
        elif mode == "blocked":
            body = (
                "<div role='dialog'><h2>You’re Temporarily Blocked</h2>"
                "<p>It looks like you were misusing this feature.</p></div>"
            )
        else:  # pragma: no cover - test bug
            raise AssertionError(mode)
        return web.Response(text=_HTML_HEAD + radius + body + _HTML_TAIL, content_type="text/html")

    async def graphql(self, request: web.Request) -> web.Response:
        form = await request.post()
        self.graphql_bodies.append(str(form.get("fb_api_req_friendly_name")))
        payload = GRAPHQL_PAGE2 if form.get("cursor") == "1" else GRAPHQL_PAGE1
        return web.Response(text=payload, content_type="application/json")

    async def checkpoint(self, request: web.Request) -> web.Response:
        return web.Response(text=_HTML_HEAD + "<h1>Confirm your identity</h1>" + _HTML_TAIL, content_type="text/html")

    async def image(self, request: web.Request) -> web.Response:
        return web.Response(body=b"GIF89a\x01\x00\x01\x00\x00\x00\x00;", content_type="image/gif")


async def _run_against(
    mode: str,
    tmp_path: Path,
    http: HttpClient,
    *,
    profiles: list[str],
    polls: int = 1,
    metrics: Metrics | None = None,
    **cfg_overrides: Any,
) -> tuple[list[list[Any]], FakeMarketplace, fb.FbMarketplaceIngestor]:
    exe = await _require_chromium("full")
    market = FakeMarketplace(mode)
    server = TestServer(market.app(), host="127.0.0.1")
    await server.start_server()
    cfg = _fb_cfg(tmp_path, exe, profiles=profiles, **cfg_overrides)
    _write_state(Path(cfg.browser.storage_state_path))
    ingestor = fb.FbMarketplaceIngestor(cfg, _ctx(http, _app_config(), metrics), base_url=str(server.make_url("/")))
    ingestor.results_timeout_seconds = 3.0
    ingestor.scroll_settle_seconds = 5.0  # returns as soon as the pagination payload arrived
    results: list[list[Any]] = []
    try:
        for _ in range(polls):
            results.append(await ingestor.poll())  # poll() starts the browser on first use
    finally:
        await ingestor.teardown()
        await server.close()
    return results, market, ingestor


async def test_poll_captures_graphql_and_scroll_pagination(tmp_path: Path, http: HttpClient) -> None:
    metrics = Metrics()
    (listings,), market, ingestor = await _run_against("graphql", tmp_path, http, profiles=["rtx3090"], metrics=metrics)
    assert [item.source_id for item in listings] == [
        "1111111111111111",
        "4444444444444444",
        "5555555555555555",
        "6666666666666666",
        "7777777777777777",
        "1212121212121212",  # second page, fetched because we scrolled
    ]
    assert all(item.query == "rtx 3090" and item.profile_hint == "rtx3090" for item in listings)
    assert all(item.url.startswith("https://www.facebook.com/marketplace/item/") for item in listings)
    assert market.graphql_bodies == ["CometMarketplaceSearchContentPaginationQuery"] * 2

    (search,) = market.searches
    assert search["slug"] == "nyc"
    assert search["query"] == {
        "minPrice": "300",
        "maxPrice": "900",
        "daysSinceListed": "1",
        "sortBy": "creation_time_descend",
        "query": "rtx 3090",
        "exact": "false",
    }
    assert (search["accept_language"] or "").startswith("en-US,en")

    state_path = Path(ingestor.cfg.browser.storage_state_path)
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
    saved = json.loads(state_path.read_text())
    assert fb.has_session_cookie(saved["cookies"])  # cookies round-trip through the browser
    assert metrics.counter("fb_payloads_total", "", ("kind",)).value(kind="graphql") == 2
    assert metrics.counter("fb_listings_skipped_total", "", ("reason",)).value(reason="sold") >= 2
    assert ingestor._page is None and ingestor._browser is None  # teardown closed everything


async def test_poll_reads_server_rendered_json(tmp_path: Path, http: HttpClient) -> None:
    (listings,), market, _ = await _run_against("ssr", tmp_path, http, profiles=["steamdeck"], scrolls_per_query=0)
    assert [item.source_id for item in listings] == ["3131313131313131"]  # the pending one is skipped
    assert listings[0].extra["via"] == "embedded"
    assert market.searches[0]["query"]["minPrice"] == "250"  # profile.search.price_min wins over band.floor


async def test_poll_falls_back_to_dom_cards_and_caps_results(tmp_path: Path, http: HttpClient) -> None:
    (listings,), _, _ = await _run_against("dom", tmp_path, http, profiles=["steamdeck"], max_listings_per_query=2)
    assert [item.source_id for item in listings] == ["4242424242424242", "4343434343434343"]
    first = listings[0]
    assert first.extra["via"] == "dom"
    assert (first.price, first.list_price, first.title) == ("$450", "$520", "Steam Deck OLED 1TB")
    assert first.location is not None and first.location.text == "Queens, NY"
    assert first.image_urls and first.image_urls[0].endswith("/img/4242.jpg")


async def test_poll_budget_rotates_through_terms(tmp_path: Path, http: HttpClient) -> None:
    # A tiny poll budget allows one query per poll; the cursor continues where it stopped
    # and duplicate terms ("alpha" / "ALPHA") are searched once.
    results, market, _ = await _run_against(
        "dom", tmp_path, http, profiles=["multi"], polls=4, scrolls_per_query=0, poll_timeout_seconds=1.0
    )
    assert [s["query"]["query"] for s in market.searches] == ["alpha", "beta", "gamma", "alpha"]
    assert [[item.title for item in batch if item.source_id == "4444444444444444"] for batch in results] == [
        ["alpha"],
        ["beta"],
        ["gamma"],
        ["alpha"],
    ]


async def test_poll_warns_about_location_and_withheld_results(
    tmp_path: Path, http: HttpClient, caplog: pytest.LogCaptureFixture
) -> None:
    metrics = Metrics()
    caplog.set_level("WARNING", logger="deal_radar.sources.fb_marketplace")
    (listings,), market, _ = await _run_against(
        "withheld", tmp_path, http, profiles=["rtx3090"], metrics=metrics, scrolls_per_query=0,
        location=_geo(city_slug="atlantis", radius_km=60),
    )
    assert listings == []
    assert [s["slug"] for s in market.searches] == ["atlantis", "category"]
    messages = [record.getMessage() for record in caplog.records]
    assert any("did not recognise location.city_slug" in m for m in messages)
    assert any("saved search radius" in m for m in messages)
    assert any("results withheld" in m for m in messages)
    assert metrics.counter("fb_blocks_total", "", ("reason",)).value(reason="withheld") == 1


@pytest.mark.parametrize(
    ("mode", "reason"),
    [("checkpoint", "checkpoint"), ("login", "login required"), ("blocked", "temporarily blocked")],
)
async def test_poll_raises_source_blocked_on_walls(tmp_path: Path, http: HttpClient, mode: str, reason: str) -> None:
    metrics = Metrics()
    with pytest.raises(SourceBlocked) as excinfo:
        await _run_against(mode, tmp_path, http, profiles=["rtx3090"], metrics=metrics)
    assert excinfo.value.cooldown_seconds == 180 * 60
    assert reason in str(excinfo.value)
    blocks = metrics.counter("fb_blocks_total", "", ("reason",))
    assert blocks.value(reason=reason.replace(" ", "_")) == 1


async def test_run_once_applies_change_detection(tmp_path: Path, http: HttpClient) -> None:
    exe = await _require_chromium("full")
    market = FakeMarketplace("dom")
    server = TestServer(market.app(), host="127.0.0.1")
    await server.start_server()
    cfg = _fb_cfg(tmp_path, exe, profiles=["steamdeck"], scrolls_per_query=0)
    _write_state(Path(cfg.browser.storage_state_path))
    ingestor = fb.FbMarketplaceIngestor(cfg, _ctx(http, _app_config()), base_url=str(server.make_url("/")))
    ingestor.results_timeout_seconds = 3.0
    try:
        await ingestor.setup()
        first = await ingestor.run_once()
        second = await ingestor.run_once()
    finally:
        await ingestor.teardown()
        await server.close()
    assert len(first) == 3 and all(item.node_id == "test-node" for item in first)
    assert second == []  # unchanged listings are not re-emitted
    assert ingestor.health.polls == 2 and ingestor.health.state == "ok"


async def test_poll_restarts_a_crashed_browser(tmp_path: Path, http: HttpClient, caplog: pytest.LogCaptureFixture) -> None:
    exe = await _require_chromium("full")
    caplog.set_level("WARNING", logger="deal_radar.sources.fb_marketplace")
    server = TestServer(FakeMarketplace("dom").app(), host="127.0.0.1")
    await server.start_server()
    cfg = _fb_cfg(tmp_path, exe, profiles=["steamdeck"], scrolls_per_query=0)
    _write_state(Path(cfg.browser.storage_state_path))
    ingestor = fb.FbMarketplaceIngestor(cfg, _ctx(http, _app_config()), base_url=str(server.make_url("/")))
    ingestor.results_timeout_seconds = 3.0
    try:
        first = await ingestor.poll()
        assert ingestor._browser is not None
        await ingestor._browser.close()  # simulate a Chromium crash between polls
        second = await ingestor.poll()
    finally:
        await ingestor.teardown()
        await server.close()
    assert [item.source_id for item in second] == [item.source_id for item in first]
    assert any("browser died" in record.getMessage() for record in caplog.records)


async def test_poll_turns_navigation_failures_into_source_error(tmp_path: Path, http: HttpClient) -> None:
    exe = await _require_chromium("full")
    cfg = _fb_cfg(tmp_path, exe, profiles=["steamdeck"], scrolls_per_query=0)
    state_path = Path(cfg.browser.storage_state_path)
    _write_state(state_path)
    original_state = state_path.read_text()
    metrics = Metrics()
    # Nothing listens on port 9: every navigation fails with a Playwright error.
    ingestor = fb.FbMarketplaceIngestor(cfg, _ctx(http, _app_config(), metrics), base_url="http://127.0.0.1:9")
    try:
        with pytest.raises(SourceError, match="searches failed"):
            await ingestor.poll()
        assert ingestor._browser is not None and ingestor._browser.is_connected()  # browser kept for the next poll
        assert not ingestor._needs_restart
    finally:
        await ingestor.teardown()
    assert metrics.counter("fb_queries_total", "", ("outcome",)).value(outcome="error") == 1
    assert state_path.read_text() == original_state  # a session that never worked is not written back


# --------------------------------------------------------------------------- CLI


def test_cli_check_without_session_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(fb, "configure_logging", lambda *a, **k: None)
    config_path = tmp_path / "config.yaml"
    state_path = tmp_path / "state.json"
    config_path.write_text(
        "sources:\n"
        "  fb_marketplace:\n"
        "    location: {latitude: 40.7, longitude: -74.0, city_slug: nyc}\n"
        f"    browser: {{storage_state_path: '{state_path}'}}\n",
        encoding="utf-8",
    )
    env_file = tmp_path / "missing.env"
    assert fb.main(["--config", str(config_path), "--env-file", str(env_file), "--check"]) == 1
    assert "no session file" in capsys.readouterr().out

    _write_state(state_path, cookies=False)
    assert fb.main(["--config", str(config_path), "--env-file", str(env_file), "--check"]) == 1
    assert "no Facebook session cookies" in capsys.readouterr().out

    assert fb.main(["--config", str(tmp_path / "nope.yaml"), "--env-file", str(env_file), "--check"]) == 2
    assert "config error" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        fb.main(["--config", str(config_path)])  # --login or --check is required


class FakeFacebookAuth:
    """Local stand-in for the login page / marketplace landing used by the CLI flows."""

    def __init__(self, *, login_redirects: bool, logged_in: bool) -> None:
        self.login_redirects = login_redirects
        self.logged_in = logged_in

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/login/", self.login)
        app.router.add_get("/home/", self.home)
        app.router.add_get("/marketplace/", self.marketplace)
        return app

    async def login(self, request: web.Request) -> web.Response:
        # A logged-in browser is bounced away from /login/ once the operator finished.
        script = "<script>setTimeout(() => { location.href = '/home/'; }, 150)</script>" if self.login_redirects else ""
        form = "<form id='login_form'><input name='email'><input type='password' name='pass'></form>"
        return web.Response(text=_HTML_HEAD + form + script + _HTML_TAIL, content_type="text/html")

    async def home(self, request: web.Request) -> web.Response:
        return web.Response(text=_HTML_HEAD + "<div role='main'>Feed</div>" + _HTML_TAIL, content_type="text/html")

    async def marketplace(self, request: web.Request) -> web.Response:
        if not self.logged_in:
            raise web.HTTPFound("/login/?next=%2Fmarketplace%2F")
        return web.Response(text=_HTML_HEAD + "<h1>Marketplace</h1>" + _HTML_TAIL, content_type="text/html")


def _cli_config(tmp_path: Path, exe: str | None) -> AppConfig:
    return AppConfig.model_validate(
        {
            "sources": {
                "fb_marketplace": {
                    "location": _geo().model_dump(),
                    "browser": {"headless": True, "executable_path": exe, "storage_state_path": str(tmp_path / "cli_state.json")},
                }
            }
        }
    )


@pytest.mark.parametrize("finishes", [True, False])
async def test_run_login_waits_for_session_and_saves_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], finishes: bool
) -> None:
    exe = await _require_chromium("full")
    monkeypatch.setattr(fb, "LOGIN_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(fb, "CHECK_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(fb, "LOGIN_POLL_SECONDS", 0.1)
    config = _cli_config(tmp_path, exe)
    state_path = Path(config.sources.fb_marketplace.browser.storage_state_path)
    _write_state(state_path)  # stands in for the cookies the operator's manual login produces
    state_path.chmod(0o644)
    auth = FakeFacebookAuth(login_redirects=finishes, logged_in=True)
    server = TestServer(auth.app(), host="127.0.0.1")
    await server.start_server()
    try:
        code = await fb.run_login(
            config, timeout_seconds=10.0 if finishes else 0.5, base_url=str(server.make_url("/")).rstrip("/"), headless=True
        )
    finally:
        await server.close()
    out = capsys.readouterr().out
    if finishes:
        assert code == 0 and "Session saved" in out
        assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
        assert fb.has_session_cookie(json.loads(state_path.read_text())["cookies"])
    else:
        assert code == 1 and "Timed out" in out
        assert stat.S_IMODE(state_path.stat().st_mode) == 0o644  # untouched


@pytest.mark.parametrize("logged_in", [True, False])
async def test_run_check_reports_session_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], logged_in: bool
) -> None:
    exe = await _require_chromium("full")
    monkeypatch.setattr(fb, "CHECK_SETTLE_SECONDS", 0.0)
    config = _cli_config(tmp_path, exe)
    _write_state(Path(config.sources.fb_marketplace.browser.storage_state_path))
    server = TestServer(FakeFacebookAuth(login_redirects=False, logged_in=logged_in).app(), host="127.0.0.1")
    await server.start_server()
    try:
        code = await fb.run_check(config, base_url=str(server.make_url("/")).rstrip("/"))
    finally:
        await server.close()
    out = capsys.readouterr().out
    if logged_in:
        assert code == 0 and out.startswith("Logged in")
    else:
        assert code == 1 and "NOT logged in (login_required)" in out


# --------------------------------------------------------------------------- adversarial-review regressions


def test_iter_json_documents_survives_pathological_nesting() -> None:
    # orjson refuses >1024 levels and the stdlib fallback raises RecursionError (not
    # ValueError) on deep input: a hostile/corrupt chunk must be skipped, not kill the poll.
    deep = "[" * 100_000 + "]" * 100_000
    body = '{"a":1}' + deep + '{"b":2}\n' + deep + "\n" + '{"c":3}'
    assert list(fb.iter_json_documents(body)) == [{"a": 1}, {"c": 3}]
    listings, stats = fb.parse_payloads([("graphql", body), ("graphql", deep)])
    assert listings == [] and stats["documents"] == 2
    cursor = '{"c2c":' + "[" * 100_000 + "]" * 100_000 + "}"
    feed = {"feed_units": {"edges": [], "page_info": {"end_cursor": cursor}}}
    assert fb.count_withheld_feeds(feed) == 0


def test_hidden_listings_are_skipped() -> None:
    node = {"id": "77", "marketplace_listing_title": "RTX 4090", "listing_price": {"amount": "900"}, "is_hidden": True}
    listings, stats = fb.parse_payloads([("graphql", json.dumps({"data": {"n": node}}))])
    assert listings == [] and stats["hidden"] == 1


@pytest.mark.parametrize(
    ("text", "code"),
    [("PHP6,500", "PHP"), ("1 200 PLN", "PLN"), ("CHF 120", "CHF"), ("CAD $500", "CAD"), ("RTX 3090", None), ("500 OBO", None)],
)
def test_currency_from_iso_codes(text: str, code: str | None) -> None:
    assert fb.currency_from_text(text) == code


def test_iso_formatted_price_is_not_mislabelled_usd() -> None:
    node = {"id": "88", "marketplace_listing_title": "RTX 3090", "listing_price": {"formatted_amount": "PHP6,500", "amount": "6500.00"}}
    (listing,) = fb.parse_graphql_payload(json.dumps({"data": node}))
    assert listing.price == 6500.0 and listing.currency == "PHP"


def test_search_feed_is_preferred_over_other_rails() -> None:
    def listing(lid: str) -> dict[str, Any]:
        return {"id": lid, "marketplace_listing_title": f"RTX 3090 #{lid}", "listing_price": {"amount": "500"}}

    page = {
        "data": {
            "marketplace_search": {"feed_units": {"edges": [{"node": {"listing": listing("101")}}]}},
            "viewer": {"marketplace_feed_stories": {"edges": [{"node": {"listing": listing("999")}}]}},  # "Today's picks"
        }
    }
    deferred = {"label": "x$defer$y", "path": ["marketplace_search", "feed_units", "edges", 1, "node", "listing"],
                "data": listing("102")}
    body = json.dumps(page) + "\n" + json.dumps(deferred)
    listings, stats = fb.parse_payloads([("graphql", body)])
    assert [item.source_id for item in listings] == ["101", "102"]
    assert stats["other_rail"] == 1
    # Without any marketplace_search root the generic walk still finds listings (path drift).
    assert [item.source_id for item in fb.parse_graphql_payload(json.dumps({"data": {"renamed": [listing("5")]}}))] == ["5"]


def test_parse_dom_cards_splits_concatenated_prices() -> None:
    cards = [{"id": "4949494949494949", "lines": ["$350$400", "RTX 3080", "Queens, NY"], "img": None, "alt": None},
             {"id": "5050505050505050", "lines": ["CA$1,200CA$1,500", "RTX 4090", "Toronto, ON"], "img": None, "alt": None}]
    first, second = fb.parse_dom_cards(cards)
    assert (first.price, first.list_price, first.title) == ("$350", "$400", "RTX 3080")
    assert first.location is not None and first.location.text == "Queens, NY"
    assert (second.price, second.list_price, second.currency, second.title) == ("CA$1,200", "CA$1,500", "CAD", "RTX 4090")


class _StateContext:
    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state

    async def storage_state(self) -> dict[str, Any]:
        return self.state

    async def close(self) -> None:
        return None


async def test_save_state_never_overwrites_a_session_with_a_logged_out_jar(tmp_path: Path, http: HttpClient) -> None:
    cfg = _fb_cfg(tmp_path, None)
    state_path = Path(cfg.browser.storage_state_path)
    _write_state(state_path)
    original = state_path.read_text()
    ingestor = fb.FbMarketplaceIngestor(cfg, _ctx(http, _app_config()))
    ingestor._context = _StateContext({"cookies": [{"name": "datr", "value": "x", "domain": ".facebook.com"}], "origins": []})  # type: ignore[assignment]
    ingestor._session_ok = True
    await ingestor._save_state()
    assert state_path.read_text() == original
    fresh = json.loads(original)
    fresh["cookies"][0]["value"] = "100099999999999"
    ingestor._context = _StateContext(fresh)  # type: ignore[assignment]
    await ingestor._save_state()
    assert json.loads(state_path.read_text()) == fresh
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
    ingestor._context = None


class _Resp:
    def __init__(self, text: str) -> None:
        self.url = "https://www.facebook.com/api/graphql/"
        self.status = 200
        self._text = text

    async def body(self) -> bytes:
        return self._text.encode()


async def test_late_graphql_body_does_not_wake_the_next_search(http: HttpClient, tmp_path: Path) -> None:
    ingestor = fb.FbMarketplaceIngestor(_fb_cfg(tmp_path, None), _ctx(http, _app_config()))
    old_sink: list[str] = []
    ingestor._capture = []  # a newer search is running
    await ingestor._read_payload(_Resp(GRAPHQL_PAGE2), old_sink)  # type: ignore[arg-type]
    assert not ingestor._captured.is_set()
    assert ingestor._capture == []


async def test_poll_respects_hourly_search_budget(tmp_path: Path, http: HttpClient, monkeypatch: pytest.MonkeyPatch) -> None:
    async def no_pause(*args: Any, **kwargs: Any) -> float:
        return 0.0

    monkeypatch.setattr(fb, "human_pause", no_pause)
    cfg = _fb_cfg(tmp_path, None, profiles=["multi"], poll_timeout_seconds=600)
    ingestor = fb.FbMarketplaceIngestor(cfg, _ctx(http, _app_config()))
    ingestor.max_searches_per_hour = 2
    now = [10_000.0]
    ingestor._clock = lambda: now[0]
    searched: list[str] = []
    ensured = [0]

    class _Page:
        def is_closed(self) -> bool:
            return False

    async def fake_ensure() -> Any:
        ensured[0] += 1
        return _Page()

    async def fake_search(page: Any, profile: Any, term: str) -> list[Any]:
        searched.append(term)
        return []

    monkeypatch.setattr(ingestor, "_ensure_page", fake_ensure)
    monkeypatch.setattr(ingestor, "_search", fake_search)
    assert await ingestor.poll() == []
    assert searched == ["alpha", "beta"]
    assert ingestor.next_interval() >= 3600 - 1  # wait for the window instead of waking up for nothing
    now[0] += 600
    assert await ingestor.poll() == []
    assert searched == ["alpha", "beta"] and ensured[0] == 1  # budget exhausted: the browser is not touched
    now[0] += 3001
    await ingestor.poll()
    assert searched == ["alpha", "beta", "gamma", "alpha"]
    assert ingestor.next_interval() >= 3600 - 1
    now[0] += 3601
    assert ingestor.next_interval() <= cfg.poll_interval_seconds * (1 + cfg.jitter_pct) + 1e-6


def _add_local_marker(path: Path, value: str) -> None:
    state = json.loads(path.read_text())
    state["cookies"].append({"name": "marker", "value": value, "domain": "127.0.0.1", "path": "/", "expires": -1,
                             "httpOnly": False, "secure": False, "sameSite": "Lax"})
    path.write_text(json.dumps(state), encoding="utf-8")


class _CookieMarketplace(FakeMarketplace):
    def __init__(self, mode: str) -> None:
        super().__init__(mode)
        self.markers: list[str | None] = []

    async def search(self, request: web.Request) -> web.Response:
        self.markers.append(request.cookies.get("marker"))
        return await super().search(request)


async def test_session_wall_reloads_refreshed_session_file(tmp_path: Path, http: HttpClient) -> None:
    # After a login wall the operator re-runs --login, which rewrites the storage-state
    # file. The running collector must pick the new cookies up on its next poll instead
    # of presenting the dead session forever.
    exe = await _require_chromium("full")
    market = _CookieMarketplace("login")
    server = TestServer(market.app(), host="127.0.0.1")
    await server.start_server()
    cfg = _fb_cfg(tmp_path, exe, profiles=["steamdeck"], scrolls_per_query=0)
    state_path = Path(cfg.browser.storage_state_path)
    _write_state(state_path)
    _add_local_marker(state_path, "old")
    ingestor = fb.FbMarketplaceIngestor(cfg, _ctx(http, _app_config()), base_url=str(server.make_url("/")))
    ingestor.results_timeout_seconds = 2.0
    try:
        with pytest.raises(SourceBlocked):
            await ingestor.poll()
        _write_state(state_path)  # what `--login` leaves behind
        _add_local_marker(state_path, "new")
        market.mode = "dom"
        listings = await ingestor.poll()
    finally:
        await ingestor.teardown()
        await server.close()
    assert market.markers == ["old", "new"]
    assert len(listings) == 3


async def test_poll_recovers_after_renderer_crash(tmp_path: Path, http: HttpClient) -> None:
    exe = await _require_chromium("full")
    server = TestServer(FakeMarketplace("dom").app(), host="127.0.0.1")
    await server.start_server()
    cfg = _fb_cfg(tmp_path, exe, profiles=["steamdeck"], scrolls_per_query=0)
    _write_state(Path(cfg.browser.storage_state_path))
    ingestor = fb.FbMarketplaceIngestor(cfg, _ctx(http, _app_config()), base_url=str(server.make_url("/")))
    ingestor.results_timeout_seconds = 2.0
    try:
        first = await ingestor.poll()
        assert ingestor._context is not None and ingestor._page is not None
        cdp = await ingestor._context.new_cdp_session(ingestor._page)
        with pytest.raises(Exception):  # noqa: B017 - the renderer dies before it can answer
            await asyncio.wait_for(cdp.send("Page.crash"), timeout=2.0)
        await asyncio.sleep(0.3)
        second = await ingestor.poll()
        third = await ingestor.poll()
    finally:
        await ingestor.teardown()
        await server.close()
    assert [i.source_id for i in second] == [i.source_id for i in first] == [i.source_id for i in third]


async def test_poll_recovers_persistent_profile_after_browser_death(tmp_path: Path, http: HttpClient) -> None:
    exe = await _require_chromium("full")
    server = TestServer(FakeMarketplace("dom").app(), host="127.0.0.1")
    await server.start_server()
    browser_cfg = BrowserSection(
        headless=True,
        executable_path=exe,
        storage_state_path=str(tmp_path / "fb_state.json"),
        user_data_dir=str(tmp_path / "profile"),
        min_action_delay_seconds=0.0,
        max_action_delay_seconds=0.02,
        navigation_timeout_seconds=20,
    )
    cfg = _fb_cfg(tmp_path, exe, profiles=["steamdeck"], scrolls_per_query=0, browser=browser_cfg)
    ingestor = fb.FbMarketplaceIngestor(cfg, _ctx(http, _app_config()), base_url=str(server.make_url("/")))
    ingestor.results_timeout_seconds = 2.0
    try:
        first = await ingestor.poll()
        assert ingestor._browser is None and ingestor._context is not None  # persistent context
        await ingestor._context.close()  # the profile's browser goes away between polls
        second = await ingestor.poll()
    finally:
        await ingestor.teardown()
        await server.close()
    assert first and [i.source_id for i in second] == [i.source_id for i in first]
    assert ingestor._context is None and ingestor._playwright is None


async def test_run_login_ignores_stale_cookies_while_a_login_form_is_shown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # The saved (server-side invalidated) session still carries c_user/xs, and Facebook
    # shows its login form on a page whose path is not /login: that is not a login.
    exe = await _require_chromium("full")
    monkeypatch.setattr(fb, "LOGIN_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(fb, "CHECK_SETTLE_SECONDS", 0.0)
    monkeypatch.setattr(fb, "LOGIN_POLL_SECONDS", 0.1)
    config = _cli_config(tmp_path, exe)
    state_path = Path(config.sources.fb_marketplace.browser.storage_state_path)
    _write_state(state_path)
    original = state_path.read_text()

    async def login(request: web.Request) -> web.Response:
        raise web.HTTPFound("/")

    async def home(request: web.Request) -> web.Response:
        form = "<form id='login_form' action='/login/'><input name='email'><input type='password' name='pass'></form>"
        return web.Response(text=_HTML_HEAD + form + _HTML_TAIL, content_type="text/html")

    app = web.Application()
    app.router.add_get("/login/", login)
    app.router.add_get("/", home)
    server = TestServer(app, host="127.0.0.1")
    await server.start_server()
    try:
        code = await fb.run_login(config, timeout_seconds=1.0, base_url=str(server.make_url("/")).rstrip("/"), headless=True)
    finally:
        await server.close()
    assert code == 1 and "Timed out" in capsys.readouterr().out
    assert state_path.read_text() == original


async def test_run_check_reports_a_browser_that_cannot_start(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    pytest.importorskip("playwright.async_api")
    config = _cli_config(tmp_path, str(tmp_path / "no-such-chrome"))
    _write_state(Path(config.sources.fb_marketplace.browser.storage_state_path))
    assert await fb.run_check(config, base_url="http://127.0.0.1:9") == 1
    assert "Could not start the browser" in capsys.readouterr().out
