"""Tests for the Slickdeals RSS ingestor (``deal_radar/sources/slickdeals_rss.py``).

Network I/O is served by a local ``aiohttp`` :class:`TestServer` impersonating
``slickdeals.net/newsearch.php`` (frontpage, Hot Deals forum and keyword-search RSS);
the ingestor is pointed at it through the configurable feed URLs and search template.
(``aioresponses`` 0.7.x cannot build responses for aiohttp 3.14, whose
``ClientResponse`` requires a ``stream_writer``.) Feed bodies are recorded-style RSS
2.0 documents under ``tests/fixtures/slickdeals/`` that mirror the live structure:
CDATA titles, utm-tagged thread links, ``thread-<id>``/permalink guids and
``content:encoded`` with thumbnail, ``Thumb Score`` and ``/click`` outlinks carrying
``data-store-slug``/``data-aps-asin``.
"""

from __future__ import annotations

import asyncio
import inspect
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from deal_radar.config_schema import AppConfig, FeedSpec, SlickdealsSource, load_config
from deal_radar.core.backoff import BackoffPolicy
from deal_radar.core.http import HttpClient, NetworkSettings
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import RawListing, SourceKind
from deal_radar.sources.base import IngestorContext, SourceBlocked, SourceError
from deal_radar.sources.slickdeals_rss import (
    BLOCK_COOLDOWN_MAX_SECONDS,
    FEED_ACCEPT,
    MAX_IMAGES,
    SEARCH_FEEDS_PER_POLL,
    EntrySkipped,
    FeedTarget,
    SlickdealsIngestor,
    build_listing,
    build_search_feed_url,
    entry_posted_at,
    entry_source_id,
    extract_body_price,
    extract_images,
    extract_list_price,
    extract_outbound_url,
    extract_price,
    extract_retailer,
    extract_shipping,
    extract_specs,
    html_to_text,
    is_block_response,
    looks_like_feed,
    looks_like_html,
    lookup_store,
    merge_duplicates,
    normalize_store,
    parse_entry,
    parse_feed,
    parse_feed_listings,
    resolve_price,
    search_feed_targets,
    strip_tracking,
    thread_id_from_url,
    title_tags,
)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "slickdeals"
RSS_HEADERS = {"Content-Type": "text/xml; charset=UTF-8", "Cache-Control": "private, max-age=0, no-cache"}
CHALLENGE_HEADERS = {"Content-Type": "text/html; charset=UTF-8", "cf-mitigated": "challenge", "Server": "cloudflare"}


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def fixture_listings(name: str, feed_name: str | None = None, **target: Any) -> list[RawListing]:
    feed = FeedTarget(name=feed_name or name.removesuffix(".xml"), url="https://slickdeals.net/rss", **target)
    parsed = parse_feed_listings(fixture_bytes(name), feed, "text/xml; charset=UTF-8")
    assert not parsed.malformed
    return parsed.listings


def by_id(listings: list[RawListing]) -> dict[str, RawListing]:
    return {raw.source_id: raw for raw in listings}


@pytest.fixture(scope="module")
def app_config() -> AppConfig:
    return load_config(CONFIG_PATH, env={})


# --------------------------------------------------------------------------- fixture feeds


def test_frontpage_fixture_entries() -> None:
    listings = fixture_listings("frontpage.xml")
    assert [raw.source_id for raw in listings] == ["20105466", "20104987", "20103311"]

    mac = listings[0]
    assert mac.source == "slickdeals"
    assert mac.source_kind is SourceKind.AGGREGATOR
    assert mac.listing_key == "slickdeals:20105466"
    assert mac.url == "https://slickdeals.net/f/20105466-apple-2026-mac-mini-m6-24gb-512gb-ssd-1-149-99-at-amazon"
    assert mac.title == "Apple 2026 Mac mini Desktop: M6 Chip, 24GB RAM, 512GB SSD $1150 + Free S&H"
    assert mac.price == 1149.99  # editorial title rounds; body has the exact amount
    assert mac.currency == "USD"
    assert mac.shipping == 0.0
    assert mac.list_price is None  # "Previous Frontpage Deal at $1199.99" is not a list price
    assert mac.retailer == "Amazon"
    assert mac.outbound_url == "https://www.amazon.com/dp/B0FMMN6QRV"
    assert mac.image_urls == ["https://static.slickdealscdn.com/attachment/3/5/9/4/0/2/8/300x300/21824502.thumb"]
    assert mac.posted_at == datetime(2026, 10, 6, 4, 28, 34, tzinfo=timezone.utc)
    assert mac.condition is None and mac.query is None and mac.profile_hint is None
    assert mac.extra["frontpage"] is True and mac.extra["popular"] is False
    assert mac.extra["thumb_score"] == 21
    assert mac.extra["feed_category"] == "Frontpage Deals"
    assert mac.extra["store_slug"] == "amazon" and mac.extra["asin"] == "B0FMMN6QRV"
    assert mac.extra["author"] == "HonestHamster907"
    assert mac.extra["feeds"] == ["frontpage"]
    assert "Thumb Score" not in mac.description
    assert mac.description.startswith("Amazon has Apple 2026 Mac mini Desktop (MHQM4LL/A) for $1149.99.")
    assert "[LIST]" not in mac.description and "<" not in mac.description

    monitor = listings[1]
    assert monitor.price == 749.99 and monitor.list_price == 1199.99 and monitor.shipping == 0.0
    assert monitor.retailer == "Dell"
    # Store URL embedded in the /click redirect's query string.
    assert monitor.outbound_url == "https://www.dell.com/en-us/shop/alienware-32-4k-qd-oled-gaming-monitor-aw3225qf/apd/210-bkxs"
    assert monitor.extra["panel"] == "QD-OLED" and monitor.extra["refresh_hz"] == 240 and monitor.extra["size_in"] == 32

    tv = listings[2]
    assert tv.price == 1296.99 and tv.list_price == 1799.99 and tv.retailer == "Best Buy"
    assert tv.outbound_url is None  # only an opaque /click link
    assert tv.extra["panel"] == "OLED" and tv.extra["size_in"] == 65 and tv.extra["refresh_hz"] == 144


def test_hot_deals_forum_fixture_entries() -> None:
    feed = FeedTarget(name="hot_deals_forum", url="https://slickdeals.net/rss")
    parsed = parse_feed_listings(fixture_bytes("hot_deals_forum.xml"), feed, "text/xml; charset=UTF-8")
    assert parsed.skipped == {"no_price": 1}  # "Extra 20% Off Select Gaming Monitors at Walmart"
    assert parsed.errors == 0
    items = by_id(parsed.listings)
    assert set(items) == {"20105590", "20105512", "20105477", "20104987", "20105488"}

    anker = items["20105590"]
    assert anker.price == 13.27 and anker.retailer == "Amazon" and anker.shipping == 0.0
    assert anker.extra["title_tags"] == ["Prime"]
    assert anker.extra["frontpage"] is False

    gpu = items["20105512"]
    assert gpu.price == 2499.99 and gpu.list_price == 2799.99 and gpu.shipping == 0.0
    assert gpu.retailer == "Newegg"
    assert gpu.outbound_url is not None and gpu.outbound_url.startswith("https://www.newegg.com/asus-tuf-gaming-tuf-rtx5090")
    assert gpu.extra["vram_gb"] == 32 and gpu.extra["gpu_model"] == "RTX 5090"
    assert gpu.extra["feed_category"] == "Slickdeals Hot Deals Forum Forum"
    assert gpu.posted_at == datetime(2026, 10, 6, 4, 35, 7, tzinfo=timezone.utc)

    micro = items["20105477"]
    assert micro.retailer == "Micro Center"  # "Micro Center: ..." title prefix, no outlinks
    assert micro.price == 1599.99 and micro.shipping is None
    assert micro.extra["in_store_only"] is True
    assert micro.extra["vram_gb"] == 24  # "24G" right after the model
    # Smilie GIF skipped, the attachment kept.
    assert micro.image_urls == ["https://static.slickdealscdn.com/attachment/7/7/4/5/0/1/2/21824510.attach"]

    old_guid = items["20104987"]  # <guid isPermaLink="true"> full URL (older item style)
    assert old_guid.retailer == "Dell" and old_guid.price == 749.99

    tv = items["20105488"]
    assert tv.price == 1097.99 and tv.shipping == 4.99 and tv.list_price == 1399.99
    assert tv.retailer == "Samsung" and tv.extra["size_in"] == 65


def test_search_fixture_carries_query_and_profile_hint() -> None:
    listings = fixture_listings("search_rtx_5090.xml", "search:rtx 5090", query="rtx 5090", profile_hint="rtx_5090")
    items = by_id(listings)
    assert set(items) == {"20105512", "20101234", "20100777"}
    assert all(raw.query == "rtx 5090" and raw.profile_hint == "rtx_5090" for raw in listings)

    omen = items["20101234"]
    assert omen.price == 3999.99 and omen.shipping == 0.0 and omen.retailer == "HP"
    assert omen.extra["vram_gb"] == 32  # not the 64GB DDR5 system RAM
    assert omen.outbound_url == "https://www.hp.com/us-en/shop/pdp/omen-45l-gaming-desktop-gt22-3000-pc-bundle?jumpid=ma_sd"

    laptop = items["20100777"]  # fuzzy search noise: an RTX 5060 laptop
    assert laptop.extra["gpu_model"] == "RTX 5060" and laptop.extra["vram_gb"] == 8
    assert laptop.extra["size_in"] == 15 and laptop.extra["refresh_hz"] == 165 and laptop.extra["panel"] == "OLED"


def test_same_thread_in_several_feeds_has_one_source_id_and_merges() -> None:
    frontpage = fixture_listings("frontpage.xml")
    forum = fixture_listings("hot_deals_forum.xml")
    search = fixture_listings("search_rtx_5090.xml", "search:rtx 5090", query="rtx 5090", profile_hint="rtx_5090")
    fp_monitor, forum_monitor = by_id(frontpage)["20104987"], by_id(forum)["20104987"]
    assert fp_monitor.listing_key == forum_monitor.listing_key  # thread-<id> guid vs permalink guid
    assert by_id(forum)["20105512"].listing_key == by_id(search)["20105512"].listing_key

    merged = by_id(merge_duplicates([*frontpage, *forum, *search]))
    assert len(merged) == 3 + 5 + 3 - 2
    monitor = merged["20104987"]
    assert monitor.extra["feeds"] == ["frontpage", "hot_deals_forum"]
    assert monitor.extra["frontpage"] is True
    assert monitor.title.startswith("Alienware 32\"")  # first feed wins
    assert monitor.list_price == 1199.99

    gpu = merged["20105512"]
    assert gpu.extra["feeds"] == ["hot_deals_forum", "search:rtx 5090"]
    assert gpu.query == "rtx 5090" and gpu.profile_hint == "rtx_5090"  # filled from the search feed
    assert gpu.extra["thumb_score"] == 13  # the fresher (higher) live score


# --------------------------------------------------------------------------- extraction tables


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Apple 2026 Mac mini Desktop: M6 Chip, 24GB RAM, 512GB SSD $1150 + Free S&H", 1150.0),
        ("[Prime] $13.27* | Anker 735 Charger (Nano II 65W) at Amazon", 13.27),
        ("[Prime, SnS] $8.54* | Amazon Basics AA Batteries 48-Pack", 8.54),
        ("$899: Samsung 49\" Odyssey OLED G9 Gaming Monitor at Amazon", 899.0),
        ("LG 65\" Class C4 OLED $1,296.99 + Free Shipping at Best Buy", 1296.99),
        ("ASUS TUF RTX 5090 32GB $2,499.99 at Newegg (Reg. $2,799.99)", 2499.99),
        ("Save $200: LG C5 OLED 65\" $1,599.99 at Best Buy", 1599.99),
        ("Samsung 65\" S90D $1,097.99 + $4.99 shipping (was $1,399.99) from Samsung", 1097.99),
        ("2-Pack Samsung 990 Pro 2TB $299.99 ($150/ea) at Amazon", 299.99),
        ("MSI RTX 4090 Gaming X Trio $1,599.99 + $100 Newegg Gift Card", 1599.99),
        ("Steam Deck OLED 512GB $439 w/ $50 Steam credit", 439.0),
        ("Sony a7 IV Body $1,698 - $300 Gift Card at B&H", 1698.0),
        ("Dell 27\" 4K Monitor $199.99 or less at Dell", 199.99),
        ("$189.99 FS AMAZON Crucial T705 2TB", 189.99),
        ("Extra 20% Off Select Gaming Monitors at Walmart", None),
        ("Spend $100, Get $20 Off at Target", None),
        ("$20 off $100+ orders at Target", None),
        ("Free Steam game this weekend", None),
    ],
)
def test_extract_price(title: str, expected: float | None) -> None:
    assert extract_price(title) == expected


@pytest.mark.parametrize(
    ("title", "body_text", "expected"),
    [
        ("Mac mini M6 $1150 + Free S&H", "Amazon has Mac mini for $1149.99. Shipping is free.", 1149.99),
        ("Mac mini M6 $1150 + Free S&H", "Amazon has Mac mini for $1,099.99.", 1150.0),  # > $1 away: not a rounding
        ("LG C4 $1,296.99 at Best Buy", "Best Buy has it for $1,296.49", 1296.99),  # title has cents: kept
        ("Micro Center: MSI GeForce RTX 4090", "Micro Center has MSI GeForce RTX 4090 for $1,599.99.", 1599.99),
        ("Walmart Rollback on monitors", "Walmart has monitors for $20 off this week.", None),
        ("Walmart Rollback on monitors", "", None),
    ],
)
def test_resolve_price_uses_body(title: str, body_text: str, expected: float | None) -> None:
    assert resolve_price(title, body_text) == expected


def test_extract_body_price_only_reads_the_editorial_phrase() -> None:
    assert extract_body_price("Previous Frontpage Deal at $1199.99") is None
    assert extract_body_price("Newegg has the card for just $2,099.99 shipped.") == 2099.99


@pytest.mark.parametrize(
    ("title", "body_text", "body_html", "price", "expected"),
    [
        ("ASUS RTX 5090 $2,499.99 at Newegg (Reg. $2,799.99)", "", "", 2499.99, 2799.99),
        ("Alienware AW3225QF $749.99 (List Price $1,199.99)", "", "", 749.99, 1199.99),
        ("Sony WH-1000XM5 $248 MSRP $399", "", "", 248.0, 399.0),
        ("Logitech G Pro X $99.99 (Orig. $149.99)", "", "", 99.99, 149.99),
        ("Samsung S90D $1,097.99 was $1,399.99", "", "", 1097.99, 1399.99),
        ("Alienware AW3225QF $749.99", "List Price: $1,199.99", "", 749.99, 1199.99),
        ("Alienware AW3225QF $749.99", "Retails for $999.99 elsewhere", "", 749.99, 999.99),
        ("Samsung S90D $1,097.99", "", "Was <del>$1,399.99</del>", 1097.99, 1399.99),
        ("Weird $999 (was $899)", "", "", 999.0, None),  # "was" below the price is not a list price
        ("Typo $50 (Reg. $5,000,000)", "", "", 50.0, None),  # absurd ratio
        ("Mac mini $1150", "Previous Frontpage Deal at $1199.99", "", 1149.99, None),
        ("No price here (Reg. $129.99)", "", "", None, 129.99),
    ],
)
def test_extract_list_price(title: str, body_text: str, body_html: str, price: float | None, expected: float | None) -> None:
    assert extract_list_price(title, body_text, body_html, price) == expected


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Alienware AW3225QF 32\" 4K QD-OLED $749.99 @ Dell", "Dell"),
        ("LG 65\" C4 OLED $1,296.99 + Free Shipping at Best Buy", "Best Buy"),
        ("Samsung S90D $1,097.99 + $4.99 shipping (was $1,399.99) from Samsung", "Samsung"),
        ("Micro Center: MSI GeForce RTX 4090 $1,599.99 (In-Store Only)", "Micro Center"),
        ("[Prime] $13.27* | Anker Charger at Amazon", "Amazon"),
        ("Sony a7 IV Body $1,698 at B&H Photo Video", "B&H Photo"),
        ("Bose QC45 $179.99 at Woot!", "Woot"),
        ("RTX 4090 FE $1,599 at Best Buy (YMMV)", "Best Buy"),
        ("Cooler $49.99 at Bob's Hardware", "Bob's Hardware"),
        ("4K Gaming Monitor at 240Hz for $299", None),
        ("Apple Mac mini M6 $1150 + Free S&H", None),
    ],
)
def test_extract_retailer_from_title(title: str, expected: str | None) -> None:
    assert extract_retailer(title) == expected


def test_extract_retailer_precedence() -> None:
    body = '<div><a data-store-slug="walmart" href="https://slickdeals.net/click?lno=1">Walmart</a> has it.</div>'
    assert extract_retailer("Thing $10 at Amazon", body) == "Walmart"  # structured slug beats the title
    exit_only = '<a data-product-exitwebsite="bhphotovideo.com" href="https://slickdeals.net/click?lno=2">B&amp;H</a>'
    assert extract_retailer("Sony a7 IV $1,698", exit_only) == "B&H Photo"
    has_only = '<div><a href="https://slickdeals.net/click?lno=3" rel="nofollow">Costco</a> has the TV for <b>$999</b></div>'
    assert extract_retailer("LG C4 $999", has_only) == "Costco"
    link = "https://slickdeals.net/f/20103311-lg-65-c4-oled-evo-1-296-99-at-best-buy?utm_source=rss"
    assert extract_retailer("LG 65 C4 OLED $1,296.99", "", link) == "Best Buy"
    assert extract_retailer("LG 65 C4 OLED $1,296.99", "", "https://slickdeals.net/f/1234567-lg-at-home-sale") is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("best-buy", "Best Buy"),
        ("bestbuy.com", "Best Buy"),
        ("www.newegg.com", "Newegg"),
        ("Amazon & Walmart", "Amazon"),
        ("B&H", "B&H Photo"),
        ("amazon", "Amazon"),
        ("some-local-shop", "Some Local Shop"),
        ("Woot!", "Woot"),
        ("", None),
        ("$5", None),
        ("x" * 60, None),
    ],
)
def test_normalize_store(raw: str, expected: str | None) -> None:
    assert normalize_store(raw) == expected


def test_lookup_store_only_knows_known_stores() -> None:
    assert lookup_store("Micro Center") == "Micro Center"
    assert lookup_store("dell.com") == "Dell"
    assert lookup_store("home") is None


@pytest.mark.parametrize(
    ("title", "body_text", "price", "expected"),
    [
        ("Mac mini $1150 + Free S&H", "", 1150.0, 0.0),
        ("$189.99 FS AMAZON Crucial T705", "", 189.99, 0.0),
        ("Anker Charger $25.99 + F/S", "", 25.99, 0.0),
        ("Samsung S90D $1,097.99 + $4.99 shipping", "", 1097.99, 4.99),
        ("Dell monitor $99 + free shipping", "", 99.0, 0.0),
        ("Bose QC45 $179.99 Ships Free", "", 179.99, 0.0),
        ("LG C4 $1,296.99", "Best Buy has it. Shipping is free.", 1296.99, 0.0),
        ("USB cable $5.99", "Free shipping on orders $35+ or with Prime.", 5.99, None),
        ("Monitor $199.99", "Free shipping on orders over $35.", 199.99, 0.0),
        ("Monitor $199.99", "Shipping is $9.99 to most states.", 199.99, 9.99),
        ("Tiffs Treats Gift Box $20", "", 20.0, None),
        ("MSI RTX 4090 $1,599.99 (In-Store Only)", "In-store pickup only.", 1599.99, None),
    ],
)
def test_extract_shipping(title: str, body_text: str, price: float, expected: float | None) -> None:
    assert extract_shipping(title, body_text, price) == expected


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("ASUS TUF Gaming GeForce RTX 5090 32GB GDDR7 OC Edition", {"gpu_model": "RTX 5090", "vram_gb": 32}),
        ("HP OMEN 45L: Core Ultra 9 285K, RTX 5090 32GB, 64GB DDR5, 2TB SSD", {"gpu_model": "RTX 5090", "vram_gb": 32}),
        ("CyberPower Gamer Supreme RTX 5090, 64GB DDR5 RAM, 4TB SSD", {"gpu_model": "RTX 5090", "vram_gb": 32}),
        ("MSI GeForce RTX 4090 Gaming X Trio 24G", {"gpu_model": "RTX 4090", "vram_gb": 24}),
        ("ASUS ROG Strix Laptop, RTX 5090 Laptop GPU 24GB, 32GB DDR5", {"gpu_model": "RTX 5090", "vram_gb": 24}),
        ("Razer Blade 16 Laptop RTX 5090, 32GB DDR5", {"gpu_model": "RTX 5090"}),
        ("PNY NVIDIA RTX PRO 6000 Blackwell Workstation Edition", {"gpu_model": "RTX PRO 6000 BLACKWELL", "vram_gb": 96}),
        ("NVIDIA RTX 6000 Ada Generation 48GB GDDR6", {"gpu_model": "RTX 6000 ADA", "vram_gb": 48}),
        ("PNY NVIDIA RTX A6000 Workstation GPU", {"gpu_model": "RTX A6000", "vram_gb": 48}),
        ("EVGA RTX 3090 Ti FTW3", {"gpu_model": "RTX 3090 TI", "vram_gb": 24}),
        ("Alienware 32\" AW3225QF 4K QD-OLED 240Hz Curved Gaming Monitor", {"size_in": 32, "panel": "QD-OLED", "refresh_hz": 240}),
        ("LG 65\" Class C4 Series OLED evo 4K 144Hz Smart TV (OLED65C4PUA)", {"size_in": 65, "panel": "OLED", "refresh_hz": 144}),
        ("LG OLED77C5PUA 4K Smart TV", {"size_in": 77, "panel": "OLED"}),
        ("TCL 85-inch QM8 Mini-LED 4K Google TV", {"size_in": 85, "panel": "Mini-LED"}),
        ("Samsung QN65S90DAEXZA 4K Smart TV", {"size_in": 65}),
        ("ASUS ROG Swift 31.5” PG32UCDM 4K QD OLED 240Hz", {"size_in": 31.5, "panel": "QD-OLED", "refresh_hz": 240}),
        ("LG 27GX790A 27 inch 1440p OLED 480Hz", {"size_in": 27, "panel": "OLED", "refresh_hz": 480}),
        ("LG UltraGear 32\" Dual Mode 4K 240Hz / 1080p 480Hz OLED", {"size_in": 32, "panel": "OLED", "refresh_hz": 480}),
        ("Valve Steam Deck OLED 512GB", {"panel": "OLED"}),
        ("Anker 735 Charger (Nano II 65W)", {}),
    ],
)
def test_extract_specs(title: str, expected: dict[str, Any]) -> None:
    assert extract_specs(title) == expected


def test_title_tags() -> None:
    assert title_tags("[Prime, SnS] [YMMV] $13.27* | Batteries") == ["Prime", "SnS", "YMMV"]
    assert title_tags("No tags $5") == []


# --------------------------------------------------------------------------- ids, urls, images


def test_thread_ids_and_source_ids() -> None:
    link = "https://slickdeals.net/f/20105466-apple-mac-mini?utm_source=rss&utm_content=fp&utm_medium=RSS2"
    assert thread_id_from_url(link) == "20105466"
    assert thread_id_from_url("https://slickdeals.net/f/20105466") == "20105466"
    assert thread_id_from_url("https://example.com/f/20105466-x") is None
    assert entry_source_id(link, "thread-20105466") == "20105466"
    assert entry_source_id(None, "thread-20105466") == "20105466"
    assert entry_source_id(None, link) == "20105466"  # permalink guid
    assert entry_source_id(None, "custom-guid-42") == "custom-guid-42"
    hashed = entry_source_id("https://example.com/deal?utm_source=rss", None)
    assert hashed is not None and len(hashed) == 20
    assert hashed == entry_source_id("https://example.com/deal", "")  # tracking params do not change the id
    assert entry_source_id(None, None) is None
    assert strip_tracking(link) == "https://slickdeals.net/f/20105466-apple-mac-mini"
    assert strip_tracking("https://slickdeals.net/f/1?page=2&utm_medium=RSS2") == "https://slickdeals.net/f/1?page=2"


def test_extract_outbound_url() -> None:
    direct = (
        '<a href="https://slickdeals.net/click?lno=1&amp;tid=1">Store</a> '
        '<a href="https://www.youtube.com/watch?v=abc">review</a> '
        '<a href="https://www.bestbuy.com/site/lg-65-c4/6578139.p?skuId=6578139">Best Buy</a>'
    )
    assert extract_outbound_url(direct) == "https://www.bestbuy.com/site/lg-65-c4/6578139.p?skuId=6578139"
    embedded = '<a href="https://slickdeals.net/click?lno=1&amp;u2=https%3A%2F%2Fwww.newegg.com%2Fp%2FN82E1">x</a>'
    assert extract_outbound_url(embedded) == "https://www.newegg.com/p/N82E1"
    asin_ca = '<a href="https://slickdeals.net/click?lno=1" data-aps-asin="B0ABCDEF12" data-product-exitWebsite="amazon.ca">A</a>'
    assert extract_outbound_url(asin_ca) == "https://www.amazon.ca/dp/B0ABCDEF12"
    assert extract_outbound_url('<a href="https://slickdeals.net/click?lno=1" data-aps-asin="bogus">A</a>') is None
    assert extract_outbound_url('<a href="https://slickdeals.net/f/123">thread</a>') is None
    assert extract_outbound_url('<a href="https://i.imgur.com/receipt.jpg">pic</a>') is None
    assert extract_outbound_url("") is None


def test_extract_images_from_media_enclosures_and_body() -> None:
    entry = {
        "media_thumbnail": [{"url": "https://static.slickdealscdn.com/attachment/1/300x300/1.thumb"}],
        "media_content": [
            {"url": "https://cdn.example.com/video.mp4", "medium": "video"},
            {"url": "https://cdn.example.com/full.jpg", "medium": "image"},
        ],
        "enclosures": [
            {"href": "https://cdn.example.com/enclosure.png", "type": "image/png"},
            {"href": "https://cdn.example.com/manual.pdf", "type": "application/pdf"},
        ],
    }
    body = (
        '<img src="//static.slickdealscdn.com/attachment/2/300x300/2.thumb">'
        '<img src="https://static.slickdealscdn.com/images/smilies/smile.gif">'
        '<img src="https://cdn.example.com/pixel.png" width="1" height="1">'
        '<img src="https://cdn.example.com/full.jpg">'
    )
    assert extract_images(entry, body) == [
        "https://static.slickdealscdn.com/attachment/1/300x300/1.thumb",
        "https://cdn.example.com/full.jpg",
        "https://cdn.example.com/enclosure.png",
        "https://static.slickdealscdn.com/attachment/2/300x300/2.thumb",
    ]
    many = "".join(f'<img src="https://cdn.example.com/{i}.jpg">' for i in range(10))
    assert len(extract_images({}, many)) == MAX_IMAGES


def test_entry_posted_at_prefers_published_then_updated() -> None:
    published = time.strptime("2026-10-06 04:28:34", "%Y-%m-%d %H:%M:%S")
    assert entry_posted_at({"published_parsed": published}) == datetime(2026, 10, 6, 4, 28, 34, tzinfo=timezone.utc)
    assert entry_posted_at({"updated_parsed": published}) == datetime(2026, 10, 6, 4, 28, 34, tzinfo=timezone.utc)
    assert entry_posted_at({"published_parsed": "garbage"}) is None
    assert entry_posted_at({}) is None


def test_parse_entry_from_plain_dict_with_summary_only() -> None:
    entry = {
        "title": "LG 77&quot; C5 OLED evo 4K 120Hz $2,296.99 at Costco",
        "link": "https://slickdeals.net/f/20109999-lg-77-c5?utm_source=rss&utm_content=&utm_medium=RSS2",
        "id": "thread-20109999",
        "summary": "*Costco* has the *LG 77\" C5* for *$2,296.99*. Shipping is free. [LIST][*]Members only...",
        "tags": [{"term": "Popular Deals"}],
    }
    raw = parse_entry(entry, "popular")
    assert raw is not None
    assert raw.title == 'LG 77" C5 OLED evo 4K 120Hz $2,296.99 at Costco'
    assert raw.price == 2296.99 and raw.retailer == "Costco" and raw.shipping == 0.0
    assert raw.extra["popular"] is True and raw.extra["size_in"] == 77
    assert "[LIST]" not in raw.description
    assert raw.image_urls == [] and raw.posted_at is None


@pytest.mark.parametrize(
    ("entry", "reason"),
    [
        ({"title": "", "link": "https://slickdeals.net/f/1234567-x"}, "no_title"),
        ({"title": "Thing $5"}, "no_id"),
        ({"title": "Thing $5", "id": "custom guid with spaces but no link"}, "no_url"),
        ({"title": "Extra 20% off sitewide", "link": "https://slickdeals.net/f/1234567-x"}, "no_price"),
    ],
)
def test_build_listing_skip_reasons(entry: dict[str, Any], reason: str) -> None:
    with pytest.raises(EntrySkipped) as info:
        build_listing(entry, "frontpage")
    assert info.value.reason == reason
    assert parse_entry(entry, "frontpage") is None


def test_html_to_text_strips_tags_and_bbcode() -> None:
    fragment = "<div>Thumb Score: +3 </div><div><b>Price</b>: <i>$5</i><br/>[LIST][*]one[*]two[/LIST]<script>x()</script></div>"
    assert html_to_text(fragment) == "Thumb Score: +3\nPrice: $5\none two"


# --------------------------------------------------------------------------- block detection


def test_block_detection_helpers() -> None:
    challenge = fixture_bytes("cloudflare_challenge.html")
    feed = fixture_bytes("frontpage.xml")
    assert looks_like_html(challenge) and not looks_like_feed(challenge)
    assert looks_like_feed(feed) and not looks_like_html(feed, "text/html")  # sniffing beats a wrong header
    assert looks_like_html(b"Access denied", "text/html; charset=UTF-8")
    assert not looks_like_html(b"", "text/xml")
    assert is_block_response({"cf-mitigated": "challenge"}, feed)
    assert is_block_response({"Content-Type": "text/html"}, challenge)
    assert not is_block_response({"Content-Type": "text/xml"}, feed)
    parsed = parse_feed(challenge, "text/html")
    assert parsed.bozo and not parsed.entries


def test_parse_feed_never_treats_body_as_a_file_name(tmp_path: Path) -> None:
    secret = tmp_path / "secret.xml"
    secret.write_bytes(fixture_bytes("frontpage.xml"))
    parsed = parse_feed(str(secret).encode("utf-8"))
    assert not parsed.entries  # the path is parsed as (invalid) XML, not opened


def test_parse_feed_listings_flags_malformed_bodies() -> None:
    feed = FeedTarget(name="broken", url="https://slickdeals.net/rss")
    assert parse_feed_listings(b"<rss><channel><item><title>oops", feed).malformed
    empty = parse_feed_listings(b'<?xml version="1.0"?><rss version="2.0"><channel><title>t</title></channel></rss>', feed)
    assert not empty.malformed and empty.listings == []


# --------------------------------------------------------------------------- search feeds


def test_build_search_feed_url() -> None:
    template = SlickdealsSource().search_feed_template
    url = build_search_feed_url(template, "  LG   C4 OLED ")
    assert url == "https://slickdeals.net/newsearch.php?mode=frontpage&searcharea=deals&searchin=first&rss=1&q=LG+C4+OLED"
    assert build_search_feed_url("https://x.test/s?q={query}&rss=1", 'B&H 4K "OLED"') == "https://x.test/s?q=B%26H+4K+%22OLED%22&rss=1"
    with pytest.raises(ValueError):
        build_search_feed_url("https://x.test/s?rss=1", "rtx 5090")


def test_search_feed_targets_from_shipped_profiles(app_config: AppConfig) -> None:
    profiles = [p for p in app_config.profiles if p.enabled and p.search.terms]
    targets = search_feed_targets(profiles, "https://x.test/s?q={query}")
    terms = [" ".join(t.split()) for p in profiles for t in p.search.terms]
    assert len(targets) == len({t.casefold() for t in terms})
    first = targets[0]
    assert first.is_search and first.name == f"search:{first.query}"
    assert first.profile_hint == profiles[0].id
    assert first.url.startswith("https://x.test/s?q=") and " " not in first.url
    duplicated = search_feed_targets([profiles[0], profiles[0]], "https://x.test/s?q={query}")
    assert len(duplicated) == len(profiles[0].search.terms)


# --------------------------------------------------------------------------- fake upstream


@dataclass
class Reply:
    status: int = 200
    body: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)


Responder = Reply | Callable[[web.Request], Reply] | Callable[[web.Request], Awaitable[Reply]]


@dataclass
class FakeSlickdeals:
    """Impersonates ``slickdeals.net/newsearch.php`` RSS endpoints."""

    base: str = ""
    responses: dict[str, Responder] = field(default_factory=dict)
    requests: list[tuple[str, dict[str, str]]] = field(default_factory=list)

    @staticmethod
    def feed_key(request: web.Request) -> str:
        query = request.query
        if "q" in query:
            return f"search:{query['q']}"
        if query.get("mode") == "frontpage":
            return "frontpage"
        if query.get("forumchoice[]") == "9":
            return "forum"
        return "unknown"

    async def handle(self, request: web.Request) -> web.Response:
        key = self.feed_key(request)
        self.requests.append((key, dict(request.headers)))
        responder = self.responses.get(key)
        if responder is None:
            return web.Response(status=404, text="no such feed")
        reply = responder(request) if callable(responder) else responder
        if inspect.isawaitable(reply):
            reply = await reply
        return web.Response(status=reply.status, body=reply.body, headers=reply.headers)

    def keys(self) -> list[str]:
        return [key for key, _ in self.requests]

    def frontpage_url(self) -> str:
        return f"{self.base}/newsearch.php?mode=frontpage&searcharea=deals&searchin=first&rss=1"

    def forum_url(self) -> str:
        return f"{self.base}/newsearch.php?searchin=first&forumchoice%5B%5D=9&rss=1"

    def search_template(self) -> str:
        return f"{self.base}/newsearch.php?q={{query}}&searcharea=deals&searchin=first&rss=1"


def rss(name: str, **headers: str) -> Reply:
    return Reply(200, fixture_bytes(name), {**RSS_HEADERS, **headers})


def challenge(status: int = 200, **headers: str) -> Reply:
    base = dict(CHALLENGE_HEADERS) if status != 200 else {"Content-Type": "text/html; charset=UTF-8"}
    return Reply(status, fixture_bytes("cloudflare_challenge.html"), {**base, **headers})


@pytest.fixture
async def upstream() -> AsyncIterator[FakeSlickdeals]:
    fake = FakeSlickdeals()
    app = web.Application()
    app.router.add_get("/newsearch.php", fake.handle)
    server = TestServer(app)
    await server.start_server()
    fake.base = str(server.make_url("")).rstrip("/")
    try:
        yield fake
    finally:
        await server.close()


@pytest.fixture
async def http() -> AsyncIterator[HttpClient]:
    client = HttpClient.create(NetworkSettings(trust_env=False, retry=BackoffPolicy(max_attempts=2, base_delay=0, max_delay=0)))
    try:
        yield client
    finally:
        await client.close()


def make_ingestor(
    upstream: FakeSlickdeals,
    http: HttpClient,
    app_config: AppConfig,
    *,
    feeds: list[str] | None = None,
    search: bool = False,
    profiles: list[str] | None = None,
    template: str | None = None,
    metrics: Metrics | None = None,
) -> SlickdealsIngestor:
    urls = {"frontpage": upstream.frontpage_url(), "hot_deals_forum": upstream.forum_url()}
    cfg = SlickdealsSource(
        enabled=True,
        feeds=[FeedSpec(name=name, url=urls[name]) for name in (feeds if feeds is not None else ["frontpage", "hot_deals_forum"])],
        search_feeds_from_profiles=search,
        search_feed_template=template or upstream.search_template(),
        profiles=profiles or [],
        max_item_age_minutes=None,
        cooldown_seconds=300,
    )
    ctx = IngestorContext(http=http, metrics=metrics or Metrics(), config=app_config, node_id="test", rng=random.Random(7))
    return SlickdealsIngestor(cfg, ctx)


# --------------------------------------------------------------------------- ingestor


async def test_poll_fetches_all_feeds_and_merges(upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig) -> None:
    upstream.responses.update(
        {
            "frontpage": rss("frontpage.xml"),
            "forum": rss("hot_deals_forum.xml"),
            "search:rtx 5090": rss("search_rtx_5090.xml"),
        }
    )
    metrics = Metrics()
    ingestor = make_ingestor(upstream, http, app_config, search=True, profiles=["rtx_5090"], metrics=metrics)
    assert [f.name for f in ingestor.search_feeds] == ["search:rtx 5090"]

    listings = await ingestor.poll()

    assert sorted(upstream.keys()) == ["forum", "frontpage", "search:rtx 5090"]
    for _, headers in upstream.requests:
        assert headers["Accept"] == FEED_ACCEPT
        assert headers["User-Agent"].startswith("Mozilla/5.0")  # browser identity
    items = by_id(listings)
    assert len(listings) == len(items) == 9  # 3 + 5 + 3 minus two cross-feed duplicates
    assert all(raw.source == "slickdeals" and raw.source_kind is SourceKind.AGGREGATOR for raw in listings)
    gpu = items["20105512"]
    assert gpu.extra["feeds"] == ["hot_deals_forum", "search:rtx 5090"]
    assert gpu.query == "rtx 5090" and gpu.profile_hint == "rtx_5090"
    omen = items["20101234"]
    assert omen.query == "rtx 5090" and omen.extra["feeds"] == ["search:rtx 5090"]
    assert items["20104987"].extra["feeds"] == ["frontpage", "hot_deals_forum"]
    assert items["20105466"].query is None

    fetches = metrics.counter("slickdeals_feed_fetch_total", labelnames=("feed", "outcome"))
    assert fetches.value(feed="frontpage", outcome="ok") == 1
    entries = metrics.counter("slickdeals_entries_total", labelnames=("feed", "outcome"))
    assert entries.value(feed="hot_deals_forum", outcome="parsed") == 5
    assert entries.value(feed="hot_deals_forum", outcome="skip_no_price") == 1


async def test_conditional_get_304_means_no_change(upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig) -> None:
    etag = 'W/"fp-20261006T0432"'
    seen_validators: list[str | None] = []

    def frontpage(request: web.Request) -> Reply:
        seen_validators.append(request.headers.get("If-None-Match"))
        if request.headers.get("If-None-Match") == etag:
            return Reply(304, b"", {"ETag": etag})
        return Reply(200, fixture_bytes("frontpage.xml"), {**RSS_HEADERS, "ETag": etag, "Last-Modified": "Tue, 06 Oct 2026 04:32:10 GMT"})

    upstream.responses.update({"frontpage": frontpage, "forum": rss("hot_deals_forum.xml")})
    metrics = Metrics()
    ingestor = make_ingestor(upstream, http, app_config, metrics=metrics)

    first = await ingestor.poll()
    assert {"20105466", "20103311"} <= {raw.source_id for raw in first}
    second = await ingestor.poll()

    assert seen_validators == [None, etag]
    assert {raw.source_id for raw in second} == {"20105590", "20105512", "20105477", "20104987", "20105488"}
    assert by_id(second)["20104987"].extra["feeds"] == ["hot_deals_forum"]  # frontpage contributed nothing
    fetches = metrics.counter("slickdeals_feed_fetch_total", labelnames=("feed", "outcome"))
    assert fetches.value(feed="frontpage", outcome="not_modified") == 1

    # A 304 on every feed is a successful "nothing changed" poll, not a failure.
    only_frontpage = make_ingestor(upstream, http, app_config, feeds=["frontpage"])
    assert await only_frontpage.poll() == []


async def test_html_challenge_on_every_feed_raises_source_blocked(
    upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig
) -> None:
    upstream.responses.update({"frontpage": challenge(200), "forum": challenge(200)})
    ingestor = make_ingestor(upstream, http, app_config)
    with pytest.raises(SourceBlocked) as info:
        await ingestor.poll()
    assert info.value.cooldown_seconds == 300
    assert "2 blocked" in str(info.value)


async def test_block_cooldown_escalates_honours_retry_after_and_resets(
    upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig
) -> None:
    ingestor = make_ingestor(upstream, http, app_config, feeds=["frontpage"])
    sequence = [
        (challenge(403), 300.0),
        (challenge(503), 600.0),
        (challenge(429, **{"Retry-After": "2000"}), 2000.0),
        (challenge(403), 2400.0),
        (challenge(403), BLOCK_COOLDOWN_MAX_SECONDS),
        (challenge(403), BLOCK_COOLDOWN_MAX_SECONDS),
    ]
    for reply, expected in sequence:
        upstream.responses["frontpage"] = reply
        with pytest.raises(SourceBlocked) as info:
            await ingestor.poll()
        assert info.value.cooldown_seconds == expected

    upstream.responses["frontpage"] = rss("frontpage.xml")
    assert len(await ingestor.poll()) == 3
    upstream.responses["frontpage"] = challenge(403)
    with pytest.raises(SourceBlocked) as info:
        await ingestor.poll()
    assert info.value.cooldown_seconds == 300  # reset by the successful poll
    assert upstream.keys().count("frontpage") == len(sequence) + 2  # block statuses are not retried


async def test_single_blocked_feed_is_skipped(upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig) -> None:
    upstream.responses.update({"frontpage": challenge(403), "forum": rss("hot_deals_forum.xml")})
    metrics = Metrics()
    ingestor = make_ingestor(upstream, http, app_config, metrics=metrics)
    listings = await ingestor.poll()
    assert len(listings) == 5
    fetches = metrics.counter("slickdeals_feed_fetch_total", labelnames=("feed", "outcome"))
    assert fetches.value(feed="frontpage", outcome="blocked") == 1
    assert fetches.value(feed="hot_deals_forum", outcome="ok") == 1


async def test_all_feeds_failing_without_a_block_raises_source_error(
    upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig
) -> None:
    upstream.responses.update({"frontpage": Reply(200, b"<rss><channel><item><title>truncated", dict(RSS_HEADERS))})
    # "forum" has no responder -> HTTP 404.
    ingestor = make_ingestor(upstream, http, app_config)
    with pytest.raises(SourceError) as info:
        await ingestor.poll()
    assert not isinstance(info.value, SourceBlocked)
    assert "malformed feed" in str(info.value) and "HTTP 404" in str(info.value)


async def test_validators_are_forgotten_after_a_block_page(
    upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig
) -> None:
    replies = iter(
        [
            Reply(200, fixture_bytes("frontpage.xml"), {**RSS_HEADERS, "ETag": '"v1"'}),
            Reply(200, fixture_bytes("cloudflare_challenge.html"), {"Content-Type": "text/html", "ETag": '"challenge"'}),
            Reply(200, fixture_bytes("frontpage.xml"), {**RSS_HEADERS, "ETag": '"v2"'}),
        ]
    )
    seen: list[str | None] = []

    def frontpage(request: web.Request) -> Reply:
        seen.append(request.headers.get("If-None-Match"))
        return next(replies)

    upstream.responses["frontpage"] = frontpage
    ingestor = make_ingestor(upstream, http, app_config, feeds=["frontpage"])
    assert len(await ingestor.poll()) == 3
    with pytest.raises(SourceBlocked):
        await ingestor.poll()
    assert len(await ingestor.poll()) == 3
    assert seen == [None, '"v1"', None]


async def test_run_once_emits_only_new_or_changed(upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig) -> None:
    upstream.responses.update({"frontpage": rss("frontpage.xml"), "forum": rss("hot_deals_forum.xml")})
    ingestor = make_ingestor(upstream, http, app_config)
    first = await ingestor.run_once()
    assert len(first) == 7 and all(raw.node_id == "test" for raw in first)  # 3 + 5 - 1 shared thread
    assert await ingestor.run_once() == []
    # The same thread reported only by the forum feed (different title) is not "changed".
    upstream.responses["frontpage"] = Reply(200, b'<?xml version="1.0"?><rss version="2.0"><channel><title>t</title></channel></rss>', dict(RSS_HEADERS))
    assert await ingestor.run_once() == []


def test_signature_ignores_title(app_config: AppConfig) -> None:
    fp = by_id(fixture_listings("frontpage.xml"))["20104987"]
    forum = by_id(fixture_listings("hot_deals_forum.xml"))["20104987"]
    assert fp.title != forum.title
    ctx = IngestorContext(http=None, metrics=Metrics(), config=app_config, node_id="test")  # type: ignore[arg-type]
    ingestor = SlickdealsIngestor(SlickdealsSource(enabled=True, search_feeds_from_profiles=False), ctx)
    assert ingestor.signature(fp) == ingestor.signature(forum)


def test_search_feed_rotation(app_config: AppConfig) -> None:
    ctx = IngestorContext(http=None, metrics=Metrics(), config=app_config, node_id="test")  # type: ignore[arg-type]
    cfg = SlickdealsSource(
        enabled=True,
        feeds=[FeedSpec(name="hot_deals_forum", url="https://slickdeals.net/newsearch.php?searchin=first&forumchoice%5B%5D=9&rss=1")],
        search_feeds_from_profiles=True,
    )
    ingestor = SlickdealsIngestor(cfg, ctx)
    expected_terms = {" ".join(t.split()).casefold() for p in ingestor.search_profiles() for t in p.search.terms}
    total = len(ingestor.search_feeds)
    assert total == len(expected_terms) > SEARCH_FEEDS_PER_POLL

    polls = -(-total // SEARCH_FEEDS_PER_POLL)
    seen: list[str] = []
    for _ in range(polls):
        feeds = ingestor.feeds_for_poll()
        assert feeds[0].name == "hot_deals_forum"
        assert len(feeds) == 1 + SEARCH_FEEDS_PER_POLL
        seen.extend(f.name for f in feeds[1:])
    assert set(seen) == {f.name for f in ingestor.search_feeds}  # every term covered in one rotation
    assert seen[:total] == [f.name for f in ingestor.search_feeds]


def test_search_feeds_disabled_or_misconfigured(app_config: AppConfig) -> None:
    ctx = IngestorContext(http=None, metrics=Metrics(), config=app_config, node_id="test")  # type: ignore[arg-type]
    disabled = SlickdealsIngestor(SlickdealsSource(enabled=True, search_feeds_from_profiles=False), ctx)
    assert disabled.search_feeds == [] and disabled.feeds_for_poll() == []
    broken = SlickdealsIngestor(
        SlickdealsSource(enabled=True, search_feeds_from_profiles=True, search_feed_template="https://slickdeals.net/rss"), ctx
    )
    assert broken.search_feeds == []


async def test_poll_without_feeds_returns_nothing(app_config: AppConfig) -> None:
    ctx = IngestorContext(http=None, metrics=Metrics(), config=app_config, node_id="test")  # type: ignore[arg-type]
    ingestor = SlickdealsIngestor(SlickdealsSource(enabled=True, search_feeds_from_profiles=False), ctx)
    assert await ingestor.poll() == []
    await ingestor.setup()


# --------------------------------------------------------------------------- review regressions
#
# ``recorded_live_items.xml`` holds six <item>s copied verbatim from feeds recorded live on
# 2026-10-06 (an 'oled' Hot Deals search, the frontpage feed and a quoted search), chosen
# because each one exposed a defect: " - $X" title prices, a third-party seller written as
# "<seller> via Amazon", multi-store posts whose ASIN belongs to a different store than the
# primary one, and Amazon's "Shipping is free with Prime or on $35+ orders" phrase.


def test_recorded_live_items_extract_dash_prices_and_consistent_stores() -> None:
    feed = FeedTarget(name="search:oled", url="https://slickdeals.net/rss", query="oled", profile_hint="lg_oled_tv")
    parsed = parse_feed_listings(fixture_bytes("recorded_live_items.xml"), feed, "text/xml; charset=UTF-8")
    assert parsed.skipped == {} and parsed.errors == 0 and not parsed.malformed
    items = by_id(parsed.listings)
    assert set(items) == {"18554311", "20099271", "20105049", "20105418", "20101683", "20103093"}

    # "... Gaming Monitor - $679.00": the body's "for $749" is the price the poster *cancelled*.
    assert items["18554311"].price == 679.0
    lg_c6 = items["20099271"]  # "LG 65\" Class C6 ... - $1499.99 @ Best Buy & Amazon"
    assert lg_c6.price == 1499.99 and lg_c6.retailer == "Best Buy"
    assert lg_c6.outbound_url is None  # the ASIN belongs to the secondary (Amazon) link
    s90h = items["20105049"]  # "Samsung 65\" Class S90H ... - $1499.99 @ Best Buy"
    assert s90h.price == 1499.99 and s90h.retailer == "Best Buy" and s90h.extra["size_in"] == 65
    monitor = items["20105418"]  # "LG 45GX900A-B ... OLED Curved Gaming Monitor - $899.99"
    assert monitor.price == 899.99 and monitor.retailer == "Amazon"
    assert monitor.outbound_url == "https://www.amazon.com/dp/B0FDC38XGQ"
    multi = items["20101683"]  # B&H first, then Samsung, then Amazon (with ASIN)
    assert multi.retailer == "B&H Photo" and multi.outbound_url is None
    insoles = items["20103093"]  # "WBHzhixin via Amazon has ..." + "Shipping is free with Prime or on $35+"
    assert insoles.retailer == "Amazon"
    assert insoles.price == 8.5 and insoles.shipping is None


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ('MSI 49" Curved OLED Display, 144Hz 0.03ms, Gaming Monitor - $679.00', 679.0),
        ("ASUS ROG Swift 32 4K OLED Gaming Monitor (PG32UCDP) - $999.99", 999.99),
        ('LG 65" Class C6 Series OLED evo AI 4K Smart webOS TV (2026) - $1499.99 @ Best Buy & Amazon', 1499.99),
        ("Zotac RTX 5080 Solid OC - Open Box w/2-year warranty - $1124.99 + FS", 1124.99),
        ('LG Partner Store: 77" LG C6H OLED 4K TV + $300 Rinse Credit - $1619.99', 1619.99),
        ('YMMV: Dell 32 Plus 32" 4K 120Hz QD-OLED S3225QC -$559.99 @ Amazon', 559.99),
        ("[S&S] SPAM 25% Less Sodium Canned Meat, 12 Pack, 12-Oz -$27.07 @Amazon", 27.07),
        ("CyberPowerPC Gaming PC, RTX 5090 32GB, 32GB DDR5, 2TB SSD, SLC8400WST - $4299", 4299.0),
        # A minus right after another amount is still a discount, not the price.
        ("MSI RTX 4090 Suprim $1,099.99 -$100 w/ code", 1099.99),
        ("Insoles $26.98 - $18.49 off", 26.98),
    ],
)
def test_extract_price_dash_separated_titles(title: str, expected: float) -> None:
    assert extract_price(title) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # data-store-slug values observed live (hyphenated multi-word slugs).
        ("bh-photo-video", "B&H Photo"),
        ("micro-center", "Micro Center"),
        ("the-home-depot", "Home Depot"),
        ("costco-wholesale", "Costco"),
        ("hp-small-medium-business", "HP"),
        ("dicks-sporting-goods", "Dick's Sporting Goods"),
        ("origin-pc", "Origin PC"),
        ("ace-hardware", "Ace Hardware"),
        # Third-party sellers on a marketplace: the marketplace is the store.
        ("WBHzhixin via Amazon", "Amazon"),
        ("Gamechest via Walmart", "Walmart"),
        ("adidas via eBay", "eBay"),
    ],
)
def test_normalize_store_live_slugs_and_via_sellers(raw: str, expected: str) -> None:
    assert normalize_store(raw) == expected


@pytest.mark.parametrize(
    ("title", "body_text", "price", "expected"),
    [
        ("Insoles $8.50", "Shipping is free with Prime or on $35+ orders.", 8.5, None),
        ("Blender $59.84", "Shipping is free with Prime or on $35+ orders.", 59.84, 0.0),
        ("28L DSG Sport Backpack (White) $8.97 + Free Shipping on $49+", "", 8.97, None),
        ("Rugged Shark Clog Sandals $2.65 + Free Shipping w/ Walmart+ or on $35+", "", 2.65, None),
        ("Fiskars Scissors $10.36 + Free Shipping w/ Prime or on $35+", "", 10.36, None),
        ("LG C5 OLED $1,499.99 + Free Shipping w/ Prime or on $35+", "", 1499.99, 0.0),
        ("Atkins Protein Bars $3.90 + Free S&H w/ Prime", "", 3.9, 0.0),
    ],
)
def test_extract_shipping_threshold_phrases(title: str, body_text: str, price: float, expected: float | None) -> None:
    assert extract_shipping(title, body_text, price) == expected


def test_extract_outbound_url_asin_only_for_amazon_retailer() -> None:
    body = (
        '<a href="https://slickdeals.net/click?lno=1" data-store-slug="best-buy" data-product-exitWebsite="bestbuy.com">Best Buy</a> '
        '<a href="https://slickdeals.net/click?lno=2" data-store-slug="amazon" data-aps-asin="B0GRK5D3RW">Amazon</a>'
    )
    assert extract_outbound_url(body) == "https://www.amazon.com/dp/B0GRK5D3RW"  # no retailer known: keep
    assert extract_outbound_url(body, retailer="Amazon") == "https://www.amazon.com/dp/B0GRK5D3RW"
    assert extract_outbound_url(body, retailer="Best Buy") is None


def test_regexes_stay_linear_on_pathological_bodies() -> None:
    # Unbounded ".*?" spans used to make these quadratic: ~15 s for 300 KB of anchors.
    anchors = '<a href="x">Store</a> <span class="e">' * 8000
    started = time.perf_counter()
    assert extract_retailer("Thing $5", anchors) is None
    assert html_to_text("<script>" * 20000 + "tail") == ""
    assert extract_list_price("Thing $5", "", "<s>" * 20000, 5.0) is None
    assert time.perf_counter() - started < 2.0


def _fixed_clock(start: float = 1000.0) -> tuple[list[float], Callable[[], float]]:
    now = [start]
    return now, lambda: now[0]


def test_feed_interval_seconds_is_honoured(app_config: AppConfig) -> None:
    ctx = IngestorContext(http=None, metrics=Metrics(), config=app_config, node_id="test")  # type: ignore[arg-type]
    cfg = SlickdealsSource(
        enabled=True,
        poll_interval_seconds=30,
        search_feeds_from_profiles=False,
        feeds=[
            FeedSpec(name="hot_deals_forum", url="https://slickdeals.net/newsearch.php?searchin=first&forumchoice%5B%5D=9&rss=1"),
            FeedSpec(name="frontpage", url="https://slickdeals.net/newsearch.php?mode=frontpage&rss=1", interval_seconds=180),
            FeedSpec(name="popular", url="https://slickdeals.net/newsearch.php?mode=popdeals&rss=1", interval_seconds=300),
        ],
    )
    ingestor = SlickdealsIngestor(cfg, ctx)
    now, ingestor.clock = _fixed_clock()

    def names() -> list[str]:
        return [f.name for f in ingestor.feeds_for_poll()]

    assert names() == ["hot_deals_forum", "frontpage", "popular"]  # everything is due at start
    schedule: list[list[str]] = []
    for _ in range(12):  # 12 polls, 30 s apart
        now[0] += 30
        schedule.append(names())
    assert all(polled[0] == "hot_deals_forum" for polled in schedule)
    assert sum("frontpage" in polled for polled in schedule) == 2  # t=180, 360
    assert sum("popular" in polled for polled in schedule) == 1  # t=300
    assert "frontpage" in schedule[5] and "popular" in schedule[9]


async def test_poll_skips_feeds_that_are_not_due(upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig) -> None:
    upstream.responses.update({"frontpage": rss("frontpage.xml"), "forum": rss("hot_deals_forum.xml")})
    ingestor = make_ingestor(upstream, http, app_config, feeds=[])
    ingestor.static_feeds = [
        FeedTarget(name="hot_deals_forum", url=upstream.forum_url()),
        FeedTarget(name="frontpage", url=upstream.frontpage_url(), interval_seconds=180),
    ]
    now, ingestor.clock = _fixed_clock()
    assert len(await ingestor.poll()) == 7
    now[0] += 30
    second = await ingestor.poll()
    assert {raw.source_id for raw in second} == {"20105590", "20105512", "20105477", "20104987", "20105488"}
    assert sorted(upstream.keys()) == ["forum", "forum", "frontpage"]  # frontpage not re-fetched 30 s later


async def test_poll_with_nothing_due_is_a_quiet_success(app_config: AppConfig) -> None:
    ctx = IngestorContext(http=None, metrics=Metrics(), config=app_config, node_id="test")  # type: ignore[arg-type]
    cfg = SlickdealsSource(
        enabled=True,
        search_feeds_from_profiles=False,
        feeds=[FeedSpec(name="frontpage", url="https://slickdeals.net/newsearch.php?mode=frontpage&rss=1", interval_seconds=180)],
    )
    ingestor = SlickdealsIngestor(cfg, ctx)
    now, ingestor.clock = _fixed_clock()
    ingestor.feeds_for_poll()  # consumes the frontpage slot
    now[0] += 30
    assert await ingestor.poll() == []
    assert ingestor._warned_no_feeds is False  # not the "no feeds configured" warning


async def test_block_burns_the_browser_identity(upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig) -> None:
    upstream.responses.update({"frontpage": challenge(403), "forum": challenge(200)})
    burned: list[str] = []
    http.identities.burn = burned.append  # type: ignore[method-assign]
    ingestor = make_ingestor(upstream, http, app_config)
    with pytest.raises(SourceBlocked):
        await ingestor.poll()
    host = upstream.base.split("://", 1)[1].split(":", 1)[0]
    assert burned == [host, host]


async def test_poll_cancellation_propagates(upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig) -> None:
    arrived = asyncio.Event()
    release = asyncio.Event()

    async def slow(request: web.Request) -> Reply:
        arrived.set()
        await release.wait()
        return rss("frontpage.xml")

    upstream.responses["frontpage"] = slow
    ingestor = make_ingestor(upstream, http, app_config, feeds=["frontpage"])
    task = asyncio.create_task(ingestor.poll())
    await asyncio.wait_for(arrived.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()


# --------------------------------------------------------------------------- cadence / consistency follow-ups


def _cadence_ingestor(app_config: AppConfig, feeds: list[FeedSpec], **cfg: Any) -> SlickdealsIngestor:
    ctx = IngestorContext(http=None, metrics=Metrics(), config=app_config, node_id="test", rng=random.Random(3))  # type: ignore[arg-type]
    return SlickdealsIngestor(SlickdealsSource(enabled=True, search_feeds_from_profiles=False, feeds=feeds, **cfg), ctx)


def test_next_interval_never_sleeps_past_the_next_due_feed(app_config: AppConfig) -> None:
    forum = FeedSpec(name="hot_deals_forum", url="https://slickdeals.net/newsearch.php?searchin=first&forumchoice%5B%5D=9&rss=1")
    frontpage = FeedSpec(name="frontpage", url="https://slickdeals.net/newsearch.php?mode=frontpage&rss=1", interval_seconds=180)

    plain = _cadence_ingestor(app_config, [forum], poll_interval_seconds=30, jitter_pct=0.15)
    assert plain.seconds_until_next_due() is None
    assert all(25.5 <= plain.next_interval() <= 34.5 for _ in range(50))  # jittered base only

    ingestor = _cadence_ingestor(app_config, [forum, frontpage], poll_interval_seconds=30, jitter_pct=0.15)
    now, ingestor.clock = _fixed_clock()
    assert ingestor.seconds_until_next_due() == 0.0  # never fetched: due now
    ingestor.feeds_for_poll()
    now[0] += 172.0  # a jittered poll lands 8 s before the frontpage is due
    assert ingestor.seconds_until_next_due() == pytest.approx(8.0)
    assert ingestor.next_interval() == pytest.approx(8.0)
    now[0] += 8.0
    assert [f.name for f in ingestor.feeds_for_poll()] == ["hot_deals_forum", "frontpage"]
    now[0] += 179.5  # within FEED_DUE_TOLERANCE_SECONDS of the due time: not pushed a whole cycle back
    assert [f.name for f in ingestor.feeds_for_poll()] == ["hot_deals_forum", "frontpage"]


async def test_cancelled_poll_gives_the_feed_slot_back(upstream: FakeSlickdeals, http: HttpClient, app_config: AppConfig) -> None:
    arrived = asyncio.Event()
    release = asyncio.Event()

    async def slow(request: web.Request) -> Reply:
        arrived.set()
        await release.wait()
        return rss("frontpage.xml")

    upstream.responses["frontpage"] = slow
    ingestor = make_ingestor(upstream, http, app_config, feeds=[])
    ingestor.static_feeds = [FeedTarget(name="frontpage", url=upstream.frontpage_url(), interval_seconds=180)]
    now, ingestor.clock = _fixed_clock()
    task = asyncio.create_task(ingestor.poll())
    await asyncio.wait_for(arrived.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    now[0] += 5
    assert [f.name for f in ingestor.feeds_for_poll()] == ["frontpage"]  # still due: nothing was delivered


def test_merge_keeps_retailer_and_outbound_url_consistent() -> None:
    def raw(feed: str, retailer: str | None, outbound: str | None) -> RawListing:
        return RawListing(
            source="slickdeals",
            source_kind=SourceKind.AGGREGATOR,
            source_id="20099271",
            url="https://slickdeals.net/f/20099271",
            title="LG C6 $1499.99",
            price=1499.99,
            retailer=retailer,
            outbound_url=outbound,
            extra={"feeds": [feed]},
        )

    amazon_dp = "https://www.amazon.com/dp/B0GRK5D3RW"
    # Primary knows no retailer but carries an Amazon link; the other feed says Best Buy.
    merged = merge_duplicates([raw("frontpage", None, amazon_dp), raw("hot_deals_forum", "Best Buy", None)])[0]
    assert merged.retailer == "Best Buy" and merged.outbound_url is None
    # A link from a feed that resolved a different store is not borrowed.
    merged = merge_duplicates([raw("frontpage", "Best Buy", None), raw("hot_deals_forum", "Amazon", amazon_dp)])[0]
    assert merged.retailer == "Best Buy" and merged.outbound_url is None
    merged = merge_duplicates([raw("frontpage", "Amazon", None), raw("hot_deals_forum", None, amazon_dp)])[0]
    assert merged.retailer == "Amazon" and merged.outbound_url == amazon_dp


def test_outbound_url_skips_links_of_another_known_store() -> None:
    body = (
        '<a href="https://www.amazon.com/dp/B0GRK5D3RW">Amazon</a> '
        '<a href="https://www.bestbuy.com/site/lg-c6/6673112.p">Best Buy</a> '
        '<a href="https://shop.example.com/lg-c6">Shop</a>'
    )
    assert extract_outbound_url(body) == "https://www.amazon.com/dp/B0GRK5D3RW"
    assert extract_outbound_url(body, retailer="Best Buy") == "https://www.bestbuy.com/site/lg-c6/6673112.p"
    assert extract_outbound_url(body, retailer="Bob's Hardware") == "https://shop.example.com/lg-c6"  # unknown host: kept


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("bh-cosmetics", "Bh Cosmetics"),  # "bh" is an abbreviation alias, not a store name prefix
        ("best-buy-outlet", "Best Buy"),
        ("Dell Refurbished", "Dell"),
        ("Seller Name That Is Far Too Long To Be A Store via Amazon", "Amazon"),
    ],
)
def test_normalize_store_prefix_guards(raw: str, expected: str) -> None:
    assert normalize_store(raw) == expected


@pytest.mark.parametrize(
    "body",
    ["<" * 100_000, "<a " * 40_000, "<a " + 'a="x ' * 20_000 + ">", "[url=" * 30_000, "<" + " " * 100_000 + "x", "<style " * 20_000],
)
def test_html_helpers_stay_linear_on_more_pathological_bodies(body: str) -> None:
    started = time.perf_counter()
    html_to_text(body)
    extract_outbound_url(body)
    extract_images({}, body)
    extract_retailer("Thing $5", body)
    assert time.perf_counter() - started < 2.0


def test_html_to_text_keeps_text_around_closed_scripts() -> None:
    assert html_to_text("a<script>var x = '<b>';</script>b<STYLE>p{}</style >c") == "a b c"
