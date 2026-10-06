"""Tests for deal_radar.engine.normalizer.

Table-driven coverage of every parser branch (money, title prices, condition,
URLs, text cleaning) plus Normalizer.normalize happy and error paths. Everything is
offline and deterministic; the shipped config.yaml is loaded with env={}.
"""

from __future__ import annotations

import hashlib
import math
import statistics
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from deal_radar.config_schema import AppConfig, load_config
from deal_radar.engine import normalizer as nm
from deal_radar.engine import types as engine_types
from deal_radar.engine.normalizer import (
    DealItem,
    NormalizationError,
    Normalizer,
    RawListing,
    canonical_url,
    clean_text,
    extract_title_price,
    parse_condition,
    parse_money,
    parse_price,
)
from deal_radar.engine.types import Condition, Location, SellerInfo, SourceKind

CONFIG_PATH = Path(__file__).parents[1] / "config.yaml"

NEW = Condition.NEW
OPEN_BOX = Condition.OPEN_BOX
REFURB = Condition.REFURBISHED
USED = Condition.USED
PARTS = Condition.FOR_PARTS
RETAIL = SourceKind.RETAIL
AGG = SourceKind.AGGREGATOR
LOCAL = SourceKind.LOCAL
MARKET = SourceKind.MARKETPLACE


@pytest.fixture(scope="module")
def config() -> AppConfig:
    return load_config(CONFIG_PATH, env={})


@pytest.fixture(scope="module")
def normalizer(config: AppConfig) -> Normalizer:
    return Normalizer(config)


def make_raw(**overrides: object) -> RawListing:
    """An eBay-like listing; override any field."""
    data: dict[str, object] = {
        "source": "ebay",
        "source_kind": SourceKind.MARKETPLACE,
        "source_id": "123456789012",
        "url": "https://www.ebay.com/itm/123456789012?_trksid=p2047675.c100005&hash=item1cbb3c4d:g:abc&var=55",
        "title": "NVIDIA GeForce RTX 4090 Founders Edition 24GB GDDR6X",
        "description": "Used for 6 months in a smoke-free home. Never mined. Ships double boxed.",
        "price": "US $1,299.99",
        "currency": "USD",
        "shipping": "Free",
        "condition": "3000",
        "seller": SellerInfo(name="gpu_trader", feedback_score=1520, feedback_pct=99.8),
        "image_urls": ["https://i.ebayimg.com/images/g/abc/s-l1600.jpg"],
        "posted_at": datetime(2026, 10, 6, 12, 30, tzinfo=timezone.utc),
    }
    data.update(overrides)
    return RawListing.model_validate(data)


# =========================================================================== exports


def test_reexports_unified_schema_from_types() -> None:
    assert nm.DealItem is engine_types.DealItem
    assert nm.RawListing is engine_types.RawListing
    for name in ("DealItem", "RawListing", "Normalizer", "NormalizationError", "parse_money", "parse_price",
                 "extract_title_price", "parse_condition", "canonical_url", "clean_text"):
        assert name in nm.__all__


# =========================================================================== money

MONEY_CASES: list[tuple[object, float | None, str | None]] = [
    # US formats
    ("$1,299.99", 1299.99, "USD"),
    ("US $1,299.99", 1299.99, "USD"),
    ("US$1,299.99", 1299.99, "USD"),
    ("USD 1299", 1299.0, "USD"),
    ("usd 1299", 1299.0, "USD"),
    ("USD $1299", 1299.0, "USD"),
    ("1299USD", 1299.0, "USD"),
    ("1299 USD", 1299.0, "USD"),
    ("1299$", 1299.0, "USD"),
    ("$ 1,299", 1299.0, "USD"),
    ("1299", 1299.0, None),
    ("1,299", 1299.0, None),
    ("1299.5", 1299.5, None),
    ("  1299.99  ", 1299.99, None),
    (1299, 1299.0, None),
    (1299.0, 1299.0, None),
    (0, 0.0, None),
    # other currencies / notations
    ("1.299,99 \u20ac", 1299.99, "EUR"),
    ("\u20ac1.299", 1299.0, "EUR"),
    ("EUR 1.299,99", 1299.99, "EUR"),
    ("1 299,99 \u20ac", 1299.99, "EUR"),
    ("1\u00a0299,99\u00a0\u20ac", 1299.99, "EUR"),
    ("\u00a3849", 849.0, "GBP"),
    ("C$1,100", 1100.0, "CAD"),
    ("CA$1,100", 1100.0, "CAD"),
    ("CAD $1299", 1299.0, "CAD"),
    ("A$1,500", 1500.0, "AUD"),
    ("AU$1500", 1500.0, "AUD"),
    ("1'299.50 CHF", 1299.5, "CHF"),
    ("\u00a5150000", 150000.0, "JPY"),
    # k multiplier
    ("$1.2k", 1200.0, "USD"),
    ("$1.2K", 1200.0, "USD"),
    ("$1,2k", 1200.0, "USD"),
    ("$1.15k", 1150.0, "USD"),
    ("2k", 2000.0, None),
    ("$1,250k", None, None),  # 1.25k or 1,250k: ambiguous, refuse
    ("$1.250k", None, None),
    # decorations
    ("1299 OBO", 1299.0, None),
    ("$800 or best offer", 800.0, "USD"),
    ("$800-$900", 800.0, "USD"),
    ("$800 - $900", 800.0, "USD"),
    ("2 for $50", 50.0, "USD"),
    ("Price: $1,299.99 + $20 shipping", 1299.99, "USD"),
    ("\U0001f525 $1,299 \U0001f525", 1299.0, "USD"),
    ("\U0001f4b01299\U0001f4b0", 1299.0, None),
    ("$12,99", 12.99, "USD"),
    ("0,99", 0.99, None),
    ("$1.299", 1299.0, "USD"),
    # free / zero
    ("Free", 0.0, None),
    ("FREE", 0.0, None),
    ("free!", 0.0, None),
    ("Price: Free", 0.0, None),
    ("FREE - pick up only", 0.0, None),
    ("$0", 0.0, "USD"),
    ("0.00", 0.0, None),
    # no price
    ("", None, None),
    ("   ", None, None),
    ("Ask", None, None),
    ("Contact for price", None, None),
    ("Contact for price, free delivery", None, None),
    ("Free shipping", None, None),
    ("Make offer", None, None),
    ("NaN", None, None),
    (None, None, None),
    (True, None, None),
    (float("nan"), None, None),
    (float("inf"), None, None),
    # negative / garbage / absurd
    ("-5", None, None),
    ("-$5", None, None),
    ("$-5", None, None),
    (-3, None, None),
    (-0.01, None, None),
    ("$99999999999", None, None),
    ("99999999", None, None),
    ("1,000,000,000", None, None),
    (1e9, None, None),
    ("1,2,3", None, None),
    ("1.2.3,4", None, None),
    ("1\ufe0f\u20e32\ufe0f\u20e3", None, None),  # keycap emoji digits are decoration
    ("$" + "1" * 300, None, None),  # not a price field
]


@pytest.mark.parametrize(("value", "amount", "currency"), MONEY_CASES)
def test_parse_money(value: object, amount: float | None, currency: str | None) -> None:
    got_amount, got_currency = parse_money(value)  # type: ignore[arg-type]
    if amount is None:
        assert got_amount is None
        assert got_currency is None
    else:
        assert got_amount == pytest.approx(amount)
        assert got_currency == currency


@pytest.mark.parametrize(("value", "amount", "_currency"), MONEY_CASES)
def test_parse_price_matches_parse_money(value: object, amount: float | None, _currency: str | None) -> None:
    got = parse_price(value)  # type: ignore[arg-type]
    if amount is None:
        assert got is None
    else:
        assert got == pytest.approx(amount)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("$1,299.99", 1299.99),
        ("1.299,99 \u20ac", 1299.99),
        ("$1.2k", 1200.0),
        ("Free", 0.0),
        ("$800 OBO", 800.0),
        ("$800-$900", 800.0),
        ("Ask", None),
        ("Contact", None),
        ("", None),
        ("-5", None),
        (float("nan"), None),
    ],
)
def test_parse_price_contract_examples(value: object, expected: float | None) -> None:
    assert parse_price(value) == expected  # type: ignore[arg-type]


def test_money_detail_flags_negative_and_bare_dollar() -> None:
    assert nm._parse_money_detail("-5").negative is True
    assert nm._parse_money_detail(-5).negative is True
    assert nm._parse_money_detail("Ask").negative is False
    assert nm._parse_money_detail("$1,100").bare_dollar is True
    assert nm._parse_money_detail("US $1,100").bare_dollar is False
    assert nm._parse_money_detail("C$1,100").bare_dollar is False
    assert nm._parse_money_detail("1100").bare_dollar is False


@pytest.mark.parametrize(
    ("run", "kilo", "expected"),
    [
        ("1,299.99", False, 1299.99),
        ("1.299,99", False, 1299.99),
        ("1,299,999", False, 1299999.0),
        ("1.299.999", False, 1299999.0),
        ("1,299", False, 1299.0),
        ("0,999", False, 0.999),
        ("1299,999", False, 1299.999),
        ("12,5", False, 12.5),
        ("1'299.50", False, 1299.5),
        ("1,29,99", False, None),
        ("12,34.5", False, None),
        ("1.2", True, 1200.0),
        ("1,2", True, 1200.0),
        ("1,250", True, None),
        ("1,250,000", True, None),
        ("10000001", False, None),
        ("10000000", False, 10_000_000.0),
    ],
)
def test_to_number(run: str, kilo: bool, expected: float | None) -> None:
    got = nm._to_number(run, kilo)
    assert (got is None) if expected is None else got == pytest.approx(expected)


# ----------------------------------------------------------------- title prices

TITLE_CASES: list[tuple[str, float | None]] = [
    ("[GPU] Zotac RTX 4070 Super - $549.99 ($599.99 - $50 promo code)", 549.99),
    ("RTX 4090 FE - $1199 ($1599 - $400)", 1199.0),
    ("RTX 4090 ($1599 - $400)", 1199.0),
    ("RTX 4090 ($1599 -$400 instant savings)", 1199.0),
    ("[CPU] AMD 7800X3D ($449 - $50 = $399)", 399.0),
    ("[GPU] RTX 4080 (\u2013$100 rebate) $999", 999.0),
    ("($800 \u2013 $900)", 800.0),  # a range, not a breakdown
    ("RTX 4090 24GB 240Hz 2023 20% off", None),  # no explicit money at all
    ("Samsung Odyssey G9 49in 240Hz 1ms 2024 model $1,099.99", 1099.99),
    ("LG 27GP850-B $296.99 (-$100 rebate)", 296.99),
    ("(-$100 rebate)", None),
    ("RTX 4070 $50 off at Newegg", None),
    ("RTX 4070 Super, $50 off with code, now $549", 549.0),
    ("Save $100 - RTX 4080 Super $999", 999.0),
    ("Get A $50 gift card with RTX 4080 $999", 999.0),
    ("RTX 4090 ($1599 value) for $1200", 1200.0),
    ("$549 + $20 shipping", 549.0),
    ("$800-$900", 800.0),
    ("RTX 4090 for rent $2/hr", 2.0),  # rentals still parse; the text filter rejects them
    ("RTX 3090 $650/mo lease", 650.0),
    ("4090 USD 1599", 1599.0),
    ("RTX 4090 1500 USD shipped", 1500.0),
    ("1500$ shipped 4090 FE", 1500.0),
    ("RTX 4090 $ shipped later $1500", 1500.0),
    ("RTX 4090 $$$ cheap $1,299.99!", 1299.99),
    ("US $1,299.99 RTX 4090", 1299.99),
    ("RTX 4090 FE C$2000", 2000.0),
    ("RTX 4090 for $1.6k", 1600.0),
    ("[GPU] RTX 4060 - Free ($0 with bundle)", 0.0),
    ("Price is usdc only", None),
    ("RTX 4090 $", None),
    ("", None),
    ("x" * 40 + " $1500", 1500.0),
]


@pytest.mark.parametrize(("title", "expected"), TITLE_CASES)
def test_extract_title_price(title: str, expected: float | None) -> None:
    got = extract_title_price(title)
    assert (got is None) if expected is None else got == pytest.approx(expected)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("RTX 4090 FE C$2000", (2000.0, "CAD", False)),
        ("RTX 4090 CAD $1500", (1500.0, "CAD", False)),
        ("RTX 4090 $1500", (1500.0, "USD", True)),
        ("RTX 4090 USD 1500", (1500.0, "USD", False)),
    ],
)
def test_text_price_currency(text: str, expected: tuple[float, str, bool]) -> None:
    assert nm._find_text_price(text) == expected


# =========================================================================== condition

EBAY_IDS = [
    ("1000", NEW), ("1500", OPEN_BOX), ("1750", OPEN_BOX), ("2000", REFURB), ("2010", REFURB),
    ("2020", REFURB), ("2030", REFURB), ("2500", REFURB), ("2750", USED), ("2990", USED),
    ("3000", USED), ("3010", USED), ("4000", USED), ("5000", USED), ("6000", USED), ("7000", PARTS),
    ("1100", NEW), ("1600", OPEN_BOX), ("2600", REFURB), ("3500", USED), (" 3000 ", USED),
]


@pytest.mark.parametrize(("raw", "expected"), EBAY_IDS)
def test_condition_ebay_ids(raw: str, expected: Condition) -> None:
    for kind in SourceKind:
        assert parse_condition(raw, kind) is expected


@pytest.mark.parametrize("raw", ["9999", "300", "70000", "0000"])
def test_condition_unknown_numeric_falls_back_to_kind(raw: str) -> None:
    assert parse_condition(raw, MARKET) is USED
    assert parse_condition(raw, RETAIL) is NEW


RAW_CONDITION_STRINGS = [
    # eBay names / enums
    ("New", NEW), ("Brand New", NEW), ("New other (see details)", OPEN_BOX), ("Open box", OPEN_BOX),
    ("New with defects", OPEN_BOX), ("Certified - Refurbished", REFURB), ("Excellent - Refurbished", REFURB),
    ("Very Good - Refurbished", REFURB), ("Seller refurbished", REFURB), ("Like New", USED), ("Used", USED),
    ("Pre-owned", USED), ("Very Good", USED), ("Good", USED), ("Acceptable", USED),
    ("For parts or not working", PARTS), ("NEW_OTHER", OPEN_BOX), ("LIKE_NEW", USED), ("USED_VERY_GOOD", USED),
    ("CERTIFIED_REFURBISHED", REFURB), ("FOR_PARTS_OR_NOT_WORKING", PARTS), ("PRE_OWNED_FAIR", USED),
    # Best Buy / Amazon / Newegg
    ("Open-Box Excellent", OPEN_BOX), ("Open-Box Good", OPEN_BOX), ("Open-Box Fair", OPEN_BOX),
    ("Refurbished", REFURB), ("Geek Squad Certified Refurbished", REFURB), ("Renewed", REFURB),
    ("Renewed Premium", REFURB), ("Used - Like New", USED), ("Used - Very Good", USED),
    # FB / OfferUp / Craigslist
    ("Used - Fair", USED), ("New (never used)", NEW), ("Open box (never used)", OPEN_BOX),
    ("Reconditioned/Certified", REFURB), ("Used (normal wear)", USED), ("For parts", PARTS),
    ("like new", USED), ("excellent", USED), ("fair", USED), ("salvage", PARTS),
    # free text in the condition field
    ("brand new sealed", NEW), ("BNIB", NEW), ("NIB", NEW), ("new in box", NEW), ("lightly used", USED),
    ("factory sealed", NEW), ("for parts or repair", PARTS),
    # Condition enum values round-trip
    ("open_box", OPEN_BOX), ("refurbished", REFURB), ("for_parts", PARTS), ("used", USED), ("new", NEW),
]


@pytest.mark.parametrize(("raw", "expected"), RAW_CONDITION_STRINGS)
def test_condition_structured_strings(raw: str, expected: Condition) -> None:
    assert parse_condition(raw, MARKET) is expected
    assert parse_condition(raw, RETAIL) is expected


@pytest.mark.parametrize(("raw", "kind", "expected"), [
    (None, RETAIL, NEW), (None, AGG, NEW), (None, LOCAL, USED), (None, MARKET, USED),
    ("", RETAIL, NEW), ("Unknown", LOCAL, USED), ("Other (see description)", LOCAL, USED), ("N/A", RETAIL, NEW),
    ("unknown", "retail", NEW), (None, "local", USED),
])
def test_condition_kind_defaults(raw: str | None, kind: SourceKind, expected: Condition) -> None:
    assert parse_condition(raw, kind) is expected


FREE_TEXT_CASES = [
    # retail / aggregator: default NEW, text may only downgrade
    (AGG, "[GPU] RTX 4070 (Open Box) $450", OPEN_BOX),
    (AGG, "[GPU] RTX 4070 Open-Box Excellent at Best Buy", OPEN_BOX),
    (AGG, "[GPU] RTX 4070 Refurbished $399", REFURB),
    (AGG, "[GPU] Renewed RTX 4070 - Amazon", REFURB),
    (AGG, "[GPU] RTX 3080 (Used - Like New) $399 Amazon Warehouse", USED),
    (AGG, "[GPU] RTX 3080 pre-owned $350", USED),
    (AGG, "[GPU] RTX 4070 $549", NEW),
    (AGG, "[GPU] RTX 4070 (not refurbished) $549", NEW),
    (AGG, "No Box - Refurbished RTX 4070", REFURB),
    (AGG, "RTX 4070 can be used for 4K gaming $549", NEW),
    (RETAIL, "Refurbished and open box RTX 4070", REFURB),
    (RETAIL, "RTX 4070 brand new sealed", NEW),
    (RETAIL, "RTX 3080 for parts or not working", PARTS),
    # local / marketplace: default USED, explicit new wording upgrades
    (LOCAL, "RTX 4090 FE brand new sealed", NEW),
    (LOCAL, "RTX 4090 BNIB", NEW),
    (LOCAL, "bnib rtx 4090", NEW),
    (LOCAL, "RTX 4090 NIB", NEW),
    (LOCAL, "RTX 4090 new in box", NEW),
    (LOCAL, "RTX 4090 factory sealed", NEW),
    (LOCAL, "RTX 4090 sealed", NEW),
    (LOCAL, "RTX 4090 unopened", NEW),
    (LOCAL, "RTX 4090 New", NEW),
    (LOCAL, "RTX 4090\nBrand new, never used", NEW),
    (LOCAL, "RTX 4090\nbrand new, never opened", NEW),
    (LOCAL, "RTX 4090\nNo longer needed, brand new sealed", NEW),
    (LOCAL, "RTX 4090 like new", USED),
    (LOCAL, "RTX 4090 like-new condition", USED),
    (LOCAL, "RTX 4090\nlightly used, works great", USED),
    (LOCAL, "RTX 4090\nPre-owned", USED),
    (LOCAL, "RTX 4090\nnever been used", USED),  # negated "used" proves nothing -> default
    (LOCAL, "RTX 4090 brand new\nused twice for testing", USED),  # any used wording wins
    (LOCAL, "RTX 4090 brand new\nopened once to test", OPEN_BOX),
    (LOCAL, "RTX 4090 brand new open box", OPEN_BOX),
    (LOCAL, "RTX 4090 brand new refurbished", REFURB),
    (LOCAL, "RTX 4090 open box", USED),  # open-box claims alone never upgrade a local listing
    (LOCAL, "RTX 4090 refurbished", USED),
    (LOCAL, "RTX 4090 re-sealed", USED),
    (LOCAL, "RTX 4090 resealed", USED),
    (LOCAL, "RTX 4090\nnew thermal pads and paste", USED),  # bare "new" only counts in the title
    (LOCAL, "RTX 4090 with new thermal pads", USED),
    (LOCAL, "RTX 4090 new-ish", USED),
    (LOCAL, "RTX 4090 FE\nThe new owner gets the box", USED),
    (LOCAL, "Renewal RTX 4090 anew", USED),  # mid-word / unrelated words
    (LOCAL, "RTX 4090\nisn't brand new", USED),
    (LOCAL, "RTX 3080 FOR PARTS", PARTS),
    (LOCAL, "RTX 3080\nsold for parts/repair", PARTS),
    (LOCAL, "RTX 3080\nparts only", PARTS),
    (LOCAL, "RTX 3080\nnot for parts, works perfectly", USED),
    (LOCAL, "RTX 3080\ndoesn't work, for parts", PARTS),
    (MARKET, "RTX 4090 brand new sealed", NEW),
    (MARKET, "RTX 4090", USED),
]


@pytest.mark.parametrize(("kind", "text", "expected"), FREE_TEXT_CASES)
def test_condition_free_text_by_kind(kind: SourceKind, text: str, expected: Condition) -> None:
    assert parse_condition(None, kind, text) is expected


@pytest.mark.parametrize(("raw", "kind", "text", "expected"), [
    ("New", MARKET, "RTX 4090 like new", USED),  # title downgrades a structured NEW
    ("1000", MARKET, "RTX 4090 refurbished", REFURB),
    ("New", RETAIL, "Refurbished RTX 4070", REFURB),
    ("1000", MARKET, "RTX 3080 for parts", PARTS),
    ("Used", MARKET, "RTX 4090 BNIB sealed", USED),  # text never upgrades a structured value
    ("1500", MARKET, "RTX 4090 brand new", OPEN_BOX),
    ("7000", MARKET, "RTX 4090 brand new", PARTS),
    ("New", MARKET, "RTX 4090\nused once", NEW),  # only the title line can downgrade
    ("New", LOCAL, "RTX 4090 never used", NEW),
])
def test_condition_structured_value_with_text(raw: str, kind: SourceKind, text: str, expected: Condition) -> None:
    assert parse_condition(raw, kind, text) is expected


def test_condition_unknown_kind_uses_safest_hint() -> None:
    assert parse_condition(None, "bogus", "") is Condition.UNKNOWN  # type: ignore[arg-type]
    assert parse_condition(None, "bogus", "brand new but used") is USED  # type: ignore[arg-type]


# =========================================================================== URLs

URL_CASES = [
    # eBay: tracking stripped, functional variation + EPN affiliate params kept verbatim
    (
        "https://www.ebay.com/itm/123456789012?_trkparms=abc&_trksid=p2047675.c100005&hash=item1cbb:g:abc"
        "&amdata=enc%3Axyz&var=55&campid=5338&mkcid=1",
        "https://www.ebay.com/itm/123456789012?var=55&campid=5338&mkcid=1",
    ),
    # Best Buy: skuId kept, Impact affiliate + utm + fragment removed
    (
        "https://www.bestbuy.com/site/nvidia-geforce-rtx-5090/6614151.p?skuId=6614151&intl=nosplash&irclickid=xyz"
        "&irgwc=1&loc=abc&acampID=1&utm_source=x#anchor",
        "https://www.bestbuy.com/site/nvidia-geforce-rtx-5090/6614151.p?skuId=6614151&intl=nosplash",
    ),
    # Amazon: product slug + ref breadcrumbs collapse to /dp/ASIN, variant params kept
    (
        "HTTPS://WWW.Amazon.com/Some-Title/dp/b0bg9xyz12/ref=sr_1_1?crid=X&keywords=rtx&qid=1&sr=8-1&th=1&psc=1",
        "https://www.amazon.com/dp/B0BG9XYZ12?th=1&psc=1",
    ),
    ("https://www.amazon.com/gp/product/B0BG9XYZ12?pf_rd_r=1&ref_=abc", "https://www.amazon.com/dp/B0BG9XYZ12"),
    ("https://smile.amazon.co.uk/dp/B0BG9XYZ12/?tag=me-21", "https://smile.amazon.co.uk/dp/B0BG9XYZ12?tag=me-21"),
    ("https://www.amazon.com/s?k=rtx+4090&ref=nb_sb_noss", "https://www.amazon.com/s"),
    # Facebook Marketplace
    (
        "https://www.facebook.com/marketplace/item/123456/?ref=search&referral_code=null&tracking=browse%3Ax&__tn__=!%3AD",
        "https://www.facebook.com/marketplace/item/123456/",
    ),
    ("https://slickdeals.net/f/1700-rtx?src=frontpage&utm_source=rss&page=2", "https://slickdeals.net/f/1700-rtx?page=2"),
    ("https://www.newegg.com/p/N82E16814500572?cm_sp=x&Item=N82E16814500572", "https://www.newegg.com/p/N82E16814500572?Item=N82E16814500572"),
    ("https://www.walmart.com/ip/123?athbdg=L1600&wmlspartner=x&selected=true", "https://www.walmart.com/ip/123?selected=true"),
    ("https://example.com/p?fbclid=1&gclid=2&mc_cid=3&mc_eid=4&_ga=5&spm=6&igshid=7&q=rtx%204090", "https://example.com/p?q=rtx%204090"),
    ("https://example.com/p?UTM_Source=x&utm_medium=y&id=5&", "https://example.com/p?id=5"),
    # host / scheme / port / credentials normalisation
    ("https://user:pw@Example.COM:443/path", "https://example.com/path"),
    ("http://example.com:80", "http://example.com/"),
    ("http://example.com:8080/a", "http://example.com:8080/a"),
    ("https://Example.com./a", "https://example.com/a"),
    ("https://[::1]:8443/x?mc_cid=1", "https://[::1]:8443/x"),
    ("//cdn.example.com/a?b=1#frag", "https://cdn.example.com/a?b=1"),
    ("www.example.com/x?utm_medium=rss", "https://www.example.com/x"),
    ("  <https://example.com/a b>  ", "https://example.com/a%20b"),
    # path case preserved, encoded query preserved byte-for-byte
    ("https://example.com/Path/To/Item?Q=A%2FB&x=%E2%82%AC", "https://example.com/Path/To/Item?Q=A%2FB&x=%E2%82%AC"),
    # not http(s): returned stripped, never raises
    ("/relative/path", "/relative/path"),
    ("javascript:alert(1)", "javascript:alert(1)"),
    ("ftp://example.com/file", "ftp://example.com/file"),
    ("http://[::1", "http://[::1"),
    ("", ""),
]


@pytest.mark.parametrize(("url", "expected"), URL_CASES)
def test_canonical_url(url: str, expected: str) -> None:
    assert canonical_url(url) == expected


@pytest.mark.parametrize(("url", "_expected"), URL_CASES)
def test_canonical_url_is_idempotent(url: str, _expected: str) -> None:
    once = canonical_url(url)
    assert canonical_url(once) == once


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://i.ebayimg.com/images/g/abc/s-l1600.jpg", "https://i.ebayimg.com/images/g/abc/s-l1600.jpg"),
        ("HTTPS://SCONTENT.xx.fbcdn.net/v/t45/a.jpg?oh=AbC&oe=1#x", "https://scontent.xx.fbcdn.net/v/t45/a.jpg?oh=AbC&oe=1#x"),
        ("//i.ebayimg.com/x.jpg", "https://i.ebayimg.com/x.jpg"),
        ("  https://img.example.com/a b.jpg ", "https://img.example.com/a%20b.jpg"),
        ("ftp://example.com/a.jpg", None),
        ("data:image/png;base64,AAAA", None),
        ("/relative.jpg", None),
        ("https:///nohost.jpg", None),
        ("https://bad host/x.jpg", None),
        (None, None),
        (42, None),
    ],
)
def test_image_url(url: object, expected: str | None) -> None:
    assert nm._image_url(url) == expected


# =========================================================================== text

CLEAN_CASES = [
    ("  GeForce RTX\u2122 4090 &amp; more  ", "GeForce RTX 4090 & more"),
    ("Intel\u00ae Core\u2122 i9-14900K", "Intel Core i9-14900K"),
    ("<p>Hello<br>world</p><script>alert(1)</script>", "Hello\nworld"),
    ("<div>a</div><div>b</div>", "a\nb"),
    ("<ul><li>one</li><li>two</li></ul>", "one\ntwo"),
    ("<b>RTX</b> <i>4090</i>", "RTX 4090"),
    ("a<style>.x{}</style>b<noscript>z</noscript>c", "a b c"),
    ("a<!-- hidden -->b<!DOCTYPE html>c<![CDATA[d]]>e", "a b c d e"),
    ("I <3 this > that", "I <3 this > that"),  # not a tag
    ("&amp;lt;b&amp;gt;bold&amp;lt;/b&amp;gt;", "bold"),  # double-escaped markup
    ("AT&T and R&D", "AT&T and R&D"),
    ("5 &gt; 3 &#36;99 &#x24;5", "5 > 3 $99 $5"),
    ("\u201cFancy\u201d \u2018quotes\u2019 \u2014 dashes \u2013 here\u2026", '"Fancy" \'quotes\' - dashes - here...'),
    ("it\u00b4s", "it's"),
    ("zero\u200bwidth\u00a0nbsp\ufeff\u200e", "zerowidth nbsp"),
    ("\uff26\uff55\uff4c\uff4c\uff57\uff49\uff44\uff54\uff48 \uff11\uff12\uff13", "Fullwidth 123"),
    ("RTX\u20114090", "RTX-4090"),  # non-breaking hyphen
    ("line1\r\n\r\n   line2\t\tend", "line1\nline2 end"),
    ("a\x00b\x07c\x1bd", "abcd"),
    ("   ", ""),
    ("", ""),
    ("\U0001f525 Deal \U0001f525", "\U0001f525 Deal \U0001f525"),
]


@pytest.mark.parametrize(("raw", "expected"), CLEAN_CASES)
def test_clean_text(raw: str, expected: str) -> None:
    assert clean_text(raw) == expected


def test_clean_text_single_line_mode() -> None:
    assert clean_text("line1\n\nline2  x\tz", keep_newlines=False) == "line1 line2 x z"
    assert clean_text("<p>a</p><p>b</p>", keep_newlines=False) == "a b"


@pytest.mark.parametrize(
    ("raw", "max_len", "expected"),
    [
        ("one two three four five six", 13, "one two three"),  # boundary right at the cut
        ("one two three four five six", 15, "one two three"),  # backs off to a word boundary
        ("onetwothreefourfive", 7, "onetwot"),  # no boundary: hard cut
        ("a bcdefghijklmnopqrstuvwxyz", 10, "a bcdefghi"),  # boundary too early: hard cut
        ("short", 300, "short"),
        ("anything", 0, ""),
    ],
)
def test_clean_text_truncation(raw: str, max_len: int, expected: str) -> None:
    got = clean_text(raw, max_len)
    assert got == expected
    assert len(got) <= max_len
    assert not got.endswith(("...", "\u2026"))


def test_clean_text_bounds_oversized_input() -> None:
    text = "<p>word " * 100_000  # ~800 KB of markup
    got = clean_text(text, 5000)
    assert 4900 <= len(got) <= 5000
    assert "<" not in got


@pytest.mark.parametrize(
    "hostile",
    [
        "<script>x" * 5000,  # unclosed blocks: must stay linear
        "<!--x" * 5000,
        "<style" * 5000,
        "&amp;" * 20000,
        "$ " * 20000,
    ],
)
def test_hostile_text_is_fast(hostile: str) -> None:
    start = time.perf_counter()
    clean_text(hostile, 5000)
    extract_title_price(hostile)
    parse_condition(None, LOCAL, hostile[:20000])
    assert time.perf_counter() - start < 0.5


# =========================================================================== Normalizer


def test_normalize_happy_path_ebay(normalizer: Normalizer) -> None:
    raw = make_raw()
    item = normalizer.normalize(raw)
    assert isinstance(item, DealItem)
    assert item.url == "https://www.ebay.com/itm/123456789012?var=55"
    assert item.title == "NVIDIA GeForce RTX 4090 Founders Edition 24GB GDDR6X"
    assert item.price == 1299.99
    assert item.currency == "USD"
    assert item.shipping == 0.0
    assert item.total_price == 1299.99
    assert item.condition is USED
    assert item.seller == raw.seller
    assert item.image_urls == ["https://i.ebayimg.com/images/g/abc/s-l1600.jpg"]
    assert item.posted_at == datetime(2026, 10, 6, 12, 30, tzinfo=timezone.utc)
    assert item.listing_key == raw.listing_key
    assert item.fingerprint == hashlib.sha1(b"ebay:123456789012").hexdigest()[:20]
    assert item.received_at == raw.received_at
    assert item.normalized_at.tzinfo is not None
    assert item.extra == {}
    # serialisable for the bus / DB
    assert DealItem.model_validate_json(item.model_dump_json()) == item


def test_normalize_copies_passthrough_fields(normalizer: Normalizer) -> None:
    raw = make_raw(
        source="retail",
        source_kind=SourceKind.RETAIL,
        source_id="bestbuy:6614151",
        url="https://www.bestbuy.com/site/x/6614151.p?skuId=6614151&irclickid=1",
        price=1999.99,
        shipping=None,
        list_price="$2,199.99",
        condition="New",
        seller=None,
        location=Location(text="Austin, TX", city="Austin", region="TX"),
        in_stock=True,
        quantity=3,
        retailer="  Best&nbsp;Buy  ",
        sku=" 6614151 ",
        query="rtx 5090",
        profile_hint="rtx_5090",
        extra={"adapter": "bestbuy", "nested": {"a": 1}},
        node_id="gcp-1",
    )
    item = normalizer.normalize(raw)
    assert item.source_kind is SourceKind.RETAIL
    assert item.condition is NEW
    assert item.shipping is None
    assert item.total_price == 1999.99
    assert item.list_price == 2199.99
    assert item.location == raw.location
    assert item.in_stock is True
    assert item.quantity == 3
    assert item.retailer == "Best Buy"
    assert item.sku == "6614151"
    assert item.query == "rtx 5090"
    assert item.profile_hint == "rtx_5090"
    assert item.extra == {"adapter": "bestbuy", "nested": {"a": 1}}
    assert item.node_id == "gcp-1"


@pytest.mark.parametrize(
    ("price", "shipping", "expected_price", "expected_shipping", "expected_total"),
    [
        ("$1,299.99", "$25.50", 1299.99, 25.5, 1325.49),
        (1299.999, 0.004, 1300.0, 0.0, 1300.0),
        (100.105, "4.995", 100.1, 5.0, 105.1),
        ("$0.10", "$0.20", 0.1, 0.2, 0.3),
        ("$800 OBO", "Free shipping", 800.0, 0.0, 800.0),
        ("$800", "FREE Standard Shipping", 800.0, 0.0, 800.0),
        ("$800", "Shipping: free", 800.0, 0.0, 800.0),
        ("$800", "Calculated", 800.0, None, 800.0),
        ("$800", "Local pickup only", 800.0, None, 800.0),
        ("$800", -5, 800.0, None, 800.0),
        ("$800", None, 800.0, None, 800.0),
        ("Free", None, 0.0, None, 0.0),
    ],
)
def test_normalize_prices_round_to_cents(
    normalizer: Normalizer,
    price: object,
    shipping: object,
    expected_price: float,
    expected_shipping: float | None,
    expected_total: float,
) -> None:
    item = normalizer.normalize(make_raw(price=price, shipping=shipping))
    assert item.price == expected_price
    assert item.shipping == expected_shipping
    assert item.total_price == expected_total
    assert item.total_price == round(item.total_price, 2)


@pytest.mark.parametrize("list_price", [None, "", "Ask", 0, "-5", "$0"])
def test_normalize_drops_unusable_list_price(normalizer: Normalizer, list_price: object) -> None:
    assert normalizer.normalize(make_raw(list_price=list_price)).list_price is None


def test_normalize_price_falls_back_to_title(normalizer: Normalizer) -> None:
    raw = make_raw(
        source="reddit",
        source_kind=SourceKind.AGGREGATOR,
        source_id="t3_1abcd",
        url="https://www.reddit.com/r/buildapcsales/comments/1abcd/gpu_zotac/",
        title="[GPU] Zotac RTX 4070 Super - $549.99 ($599.99 - $50 promo code)",
        description="",
        price=None,
        shipping=None,
        condition=None,
        outbound_url="https://www.newegg.com/p/N82E16814500572?cm_sp=Homepage&Item=N82E16814500572#reviews",
        extra={"flair": "GPU"},
    )
    item = normalizer.normalize(raw)
    assert item.price == 549.99
    assert item.currency == "USD"
    assert item.condition is NEW
    assert item.extra == {"flair": "GPU", "price_origin": "title"}
    assert raw.extra == {"flair": "GPU"}  # the raw listing is never mutated
    assert item.outbound_url == "https://www.newegg.com/p/N82E16814500572?Item=N82E16814500572"
    assert item.best_url == item.outbound_url


def test_normalize_price_falls_back_to_description(normalizer: Normalizer) -> None:
    raw = make_raw(
        source="reddit",
        source_kind=SourceKind.LOCAL,
        source_id="t3_hws1",
        url="https://www.reddit.com/r/hardwareswap/comments/hws1/x/",
        title="[USA-TX] [H] RTX 4090 FE [W] PayPal, Local Cash",
        description="Selling my 4090 FE, works perfectly.\n\nAsking $1,500 shipped, $1,450 local.",
        price="Ask",
        condition=None,
    )
    item = normalizer.normalize(raw)
    assert item.price == 1500.0
    assert item.extra["price_origin"] == "description"


def test_normalize_garbage_price_field_uses_title(normalizer: Normalizer) -> None:
    item = normalizer.normalize(make_raw(price="$99999999999", title="RTX 4090 FE $1,499"))
    assert item.price == 1499.0
    assert item.extra["price_origin"] == "title"


@pytest.mark.parametrize(
    ("raw_currency", "price", "expected"),
    [
        ("USD", "$1,000", "USD"),
        ("usd", "1000", "USD"),
        ("US$", 1000, "USD"),
        ("$", 1000, "USD"),
        ("", 1000, "USD"),
        ("USD", "US $1,000", "USD"),
    ],
)
def test_normalize_currency_resolution_allowed(normalizer: Normalizer, raw_currency: str, price: object, expected: str) -> None:
    assert normalizer.normalize(make_raw(currency=raw_currency, price=price)).currency == expected


@pytest.mark.parametrize(
    ("raw_currency", "price", "title"),
    [
        ("USD", "1.299,99 \u20ac", "RTX 4090"),  # explicit marker in the price beats the default
        ("USD", "\u00a3849", "RTX 4090"),
        ("USD", "C$1,100", "RTX 4090"),
        ("CAD", "$1,100", "RTX 4090"),  # bare $ inherits the listing's dollar currency
        ("CAD", 1100, "RTX 4090"),
        ("EUR", 1100, "RTX 4090"),
        ("\u20ac", 1100, "RTX 4090"),
        ("USD", None, "RTX 4090 FE C$2000"),  # title fallback keeps the explicit marker
    ],
)
def test_normalize_unsupported_currency(normalizer: Normalizer, raw_currency: str, price: object, title: str) -> None:
    with pytest.raises(NormalizationError) as exc:
        normalizer.normalize(make_raw(currency=raw_currency, price=price, title=title, description=""))
    assert exc.value.code == "unsupported_currency"


def test_normalize_accepts_currency_allowed_by_config(config: AppConfig) -> None:
    cfg = config.model_copy(update={"filters": config.filters.model_copy(update={"allowed_currencies": ["usd", "cad"]})})
    item = Normalizer(cfg).normalize(make_raw(currency="CAD", price="$1,100"))
    assert (item.price, item.currency) == (1100.0, "CAD")


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"price": None, "title": "RTX 4090 Founders Edition", "description": "DM me for price"}, "no_price"),
        ({"price": "Contact for price", "title": "RTX 4090 FE ($50 off)", "description": ""}, "no_price"),
        ({"price": "-5", "title": "RTX 4090 $1500"}, "negative_price"),
        ({"price": -1299.0}, "negative_price"),
        ({"price": "-$1,299"}, "negative_price"),
        ({"currency": "GBP", "price": 900}, "unsupported_currency"),
        ({"url": "/itm/123"}, "bad_url"),
        ({"url": "javascript:alert(1)"}, "bad_url"),
        ({"url": "ftp://ebay.com/itm/1"}, "bad_url"),
        ({"url": ""}, "bad_url"),
        ({"url": "https://"}, "bad_url"),
        ({"title": ""}, "empty_title"),
        ({"title": "   \n\t "}, "empty_title"),
        ({"title": "<b></b>&nbsp;"}, "empty_title"),
        ({"title": "\U0001f525\U0001f525\U0001f525 !!!"}, "empty_title"),
    ],
)
def test_normalize_errors(normalizer: Normalizer, overrides: dict[str, object], code: str) -> None:
    raw = make_raw(**overrides)
    with pytest.raises(NormalizationError) as exc:
        normalizer.normalize(raw)
    assert exc.value.code == code
    assert raw.listing_key in exc.value.detail
    assert str(exc.value).startswith(code)


def test_normalization_error_shape() -> None:
    err = NormalizationError("bad_url", "ebay:1: url ''")
    assert (err.code, err.detail) == ("bad_url", "ebay:1: url ''")
    assert str(err) == "bad_url: ebay:1: url ''"
    assert str(NormalizationError("no_price")) == "no_price"
    assert isinstance(err, Exception)


def test_normalize_drops_invalid_outbound_url(normalizer: Normalizer) -> None:
    item = normalizer.normalize(make_raw(outbound_url="javascript:void(0)"))
    assert item.outbound_url is None
    assert item.best_url == item.url


def test_normalize_images(normalizer: Normalizer) -> None:
    urls = [
        "https://i.ebayimg.com/a.jpg",
        "https://i.ebayimg.com/a.jpg",  # duplicate
        "HTTPS://I.EBAYIMG.COM/a.jpg",  # duplicate after scheme/host normalisation
        "ftp://example.com/x.jpg",
        "data:image/png;base64,AAAA",
        "",
        "//i.ebayimg.com/b.jpg",
        "https://scontent.fbcdn.net/c.jpg?oh=SiGnEd&oe=65F",
    ] + [f"https://i.ebayimg.com/{i}.jpg" for i in range(10)]
    item = normalizer.normalize(make_raw(image_urls=urls))
    assert len(item.image_urls) == nm.MAX_IMAGES == 8
    assert item.image_urls[:3] == [
        "https://i.ebayimg.com/a.jpg",
        "https://i.ebayimg.com/b.jpg",
        "https://scontent.fbcdn.net/c.jpg?oh=SiGnEd&oe=65F",  # signed query untouched
    ]
    assert len(set(item.image_urls)) == len(item.image_urls)
    assert item.primary_image == "https://i.ebayimg.com/a.jpg"


@pytest.mark.parametrize(
    ("posted_at", "expected"),
    [
        (datetime(2026, 10, 6, 12, 0), datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)),  # naive = UTC
        (datetime(2026, 10, 6, 8, 0, tzinfo=timezone(timedelta(hours=-4))), datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)),
        (None, None),
    ],
)
def test_normalize_posted_at_is_utc(normalizer: Normalizer, posted_at: datetime | None, expected: datetime | None) -> None:
    item = normalizer.normalize(make_raw(posted_at=posted_at))
    assert item.posted_at == expected
    if item.posted_at is not None:
        assert item.posted_at.utcoffset() == timedelta(0)


def test_normalize_naive_received_at_becomes_utc(normalizer: Normalizer) -> None:
    item = normalizer.normalize(make_raw(received_at=datetime(2026, 10, 6, 12, 0)))
    assert item.received_at == datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)


def test_normalize_truncates_title_and_description(normalizer: Normalizer) -> None:
    title = "RTX 4090 " + "word " * 200
    description = "<p>" + "lorem ipsum " * 1000 + "</p>"
    item = normalizer.normalize(make_raw(title=title, description=description))
    assert len(item.title) <= nm.MAX_TITLE_LEN == 300
    assert item.title.startswith("RTX 4090 word")
    assert not item.title.endswith(" ")
    assert len(item.description) <= nm.MAX_DESCRIPTION_LEN == 5000
    assert item.description.endswith(("lorem", "ipsum"))


def test_normalize_cleans_html_title_and_description(normalizer: Normalizer) -> None:
    item = normalizer.normalize(
        make_raw(
            title="  NVIDIA GeForce RTX&trade; 4090 &amp; box\n",
            description="<p>Works great.</p><p>No issues &ndash; never mined.</p>",
        )
    )
    assert item.title == "NVIDIA GeForce RTX 4090 & box"
    assert item.description == "Works great.\nNo issues - never mined."


@pytest.mark.parametrize(
    ("kind", "condition", "title", "description", "expected"),
    [
        (SourceKind.LOCAL, None, "RTX 4090 FE", "Brand new sealed, never opened.", NEW),
        (SourceKind.LOCAL, None, "RTX 4090 FE", "Lightly used for 3 months.", USED),
        (SourceKind.LOCAL, "New", "RTX 4090 FE like new", "", USED),
        (SourceKind.AGGREGATOR, None, "[GPU] RTX 4070 $549", "Also available refurbished for $449.", NEW),
        (SourceKind.AGGREGATOR, None, "[GPU] RTX 4070 Open Box $449", "", OPEN_BOX),
        (SourceKind.MARKETPLACE, "1000", "RTX 4090 FE", "used once", NEW),
    ],
)
def test_normalize_condition(
    normalizer: Normalizer,
    kind: SourceKind,
    condition: str | None,
    title: str,
    description: str,
    expected: Condition,
) -> None:
    item = normalizer.normalize(make_raw(source_kind=kind, condition=condition, title=title, description=description))
    assert item.condition is expected


def test_normalize_is_fast(normalizer: Normalizer) -> None:
    """Typical listings normalise in tens of microseconds (target < 50 us).

    The bound asserted here is deliberately loose so a loaded CI box never flakes;
    it still catches pathological regressions (catastrophic regex backtracking,
    accidental per-call compilation).
    """
    listings = [
        make_raw(),
        make_raw(
            source="reddit",
            source_kind=SourceKind.AGGREGATOR,
            source_id="t3_x",
            url="https://www.reddit.com/r/buildapcsales/comments/x/y/",
            title="[GPU] Zotac RTX 4070 Super - $549.99 ($599.99 - $50 promo code)",
            description="",
            price=None,
            condition=None,
            outbound_url="https://www.newegg.com/p/N82E16814500572?Item=N82E16814500572&cm_sp=x",
        ),
        make_raw(
            source="fb_marketplace",
            source_kind=SourceKind.LOCAL,
            source_id="98765",
            url="https://www.facebook.com/marketplace/item/98765/?ref=search&tracking=x",
            title="RTX 4090 Founders Edition",
            description="Brand new sealed, never opened. Upgraded so I don't need it. Cash only, local pickup.",
            price="$1,450",
            condition=None,
            seller=None,
        ),
    ]
    for raw in listings:
        normalizer.normalize(raw)  # warm up
    samples = []
    for _ in range(5):
        start = time.perf_counter()
        for _ in range(200):
            for raw in listings:
                normalizer.normalize(raw)
        samples.append((time.perf_counter() - start) / (200 * len(listings)))
    assert statistics.median(samples) < 500e-6


def test_parse_functions_never_raise_on_odd_input() -> None:
    odd: list[object] = [None, "", " ", "\x00", "\ufffd", "$", "-", "--5", "$$$", "1e5", "0x10", "\u221e", "\U0001f600" * 10,
                         math.pi, 10**30, -(10**30), "\u0661\u0662\u0663"]
    for value in odd:
        parse_money(value)  # type: ignore[arg-type]
        parse_price(value)  # type: ignore[arg-type]
        if isinstance(value, str):
            extract_title_price(value)
            clean_text(value, 10)
            canonical_url(value)
            parse_condition(value, LOCAL, value)
