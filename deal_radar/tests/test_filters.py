"""Tests for engine/text_filter.py against the SHIPPED config.yaml.

The bulk of this suite is a table of realistic listings (titles/descriptions as they
appear on eBay, r/hardwareswap, Facebook Marketplace, Slickdeals...) with the exact
outcome the shipped rules must produce: which product/variant is identified, which
scam/noise rule rejects it, and which risk signals it carries. Every row is
deterministic; nothing touches the network.

Further sections pin down engine semantics with small purpose-built configs
(priority/variant resolution, negation mechanics, rule scoping, regex edge cases),
prove the literal-anchor gate never changes a result (differential test against an
ungated filter over a fuzzed corpus) and check the performance budget.
"""

from __future__ import annotations

import itertools
import random
import time
from pathlib import Path
from typing import Any

import pytest

from deal_radar.config_schema import AppConfig, load_config
from deal_radar.engine.text_filter import TextFilter
from deal_radar.engine.types import Condition, DealItem, FilterResult, SellerInfo, SourceKind

CONFIG_PATH = Path(__file__).parents[1] / "config.yaml"

_SOURCE_FOR_KIND = {
    SourceKind.LOCAL: "fb_marketplace",
    SourceKind.MARKETPLACE: "ebay",
    SourceKind.RETAIL: "retail",
    SourceKind.AGGREGATOR: "slickdeals",
}
_ids = itertools.count(1)


@pytest.fixture(scope="module")
def config() -> AppConfig:
    return load_config(CONFIG_PATH, env={})


@pytest.fixture(scope="module")
def tf(config: AppConfig) -> TextFilter:
    return TextFilter(config)


def make_item(
    title: str,
    description: str = "",
    *,
    kind: SourceKind = SourceKind.LOCAL,
    price: float = 1000.0,
    condition: Condition = Condition.USED,
    shipping: float | None = None,
) -> DealItem:
    n = next(_ids)
    source = _SOURCE_FOR_KIND[kind]
    seller = SellerInfo(name="gpu_trader", feedback_score=212, feedback_pct=99.6) if kind is SourceKind.MARKETPLACE else None
    return DealItem(
        source=source,
        source_kind=kind,
        source_id=f"{source}-{n}",
        url=f"https://example.com/{source}/item/{n}",
        title=title,
        description=description,
        price=price,
        shipping=shipping,
        total_price=price + (shipping or 0.0),
        condition=condition,
        seller=seller,
        image_urls=[f"https://img.example.com/{n}/1.jpg"],
    )


def outcome(result: FilterResult) -> str:
    """``reject:<code>`` or ``<profile>[/<variant>]``."""
    if not result.accepted:
        return f"reject:{result.reject_code}"
    return f"{result.profile_id}/{result.variant_id}" if result.variant_id else str(result.profile_id)


def codes(result: FilterResult) -> set[str]:
    return {s.code for s in result.risk_signals}


# ============================================================================ shipped rules: outcomes

# (title, description, expected outcome). Source kind is LOCAL (FB Marketplace) unless
# a row needs another kind; those live in dedicated tests below.
OUTCOME_CASES: list[tuple[str, str, str]] = [
    # ---- box-only bait vs. legit "no box" / "with box" listings
    ("RTX 4090 Box Only", "", "reject:box_only"),
    ("ASUS TUF RTX 4090 - EMPTY BOX, no card", "", "reject:box_only"),
    ("RTX 4090 FE - no box only the card, works great", "", "rtx_4090"),
    ("RTX 4090 FE, no box", "", "rtx_4090"),
    ("Gigabyte RTX 4090 Gaming OC with original box", "", "rtx_4090"),
    ("RTX 4090 FE box-only", "", "reject:box_only"),
    ("NVIDIA RTX 4090 Founders Edition", "Empty box only, perfect for display. No GPU is included.", "reject:box_only"),
    ("RTX 5090 FE retail box", "Just the box, no card. Great for collectors.", "reject:box_only"),
    ("Original box for RTX 4090 FE", "", "reject:box_only"),
    ("LG OLED65C4PUA", "TV not included - box and stand only", "reject:box_only"),
    ("RTX 5090 Founders Edition", "Comes with just the box and the power adapter.", "rtx_5090"),
    ("RTX 4090 FE", "No original box, only the card and the 12VHPWR adapter.", "rtx_4090"),
    ("RTX 4090 FE no original box only card", "", "rtx_4090"),
    ("NO CARD BOX ONLY RTX 4090", "", "reject:box_only"),
    # ---- damage: product vs packaging, negation, clause boundaries
    ("Broken Alienware OLED AW3225QF", "", "reject:damaged"),
    ("AW3225QF cracked screen", "", "reject:damaged"),
    ("AW3225QF - no burn-in, no dead pixels, perfect", "", "oled_4k_monitor/aw3225qf"),
    ("LG C4 65 burn-in on left side", "", "reject:damaged"),
    ("LG OLED65C4PUA, box is damaged but TV is perfect", "", "lg_oled_tv/c4_65"),
    ("RTX 3090 FE, cracked box", "", "rtx_3090"),
    ("RTX 3090 FE", "No issues, broken box though", "rtx_3090"),
    ("RTX 3090 FE not working", "", "reject:damaged"),
    ("RTX 3090 FE", "doesn't post", "reject:damaged"),
    ("RTX 4090 Suprim X", "no display output", "reject:damaged"),
    ("RTX 4090 FE", "Card has never been damaged, never mined, never overclocked.", "rtx_4090"),
    ("Samsung QN65S90D", "Screen is cracked in the corner, sold as is", "reject:damaged"),
    ("Samsung QN65S90D", "No cracks, no dead pixels, no burn in. Box was damaged in shipping.", "samsung_s90_oled_tv/s90d_65"),
    ("RTX 4090 FE", "Burn-in tested for 24h with FurMark, runs cool.", "rtx_4090"),
    ("Alienware AW3225QF", "Dead pixels: none. Burn-in: no. Always used dark mode.", "oled_4k_monitor/aw3225qf"),
    ("RTX 4090 FE", "Overheating? No. Coil whine? A little under load.", "rtx_4090"),
    ("Alienware AW3225QF", "Burn-in - no returns, sold as seen", "reject:damaged"),
    ("ASUS PG32UCDM", "Zero dead pixels and free of burn-in.", "oled_4k_monitor/pg32ucdm"),
    ("Alienware AW3225QF", "No box cracked screen", "reject:damaged"),
    ("Alienware AW3225QF", "No issues except dead pixels in the corner", "reject:damaged"),
    ("Alienware AW3225QF", "Works perfectly, no box. However it has some burn-in", "reject:damaged"),
    ("Alienware AW3225QF", "Works great, not a single dead pixel, but there is a crack on the screen", "reject:damaged"),
    ("Alienware AW3225QF", "No dead pixels, burn-in or scratches", "oled_4k_monitor/aw3225qf"),
    ("Alienware AW3225QF", "No dead pixels, cracked screen", "reject:damaged"),
    ("Alienware AW3225QF", "Has 2 dead pixels near the top edge, barely noticeable", "reject:damaged"),
    ("RTX 3090 FE", "Card powers on but no display, sold as is for parts", "reject:damaged"),
    ("RTX 3090 FE", "Repasted and repadded last month, runs cool", "rtx_3090"),
    ("RTX 3090 FE", "Fans are not spinning, sold for repair", "reject:damaged"),
    ("RTX 3090 FE", "Selling as non-working, artifacting in games", "reject:damaged"),
    ("RTX 4090 FE", "Card won't post after a BIOS update", "reject:damaged"),
    ("RTX 4090 FE", "Melted 12VHPWR connector, otherwise clean", "reject:damaged"),
    ("RTX 4090 FE", "Not a scratch on it, works great in every game", "rtx_4090"),
    ("RTX 4090 FE", "Doesn't display any artifacts, never had issues", "rtx_4090"),
    ("LG OLED65C4PUA", "No display issues, no image retention, no burn-in", "lg_oled_tv/c4_65"),
    ("RTX 3090 FE for parts", "", "reject:damaged"),
    ("Red Dead Redemption 2 bundle + RTX 3090 FE", "", "rtx_3090"),
    ("RTX 4090 FE", "Not fake, bought at Best Buy, receipt available", "rtx_4090"),
    ("Fake RTX 4090 relabeled card", "", "reject:counterfeit"),
    # ---- rentals / hosting
    ("RTX 3090 Mining Rig Rental $50/mo", "", "reject:rental"),
    ("Rent my RTX 4090 by the hour", "", "reject:rental"),
    ("RTX 5090 cloud GPU access $1.20/hr", "", "reject:rental"),
    ("RTX 4090 available for rent", "", "reject:rental"),
    ("RTX 5090 FE", "Also offering GPU hosting at $0.80/hr for AI training", "reject:rental"),
    ("RTX 4090 FE", "Selling because I need money for rent this month", "rtx_4090"),
    ("RTX 4090 FE", "Used about 2 hours a day for gaming", "rtx_4090"),
    # ---- photo-of-a-GPU bait
    ("You are buying a PHOTO of an RTX 5090", "", "reject:photo_only"),
    ("RTX 4090 picture only", "", "reject:photo_only"),
    ("Photo of RTX 5090 Founders Edition", "", "reject:photo_only"),
    ("RTX 5090 FE", "This listing is for a picture of the card, not the actual GPU.", "reject:photo_only"),
    ("RTX 4090 FE", "Stock photo only for reference, real pics on request", "rtx_4090"),
    # ---- buy / trade requests
    ("WTB RTX 4090", "", "reject:wanted"),
    ("Looking for 3090", "", "reject:wanted"),
    ("[USA-TX] [H] PayPal, Local Cash [W] RTX 4090 FE", "", "reject:wanted"),
    ("ISO: LG C4 65", "", "reject:wanted"),
    ("Buying RTX 3090s, paying cash", "", "reject:wanted"),
    ("RTX 3090 FE - trade for a PS5", "", "reject:wanted"),
    ("RTX 3090 FE trade for RX 7900 XTX", "", "reject:no_profile_match"),  # second GPU named: ambiguous
    ("RTX 4090 FE", "Open to trades, no lowballs", "rtx_4090"),
    ("[USA-CA] [H] RTX 4090 FE [W] PayPal, Local Cash", "", "rtx_4090"),
    ("RTX 4090 FE", "WTB a 5090, so this one has to go", "rtx_4090"),  # wanted is a title-only rule
    ("Sony a7 IV body, ISO 100-51200 tested", "", "sony_a7iv"),
    # ---- accessories and parts
    ("EK waterblock for RTX 4090", "", "reject:accessory"),
    ("RTX 4090 with EK waterblock installed", "", "rtx_4090"),
    ("Replacement fan for RTX 3090 FE", "", "reject:accessory"),
    ("CableMod 12VHPWR cable for RTX 4090", "", "reject:accessory"),
    ("RTX 4090 FE backplate only", "", "reject:accessory"),
    ("RTX 4090 Founders Edition shroud", "", "reject:accessory"),
    ("Wall mount for LG C4 65", "", "reject:accessory"),
    ("RTX 4090 keychain", "", "reject:accessory"),
    ("LG OLED65C4PUA main board", "", "reject:no_profile_match"),
    ("Steam Deck OLED 1TB with case and dock", "", "steam_deck_oled/tb1"),
    ("Steam Deck OLED carrying case", "", "reject:no_profile_match"),
    ("Sony a7 IV body + 2 batteries + charger", "", "sony_a7iv"),
    ("NP-FZ100 battery for Sony a7 IV", "", "reject:no_profile_match"),
    ("RTX 4090 GPU Support Bracket Anti Sag Holder", "", "reject:accessory"),
    ("12VHPWR Cable RTX 4090 16 Pin", "", "reject:accessory"),
    ("RTX 4090 Thermal Pads Replacement Kit", "", "reject:accessory"),
    ("RTX 3090 FE repasted, new thermal pads", "", "rtx_3090"),
    ("LG OLED65C4PUA Power Supply Board EAY65170001", "", "reject:accessory"),
    ("LG C4 65 remote control", "", "reject:accessory"),
    ("LG C4 65 OLED with magic remote", "", "lg_oled_tv/c4_65"),
    ("Steam Deck OLED Screen Protector 2 pack", "", "reject:accessory"),
    ("Steam Deck OLED 1TB + case + screen protector", "", "steam_deck_oled/tb1"),
    ("Corsair 1000W PSU for RTX 4090 build", "", "reject:accessory"),
    ("Monitor stand for AW3225QF", "", "reject:accessory"),
    ("Gaming PC i9-13900K RTX 4090 64GB DDR5 tempered glass case", "", "prebuilt_flagship/gpu_4090"),
    # ---- sold / pending
    ("SOLD - RTX 4090 Suprim", "", "reject:sold_marker"),
    ("RTX 4090 FE [SOLD]", "", "reject:sold_marker"),
    ("RTX 3090 FE - pending pickup", "", "reject:sold_marker"),
    ("RTX 4090 untested, sold as-is", "", "rtx_4090"),
    # ---- gift cards (a bundled gift-card promo is fine)
    ("Best Buy gift card - use toward an RTX 5090", "", "reject:gift_card"),
    ("LG C4 65 OLED + $100 Gift Card", "", "lg_oled_tv/c4_65"),
    # ---- model disambiguation
    ("Quadro RTX 6000 24GB", "", "reject:no_profile_match"),
    ("NVIDIA RTX 6000 Ada Generation 48GB", "", "rtx_6000_ada"),
    ("RTX A6000 Ada 48GB", "", "rtx_6000_ada"),
    ("PNY RTX A6000 48GB", "", "rtx_a6000"),
    ("RTX PRO 6000 Blackwell 96GB", "", "rtx_pro_6000_blackwell"),
    ("RTX A5000 24GB", "", "rtx_a5000"),
    ("RTX 5000 Ada 32GB", "", "reject:no_profile_match"),
    ("RTX 4090 Laptop GPU - Legion Pro 7i", "", "reject:no_profile_match"),
    ("Zotac RTX 4090D", "", "reject:no_profile_match"),
    ("RTX 3090 Ti FTW3", "", "rtx_3090/ti"),
    ("RTX 3090Ti Founders", "", "rtx_3090/ti"),
    ("RTX 3090 FE", "", "rtx_3090"),
    ("RTX 4080 Super", "", "reject:no_profile_match"),
    ("EVGA 3090 FTW3 Ultra", "", "rtx_3090"),
    ("[USA-CA][H] 4090 FE [W] PayPal", "", "rtx_4090"),
    ("ASUS ROG Strix RTX 4090 OC 24GB", "", "rtx_4090"),
    ("ASUS ROG Astral RTX 5090 OC", "", "rtx_5090"),
    ("RTX 4090 vs RTX 4080 Super benchmark rig", "", "reject:no_profile_match"),
    ("Sony a6000 camera with kit lens", "", "reject:no_profile_match"),
    ("Dell OptiPlex 5090 i5-10500 16GB RAM desktop", "", "reject:no_profile_match"),
    ("Brother HL-4090 laser printer", "", "reject:no_profile_match"),
    ("Anker 5090mAh power bank", "", "reject:no_profile_match"),
    ("Two RTX 3090s for an AI build", "", "rtx_3090"),
    ("RTX 4090 FE (faster than 3090)", "", "reject:no_profile_match"),  # two tracked models: ambiguous
    ("Nvidia 4090", "", "rtx_4090"),
    ("3090 24gb", "", "rtx_3090"),
    ("RTX 3090 for AI / LLM / deep learning", "", "rtx_3090"),
    ("RTX 3090 FE - for sale", "", "rtx_3090"),
    # ---- prebuilt resolution
    ("Gaming PC i9-13900K RTX 4090 64GB DDR5 2TB", "", "prebuilt_flagship/gpu_4090"),
    ("Custom build Ryzen 7 9800X3D RTX 5090 32GB DDR5", "", "prebuilt_flagship/gpu_5090"),
    ("RTX 4090 FE pulled from my gaming PC, works great", "", "rtx_4090"),
    ("Gaming PC", "Specs: Ryzen 9 7950X3D, RTX 5090 FE, 64GB DDR5, 2TB NVMe, 1200W PSU", "prebuilt_flagship/gpu_5090"),
    ("Alienware Aurora R16 Gaming Desktop RTX 4090 i9-14900KF 64GB", "", "prebuilt_flagship/gpu_4090"),
    ("Gaming PC i9-14900K RTX 4090 64GB DDR5", "Case is big enough to upgrade to a 5090 later.", "prebuilt_flagship/gpu_4090"),
    ("Gaming PC i7-12700K RTX 3080 32GB DDR4", "The PSU is upgradable to a 4090 later.", "reject:no_profile_match"),
    ("RTX 4090 FE", "Tested in my PC with an i9-13900K and 64GB DDR5, never mined.", "rtx_4090"),
    ("Gaming laptop RTX 4090 i9 32GB DDR5", "", "reject:no_profile_match"),
    # ---- TVs, monitors, camera, handheld
    ("LG OLED65C4PUA", "", "lg_oled_tv/c4_65"),
    ("LG C5 77 inch OLED evo", "", "lg_oled_tv/c5_77"),
    ('LG 65" Class C4 Series OLED evo 4K', "", "lg_oled_tv/c4_65"),
    ("LG C4 OLED TV", "", "reject:variant_unknown"),
    ("LG 65in C4 OLED", "", "lg_oled_tv/c4_65"),
    ("LG OLED55C5PUA 55 inch", "", "lg_oled_tv/c5_55"),
    ("LG G4 65 inch OLED evo", "", "lg_oled_tv/g4_65"),
    ("LG OLED42C4PUA 42 inch", "", "reject:variant_unknown"),
    ("LG C3 65 OLED", "", "reject:no_profile_match"),
    ("LG G5 phone 32GB unlocked", "", "reject:no_profile_match"),
    ("LG UltraGear OLED 32GS95UE monitor", "", "reject:no_profile_match"),
    ("Samsung QN65S90D", "", "samsung_s90_oled_tv/s90d_65"),
    ("Samsung 65 inch S90F QD-OLED", "", "samsung_s90_oled_tv/s90f_65"),
    ("Samsung S90D 55", "", "reject:variant_unknown"),
    ("ASUS ROG Swift PG32UCDM 32 4K 240Hz QD-OLED", "", "oled_4k_monitor/pg32ucdm"),
    ("Alienware AW2725Q 27 4K QD-OLED", "", "oled_4k_monitor/aw2725q"),
    ("MSI MPG 321URX QD-OLED", "", "oled_4k_monitor/mpg321urx"),
    ("Samsung Odyssey OLED G8 32 inch 4K 240Hz", "", "oled_4k_monitor/g80sd"),
    ("Samsung Odyssey OLED G8 34 ultrawide", "", "reject:variant_unknown"),
    ("Steam Deck OLED 1TB", "", "steam_deck_oled/tb1"),
    ("Steam Deck OLED 512GB", "", "steam_deck_oled/gb512"),
    ("Steam Deck OLED Limited Edition", "", "steam_deck_oled"),
    ("Steam Deck LCD 256GB", "", "reject:no_profile_match"),
    ("Sony a7 IV body", "", "sony_a7iv"),
    ("Sony a7R IV", "", "reject:no_profile_match"),
    ("Sony Alpha 7 IV ILCE-7M4 body", "", "sony_a7iv"),
    ("Sony A7IV + 28-70mm kit lens", "", "sony_a7iv"),
    ("Sony a7 III body", "", "reject:no_profile_match"),
    ("Sony a7C II", "", "reject:no_profile_match"),
    # ---- not a product we track at all
    ("Herman Miller Aeron chair size B", "", "reject:no_profile_match"),
]


@pytest.mark.parametrize(("title", "description", "expected"), OUTCOME_CASES, ids=[c[0][:48] for c in OUTCOME_CASES])
def test_shipped_rules_outcome(tf: TextFilter, title: str, description: str, expected: str) -> None:
    result = tf.evaluate(make_item(title, description))
    assert outcome(result) == expected, (result.reject_detail, result.matched_terms)


def test_reject_detail_is_the_matched_text(tf: TextFilter) -> None:
    result = tf.evaluate(make_item("ASUS TUF RTX 4090 - EMPTY BOX, no card"))
    assert result.reject_code == "box_only"
    assert result.reject_detail == "EMPTY BOX"  # original casing, not the folded copy
    assert result.profile_id == "rtx_4090" and result.category == "gpu"


def test_rejections_after_identification_keep_the_product(tf: TextFilter) -> None:
    result = tf.evaluate(make_item("AW3225QF cracked screen"))
    assert result.reject_code == "damaged"
    assert (result.profile_id, result.variant_id, result.category) == ("oled_4k_monitor", "aw3225qf", "monitor")
    assert result.product_key == "oled_4k_monitor:aw3225qf"


def test_no_profile_match_is_bare(tf: TextFilter) -> None:
    result = tf.evaluate(make_item("Herman Miller Aeron chair size B"))
    assert result.reject_code == "no_profile_match"
    assert result.profile_id is None and result.category is None and result.product_key is None


def test_variant_unknown_reports_profile(tf: TextFilter) -> None:
    result = tf.evaluate(make_item("LG C4 OLED TV"))
    assert result.reject_code == "variant_unknown"
    assert result.profile_id == "lg_oled_tv" and result.category == "tv" and result.variant_id is None
    assert result.match_strength == pytest.approx(0.85)


# ============================================================================ shipped rules: risk signals


@pytest.mark.parametrize(
    ("title", "description", "kind", "present", "absent"),
    [
        ("never mined RTX 3090, gaming only", "", SourceKind.LOCAL, set(), {"mining_history"}),
        ("RTX 3090 used for mining, still works", "", SourceKind.LOCAL, {"mining_history"}, set()),
        ("RTX 3090 FE", "Mining: never. Gaming only.", SourceKind.LOCAL, set(), {"mining_history"}),
        ("RTX 3090 FE", "Card has not been used for mining", SourceKind.LOCAL, set(), {"mining_history"}),
        ("RTX 3090 FE", "Ex-miner card, repasted", SourceKind.LOCAL, {"mining_history"}, set()),
        ("RTX 4090 Zelle only, shipping only", "", SourceKind.LOCAL, {"payment_only_red_flag", "scam_story"}, set()),
        ("RTX 4090 untested, sold as-is", "", SourceKind.LOCAL, {"untested_as_is"}, set()),
        ("RTX 4090 tested and working", "", SourceKind.LOCAL, set(), {"untested_as_is"}),
        ("RTX 4090 FE", "Not untested - fully tested and working", SourceKind.LOCAL, set(), {"untested_as_is"}),
        ("RTX 4090 FE", "No returns, untested", SourceKind.LOCAL, {"untested_as_is"}, set()),
        ("RTX 4090 FE", "Text me at 555-123-4567 or on WhatsApp", SourceKind.LOCAL, {"off_platform_contact", "phone_number"}, set()),
        ("RTX 4090 FE", "Email seller.gpu@gmail.com for details", SourceKind.LOCAL, {"off_platform_contact"}, set()),
        ("RTX 4090 FE", "Serial 4090-1234 on the sticker, 24GB GDDR6X", SourceKind.LOCAL, set(), {"phone_number"}),
        ("RTX 4090 FE", "PayPal F&F only please, no G&S", SourceKind.LOCAL, {"payment_only_red_flag"}, set()),
        ("RTX 4090 FE", "PayPal G&S or local cash. Buyer pays shipping.", SourceKind.LOCAL, set(), {"payment_only_red_flag"}),
        ("RTX 4090 FE", "No deposits, no holds, cash at pickup", SourceKind.LOCAL, set(), {"deposit_request"}),
        ("RTX 4090 FE", "A $200 deposit is required to hold it for you", SourceKind.LOCAL, {"deposit_request"}, set()),
        ("RTX 4090 FE", "I am deployed overseas, my shipping agent will deliver it", SourceKind.LOCAL, {"scam_story"}, set()),
        ("RTX 4090 FE", "Please read the full description before messaging", SourceKind.LOCAL, {"read_description"}, set()),
        ("RTX 4090 FE", "Must sell today, moving!", SourceKind.LOCAL, {"urgency"}, set()),
        ("RTX 4090 FE", "Card was repaired (replaced power connector) by a pro", SourceKind.LOCAL, {"repaired_modified"}, set()),
        ("RTX 4090 FE", "Never repaired, reflowed or modified", SourceKind.LOCAL, set(), {"repaired_modified"}),
        ("RTX 4090 FE", "Never repaired. Reflowed once by me", SourceKind.LOCAL, {"repaired_modified"}, set()),
        ("RTX 4090 FE", "Clean card from a smoke-free home", SourceKind.LOCAL, set(), {"untested_as_is", "mining_history", "scam_story"}),
    ],
)
def test_shipped_risk_signals(
    tf: TextFilter, title: str, description: str, kind: SourceKind, present: set[str], absent: set[str]
) -> None:
    result = tf.evaluate(make_item(title, description, kind=kind))
    assert result.accepted, (result.reject_code, result.reject_detail)
    assert present <= codes(result), codes(result)
    assert not (absent & codes(result)), codes(result)


def test_risk_signal_fields_come_from_the_group(tf: TextFilter, config: AppConfig) -> None:
    result = tf.evaluate(make_item("RTX 4090 Zelle only, shipping only"))
    by_code = {s.code: s for s in result.risk_signals}
    flag = by_code["payment_only_red_flag"]
    assert flag.probability == config.filters.rules["payment_only_red_flag"].probability
    assert flag.origin == "text" and flag.detail == "Zelle only"
    assert by_code["scam_story"].detail == "shipping only"


def test_source_kind_scoping(tf: TextFilter) -> None:
    text = ("RTX 4090 FE", "Zelle accepted. Shipping only, no local pickup.")
    local = tf.evaluate(make_item(*text, kind=SourceKind.LOCAL))
    ebay = tf.evaluate(make_item(*text, kind=SourceKind.MARKETPLACE))
    retail = tf.evaluate(make_item(*text, kind=SourceKind.RETAIL, condition=Condition.NEW))
    assert {"payment_mention", "scam_story"} <= codes(local)
    assert "payment_mention" not in codes(ebay) and "scam_story" in codes(ebay)
    assert not ({"payment_mention", "scam_story"} & codes(retail))

    returns = ("RTX 4090 FE", "No returns accepted.")
    assert "no_returns" in codes(tf.evaluate(make_item(*returns, kind=SourceKind.MARKETPLACE)))
    assert "no_returns" not in codes(tf.evaluate(make_item(*returns, kind=SourceKind.LOCAL)))


def test_category_scoping(tf: TextFilter) -> None:
    # mining_history only applies to gpu / workstation_gpu
    tv = tf.evaluate(make_item("LG OLED65C4PUA", "Used as a mining dashboard display", kind=SourceKind.MARKETPLACE))
    gpu = tf.evaluate(make_item("RTX A6000 48GB", "Used as a mining card for a year", kind=SourceKind.MARKETPLACE))
    assert tv.accepted and "mining_history" not in codes(tv)
    assert gpu.profile_id == "rtx_a6000" and "mining_history" in codes(gpu)


# ============================================================================ title / price mismatch


@pytest.mark.parametrize(
    ("title", "price", "flagged"),
    [
        ("RTX 4090 $1500", 150.0, True),
        ("RTX 4090 FE - $1500", 1450.0, False),
        ("RTX 4090 FE - $1500", 3000.0, False),  # ratio exactly 2.0 is inside the band
        ("RTX 4090 FE - $1500", 3100.0, True),
        ("RTX 4090 FE - $1500", 750.0, False),  # ratio exactly 0.5
        ("RTX 4090 FE - $1500", 749.0, True),
        ("RTX 4090 FE", 150.0, False),  # no price in the title
        ("RTX 4090 FE - $1199 ($1599 - $400)", 1199.0, False),
    ],
)
def test_title_price_mismatch(tf: TextFilter, config: AppConfig, title: str, price: float, flagged: bool) -> None:
    result = tf.evaluate(make_item(title, price=price))
    assert result.accepted
    signal = next((s for s in result.risk_signals if s.code == "title_price_mismatch"), None)
    assert (signal is not None) is flagged
    if signal is not None:
        assert signal.origin == "listing"
        assert signal.probability == config.scoring.risk_probabilities["title_price_mismatch"]


# ============================================================================ conditions, title length


def test_for_parts_condition_rejected_after_identification(tf: TextFilter) -> None:
    result = tf.evaluate(make_item("RTX 4090 Founders Edition", condition=Condition.FOR_PARTS, kind=SourceKind.MARKETPLACE))
    assert outcome(result) == "reject:for_parts"
    assert result.profile_id == "rtx_4090"
    unknown = tf.evaluate(make_item("Herman Miller Aeron chair", condition=Condition.FOR_PARTS))
    assert outcome(unknown) == "reject:no_profile_match"


@pytest.mark.parametrize("condition", [Condition.NEW, Condition.OPEN_BOX, Condition.REFURBISHED, Condition.USED, Condition.UNKNOWN])
def test_default_conditions_accepted(tf: TextFilter, condition: Condition) -> None:
    assert tf.evaluate(make_item("RTX 4090 Founders Edition", condition=condition)).accepted


@pytest.mark.parametrize("title", ["4090", "  RTX  ", ""])
def test_title_too_short(tf: TextFilter, title: str) -> None:
    result = tf.evaluate(make_item(title, "RTX 4090 FE, great card, never mined"))
    assert outcome(result) == "reject:title_too_short"


# ============================================================================ result fields


def test_match_strength_rules(tf: TextFilter) -> None:
    assert tf.evaluate(make_item("RTX 4090 Founders Edition")).match_strength == 1.0
    assert tf.evaluate(make_item("RTX 3090 Ti FTW3")).match_strength == 1.0
    # rtx_3090 has variants, none matched
    assert tf.evaluate(make_item("RTX 3090 FE")).match_strength == pytest.approx(0.85)
    # rtx_4090 matched too, but at a lower priority than the prebuilt profile
    assert tf.evaluate(make_item("Gaming PC i9-13900K RTX 4090 64GB DDR5")).match_strength == 1.0
    # two priority-0 profiles: config order wins, strength drops
    both = tf.evaluate(make_item("LG OLED65C4PUA and Samsung QN65S90D bundle"))
    assert (both.profile_id, both.variant_id) == ("lg_oled_tv", "c4_65")
    assert both.match_strength == pytest.approx(0.9)


def test_matched_terms_and_timing(tf: TextFilter) -> None:
    result = tf.evaluate(make_item("ASUS ROG Swift PG32UCDM 32 4K 240Hz QD-OLED"))
    assert "PG32UCDM" in result.matched_terms
    assert result.elapsed_us > 0
    prebuilt = tf.evaluate(make_item("Custom build Ryzen 7 9800X3D RTX 5090 32GB DDR5"))
    assert {"RTX 5090", "Custom build", "Ryzen 7 9800X3D"} <= set(prebuilt.matched_terms)


def test_evaluate_is_pure_and_deterministic(tf: TextFilter) -> None:
    item = make_item("RTX 4090 Zelle only, shipping only", "Text me at 555-123-4567. Never mined.", price=150.0)
    before = item.model_dump()
    first = tf.evaluate(item).model_dump(exclude={"elapsed_us"})
    second = tf.evaluate(item).model_dump(exclude={"elapsed_us"})
    assert first == second
    assert item.model_dump() == before


def test_description_is_part_of_text_rules_but_not_title_rules(tf: TextFilter) -> None:
    # box_only looks at title + description, sold_marker only at the title
    assert outcome(tf.evaluate(make_item("RTX 4090 FE", "box only"))) == "reject:box_only"
    assert outcome(tf.evaluate(make_item("RTX 4090 FE", "My last one SOLD"))) == "rtx_4090"


def test_shipped_config_shape(config: AppConfig, tf: TextFilter) -> None:
    assert len(config.profiles) == 13
    assert tf.rule_names == list(config.filters.rules)
    assert {"box_only", "damaged", "rental", "photo_only", "wanted", "accessory", "sold_marker"} <= set(tf.rule_names)
    for name, group in config.filters.rules.items():
        assert group.patterns, name


# ============================================================================ engine semantics (custom configs)

_BAND = {"reference_new": 200.0, "floor": 50.0, "target": 150.0, "ceiling": 250.0}


def custom_config(profiles: list[dict[str, Any]], rules: dict[str, Any] | None = None, **filters: Any) -> AppConfig:
    full_profiles = []
    for p in profiles:
        merged = {"name": p["id"], "category": "gadget", "price": _BAND, **p}
        full_profiles.append(merged)
    return AppConfig.model_validate({"filters": {"rules": rules or {}, **filters}, "profiles": full_profiles})


def test_priority_and_config_order() -> None:
    cfg = custom_config(
        [
            {"id": "low", "match": {"any": [r"\bwidget\b"]}},
            {"id": "high", "priority": 5, "match": {"any": [r"\bwidget\b"]}},
            {"id": "also_high", "priority": 5, "match": {"any": [r"\bwidget\b"]}},
        ]
    )
    result = TextFilter(cfg).evaluate(make_item("Blue widget for sale"))
    assert result.profile_id == "high"
    assert result.match_strength == pytest.approx(0.9)  # also_high tied


def test_variant_required_falls_back_to_next_profile() -> None:
    profiles = [
        {"id": "pro", "priority": 5, "variant_required": True, "match": {"any": [r"\bwidget\b"]},
         "variants": [{"id": "max", "match": [r"\bmax\b"]}]},
        {"id": "plain", "match": {"any": [r"\bwidget\b"]}},
    ]
    tf = TextFilter(custom_config(profiles))
    assert outcome(tf.evaluate(make_item("Widget max edition"))) == "pro/max"
    fallback = tf.evaluate(make_item("Widget standard edition"))
    assert outcome(fallback) == "plain"
    assert fallback.match_strength == 1.0  # different priorities
    only_pro = TextFilter(custom_config(profiles[:1]))
    assert outcome(only_pro.evaluate(make_item("Widget standard edition"))) == "reject:variant_unknown"


def test_match_all_none_field_and_disabled_profiles() -> None:
    profiles = [
        {"id": "off", "enabled": False, "priority": 9, "match": {"any": [r"\bwidget\b"]}},
        {"id": "needs_both", "match": {"all": [r"\bwidget\b", r"\bturbo\b"], "none": [r"\bbroken\s+english\b"]}},
        {"id": "desc", "match": {"field": "text", "any": [r"\bgizmo\b"]}},
    ]
    tf = TextFilter(custom_config(profiles))
    assert outcome(tf.evaluate(make_item("Widget turbo kit"))) == "needs_both"
    assert outcome(tf.evaluate(make_item("Widget only kit"))) == "reject:no_profile_match"
    assert outcome(tf.evaluate(make_item("Widget turbo, broken english manual"))) == "reject:no_profile_match"
    assert outcome(tf.evaluate(make_item("Mystery bundle", "includes a gizmo"))) == "desc"
    assert outcome(tf.evaluate(make_item("Mystery bundle", "includes a widget"))) == "reject:no_profile_match"


def test_condition_not_allowed() -> None:
    tf = TextFilter(custom_config([{"id": "w", "match": {"any": [r"\bwidget\b"]}, "conditions": ["new"]}]))
    assert tf.evaluate(make_item("Widget sealed", condition=Condition.NEW)).accepted
    used = tf.evaluate(make_item("Widget sealed", condition=Condition.USED))
    assert (used.reject_code, used.reject_detail) == ("condition_not_allowed", "used")
    assert outcome(tf.evaluate(make_item("Widget sealed", condition=Condition.FOR_PARTS))) == "reject:for_parts"


def test_first_reject_group_in_config_order_wins() -> None:
    rules = {
        "first": {"action": "reject", "patterns": [r"\bzzz\b"]},
        "second": {"action": "reject", "patterns": [r"\baaa\b"]},
    }
    tf = TextFilter(custom_config([{"id": "w", "match": {"any": [r"\bwidget\b"]}}], rules))
    # "aaa" occurs earlier in the text, but group order decides
    assert tf.evaluate(make_item("Widget aaa then zzz")).reject_code == "first"


def test_group_scoping_by_category_kind_and_field() -> None:
    rules = {
        "gadget_only": {"action": "risk", "probability": 0.2, "categories": ["gadget"], "patterns": [r"\bflag\b"]},
        "other_only": {"action": "risk", "probability": 0.2, "categories": ["other"], "patterns": [r"\bflag\b"]},
        "ebay_only": {"action": "risk", "probability": 0.2, "source_kinds": ["marketplace"], "patterns": [r"\bflag\b"]},
        "title_only": {"action": "risk", "probability": 0.2, "field": "title", "patterns": [r"\bflag\b"]},
    }
    tf = TextFilter(custom_config([{"id": "w", "match": {"any": [r"\bwidget\b"]}}], rules))
    local_desc = tf.evaluate(make_item("Widget", "flag", kind=SourceKind.LOCAL))
    assert codes(local_desc) == {"gadget_only"}
    ebay_title = tf.evaluate(make_item("Widget flag", kind=SourceKind.MARKETPLACE))
    assert codes(ebay_title) == {"gadget_only", "ebay_only", "title_only"}


def _negation_filter(window: int = 3, terms: list[str] | None = None) -> TextFilter:
    rules = {
        "defect": {"action": "reject", "negatable": True, "patterns": [r"\bscratch(?:es|ed)?\b", r"\bdent\b"]},
        "plain": {"action": "risk", "probability": 0.3, "patterns": [r"\bnot\s+boxed\b"]},
    }
    extra: dict[str, Any] = {"negation_window_words": window}
    if terms is not None:
        extra["negation_terms"] = terms
    return TextFilter(custom_config([{"id": "w", "match": {"any": [r"\bwidget\b"]}}], rules, **extra))


@pytest.mark.parametrize(
    ("description", "rejected"),
    [
        ("no scratches", False),
        ("NO SCRATCHES AT ALL", False),
        ("there are no visible scratches", False),  # term within 3 words
        ("not a single visible light scratch", True),  # term 4 words back, window is 3
        ("no box, scratches on the side", True),  # comma ends the clause
        ("no box. scratches on the side", True),
        ("no issues but scratches on the side", True),
        ("no issues; however scratches on the side", True),
        ("no box scratched case", True),  # 'box' closes the negation scope
        ("no returns dent on top", True),  # 'returns' closes the negation scope
        ("free of scratches", False),  # multi-word term
        ("totally free of any scratches", False),
        ("Scratches: none", False),  # form-style answer
        ("dent: no", False),
        ("Dent - none", False),
        ("scratches? none visible", False),
        ("dent - no returns", True),  # 'no' governs 'returns', not the dent
        ("dent-free? no", True),  # i.e. it HAS a dent: the answer does not follow the match
        ("PCIe 4.0 x16 slot scratched", True),  # "4.0" is one token: no stray "0" term
        ("0 scratches", False),
        ("isn't scratched", False),
        ("isn’t scratched", False),  # typographic apostrophe
        ("w/o scratches", False),
        ("no scratches, dent or chips", False),  # negation distributes over a list
        ("no scratches or dent", False),
        ("no scratches, dent, chips", False),  # the list goes on after 'dent'
        ("no scratches, dent on top", True),  # a bare comma without a continuing list
        ("no scratches. dent or two", True),  # sentence boundary ends the list
    ],
)
def test_negation_mechanics(description: str, rejected: bool) -> None:
    terms = ["no", "not", "never", "free of", "none", "0", "isn't", "w/o"]
    result = _negation_filter(window=3, terms=terms).evaluate(make_item("Widget deluxe", description))
    assert (result.reject_code == "defect") is rejected, result.reject_detail


def test_negation_window_size_and_non_negatable_groups() -> None:
    wide = _negation_filter(window=5).evaluate(make_item("Widget deluxe", "not a single visible light scratch"))
    assert wide.accepted
    zero = _negation_filter(window=0).evaluate(make_item("Widget deluxe", "no scratches"))
    assert zero.reject_code == "defect"
    # "not boxed" lives in a non-negatable group: the leading "not" is part of the match
    plain = _negation_filter().evaluate(make_item("Widget deluxe", "never not boxed"))
    assert "plain" in codes(plain)


def test_a_later_unnegated_match_still_counts() -> None:
    result = _negation_filter().evaluate(make_item("Widget deluxe", "no scratches on the front. dent on the back"))
    assert (result.reject_code, result.reject_detail) == ("defect", "dent")


def test_unjoinable_and_case_sensitive_patterns() -> None:
    rules = {
        # numbered back-reference (doubled word) cannot be joined with siblings
        "stutter": {"action": "risk", "probability": 0.1, "patterns": [r"\b(\w+)\s+\1\b", r"\bzz+\b"]},
        # scoped case-sensitivity must survive the case-folding optimisation
        "shouting_sku": {"action": "risk", "probability": 0.1, "patterns": [r"\b(?-i:SKU)\d+\b"]},
        "named": {"action": "risk", "probability": 0.1, "patterns": [r"\b(?P<w>qq)\s+(?P=w)\b"]},
    }
    tf = TextFilter(custom_config([{"id": "w", "match": {"any": [r"\bwidget\b"]}}], rules))
    assert "stutter" in codes(tf.evaluate(make_item("Widget the the best")))
    assert "stutter" in codes(tf.evaluate(make_item("Widget zzz")))
    assert "stutter" not in codes(tf.evaluate(make_item("Widget the best")))
    assert "shouting_sku" in codes(tf.evaluate(make_item("Widget SKU123")))
    assert "shouting_sku" not in codes(tf.evaluate(make_item("Widget sku123")))
    assert "named" in codes(tf.evaluate(make_item("Widget qq QQ")))


def test_empty_config_matches_nothing() -> None:
    tf = TextFilter(AppConfig())
    assert outcome(tf.evaluate(make_item("RTX 4090 Founders Edition"))) == "reject:no_profile_match"


def test_non_ascii_text(tf: TextFilter) -> None:
    result = tf.evaluate(make_item("🔥 RTX 4090 FE 🔥", "🚨BOX ONLY🚨 – read description"))
    assert outcome(result) == "reject:box_only"
    accented = tf.evaluate(make_item("RTX 4090 FE — état neuf", "Aucun problème, never mined"))
    assert outcome(accented) == "rtx_4090" and "mining_history" not in codes(accented)


# ============================================================================ the anchor gate never changes a result

_FUZZ_WORDS = [
    "no", "not", "never", "box", "only", "empty", "just", "the", "card", "gpu", "rtx", "4090", "3090", "5090",
    "broken", "cracked", "dead", "pixels", "burn-in", "artifacts", "working", "doesn't", "post", "display",
    "rent", "rental", "for", "lease", "$50/mo", "$1.20/hr", "per", "hour", "cloud", "photo", "picture",
    "wtb", "iso", "looking", "sold", "pending", "zelle", "venmo", "only,", "shipping", "deposit", "hold",
    "whatsapp", "text", "me", "at", "555-123-4567", "untested", "as-is", "mined", "mining", "repaired",
    "lg", "oled", "c4", "65", "samsung", "s90d", "aw3225qf", "pg32ucdm", "steam", "deck", "1tb", "sony",
    "a7", "iv", "gaming", "pc", "i9-13900k", "64gb", "ddr5", "laptop", "fe", "founders", "ti", "waterblock",
    "fan", "replacement", "gift", "card", "pulled", "from", "ada", "a6000", "pro", "6000", "blackwell",
    ",", ".", ";", "but", "however", "free", "of", ":", "none", "0", "w/o", "\n", "🔥", "Ünïcödé",
]


def _fuzz_corpus(n: int) -> list[DealItem]:
    rng = random.Random(1337)
    items: list[DealItem] = []
    kinds = list(SourceKind)
    for i in range(n):
        title = " ".join(rng.choice(_FUZZ_WORDS) for _ in range(rng.randint(3, 9)))
        desc = " ".join(rng.choice(_FUZZ_WORDS) for _ in range(rng.randint(0, 60)))
        if rng.random() < 0.3:
            title = title.upper()
        items.append(make_item(title, desc, kind=kinds[i % len(kinds)], price=rng.choice([50.0, 900.0, 1500.0])))
    return items


def test_anchor_gate_is_exact(config: AppConfig, tf: TextFilter) -> None:
    ungated = TextFilter(config, literal_gate=False)
    corpus = [make_item(t, d) for t, d, _ in OUTCOME_CASES] + _fuzz_corpus(1500)
    for item in corpus:
        gated = tf.evaluate(item).model_dump(exclude={"elapsed_us"})
        plain = ungated.evaluate(item).model_dump(exclude={"elapsed_us"})
        assert gated == plain, (item.title, item.description)


_TRICKY_PATTERNS = [
    r"\bfoo(?:bar\d|baz\d)",  # incomplete node right after a word-char literal
    r"(?<![\w.$-])a6000\b",  # look-behind proving a word start
    r"^\W*rent\b",
    r"\$\s*\d[\d,.]*(?:\s*/\s*|\s+(?:per|an?)\s+)(?:hr|day)\b",
    r"^(?:[\w-]+\s+){0,2}battery\b",
    r"\bcan['’]?t\s+test\b",
    r"\be-?gift\s*cards?",
    r"(?<=\d)x3d\b",
    r"\b(?:no|zero)\s+(?:dead|stuck)\s+pixels?\b",
    r"[\[(]\s*sold\s*[\])]",
    r"\bw/o\b",
    r"x{2,}y",
    r"(?:ab|cd)+ef",
    r"\d(?<!\d\d)\d{2}-\d{4}",
    r"(?=.*\bqq\b)(?=.*\b55(?:\s*-?\s*in)?(?!\w))",
    r"\b(?:x|y)\s+(?:z\s+)?end\b",
    r"(?i:mixed)case",
]
_TRICKY_FRAGMENTS = [
    "foobar1", "foobaz2", "foo bar1", "xa6000", "a6000", "-a6000", "rent", "  rent", "_rent", "$5/hr", "$5 / hr",
    "$5 per day", "$5 a day", "$5hr", "battery", "aa battery", "aa bb battery", "aa bb cc battery", "can't test",
    "cant test", "can’t  test", "egift card", "e-gift cards", "e gift card", "9800x3d", "x3d", "no dead pixel",
    "zero  stuck pixels", "(sold)", "[ sold ]", "w/o", "w / o", "xxy", "xy", "abcdabef", "ef", "555-1234", "5555-1234",
    "qq", "55", "55in", "55 in", "550", "x end", "y z end", "xend", "MIXEDcase", "mixedCASE", "\n", ",", ".", "-",
    "🔥", "Ü", "widget",
]


def test_anchor_gate_is_exact_on_tricky_patterns() -> None:
    rules = {f"r{i}": {"action": "risk", "probability": 0.1, "patterns": [p]} for i, p in enumerate(_TRICKY_PATTERNS)}
    cfg = custom_config([{"id": "w", "match": {"field": "text", "any": [r"\bwidget\b"]}}], rules)
    gated, ungated = TextFilter(cfg), TextFilter(cfg, literal_gate=False)
    rng = random.Random(7)
    hits = 0
    for _ in range(3000):
        parts = [rng.choice(_TRICKY_FRAGMENTS) for _ in range(rng.randint(1, 8))]
        joiner = rng.choice([" ", "", "  ", "\n", " - "])
        desc = joiner.join(parts).replace("\\n", "\n")
        item = make_item("widget listing", desc)
        a = gated.evaluate(item).model_dump(exclude={"elapsed_us"})
        b = ungated.evaluate(item).model_dump(exclude={"elapsed_us"})
        assert a == b, desc
        hits += len(a["risk_signals"])
    assert hits > 1000  # the corpus really exercises the patterns


# ============================================================================ performance


def _perf_corpus() -> list[DealItem]:
    fb_desc = (
        "Selling my card because I upgraded. Works great in games, no issues, never overclocked. "
        "Comes with original box and all accessories. Local pickup preferred, can ship at buyer's cost. "
        "Cash or PayPal G&S. Smoke free home, no trades."
    )
    hws_post = (
        "Timestamps: https://imgur.com/a/abc123\n\nRTX 4090 FE - $1450 shipped\nBought at launch, used for gaming "
        "only, never mined. Repasted last year, temps are great. Comes with the original box and adapter.\n"
        "Local to 78701 for cash. Comment before PM, no chats please."
    ) * 2
    slickdeals = (
        "Best Buy has the LG 65\" C4 OLED evo for $1,299.99 with free shipping. Price matched at Amazon. "
        "Thanks to community member for finding this deal. " * 6
    )
    rows = [
        ("RTX 4090 Founders Edition", "", SourceKind.MARKETPLACE),
        ("ASUS TUF Gaming GeForce RTX 4090 OC 24GB", "", SourceKind.MARKETPLACE),
        ("RTX 4090 FE", fb_desc, SourceKind.LOCAL),
        ("[USA-TX] [H] RTX 4090 FE [W] PayPal, Local Cash", hws_post, SourceKind.LOCAL),
        ("LG 65\" C4 OLED evo 4K TV $1299.99", slickdeals, SourceKind.AGGREGATOR),
        ("Gaming PC i9-13900K RTX 4090 64GB DDR5 2TB", fb_desc, SourceKind.LOCAL),
        ("Alienware AW3225QF", "No burn-in, no dead pixels. " + fb_desc, SourceKind.LOCAL),
        ("RTX 4090 Box Only", "", SourceKind.MARKETPLACE),
        ("Herman Miller Aeron chair", fb_desc, SourceKind.LOCAL),
        ("iPhone 15 Pro 256GB unlocked", "", SourceKind.MARKETPLACE),
        ("Samsung QN65S90D", "Screen is cracked. " + fb_desc, SourceKind.LOCAL),
        ("Steam Deck OLED 1TB", "Barely used, with case.", SourceKind.LOCAL),
        ("Sony a7 IV body", "Shutter count 4k, comes with 2 batteries.", SourceKind.MARKETPLACE),
        ("RTX 5090 cloud GPU access $1.20/hr", "", SourceKind.LOCAL),
        ("NVIDIA RTX 6000 Ada Generation 48GB", "", SourceKind.MARKETPLACE),
    ]
    return [make_item(t, d, kind=k) for t, d, k in rows]


def test_performance_budget(tf: TextFilter) -> None:
    corpus = _perf_corpus()
    for item in corpus:  # warm-up
        tf.evaluate(item)
    n = 5000
    started = time.perf_counter()
    for i in range(n):
        tf.evaluate(corpus[i % len(corpus)])
    avg_us = (time.perf_counter() - started) / n * 1e6
    assert avg_us < 300.0, f"average {avg_us:.1f} us per evaluation"


def test_long_description_stays_within_budget(tf: TextFilter) -> None:
    # normalizer caps descriptions at 5000 chars; even at the cap we stay well under 0.5 ms
    filler = "Selling my card because I upgraded, works great in games, smoke free home. "
    item = make_item("RTX 4090 FE", (filler * 70)[:5000])
    tf.evaluate(item)
    started = time.perf_counter()
    for _ in range(200):
        tf.evaluate(item)
    avg_us = (time.perf_counter() - started) / 200 * 1e6
    assert avg_us < 1500.0, f"average {avg_us:.1f} us for a 5000-char description"
