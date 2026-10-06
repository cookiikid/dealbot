"""Tests for engine/anomaly.py: robust price history + explainable anomaly scoring."""

from __future__ import annotations

import math
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from deal_radar.config_schema import AppConfig, PriceBand, load_config
from deal_radar.engine.anomaly import (
    REQUIRED_COMPONENTS,
    AnomalyScorer,
    HistoryStats,
    PriceHistory,
    history_key,
    weighted_quantiles,
)
from deal_radar.engine.types import (
    Condition,
    DealItem,
    FilterResult,
    RiskSignal,
    ScoreResult,
    SellerInfo,
    Severity,
    SourceKind,
    VisionResult,
    VisionVerdict,
)

CONFIG_PATH = Path(__file__).parents[1] / "config.yaml"
NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
DAY = timedelta(days=1)

KIND_BY_SOURCE = {
    "retail": SourceKind.RETAIL,
    "ebay": SourceKind.MARKETPLACE,
    "slickdeals": SourceKind.AGGREGATOR,
    "reddit": SourceKind.AGGREGATOR,
    "fb_marketplace": SourceKind.LOCAL,
    "offerup": SourceKind.LOCAL,
    "craigslist": SourceKind.LOCAL,
}


# --------------------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def base_config() -> AppConfig:
    return load_config(CONFIG_PATH, env={})


@pytest.fixture
def config(base_config: AppConfig) -> AppConfig:
    return base_config.model_copy(deep=True)  # tests may mutate profiles


@pytest.fixture
def scorer(config: AppConfig) -> AnomalyScorer:
    return AnomalyScorer(config)


def make_item(
    price: float,
    *,
    source: str = "fb_marketplace",
    condition: Condition = Condition.USED,
    source_id: str = "1001",
    shipping: float | None = None,
    images: bool = True,
    in_stock: bool | None = None,
    seller: SellerInfo | None = None,
    received_at: datetime = NOW,
    title: str = "NVIDIA GeForce RTX 4090 Founders Edition 24GB",
) -> DealItem:
    return DealItem(
        source=source,
        source_kind=KIND_BY_SOURCE.get(source, SourceKind.LOCAL),
        source_id=source_id,
        url=f"https://example.com/{source}/{source_id}",
        title=title,
        price=price,
        shipping=shipping,
        total_price=round(price + (shipping or 0.0), 2),
        condition=condition,
        image_urls=["https://img.example.com/1.jpg"] if images else [],
        in_stock=in_stock,
        seller=seller,
        received_at=received_at,
    )


def make_fr(
    profile_id: str = "rtx_4090",
    *,
    variant_id: str | None = None,
    match_strength: float = 1.0,
    risk_signals: list[RiskSignal] | None = None,
    accepted: bool = True,
    reject_code: str | None = None,
) -> FilterResult:
    return FilterResult(
        accepted=accepted,
        profile_id=profile_id,
        variant_id=variant_id,
        category="gpu",
        match_strength=match_strength,
        reject_code=reject_code,
        risk_signals=risk_signals or [],
        matched_terms=["rtx 4090"],
    )


def seed_history(
    scorer: AnomalyScorer,
    product_key: str,
    market_class: str,
    prices: list[float],
    *,
    source: str = "fb_marketplace",
    spacing: timedelta = timedelta(hours=12),
    prefix: str = "h",
) -> None:
    key = history_key(product_key, market_class)
    for i, price in enumerate(prices):
        scorer.history.observe(key, f"{source}:{prefix}{i}", price, NOW - spacing * i, source)


def around(center: float, n: int, spread: float = 100.0) -> list[float]:
    """Deterministic prices spread evenly over [center - spread, center + spread]."""
    return [center - spread + (2 * spread) * ((i * 7) % n) / (n - 1) for i in range(n)]


def new_history(**kw: float) -> PriceHistory:
    params: dict = {"half_life_days": 14.0, "window_days": 60.0, "max_samples": 400}
    params.update(kw)
    return PriceHistory(**params)


# --------------------------------------------------------------------------- weighted statistics


def test_weighted_quantiles_equal_weights_match_hazen() -> None:
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    q1, median, q3 = weighted_quantiles(values, [1.0] * 5, (0.25, 0.5, 0.75))
    assert median == pytest.approx(3.0)
    assert q1 == pytest.approx(1.75)
    assert q3 == pytest.approx(4.25)
    # even count: median interpolates between the two middle order statistics
    assert weighted_quantiles([10.0, 20.0, 30.0, 40.0], [1.0] * 4, (0.5,))[0] == pytest.approx(25.0)


def test_weighted_quantiles_interpolate_toward_heavier_sample() -> None:
    # positions: 100 -> 1.5/4 = 0.375, 200 -> 3.5/4 = 0.875 ; q=0.5 -> 25% of the way
    assert weighted_quantiles([100.0, 200.0], [3.0, 1.0], (0.5,))[0] == pytest.approx(125.0)
    assert weighted_quantiles([100.0, 200.0], [1.0, 3.0], (0.5,))[0] == pytest.approx(175.0)
    # extremes clamp to the min/max order statistics
    assert weighted_quantiles([100.0, 200.0], [1.0, 1.0], (0.0, 1.0)) == [100.0, 200.0]
    assert weighted_quantiles([7.0], [0.3], (0.1, 0.5, 0.9)) == [7.0, 7.0, 7.0]
    with pytest.raises(ValueError):
        weighted_quantiles([], [], (0.5,))


def test_stats_are_robust_to_scam_outliers() -> None:
    history = new_history()
    key = history_key("rtx_4090", "used")
    for i, price in enumerate(around(1700, 20)):
        history.observe(key, f"fb:{i}", price, NOW - timedelta(hours=i), "fb_marketplace")
    for i, price in enumerate([150.0, 200.0, 9999.0]):  # box-only bait + "don't lowball me" anchor
        history.observe(key, f"scam:{i}", price, NOW, "fb_marketplace")
    stats = history.stats(key, NOW)
    assert stats is not None and stats.n == 23
    assert 1650 <= stats.median <= 1750
    assert stats.mad < 120
    assert stats.min == 150.0 and stats.max == 9999.0
    assert stats.q1 <= stats.median <= stats.q3
    assert stats.iqr == pytest.approx(stats.q3 - stats.q1)


def test_kish_effective_sample_size_and_source_weights() -> None:
    history = new_history(source_weights={"retail": 1.0, "craigslist": 0.5})
    key = "k|used"
    history.observe(key, "retail:1", 100.0, NOW, "retail")
    history.observe(key, "craigslist:1", 200.0, NOW, "craigslist")
    stats = history.stats(key, NOW)
    assert stats is not None
    # weights 1.0 and 0.5 -> (1.5)^2 / 1.25 = 1.8
    assert stats.n == 2 and stats.n_eff == pytest.approx(1.8)
    # weighted median leans toward the trusted retail price: positions 1/3 and 5/6
    assert stats.median == pytest.approx(100.0 + 100.0 * (0.5 - 1 / 3) / (5 / 6 - 1 / 3))
    assert history.source_weight("unknown_source") == pytest.approx(0.7)


# --------------------------------------------------------------------------- price history


def test_history_replaces_sample_per_listing() -> None:
    history = new_history()
    key = history_key("rtx_4090", "used")
    assert history.observe(key, "fb_marketplace:42", 1500.0, NOW - DAY, "fb_marketplace")
    assert history.observe(key, "fb_marketplace:42", 1400.0, NOW, "fb_marketplace")
    assert len(history) == 1
    stats = history.stats(key, NOW)
    assert stats is not None and stats.n == 1 and stats.median == 1400.0
    # an out-of-order (older) update never overwrites newer knowledge
    assert not history.observe(key, "fb_marketplace:42", 1600.0, NOW - 2 * DAY, "fb_marketplace")
    assert history.stats(key, NOW).median == 1400.0  # type: ignore[union-attr]


def test_history_rejects_invalid_samples() -> None:
    history = new_history(source_weights={"craigslist": 0.0})
    key = "k|new"
    assert not history.observe(key, "a", 0.0, NOW, "retail")
    assert not history.observe(key, "b", -5.0, NOW, "retail")
    assert not history.observe(key, "c", float("nan"), NOW, "retail")
    assert not history.observe(key, "d", 100.0, NOW, "craigslist")  # weight 0 = do not learn
    assert len(history) == 0 and history.keys() == [] and history.stats(key, NOW) is None


def test_time_decay_halves_weight_per_half_life() -> None:
    history = new_history()
    key = "k|used"
    history.observe(key, "fresh", 1000.0, NOW, "retail")
    history.observe(key, "old", 2000.0, NOW - timedelta(days=14), "retail")
    weights = dict(history.weighted_samples(key, NOW))
    assert weights[1000.0] == pytest.approx(0.7)  # default source weight
    assert weights[2000.0] == pytest.approx(0.35)  # one half-life old -> half the weight
    stats = history.stats(key, NOW)
    assert stats is not None and stats.n_eff == pytest.approx(1.8)
    # statistics do not depend on "now" (common decay factor cancels) ...
    later = history.stats(key, NOW + timedelta(days=10))
    assert later == stats
    # ... but the absolute weights do
    later_weights = dict(history.weighted_samples(key, NOW + timedelta(days=14)))
    assert later_weights[1000.0] == pytest.approx(0.35)


def test_window_prunes_old_samples() -> None:
    history = new_history()
    key = "k|used"
    history.observe(key, "ancient", 900.0, NOW - timedelta(days=61), "retail")
    history.observe(key, "recent", 1000.0, NOW - timedelta(days=1), "retail")
    stats = history.stats(key, NOW)
    assert stats is not None and stats.n == 1 and stats.median == 1000.0
    assert len(history) == 1
    # observe() prunes relative to the newest sample of the key, independent of the wall clock
    history.observe(key, "older", 950.0, NOW - timedelta(days=70), "retail")
    assert len(history) == 1
    # everything ages out eventually and the key disappears
    assert history.stats(key, NOW + timedelta(days=62)) is None
    assert key not in history and history.keys() == []


def test_max_samples_cap_drops_oldest() -> None:
    history = new_history(max_samples=10)
    key = "k|used"
    for i in range(15):
        history.observe(key, f"l{i}", 1000.0 + i, NOW - timedelta(hours=15 - i), "retail")
    assert len(history) == 10
    stats = history.stats(key, NOW)
    assert stats is not None and stats.min == 1005.0 and stats.max == 1014.0


def test_identical_prices_mad_zero_no_division_by_zero(scorer: AnomalyScorer) -> None:
    seed_history(scorer, "rtx_4090", "used", [1700.0] * 12)
    stats = scorer.history.stats(history_key("rtx_4090", "used"), NOW)
    assert stats is not None and stats.mad == 0.0 and stats.iqr == 0.0
    result = scorer.score(make_item(1500.0), make_fr(), now=NOW)
    # scale = max(1.4826*0, 0.02*1700) = 34 -> z = 200/34
    assert result.robust_z == pytest.approx(200 / 34, rel=1e-3)
    assert result.iqr_position is None
    assert result.components["I"] == result.components["Zc"]
    assert math.isfinite(result.score)
    same = scorer.score(make_item(1700.0, source_id="same"), make_fr(), now=NOW)
    assert same.robust_z == 0.0 and same.components["Zc"] == 0.0


def test_warm_start_load() -> None:
    history = new_history(source_weights={"retail": 1.0})
    key_new = history_key("rtx_5090", "new")
    key_used = history_key("rtx_5090", "used")
    rows = [
        (key_new, "retail:1", 2399.0, NOW - DAY, "retail"),
        (key_new, "retail:2", 2299.0, (NOW - 2 * DAY).replace(tzinfo=None), "retail"),  # naive = UTC (SQLite)
        (key_new, "retail:1", 2350.0, NOW - 3 * DAY, "retail"),  # stale duplicate: ignored
        (key_used, "ebay:9", 2100.0, NOW - DAY, "ebay"),
        (key_used, "ebay:10", -1.0, NOW, "ebay"),  # invalid price
        (key_used, "ebay:11", "abc", NOW, "ebay"),  # malformed
        ("too", "short"),  # malformed shape
        (key_used, "ebay:12", 2000.0, "yesterday", "ebay"),  # bad timestamp
    ]
    assert history.load(rows) == 3  # type: ignore[arg-type]
    assert len(history) == 3
    assert sorted(history.keys()) == sorted([key_new, key_used])
    stats = history.stats(key_new, NOW)
    assert stats is not None and stats.n == 2 and stats.max == 2399.0


def test_condition_classes_have_separate_histories(scorer: AnomalyScorer) -> None:
    fr = make_fr("rtx_5090")
    new = make_item(2300.0, source="retail", condition=Condition.NEW, source_id="n1", in_stock=True)
    used = make_item(1900.0, source="ebay", condition=Condition.USED, source_id="u1")
    open_box = make_item(2000.0, source="retail", condition=Condition.OPEN_BOX, source_id="o1", in_stock=True)
    for item in (new, used, open_box):
        assert scorer.observe(item, fr, scorer.score(item, fr, now=NOW))
    assert sorted(scorer.history.keys()) == ["rtx_5090|new", "rtx_5090|refurb", "rtx_5090|used"]
    assert history_key("rtx_3090:ti", "used") == "rtx_3090:ti|used"
    assert scorer.history.stats("rtx_5090|new", NOW).median == 2300.0  # type: ignore[union-attr]


# --------------------------------------------------------------------------- scoring: headline scenarios


def test_retail_price_error_is_critical(scorer: AnomalyScorer) -> None:
    item = make_item(999.0, source="retail", condition=Condition.NEW, in_stock=True, title="NVIDIA GeForce RTX 5090 FE")
    result = scorer.score(item, make_fr("rtx_5090"), now=NOW)
    assert result.is_price_error
    assert result.severity is Severity.CRITICAL
    assert result.score >= 85
    assert not result.rejected and result.reject_reason is None
    assert result.market_basis == "config" and result.market_price == 2399.0
    assert result.discount_pct == pytest.approx((2399 - 999) / 2399, abs=1e-4)
    assert result.risk == 0.0 and result.risk_signals == []  # a retailer's $999 is not a placeholder
    assert result.components["S_model"] < result.score  # lifted to agree with the forced severity
    assert any("PRICE ERROR" in line for line in result.explain)


def test_price_error_requires_new_or_refurb_and_trusted_source(scorer: AnomalyScorer) -> None:
    used = scorer.score(make_item(1000.0, source="ebay", condition=Condition.USED), make_fr("rtx_5090"), now=NOW)
    assert used.discount_pct is not None and used.discount_pct >= 0.5
    assert not used.is_price_error  # used market: big discounts are normal negotiation, not errors
    local_new = scorer.score(make_item(1100.0, source="offerup", condition=Condition.NEW), make_fr("rtx_5090"), now=NOW)
    assert not local_new.is_price_error  # offerup reliability 0.7 < 0.9
    refurb = scorer.score(
        make_item(900.0, source="retail", condition=Condition.REFURBISHED, in_stock=True), make_fr("rtx_5090"), now=NOW
    )
    assert refurb.is_price_error and refurb.severity is Severity.CRITICAL


def test_used_marketplace_deal_against_history(scorer: AnomalyScorer) -> None:
    seed_history(scorer, "rtx_4090", "used", around(1700, 30))
    result = scorer.score(make_item(1150.0), make_fr(), now=NOW)
    assert result.severity in (Severity.HIGH, Severity.MEDIUM)
    assert not result.rejected and not result.is_price_error
    assert result.market_basis == "blend" and result.history_n == 30
    assert 1650 <= result.market_price <= 1750  # type: ignore[operator]
    c = result.components
    assert 0.5 < c["D"] < 1.0
    assert c["Zc"] == 1.0 and c["I"] == 1.0 and c["S_stat"] == 1.0  # far outside the observed spread
    assert c["T"] == 1.0  # below the $1400 target
    assert c["c_src"] == pytest.approx(0.7)
    assert c["c_hist"] > 0.9 and c["c_ref"] == pytest.approx(0.5 + 0.5 * c["c_hist"], abs=1e-4)
    assert c["R"] == 0.0
    assert result.robust_z is not None and result.robust_z > 4
    assert result.iqr_position is not None and result.iqr_position > 3

    genuine = VisionResult(verdict=VisionVerdict.GENUINE, confidence=0.9, model="qwen2.5vl:3b")
    boosted = scorer.score(make_item(1150.0), make_fr(), genuine, now=NOW)
    assert boosted.score > result.score
    assert boosted.components["c_src"] == pytest.approx(0.85)  # 0.7 + 0.3 * 0.5
    unsure = VisionResult(verdict=VisionVerdict.GENUINE, confidence=0.5)
    assert scorer.score(make_item(1150.0), make_fr(), unsure, now=NOW).score == result.score


def test_bait_price_is_rejected_as_scam(scorer: AnomalyScorer) -> None:
    result = scorer.score(make_item(100.0), make_fr(), now=NOW)
    assert result.rejected and result.reject_reason == "scam_risk"
    assert result.severity is None
    codes = {s.code for s in result.risk_signals}
    assert "bait_price" in codes
    assert result.risk >= 0.95
    assert not scorer.observe(make_item(100.0), make_fr(), result)  # scams never enter the history


def test_placeholder_price_penalised(scorer: AnomalyScorer) -> None:
    placeholder = scorer.score(make_item(1234.0, source_id="p1"), make_fr(), now=NOW)
    honest = scorer.score(make_item(1250.0, source_id="p2"), make_fr(), now=NOW)
    assert "placeholder_price" in {s.code for s in placeholder.risk_signals}
    assert "placeholder_price" not in {s.code for s in honest.risk_signals}
    assert placeholder.risk == pytest.approx(0.6)
    assert placeholder.score < honest.score * 0.3
    assert not placeholder.rejected  # risk 0.6 < reject_risk 0.85: penalised, not dropped
    # $1 on any source for a $450-floor product is a "price in cart"/contact placeholder
    one_dollar = scorer.score(make_item(1.0, source="retail", condition=Condition.NEW, in_stock=True), make_fr(), now=NOW)
    assert {"placeholder_price", "bait_price"} <= {s.code for s in one_dollar.risk_signals}


def test_above_ceiling_rejected_but_observed(scorer: AnomalyScorer) -> None:
    item = make_item(2000.0)
    result = scorer.score(item, make_fr(), now=NOW)
    assert result.rejected and result.reject_reason == "above_ceiling"
    assert result.severity is None
    assert result.components["T"] == 0.0 and result.components["D"] == 0.0
    assert scorer.observe(item, make_fr(), result)  # still a valid market price
    assert len(scorer.history) == 1


def test_out_of_stock_rejected(scorer: AnomalyScorer) -> None:
    item = make_item(999.0, source="retail", condition=Condition.NEW, in_stock=False)
    result = scorer.score(item, make_fr("rtx_5090"), now=NOW)
    assert result.rejected and result.reject_reason == "out_of_stock"
    assert result.severity is None and not result.is_price_error
    assert any("would be a price error" in line for line in result.explain)
    assert not scorer.observe(item, make_fr("rtx_5090"), result)


def test_non_positive_price_rejected(scorer: AnomalyScorer) -> None:
    result = scorer.score(make_item(0.0), make_fr(), now=NOW)
    assert result.rejected and result.reject_reason == "non_positive_price"
    assert result.severity is None


def test_vision_box_only_confident_rejects(scorer: AnomalyScorer) -> None:
    vision = VisionResult(verdict=VisionVerdict.BOX_ONLY, confidence=0.9)
    result = scorer.score(make_item(1150.0), make_fr(), vision, now=NOW)
    assert result.risk >= 0.95
    assert result.rejected and result.reject_reason == "scam_risk" and result.severity is None
    sig = next(s for s in result.risk_signals if s.code == "vision_box_only")
    assert sig.origin == "vision" and sig.probability == pytest.approx(0.95)


def test_vision_low_confidence_negative_is_discounted(scorer: AnomalyScorer) -> None:
    vision = VisionResult(verdict=VisionVerdict.BOX_ONLY, confidence=0.4)
    result = scorer.score(make_item(1150.0), make_fr(), vision, now=NOW)
    sig = next(s for s in result.risk_signals if s.code == "vision_box_only")
    assert sig.probability == pytest.approx(0.95 * 0.4 * 0.5)
    assert not result.rejected


def test_vision_error_and_uncertain(scorer: AnomalyScorer) -> None:
    error = VisionResult(verdict=VisionVerdict.ERROR, error="timeout")
    result = scorer.score(make_item(1150.0), make_fr(), error, now=NOW)
    assert [s.code for s in result.risk_signals] == ["vision_unverified"]
    assert result.risk == pytest.approx(0.05)
    uncertain = scorer.score(make_item(1150.0), make_fr(), VisionResult(verdict=VisionVerdict.UNCERTAIN, confidence=0.9), now=NOW)
    assert uncertain.risk_signals == []


# --------------------------------------------------------------------------- scoring: risk signals


def test_seller_feedback_signals(scorer: AnomalyScorer) -> None:
    fresh = make_item(1300.0, source="ebay", seller=SellerInfo(feedback_score=2, feedback_pct=100.0))
    codes = {s.code for s in scorer.score(fresh, make_fr(), now=NOW).risk_signals}
    assert codes == {"low_feedback"}
    poor = make_item(1300.0, source="ebay", seller=SellerInfo(feedback_score=500, feedback_pct=91.5))
    assert {s.code for s in scorer.score(poor, make_fr(), now=NOW).risk_signals} == {"poor_feedback_pct"}
    zero = make_item(1300.0, source="ebay", seller=SellerInfo(feedback_score=0, feedback_pct=0.0))
    assert {s.code for s in scorer.score(zero, make_fr(), now=NOW).risk_signals} == {"low_feedback"}
    trusted = make_item(1300.0, source="ebay", seller=SellerInfo(feedback_score=2500, feedback_pct=99.9))
    assert scorer.score(trusted, make_fr(), now=NOW).risk_signals == []
    # low_feedback is a MARKETPLACE concept (local sellers have no feedback system)
    local = make_item(1300.0, seller=SellerInfo(feedback_score=0))
    assert scorer.score(local, make_fr(), now=NOW).risk_signals == []


def test_no_image_and_extreme_discount_low_trust(scorer: AnomalyScorer) -> None:
    no_photo = scorer.score(make_item(1300.0, images=False), make_fr(), now=NOW)
    assert {s.code for s in no_photo.risk_signals} == {"no_image"}
    steep = scorer.score(make_item(700.0), make_fr(), now=NOW)  # 60% below the $1750 reference on FB
    assert "extreme_discount_low_trust" in {s.code for s in steep.risk_signals}
    trusted = scorer.score(make_item(700.0, source="ebay", seller=SellerInfo(feedback_score=900, feedback_pct=100.0)), make_fr(), now=NOW)
    assert "extreme_discount_low_trust" not in {s.code for s in trusted.risk_signals}


def test_noisy_or_combines_text_signals_and_dedupes_codes(scorer: AnomalyScorer) -> None:
    text_signals = [
        RiskSignal(code="payment_red_flag", probability=0.5, origin="text"),
        RiskSignal(code="no_image", probability=0.05, origin="text"),  # duplicate code: max wins
    ]
    result = scorer.score(make_item(1300.0, images=False), make_fr(risk_signals=text_signals), now=NOW)
    probs = {s.code: s.probability for s in result.risk_signals}
    assert probs == {"payment_red_flag": 0.5, "no_image": 0.15}
    assert result.risk == pytest.approx(1 - (1 - 0.5) * (1 - 0.15), abs=1e-4)
    clean = scorer.score(make_item(1300.0, source_id="c"), make_fr(), now=NOW)
    penalty = (1 - result.risk) ** 2
    assert result.score == pytest.approx(clean.score * penalty, abs=0.05)


# --------------------------------------------------------------------------- scoring: market reference


def test_shrinkage_toward_config_reference(config: AppConfig) -> None:
    thin = AnomalyScorer(config)
    seed_history(thin, "rtx_4090", "used", [1400.0, 1420.0])
    r_thin = thin.score(make_item(1300.0), make_fr(), now=NOW)
    assert r_thin.market_basis == "config" and r_thin.market_price == 1750.0
    assert r_thin.history_n == 2 and r_thin.components["c_ref"] == 0.5
    assert r_thin.robust_z is None and r_thin.components["S_stat"] == r_thin.components["D"]

    six = AnomalyScorer(config)
    seed_history(six, "rtx_4090", "used", around(1400, 6, 10), spacing=timedelta(0))
    r_six = six.score(make_item(1300.0), make_fr(), now=NOW)
    c_hist = 1 - math.exp(-6 / 8)
    assert r_six.market_basis == "blend"
    assert r_six.components["c_hist"] == pytest.approx(c_hist, abs=1e-3)
    assert r_six.market_price == pytest.approx(c_hist * 1400 + (1 - c_hist) * 1750, abs=1.0)

    mature = AnomalyScorer(config)
    seed_history(mature, "rtx_4090", "used", around(1400, 50, 20), spacing=timedelta(hours=1))
    stats = mature.history.stats(history_key("rtx_4090", "used"), NOW)
    assert stats is not None and stats.n_eff > 45
    r_mature = mature.score(make_item(1300.0), make_fr(), now=NOW)
    assert r_mature.market_basis == "blend"
    assert abs(r_mature.market_price - stats.median) < 0.01 * (1750 - stats.median)  # type: ignore[operator]
    assert r_mature.components["c_ref"] > 0.99
    assert r_mature.confidence > r_thin.confidence


def test_history_only_and_no_reference_paths(scorer: AnomalyScorer, monkeypatch: pytest.MonkeyPatch) -> None:
    # Validated bands always yield a reference; force the reference-less branches.
    monkeypatch.setattr(PriceBand, "reference_for", lambda self, market_class: None)
    none = scorer.score(make_item(1300.0), make_fr(), now=NOW)
    assert none.market_basis == "none" and none.market_price is None and none.discount_pct is None
    assert none.components["c_ref"] == 0.25 and none.components["D"] == 0.0

    seed_history(scorer, "rtx_4090", "used", [1600.0, 1700.0, 1800.0], spacing=timedelta(0))
    thin = scorer.score(make_item(1300.0), make_fr(), now=NOW)
    assert thin.market_basis == "history" and thin.market_price == pytest.approx(1700.0)
    assert thin.components["c_ref"] == 0.35

    seed_history(scorer, "rtx_4090", "used", around(1700, 12), prefix="more")
    rich = scorer.score(make_item(1300.0), make_fr(), now=NOW)
    assert rich.market_basis == "history" and rich.history_n == 15
    assert rich.components["c_ref"] == pytest.approx(0.5 + 0.5 * rich.components["c_hist"], abs=1e-4)


# --------------------------------------------------------------------------- severity & explainability


def test_severity_thresholds(scorer: AnomalyScorer, config: AppConfig) -> None:
    profile = config.profile("rtx_4090")
    assert scorer.severity_for(90.0, profile) is Severity.CRITICAL
    assert scorer.severity_for(85.0, profile) is Severity.CRITICAL
    assert scorer.severity_for(75.0, profile) is Severity.HIGH
    assert scorer.severity_for(70.0, profile) is Severity.HIGH
    assert scorer.severity_for(60.0, profile) is Severity.MEDIUM
    assert scorer.severity_for(55.0, profile) is Severity.MEDIUM
    assert scorer.severity_for(54.99, profile) is None
    assert scorer.severity_for(0.0, None) is None
    profile.min_score = 65
    assert scorer.severity_for(60.0, profile) is None
    assert scorer.severity_for(66.0, profile) is Severity.MEDIUM
    profile.min_score = 40  # a lower min_score never undercuts the global medium threshold
    assert scorer.severity_for(50.0, profile) is None
    profile.min_score = 80  # above "high": nothing below the profile minimum alerts
    assert scorer.severity_for(75.0, profile) is None
    assert scorer.severity_for(81.0, profile) is Severity.HIGH


def test_profile_min_score_override_end_to_end(config: AppConfig) -> None:
    scorer = AnomalyScorer(config)
    seed_history(scorer, "rtx_4090", "used", around(1700, 30))
    baseline = scorer.score(make_item(1150.0), make_fr(), now=NOW)
    assert baseline.severity is not None
    config.profile("rtx_4090").min_score = min(100.0, baseline.score + 1)
    suppressed = scorer.score(make_item(1150.0), make_fr(), now=NOW)
    assert suppressed.score == baseline.score and suppressed.severity is None and not suppressed.rejected


def test_explain_and_components_populated(scorer: AnomalyScorer) -> None:
    seed_history(scorer, "rtx_4090", "used", around(1700, 10))
    result = scorer.score(make_item(1300.0, images=False), make_fr(match_strength=0.85), now=NOW)
    assert set(REQUIRED_COMPONENTS) <= set(result.components)
    assert all(isinstance(v, float) for v in result.components.values())
    assert result.components["c_match"] == pytest.approx(0.85)
    assert result.components["C"] == pytest.approx(
        result.components["c_src"] * result.components["c_match"] * result.components["c_ref"], abs=1e-3
    )
    lam = 0.4
    expected = 100 * result.components["O"] * (lam + (1 - lam) * result.components["C"]) * (1 - result.components["R"]) ** 2
    assert result.score == pytest.approx(expected, abs=0.05)
    assert len(result.explain) >= 5
    text = "\n".join(result.explain)
    assert "market" in text and "below market" in text and "confidence" in text and "risk" in text
    assert result.confidence == pytest.approx(result.components["C"])
    assert result.history_n == 10
    # round-trips through JSON (alerts and snapshots serialise it)
    assert ScoreResult.model_validate_json(result.model_dump_json()) == result


def test_variant_band_is_used(scorer: AnomalyScorer) -> None:
    # rtx_3090 base ceiling is $950; the Ti variant raises it to $1050
    base = scorer.score(make_item(1000.0), make_fr("rtx_3090"), now=NOW)
    ti = scorer.score(make_item(1000.0), make_fr("rtx_3090", variant_id="ti"), now=NOW)
    assert base.reject_reason == "above_ceiling"
    assert not ti.rejected and ti.market_price == 900.0


def test_unknown_profile_and_filter_rejection(scorer: AnomalyScorer) -> None:
    unknown = scorer.score(make_item(1000.0), make_fr("does_not_exist"), now=NOW)
    assert unknown.rejected and unknown.reject_reason == "unknown_profile" and unknown.score == 0.0
    filtered = scorer.score(make_item(1150.0), make_fr(accepted=False, reject_code="box_only"), now=NOW)
    assert filtered.rejected and filtered.reject_reason == "box_only" and filtered.severity is None
    assert not scorer.observe(make_item(1150.0), make_fr(accepted=False, reject_code="box_only"), filtered)


def test_observe_rules(scorer: AnomalyScorer) -> None:
    fr = make_fr()
    good = make_item(1500.0, source_id="g")
    assert scorer.observe(good, fr, scorer.score(good, fr, now=NOW))
    risky = make_item(1500.0, source_id="r")
    risky_result = scorer.score(risky, make_fr(risk_signals=[RiskSignal(code="payment_red_flag", probability=0.6)]), now=NOW)
    assert risky_result.risk > 0.5 and not risky_result.rejected
    assert not scorer.observe(risky, fr, risky_result)  # risk > history_max_risk
    assert len(scorer.history) == 1
    assert scorer.history.keys() == ["rtx_4090|used"]


def test_reliability_lookup(scorer: AnomalyScorer) -> None:
    assert scorer.reliability("retail") == 1.0
    assert scorer.reliability("ebay") == pytest.approx(0.95)
    assert scorer.reliability("fb_marketplace") == pytest.approx(0.7)
    assert scorer.reliability("craigslist") == pytest.approx(0.65)
    assert scorer.reliability("mystery") == pytest.approx(0.7)
    assert scorer.reliability("items") == pytest.approx(0.7)  # not a source, a SourcesSection method


def test_default_history_built_from_config(config: AppConfig) -> None:
    scorer = AnomalyScorer(config)
    assert scorer.history.half_life_days == config.scoring.history_half_life_days
    assert scorer.history.max_samples == config.scoring.history_max_samples
    assert scorer.history.source_weight("craigslist") == pytest.approx(0.5)


# --------------------------------------------------------------------------- performance


def test_score_is_fast_with_full_history(scorer: AnomalyScorer) -> None:
    seed_history(scorer, "rtx_4090", "used", around(1700, 400, 250), spacing=timedelta(minutes=200))
    assert len(scorer.history) == 400
    item, fr = make_item(1150.0), make_fr()
    scorer.score(item, fr, now=NOW)  # warm the per-key cache
    runs = 500
    started = time.perf_counter()
    for _ in range(runs):
        scorer.score(item, fr, now=NOW)
    cached_ms = (time.perf_counter() - started) * 1000 / runs
    assert cached_ms < 1.0, f"cached score() {cached_ms:.3f} ms"

    # worst case: every score follows an observe (cache invalidated, full recompute)
    key = history_key("rtx_4090", "used")
    started = time.perf_counter()
    for i in range(100):
        scorer.history.observe(key, f"fb_marketplace:cold{i % 5}", 1700.0 + i, NOW, "fb_marketplace")
        scorer.score(item, fr, now=NOW)
    cold_ms = (time.perf_counter() - started) * 1000 / 100
    assert cold_ms < 5.0, f"observe+score {cold_ms:.3f} ms"


def test_history_stats_dataclass_is_frozen() -> None:
    stats = HistoryStats(n=1, n_eff=1.0, median=1.0, q1=1.0, q3=1.0, mad=0.0, iqr=0.0, min=1.0, max=1.0)
    with pytest.raises(AttributeError):
        stats.n = 2  # type: ignore[misc]
