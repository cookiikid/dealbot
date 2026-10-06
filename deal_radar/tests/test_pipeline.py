"""End-to-end tests: RawListing -> normalize -> filter -> score -> vision -> dedup -> route -> record.

Uses the real engine modules and the shipped config.yaml; only the delivery channels
and the vision backend are faked.
"""

from __future__ import annotations

import asyncio
import statistics
import time
from datetime import timedelta
from pathlib import Path

import pytest

from deal_radar.config_schema import AppConfig, load_config
from deal_radar.db.database import Database, Recorder
from deal_radar.dispatchers.base import Dispatcher
from deal_radar.dispatchers.router import AlertRouter
from deal_radar.engine.anomaly import AnomalyScorer, PriceHistory
from deal_radar.engine.bus import MemoryBus
from deal_radar.engine.dedup import MemoryDeduplicator
from deal_radar.engine.normalizer import Normalizer
from deal_radar.engine.pipeline import Pipeline, PipelineRunner
from deal_radar.engine.text_filter import TextFilter
from deal_radar.engine.types import (
    Alert,
    DealItem,
    DispatchResult,
    RawListing,
    SellerInfo,
    Severity,
    SourceKind,
    VisionResult,
    VisionVerdict,
    utcnow,
)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"


class RecordingDispatcher(Dispatcher):
    def __init__(self, target: str, *, ok: bool = True) -> None:
        self.target = target
        self.ok = ok
        self.alerts: list[Alert] = []
        self.notices: list[tuple[str, str]] = []

    async def send(self, alert: Alert) -> DispatchResult:
        self.alerts.append(alert)
        return DispatchResult(target=self.target, ok=self.ok, error=None if self.ok else "boom")

    async def send_notice(self, title: str, message: str) -> DispatchResult:
        self.notices.append((title, message))
        return DispatchResult(target=self.target, ok=True)


class FakeVision:
    """Stands in for VisionFilter (same duck-typed interface the pipeline uses)."""

    def __init__(self, verdict: VisionVerdict, confidence: float = 0.95) -> None:
        self.verdict = verdict
        self.confidence = confidence
        self.calls = 0

    def applies_to(self, item: DealItem, profile) -> bool:  # noqa: ANN001
        return item.source_kind is SourceKind.LOCAL and bool(item.image_urls)

    async def check(self, item: DealItem, profile) -> VisionResult:  # noqa: ANN001
        self.calls += 1
        return VisionResult(verdict=self.verdict, confidence=self.confidence, model="fake", latency_ms=5, images_checked=1)


@pytest.fixture
def config() -> AppConfig:
    return load_config(CONFIG_PATH, env={})


def build(config: AppConfig, *, vision=None, dispatch_ok: bool = True, recorder=None):  # noqa: ANN001, ANN201
    targets = {t: RecordingDispatcher(t, ok=dispatch_ok) for t in config.known_targets()}
    router = AlertRouter(config, targets)
    scorer = AnomalyScorer(
        config,
        PriceHistory(
            half_life_days=config.scoring.history_half_life_days,
            window_days=config.scoring.history_window_days,
            max_samples=config.scoring.history_max_samples,
            source_weights=config.scoring.source_history_weights,
        ),
    )
    pipeline = Pipeline(
        config,
        normalizer=Normalizer(config),
        text_filter=TextFilter(config),
        scorer=scorer,
        dedup=MemoryDeduplicator(config),
        router=router,
        vision=vision,
        recorder=recorder,
    )
    return pipeline, targets


def retail(source_id: str, title: str, price: float, **kw) -> RawListing:  # noqa: ANN003
    return RawListing(
        source="retail",
        source_kind=SourceKind.RETAIL,
        source_id=source_id,
        url=f"https://www.bestbuy.com/site/{source_id}.p",
        title=title,
        price=price,
        retailer="Best Buy",
        in_stock=True,
        condition="New",
        **kw,
    )


def fb(source_id: str, title: str, price: float, description: str = "", images: bool = True) -> RawListing:
    return RawListing(
        source="fb_marketplace",
        source_kind=SourceKind.LOCAL,
        source_id=source_id,
        url=f"https://www.facebook.com/marketplace/item/{source_id}/",
        title=title,
        description=description,
        price=str(price),
        image_urls=["https://scontent.example/photo.jpg"] if images else [],
        posted_at=utcnow() - timedelta(minutes=3),
    )


async def test_retail_price_error_alerts_once_then_on_price_drop(config: AppConfig) -> None:
    pipeline, targets = build(config)
    first = await pipeline.process(retail("6614151", "NVIDIA GeForce RTX 5090 Founders Edition 32GB GDDR7", 999.99))
    assert first.stage == "alerted", (first.stage, first.reason, first.score)
    assert first.alert is not None and first.alert.severity is Severity.CRITICAL
    assert first.score.is_price_error
    assert first.alert.mention  # price-error route pings humans
    delivered = {t for t, d in targets.items() if d.alerts}
    assert {"console", "websocket"} <= delivered

    again = await pipeline.process(retail("6614151", "NVIDIA GeForce RTX 5090 Founders Edition 32GB GDDR7", 999.99))
    assert again.stage == "duplicate"

    small_drop = await pipeline.process(retail("6614151", "NVIDIA GeForce RTX 5090 Founders Edition 32GB GDDR7", 989.99))
    assert small_drop.stage == "duplicate"  # < 5% drop

    drop = await pipeline.process(retail("6614151", "NVIDIA GeForce RTX 5090 Founders Edition 32GB GDDR7", 899.99))
    assert drop.stage == "alerted" and drop.alert.is_update
    assert drop.alert.dedup.previous_price == pytest.approx(999.99)


async def test_scam_and_noise_are_rejected(config: AppConfig) -> None:
    pipeline, targets = build(config)
    box = await pipeline.process(fb("1", "RTX 4090 Box Only", 150))
    assert box.stage == "filter" and box.reason == "box_only"
    rental = await pipeline.process(fb("2", "RTX 3090 Mining Rig Rental $50/mo", 50))
    assert rental.stage == "filter" and rental.reason == "rental"
    broken = await pipeline.process(fb("3", "Broken Alienware OLED AW3225QF", 200))
    assert broken.stage == "filter" and broken.reason == "damaged"
    bait = await pipeline.process(fb("4", "RTX 4090 Founders Edition", 100))
    assert bait.stage == "score" and bait.reason == "scam_risk"
    unrelated = await pipeline.process(fb("5", "Vintage oak dining table", 300))
    assert unrelated.stage == "filter" and unrelated.reason == "no_profile_match"
    assert not any(d.alerts for d in targets.values())


async def test_vision_negative_verdict_blocks_marketplace_alert(config: AppConfig) -> None:
    vision = FakeVision(VisionVerdict.BOX_ONLY, 0.97)
    pipeline, targets = build(config, vision=vision)
    outcome = await pipeline.process(fb("10", "RTX 4090 FE 24GB", 1050, description="Works great, upgrading."))
    assert vision.calls == 1
    assert outcome.stage == "vision" and outcome.reason == "scam_risk"
    assert not any(d.alerts for d in targets.values())


async def test_vision_genuine_marketplace_deal_alerts(config: AppConfig) -> None:
    vision = FakeVision(VisionVerdict.GENUINE, 0.9)
    pipeline, targets = build(config, vision=vision)
    outcome = await pipeline.process(fb("11", "RTX 4090 Founders Edition 24GB", 1050, description="Works great, upgrading to a 5090."))
    assert vision.calls == 1
    assert outcome.stage == "alerted", (outcome.stage, outcome.reason, outcome.score)
    assert outcome.alert.vision is not None and outcome.alert.vision.verdict is VisionVerdict.GENUINE
    assert outcome.vision_ms > 0 and outcome.internal_ms >= 0


async def test_failed_dispatch_rolls_back_claim(config: AppConfig) -> None:
    pipeline, targets = build(config, dispatch_ok=False)
    raw = retail("6614151", "NVIDIA GeForce RTX 5090 Founders Edition 32GB GDDR7", 999.99)
    first = await pipeline.process(raw)
    assert first.stage == "dispatch_failed"
    for d in targets.values():
        d.ok = True
    second = await pipeline.process(raw)
    assert second.stage == "alerted"  # claim was released, so the retry alerts


async def test_ebay_low_feedback_seller_is_penalised(config: AppConfig) -> None:
    pipeline, _ = build(config)

    def ebay(source_id: str, feedback: int) -> RawListing:
        return RawListing(
            source="ebay", source_kind=SourceKind.MARKETPLACE, source_id=source_id,
            url=f"https://www.ebay.com/itm/{source_id}", title="NVIDIA GeForce RTX 4090 Founders Edition 24GB",
            price=1250.0, shipping=0.0, condition="3000", image_urls=["https://i.ebayimg.com/x.jpg"],
            seller=SellerInfo(name="s", feedback_score=feedback, feedback_pct=100.0),
        )

    trusted = await pipeline.process(ebay("1", 2500))
    fresh = await pipeline.process(ebay("2", 0))
    assert trusted.score is not None and fresh.score is not None
    assert fresh.score.score < trusted.score.score
    assert any(s.code == "low_feedback" for s in fresh.score.risk_signals)


async def test_internal_overhead_under_50ms(config: AppConfig) -> None:
    pipeline, _ = build(config)
    titles = [
        "ASUS TUF RTX 4090 OC 24GB", "RTX 3090 FE 24GB", "LG OLED65C4PUA 65 inch", "AW3225QF 32 4K QD-OLED",
        "Gaming PC i9-13900K RTX 4090 64GB DDR5", "RTX 4090 Box Only", "Sony a7 IV body", "Steam Deck OLED 1TB",
    ]
    timings = []
    for i in range(400):
        outcome = await pipeline.process(fb(f"p{i}", titles[i % len(titles)], 600 + (i % 50) * 37, images=False))
        timings.append(outcome.internal_ms)
    p95 = statistics.quantiles(timings, n=20)[18]
    assert p95 < 50, f"p95 internal overhead {p95:.2f} ms"


async def test_runner_consumes_bus_and_records(config: AppConfig, tmp_path: Path) -> None:
    db = Database(f"sqlite+aiosqlite:///{tmp_path / 'e2e.db'}")
    await db.connect()
    recorder = Recorder(db, batch_size=50, flush_seconds=0.05)
    await recorder.start()
    pipeline, _ = build(config, recorder=recorder)
    bus = MemoryBus(maxsize=1000)
    await bus.start()
    runner = PipelineRunner(pipeline, bus, workers=4)
    task = asyncio.create_task(runner.run())
    for i in range(60):
        await bus.publish(fb(f"r{i}", "RTX 3090 FE 24GB" if i % 2 else "RTX 4090 Box Only", 650 + i))
    deadline = time.monotonic() + 10
    while runner.processed < 60 and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    assert runner.processed == 60
    await bus.close()
    await asyncio.wait_for(task, timeout=5)
    await runner.drain(timeout=1)
    await recorder.stop()
    counts = await db.counts()
    assert sum(counts.values()) > 0
    await db.close()


def test_main_check_config_and_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Full component wiring via the CLI (sync test: main() owns its own event loop)."""
    from deal_radar import main as main_mod

    monkeypatch.setattr(main_mod, "configure_logging", lambda *a, **k: None)  # keep pytest's log capture intact
    for key in ("REDDIT_ENABLED", "SLICKDEALS_ENABLED"):
        monkeypatch.setenv(key, "false")
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path / 'once.db'}")
    assert main_mod.main(["--config", str(CONFIG_PATH), "--env-file", str(tmp_path / "none.env"), "--check-config"]) == 0
    assert "config OK" in capsys.readouterr().out
    assert main_mod.main(["--config", str(CONFIG_PATH), "--env-file", str(tmp_path / "none.env"), "--once", "--dry-run"]) == 0
    assert "outcomes:" in capsys.readouterr().out
