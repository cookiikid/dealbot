"""Tests for the BaseIngestor loop: change detection, stale guard, failure handling, leases."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import fakeredis
import pytest

from deal_radar.config_schema import AppConfig, SourceCommon
from deal_radar.core.http import HttpClient, NetworkSettings
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import RawListing, SourceKind, utcnow
from deal_radar.sources.base import BaseIngestor, ChangeDetector, IngestorContext, SourceBlocked


def _raw(source_id: str, price: float, minutes_ago: float | None = 1.0) -> RawListing:
    return RawListing(
        source="fake",
        source_kind=SourceKind.RETAIL,
        source_id=source_id,
        url=f"https://example.com/{source_id}",
        title=f"Item {source_id}",
        price=price,
        posted_at=utcnow() - timedelta(minutes=minutes_ago) if minutes_ago is not None else None,
    )


class FakeIngestor(BaseIngestor):
    name = "fake"
    kind = SourceKind.RETAIL

    def __init__(self, cfg: SourceCommon, ctx: IngestorContext, script: list) -> None:
        super().__init__(cfg, ctx)
        self.script = list(script)
        self.polls = 0
        self.setup_called = False
        self.teardown_called = False

    async def setup(self) -> None:
        self.setup_called = True

    async def teardown(self) -> None:
        self.teardown_called = True

    async def poll(self) -> list[RawListing]:
        self.polls += 1
        step = self.script.pop(0) if self.script else []
        if isinstance(step, BaseException):
            raise step
        return step


@pytest.fixture
async def ctx():
    http = HttpClient.create(NetworkSettings(trust_env=False))
    notices: list[tuple[str, str]] = []

    async def notify(title: str, message: str) -> None:
        notices.append((title, message))

    context = IngestorContext(http=http, metrics=Metrics(), config=AppConfig(), node_id="node-a", notify=notify)
    context.notices = notices  # type: ignore[attr-defined]
    yield context
    await http.close()


def test_change_detector_lru() -> None:
    cd = ChangeDetector(max_entries=2)
    assert cd.changed("a", (1,))
    assert not cd.changed("a", (1,))
    assert cd.changed("a", (2,))
    cd.changed("b", (1,))
    cd.changed("c", (1,))
    assert "a" not in cd and len(cd) == 2


async def test_run_once_emits_only_new_or_changed(ctx: IngestorContext) -> None:
    cfg = SourceCommon(enabled=True, max_item_age_minutes=60)
    ing = FakeIngestor(cfg, ctx, [
        [_raw("1", 100), _raw("2", 200), _raw("old", 50, minutes_ago=120)],
        [_raw("1", 100), _raw("2", 180)],
    ])
    first = await ing.run_once()
    assert [r.source_id for r in first] == ["1", "2"]
    assert all(r.node_id == "node-a" for r in first)
    second = await ing.run_once()
    assert [(r.source_id, r.price) for r in second] == [("2", 180)]
    assert ing.health.items_emitted == 3 and ing.health.state == "ok"


async def test_loop_backs_off_and_notifies_on_block(ctx: IngestorContext) -> None:
    cfg = SourceCommon(enabled=True, poll_interval_seconds=0.01, jitter_pct=0, cooldown_seconds=0.05, max_consecutive_failures=2)
    ing = FakeIngestor(cfg, ctx, [RuntimeError("boom"), SourceBlocked("checkpoint", cooldown_seconds=0.05), [_raw("1", 10)]])
    emitted: list[RawListing] = []
    stop = asyncio.Event()

    async def emit(raw: RawListing) -> None:
        emitted.append(raw)
        stop.set()

    await asyncio.wait_for(ing.run(emit, stop), timeout=5)
    assert ing.setup_called and ing.teardown_called
    assert [r.source_id for r in emitted] == ["1"]
    assert ing.health.failures == 2
    assert any("blocked" in title for title, _ in ctx.notices)  # type: ignore[attr-defined]


async def test_setup_failure_stops_source_and_notifies(ctx: IngestorContext) -> None:
    class Broken(FakeIngestor):
        async def setup(self) -> None:
            raise RuntimeError("no browser")

    ing = Broken(SourceCommon(enabled=True, cooldown_seconds=0.02), ctx, [])
    stop = asyncio.Event()
    task = asyncio.create_task(ing.run(lambda raw: asyncio.sleep(0), stop))
    await asyncio.sleep(0.1)
    assert ing.health.state == "setup_failed" and ing.polls == 0
    assert ing.health.failures >= 2  # retried after the cooldown
    assert sum("failed to start" in t for t, _ in ctx.notices) == 1  # type: ignore[attr-defined]
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert ing.health.state == "stopped" and ing.teardown_called


async def test_setup_recovers_after_transient_failure(ctx: IngestorContext) -> None:
    class Flaky(FakeIngestor):
        attempts = 0

        async def setup(self) -> None:
            Flaky.attempts += 1
            if Flaky.attempts == 1:
                raise ConnectionError("dns not ready")

    ing = Flaky(SourceCommon(enabled=True, cooldown_seconds=0.01, poll_interval_seconds=0.01), ctx, [[_raw("9", 9)]])
    stop = asyncio.Event()
    got: list[RawListing] = []

    async def emit(raw: RawListing) -> None:
        got.append(raw)
        stop.set()

    await asyncio.wait_for(ing.run(emit, stop), timeout=2)
    assert [r.source_id for r in got] == ["9"] and Flaky.attempts == 2


async def test_lease_makes_second_node_standby() -> None:
    redis = fakeredis.FakeAsyncRedis()
    http = HttpClient.create(NetworkSettings(trust_env=False))
    try:
        cfg = SourceCommon(enabled=True, lease_ttl_seconds=30, poll_interval_seconds=0.01, jitter_pct=0)
        a = FakeIngestor(cfg, IngestorContext(http=http, metrics=Metrics(), config=AppConfig(), node_id="a", redis=redis), [[_raw("1", 1)]])
        b = FakeIngestor(cfg, IngestorContext(http=http, metrics=Metrics(), config=AppConfig(), node_id="b", redis=redis), [[_raw("1", 1)]])
        out: list[RawListing] = []

        async def emit(raw: RawListing) -> None:
            out.append(raw)

        await a._cycle(emit)
        await b._cycle(emit)
        assert a.health.state == "ok" and b.health.state == "standby"
        assert len(out) == 1 and b.polls == 0
        await a._lease.release()  # type: ignore[union-attr]
        await b._cycle(emit)
        assert b.health.state == "ok" and b.polls == 1
    finally:
        await http.close()
        await redis.aclose()
