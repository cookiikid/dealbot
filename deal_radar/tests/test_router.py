"""Tests for dispatchers/router.py: routing policy, quiet hours, flood guard and fan-out.

Delivery channels are fake :class:`Dispatcher` subclasses that record calls and can be
told to be slow, to fail, to raise or to be unconfigured. Configs are small dicts run
through ``AppConfig.model_validate`` plus the shipped ``config.yaml``. Time comes from
an injected, manually advanced clock, so every test is deterministic.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from deal_radar.config_schema import AppConfig, load_config
from deal_radar.core.metrics import Metrics
from deal_radar.dispatchers.base import ConsoleDispatcher, Dispatcher
from deal_radar.dispatchers.router import (
    FLOOD_WINDOW_SECONDS,
    REASON_FLOOD_GUARD,
    REASON_NO_ROUTE,
    REASON_QUIET_HOURS,
    AlertRouter,
    in_window,
    parse_hhmm,
)
from deal_radar.engine.types import (
    Alert,
    DealItem,
    DedupDecision,
    DedupStatus,
    DispatchReport,
    DispatchResult,
    ScoreResult,
    Severity,
    SourceKind,
)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
ROUTER_LOGGER = "deal_radar.dispatch.router"

# 2026-01-15 is in EST (UTC-5); 2026-07-15 is in EDT (UTC-4).
WINTER_NOON_UTC = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- fakes


class FakeDispatcher(Dispatcher):
    """Records every call; behaviour is configurable per instance."""

    def __init__(
        self,
        target: str,
        *,
        ok: bool = True,
        delay: float = 0.0,
        raise_exc: BaseException | None = None,
        configured: bool = True,
        notice_ok: bool = True,
        notice_delay: float = 0.0,
        notice_raise: BaseException | None = None,
        close_raise: BaseException | None = None,
        result_target: str | None = None,
        latency_ms: float = 1.5,
    ) -> None:
        self.target = target
        self.ok = ok
        self.delay = delay
        self.raise_exc = raise_exc
        self._configured = configured
        self.notice_ok = notice_ok
        self.notice_delay = notice_delay
        self.notice_raise = notice_raise
        self.close_raise = close_raise
        self.result_target = result_target
        self.latency_ms = latency_ms
        self.sent: list[Alert] = []
        self.mentions: list[bool] = []
        self.notices: list[tuple[str, str]] = []
        self.closed = 0
        self.started = 0

    @property
    def configured(self) -> bool:
        return self._configured

    @configured.setter
    def configured(self, value: bool) -> None:
        self._configured = value

    async def send(self, alert: Alert) -> DispatchResult:
        self.started += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.raise_exc is not None:
            raise self.raise_exc
        self.sent.append(alert)
        self.mentions.append(alert.mention)
        return DispatchResult(
            target=self.result_target or self.target,
            ok=self.ok,
            status=200 if self.ok else 500,
            latency_ms=self.latency_ms,
            error=None if self.ok else "upstream 500",
        )

    async def send_notice(self, title: str, message: str) -> DispatchResult:
        if self.notice_delay:
            await asyncio.sleep(self.notice_delay)
        if self.notice_raise is not None:
            raise self.notice_raise
        self.notices.append((title, message))
        return DispatchResult(target=self.target, ok=self.notice_ok, error=None if self.notice_ok else "nope")

    async def close(self) -> None:
        self.closed += 1
        if self.close_raise is not None:
            raise self.close_raise


class WrongTypeDispatcher(FakeDispatcher):
    async def send(self, alert: Alert) -> Any:  # deliberately violates the contract
        return {"ok": True}


class SelfCancellingDispatcher(FakeDispatcher):
    """Raises CancelledError although nobody cancelled the caller (e.g. an inner task died)."""

    async def send(self, alert: Alert) -> DispatchResult:
        raise asyncio.CancelledError()


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


# --------------------------------------------------------------------------- builders


def _profile(pid: str, category: str) -> dict[str, Any]:
    return {
        "id": pid,
        "name": pid.replace("_", " ").title(),
        "category": category,
        "match": {"any": [rf"\b{pid}\b"]},
        "price": {"reference_new": 1000, "floor": 300, "target": 700, "ceiling": 900},
    }


DEFAULT_PROFILES = [_profile("gpu_x", "gpu"), _profile("gpu_y", "gpu"), _profile("mon_x", "monitor")]


def make_config(
    routes: list[dict[str, Any]],
    *,
    tz: str = "UTC",
    dry_run: bool = False,
    timeout: float = 2.0,
    system_targets: list[str] | None = None,
) -> AppConfig:
    data: dict[str, Any] = {
        "app": {"timezone": tz, "dry_run": dry_run},
        "dispatch": {
            "timeout_seconds": timeout,
            "discord": {
                "webhooks": {
                    name: {"webhook_url": f"https://discord.example/api/webhooks/1/{name}"}
                    for name in ("a", "b", "c", "ops")
                }
            },
            "telegram": {"bot_token": "123:abc", "chats": {"main": {"chat_id": "42"}}},
            "routes": routes,
        },
        "profiles": DEFAULT_PROFILES,
    }
    if system_targets is not None:
        data["dispatch"]["system_targets"] = system_targets
    return AppConfig.model_validate(data)


def fakes_for(config: AppConfig, **overrides: FakeDispatcher) -> dict[str, FakeDispatcher]:
    out = {t: FakeDispatcher(t) for t in sorted(config.known_targets())}
    for key, value in overrides.items():
        out[key.replace("__", ":")] = value
    return out


def make_alert(
    *,
    severity: Severity = Severity.MEDIUM,
    profile_id: str = "gpu_x",
    category: str = "gpu",
    source: str = "ebay",
    price_error: bool = False,
) -> Alert:
    item = DealItem(
        source=source,
        source_kind=SourceKind.MARKETPLACE,
        source_id="123",
        url="https://www.ebay.com/itm/123",
        title="Some GPU X card",
        price=650.0,
        total_price=650.0,
    )
    score = ScoreResult(score=80.0, severity=severity, is_price_error=price_error, market_price=1000.0, discount_pct=0.35)
    return Alert(
        item=item,
        profile_id=profile_id,
        profile_name=profile_id,
        category=category,
        severity=severity,
        score=score,
        dedup=DedupDecision(status=DedupStatus.NEW),
    )


def router_for(
    config: AppConfig,
    dispatchers: dict[str, FakeDispatcher] | None = None,
    *,
    now: datetime = WINTER_NOON_UTC,
    metrics: Metrics | None = None,
) -> tuple[AlertRouter, dict[str, FakeDispatcher], Clock]:
    clock = Clock(now)
    dispatchers = dispatchers if dispatchers is not None else fakes_for(config)
    return AlertRouter(config, dispatchers, metrics=metrics, clock=clock), dispatchers, clock


# --------------------------------------------------------------------------- helpers


def test_parse_hhmm() -> None:
    assert parse_hhmm("00:00") == 0
    assert parse_hhmm("07:30") == 450
    assert parse_hhmm("23:59") == 1439
    for bad in ("24:00", "12:60", "1230", "ab:cd", "12:5", ""):
        with pytest.raises(ValueError):
            parse_hhmm(bad)


def test_in_window_same_day_crossing_midnight_and_empty() -> None:
    nine, five = 9 * 60, 17 * 60
    assert in_window(nine, nine, five)  # start inclusive
    assert in_window(12 * 60, nine, five)
    assert not in_window(five, nine, five)  # end exclusive
    assert not in_window(8 * 60 + 59, nine, five)

    eleven_pm, seven_am = 23 * 60, 7 * 60
    assert in_window(eleven_pm, eleven_pm, seven_am)
    assert in_window(0, eleven_pm, seven_am)
    assert in_window(6 * 60 + 59, eleven_pm, seven_am)
    assert not in_window(seven_am, eleven_pm, seven_am)
    assert not in_window(12 * 60, eleven_pm, seven_am)
    assert not in_window(22 * 60 + 59, eleven_pm, seven_am)

    assert not in_window(600, 600, 600)  # start == end -> empty window


# --------------------------------------------------------------------------- route matching


def test_severity_thresholds() -> None:
    config = make_config(
        [
            {"name": "all", "min_severity": "medium", "targets": ["discord:a"]},
            {"name": "loud", "min_severity": "high", "targets": ["discord:b"]},
            {"name": "page", "min_severity": "critical", "targets": ["telegram:main"]},
        ]
    )
    router, _, _ = router_for(config)
    assert router.targets_for(make_alert(severity=Severity.MEDIUM)) == (["discord:a"], False, None)
    assert router.targets_for(make_alert(severity=Severity.HIGH)) == (["discord:a", "discord:b"], False, None)
    assert router.targets_for(make_alert(severity=Severity.CRITICAL)) == (
        ["discord:a", "discord:b", "telegram:main"],
        False,
        None,
    )

    only_high = make_config([{"name": "loud", "min_severity": "high", "targets": ["discord:b"]}])
    router, _, _ = router_for(only_high)
    assert router.targets_for(make_alert(severity=Severity.MEDIUM)) == ([], False, REASON_NO_ROUTE)


def test_category_profile_and_source_filters() -> None:
    config = make_config(
        [
            {"name": "gpus", "categories": ["gpu"], "targets": ["discord:a"]},
            {"name": "gpu_x_only", "profiles": ["gpu_x"], "targets": ["discord:b"]},
            {"name": "reddit_only", "sources": ["reddit"], "targets": ["discord:c"]},
        ]
    )
    router, _, _ = router_for(config)
    assert router.targets_for(make_alert(profile_id="gpu_x", category="gpu"))[0] == ["discord:a", "discord:b"]
    assert router.targets_for(make_alert(profile_id="gpu_y", category="gpu"))[0] == ["discord:a"]
    assert router.targets_for(make_alert(profile_id="mon_x", category="monitor", source="reddit"))[0] == ["discord:c"]
    assert router.targets_for(make_alert(profile_id="gpu_y", category="gpu", source="reddit"))[0] == ["discord:a", "discord:c"]
    assert router.targets_for(make_alert(profile_id="mon_x", category="monitor", source="ebay")) == ([], False, REASON_NO_ROUTE)


def test_combined_filters_must_all_match() -> None:
    config = make_config(
        [{"name": "narrow", "categories": ["gpu"], "profiles": ["gpu_y"], "sources": ["ebay"], "targets": ["discord:a"]}]
    )
    router, _, _ = router_for(config)
    assert router.targets_for(make_alert(profile_id="gpu_y"))[0] == ["discord:a"]
    assert router.targets_for(make_alert(profile_id="gpu_x"))[2] == REASON_NO_ROUTE
    assert router.targets_for(make_alert(profile_id="gpu_y", source="reddit"))[2] == REASON_NO_ROUTE


def test_price_error_only() -> None:
    config = make_config(
        [
            {"name": "errors", "min_severity": "high", "price_error_only": True, "mention": True, "targets": ["discord:a"]},
            {"name": "rest", "min_severity": "high", "targets": ["discord:b"]},
        ]
    )
    router, _, _ = router_for(config)
    assert router.targets_for(make_alert(severity=Severity.CRITICAL, price_error=True)) == (["discord:a", "discord:b"], True, None)
    assert router.targets_for(make_alert(severity=Severity.CRITICAL, price_error=False)) == (["discord:b"], False, None)
    # still subject to the route's min_severity
    assert router.targets_for(make_alert(severity=Severity.MEDIUM, price_error=True)) == ([], False, REASON_NO_ROUTE)


def test_union_dedupe_keeps_order_and_mention_is_any() -> None:
    config = make_config(
        [
            {"name": "one", "targets": ["discord:b", "websocket", "console"]},
            {"name": "two", "mention": True, "targets": ["console", "discord:a", "discord:b"]},
            {"name": "three", "targets": ["websocket", "telegram:main"]},
        ]
    )
    router, _, _ = router_for(config)
    targets, mention, reason = router.targets_for(make_alert())
    assert targets == ["discord:b", "websocket", "console", "discord:a", "telegram:main"]
    assert mention is True and reason is None

    no_mention = make_config([{"name": "one", "targets": ["discord:a"]}, {"name": "two", "targets": ["discord:a"]}])
    router, _, _ = router_for(no_mention)
    assert router.targets_for(make_alert()) == (["discord:a"], False, None)


# --------------------------------------------------------------------------- quiet hours


def _quiet_config(**quiet: Any) -> AppConfig:
    return make_config(
        [
            {
                "name": "phone",
                "mention": True,
                "targets": ["telegram:main"],
                "quiet_hours": {"start": "23:00", "end": "07:00", **quiet},
            }
        ],
        tz="America/New_York",
    )


@pytest.mark.parametrize(
    ("utc", "quiet"),
    [
        (datetime(2026, 1, 15, 4, 0, tzinfo=timezone.utc), True),  # 23:00 EST (start, inclusive)
        (datetime(2026, 1, 15, 5, 0, tzinfo=timezone.utc), True),  # 00:00 EST (after midnight)
        (datetime(2026, 1, 15, 11, 59, tzinfo=timezone.utc), True),  # 06:59 EST
        (datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc), False),  # 07:00 EST (end, exclusive)
        (datetime(2026, 1, 15, 3, 59, tzinfo=timezone.utc), False),  # 22:59 EST
        (datetime(2026, 1, 15, 17, 0, tzinfo=timezone.utc), False),  # 12:00 EST
        (datetime(2026, 7, 15, 3, 30, tzinfo=timezone.utc), True),  # 23:30 EDT (DST offset honoured)
        (datetime(2026, 7, 15, 11, 30, tzinfo=timezone.utc), False),  # 07:30 EDT
        (datetime(2026, 7, 15, 10, 30, tzinfo=timezone.utc), True),  # 06:30 EDT
    ],
)
def test_quiet_hours_window_in_app_timezone(utc: datetime, quiet: bool) -> None:
    router, _, _ = router_for(_quiet_config(), now=utc)
    result = router.targets_for(make_alert(severity=Severity.HIGH))
    if quiet:
        assert result == ([], False, REASON_QUIET_HOURS)
    else:
        assert result == (["telegram:main"], True, None)


def test_quiet_hours_critical_passes_and_min_severity_is_configurable() -> None:
    midnight_est = datetime(2026, 1, 15, 5, 0, tzinfo=timezone.utc)
    router, _, _ = router_for(_quiet_config(), now=midnight_est)
    assert router.targets_for(make_alert(severity=Severity.CRITICAL)) == (["telegram:main"], True, None)
    assert router.targets_for(make_alert(severity=Severity.MEDIUM))[2] == REASON_QUIET_HOURS

    router, _, _ = router_for(_quiet_config(min_severity="high"), now=midnight_est)
    assert router.targets_for(make_alert(severity=Severity.HIGH))[0] == ["telegram:main"]
    assert router.targets_for(make_alert(severity=Severity.MEDIUM))[2] == REASON_QUIET_HOURS


def test_same_day_quiet_window() -> None:
    config = make_config(
        [{"name": "day", "targets": ["discord:a"], "quiet_hours": {"start": "09:00", "end": "17:00"}}],
        tz="Europe/Berlin",
    )
    # 2026-01-15 10:00 UTC = 11:00 CET -> inside
    router, _, clock = router_for(config, now=datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc))
    assert router.targets_for(make_alert())[2] == REASON_QUIET_HOURS
    clock.now = datetime(2026, 1, 15, 16, 0, tzinfo=timezone.utc)  # 17:00 CET -> outside
    assert router.targets_for(make_alert())[0] == ["discord:a"]
    clock.now = datetime(2026, 1, 15, 7, 59, tzinfo=timezone.utc)  # 08:59 CET -> outside
    assert router.targets_for(make_alert())[0] == ["discord:a"]


def test_quiet_route_targets_kept_when_another_active_route_lists_them() -> None:
    config = make_config(
        [
            {
                "name": "phone",
                "mention": True,
                "targets": ["telegram:main", "console"],
                "quiet_hours": {"start": "23:00", "end": "07:00"},
            },
            {"name": "feed", "targets": ["websocket", "console"]},
        ],
    )
    router, _, _ = router_for(config, now=datetime(2026, 1, 15, 2, 0, tzinfo=timezone.utc))
    targets, mention, reason = router.targets_for(make_alert(severity=Severity.HIGH))
    assert targets == ["websocket", "console"]  # telegram dropped, console survives via "feed"
    assert mention is False  # the suppressed route must not ping
    assert reason is None


def test_naive_clock_is_treated_as_utc() -> None:
    config = _quiet_config()
    router = AlertRouter(config, fakes_for(config), clock=lambda: datetime(2026, 1, 15, 5, 0))  # 00:00 EST
    assert router.targets_for(make_alert(severity=Severity.HIGH))[2] == REASON_QUIET_HOURS


# --------------------------------------------------------------------------- flood guard


def _flood_config(limit: int = 20, extra_routes: list[dict[str, Any]] | None = None) -> AppConfig:
    return make_config([{"name": "gpus", "max_per_minute": limit, "targets": ["discord:a", "console"]}, *(extra_routes or [])])


async def test_flood_guard_suppresses_21st_medium_but_critical_passes() -> None:
    metrics = Metrics()
    router, fakes, clock = router_for(_flood_config(), metrics=metrics)
    for i in range(20):
        report = await router.route(make_alert())
        assert report.suppressed_reason is None and report.any_ok, i
        clock.advance(1)

    flooded = await router.route(make_alert())
    assert flooded.results == []
    assert flooded.suppressed_reason == REASON_FLOOD_GUARD
    assert not flooded.all_failed and not flooded.any_ok

    high = await router.route(make_alert(severity=Severity.HIGH))
    assert high.suppressed_reason == REASON_FLOOD_GUARD  # only CRITICAL bypasses

    critical = await router.route(make_alert(severity=Severity.CRITICAL))
    assert critical.suppressed_reason is None
    assert [r.target for r in critical.results] == ["discord:a", "console"]
    assert len(fakes["discord:a"].sent) == 21

    assert metrics.counter("alerts_suppressed_total", labelnames=("reason",)).value(reason="flood_guard") == 2
    assert metrics.counter("route_suppressed_total", labelnames=("route", "reason")).value(route="gpus", reason="flood_guard") == 2


def test_flood_guard_sliding_window_and_recovery() -> None:
    router, _, clock = router_for(_flood_config(limit=3))
    t0 = clock.now
    assert router.targets_for(make_alert())[2] is None  # t0
    clock.advance(20)
    assert router.targets_for(make_alert())[2] is None  # t0+20
    clock.advance(20)
    assert router.targets_for(make_alert())[2] is None  # t0+40
    clock.advance(10)
    assert router.targets_for(make_alert())[2] == REASON_FLOOD_GUARD  # t0+50: 3 in window
    # Suppressed alerts do not consume budget: exactly at t0+60 the first one expires.
    clock.now = t0 + timedelta(seconds=FLOOD_WINDOW_SECONDS)
    assert router.targets_for(make_alert())[2] is None
    assert router.targets_for(make_alert())[2] == REASON_FLOOD_GUARD
    clock.now = t0 + timedelta(seconds=81)  # t0+20 expired too
    assert router.targets_for(make_alert())[0] == ["discord:a", "console"]


def test_critical_alerts_occupy_the_flood_window() -> None:
    router, _, _ = router_for(_flood_config(limit=2))
    for _ in range(3):
        assert router.targets_for(make_alert(severity=Severity.CRITICAL))[2] is None
    assert router.targets_for(make_alert(severity=Severity.MEDIUM))[2] == REASON_FLOOD_GUARD


def test_flood_guard_is_per_route_and_shared_targets_survive() -> None:
    config = _flood_config(limit=1, extra_routes=[{"name": "feed", "targets": ["console", "websocket"]}])
    router, _, _ = router_for(config)
    assert router.targets_for(make_alert()) == (["discord:a", "console", "websocket"], False, None)
    # "gpus" is now flooded; its console target still goes out through "feed".
    assert router.targets_for(make_alert()) == (["console", "websocket"], False, None)


def test_flood_guard_logs_engage_once_and_release(caplog: pytest.LogCaptureFixture) -> None:
    router, _, clock = router_for(_flood_config(limit=1))
    with caplog.at_level(logging.INFO, logger=ROUTER_LOGGER):
        router.targets_for(make_alert())
        for _ in range(5):
            router.targets_for(make_alert())
        clock.advance(61)
        router.targets_for(make_alert())
    engaged = [r for r in caplog.records if "flood guard engaged" in r.getMessage()]
    released = [r for r in caplog.records if "flood guard released" in r.getMessage()]
    assert len(engaged) == 1
    assert len(released) == 1 and released[0].suppressed == 5  # type: ignore[attr-defined]


def test_mixed_suppression_reports_first_suppressed_route() -> None:
    config = make_config(
        [
            {"name": "phone", "targets": ["telegram:main"], "quiet_hours": {"start": "00:00", "end": "23:59"}},
            {"name": "gpus", "max_per_minute": 1, "targets": ["discord:a"]},
        ]
    )
    router, _, _ = router_for(config)  # 12:00 UTC -> inside the quiet window
    assert router.targets_for(make_alert()) == (["discord:a"], False, None)
    assert router.targets_for(make_alert()) == ([], False, REASON_QUIET_HOURS)


# --------------------------------------------------------------------------- dry run


async def test_dry_run_routes_everything_to_console_only() -> None:
    config = make_config(
        [{"name": "errors", "mention": True, "targets": ["discord:a", "telegram:main", "websocket"]}],
        dry_run=True,
        system_targets=["console", "discord:ops"],
    )
    router, fakes, _ = router_for(config)
    report = await router.route(make_alert(severity=Severity.CRITICAL))
    assert [r.target for r in report.results] == ["console"]
    assert len(fakes["console"].sent) == 1
    assert all(not fakes[t].sent for t in ("discord:a", "telegram:main", "websocket"))

    # even alerts no route would carry are shown on the console in a dry run
    unrouted = make_config([{"name": "x", "min_severity": "critical", "targets": ["discord:a"]}], dry_run=True)
    router, fakes, _ = router_for(unrouted)
    assert router.targets_for(make_alert()) == (["console"], False, None)

    await router_for(config)[0].notify("t", "m")


async def test_dry_run_notices_go_to_console_only() -> None:
    config = make_config([{"name": "a", "targets": ["discord:a"]}], dry_run=True, system_targets=["console", "discord:ops"])
    router, fakes, _ = router_for(config)
    await router.notify("ebay blocked", "pausing 300s")
    assert fakes["console"].notices == [("ebay blocked", "pausing 300s")]
    assert fakes["discord:ops"].notices == []


async def test_console_fallback_when_mapping_lacks_console() -> None:
    config = make_config([{"name": "a", "targets": ["console"]}])
    router = AlertRouter(config, {}, clock=Clock(WINTER_NOON_UTC))
    report = await router.route(make_alert())
    assert [(r.target, r.ok) for r in report.results] == [("console", True)]


# --------------------------------------------------------------------------- delivery


async def test_route_dispatches_concurrently_and_sets_mention() -> None:
    config = make_config([{"name": "all", "mention": True, "targets": ["discord:a", "discord:b", "telegram:main"]}])
    fakes = fakes_for(
        config,
        discord__a=FakeDispatcher("discord:a", delay=0.2),
        discord__b=FakeDispatcher("discord:b", delay=0.2),
        telegram__main=FakeDispatcher("telegram:main", delay=0.2),
    )
    router, _, _ = router_for(config, fakes)
    alert = make_alert()
    started = time.perf_counter()
    report = await router.route(alert)
    elapsed = time.perf_counter() - started
    assert elapsed < 0.5  # concurrent, not 3 x 0.2 s
    assert report.alert_id == alert.alert_id
    assert [r.target for r in report.results] == ["discord:a", "discord:b", "telegram:main"]
    assert report.any_ok and not report.all_failed
    assert alert.mention is True
    assert fakes["discord:a"].mentions == [True]


async def test_timeout_becomes_failed_result_while_others_succeed() -> None:
    config = make_config([{"name": "all", "targets": ["discord:a", "console"]}], timeout=0.05)
    slow = FakeDispatcher("discord:a", delay=5.0)
    router, fakes, _ = router_for(config, fakes_for(config, discord__a=slow))
    started = time.perf_counter()
    report = await router.route(make_alert())
    assert time.perf_counter() - started < 1.0
    by_target = {r.target: r for r in report.results}
    assert by_target["discord:a"].ok is False
    assert "timeout" in (by_target["discord:a"].error or "")
    assert by_target["discord:a"].latency_ms >= 40
    assert by_target["console"].ok is True
    assert report.any_ok and not report.all_failed
    assert slow.started == 1 and slow.sent == []


async def test_raising_dispatcher_becomes_failed_result() -> None:
    config = make_config([{"name": "all", "targets": ["discord:a", "discord:b"]}])
    boom = FakeDispatcher("discord:a", raise_exc=RuntimeError("socket exploded"))
    router, _, _ = router_for(config, fakes_for(config, discord__a=boom))
    report = await router.route(make_alert())
    a, b = report.results
    assert (a.target, a.ok) == ("discord:a", False)
    assert "RuntimeError" in (a.error or "") and "socket exploded" in (a.error or "")
    assert (b.target, b.ok) == ("discord:b", True)


async def test_invalid_return_and_stray_cancellation_become_failures() -> None:
    config = make_config([{"name": "all", "targets": ["discord:a", "discord:b", "console"]}])
    fakes = fakes_for(
        config,
        discord__a=WrongTypeDispatcher("discord:a"),
        discord__b=SelfCancellingDispatcher("discord:b"),
    )
    router, _, _ = router_for(config, fakes)
    report = await router.route(make_alert())
    by_target = {r.target: r for r in report.results}
    assert by_target["discord:a"].ok is False and "expected DispatchResult" in (by_target["discord:a"].error or "")
    assert by_target["discord:b"].ok is False and by_target["discord:b"].error == "cancelled"
    assert by_target["console"].ok is True


async def test_caller_cancellation_propagates() -> None:
    config = make_config([{"name": "all", "targets": ["discord:a"]}])
    slow = FakeDispatcher("discord:a", delay=10.0)
    router, _, _ = router_for(config, fakes_for(config, discord__a=slow))
    task = asyncio.create_task(router.route(make_alert()))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_route_never_raises_when_routing_itself_fails() -> None:
    config = make_config([{"name": "all", "targets": ["discord:a"]}])

    def broken_clock() -> datetime:
        raise RuntimeError("clock unavailable")

    router = AlertRouter(config, fakes_for(config), clock=broken_clock)
    report = await router.route(make_alert())
    assert report.results == [] and report.suppressed_reason is None


async def test_result_target_normalised_and_latency_filled() -> None:
    config = make_config([{"name": "all", "targets": ["discord:a"]}])
    odd = FakeDispatcher("discord:a", result_target="discord:something_else", latency_ms=0.0)
    router, _, _ = router_for(config, fakes_for(config, discord__a=odd))
    (result,) = (await router.route(make_alert())).results
    assert result.target == "discord:a"
    assert result.latency_ms > 0


async def test_all_failed_and_any_ok_semantics() -> None:
    config = make_config([{"name": "all", "targets": ["discord:a", "discord:b"]}])
    fakes = fakes_for(config, discord__a=FakeDispatcher("discord:a", ok=False), discord__b=FakeDispatcher("discord:b", ok=False))
    router, _, _ = router_for(config, fakes)
    report = await router.route(make_alert())
    assert len(report.results) == 2 and report.all_failed and not report.any_ok

    fakes["discord:b"].ok = True
    report = await router.route(make_alert())
    assert report.any_ok and not report.all_failed

    empty = DispatchReport(alert_id="x")
    assert not empty.all_failed and not empty.any_ok


async def test_no_route_report() -> None:
    config = make_config([{"name": "loud", "min_severity": "critical", "targets": ["discord:a"]}])
    router, fakes, _ = router_for(config)
    alert = make_alert()
    report = await router.route(alert)
    assert report.results == [] and report.suppressed_reason == REASON_NO_ROUTE
    assert alert.mention is False
    assert fakes["discord:a"].sent == []


async def test_quiet_hours_report_and_critical_delivery() -> None:
    router, fakes, _ = router_for(_quiet_config(), now=datetime(2026, 1, 15, 5, 0, tzinfo=timezone.utc))
    quiet = await router.route(make_alert(severity=Severity.HIGH))
    assert quiet.results == [] and quiet.suppressed_reason == REASON_QUIET_HOURS
    loud = await router.route(make_alert(severity=Severity.CRITICAL))
    assert [r.target for r in loud.results] == ["telegram:main"] and loud.any_ok
    assert fakes["telegram:main"].mentions == [True]


async def test_unconfigured_and_unknown_targets_skipped_with_single_warning(caplog: pytest.LogCaptureFixture) -> None:
    config = make_config(
        [
            {"name": "one", "targets": ["discord:a", "discord:b", "console"]},
            {"name": "two", "targets": ["discord:a", "discord:b"]},
        ]
    )
    fakes = fakes_for(config, discord__a=FakeDispatcher("discord:a", configured=False))
    del fakes["discord:b"]  # declared in config but no dispatcher was built
    metrics = Metrics()
    router, _, _ = router_for(config, fakes, metrics=metrics)
    with caplog.at_level(logging.WARNING, logger=ROUTER_LOGGER):
        for _ in range(3):
            report = await router.route(make_alert())
            assert [r.target for r in report.results] == ["console"]
    assert fakes["discord:a"].sent == []
    warnings = [r for r in caplog.records if "skipping" in r.getMessage()]
    assert sorted(r.target for r in warnings) == ["discord:a", "discord:b"]  # type: ignore[attr-defined]
    skipped = metrics.counter("dispatch_skipped_total", labelnames=("target", "reason"))
    assert skipped.value(target="discord:a", reason="unconfigured") == 3
    assert skipped.value(target="discord:b", reason="unknown") == 3

    # a target that becomes configured is used again; if it drops out later it warns again
    fakes["discord:a"].configured = True
    report = await router.route(make_alert())
    assert [r.target for r in report.results] == ["discord:a", "console"]
    fakes["discord:a"].configured = False
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=ROUTER_LOGGER):
        await router.route(make_alert())
    assert [r.target for r in caplog.records if "skipping" in r.getMessage()] == ["discord:a"]  # type: ignore[attr-defined]


async def test_all_targets_unusable_is_a_failure_not_a_suppression() -> None:
    config = make_config([{"name": "one", "targets": ["discord:a"]}])
    router, _, _ = router_for(config, fakes_for(config, discord__a=FakeDispatcher("discord:a", configured=False)))
    report = await router.route(make_alert())
    assert report.results == [] and report.suppressed_reason is None  # pipeline rolls the claim back


async def test_metrics_are_recorded() -> None:
    config = make_config([{"name": "all", "targets": ["discord:a", "discord:b"]}])
    metrics = Metrics()
    fakes = fakes_for(config, discord__b=FakeDispatcher("discord:b", ok=False))
    router, _, _ = router_for(config, fakes, metrics=metrics)
    await router.route(make_alert(severity=Severity.HIGH))
    await router.route(make_alert(severity=Severity.HIGH))
    await router.route(make_alert(severity=Severity.CRITICAL))
    routed = metrics.counter("alerts_routed_total", labelnames=("severity",))
    assert routed.value(severity="high") == 2 and routed.value(severity="critical") == 1
    total = metrics.counter("dispatch_total", labelnames=("target", "ok"))
    assert total.value(target="discord:a", ok="true") == 3
    assert total.value(target="discord:b", ok="false") == 3
    assert metrics.histogram("dispatch_ms", labelnames=("target",)).count(target="discord:a") == 3
    assert "dealradar_dispatch_total" in metrics.render()


# --------------------------------------------------------------------------- notices + lifecycle


async def test_notify_fans_out_to_system_targets_and_swallows_errors() -> None:
    config = make_config(
        [{"name": "a", "targets": ["discord:a"]}],
        timeout=0.05,
        system_targets=["console", "discord:ops", "discord:a", "discord:b", "telegram:main"],
    )
    fakes = fakes_for(
        config,
        discord__ops=FakeDispatcher("discord:ops", notice_delay=0.2),  # concurrent with the others
        discord__a=FakeDispatcher("discord:a", notice_raise=RuntimeError("down")),
        discord__b=FakeDispatcher("discord:b", notice_ok=False),
        telegram__main=FakeDispatcher("telegram:main", configured=False),
    )
    metrics = Metrics()
    router, _, _ = router_for(config, fakes, metrics=metrics)
    started = time.perf_counter()
    await router.notify("reddit blocked", "429 storm")
    assert time.perf_counter() - started < 0.5
    assert fakes["console"].notices == [("reddit blocked", "429 storm")]
    assert fakes["discord:b"].notices == [("reddit blocked", "429 storm")]
    assert fakes["discord:ops"].notices == []  # timed out
    assert fakes["telegram:main"].notices == []  # unconfigured -> skipped
    notices = metrics.counter("notices_total", labelnames=("target", "ok"))
    assert notices.value(target="console", ok="true") == 1
    assert notices.value(target="discord:ops", ok="false") == 1
    assert notices.value(target="discord:a", ok="false") == 1
    assert notices.value(target="discord:b", ok="false") == 1


async def test_notify_with_default_console_dispatcher_and_no_usable_targets() -> None:
    config = make_config([{"name": "a", "targets": ["discord:a"]}], system_targets=["console"])
    router = AlertRouter(config, {"console": ConsoleDispatcher()}, clock=Clock(WINTER_NOON_UTC))
    await router.notify("hello", "world")

    config = make_config([{"name": "a", "targets": ["discord:a"]}], system_targets=["discord:ops"])
    fakes = fakes_for(config, discord__ops=FakeDispatcher("discord:ops", configured=False))
    router, _, _ = router_for(config, fakes)
    await router.notify("hello", "world")  # nothing usable -> silently nothing
    assert fakes["discord:ops"].notices == []


async def test_close_closes_each_dispatcher_once_and_swallows_errors() -> None:
    config = make_config([{"name": "a", "targets": ["discord:a"]}])
    shared = FakeDispatcher("discord:a")
    fakes = fakes_for(config, discord__a=shared, discord__b=FakeDispatcher("discord:b", close_raise=RuntimeError("x")))
    fakes["discord:c"] = shared  # same object under two names
    router, _, _ = router_for(config, fakes)
    await router.close()
    await router.close()  # idempotent
    assert shared.closed == 1
    assert fakes["discord:b"].closed == 1
    assert fakes["console"].closed == 1


# --------------------------------------------------------------------------- shipped config.yaml


@pytest.fixture
def shipped() -> AppConfig:
    config = load_config(CONFIG_PATH, env={})
    assert config.app.dry_run is False
    return config


def test_shipped_config_routes(shipped: AppConfig) -> None:
    router, _, _ = router_for(shipped, now=datetime(2026, 3, 10, 18, 0, tzinfo=timezone.utc))  # 14:00 EDT

    price_error = make_alert(severity=Severity.HIGH, profile_id="rtx_4090", category="gpu", price_error=True)
    assert router.targets_for(price_error) == (
        ["discord:price_errors", "telegram:main", "websocket", "console", "discord:gpu"],
        True,
        None,
    )

    high_gpu = make_alert(severity=Severity.HIGH, profile_id="rtx_4090", category="gpu")
    assert router.targets_for(high_gpu) == (["discord:gpu", "websocket", "console", "telegram:main"], False, None)

    medium_gpu = make_alert(severity=Severity.MEDIUM, profile_id="rtx_a6000", category="workstation_gpu")
    assert router.targets_for(medium_gpu) == (["discord:gpu", "websocket", "console"], False, None)

    monitor = make_alert(severity=Severity.MEDIUM, profile_id="oled_4k_monitor", category="monitor")
    assert router.targets_for(monitor) == (["discord:displays", "websocket", "console"], False, None)

    camera = make_alert(severity=Severity.HIGH, profile_id="sony_a7iv", category="camera")
    assert router.targets_for(camera) == (["discord:general", "websocket", "console"], False, None)

    critical_camera = make_alert(severity=Severity.CRITICAL, profile_id="sony_a7iv", category="camera")
    assert router.targets_for(critical_camera) == (
        ["discord:price_errors", "telegram:main", "websocket", "console", "discord:general"],
        True,
        None,
    )

    uncovered = make_alert(severity=Severity.HIGH, profile_id="rtx_4090", category="laptop")
    assert router.targets_for(uncovered) == ([], False, REASON_NO_ROUTE)


def test_shipped_config_gpu_phone_quiet_hours(shipped: AppConfig) -> None:
    # 2026-03-10 07:00 UTC = 03:00 EDT (DST began 2026-03-08): inside 00:30-07:30
    router, _, clock = router_for(shipped, now=datetime(2026, 3, 10, 7, 0, tzinfo=timezone.utc))
    high_gpu = make_alert(severity=Severity.HIGH, profile_id="rtx_5090", category="gpu")
    assert router.targets_for(high_gpu) == (["discord:gpu", "websocket", "console"], False, None)

    critical_gpu = make_alert(severity=Severity.CRITICAL, profile_id="rtx_5090", category="gpu")
    targets, mention, _ = router.targets_for(critical_gpu)
    assert "telegram:main" in targets and mention is True

    clock.now = datetime(2026, 3, 10, 11, 30, tzinfo=timezone.utc)  # 07:30 EDT -> window over
    assert router.targets_for(high_gpu)[0] == ["discord:gpu", "websocket", "console", "telegram:main"]


async def test_shipped_config_without_credentials_uses_websocket_and_console(shipped: AppConfig) -> None:
    fakes = {t: FakeDispatcher(t, configured=t in ("websocket", "console")) for t in shipped.known_targets()}
    router, _, _ = router_for(shipped, fakes)
    report = await router.route(make_alert(severity=Severity.CRITICAL, profile_id="rtx_4090", category="gpu", price_error=True))
    assert [r.target for r in report.results] == ["websocket", "console"]
    assert report.any_ok
    await router.notify("source blocked", "fb_marketplace checkpoint")
    assert fakes["console"].notices == [("source blocked", "fb_marketplace checkpoint")]
    assert fakes["discord:ops"].notices == []


async def test_shipped_config_gpu_flood_guard(shipped: AppConfig) -> None:
    router, fakes, clock = router_for(shipped, now=datetime(2026, 3, 10, 18, 0, tzinfo=timezone.utc))
    for _ in range(20):
        report = await router.route(make_alert(profile_id="rtx_3090", category="gpu"))
        assert report.suppressed_reason is None
        clock.advance(2)
    flooded = await router.route(make_alert(profile_id="rtx_3090", category="gpu"))
    assert flooded.results == [] and flooded.suppressed_reason == REASON_FLOOD_GUARD

    critical = await router.route(make_alert(severity=Severity.CRITICAL, profile_id="rtx_3090", category="gpu"))
    assert "discord:gpu" in [r.target for r in critical.results] and critical.any_ok

    # monitors have their own budget
    monitor = await router.route(make_alert(profile_id="oled_4k_monitor", category="monitor"))
    assert monitor.suppressed_reason is None and monitor.any_ok

    clock.advance(61)
    recovered = await router.route(make_alert(profile_id="rtx_3090", category="gpu"))
    assert recovered.suppressed_reason is None
    assert len(fakes["discord:gpu"].sent) == 22
