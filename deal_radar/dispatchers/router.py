"""Alert routing: which channels get an alert, when, and how loudly.

Design decisions
----------------
* **Routing is a pure decision, delivery is separate.** :meth:`AlertRouter.targets_for`
  evaluates every ``dispatch.routes`` rule against an alert and returns
  ``(targets, mention, suppressed_reason)`` without doing any I/O; :meth:`route` only
  fans the alert out. That keeps the policy (severity, filters, quiet hours, flood
  guard) unit-testable and makes the reported targets exactly what is dispatched.
* **Union of matching routes.** A route matches when ``alert.severity`` is at least
  ``min_severity``, the ``profiles`` / ``categories`` / ``sources`` filters accept the
  alert (an empty filter accepts everything) and, for ``price_error_only`` routes, the
  score flagged a price error. Targets of all *active* matching routes are merged in
  config order without duplicates; ``mention`` is set when any active route asks for
  it, so a suppressed route can never ping anyone.
* **Quiet hours** are evaluated on the wall clock of ``app.timezone`` (DST-aware via
  ``zoneinfo``) using the injected clock, as the half-open window ``[start, end)``.
  ``start > end`` crosses midnight (``23:00-07:00``); ``start == end`` is an empty
  window. Inside the window a route only carries alerts of at least
  ``quiet_hours.min_severity``.
* **Flood guard.** Routes with ``max_per_minute`` keep a deque of the timestamps of
  the alerts they carried during the last 60 s (a true sliding window, not a fixed
  bucket). A non-critical alert that would exceed the budget drops the route; CRITICAL
  alerts always pass and still occupy the window, because they flood a channel just
  as much. Suppressed alerts do not consume budget. One warning is logged when a
  route starts shedding and an info line (with the shed count) when it recovers.
* **Suppression only drops what nothing else carries.** A target of a quiet / flooded
  route is still delivered when another active matching route lists it. When nothing
  is left, ``suppressed_reason`` explains why: ``"no_route"`` (no route matched),
  otherwise ``"quiet_hours"`` / ``"flood_guard"`` of the first suppressed route in
  config order. The pipeline keeps the dedup claim for suppressed alerts so the deal
  does not page someone the moment quiet hours end. If no route was suppressed but
  every target of the active routes is unusable, the reason stays ``None`` and the
  report is empty, which the pipeline treats as a failed delivery (claim rolled back).
* **Dry run** (``app.dry_run``) sends every alert and operator notice to ``console``
  only, regardless of routes, so a dry run shows everything the engine would alert on
  without anything leaving the machine. A :class:`ConsoleDispatcher` is provided when
  the mapping lacks one, since the console is always a valid target.
* **Unusable targets are skipped, not fatal.** A target missing from the dispatcher
  mapping, or whose ``configured`` is ``False`` (no credentials, or a webhook the
  dispatcher disabled at runtime after a 401/404), is skipped with a single warning
  per target (re-armed once the target becomes usable again).
* **Delivery never raises.** Targets are sent concurrently with ``asyncio.gather``;
  each send is bounded by ``dispatch.timeout_seconds``. A timeout, an exception or an
  invalid return value becomes a failed :class:`DispatchResult`, so one slow or broken
  channel can neither delay nor sink the others. Cancellation of the caller is
  re-raised; a stray ``CancelledError`` raised by a dispatcher while the caller is not
  being cancelled is treated as a failed delivery.
* **Metrics:** ``alerts_routed_total{severity}``, ``alerts_suppressed_total{reason}``,
  ``route_suppressed_total{route,reason}``, ``dispatch_total{target,ok}``,
  ``dispatch_ms{target}``, ``dispatch_skipped_total{target,reason}`` and
  ``notices_total{target,ok}``.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from deal_radar.config_schema import AppConfig, RouteRule
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.dispatchers.base import ConsoleDispatcher, Dispatcher
from deal_radar.engine.types import Alert, DispatchReport, DispatchResult, Severity, utcnow

log = get_logger("dispatch.router")

FLOOD_WINDOW_SECONDS = 60.0
REASON_NO_ROUTE = "no_route"
REASON_QUIET_HOURS = "quiet_hours"
REASON_FLOOD_GUARD = "flood_guard"
_ERROR_MAX_LEN = 500
_MINUTES_PER_DAY = 24 * 60


def parse_hhmm(value: str) -> int:
    """``"HH:MM"`` -> minutes after midnight. Raises ``ValueError`` on malformed input."""
    hours, sep, minutes = value.strip().partition(":")
    if not sep or not hours.isdigit() or not minutes.isdigit() or len(minutes) != 2:
        raise ValueError(f"expected HH:MM, got {value!r}")
    h, m = int(hours), int(minutes)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"expected HH:MM, got {value!r}")
    return h * 60 + m


def in_window(minute_of_day: int, start: int, end: int) -> bool:
    """Whether ``minute_of_day`` lies in the half-open window ``[start, end)``.

    ``start > end`` wraps around midnight; ``start == end`` is an empty window.
    """
    if start == end:
        return False
    if start < end:
        return start <= minute_of_day < end
    return minute_of_day >= start or minute_of_day < end


@dataclass(slots=True)
class _Route:
    """A ``RouteRule`` with its filters pre-built as sets and its flood-guard state."""

    rule: RouteRule
    profiles: frozenset[str]
    categories: frozenset[str]
    sources: frozenset[str]
    quiet: tuple[int, int, Severity] | None
    window: deque[float] = field(default_factory=deque)
    shedding: bool = False
    shed_count: int = 0

    @classmethod
    def build(cls, rule: RouteRule) -> "_Route":
        quiet = None
        if rule.quiet_hours is not None:
            quiet = (parse_hhmm(rule.quiet_hours.start), parse_hhmm(rule.quiet_hours.end), rule.quiet_hours.min_severity)
        return cls(
            rule=rule,
            profiles=frozenset(rule.profiles),
            categories=frozenset(rule.categories),
            sources=frozenset(rule.sources),
            quiet=quiet,
        )

    @property
    def name(self) -> str:
        return self.rule.name

    def matches(self, alert: Alert) -> bool:
        rule = self.rule
        if not alert.severity.at_least(rule.min_severity):
            return False
        if rule.price_error_only and not alert.score.is_price_error:
            return False
        if self.profiles and alert.profile_id not in self.profiles:
            return False
        if self.categories and alert.category not in self.categories:
            return False
        if self.sources and alert.item.source not in self.sources:
            return False
        return True

    def quiet_blocks(self, alert: Alert, minute_of_day: int) -> bool:
        if self.quiet is None:
            return False
        start, end, min_severity = self.quiet
        return in_window(minute_of_day, start, end) and not alert.severity.at_least(min_severity)

    def flood_admit(self, alert: Alert, now_ts: float) -> bool:
        """Sliding-window admission; records the alert when admitted."""
        limit = self.rule.max_per_minute
        if limit is None:
            return True
        window = self.window
        horizon = now_ts - FLOOD_WINDOW_SECONDS
        while window and window[0] <= horizon:
            window.popleft()
        if alert.severity is not Severity.CRITICAL and len(window) >= limit:
            return False
        # Keep the deque monotonic even if the wall clock steps backwards (NTP), so
        # pruning from the left stays correct.
        window.append(max(now_ts, window[-1]) if window else now_ts)
        return True


class AlertRouter:
    """Decides the targets of each alert and delivers it to them concurrently."""

    def __init__(
        self,
        config: AppConfig,
        dispatchers: Mapping[str, Dispatcher],
        *,
        metrics: Metrics | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.config = config
        self.metrics = metrics or Metrics()
        self.clock = clock
        self._dispatchers: dict[str, Dispatcher] = dict(dispatchers)
        self._dispatchers.setdefault("console", ConsoleDispatcher())
        self._tz = ZoneInfo(config.app.timezone)
        self._timeout = config.dispatch.timeout_seconds
        self._routes = [_Route.build(rule) for rule in config.dispatch.routes]
        self._system_targets = list(dict.fromkeys(config.dispatch.system_targets))
        self._warned: set[str] = set()
        self._closed = False
        m = self.metrics
        self._m_routed = m.counter("alerts_routed_total", "Alerts handed to the router", ("severity",))
        self._m_suppressed = m.counter("alerts_suppressed_total", "Alerts not delivered because of routing policy", ("reason",))
        self._m_route_suppressed = m.counter(
            "route_suppressed_total", "Matching routes dropped by quiet hours / flood guard", ("route", "reason")
        )
        self._m_dispatch = m.counter("dispatch_total", "Alert deliveries per target", ("target", "ok"))
        self._m_dispatch_ms = m.histogram("dispatch_ms", "Alert delivery latency per target (ms)", ("target",))
        self._m_skipped = m.counter("dispatch_skipped_total", "Targets skipped as unusable", ("target", "reason"))
        self._m_notices = m.counter("notices_total", "Operator notices per target", ("target", "ok"))

    # ------------------------------------------------------------------ routing policy

    def targets_for(self, alert: Alert) -> tuple[list[str], bool, str | None]:
        """Return ``(targets, mention, suppressed_reason)`` for ``alert``.

        Only usable targets are returned. Admitting an alert through a route with a
        flood guard records it in that route's window, so call this once per alert.
        """
        if self.config.app.dry_run:
            mention = any(route.rule.mention for route in self._routes if route.matches(alert))
            return [t for t in ("console",) if self._usable(t)], mention, None

        now = self._now()
        minute_of_day = self._local_minute(now)
        now_ts = now.timestamp()
        targets: list[str] = []
        seen: set[str] = set()
        mention = False
        matched = False
        active = False
        first_suppression: str | None = None
        for route in self._routes:
            if not route.matches(alert):
                continue
            matched = True
            reason: str | None = None
            if route.quiet_blocks(alert, minute_of_day):
                reason = REASON_QUIET_HOURS
            elif not route.flood_admit(alert, now_ts):
                reason = REASON_FLOOD_GUARD
            if reason is not None:
                self._note_suppressed(route, reason, alert)
                first_suppression = first_suppression or reason
                continue
            self._note_admitted(route, alert)
            active = True
            mention = mention or route.rule.mention
            for target in route.rule.targets:
                if target not in seen:
                    seen.add(target)
                    targets.append(target)

        usable = [t for t in targets if self._usable(t)]
        if usable:
            return usable, mention, None
        if not matched:
            return [], False, REASON_NO_ROUTE
        # Nothing deliverable: name the policy that removed a route, if any; ``None``
        # means active routes exist but all their targets are unusable (a failure).
        return [], mention if active else False, first_suppression

    def _now(self) -> datetime:
        now = self.clock()
        return now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)

    def _local_minute(self, now: datetime) -> int:
        local = now.astimezone(self._tz)
        return (local.hour * 60 + local.minute) % _MINUTES_PER_DAY

    def _note_suppressed(self, route: _Route, reason: str, alert: Alert) -> None:
        self._m_route_suppressed.inc(route=route.name, reason=reason)
        if reason == REASON_FLOOD_GUARD:
            route.shed_count += 1
            if not route.shedding:
                route.shedding = True
                log.warning(
                    "flood guard engaged; shedding non-critical alerts",
                    extra={"route": route.name, "max_per_minute": route.rule.max_per_minute},
                )
        log.debug(
            "route suppressed",
            extra={"route": route.name, "reason": reason, "alert_id": alert.alert_id, "severity": alert.severity.value},
        )

    def _note_admitted(self, route: _Route, alert: Alert) -> None:
        if route.shedding and alert.severity is not Severity.CRITICAL:
            log.info("flood guard released", extra={"route": route.name, "suppressed": route.shed_count})
            route.shedding = False
            route.shed_count = 0

    def _usable(self, target: str) -> bool:
        dispatcher = self._dispatchers.get(target)
        if dispatcher is None:
            self._skip(target, "unknown")
            return False
        try:
            configured = bool(dispatcher.configured)
        except Exception:  # noqa: BLE001 - a broken property must not break routing
            configured = False
        if not configured:
            self._skip(target, "unconfigured")
            return False
        self._warned.discard(target)
        return True

    def _skip(self, target: str, reason: str) -> None:
        self._m_skipped.inc(target=target, reason=reason)
        if target not in self._warned:
            self._warned.add(target)
            if reason == "unknown":
                log.warning("dispatch target has no dispatcher; skipping it", extra={"target": target})
            else:
                log.warning("dispatch target is not configured; skipping it", extra={"target": target})

    # ------------------------------------------------------------------ delivery

    async def route(self, alert: Alert) -> DispatchReport:
        """Route and deliver ``alert``; never raises (except on caller cancellation)."""
        self._m_routed.inc(severity=alert.severity.value)
        try:
            targets, mention, reason = self.targets_for(alert)
        except Exception:  # noqa: BLE001 - routing bugs must surface as a failed delivery, not a crash
            log.exception("alert routing failed", extra={"alert_id": alert.alert_id})
            return DispatchReport(alert_id=alert.alert_id)
        alert.mention = mention
        if not targets:
            if reason is not None:
                self._m_suppressed.inc(reason=reason)
                log.info(
                    "alert suppressed",
                    extra={"alert_id": alert.alert_id, "reason": reason, "severity": alert.severity.value, "profile": alert.product_key},
                )
            else:
                log.warning("alert has no usable dispatch target", extra={"alert_id": alert.alert_id, "profile": alert.product_key})
            return DispatchReport(alert_id=alert.alert_id, suppressed_reason=reason)

        results = await asyncio.gather(*(self._deliver(target, alert) for target in targets))
        report = DispatchReport(alert_id=alert.alert_id, results=list(results))
        if report.all_failed:
            log.warning(
                "alert delivery failed on every target",
                extra={"alert_id": alert.alert_id, "errors": {r.target: r.error for r in report.results}},
            )
        return report

    async def _deliver(self, target: str, alert: Alert) -> DispatchResult:
        dispatcher = self._dispatchers[target]
        started = time.perf_counter()
        result = await self._guarded(target, lambda: dispatcher.send(alert))
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if result.latency_ms <= 0:
            result = result.model_copy(update={"latency_ms": round(elapsed_ms, 3)})
        self._m_dispatch.inc(target=target, ok="true" if result.ok else "false")
        self._m_dispatch_ms.observe(elapsed_ms, target=target)
        if not result.ok:
            log.warning(
                "alert delivery failed",
                extra={"alert_id": alert.alert_id, "target": target, "status": result.status, "error": result.error},
            )
        return result

    async def _guarded(self, target: str, call: Callable[[], Awaitable[DispatchResult]]) -> DispatchResult:
        """Run one dispatcher call under the per-target timeout; convert every failure."""
        started = time.perf_counter()

        def failed(error: str) -> DispatchResult:
            elapsed = round((time.perf_counter() - started) * 1000.0, 3)
            return DispatchResult(target=target, ok=False, latency_ms=elapsed, error=error[:_ERROR_MAX_LEN])

        try:
            result: object = await asyncio.wait_for(call(), timeout=self._timeout)
        except TimeoutError:
            return failed(f"timeout after {self._timeout:g}s")
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if task is None or task.cancelling():
                raise
            return failed("cancelled")
        except Exception as exc:  # noqa: BLE001 - dispatchers must not take routing down
            log.debug("dispatcher raised", exc_info=True, extra={"target": target})
            return failed(f"{type(exc).__name__}: {exc}")
        if not isinstance(result, DispatchResult):
            return failed(f"dispatcher returned {type(result).__name__}, expected DispatchResult")
        if result.target != target:
            result = result.model_copy(update={"target": target})
        return result

    # ------------------------------------------------------------------ operator notices

    async def notify(self, title: str, message: str) -> None:
        """Send an operator notice to ``dispatch.system_targets`` (console in dry run). Never raises."""
        try:
            targets = ["console"] if self.config.app.dry_run else self._system_targets
            usable = [t for t in targets if self._usable(t)]
            if not usable:
                return
            await asyncio.gather(*(self._notice(t, title, message) for t in usable))
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - notices are best effort
            log.exception("operator notice failed", extra={"title": title})

    async def _notice(self, target: str, title: str, message: str) -> None:
        dispatcher = self._dispatchers[target]
        result = await self._guarded(target, lambda: dispatcher.send_notice(title, message))
        self._m_notices.inc(target=target, ok="true" if result.ok else "false")
        if not result.ok:
            log.warning("operator notice not delivered", extra={"target": target, "error": result.error})

    # ------------------------------------------------------------------ lifecycle

    async def close(self) -> None:
        """Close every dispatcher once (idempotent); errors are logged, not raised."""
        if self._closed:
            return
        self._closed = True
        unique: dict[int, Dispatcher] = {}
        for dispatcher in self._dispatchers.values():
            unique.setdefault(id(dispatcher), dispatcher)

        async def _close(dispatcher: Dispatcher) -> None:
            try:
                await asyncio.wait_for(dispatcher.close(), timeout=self._timeout)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep closing the rest
                log.warning("dispatcher close failed", exc_info=True, extra={"target": getattr(dispatcher, "target", "?")})

        await asyncio.gather(*(_close(d) for d in unique.values()))


__all__ = [
    "AlertRouter",
    "FLOOD_WINDOW_SECONDS",
    "REASON_FLOOD_GUARD",
    "REASON_NO_ROUTE",
    "REASON_QUIET_HOURS",
    "in_window",
    "parse_hhmm",
]
