"""Per-listing processing pipeline and the bus-driven worker pool.

``Pipeline.process`` takes one :class:`RawListing` through every engine stage:

    normalize → text filter → preliminary score → [vision] → final score → history
    → snapshot → severity gate → atomic dedup claim → route/dispatch → (rollback on failure)

Latency accounting
------------------
``Alert.pipeline_ms`` is the *internal* overhead: wall time spent in this process
minus time spent waiting on the vision model and on dispatch network I/O. That is
the number the < 50 ms budget applies to (normalize ≈ 20 µs, filter ≈ 100 µs,
scoring ≈ 100 µs, Redis claim ≈ 0.3-1 ms on the same VM). Vision inference is
measured separately because it only runs for local-marketplace candidates that
already passed a preliminary score.

Failure semantics
-----------------
* A stage exception never kills a worker: the item is counted as ``error`` and
  the worker moves on.
* If the shared Redis deduplicator is unreachable, the pipeline falls back to a
  process-local deduplicator (alerts keep flowing; the once-only guarantee
  degrades from cluster-wide to per-process until Redis recovers).
* If every dispatch target fails, the dedup claim is rolled back so a later
  observation (or another node) can alert again instead of silently losing it.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime

from deal_radar.config_schema import AppConfig
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.db.database import Recorder, SnapshotRecord
from deal_radar.dispatchers.router import AlertRouter
from deal_radar.engine.anomaly import AnomalyScorer
from deal_radar.engine.bus import BusMessage, ListingBus
from deal_radar.engine.dedup import Deduplicator, MemoryDeduplicator
from deal_radar.engine.normalizer import NormalizationError, Normalizer
from deal_radar.engine.text_filter import TextFilter
from deal_radar.engine.types import (
    Alert,
    DealItem,
    DedupDecision,
    DispatchReport,
    FilterResult,
    RawListing,
    ScoreResult,
    VisionResult,
    VisionVerdict,
    utcnow,
)
from deal_radar.engine.vision_filter import VisionFilter

log = get_logger("pipeline")


@dataclass
class PipelineOutcome:
    """What happened to one listing (returned for tests, ``--once`` and metrics)."""

    stage: str  # normalize | filter | score | vision | below_threshold | duplicate | alerted | dispatch_failed | error
    reason: str | None = None
    item: DealItem | None = None
    filter_result: FilterResult | None = None
    score: ScoreResult | None = None
    vision: VisionResult | None = None
    dedup: DedupDecision | None = None
    alert: Alert | None = None
    report: DispatchReport | None = None
    internal_ms: float = 0.0
    vision_ms: float = 0.0
    dispatch_ms: float = 0.0
    timings: dict[str, float] = field(default_factory=dict)

    @property
    def alerted(self) -> bool:
        return self.stage == "alerted"


class Pipeline:
    def __init__(
        self,
        config: AppConfig,
        *,
        normalizer: Normalizer,
        text_filter: TextFilter,
        scorer: AnomalyScorer,
        dedup: Deduplicator,
        router: AlertRouter,
        vision: VisionFilter | None = None,
        recorder: Recorder | None = None,
        metrics: Metrics | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.config = config
        self.normalizer = normalizer
        self.text_filter = text_filter
        self.scorer = scorer
        self.dedup = dedup
        self.router = router
        self.vision = vision
        self.recorder = recorder
        self.metrics = metrics or Metrics()
        self.clock = clock
        self._fallback_dedup: MemoryDeduplicator | None = None
        self._profiles = {p.id: p for p in config.profiles}
        m = self.metrics
        self._m_items = m.counter("pipeline_items_total", "Listings entering the pipeline", ("source",))
        self._m_outcomes = m.counter("pipeline_outcomes_total", "Pipeline outcomes", ("stage", "reason"))
        self._m_internal = m.histogram("pipeline_internal_ms", "Internal pipeline overhead per listing (ms)", ())
        self._m_stage = m.histogram("pipeline_stage_ms", "Per-stage latency (ms)", ("stage",))
        self._m_vision = m.histogram("pipeline_vision_ms", "Vision validation latency (ms)", ())
        self._m_e2e = m.histogram(
            "alert_end_to_end_ms",
            "Collector receive time -> dispatch complete (ms)",
            (),
            buckets=(10, 25, 50, 100, 250, 500, 1000, 2000, 5000, 10000, 30000, 60000),
        )
        self._m_alerts = m.counter("alerts_total", "Alerts created", ("severity", "kind"))

    # ------------------------------------------------------------------ main entry

    async def process(self, raw: RawListing) -> PipelineOutcome:
        started = time.perf_counter()
        self._m_items.inc(source=raw.source)
        timings: dict[str, float] = {}
        outcome = PipelineOutcome(stage="error")
        try:
            outcome = await self._process(raw, timings)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - one bad listing must never stop a worker
            log.exception("pipeline stage crashed", extra={"source": raw.source, "listing": raw.listing_key})
            outcome = PipelineOutcome(stage="error", reason=type(exc).__name__)
        total_ms = (time.perf_counter() - started) * 1000.0
        outcome.internal_ms = max(0.0, total_ms - outcome.vision_ms - outcome.dispatch_ms)
        outcome.timings = timings
        self._m_internal.observe(outcome.internal_ms)
        self._m_outcomes.inc(stage=outcome.stage, reason=outcome.reason or "")
        if outcome.alert is not None:
            outcome.alert.pipeline_ms = round(outcome.internal_ms, 3)
        return outcome

    async def _process(self, raw: RawListing, timings: dict[str, float]) -> PipelineOutcome:
        # 1. normalize -------------------------------------------------------
        t = time.perf_counter()
        try:
            item = self.normalizer.normalize(raw)
        except NormalizationError as exc:
            return PipelineOutcome(stage="normalize", reason=getattr(exc, "code", "invalid"))
        finally:
            timings["normalize"] = self._stage("normalize", t)

        # 2. deterministic text filter --------------------------------------
        t = time.perf_counter()
        fr = self.text_filter.evaluate(item)
        timings["filter"] = self._stage("filter", t)
        if not fr.accepted:
            if fr.reject_code != "no_profile_match":
                self._record(item, fr, None)
            return PipelineOutcome(stage="filter", reason=fr.reject_code, item=item, filter_result=fr)
        profile = self._profiles[fr.profile_id]  # type: ignore[index]

        # 3. preliminary score (no vision) ----------------------------------
        t = time.perf_counter()
        score = self.scorer.score(item, fr, None)
        timings["score"] = self._stage("score", t)
        if score.rejected:
            self.scorer.observe(item, fr, score)
            self._record(item, fr, score)
            return PipelineOutcome(stage="score", reason=score.reject_reason, item=item, filter_result=fr, score=score)

        # 4. vision (local GPU) only for candidates worth the inference -------
        vision_result: VisionResult | None = None
        vision_ms = 0.0
        if self.vision is not None and self.vision.applies_to(item, profile) and score.score >= self.config.vision.min_prelim_score:
            t = time.perf_counter()
            vision_result = await self.vision.check(item, profile)
            vision_ms = (time.perf_counter() - t) * 1000.0
            timings["vision"] = round(vision_ms, 3)
            self._m_vision.observe(vision_ms)
            if vision_result.verdict is VisionVerdict.ERROR and self.config.vision.on_error == "reject":
                self._record(item, fr, score)
                return PipelineOutcome(
                    stage="vision", reason="vision_unavailable", item=item, filter_result=fr, score=score,
                    vision=vision_result, vision_ms=vision_ms,
                )
            t = time.perf_counter()
            score = self.scorer.score(item, fr, vision_result)
            timings["rescore"] = self._stage("rescore", t)

        # 5. learn from the observation, persist it off the hot path --------
        self.scorer.observe(item, fr, score)
        self._record(item, fr, score)
        if score.rejected:
            return PipelineOutcome(
                stage="vision" if vision_result is not None else "score", reason=score.reject_reason, item=item,
                filter_result=fr, score=score, vision=vision_result, vision_ms=vision_ms,
            )
        if score.severity is None:
            return PipelineOutcome(
                stage="below_threshold", reason=None, item=item, filter_result=fr, score=score,
                vision=vision_result, vision_ms=vision_ms,
            )

        # 6. atomic once-only claim -------------------------------------------
        t = time.perf_counter()
        decision, dedup_impl = await self._claim(item, fr.product_key or fr.profile_id or "unknown")
        timings["dedup"] = self._stage("dedup", t)
        if not decision.should_alert:
            return PipelineOutcome(
                stage="duplicate", reason=decision.status.value, item=item, filter_result=fr, score=score,
                vision=vision_result, dedup=decision, vision_ms=vision_ms,
            )

        # 7. alert + dispatch ---------------------------------------------------
        now = self.clock()
        alert = Alert(
            item=item,
            profile_id=profile.id,
            profile_name=profile.name,
            variant_id=fr.variant_id,
            category=profile.category,
            severity=score.severity,
            score=score,
            vision=vision_result,
            dedup=decision,
            created_at=now,
            ingest_lag_ms=round((now - item.posted_at).total_seconds() * 1000.0, 1) if item.posted_at else None,
        )
        t = time.perf_counter()
        report = await self.router.route(alert)
        dispatch_ms = (time.perf_counter() - t) * 1000.0
        timings["dispatch"] = round(dispatch_ms, 3)
        e2e_ms = (self.clock() - item.received_at).total_seconds() * 1000.0
        self._m_e2e.observe(max(0.0, e2e_ms))
        if report.all_failed or (not report.results and report.suppressed_reason is None):
            try:
                await dedup_impl.rollback(decision)
            except Exception:  # noqa: BLE001 - rollback is best effort
                log.warning("dedup rollback failed", extra={"listing": item.listing_key})
            stage = "dispatch_failed"
        elif report.suppressed_reason is not None and not report.results:
            # Suppressed by routing policy (quiet hours / flood guard): keep the claim so the
            # same deal does not page someone the moment quiet hours end.
            stage = "suppressed"
        else:
            stage = "alerted"
            self._m_alerts.inc(severity=alert.severity.value, kind="price_error" if score.is_price_error else decision.status.value)
            log.info(
                "alert dispatched",
                extra={
                    "alert_id": alert.alert_id,
                    "profile": alert.product_key,
                    "severity": alert.severity.value,
                    "score": round(score.score, 1),
                    "price": item.total_price,
                    "targets": [r.target for r in report.results if r.ok],
                    "e2e_ms": round(e2e_ms, 1),
                },
            )
        if self.recorder is not None:
            self.recorder.record_alert(alert, report)
        return PipelineOutcome(
            stage=stage, reason=report.suppressed_reason, item=item, filter_result=fr, score=score,
            vision=vision_result, dedup=decision, alert=alert, report=report, vision_ms=vision_ms,
            dispatch_ms=dispatch_ms,
        )

    # ------------------------------------------------------------------ helpers

    async def _claim(self, item: DealItem, product_key: str) -> tuple[DedupDecision, Deduplicator]:
        try:
            return await self.dedup.claim(item, product_key), self.dedup
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - shared store down: degrade to local dedup
            if self._fallback_dedup is None:
                self._fallback_dedup = MemoryDeduplicator(self.config)
                log.error("shared dedup unavailable; falling back to process-local dedup", extra={"error": repr(exc)})
            return await self._fallback_dedup.claim(item, product_key), self._fallback_dedup

    def _record(self, item: DealItem, fr: FilterResult | None, score: ScoreResult | None) -> None:
        if self.recorder is None:
            return
        if fr is not None and not fr.accepted and not self.config.storage.record_rejected:
            return
        self.recorder.record(SnapshotRecord(item=item, filter_result=fr, score=score, observed_at=item.received_at))

    def _stage(self, name: str, started: float) -> float:
        elapsed = (time.perf_counter() - started) * 1000.0
        self._m_stage.observe(elapsed, stage=name)
        return round(elapsed, 4)


class PipelineRunner:
    """Consumes the listing bus and processes messages with bounded concurrency.

    A single consumer loop reads from the bus (one XREADGROUP stream per process) and
    fans messages out to at most ``workers`` concurrent ``Pipeline.process`` tasks;
    each message is acknowledged only after processing so a crash mid-flight leaves it
    pending for XAUTOCLAIM recovery on another processor.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        bus: ListingBus,
        *,
        workers: int,
        on_outcome: Callable[[PipelineOutcome], Awaitable[None] | None] | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.bus = bus
        self.workers = max(1, workers)
        self.on_outcome = on_outcome
        self._sem = asyncio.Semaphore(self.workers)
        self._tasks: set[asyncio.Task[None]] = set()
        self.processed = 0
        self._inflight = pipeline.metrics.gauge("pipeline_inflight", "Listings currently being processed", ())

    async def run(self) -> None:
        async for message in self.bus.consume():
            await self._sem.acquire()
            task = asyncio.create_task(self._handle(message), name=f"pipeline:{message.raw.listing_key}")
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _handle(self, message: BusMessage) -> None:
        self._inflight.inc()
        try:
            outcome = await self.pipeline.process(message.raw)
            self.processed += 1
            if self.on_outcome is not None:
                result = self.on_outcome(outcome)
                if asyncio.iscoroutine(result):
                    await result
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - keep the runner alive
            log.exception("pipeline worker failed", extra={"listing": message.raw.listing_key})
        finally:
            try:
                await self.bus.ack(message)
            except Exception:  # noqa: BLE001 - unacked messages are reclaimed later
                log.warning("bus ack failed", extra={"listing": message.raw.listing_key})
            self._inflight.dec()
            self._sem.release()

    async def drain(self, timeout: float) -> int:
        """Wait up to ``timeout`` seconds for in-flight items; cancel the rest. Returns #cancelled."""
        if not self._tasks:
            return 0
        done, pending = await asyncio.wait(set(self._tasks), timeout=timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        return len(pending)


__all__ = ["Pipeline", "PipelineOutcome", "PipelineRunner"]
