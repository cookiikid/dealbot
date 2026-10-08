"""DealRadar entrypoint: builds the component graph for this node and runs it.

Roles (``app.roles``):

* ``collector`` — runs the sources assigned to this node and publishes every new or
  changed :class:`RawListing` onto the listing bus.
* ``processor`` — consumes the bus and runs the pipeline (filter → score → vision →
  dedup → dispatch), owns the database and the price-history model.

Typical deployment: the GCP VM runs both roles with the Redis bus; the home laptop
runs ``collector`` only (Facebook Marketplace / OfferUp on a residential IP) and
publishes into the same Redis stream over Tailscale; the RTX 3060 desktop serves
the vision model that processors call.

Usage::

    python -m deal_radar.main                       # run forever
    python -m deal_radar.main --check-config        # validate config + show what would run
    python -m deal_radar.main --once --dry-run      # one poll of every enabled source, alerts to console
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import signal
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from deal_radar.config_schema import AppConfig, ConfigError, load_config, load_dotenv
from deal_radar.core.backoff import BackoffPolicy
from deal_radar.core.http import HttpClient, NetworkSettings
from deal_radar.core.logs import configure_logging, get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.core.ratelimit import HostLimit
from deal_radar.core.server import OpsServer
from deal_radar.dispatchers.base import ConsoleDispatcher, Dispatcher
from deal_radar.engine.types import RawListing, utcnow

log = get_logger("main")

DEFAULT_CONFIG = Path(__file__).resolve().parent / "config.yaml"


# --------------------------------------------------------------------------- config helpers


def network_settings(config: AppConfig) -> NetworkSettings:
    net = config.network
    return NetworkSettings(
        timeout_seconds=net.timeout_seconds,
        connect_timeout_seconds=net.connect_timeout_seconds,
        max_connections=net.max_connections,
        max_connections_per_host=net.max_connections_per_host,
        dns_cache_ttl_seconds=net.dns_cache_ttl_seconds,
        keepalive_seconds=net.keepalive_seconds,
        trust_env=net.trust_env,
        proxy=net.proxy,
        ca_bundle=net.ca_bundle,
        retry=BackoffPolicy(
            max_attempts=net.retry.max_attempts,
            base_delay=net.retry.base_delay_seconds,
            max_delay=net.retry.max_delay_seconds,
            max_total_seconds=net.retry.max_total_seconds,
        ),
        default_host_limit=(
            HostLimit(net.default_host_limit.rate_per_second, net.default_host_limit.burst) if net.default_host_limit else None
        ),
        host_limits={host: HostLimit(lim.rate_per_second, lim.burst) for host, lim in net.host_limits.items()},
        chrome_major_version=net.chrome_major_version,
        accept_language=net.accept_language,
        identity_rotation_seconds=net.identity_rotation_minutes * 60.0,
    )


def build_dispatchers(config: AppConfig, http: HttpClient, metrics: Metrics) -> dict[str, Dispatcher]:
    """Instantiate every channel declared in ``dispatch`` (unconfigured ones report configured=False)."""
    from deal_radar.dispatchers.discord import DiscordDispatcher
    from deal_radar.dispatchers.telegram import TelegramDispatcher
    from deal_radar.dispatchers.websocket import WebSocketHub

    timeout = config.dispatch.timeout_seconds
    out: dict[str, Dispatcher] = {"console": ConsoleDispatcher()}
    if config.dispatch.websocket.enabled:
        out["websocket"] = WebSocketHub(config.dispatch.websocket, auth_token=config.http_server.auth_token, metrics=metrics)
    for name, target in config.dispatch.discord.webhooks.items():
        out[f"discord:{name}"] = DiscordDispatcher(name, target, http, timeout=timeout, metrics=metrics, node_id=config.app.node_id)
    tg = config.dispatch.telegram
    for name, chat in tg.chats.items():
        out[f"telegram:{name}"] = TelegramDispatcher(name, chat, tg.bot_token, tg.api_base, http, timeout=timeout, metrics=metrics)
    return out


# --------------------------------------------------------------------------- application


class DealRadarApp:
    """Owns every long-lived component of one node and their lifecycle."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.metrics = Metrics()
        self.stop_event = asyncio.Event()
        self.started_at = time.time()
        self.http: HttpClient | None = None
        self.redis: Any = None
        self.bus: Any = None
        self.db: Any = None
        self.recorder: Any = None
        self.scorer: Any = None
        self.vision: Any = None
        self.dedup: Any = None
        self.router: Any = None
        self.pipeline: Any = None
        self.runner: Any = None
        self.server: OpsServer | None = None
        self.dispatchers: dict[str, Dispatcher] = {}
        self.ingestors: list[Any] = []
        self._tasks: list[asyncio.Task[Any]] = []
        self._source_tasks: list[asyncio.Task[Any]] = []

    @property
    def is_collector(self) -> bool:
        return "collector" in self.config.app.roles

    @property
    def is_processor(self) -> bool:
        return "processor" in self.config.app.roles

    # ------------------------------------------------------------------ build

    async def build(self, *, with_server: bool = True) -> None:
        from deal_radar.dispatchers.router import AlertRouter
        from deal_radar.engine.bus import build_bus

        cfg = self.config
        self.http = HttpClient.create(network_settings(cfg), metrics=self.metrics)

        if cfg.storage.redis_url:
            from deal_radar.db.database import connect_redis

            try:
                self.redis = await connect_redis(
                    cfg.storage.redis_url,
                    max_connections=cfg.storage.redis_max_connections,
                    socket_timeout=cfg.storage.redis_socket_timeout_seconds,
                )
                log.info("redis connected")
            except Exception as exc:
                if cfg.bus.backend == "redis":
                    raise RuntimeError(f"bus.backend=redis but Redis is unreachable: {exc!r}") from exc
                log.error("redis unreachable; continuing in single-node memory mode", extra={"error": repr(exc)})
                self.redis = None

        self.bus = build_bus(cfg, self.redis)
        await self.bus.start()

        self.dispatchers = build_dispatchers(cfg, self.http, self.metrics)
        self.router = AlertRouter(cfg, self.dispatchers, metrics=self.metrics)

        if self.is_processor:
            await self._build_processor()

        if self.is_collector:
            from deal_radar.sources.base import IngestorContext
            from deal_radar.sources.registry import build_ingestors

            ctx = IngestorContext(
                http=self.http,
                metrics=self.metrics,
                config=cfg,
                node_id=cfg.app.node_id,
                redis=self.redis,
                notify=self.router.notify,
            )
            self.ingestors = build_ingestors(cfg, ctx)
            if not self.ingestors:
                log.warning("collector role active but no sources are enabled for this node", extra={"node": cfg.app.node_id})

        if with_server and cfg.http_server.enabled:
            token = cfg.http_server.auth_token.get_secret_value() if cfg.http_server.auth_token else None
            self.server = OpsServer(
                host=cfg.http_server.host,
                port=cfg.http_server.port,
                metrics=self.metrics,
                status_fn=self.status,
                ready_fn=self.ready,
                alerts_fn=self.db.recent_alerts if self.db is not None else None,
                auth_token=token,
            )
            hub = self.dispatchers.get("websocket")
            if hub is not None and hasattr(hub, "attach"):
                hub.attach(self.server.app)

    async def _build_processor(self) -> None:
        from deal_radar.db.database import Database, Recorder
        from deal_radar.engine.anomaly import AnomalyScorer, PriceHistory, history_key
        from deal_radar.engine.dedup import build_deduplicator
        from deal_radar.engine.normalizer import Normalizer
        from deal_radar.engine.pipeline import Pipeline, PipelineRunner
        from deal_radar.engine.text_filter import TextFilter
        from deal_radar.engine.vision_filter import VisionFilter

        cfg = self.config
        self.db = Database(cfg.storage.database_url, echo=cfg.storage.database_echo, pool_size=cfg.storage.database_pool_size)
        await self.db.connect()
        self.recorder = Recorder(
            self.db,
            batch_size=cfg.storage.snapshot_batch_size,
            flush_seconds=cfg.storage.snapshot_flush_seconds,
            metrics=self.metrics,
        )
        await self.recorder.start()

        sc = cfg.scoring
        history = PriceHistory(
            half_life_days=sc.history_half_life_days,
            window_days=sc.history_window_days,
            max_samples=sc.history_max_samples,
            source_weights=sc.source_history_weights,
        )
        since = utcnow() - timedelta(days=sc.history_window_days)
        rows = await self.db.load_price_history(since, max_per_key=sc.history_max_samples, max_risk=sc.history_max_risk)
        loaded = history.load(
            (history_key(product_key, condition_class), listing_key, price, observed_at, source)
            for product_key, condition_class, listing_key, price, observed_at, source in rows
        )
        log.info("price history warm-started", extra={"samples": loaded, "keys": len(history.keys())})
        self.scorer = AnomalyScorer(cfg, history)

        self.vision = VisionFilter(cfg, self.http, metrics=self.metrics, redis=self.redis) if cfg.vision.enabled else None
        self.dedup = build_deduplicator(cfg, self.redis)
        self.pipeline = Pipeline(
            cfg,
            normalizer=Normalizer(cfg),
            text_filter=TextFilter(cfg),
            scorer=self.scorer,
            dedup=self.dedup,
            router=self.router,
            vision=self.vision,
            recorder=self.recorder,
            metrics=self.metrics,
        )
        self.runner = PipelineRunner(self.pipeline, self.bus, workers=cfg.bus.workers)

    # ------------------------------------------------------------------ run

    async def start(self) -> None:
        if self.server is not None:
            await self.server.start()
        if self.vision is not None:
            healthy = await self.vision.health()
            log.info("vision backend", extra={"healthy": healthy, "url": self.config.vision.base_url, "model": self.config.vision.model})
            if healthy:
                # Load the weights now (2-10 s cold start) instead of on the first real candidate.
                self._tasks.append(asyncio.create_task(self.vision.warmup(), name="vision-warmup"))
        if self.runner is not None:
            self._tasks.append(asyncio.create_task(self.runner.run(), name="pipeline-runner"))
        for ingestor in self.ingestors:
            task = asyncio.create_task(ingestor.run(self.bus.publish, self.stop_event), name=f"source:{ingestor.name}")
            self._source_tasks.append(task)
        self._tasks.append(asyncio.create_task(self._maintenance(), name="maintenance"))
        log.info(
            "dealradar started",
            extra={
                "node": self.config.app.node_id,
                "roles": self.config.app.roles,
                "sources": [i.name for i in self.ingestors],
                "bus": self.config.bus.backend,
                "redis": bool(self.redis),
                "dry_run": self.config.app.dry_run,
            },
        )

    async def _maintenance(self) -> None:
        """Periodic housekeeping: gauges every 15 s, DB retention pruning every 6 h."""
        backlog = self.metrics.gauge("bus_backlog_items", "Listings waiting on the bus", ())
        hist = self.metrics.gauge("history_keys", "Product/condition keys in the price model", ())
        last_prune = 0.0
        last_scrub = 0.0
        while not self.stop_event.is_set():
            with contextlib.suppress(Exception):
                backlog.set(self.bus.backlog())
                if self.scorer is not None:
                    hist.set(len(self.scorer.history.keys()))
            if self.db is not None and time.time() - last_scrub > 3600:
                last_scrub = time.time()
                for source, hours in self.config.storage.source_content_ttl_hours.items():
                    try:
                        await self.db.scrub_source_content(source, hours)
                    except Exception:  # noqa: BLE001
                        log.exception("source content scrub failed", extra={"source": source})
            if self.db is not None and time.time() - last_prune > 6 * 3600:
                last_prune = time.time()
                try:
                    removed = await self.db.prune(self.config.storage.retention_days)
                    if removed:
                        log.info("pruned old rows", extra={"rows": removed})
                except Exception:  # noqa: BLE001
                    log.exception("retention prune failed")
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.stop_event.wait(), timeout=15)

    async def wait(self) -> None:
        await self.stop_event.wait()

    async def shutdown(self, grace_seconds: float = 15.0) -> None:
        log.info("shutting down")
        self.stop_event.set()
        if self._source_tasks:
            _done, pending = await asyncio.wait(self._source_tasks, timeout=grace_seconds)
            for task in pending:
                task.cancel()
            await asyncio.gather(*self._source_tasks, return_exceptions=True)
        if self.bus is not None:
            with contextlib.suppress(Exception):
                await self.bus.close()
        if self.runner is not None:
            cancelled = await self.runner.drain(timeout=grace_seconds)
            if cancelled:
                log.warning("cancelled in-flight listings at shutdown", extra={"count": cancelled})
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for closer in (
            getattr(self.recorder, "stop", None),
            getattr(self.db, "close", None),
            getattr(self.router, "close", None),
            getattr(self.vision, "close", None),
            getattr(self.dedup, "close", None),
            getattr(self.server, "stop", None),
            getattr(self.http, "close", None),
        ):
            if closer is None:
                continue
            try:
                await closer()
            except Exception:  # noqa: BLE001 - keep closing the rest
                log.exception("error during shutdown")
        if self.redis is not None:
            with contextlib.suppress(Exception):
                await self.redis.aclose()
        log.info("shutdown complete")

    # ------------------------------------------------------------------ introspection

    def status(self) -> dict[str, Any]:
        return {
            "node": self.config.app.node_id,
            "roles": self.config.app.roles,
            "uptime_s": round(time.time() - self.started_at, 1),
            "dry_run": self.config.app.dry_run,
            "redis": bool(self.redis),
            "bus": {"backend": self.config.bus.backend, "backlog": self._safe(lambda: self.bus.backlog())},
            "sources": [i.health.as_dict() for i in self.ingestors],
            "processed": getattr(self.runner, "processed", None),
            "history_keys": self._safe(lambda: len(self.scorer.history.keys())) if self.scorer else None,
            "dispatch_targets": {name: d.configured for name, d in self.dispatchers.items()},
            "metrics": self.metrics.snapshot(),
        }

    def ready(self) -> tuple[bool, str]:
        if self.is_processor and (self.runner is None or not any(t.get_name() == "pipeline-runner" and not t.done() for t in self._tasks)):
            return False, "pipeline runner not running"
        if self.is_collector and self.ingestors and all(i.health.state in ("open", "stopped") for i in self.ingestors):
            return False, "all sources unavailable"
        return True, "ok"

    @staticmethod
    def _safe(fn: Any) -> Any:
        try:
            return fn()
        except Exception:  # noqa: BLE001
            return None


# --------------------------------------------------------------------------- modes


async def run_forever(config: AppConfig) -> int:
    app = DealRadarApp(config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, app.stop_event.set)
    try:
        await app.build()
        await app.start()
        await app.wait()
    finally:
        await app.shutdown()
    return 0


async def run_once(config: AppConfig, sources: list[str] | None) -> int:
    """Poll each enabled source once and push results straight through the pipeline."""
    from deal_radar.sources.registry import enabled_source_names

    if not ("processor" in config.app.roles):
        print("--once needs the processor role", file=sys.stderr)
        return 2
    roles = sorted(set(config.app.roles) | {"collector"})
    config = config.model_copy(update={"app": config.app.model_copy(update={"roles": roles})})
    names = sources or enabled_source_names(config)
    app = DealRadarApp(config)
    await app.build(with_server=False)
    totals: dict[str, int] = {}
    try:
        selected = [i for i in app.ingestors if i.name in names]
        for ingestor in selected:
            started = time.perf_counter()
            listings: list[RawListing] = []
            try:
                await ingestor.setup()
                listings = await ingestor.run_once()
            except Exception as exc:  # noqa: BLE001
                print(f"[{ingestor.name}] poll failed: {exc!r}")
            finally:
                with contextlib.suppress(Exception):
                    await ingestor.teardown()
            print(f"[{ingestor.name}] {len(listings)} listing(s) in {(time.perf_counter() - started) * 1000:.0f} ms")
            for raw in listings:
                outcome = await app.pipeline.process(raw)
                totals[outcome.stage] = totals.get(outcome.stage, 0) + 1
                if outcome.score is not None and outcome.stage in ("alerted", "below_threshold", "duplicate", "suppressed"):
                    print(
                        f"  {outcome.stage:<16} {outcome.score.score:5.1f}  "
                        f"${outcome.item.total_price:>9,.2f}  {outcome.filter_result.product_key:<28} {outcome.item.title[:70]}"
                    )
        print("outcomes:", ", ".join(f"{k}={v}" for k, v in sorted(totals.items())) or "none")
    finally:
        await app.shutdown(grace_seconds=5)
    return 0


def check_config(config: AppConfig, path: Path) -> int:
    from deal_radar.sources.registry import enabled_source_names

    print(f"config OK: {path}")
    print(f"  node={config.app.node_id} roles={','.join(config.app.roles)} bus={config.bus.backend} "
          f"redis={'yes' if config.storage.redis_url else 'no'} db={config.storage.database_url.split('://')[0]}")
    print(f"  profiles: {len(config.enabled_profiles())} enabled / {len(config.profiles)} "
          f"({', '.join(p.id for p in config.enabled_profiles())})")
    print(f"  sources on this node: {', '.join(enabled_source_names(config)) or 'none'}")
    print(f"  filter rule groups: {len(config.filters.rules)}; vision: {'on' if config.vision.enabled else 'off'} "
          f"({config.vision.backend} {config.vision.model} @ {config.vision.base_url})")
    configured = []
    for name, hook in config.dispatch.discord.webhooks.items():
        configured.append(f"discord:{name}={'set' if hook.configured else 'unset'}")
    for name, chat in config.dispatch.telegram.chats.items():
        ok = chat.configured and config.dispatch.telegram.bot_token is not None
        configured.append(f"telegram:{name}={'set' if ok else 'unset'}")
    print(f"  dispatch targets: {', '.join(configured) or 'console only'}")
    print(f"  routes: {', '.join(r.name for r in config.dispatch.routes)}")
    return 0


# --------------------------------------------------------------------------- CLI


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="dealradar", description="Autonomous low-latency deal & pricing-anomaly engine")
    parser.add_argument("--config", default=os.environ.get("DEALRADAR_CONFIG", str(DEFAULT_CONFIG)), help="path to config.yaml")
    parser.add_argument("--env-file", default=os.environ.get("DEALRADAR_ENV_FILE", ".env"), help=".env file to load (if present)")
    parser.add_argument("--check-config", action="store_true", help="validate configuration and exit")
    parser.add_argument("--once", action="store_true", help="poll every enabled source once, process, print, exit")
    parser.add_argument("--sources", default=None, help="comma-separated subset of sources for --once")
    parser.add_argument("--dry-run", action="store_true", help="route alerts to the console only")
    parser.add_argument("--roles", default=None, help="override app.roles, e.g. collector or collector,processor")
    parser.add_argument("--node-id", default=None, help="override app.node_id")
    parser.add_argument("--log-level", default=None, choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args(argv)


def apply_overrides(config: AppConfig, args: argparse.Namespace) -> AppConfig:
    data = config.model_dump(mode="python")
    app = data["app"]
    if args.dry_run:
        app["dry_run"] = True
    if args.roles:
        app["roles"] = [r.strip() for r in args.roles.split(",") if r.strip()]
    if args.node_id:
        app["node_id"] = args.node_id
    if args.log_level:
        app["log_level"] = args.log_level
    # Secrets were already resolved; re-validate the overridden tree as a whole.
    return AppConfig.model_validate(_unwrap_secrets(data))


def _unwrap_secrets(value: Any) -> Any:
    from pydantic import SecretStr

    if isinstance(value, SecretStr):
        return value.get_secret_value()
    if isinstance(value, dict):
        return {k: _unwrap_secrets(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_unwrap_secrets(v) for v in value]
    return value


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(args.env_file)
    path = Path(args.config)
    try:
        config = apply_overrides(load_config(path), args)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:  # pydantic errors from overrides
        print(f"invalid configuration after CLI overrides: {exc}", file=sys.stderr)
        return 2
    configure_logging(config.app.log_level, json_output=config.app.log_json, node_id=config.app.node_id)
    if args.check_config:
        return check_config(config, path)
    runner: Any = asyncio.run
    try:  # uvloop is optional: ~2x faster event loop on Linux when installed
        import uvloop  # type: ignore[import-not-found]

        runner = getattr(uvloop, "run", asyncio.run)
    except ImportError:
        pass
    try:
        if args.once:
            sources = [s.strip() for s in args.sources.split(",")] if args.sources else None
            return runner(run_once(config, sources))
        return runner(run_forever(config))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
