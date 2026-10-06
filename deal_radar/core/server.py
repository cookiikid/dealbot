"""Operational HTTP server: health, readiness, Prometheus metrics, status and the alert feed.

Endpoints
---------
``GET /healthz``        liveness — the event loop is responsive (always 200 while running).
``GET /readyz``         readiness — 200 when the node's roles are running (processor
                        consuming, at least one source healthy when collector), else 503.
``GET /metrics``        Prometheus text exposition of :class:`Metrics`.
``GET /status``         JSON: sources, bus backlog, history size, config summary (auth).
``GET /alerts/recent``  JSON: last alerts from the database (auth).
``GET /ws``             live alert WebSocket feed (registered by the WebSocket hub).

When ``http_server.auth_token`` is set, ``/status``, ``/alerts/recent`` and the
WebSocket require ``Authorization: Bearer <token>`` (or ``?token=``). Health and
metrics stay open so load balancers and Prometheus can scrape them; bind the server
to the Tailscale interface or firewall it if that is not acceptable.
"""

from __future__ import annotations

import hmac
from collections.abc import Awaitable, Callable
from typing import Any

from aiohttp import web

from deal_radar.core.http import json_dumps
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics

log = get_logger("server")

StatusFn = Callable[[], dict[str, Any]]
ReadyFn = Callable[[], tuple[bool, str]]
AlertsFn = Callable[[int], Awaitable[list[dict[str, Any]]]]


def _json(data: Any, status: int = 200) -> web.Response:
    return web.Response(text=json_dumps(data), status=status, content_type="application/json")


class OpsServer:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        metrics: Metrics,
        status_fn: StatusFn,
        ready_fn: ReadyFn,
        alerts_fn: AlertsFn | None = None,
        auth_token: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.metrics = metrics
        self.status_fn = status_fn
        self.ready_fn = ready_fn
        self.alerts_fn = alerts_fn
        self.auth_token = auth_token
        self.app = web.Application(client_max_size=64 * 1024)
        self.app.router.add_get("/healthz", self._healthz)
        self.app.router.add_get("/readyz", self._readyz)
        self.app.router.add_get("/metrics", self._metrics)
        self.app.router.add_get("/status", self._status)
        self.app.router.add_get("/alerts/recent", self._alerts)
        self._runner: web.AppRunner | None = None
        self.bound_port: int | None = None

    def _authorized(self, request: web.Request) -> bool:
        if not self.auth_token:
            return True
        header = request.headers.get("Authorization", "")
        supplied = header[7:] if header.startswith("Bearer ") else request.query.get("token", "")
        return hmac.compare_digest(supplied.encode(), self.auth_token.encode())

    async def _healthz(self, request: web.Request) -> web.Response:
        return _json({"status": "ok"})

    async def _readyz(self, request: web.Request) -> web.Response:
        ready, reason = self.ready_fn()
        return _json({"ready": ready, "reason": reason}, status=200 if ready else 503)

    async def _metrics(self, request: web.Request) -> web.Response:
        return web.Response(text=self.metrics.render(), content_type="text/plain", charset="utf-8",
                            headers={"X-Content-Type-Options": "nosniff"})

    async def _status(self, request: web.Request) -> web.Response:
        if not self._authorized(request):
            return _json({"error": "unauthorized"}, status=401)
        return _json(self.status_fn())

    async def _alerts(self, request: web.Request) -> web.Response:
        if not self._authorized(request):
            return _json({"error": "unauthorized"}, status=401)
        if self.alerts_fn is None:
            return _json([])
        try:
            limit = max(1, min(500, int(request.query.get("limit", "50"))))
        except ValueError:
            return _json({"error": "limit must be an integer"}, status=400)
        return _json(await self.alerts_fn(limit))

    async def start(self) -> None:
        self._runner = web.AppRunner(self.app, access_log=None, handle_signals=False)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port, reuse_address=True)
        await site.start()
        sockets = getattr(site._server, "sockets", None) or []  # noqa: SLF001 - aiohttp exposes no public accessor
        self.bound_port = sockets[0].getsockname()[1] if sockets else self.port
        log.info("ops server listening", extra={"host": self.host, "port": self.bound_port})

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None


__all__ = ["OpsServer"]
