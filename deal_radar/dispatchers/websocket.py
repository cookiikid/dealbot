"""Live alert feed over WebSocket (``GET {dispatch.websocket.path}`` on the ops server).

Protocol (server → client, JSON text frames)::

    {"type": "hello", "protocol": 1, "replay": 3, "buffered": 57, "clients": 2, "heartbeat": 20.0, "ts": "..."}
    {"type": "alert", "replay": true, "alert": {...Alert.model_dump(mode="json")...}}   # replayed history
    {"type": "alert", "alert": {...}}                                                  # live
    {"type": "notice", "title": "...", "message": "...", "ts": "..."}                   # operator notices
    {"type": "pong", "ts": "..."}                                                       # reply to {"type": "ping"}

Design decisions
----------------
* **A feed, not a delivery guarantee.** ``send()`` succeeds even with zero clients
  (``message_id`` is always ``None``): the WebSocket is a best-effort mirror of what
  the other channels deliver, and its failure must never trigger a dedup rollback.
* **Serialise once.** Each alert is dumped to JSON once; the same string is written to
  every client and kept in the replay ring buffer (``recent_buffer`` entries), so a
  broadcast costs one ``model_dump`` regardless of the number of clients.
* **Slow clients never block the fan-out.** Clients are written concurrently, each
  with a per-client timeout (1 s). A client whose socket buffer is full, or that
  errors, is dropped: removed immediately and closed in the background (graceful
  close bounded by a timeout, then the transport is aborted).
* **Ordered replay.** A new client is registered *before* the hello/replay frames are
  written; live alerts that arrive meanwhile are queued on that client and flushed
  after the replay, so the client sees history then live alerts, in order, without
  gaps.
* **Auth and capacity** are checked before the upgrade: ``?token=`` (browsers cannot
  set headers on WebSocket requests) or ``Authorization: Bearer`` compared in
  constant time → 401; ``max_clients`` (handshakes in flight included) → 503.
* **Heartbeat.** aiohttp pings every 20 s and closes peers that stop answering, which
  reaps half-open connections (laptops going to sleep, NAT timeouts).
* **Shutdown** closes every socket with 1001 (going away); the hub registers itself in
  the application's ``on_shutdown`` hooks so the ops server does not wait on them. A
  handshake that completes while the hub is closing is closed with 1001 right away
  instead of registering a client nobody would ever close.
"""

from __future__ import annotations

import asyncio
import hmac
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from aiohttp import WSCloseCode, WSMsgType, web
from pydantic import SecretStr

from deal_radar.config_schema import WebSocketSection
from deal_radar.core.http import json_dumps, json_loads
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.dispatchers.base import Dispatcher
from deal_radar.dispatchers.discord import scrub_surrogates
from deal_radar.engine.types import Alert, DispatchResult, utcnow

log = get_logger("dispatch.websocket")

PROTOCOL_VERSION = 1
HEARTBEAT_SECONDS = 20.0
SEND_TIMEOUT_SECONDS = 1.0
CLOSE_TIMEOUT_SECONDS = 1.0
MAX_INBOUND_BYTES = 4096  # clients only ever send tiny control messages
WARMUP_BACKLOG_LIMIT = 1000  # live frames queued for a client still receiving its replay


@dataclass(eq=False, slots=True)
class _Client:
    ws: Any  # web.WebSocketResponse (duck-typed so tests can inject stubs)
    peer: str
    transport: Any = None  # asyncio transport, aborted when a graceful close stalls
    connected_at: float = field(default_factory=time.monotonic)
    sent: int = 0
    backlog: list[str] | None = field(default_factory=list)  # None once live


def _frame(kind: str, alert_json: str, *, replay: bool = False) -> str:
    # String concatenation instead of re-serialising the (already JSON) alert body.
    flag = '"replay":true,' if replay else ""
    return '{"type":"' + kind + '",' + flag + '"alert":' + alert_json + "}"


class WebSocketHub(Dispatcher):
    """Broadcasts alerts to every connected WebSocket client (``target = websocket``)."""

    target = "websocket"

    def __init__(
        self,
        cfg: WebSocketSection,
        *,
        auth_token: SecretStr | None = None,
        metrics: Metrics | None = None,
        heartbeat: float = HEARTBEAT_SECONDS,
        send_timeout: float = SEND_TIMEOUT_SECONDS,
        close_timeout: float = CLOSE_TIMEOUT_SECONDS,
    ) -> None:
        self.cfg = cfg
        self.heartbeat = heartbeat
        self.send_timeout = send_timeout
        self.close_timeout = close_timeout
        self.metrics = metrics or Metrics()
        self._auth_token = auth_token
        self._clients: set[_Client] = set()
        self._pending = 0  # upgrades in progress (count towards max_clients)
        self._recent: deque[str] = deque(maxlen=cfg.recent_buffer)  # serialized alert bodies
        self._closing = False
        self._background: set[asyncio.Task[None]] = set()
        self._clients_gauge = self.metrics.gauge("ws_clients", "Connected WebSocket clients")
        self._messages = self.metrics.counter("ws_messages_total", "WebSocket frames written", ("type",))
        self._dropped = self.metrics.counter("ws_dropped_total", "WebSocket clients dropped", ("reason",))
        self._rejected = self.metrics.counter("ws_rejected_total", "WebSocket connections refused", ("reason",))

    # ------------------------------------------------------------------ wiring

    def attach(self, app: web.Application) -> None:
        """Register ``GET cfg.path`` and a shutdown hook on the ops server app."""
        app.router.add_get(self.cfg.path, self.handle, allow_head=False)
        app.on_shutdown.append(self._on_shutdown)

    @property
    def configured(self) -> bool:
        return self.cfg.enabled

    @property
    def client_count(self) -> int:
        return len(self._clients)

    @property
    def buffered(self) -> int:
        return len(self._recent)

    # ------------------------------------------------------------------ connection handling

    def _authorized(self, request: web.Request) -> bool:
        if self._auth_token is None:
            return True
        expected = self._auth_token.get_secret_value().encode()
        candidates: list[str] = []
        header = request.headers.get("Authorization", "")
        if header[:7].lower() == "bearer ":
            candidates.append(header[7:].strip())
        if "token" in request.query:
            candidates.append(request.query.get("token", ""))
        # Evaluate every candidate (no short-circuit) to keep the timing uniform.
        matched = False
        for supplied in candidates:
            matched |= hmac.compare_digest(supplied.encode(), expected)
        return matched

    def _reject(self, status: int, reason: str, message: str) -> web.Response:
        self._rejected.inc(reason=reason)
        return web.Response(text=json_dumps({"error": message}), status=status, content_type="application/json")

    def _replay_count(self, request: web.Request) -> int | None:
        raw = request.query.get("replay")
        if raw is None:
            return len(self._recent)
        try:
            value = int(raw)
        except ValueError:
            return None
        return max(0, min(value, self.cfg.recent_buffer, len(self._recent)))

    async def handle(self, request: web.Request) -> web.StreamResponse:
        if self._closing:
            return self._reject(503, "shutting_down", "server shutting down")
        if not self._authorized(request):
            return self._reject(401, "unauthorized", "unauthorized")
        replay = self._replay_count(request)
        if replay is None:
            return self._reject(400, "bad_request", "replay must be an integer")
        ws = web.WebSocketResponse(heartbeat=self.heartbeat, max_msg_size=MAX_INBOUND_BYTES, timeout=self.close_timeout)
        if not ws.can_prepare(request).ok:
            return self._reject(400, "not_websocket", "websocket upgrade required")
        if len(self._clients) + self._pending >= self.cfg.max_clients:
            return self._reject(503, "full", "too many clients")

        self._pending += 1
        try:
            await ws.prepare(request)
        finally:
            self._pending -= 1
        if self._closing:  # close() ran while the handshake was in flight
            self._rejected.inc(reason="shutting_down")
            late = _Client(ws=ws, peer=request.remote or "?", transport=request.transport)
            await self._close_client(late, WSCloseCode.GOING_AWAY, b"server shutdown")
            return ws

        client = _Client(ws=ws, peer=request.remote or "?", transport=request.transport)
        # Register and snapshot the replay without an await in between: alerts that
        # arrive while history is being written queue on client.backlog.
        self._clients.add(client)
        self._clients_gauge.set(len(self._clients))
        history = list(self._recent)[len(self._recent) - replay :] if replay else []
        log.info("websocket client connected", extra={"peer": client.peer, "clients": len(self._clients), "replay": replay})
        try:
            if await self._warm_up(client, history):
                async for msg in ws:
                    if msg.type is WSMsgType.TEXT:
                        await self._on_text(client, msg.data)
                    elif msg.type is WSMsgType.ERROR:
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # a misbehaving client must not crash the server
            log.debug("websocket client error", extra={"peer": client.peer, "error": repr(exc)})
        finally:
            self._forget(client)
            log.info("websocket client disconnected", extra={"peer": client.peer, "clients": len(self._clients)})
        return ws

    async def _warm_up(self, client: _Client, history: list[str]) -> bool:
        hello = {
            "type": "hello",
            "protocol": PROTOCOL_VERSION,
            "replay": len(history),
            "buffered": len(self._recent),
            "clients": len(self._clients),
            "heartbeat": self.heartbeat,
            "ts": utcnow().isoformat(),
        }
        if not await self._write(client, json_dumps(hello), "hello"):
            return False
        for body in history:
            if not await self._write(client, _frame("alert", body, replay=True), "replay"):
                return False
        while client.backlog:
            queued, client.backlog = client.backlog, []
            for frame in queued:
                if not await self._write(client, frame, "alert"):
                    return False
        client.backlog = None  # live from now on: broadcasts write directly
        return True

    async def _on_text(self, client: _Client, data: str) -> None:
        text = data.strip()
        kind: Any = text
        if text.startswith("{"):
            try:
                parsed = json_loads(text)
            except ValueError:
                return
            kind = parsed.get("type") if isinstance(parsed, dict) else None
        if kind == "ping":
            await self._write(client, json_dumps({"type": "pong", "ts": utcnow().isoformat()}), "pong")

    # ------------------------------------------------------------------ writing / dropping

    async def _write(self, client: _Client, frame: str, kind: str) -> bool:
        """Write one frame with the per-client timeout; drop the client on failure."""
        if client.ws.closed:
            self._drop(client, "closed")
            return False
        try:
            await asyncio.wait_for(client.ws.send_str(frame), self.send_timeout)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            self._drop(client, "slow")
            return False
        except Exception as exc:  # ConnectionResetError, RuntimeError on closing transports...
            log.debug("websocket write failed", extra={"peer": client.peer, "error": repr(exc)})
            self._drop(client, "error")
            return False
        client.sent += 1
        self._messages.inc(type=kind)
        return True

    def _forget(self, client: _Client) -> bool:
        if client in self._clients:
            self._clients.discard(client)
            self._clients_gauge.set(len(self._clients))
            return True
        return False

    def _drop(self, client: _Client, reason: str) -> None:
        if not self._forget(client):
            return
        self._dropped.inc(reason=reason)
        log.info("websocket client dropped", extra={"peer": client.peer, "reason": reason, "clients": len(self._clients)})
        if reason == "closed":
            return
        code = WSCloseCode.POLICY_VIOLATION if reason in ("slow", "backlog") else WSCloseCode.INTERNAL_ERROR
        self._spawn(self._close_client(client, code, reason.encode()))

    def _spawn(self, coro: Any) -> None:
        task = asyncio.get_running_loop().create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _close_client(self, client: _Client, code: int, message: bytes) -> None:
        try:
            await asyncio.wait_for(client.ws.close(code=code, message=message), self.close_timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # timeout or broken transport: fall through to abort
            log.debug("websocket close failed", extra={"peer": client.peer, "error": repr(exc)})
        finally:
            transport = client.transport
            if transport is not None and not transport.is_closing():
                transport.abort()

    async def _broadcast(self, frame: str, kind: str) -> tuple[int, int]:
        live: list[_Client] = []
        for client in list(self._clients):
            if client.backlog is not None:  # still receiving its replay
                if len(client.backlog) >= WARMUP_BACKLOG_LIMIT:
                    self._drop(client, "backlog")
                else:
                    client.backlog.append(frame)
            else:
                live.append(client)
        if not live:
            return 0, 0
        results = await asyncio.gather(*(self._write(client, frame, kind) for client in live))
        delivered = sum(1 for ok in results if ok)
        return delivered, len(results) - delivered

    # ------------------------------------------------------------------ Dispatcher API

    async def send(self, alert: Alert) -> DispatchResult:
        started = time.perf_counter()
        try:
            data = alert.model_dump(mode="json")
            try:
                body = json_dumps(data)
            except TypeError:  # lone UTF-16 surrogates are not valid UTF-8 (see dispatchers.discord)
                body = json_dumps(scrub_surrogates(data))
        except Exception as exc:
            log.exception("websocket alert serialisation failed", extra={"alert_id": alert.alert_id})
            return DispatchResult(target=self.target, ok=False, error=f"serialisation failed: {exc!r}"[:300])
        self._recent.append(body)
        delivered, dropped = await self._broadcast(_frame("alert", body), "alert")
        if dropped:
            log.debug(
                "websocket broadcast dropped clients",
                extra={"alert_id": alert.alert_id, "delivered": delivered, "dropped": dropped},
            )
        return DispatchResult(
            target=self.target,
            ok=True,
            latency_ms=round((time.perf_counter() - started) * 1000.0, 3),
            message_id=None,
        )

    async def send_notice(self, title: str, message: str) -> DispatchResult:
        started = time.perf_counter()
        notice = {"type": "notice", "title": title, "message": message, "ts": utcnow().isoformat()}
        frame = json_dumps(scrub_surrogates(notice))
        await self._broadcast(frame, "notice")
        return DispatchResult(target=self.target, ok=True, latency_ms=round((time.perf_counter() - started) * 1000.0, 3))

    async def close(self) -> None:
        """Close every client with 1001 (going away) and refuse new connections."""
        self._closing = True
        clients = list(self._clients)
        self._clients.clear()
        self._clients_gauge.set(0)
        if clients:
            await asyncio.gather(
                *(self._close_client(c, WSCloseCode.GOING_AWAY, b"server shutdown") for c in clients),
                return_exceptions=True,
            )
        if self._background:
            await asyncio.gather(*list(self._background), return_exceptions=True)

    async def _on_shutdown(self, app: web.Application) -> None:
        await self.close()


__all__ = ["PROTOCOL_VERSION", "WebSocketHub"]
