"""Tests for the operational HTTP server."""

from __future__ import annotations

import aiohttp
import pytest

from deal_radar.core.metrics import Metrics
from deal_radar.core.server import OpsServer


@pytest.fixture
async def ops():
    metrics = Metrics()
    metrics.counter("demo_total", "demo").inc()
    state = {"ready": (True, "ok")}

    async def alerts(limit: int) -> list[dict]:
        return [{"alert_id": str(i)} for i in range(limit)]

    server = OpsServer(
        host="127.0.0.1",
        port=0,
        metrics=metrics,
        status_fn=lambda: {"node": "t"},
        ready_fn=lambda: state["ready"],
        alerts_fn=alerts,
        auth_token="s3cret",
    )
    await server.start()
    server.state = state  # type: ignore[attr-defined]
    yield server
    await server.stop()


async def test_health_ready_metrics_status_auth(ops: OpsServer) -> None:
    base = f"http://127.0.0.1:{ops.bound_port}"
    async with aiohttp.ClientSession() as s:
        async with s.get(base + "/healthz") as r:
            assert r.status == 200 and (await r.json()) == {"status": "ok"}
        async with s.get(base + "/readyz") as r:
            assert r.status == 200
        ops.state["ready"] = (False, "pipeline runner not running")  # type: ignore[attr-defined]
        async with s.get(base + "/readyz") as r:
            assert r.status == 503 and (await r.json())["reason"] == "pipeline runner not running"
        async with s.get(base + "/metrics") as r:
            assert r.status == 200 and "dealradar_demo_total 1" in await r.text()
        async with s.get(base + "/status") as r:
            assert r.status == 401
        async with s.get(base + "/status", headers={"Authorization": "Bearer s3cret"}) as r:
            assert r.status == 200 and (await r.json()) == {"node": "t"}
        async with s.get(base + "/alerts/recent?limit=3&token=s3cret") as r:
            assert r.status == 200 and len(await r.json()) == 3
        async with s.get(base + "/alerts/recent?limit=abc&token=s3cret") as r:
            assert r.status == 400
        async with s.get(base + "/alerts/recent?token=wrong") as r:
            assert r.status == 401
