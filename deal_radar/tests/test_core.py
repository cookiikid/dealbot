"""Tests for deal_radar.core: backoff, rate limiting, circuit breaker, metrics, HTTP client."""

from __future__ import annotations

import asyncio
import random
import time

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from deal_radar.core.backoff import (
    BackoffPolicy,
    ExponentialBackoff,
    RetryableError,
    RetryExhausted,
    parse_retry_after,
    retry_async,
)
from deal_radar.core.http import (
    ConditionalCache,
    HttpClient,
    HttpStatusError,
    IdentityRotator,
    NetworkSettings,
    ResponseTooLarge,
    build_header_profiles,
    cache_bust,
    chromium_sec_ch_ua,
    estimate_chrome_major,
)
from deal_radar.core.metrics import Metrics
from deal_radar.core.ratelimit import CircuitBreaker, HostLimit, RateLimiterRegistry, TokenBucket

# --------------------------------------------------------------------------- backoff


def test_backoff_delay_full_jitter_is_bounded_and_capped() -> None:
    policy = BackoffPolicy(base_delay=0.5, max_delay=4.0)
    rng = random.Random(7)
    for attempt in range(1, 12):
        delay = policy.delay(attempt, rng)
        assert 0.0 <= delay <= min(4.0, 0.5 * 2 ** (attempt - 1))


def test_backoff_equal_and_none_jitter() -> None:
    assert BackoffPolicy(base_delay=1, max_delay=100, jitter="none").delay(3) == 4
    d = BackoffPolicy(base_delay=1, max_delay=100, jitter="equal").delay(3, random.Random(1))
    assert 2 <= d <= 4
    with pytest.raises(ValueError):
        BackoffPolicy(jitter="bogus")
    with pytest.raises(ValueError):
        BackoffPolicy(max_attempts=0)


def test_parse_retry_after_seconds_and_http_date() -> None:
    assert parse_retry_after("3") == 3.0
    assert parse_retry_after(" 1.5 ") == 1.5
    assert parse_retry_after(None) is None
    assert parse_retry_after("") is None
    assert parse_retry_after("garbage") is None
    assert parse_retry_after("-4") == 0.0
    now = 1_700_000_000.0
    assert parse_retry_after("Tue, 14 Nov 2023 22:13:30 GMT", now=now) == pytest.approx(10.0, abs=1)


async def test_retry_async_retries_then_succeeds_and_uses_server_hint() -> None:
    calls = 0
    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RetryableError("429", retry_after=2.0)
        if calls == 2:
            raise ConnectionError("reset")
        return "ok"

    result = await retry_async(flaky, policy=BackoffPolicy(max_attempts=5, base_delay=0.1), sleep=fake_sleep, rng=random.Random(0))
    assert result == "ok"
    assert calls == 3
    assert 2.0 <= sleeps[0] <= 2.25  # server hint (+ small jitter)
    assert sleeps[1] <= 0.2


async def test_retry_async_gives_up_and_respects_deadline_and_non_retryable() -> None:
    async def always_fail() -> None:
        raise RetryableError("boom")

    async def no_sleep(_: float) -> None:
        return None

    with pytest.raises(RetryExhausted) as info:
        await retry_async(always_fail, policy=BackoffPolicy(max_attempts=3, base_delay=0), sleep=no_sleep)
    assert info.value.attempts == 3

    async def huge_hint() -> None:
        raise RetryableError("slow down", retry_after=10_000)

    with pytest.raises(RetryExhausted):
        await retry_async(huge_hint, policy=BackoffPolicy(max_attempts=5), sleep=no_sleep)

    async def value_error() -> None:
        raise ValueError("not retryable")

    with pytest.raises(ValueError):
        await retry_async(value_error, policy=BackoffPolicy(max_attempts=5), sleep=no_sleep)


def test_exponential_backoff_state() -> None:
    eb = ExponentialBackoff(base=1, cap=8, jitter="none")
    assert [eb.next_delay() for _ in range(5)] == [1, 2, 4, 8, 8]
    assert eb.failures == 5
    eb.reset()
    assert eb.next_delay() == 1


# --------------------------------------------------------------------------- rate limiting


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    async def sleep(self, d: float) -> None:
        self.now += d


async def test_token_bucket_rate_and_penalize() -> None:
    clock = FakeClock()
    bucket = TokenBucket(2.0, 2, clock=clock, sleep=clock.sleep)
    assert await bucket.acquire() == 0
    assert await bucket.acquire() == 0
    waited = await bucket.acquire()
    assert waited == pytest.approx(0.5)
    bucket.penalize(10)
    assert not bucket.try_acquire()
    waited = await bucket.acquire()
    assert waited >= 10
    with pytest.raises(ValueError):
        await bucket.acquire(5)


async def test_token_bucket_real_time_throughput() -> None:
    bucket = TokenBucket(50.0, 1)
    started = time.perf_counter()
    for _ in range(6):
        await bucket.acquire()
    assert time.perf_counter() - started >= 0.08  # 5 waits of 20 ms


def test_rate_limiter_registry_suffix_matching() -> None:
    reg = RateLimiterRegistry(HostLimit(1.0), {"bestbuy.com": HostLimit(5.0, 5), "api.bestbuy.com": HostLimit(4.0, 4)})
    assert reg.for_host("api.bestbuy.com").rate == 4.0
    assert reg.for_host("www.bestbuy.com").rate == 5.0
    assert reg.for_host("example.com").rate == 1.0
    assert reg.for_host("api.bestbuy.com") is reg.for_host("API.BESTBUY.COM")
    assert RateLimiterRegistry(None, {}).for_host("example.com") is None
    assert reg.for_key("discord:abc", 2.0) is reg.for_key("discord:abc", 9.0)


def test_circuit_breaker_transitions() -> None:
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=10, max_timeout=40, clock=clock)
    assert cb.allow()
    cb.record_failure()
    assert cb.state == CircuitBreaker.CLOSED
    cb.record_failure()
    assert cb.state == CircuitBreaker.OPEN and not cb.allow()
    assert cb.seconds_until_retry() == pytest.approx(10)
    clock.now += 10
    assert cb.state == CircuitBreaker.HALF_OPEN
    assert cb.allow()  # single trial
    assert not cb.allow()
    cb.record_failure()  # trial failed -> reopen with doubled timeout
    assert cb.state == CircuitBreaker.OPEN and cb.seconds_until_retry() == pytest.approx(20)
    clock.now += 20
    assert cb.allow()
    cb.record_success()
    assert cb.state == CircuitBreaker.CLOSED and cb.consecutive_failures == 0


# --------------------------------------------------------------------------- metrics


def test_metrics_render_prometheus_text() -> None:
    m = Metrics()
    c = m.counter("hits_total", "Hits", ("route",))
    c.inc(route="/a")
    c.inc(2, route="/a")
    g = m.gauge("queue", "Queue", ())
    g.set(5)
    h = m.histogram("lat_ms", "Latency", ("op",), buckets=(1, 10, 100))
    for v in (0.5, 5, 50, 500):
        h.observe(v, op="x")
    text = m.render()
    assert 'dealradar_hits_total{route="/a"} 3' in text
    assert "dealradar_queue 5" in text
    assert 'dealradar_lat_ms_bucket{op="x",le="1"} 1' in text
    assert 'dealradar_lat_ms_bucket{op="x",le="+Inf"} 4' in text
    assert 'dealradar_lat_ms_count{op="x"} 4' in text
    assert h.quantile(0.5, op="x") is not None
    assert m.counter("hits_total") is c
    with pytest.raises(TypeError):
        m.gauge("hits_total")
    with pytest.raises(ValueError):
        c.inc(route="/b", bogus="1")
    with pytest.raises(ValueError):
        c.inc(-1, route="/a")
    snap = m.snapshot()
    assert "dealradar_lat_ms" in snap


def test_metrics_label_escaping() -> None:
    m = Metrics(namespace="")
    m.counter("x", "x", ("v",)).inc(v='a"b\\c\nd')
    assert 'x{v="a\\"b\\\\c\\nd"} 1' in m.render()


# --------------------------------------------------------------------------- identities


def test_sec_ch_ua_matches_real_chrome_values() -> None:
    assert chromium_sec_ch_ua(120) == '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"'
    assert chromium_sec_ch_ua(124) == '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'
    assert chromium_sec_ch_ua(131) == '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"'


def test_header_profiles_are_coherent() -> None:
    from datetime import date

    assert estimate_chrome_major(date(2025, 6, 24)) == 137
    assert estimate_chrome_major(date(2026, 10, 6)) >= 150
    for profile in build_header_profiles(140):
        headers = profile.build()
        ua = headers["User-Agent"]
        if "Chrome/" in ua:
            assert "sec-ch-ua" in headers and '"140"' in headers["sec-ch-ua"]
            platform = headers["sec-ch-ua-platform"]
            assert ("Windows" in ua) == ("Windows" in platform)
        else:
            assert "sec-ch-ua" not in headers  # Firefox/Safari never send client hints


def test_fetch_mode_drops_navigation_only_headers() -> None:
    for profile in build_header_profiles(140):
        nav = profile.build()
        api = profile.build(fetch_mode="cors", accept="application/json")
        assert "Sec-Fetch-User" not in api and "Upgrade-Insecure-Requests" not in api
        assert api["Accept"] == "application/json" and api["User-Agent"] == nav["User-Agent"]
        if "Sec-Fetch-Dest" in nav:
            assert api["Sec-Fetch-Dest"] == "empty" and api["Sec-Fetch-Mode"] == "cors"


def test_identity_rotator_sticky_and_burn() -> None:
    clock = FakeClock()
    rot = IdentityRotator(build_header_profiles(140), rotation_seconds=60, rng=random.Random(3), clock=clock)
    first = rot.for_host("example.com")
    assert rot.for_host("example.com") is first
    rot.burn("example.com")
    assert rot.for_host("example.com") is not first
    second = rot.for_host("example.com")
    clock.now += 61
    assert rot.for_host("example.com") is not second


def test_cache_bust_and_conditional_cache() -> None:
    url = cache_bust("https://x.test/p?a=1&_=old", "_", "new")
    assert url == "https://x.test/p?a=1&_=new"
    cc = ConditionalCache(max_entries=2)
    cc.store("u1", {"ETag": '"v1"'})
    cc.store("u2", {"Last-Modified": "Wed, 21 Oct 2015 07:28:00 GMT"})
    cc.store("u3", {"etag": '"v3"'})
    assert len(cc) == 2 and cc.headers_for("u1") == {}
    assert cc.headers_for("u3") == {"If-None-Match": '"v3"'}
    cc.store("u3", {})
    assert cc.headers_for("u3") == {}


# --------------------------------------------------------------------------- HttpClient against a real local server


@pytest.fixture
async def server():
    state = {"flaky": 0, "seen_headers": {}}

    async def ok(request: web.Request) -> web.Response:
        state["seen_headers"] = dict(request.headers)
        return web.json_response({"ok": True, "q": request.query.get("q")})

    async def etag(request: web.Request) -> web.Response:
        if request.headers.get("If-None-Match") == '"abc"':
            return web.Response(status=304)
        return web.json_response({"v": 1}, headers={"ETag": '"abc"'})

    async def flaky(request: web.Request) -> web.Response:
        state["flaky"] += 1
        if state["flaky"] < 3:
            return web.Response(status=503, headers={"Retry-After": "0"})
        return web.json_response({"attempt": state["flaky"]})

    async def forbidden(request: web.Request) -> web.Response:
        return web.Response(status=403, text="blocked")

    async def big(request: web.Request) -> web.Response:
        return web.Response(body=b"x" * 50_000)

    async def text(request: web.Request) -> web.Response:
        return web.Response(text="<rss>héllo</rss>", content_type="application/xml", charset="utf-8")

    app = web.Application()
    app.router.add_get("/ok", ok)
    app.router.add_get("/etag", etag)
    app.router.add_get("/flaky", flaky)
    app.router.add_get("/forbidden", forbidden)
    app.router.add_get("/big", big)
    app.router.add_get("/text", text)
    srv = TestServer(app)
    await srv.start_server()
    srv.state = state  # type: ignore[attr-defined]
    yield srv
    await srv.close()


@pytest.fixture
async def client():
    c = HttpClient.create(NetworkSettings(retry=BackoffPolicy(max_attempts=4, base_delay=0, max_delay=0), trust_env=False))
    yield c
    await c.close()


async def test_http_client_json_params_and_identity(server: TestServer, client: HttpClient) -> None:
    resp = await client.get_json(str(server.make_url("/ok")), params={"q": "rtx 4090"}, browser_identity=True)
    assert resp.status == 200 and resp.data == {"ok": True, "q": "rtx 4090"}
    sent = server.state["seen_headers"]  # type: ignore[attr-defined]
    assert sent["User-Agent"].startswith("Mozilla/5.0")
    assert "gzip" in sent["Accept-Encoding"]
    assert client.metrics.counter("http_requests_total", "", ("host", "status")).value(host="127.0.0.1", status=200) == 1


async def test_http_client_conditional_get_returns_not_modified(server: TestServer, client: HttpClient) -> None:
    url = str(server.make_url("/etag"))
    first = await client.get_json(url, conditional=True)
    assert first.data == {"v": 1} and not first.not_modified
    second = await client.get_json(url, conditional=True)
    assert second.not_modified and second.status == 304 and second.data is None


async def test_http_client_retries_transient_statuses(server: TestServer, client: HttpClient) -> None:
    resp = await client.get_json(str(server.make_url("/flaky")))
    assert resp.data == {"attempt": 3} and resp.attempts == 3


async def test_http_client_non_retryable_status_raises(server: TestServer, client: HttpClient) -> None:
    with pytest.raises(HttpStatusError) as info:
        await client.get_json(str(server.make_url("/forbidden")))
    assert info.value.status == 403 and "blocked" in info.value.body


async def test_http_client_max_bytes_and_text(server: TestServer, client: HttpClient) -> None:
    with pytest.raises(ResponseTooLarge):
        await client.get_bytes(str(server.make_url("/big")), max_bytes=1000)
    resp = await client.get_text(str(server.make_url("/text")))
    assert resp.data == "<rss>héllo</rss>"


async def test_http_client_connection_error_exhausts_retries(client: HttpClient) -> None:
    from deal_radar.core.backoff import RetryExhausted

    with pytest.raises(RetryExhausted):
        await client.get_json("http://127.0.0.1:9/never", timeout=1)


async def test_http_client_cache_bust_param(server: TestServer, client: HttpClient) -> None:
    resp = await client.get_json(str(server.make_url("/ok")), bust_cache="_cb")
    assert "_cb=" in resp.url


async def test_http_client_respects_host_rate_limit(server: TestServer) -> None:
    settings = NetworkSettings(host_limits={"127.0.0.1": HostLimit(20.0, 1)}, trust_env=False)
    c = HttpClient.create(settings)
    try:
        started = time.perf_counter()
        await asyncio.gather(*(c.get_json(str(server.make_url("/ok"))) for _ in range(4)))
        assert time.perf_counter() - started >= 0.14  # 3 waits of 50 ms
    finally:
        await c.close()
