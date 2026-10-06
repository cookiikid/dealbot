"""Tests for engine/bus.py — MemoryBus, RedisStreamBus (real redis-server + fakeredis), build_bus."""

from __future__ import annotations

import asyncio
import contextlib
import json
import shutil
import socket
import subprocess
import time
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import redis.asyncio as aioredis
from redis import exceptions as redis_exc

from deal_radar.config_schema import AppConfig, load_config
from deal_radar.core.metrics import Metrics
from deal_radar.engine.bus import (
    PAYLOAD_FIELD,
    BusClosedError,
    BusError,
    BusMessage,
    ListingBus,
    MemoryBus,
    RedisStreamBus,
    build_bus,
    decode_payload,
    encode_listing,
    parse_autoclaim,
    stream_entries,
)
from deal_radar.engine.types import Location, RawListing, SellerInfo, SourceKind

CONFIG_PATH = Path(__file__).parents[1] / "config.yaml"
REDIS_SERVER = Path("/usr/bin/redis-server")


# --------------------------------------------------------------------------- helpers


def make_listing(i: int = 0, **overrides: Any) -> RawListing:
    data: dict[str, Any] = {
        "source": "fb_marketplace",
        "source_kind": SourceKind.LOCAL,
        "source_id": f"1029384756{i}",
        "url": f"https://www.facebook.com/marketplace/item/1029384756{i}/",
        "title": f"RTX 4090 Founders Edition — barely used #{i}",
        "description": "Selling my 4090 FE, works perfectly.\nLocal pickup only, cash or Zelle.",
        "price": "$1,150 OBO",
        "currency": "USD",
        "shipping": 0.0,
        "list_price": 1599.99,
        "condition": "Used - Like New",
        "seller": SellerInfo(name="Jane D.", feedback_score=42, feedback_pct=98.5, account_age_days=2100, is_business=False),
        "location": Location(
            text="Austin, TX",
            city="Austin",
            region="TX",
            postal_code="78701",
            country="US",
            latitude=30.2672,
            longitude=-97.7431,
            distance_miles=12.4,
        ),
        "image_urls": [
            "https://scontent.xx.fbcdn.net/v/t45/1.jpg",
            "https://scontent.xx.fbcdn.net/v/t45/2.jpg",
        ],
        "posted_at": datetime(2026, 10, 5, 18, 30, 15, 123456, tzinfo=timezone.utc),
        "in_stock": True,
        "quantity": 1,
        "retailer": None,
        "sku": None,
        "outbound_url": None,
        "query": "rtx 4090",
        "profile_hint": "rtx_4090",
        "extra": {"marketplace_id": "abc", "photos": 2, "nested": {"delivery": ["pickup"], "ratio": 0.5}},
        "received_at": datetime(2026, 10, 5, 18, 31, 0, tzinfo=timezone.utc),
        "node_id": "laptop-1",
    }
    data.update(overrides)
    return RawListing(**data)


def make_config(**bus_overrides: Any) -> AppConfig:
    """Default config with small bus timings. model_copy skips validation, which lets
    tests use a claim_idle_ms below the production minimum."""
    cfg = AppConfig()
    bus = cfg.bus.model_copy(update={"block_ms": 50, "claim_idle_ms": 300, "batch_size": 8, **bus_overrides})
    storage = cfg.storage.model_copy(update={"redis_url": "redis://127.0.0.1:6379/0", "redis_key_prefix": "test:"})
    return cfg.model_copy(update={"bus": bus, "storage": storage})


async def collect(
    bus: ListingBus, n: int, *, timeout: float = 5.0, ack: bool = True, out: list[BusMessage] | None = None
) -> list[BusMessage]:
    """Consume ``n`` messages (acking them unless told otherwise)."""
    received: list[BusMessage] = out if out is not None else []

    async def run() -> None:
        async with contextlib.aclosing(bus.consume()) as stream:
            async for msg in stream:
                received.append(msg)
                if ack:
                    await bus.ack(msg)
                if len(received) >= n:
                    return

    await asyncio.wait_for(run(), timeout)
    return received


def metric(metrics: Metrics, kind: str, name: str, **labels: Any) -> float:
    collector = metrics.counter(name) if kind == "counter" else metrics.gauge(name)
    return collector.value(**labels)


# --------------------------------------------------------------------------- MemoryBus


async def test_memory_publish_consume_preserves_order_and_object() -> None:
    metrics = Metrics()
    bus = MemoryBus(maxsize=100, metrics=metrics)
    await bus.start()
    listings = [make_listing(i) for i in range(5)]
    for raw in listings:
        await bus.publish(raw)
    assert bus.backlog() == 5
    assert metric(metrics, "gauge", "bus_backlog") == 5

    got = await collect(bus, 5)
    assert [m.raw for m in got] == listings
    assert all(m.raw is raw for m, raw in zip(got, listings))  # no copy on the in-process path
    assert all(m.id is None for m in got)
    assert bus.backlog() == 0
    assert metric(metrics, "counter", "bus_published_total") == 5
    assert metric(metrics, "counter", "bus_consumed_total") == 5
    assert metric(metrics, "gauge", "bus_backlog") == 0
    await bus.close()


async def test_memory_backpressure_blocks_publisher_until_space() -> None:
    metrics = Metrics()
    bus = MemoryBus(maxsize=2, metrics=metrics)
    await bus.publish(make_listing(1))
    await bus.publish(make_listing(2))

    third = asyncio.create_task(bus.publish(make_listing(3)))
    await asyncio.sleep(0.05)
    assert not third.done(), "publish must wait while the queue is full"
    assert bus.backlog() == 2
    assert metric(metrics, "counter", "bus_backpressure_total") == 1

    got = await collect(bus, 1)
    assert got[0].raw.source_id.endswith("1")
    await asyncio.wait_for(third, 1.0)
    assert bus.backlog() == 2
    rest = await collect(bus, 2)
    assert [m.raw.source_id[-1] for m in rest] == ["2", "3"]
    assert metric(metrics, "counter", "bus_published_total") == 3
    await bus.close()


async def test_memory_close_ends_waiting_consumers_promptly() -> None:
    bus = MemoryBus(maxsize=10)
    received: list[BusMessage] = []

    async def consumer() -> None:
        async for msg in bus.consume():
            received.append(msg)

    tasks = [asyncio.create_task(consumer()) for _ in range(3)]
    await bus.publish(make_listing(1))
    await asyncio.sleep(0.02)
    assert len(received) == 1

    started = time.monotonic()
    await bus.close()
    await asyncio.wait_for(asyncio.gather(*tasks), 1.0)
    assert time.monotonic() - started < 0.5
    assert bus.closed

    # A consume() started after close ends immediately, and close is idempotent.
    assert [m async for m in bus.consume()] == []
    await bus.close()


async def test_memory_close_drops_queue_and_rejects_publishers() -> None:
    metrics = Metrics()
    bus = MemoryBus(maxsize=2, metrics=metrics)
    await bus.publish(make_listing(1))
    await bus.publish(make_listing(2))
    blocked = asyncio.create_task(bus.publish(make_listing(3)))
    await asyncio.sleep(0.02)
    assert not blocked.done()

    await bus.close()
    with pytest.raises(BusClosedError):
        await asyncio.wait_for(blocked, 1.0)
    with pytest.raises(BusClosedError):
        await bus.publish(make_listing(4))
    assert bus.backlog() == 0
    assert metric(metrics, "gauge", "bus_backlog") == 0
    # Queued items are discarded: consumers stop instead of draining stale work.
    assert [m async for m in bus.consume()] == []


async def test_memory_close_stops_consumer_mid_stream() -> None:
    bus = MemoryBus(maxsize=10)
    for i in range(5):
        await bus.publish(make_listing(i))
    seen: list[str] = []
    async for msg in bus.consume():
        seen.append(msg.raw.source_id)
        await bus.ack(msg)  # no-op, must not fail
        if len(seen) == 2:
            await bus.close()
    assert len(seen) == 2


async def test_memory_multiple_consumers_share_work_without_duplicates() -> None:
    bus = MemoryBus(maxsize=5)
    seen: dict[str, list[str]] = {"a": [], "b": []}

    async def consumer(tag: str) -> None:
        async for msg in bus.consume():
            seen[tag].append(msg.raw.source_id)
            await asyncio.sleep(0.001)

    tasks = [asyncio.create_task(consumer("a")), asyncio.create_task(consumer("b"))]
    for i in range(40):
        await bus.publish(make_listing(i))  # maxsize 5: exercises backpressure as well
    deadline = time.monotonic() + 2.0
    while len(seen["a"]) + len(seen["b"]) < 40 and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    await bus.close()
    await asyncio.wait_for(asyncio.gather(*tasks), 1.0)
    everything = seen["a"] + seen["b"]
    assert len(everything) == 40 and len(set(everything)) == 40
    assert seen["a"] and seen["b"]


async def test_memory_cancelled_backpressured_publish_leaves_bus_consistent() -> None:
    metrics = Metrics()
    bus = MemoryBus(maxsize=1, metrics=metrics)
    await bus.publish(make_listing(1))
    blocked = asyncio.create_task(bus.publish(make_listing(2)))
    await asyncio.sleep(0.02)
    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked
    await asyncio.sleep(0)  # let the cancelled inner put settle
    assert bus.backlog() == 1  # the cancelled listing was not enqueued
    got = await collect(bus, 1)
    assert got[0].raw.source_id == make_listing(1).source_id
    await bus.publish(make_listing(3))  # a slot is free again: fast path, no waiting
    assert bus.backlog() == 1
    assert metric(metrics, "counter", "bus_published_total") == 2
    await bus.close()


async def test_memory_close_with_single_slot_queue_and_many_waiters() -> None:
    bus = MemoryBus(maxsize=1)
    ended: list[int] = []

    async def consumer(i: int) -> None:
        async for _ in bus.consume():
            pass
        ended.append(i)

    tasks = [asyncio.create_task(consumer(i)) for i in range(5)]
    await asyncio.sleep(0.01)
    await bus.close()  # the sentinel is passed along through a 1-slot queue
    await asyncio.wait_for(asyncio.gather(*tasks), 1.0)
    assert sorted(ended) == list(range(5))


def test_memory_rejects_bad_maxsize() -> None:
    with pytest.raises(ValueError):
        MemoryBus(maxsize=0)


# --------------------------------------------------------------------------- build_bus


def test_build_bus_memory_from_defaults() -> None:
    cfg = AppConfig()
    bus = build_bus(cfg)
    assert isinstance(bus, MemoryBus)
    assert bus.maxsize == cfg.bus.queue_maxsize


def test_build_bus_from_shipped_config() -> None:
    cfg = load_config(CONFIG_PATH, env={})
    bus = build_bus(cfg)
    assert isinstance(bus, MemoryBus)


async def test_build_bus_redis_requires_client() -> None:
    cfg = load_config(CONFIG_PATH, env={"BUS_BACKEND": "redis", "REDIS_URL": "redis://127.0.0.1:6379/0", "NODE_ID": "gcp-1"})
    with pytest.raises(ValueError):
        build_bus(cfg)
    client = aioredis.Redis(host="127.0.0.1", port=6379)  # never connects: construction only
    try:
        metrics = Metrics()
        bus = build_bus(cfg, client, metrics=metrics)
        assert isinstance(bus, RedisStreamBus)
        assert bus.consumer == "gcp-1"
        assert bus.group == cfg.bus.group
        assert bus.stream == f"{cfg.storage.redis_key_prefix}{cfg.bus.stream_key}"
        assert bus.metrics is metrics
    finally:
        await client.aclose()


async def test_block_ms_is_clamped_below_socket_timeout() -> None:
    cfg = make_config(block_ms=1000)
    tight = aioredis.Redis(host="127.0.0.1", port=6379, socket_timeout=0.2)
    roomy = aioredis.Redis(host="127.0.0.1", port=6379, socket_timeout=5.0)
    try:
        assert RedisStreamBus(tight, cfg, "n").block_ms == 150
        assert RedisStreamBus(roomy, cfg, "n").block_ms == 1000
        with pytest.raises(ValueError):
            RedisStreamBus(roomy, cfg, "")
    finally:
        await tight.aclose()
        await roomy.aclose()


# --------------------------------------------------------------------------- pure helpers


def test_stream_entries_accepts_all_reply_shapes() -> None:
    fields = {b"d": b"{}"}
    resp2 = [[b"s", [(b"1-0", fields), (b"2-0", fields)]]]
    resp3_unified = {b"s": [(b"1-0", fields), (b"2-0", fields)]}
    resp3_legacy = {b"s": [[(b"1-0", fields), (b"2-0", fields)]]}
    unparsed = [[b"s", [[b"1-0", [b"d", b"{}"]], [b"2-0", [b"d", b"{}"]]]]]
    for shape in (resp2, resp3_unified, resp3_legacy, unparsed):
        entries = stream_entries(shape)
        assert [e[0] for e in entries] == [b"1-0", b"2-0"]
        assert all(e[1] == fields for e in entries)
    assert stream_entries(None) == []
    assert stream_entries([]) == []
    assert stream_entries({}) == []


def test_parse_autoclaim_shapes() -> None:
    cursor, entries, deleted = parse_autoclaim([b"5-0", [(b"3-0", {b"d": b"x"}), (None, None)], [b"4-0"]])
    assert cursor == "5-0"
    assert entries == [(b"3-0", {b"d": b"x"}), (None, None)]
    assert deleted == ["4-0"]
    assert parse_autoclaim(["0-0", [], []]) == ("0-0", [], [])
    assert parse_autoclaim([b"0-0", []]) == ("0-0", [], [])  # Redis 6.2 (no deleted list)
    assert parse_autoclaim(None) == ("0-0", [], [])


def test_decode_payload_variants() -> None:
    raw = make_listing(7)
    payload = raw.model_dump_json()
    ok, problem = decode_payload({b"d": payload.encode()})
    assert problem is None and ok == raw
    ok_str, _ = decode_payload({"d": payload})  # decode_responses=True clients
    assert ok_str == raw
    assert decode_payload({}) == (None, "trimmed")
    assert decode_payload(None) == (None, "trimmed")
    assert decode_payload({b"x": b"1"}) == (None, "missing_payload")
    bad_json, why = decode_payload({b"d": b"{not json"})
    assert bad_json is None and why is not None and why.startswith("invalid")
    bad_schema, why = decode_payload({b"d": b'{"source": "ebay", "unknown_field": 1}'})
    assert bad_schema is None and why is not None and why.startswith("invalid")


class _Hostile:
    def __str__(self) -> str:
        raise RuntimeError("no str")

    def __repr__(self) -> str:
        raise RuntimeError("no repr")


def test_encode_listing_fast_path_is_plain_model_dump_json() -> None:
    raw = make_listing(1)
    payload, sanitized = encode_listing(raw)
    assert not sanitized
    assert payload == raw.model_dump_json()


def test_encode_listing_scrubs_content_that_json_cannot_hold() -> None:
    # Truncated emoji in upstream JSON decode to lone surrogates; pydantic refuses to
    # encode them (and so would orjson), which used to raise out of publish().
    title = json.loads('"RTX 4090 FE \\ud83d deal"')
    cyclic: dict[str, Any] = {"a": 1}
    cyclic["self"] = cyclic
    raw = make_listing(
        2,
        title=title,
        description="split pair 😀 ok",
        seller=SellerInfo(name="Bob \udcff"),
        extra={"body": b"\xff\xfe raw", "cyclic": cyclic, b"key": [b"\x00ok", {"\ud800": 1}]},
    )
    with pytest.raises(ValueError):
        raw.model_dump_json()  # the failure encode_listing exists for
    payload, sanitized = encode_listing(raw)
    assert sanitized
    back = RawListing.model_validate_json(payload)
    assert back.title == "RTX 4090 FE � deal"
    assert back.description == "split pair \U0001f600 ok"  # a valid pair is re-joined, not mangled
    assert back.seller is not None and back.seller.name == "Bob �"
    assert back.extra["body"] == "�� raw"
    assert back.extra["cyclic"]["a"] == 1 and isinstance(back.extra["cyclic"]["self"], dict)
    assert back.extra["key"] == ["\x00ok", {"�": 1}]
    assert back.location == raw.location and back.posted_at == raw.posted_at
    assert back.source_kind is raw.source_kind and back.listing_key == raw.listing_key

    hostile, sanitized = encode_listing(make_listing(3, title=title, extra={"obj": _Hostile()}))
    assert sanitized
    back = RawListing.model_validate_json(hostile)
    assert back.extra == {"bus_unserializable_extra": True}
    assert back.title == "RTX 4090 FE � deal"


# --------------------------------------------------------------------------- real redis-server


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _ping(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2) as sock:
            sock.sendall(b"PING\r\n")
            return sock.recv(64).startswith(b"+PONG")
    except OSError:
        return False


def _redis_binary() -> str | None:
    return str(REDIS_SERVER) if REDIS_SERVER.exists() else shutil.which("redis-server")


def _spawn_redis(binary: str, port: int) -> subprocess.Popen[bytes] | None:
    """Start a throw-away, persistence-free redis-server; None if it does not come up."""
    proc = subprocess.Popen(
        [
            binary, "--port", str(port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no",
            "--protected-mode", "no", "--daemonize", "no", "--loglevel", "warning",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline and proc.poll() is None and not _ping(port):
        time.sleep(0.02)
    if proc.poll() is None and _ping(port):
        return proc
    _stop_redis(proc)
    return None


def _stop_redis(proc: subprocess.Popen[bytes]) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture(scope="module")
def redis_port() -> Iterator[int]:
    binary = _redis_binary()
    if not binary:
        pytest.skip("redis-server binary not available")
    for _ in range(3):
        port = _free_port()
        proc = _spawn_redis(binary, port)
        if proc is not None:
            break
    else:
        pytest.skip("could not start redis-server")
    try:
        yield port
    finally:
        _stop_redis(proc)


@pytest.fixture
async def rclient(redis_port: int) -> AsyncIterator[aioredis.Redis]:
    client = aioredis.Redis(host="127.0.0.1", port=redis_port, socket_timeout=5.0)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.aclose()


async def pending_count(client: Any, bus: RedisStreamBus) -> int:
    info = await client.xpending(bus.stream, bus.group)
    return int(info["pending"])


async def test_redis_round_trip_full_listing(rclient: aioredis.Redis) -> None:
    cfg = make_config()
    metrics = Metrics()
    producer = RedisStreamBus(rclient, cfg, "laptop-1", metrics=Metrics())
    consumer = RedisStreamBus(rclient, cfg, "gcp-1", metrics=metrics)
    await producer.start()
    await consumer.start()
    await consumer.start()  # idempotent

    original = make_listing(1)
    await producer.publish(original)
    got = await collect(consumer, 1, ack=False)
    msg = got[0]
    assert isinstance(msg.id, str) and "-" in msg.id
    assert msg.raw == original
    assert msg.raw.seller == original.seller and msg.raw.location == original.location
    assert msg.raw.posted_at == original.posted_at and msg.raw.posted_at.tzinfo is not None
    assert msg.raw.received_at == original.received_at
    assert msg.raw.extra == original.extra
    assert msg.raw.listing_key == original.listing_key
    assert await pending_count(rclient, consumer) == 1
    await consumer.ack(msg)
    assert await pending_count(rclient, consumer) == 0
    assert metric(metrics, "counter", "bus_consumed_total") == 1
    assert metric(producer.metrics, "counter", "bus_published_total") == 1
    # Stored with the documented layout: one field "d" holding RawListing JSON.
    entries = await rclient.xrange(producer.stream)
    assert len(entries) == 1 and set(entries[0][1]) == {PAYLOAD_FIELD.encode()}
    await producer.close()
    await consumer.close()


async def test_redis_publish_scrubs_unencodable_listing(rclient: aioredis.Redis) -> None:
    metrics = Metrics()
    bus = RedisStreamBus(rclient, make_config(), "gcp-1", metrics=metrics)
    await bus.start()
    title = json.loads('"Steam Deck OLED \\ud83d"')
    await bus.publish(make_listing(1, title=title, extra={"html": b"\xff<div>"}))
    got = await collect(bus, 1)
    assert got[0].raw.title == "Steam Deck OLED �"
    assert got[0].raw.extra == {"html": "�<div>"}
    assert metric(metrics, "counter", "bus_sanitized_total") == 1
    assert metric(metrics, "counter", "bus_published_total") == 1
    await bus.close()


async def test_redis_ack_after_close_still_acknowledges(rclient: aioredis.Redis) -> None:
    # main.py closes the bus first and then drains in-flight workers, which ack in `finally`.
    bus = RedisStreamBus(rclient, make_config(), "gcp-1")
    await bus.publish(make_listing(1))
    got = await collect(bus, 1, ack=False)
    await bus.close()
    await bus.ack(got[0])
    assert await pending_count(rclient, bus) == 0


async def test_redis_entries_published_before_first_consumer_are_delivered(rclient: aioredis.Redis) -> None:
    cfg = make_config()
    producer = RedisStreamBus(rclient, cfg, "laptop-1")
    # No start(): publish must create the group itself so nothing published is skipped.
    for i in range(3):
        await producer.publish(make_listing(i))
    groups = await rclient.xinfo_groups(producer.stream)
    assert [g["name"] for g in groups] == [cfg.bus.group.encode()]
    consumer = RedisStreamBus(rclient, cfg, "gcp-1")
    got = await collect(consumer, 3)
    assert [m.raw.source_id for m in got] == [make_listing(i).source_id for i in range(3)]
    await consumer.close()


async def test_redis_group_creation_is_idempotent_and_keeps_existing_group(rclient: aioredis.Redis) -> None:
    cfg = make_config()
    first = RedisStreamBus(rclient, cfg, "a")
    await first.start()
    await first.publish(make_listing(1))
    second = RedisStreamBus(rclient, cfg, "b")
    await second.start()  # BUSYGROUP ignored; group (and its undelivered entry) kept
    got = await collect(second, 1)
    assert got[0].raw.source_id == make_listing(1).source_id


async def test_redis_two_consumers_split_messages_without_duplicates(rclient: aioredis.Redis) -> None:
    cfg = make_config(batch_size=4)
    producer = RedisStreamBus(rclient, cfg, "collector")
    a = RedisStreamBus(rclient, cfg, "proc-a")
    b = RedisStreamBus(rclient, cfg, "proc-b")
    for bus in (producer, a, b):
        await bus.start()
    seen: dict[str, list[str]] = {"a": [], "b": []}

    async def worker(tag: str, bus: RedisStreamBus) -> None:
        async for msg in bus.consume():
            seen[tag].append(msg.raw.source_id)
            await asyncio.sleep(0.003)  # simulated processing
            await bus.ack(msg)

    tasks = [asyncio.create_task(worker("a", a)), asyncio.create_task(worker("b", b))]
    await asyncio.sleep(0.1)  # both consumers blocked in XREADGROUP
    total = 60
    for i in range(total):
        await producer.publish(make_listing(i))
        if i % 3 == 0:
            await asyncio.sleep(0.002)
    deadline = time.monotonic() + 5.0
    while len(seen["a"]) + len(seen["b"]) < total and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    await a.close()
    await b.close()
    await asyncio.wait_for(asyncio.gather(*tasks), 2.0)

    everything = seen["a"] + seen["b"]
    assert len(everything) == total
    assert len(set(everything)) == total, "a message was delivered to both consumers"
    assert seen["a"] and seen["b"], "the group should spread work over both consumers"
    assert await pending_count(rclient, producer) == 0


async def test_redis_unacked_message_is_reclaimed_by_running_consumer(rclient: aioredis.Redis) -> None:
    cfg = make_config(claim_idle_ms=300)
    producer = RedisStreamBus(rclient, cfg, "collector")
    crashed = RedisStreamBus(rclient, cfg, "proc-a")
    survivor_metrics = Metrics()
    survivor = RedisStreamBus(rclient, cfg, "proc-b", metrics=survivor_metrics)
    await producer.start()
    for i in range(3):
        await producer.publish(make_listing(i))

    taken = await collect(crashed, 3, ack=False)  # proc-a "crashes" holding 3 entries
    taken_at = time.monotonic()
    await crashed.close()
    assert await pending_count(rclient, producer) == 3

    # proc-b is already running when the entries become claimable: its periodic
    # XAUTOCLAIM scan (not just the start-up one) must pick them up.
    got = await collect(survivor, 3, timeout=5.0)
    waited = time.monotonic() - taken_at
    assert sorted(m.id for m in got) == sorted(m.id for m in taken)
    assert sorted(m.raw.source_id for m in got) == sorted(m.raw.source_id for m in taken)
    assert waited >= 0.25, "entries must not be stolen before claim_idle_ms"
    assert metric(survivor_metrics, "counter", "bus_recovered_total", via="autoclaim") == 3
    assert await pending_count(rclient, producer) == 0
    await survivor.close()


async def test_redis_reclaim_walks_the_whole_pel_through_the_cursor(rclient: aioredis.Redis) -> None:
    # batch_size=2 -> XAUTOCLAIM COUNT 2: recovering 7 entries needs several cursor steps.
    cfg = make_config(claim_idle_ms=200, batch_size=2)
    producer = RedisStreamBus(rclient, cfg, "collector")
    await producer.start()
    for i in range(7):
        await producer.publish(make_listing(i))
    crashed = RedisStreamBus(rclient, cfg, "proc-a")
    await collect(crashed, 7, ack=False)
    await crashed.close()
    await asyncio.sleep(0.25)

    survivor = RedisStreamBus(rclient, cfg, "proc-b")
    got = await collect(survivor, 7, timeout=5.0)
    assert len({m.id for m in got}) == 7
    assert await pending_count(rclient, producer) == 0


async def test_redis_restarted_consumer_resumes_its_own_pending_entries(rclient: aioredis.Redis) -> None:
    cfg = make_config(claim_idle_ms=60_000)  # far away: recovery must come from the own-PEL replay
    producer = RedisStreamBus(rclient, cfg, "collector")
    await producer.start()
    for i in range(2):
        await producer.publish(make_listing(i))
    before = RedisStreamBus(rclient, cfg, "gcp-1")
    held = await collect(before, 2, ack=False)
    await before.close()

    metrics = Metrics()
    after = RedisStreamBus(rclient, cfg, "gcp-1", metrics=metrics)  # same node id after restart
    started = time.monotonic()
    got = await collect(after, 2, timeout=3.0)
    assert time.monotonic() - started < 1.0
    assert [m.id for m in got] == [m.id for m in held]
    assert metric(metrics, "counter", "bus_recovered_total", via="pel") == 2
    assert await pending_count(rclient, producer) == 0
    await after.close()


async def test_redis_inflight_entry_is_not_redelivered_locally(rclient: aioredis.Redis) -> None:
    cfg = make_config(claim_idle_ms=150, block_ms=30)
    producer = RedisStreamBus(rclient, cfg, "collector")
    bus = RedisStreamBus(rclient, cfg, "gcp-1")
    bus.reclaim_grace = 30.0  # isolate the in-flight guard from scheduler jitter
    await producer.start()
    await producer.publish(make_listing(1))
    received: list[BusMessage] = []

    async def run() -> None:
        async for msg in bus.consume():
            received.append(msg)  # never acked while the test watches: a slow worker

    task = asyncio.create_task(run())
    await asyncio.sleep(0.45)  # > claim_idle + scan interval: the scan reclaims it from ourselves
    assert len(received) == 1, "a slow local worker's entry must not be handed out twice"
    # ...but the scan did claim it, which keeps other nodes from stealing a live entry.
    detail = await rclient.xpending_range(producer.stream, producer.group, "-", "+", 10)
    assert detail[0]["times_delivered"] >= 2
    await bus.ack(received[0])
    await bus.close()
    await asyncio.wait_for(task, 1.0)
    assert await pending_count(rclient, producer) == 0


async def test_redis_entry_lost_by_local_worker_is_redelivered_after_grace(rclient: aioredis.Redis) -> None:
    cfg = make_config(claim_idle_ms=100, block_ms=30)
    producer = RedisStreamBus(rclient, cfg, "collector")
    metrics = Metrics()
    bus = RedisStreamBus(rclient, cfg, "gcp-1", metrics=metrics)
    bus.reclaim_grace = 0.25  # production: 4 x claim_idle
    await producer.start()
    await producer.publish(make_listing(1))
    received: list[BusMessage] = []

    async def run() -> None:
        async for msg in bus.consume():
            received.append(msg)  # first delivery is "lost" by its worker (never acked)
            if len(received) == 2:
                await bus.ack(msg)

    task = asyncio.create_task(run())
    deadline = time.monotonic() + 3.0
    while len(received) < 2 and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    await bus.close()
    await asyncio.wait_for(task, 1.0)
    assert len(received) == 2 and received[0].id == received[1].id
    assert metric(metrics, "counter", "bus_recovered_total", via="autoclaim") == 1
    assert await pending_count(rclient, producer) == 0


async def test_redis_poison_messages_are_acked_and_counted(rclient: aioredis.Redis) -> None:
    cfg = make_config()
    metrics = Metrics()
    bus = RedisStreamBus(rclient, cfg, "gcp-1", metrics=metrics)
    await bus.start()
    await rclient.xadd(bus.stream, {PAYLOAD_FIELD: b"{definitely not json"})
    await rclient.xadd(bus.stream, {PAYLOAD_FIELD: b'{"source": "ebay", "bogus": true}'})
    await rclient.xadd(bus.stream, {"other": "field"})
    await bus.publish(make_listing(9))

    got = await collect(bus, 1)
    assert [m.raw.source_id for m in got] == [make_listing(9).source_id]
    assert metric(metrics, "counter", "bus_poison_total") == 3
    assert metric(metrics, "counter", "bus_consumed_total") == 1
    assert await pending_count(rclient, bus) == 0  # poison is acked, never redelivered
    await bus.close()


async def test_redis_trimmed_pending_entries_are_counted_as_lost(rclient: aioredis.Redis) -> None:
    cfg = make_config(claim_idle_ms=200)
    producer = RedisStreamBus(rclient, cfg, "collector")
    await producer.start()
    for i in range(2):
        await producer.publish(make_listing(i))
    holder = RedisStreamBus(rclient, cfg, "gcp-1")
    held = await collect(holder, 2, ack=False)
    await holder.close()
    for msg in held:
        await rclient.xdel(producer.stream, msg.id)  # what MAXLEN trimming does to a lagging PEL

    # Own-PEL replay path: deleted entries come back with empty fields.
    metrics = Metrics()
    restarted = RedisStreamBus(rclient, cfg, "gcp-1", metrics=metrics)
    await producer.publish(make_listing(5))
    got = await collect(restarted, 1)
    assert got[0].raw.source_id == make_listing(5).source_id
    assert metric(metrics, "counter", "bus_lost_total") == 2
    assert metric(metrics, "counter", "bus_poison_total") == 0
    assert await pending_count(rclient, producer) == 0
    await restarted.close()


async def test_redis_trimmed_entries_found_by_autoclaim_are_counted(rclient: aioredis.Redis) -> None:
    cfg = make_config(claim_idle_ms=200)
    producer = RedisStreamBus(rclient, cfg, "collector")
    await producer.start()
    await producer.publish(make_listing(1))
    crashed = RedisStreamBus(rclient, cfg, "proc-a")
    held = await collect(crashed, 1, ack=False)
    await crashed.close()
    await rclient.xdel(producer.stream, held[0].id)
    await asyncio.sleep(0.25)

    metrics = Metrics()
    survivor = RedisStreamBus(rclient, cfg, "proc-b", metrics=metrics)
    await producer.publish(make_listing(2))
    got = await collect(survivor, 1)
    assert got[0].raw.source_id == make_listing(2).source_id
    assert metric(metrics, "counter", "bus_lost_total") == 1
    assert await pending_count(rclient, producer) == 0
    await survivor.close()


async def test_redis_close_terminates_blocked_consume_promptly(rclient: aioredis.Redis) -> None:
    cfg = make_config(block_ms=400)
    bus = RedisStreamBus(rclient, cfg, "gcp-1")
    await bus.start()
    received: list[BusMessage] = []

    async def run() -> None:
        async for msg in bus.consume():
            received.append(msg)

    task = asyncio.create_task(run())
    await asyncio.sleep(0.15)  # now blocked inside XREADGROUP BLOCK 400
    started = time.monotonic()
    await bus.close()
    await asyncio.wait_for(task, 2.0)
    assert time.monotonic() - started < 0.4 + 0.3
    assert received == []
    with pytest.raises(BusClosedError):
        await bus.publish(make_listing(1))
    assert [m async for m in bus.consume()] == []
    await bus.close()  # idempotent


async def test_redis_close_mid_batch_leaves_rest_pending_for_recovery(rclient: aioredis.Redis) -> None:
    cfg = make_config(batch_size=10, claim_idle_ms=200)
    producer = RedisStreamBus(rclient, cfg, "collector")
    await producer.start()
    for i in range(5):
        await producer.publish(make_listing(i))
    bus = RedisStreamBus(rclient, cfg, "proc-a")
    seen: list[BusMessage] = []
    async with contextlib.aclosing(bus.consume()) as stream:
        async for msg in stream:
            seen.append(msg)
            await bus.ack(msg)
            if len(seen) == 2:
                await bus.close()
    assert len(seen) == 2
    # The other 3 were fetched in the same batch: still pending, recovered by another node.
    assert await pending_count(rclient, producer) == 3
    other = RedisStreamBus(rclient, cfg, "proc-b")
    got = await collect(other, 3, timeout=5.0)
    assert {m.raw.source_id for m in got} == {make_listing(i).source_id for i in range(2, 5)}


async def test_redis_backlog_reports_group_lag(rclient: aioredis.Redis) -> None:
    cfg = make_config()
    metrics = Metrics()
    bus = RedisStreamBus(rclient, cfg, "gcp-1", metrics=metrics)
    await bus.start()
    assert bus.backlog() == 0
    for i in range(5):
        await bus.publish(make_listing(i))
    assert await bus.refresh_backlog() == 5
    assert bus.backlog() == 5
    assert metric(metrics, "gauge", "bus_backlog") == 5
    got = await collect(bus, 5, ack=False)
    assert await bus.refresh_backlog() == 0
    assert metric(metrics, "gauge", "bus_pending") == 5
    for msg in got:
        await bus.ack(msg)
    await bus.refresh_backlog()
    assert metric(metrics, "gauge", "bus_pending") == 0
    await bus.close()


async def test_redis_backlog_falls_back_to_xlen_without_group(rclient: aioredis.Redis) -> None:
    cfg = make_config()
    bus = RedisStreamBus(rclient, cfg, "gcp-1")
    assert await bus.refresh_backlog() == 0  # stream does not exist yet
    await rclient.xadd(bus.stream, {PAYLOAD_FIELD: make_listing(1).model_dump_json()})
    assert await bus.refresh_backlog() == 1  # no group yet -> XLEN


async def test_redis_maxlen_trims_stream_approximately(rclient: aioredis.Redis) -> None:
    cfg = make_config(maxlen=1000)
    bus = RedisStreamBus(rclient, cfg, "collector")
    await bus.start()
    for i in range(1600):
        await bus.publish(make_listing(i))
    length = await rclient.xlen(bus.stream)
    assert 1000 <= length < 1600


@contextlib.asynccontextmanager
async def one_entry_per_stream_node(client: aioredis.Redis) -> AsyncIterator[None]:
    """Make approximate (``~``) trimming exact so the trim floor can be asserted precisely."""
    previous = (await client.config_get("stream-node-max-entries"))["stream-node-max-entries"]
    await client.config_set("stream-node-max-entries", 1)
    try:
        yield
    finally:
        await client.config_set("stream-node-max-entries", previous)


async def test_redis_trim_removes_only_fully_processed_history(rclient: aioredis.Redis) -> None:
    async with one_entry_per_stream_node(rclient):
        cfg = make_config(batch_size=20)
        metrics = Metrics()
        bus = RedisStreamBus(rclient, cfg, "gcp-1", metrics=metrics)
        await bus.start()
        assert await bus.trim_acknowledged() == 0  # empty stream
        for i in range(30):
            await bus.publish(make_listing(i))
        ids = [entry_id.decode() for entry_id, _ in await rclient.xrange(bus.stream)]
        delivered = await collect(bus, 20, ack=False)
        assert [m.id for m in delivered] == ids[:20]
        for msg in delivered[:5] + delivered[8:10]:
            await bus.ack(msg)  # acked: 0-4 and 8-9; pending: 5-7 and 10-19; undelivered: 20-29

        assert await bus.trim_acknowledged() == 5  # only 0-4: below the oldest pending entry (5)
        remaining = [entry_id.decode() for entry_id, _ in await rclient.xrange(bus.stream)]
        assert remaining == ids[5:]
        assert await pending_count(rclient, bus) == 13
        assert metric(metrics, "counter", "bus_trimmed_total") == 5

        for msg in delivered[5:8] + delivered[10:]:
            await bus.ack(msg)
        # Nothing pending: the floor is the last delivered id (19), undelivered 20-29 stay.
        assert await bus.trim_acknowledged() == 14
        assert [e.decode() for e, _ in await rclient.xrange(bus.stream)] == ids[19:]
        rest = await collect(bus, 10)
        assert [m.id for m in rest] == ids[20:]
        assert await bus.trim_acknowledged() == 10  # keeps only the last delivered entry
        assert await rclient.xlen(bus.stream) == 1
        await bus.close()


async def test_redis_trim_respects_every_consumer_group(rclient: aioredis.Redis) -> None:
    async with one_entry_per_stream_node(rclient):
        cfg = make_config(batch_size=50)
        bus = RedisStreamBus(rclient, cfg, "gcp-1")
        await bus.start()
        # An unrelated group (analytics, debugging) created before any entry and never read.
        await rclient.xgroup_create(bus.stream, "audit", id="$")
        for i in range(10):
            await bus.publish(make_listing(i))
        await collect(bus, 10)  # our group delivered + acked everything
        assert await bus.trim_acknowledged() == 0
        assert await rclient.xlen(bus.stream) == 10
        await rclient.xreadgroup("audit", "x", {bus.stream: ">"}, count=4)  # audit: 4 delivered, pending
        assert await bus.trim_acknowledged() == 0  # its oldest pending entry is the first one
        first_four = [e for e, _ in await rclient.xrange(bus.stream, count=4)]
        await rclient.xack(bus.stream, "audit", *first_four)
        assert await bus.trim_acknowledged() == 3  # floor = audit's last delivered (4th) entry
        await bus.close()


async def test_redis_trim_is_a_noop_without_any_group(rclient: aioredis.Redis) -> None:
    bus = RedisStreamBus(rclient, make_config(), "gcp-1")
    assert await bus.trim_acknowledged() == 0  # no stream at all
    await rclient.xadd(bus.stream, {PAYLOAD_FIELD: make_listing(1).model_dump_json()})
    assert await bus.trim_acknowledged() == 0  # stream without group: entries wait for one
    assert await rclient.xlen(bus.stream) == 1


async def test_redis_consumer_trims_acknowledged_history_periodically(
    rclient: aioredis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    import deal_radar.engine.bus as bus_module

    monkeypatch.setattr(bus_module, "_TRIM_INTERVAL_SECONDS", 0.05)
    cfg = make_config(batch_size=10)
    metrics = Metrics()
    bus = RedisStreamBus(rclient, cfg, "gcp-1", metrics=metrics)
    await bus.start()
    for i in range(200):
        await bus.publish(make_listing(i))
    received: list[BusMessage] = []

    async def run() -> None:
        async for msg in bus.consume():
            received.append(msg)
            await bus.ack(msg)

    task = asyncio.create_task(run())
    deadline = time.monotonic() + 5.0
    while (len(received) < 200 or await rclient.xlen(bus.stream) > 10) and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    await bus.close()
    await asyncio.wait_for(task, 2.0)
    assert len({m.id for m in received}) == 200
    assert await rclient.xlen(bus.stream) <= 10  # history follows the backlog, not maxlen
    assert metric(metrics, "counter", "bus_trimmed_total") >= 190
    assert await bus.refresh_backlog() == 0  # group lag is still computable after MINID trims

    off = RedisStreamBus(rclient, cfg, "gcp-2")
    off.trim_acked = False
    await off.publish(make_listing(999))
    await collect(off, 1)
    assert metric(off.metrics, "counter", "bus_trimmed_total") == 0


async def test_redis_start_and_consume_work_while_redis_is_out_of_memory(
    rclient: aioredis.Redis, fast_publish_backoff: None
) -> None:
    cfg = make_config()
    producer = RedisStreamBus(rclient, cfg, "collector")
    await producer.start()
    for i in range(3):
        await producer.publish(make_listing(i))
    await rclient.config_set("maxmemory", 1)  # every denyoom command (XADD, XGROUP CREATE) now fails
    try:
        late = RedisStreamBus(rclient, cfg, "collector-2")
        blocked = asyncio.create_task(late.publish(make_listing(3)))
        consumer = RedisStreamBus(rclient, cfg, "gcp-1")
        await consumer.start()  # XGROUP CREATE refused with OOM, but the group exists
        got = await collect(consumer, 3)  # XREADGROUP / XACK are allowed under OOM
        assert sorted(m.raw.source_id for m in got) == sorted(make_listing(i).source_id for i in range(3))
        assert not blocked.done(), "publish waits while Redis refuses writes"
        missing = RedisStreamBus(rclient, cfg.model_copy(update={"bus": cfg.bus.model_copy(update={"group": "other"})}), "x")
        with pytest.raises(BusError):
            await missing.start()  # a group that does not exist cannot be created under OOM
    finally:
        await rclient.config_set("maxmemory", 0)
    await asyncio.wait_for(blocked, 2.0)  # memory is back: the waiting publish goes through
    rest = await collect(consumer, 1)
    assert rest[0].raw.source_id == make_listing(3).source_id
    await consumer.close()


async def test_redis_processor_trimming_unblocks_publishers_at_maxmemory(fast_publish_backoff: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """At maxmemory XADD is refused and cannot trim the stream itself; the processor's
    MINID trims of acknowledged history must free memory so publishing never deadlocks."""
    import deal_radar.engine.bus as bus_module

    binary = _redis_binary()
    if not binary:
        pytest.skip("redis-server binary not available")
    port = _free_port()
    proc = _spawn_redis(binary, port)
    if proc is None:
        pytest.skip("could not start redis-server")
    monkeypatch.setattr(bus_module, "_TRIM_INTERVAL_SECONDS", 0.05)
    client = aioredis.Redis(host="127.0.0.1", port=port, socket_timeout=5.0)
    cfg = make_config(batch_size=32, maxlen=100_000)  # MAXLEN far away: only MINID trims help
    producer = RedisStreamBus(client, cfg, "laptop-1")
    consumer = RedisStreamBus(client, cfg, "gcp-1")
    received: list[str] = []
    task: asyncio.Task[None] | None = None
    try:
        await producer.start()
        used = int((await client.info("memory"))["used_memory"])
        await client.config_set("maxmemory", used + 1_500_000)  # ~1.5 MB of headroom
        await client.config_set("maxmemory-policy", "noeviction")

        async def run() -> None:
            async for msg in consumer.consume():
                received.append(msg.raw.source_id)
                await consumer.ack(msg)

        total = 1500  # ~4 MB of listings: more than twice the headroom

        async def produce() -> None:
            for i in range(total):
                await producer.publish(make_listing(i, description="x" * 1500))

        producing = asyncio.create_task(produce())
        deadline = time.monotonic() + 10.0
        while metric(producer.metrics, "counter", "bus_errors_total", op="publish") == 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert not producing.done(), "the collector must be stuck on OOM before the processor starts"
        stuck_at = metric(producer.metrics, "counter", "bus_published_total")
        assert 0 < stuck_at < total

        task = asyncio.create_task(run())  # processor (re)starts: consumes, acks and trims
        await asyncio.wait_for(producing, 20.0)
        deadline = time.monotonic() + 10.0
        while len(received) < total and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert len(set(received)) == total
        assert metric(consumer.metrics, "counter", "bus_trimmed_total") > 0
    finally:
        await consumer.close()
        if task is not None:
            await asyncio.wait_for(task, 5.0)
        await client.aclose()
        _stop_redis(proc)


@pytest.mark.parametrize("gap", [0.0, 0.1], ids=["publish-immediately", "publish-later"])
async def test_redis_consumer_recovers_after_stream_and_group_vanish(rclient: aioredis.Redis, gap: float) -> None:
    # The idle consumer is parked in XREADGROUP BLOCK; Redis 7 wakes it with
    # "-UNBLOCKED the stream key no longer exists". An entry XADDed right after the
    # loss (gap 0, before the group is recreated) must not be skipped by a "$" group.
    cfg = make_config()
    producer = RedisStreamBus(rclient, cfg, "collector")
    metrics = Metrics()
    consumer = RedisStreamBus(rclient, cfg, "gcp-1", metrics=metrics)
    await producer.start()
    await producer.publish(make_listing(1))
    received: list[BusMessage] = []

    async def run() -> None:
        async for msg in consumer.consume():
            received.append(msg)
            await consumer.ack(msg)

    task = asyncio.create_task(run())
    deadline = time.monotonic() + 3.0
    while not received and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    assert len(received) == 1

    await rclient.delete(producer.stream)  # FLUSHALL / eviction: stream and group are gone
    await asyncio.sleep(gap)
    await producer.publish(make_listing(2))  # XADD recreates the stream without the group
    deadline = time.monotonic() + 5.0
    while len(received) < 2 and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    await consumer.close()
    await asyncio.wait_for(task, 2.0)
    assert [m.raw.source_id for m in received] == [make_listing(1).source_id, make_listing(2).source_id]
    assert metric(metrics, "counter", "bus_errors_total", op="consume") >= 1


class FlakyRedis:
    """Delegates to a real client but fails selected commands a number of times."""

    def __init__(self, inner: Any, failures: dict[str, int], exc: BaseException | None = None) -> None:
        self._inner = inner
        self.failures = dict(failures)
        self.calls: dict[str, int] = {}
        self.exc = exc or redis_exc.ConnectionError("Connection reset by peer")

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._inner, name)
        if name not in self.failures:
            return target

        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            self.calls[name] = self.calls.get(name, 0) + 1
            if self.failures[name] != 0:
                self.failures[name] -= 1
                raise self.exc
            return await target(*args, **kwargs)

        return wrapper


async def test_redis_group_recreation_after_loss_survives_an_intermediate_error(rclient: aioredis.Redis) -> None:
    cfg = make_config()
    producer = RedisStreamBus(rclient, cfg, "collector")
    await producer.start()
    flaky = FlakyRedis(rclient, {"xgroup_create": 0}, exc=redis_exc.OutOfMemoryError("command not allowed when used memory > 'maxmemory'"))
    metrics = Metrics()
    consumer = RedisStreamBus(flaky, cfg, "gcp-1", metrics=metrics)  # type: ignore[arg-type]
    received: list[str] = []

    async def run() -> None:
        async for msg in consumer.consume():
            received.append(msg.raw.source_id)
            await consumer.ack(msg)

    task = asyncio.create_task(run())
    await producer.publish(make_listing(1))
    deadline = time.monotonic() + 3.0
    while not received and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    assert received == [make_listing(1).source_id]

    flaky.failures["xgroup_create"] = 1  # the first recreation attempt hits OOM
    await rclient.delete(producer.stream)  # stream + group gone (eviction / FLUSHALL)
    await producer.publish(make_listing(2))  # recreates the stream, without the group
    deadline = time.monotonic() + 5.0
    while len(received) < 2 and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    await consumer.close()
    await asyncio.wait_for(task, 2.0)
    # The recreation must still start from "0" after the OOM, or listing 2 is skipped.
    assert received == [make_listing(1).source_id, make_listing(2).source_id]
    assert flaky.calls["xgroup_create"] >= 3  # initial create, OOM, successful recreate
    assert metric(metrics, "counter", "bus_errors_total", op="consume") >= 2


def test_loop_backoff_saturates_instead_of_overflowing() -> None:
    from deal_radar.engine.bus import _LoopBackoff

    backoff = _LoopBackoff(0.1, 5.0)
    delays = [backoff.next_delay() for _ in range(5000)]  # core ExponentialBackoff dies at #1025
    assert 0.05 <= delays[0] <= 0.1
    assert all(2.5 <= d <= 5.0 for d in delays[10:])
    backoff.reset()
    assert backoff.next_delay() <= 0.1


async def test_redis_consumer_outlives_more_than_1024_consecutive_failures(
    rclient: aioredis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    # ~1 h of Redis outage at the production 5 s cap; compressed with a tiny backoff.
    import deal_radar.engine.bus as bus_module

    monkeypatch.setattr(bus_module, "_CONSUME_BACKOFF_BASE", 0.00001)
    monkeypatch.setattr(bus_module, "_CONSUME_BACKOFF_CAP", 0.00002)
    producer = RedisStreamBus(rclient, make_config(), "collector")
    await producer.publish(make_listing(1))
    flaky = FlakyRedis(rclient, {"xreadgroup": 1100})
    bus = RedisStreamBus(flaky, make_config(), "gcp-1")  # type: ignore[arg-type]
    got = await collect(bus, 1, timeout=20.0)
    assert got[0].raw.source_id == make_listing(1).source_id
    assert flaky.calls["xreadgroup"] > 1100
    await bus.close()


async def test_redis_consumer_survives_transient_errors(rclient: aioredis.Redis) -> None:
    cfg = make_config()
    producer = RedisStreamBus(rclient, cfg, "collector")
    await producer.start()
    await producer.publish(make_listing(1))
    flaky = FlakyRedis(rclient, {"xreadgroup": 3})
    metrics = Metrics()
    bus = RedisStreamBus(flaky, cfg, "gcp-1", metrics=metrics)  # type: ignore[arg-type]
    got = await collect(bus, 1, timeout=5.0)
    assert got[0].raw.source_id == make_listing(1).source_id
    assert flaky.calls["xreadgroup"] >= 4
    assert metric(metrics, "counter", "bus_errors_total", op="consume") == 3
    await bus.close()


@pytest.fixture
def fast_publish_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    import deal_radar.engine.bus as bus_module

    monkeypatch.setattr(bus_module, "_PUBLISH_BACKOFF_BASE", 0.005)
    monkeypatch.setattr(bus_module, "_PUBLISH_BACKOFF_CAP", 0.02)


async def test_redis_publish_waits_out_a_long_outage(rclient: aioredis.Redis, fast_publish_backoff: None) -> None:
    # The source loop (BaseIngestor) does not guard emit(): a publish that raised after a
    # few seconds of outage would kill the collector's sources for good. It must wait.
    cfg = make_config()
    flaky = FlakyRedis(rclient, {"xadd": 25})
    metrics = Metrics()
    bus = RedisStreamBus(flaky, cfg, "collector", metrics=metrics)  # type: ignore[arg-type]
    await bus.publish(make_listing(1))
    assert flaky.calls["xadd"] == 26
    assert await rclient.xlen(bus.stream) == 1
    assert metric(metrics, "counter", "bus_published_total") == 1
    assert metric(metrics, "counter", "bus_errors_total", op="publish") == 25


async def test_redis_publish_waits_out_oom(rclient: aioredis.Redis, fast_publish_backoff: None) -> None:
    oom = FlakyRedis(rclient, {"xadd": 3}, exc=redis_exc.OutOfMemoryError("command not allowed when used memory > 'maxmemory'"))
    bus = RedisStreamBus(oom, make_config(), "collector")  # type: ignore[arg-type]
    await bus.publish(make_listing(1))
    assert oom.calls["xadd"] == 4


async def test_redis_publish_blocked_by_outage_is_released_by_close(rclient: aioredis.Redis) -> None:
    dead = FlakyRedis(rclient, {"xadd": -1})  # fails forever
    metrics = Metrics()
    bus = RedisStreamBus(dead, make_config(), "collector", metrics=metrics)  # type: ignore[arg-type]
    blocked = asyncio.create_task(bus.publish(make_listing(2)))
    await asyncio.sleep(0.3)
    assert not blocked.done(), "publish must keep waiting while Redis is unreachable"
    started = time.monotonic()
    await bus.close()
    with pytest.raises(BusClosedError):
        await asyncio.wait_for(blocked, 1.0)
    assert time.monotonic() - started < 0.2  # the backoff sleep is interrupted by close()
    assert metric(metrics, "counter", "bus_published_total") == 0
    assert metric(metrics, "counter", "bus_errors_total", op="publish") >= 1


async def test_redis_publish_blocked_by_outage_can_be_cancelled(rclient: aioredis.Redis) -> None:
    dead = FlakyRedis(rclient, {"xadd": -1})
    bus = RedisStreamBus(dead, make_config(), "collector")  # type: ignore[arg-type]
    blocked = asyncio.create_task(bus.publish(make_listing(2)))
    await asyncio.sleep(0.1)
    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked


async def test_redis_publish_non_transient_error_raises_bus_error(rclient: aioredis.Redis) -> None:
    noperm = FlakyRedis(rclient, {"xadd": -1}, exc=redis_exc.NoPermissionError("NOPERM no permissions to run xadd"))
    metrics = Metrics()
    bus = RedisStreamBus(noperm, make_config(), "collector", metrics=metrics)  # type: ignore[arg-type]
    with pytest.raises(BusError):
        await bus.publish(make_listing(3))
    assert noperm.calls["xadd"] == 1  # non-transient: no retries
    assert metric(metrics, "counter", "bus_errors_total", op="publish") == 1

    wrongtype = RedisStreamBus(rclient, make_config(), "collector")
    await rclient.set(wrongtype.stream, "not a stream")  # real WRONGTYPE from the server
    with pytest.raises(BusError):
        await wrongtype.publish(make_listing(4))


async def test_redis_publish_survives_real_server_restart() -> None:
    """Kill redis-server under a running collector + processor, restart it empty."""
    binary = _redis_binary()
    if not binary:
        pytest.skip("redis-server binary not available")
    port = _free_port()
    proc = _spawn_redis(binary, port)
    if proc is None:
        pytest.skip("could not start redis-server")
    client = aioredis.Redis(host="127.0.0.1", port=port, socket_timeout=2.0, socket_connect_timeout=0.5)
    cfg = make_config(block_ms=100)
    bus = RedisStreamBus(client, cfg, "gcp-1")  # one object publishes and consumes, as in main.py
    received: list[str] = []
    task: asyncio.Task[None] | None = None
    try:
        await bus.start()

        async def run() -> None:
            async for msg in bus.consume():
                received.append(msg.raw.source_id)
                await bus.ack(msg)

        task = asyncio.create_task(run())
        await bus.publish(make_listing(1))
        deadline = time.monotonic() + 3.0
        while not received and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert received == [make_listing(1).source_id]
        _stop_redis(proc)  # no persistence: the restarted server is empty (no stream, no group)
        blocked = asyncio.create_task(bus.publish(make_listing(2)))
        await asyncio.sleep(0.5)
        assert not blocked.done(), "publish must wait for Redis to come back"
        proc = _spawn_redis(binary, port)
        if proc is None:
            pytest.skip("could not restart redis-server on the same port")
        await asyncio.wait_for(blocked, 10.0)
        deadline = time.monotonic() + 10.0
        while len(received) < 2 and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        # The restarted server has no group: the consumer recreates it from "0", so the
        # entry published right after the restart is not skipped.
        assert received == [make_listing(1).source_id, make_listing(2).source_id]
    finally:
        await bus.close()
        if task is not None:
            await asyncio.wait_for(task, 5.0)
        await client.aclose()
        if proc is not None:
            _stop_redis(proc)


async def test_redis_ack_failure_is_swallowed_and_counted(rclient: aioredis.Redis) -> None:
    cfg = make_config()
    flaky = FlakyRedis(rclient, {"xack": 1})
    metrics = Metrics()
    bus = RedisStreamBus(flaky, cfg, "gcp-1", metrics=metrics)  # type: ignore[arg-type]
    await bus.publish(make_listing(1))
    got = await collect(bus, 1)  # collect() acks -> the first XACK fails silently
    assert metric(metrics, "counter", "bus_errors_total", op="ack") == 1
    assert await pending_count(rclient, bus) == 1  # stays pending -> redelivered later
    await bus.ack(got[0])
    assert await pending_count(rclient, bus) == 0
    await bus.ack(BusMessage(raw=make_listing(2), id=None))  # no id: no-op


async def test_redis_start_failure_raises_bus_error(rclient: aioredis.Redis, monkeypatch: pytest.MonkeyPatch) -> None:
    import deal_radar.engine.bus as bus_module
    from deal_radar.core.backoff import BackoffPolicy

    monkeypatch.setattr(bus_module, "_START_POLICY", BackoffPolicy(max_attempts=2, base_delay=0.0, max_total_seconds=1.0))
    dead = FlakyRedis(rclient, {"xgroup_create": -1})
    bus = RedisStreamBus(dead, make_config(), "gcp-1")  # type: ignore[arg-type]
    with pytest.raises(BusError):
        await bus.start()
    assert dead.calls["xgroup_create"] == 2


async def test_redis_start_rejects_non_stream_key(rclient: aioredis.Redis) -> None:
    cfg = make_config()
    bus = RedisStreamBus(rclient, cfg, "gcp-1")
    await rclient.set(bus.stream, "not a stream")
    with pytest.raises(BusError):
        await bus.start()


async def test_redis_decode_responses_client(redis_port: int) -> None:
    client = aioredis.Redis(host="127.0.0.1", port=redis_port, decode_responses=True)
    try:
        await client.flushdb()
        cfg = make_config()
        bus = RedisStreamBus(client, cfg, "gcp-1")
        await bus.start()
        original = make_listing(3)
        await bus.publish(original)
        got = await collect(bus, 1)
        assert got[0].raw == original
        assert await bus.refresh_backlog() == 0
        await bus.close()
    finally:
        await client.aclose()


# --------------------------------------------------------------------------- fakeredis


@pytest.fixture
async def fake_client() -> AsyncIterator[Any]:
    fakeredis = pytest.importorskip("fakeredis")
    client = fakeredis.FakeAsyncRedis()
    try:
        await client.xautoclaim("probe", "g", "c", 0)
    except redis_exc.ResponseError as exc:
        if "unknown command" in str(exc).lower():
            pytest.skip("fakeredis lacks XAUTOCLAIM")
    try:
        yield client
    finally:
        await client.aclose()


async def test_fakeredis_round_trip_and_poison(fake_client: Any) -> None:
    cfg = make_config()
    metrics = Metrics()
    bus = RedisStreamBus(fake_client, cfg, "gcp-1", metrics=metrics)
    await bus.start()
    await fake_client.xadd(bus.stream, {PAYLOAD_FIELD: b"[]"})
    original = make_listing(4, posted_at=datetime.now(timezone.utc) - timedelta(minutes=3))
    await bus.publish(original)
    got = await collect(bus, 1)
    assert got[0].raw == original
    assert metric(metrics, "counter", "bus_poison_total") == 1
    assert await pending_count(fake_client, bus) == 0
    await bus.close()


async def test_fakeredis_autoclaim_recovery(fake_client: Any) -> None:
    cfg = make_config(claim_idle_ms=200)
    producer = RedisStreamBus(fake_client, cfg, "collector")
    await producer.start()
    await producer.publish(make_listing(1))
    crashed = RedisStreamBus(fake_client, cfg, "proc-a")
    held = await collect(crashed, 1, ack=False)
    await crashed.close()
    survivor = RedisStreamBus(fake_client, cfg, "proc-b")
    got = await collect(survivor, 1, timeout=5.0)
    assert got[0].id == held[0].id
    assert await pending_count(fake_client, producer) == 0


async def test_fakeredis_does_not_busy_spin_when_block_is_ignored(fake_client: Any) -> None:
    cfg = make_config(block_ms=100)
    bus = RedisStreamBus(fake_client, cfg, "gcp-1")
    await bus.start()
    flaky = FlakyRedis(fake_client, {"xreadgroup": 0})  # counts calls, never fails
    bus.redis = flaky  # type: ignore[assignment]

    async def run() -> None:
        async for _ in bus.consume():
            pass

    task = asyncio.create_task(run())
    await asyncio.sleep(0.5)
    await bus.close()
    await asyncio.wait_for(task, 1.0)
    # ~5 reads in 0.5 s with BLOCK 100 (plus the own-PEL replay), not thousands.
    assert flaky.calls["xreadgroup"] <= 12
