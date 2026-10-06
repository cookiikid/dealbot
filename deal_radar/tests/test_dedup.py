"""Tests for engine/dedup.py.

One behavioural suite runs against three backends so the in-memory fallback can never
drift from the distributed implementation:

* ``memory``    — :class:`MemoryDeduplicator` with an injected fake clock;
* ``fakeredis`` — :class:`RedisDeduplicator` on ``fakeredis`` (Lua via lupa);
* ``redis``     — :class:`RedisDeduplicator` on a real ``redis-server`` spawned on a free
  local port in a temp dir (skipped when the binary is missing).

TTL tests advance the fake clock for the memory backend and sleep (sub-second TTLs via a
config override) for the Redis backends.
"""

from __future__ import annotations

import asyncio
import json
import math
import shutil
import socket
import statistics
import subprocess
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import fakeredis
import pytest
import redis.asyncio as aioredis
from redis.crc import key_slot
from redis.exceptions import ConnectionError as RedisConnectionError

from deal_radar.config_schema import AppConfig, load_config
from deal_radar.core.metrics import Metrics
from deal_radar.engine.dedup import (
    Deduplicator,
    MemoryDeduplicator,
    RedisDeduplicator,
    build_deduplicator,
    cluster_identity,
    price_bucket,
)
from deal_radar.engine.types import DealItem, DedupDecision, DedupStatus, SourceKind

CONFIG_PATH = Path(__file__).parents[1] / "config.yaml"
REDIS_SERVER = Path("/usr/bin/redis-server")

PRODUCT = "gpu_rtx_5090:founders"
BESTBUY_URL = "https://www.bestbuy.com/site/nvidia-geforce-rtx-5090-32gb-gddr7-graphics-card-dark-gun-metal/6614151.p?skuId=6614151"
NEWEGG_URL = "https://www.newegg.com/p/N82E16814137911"


# --------------------------------------------------------------------------- config / items


@pytest.fixture(scope="module")
def base_config() -> AppConfig:
    return load_config(CONFIG_PATH, env={})


def with_dedup(config: AppConfig, **overrides: object) -> AppConfig:
    return config.model_copy(update={"dedup": config.dedup.model_copy(update=overrides)})


def with_prefix(config: AppConfig, prefix: str) -> AppConfig:
    return config.model_copy(update={"storage": config.storage.model_copy(update={"redis_key_prefix": prefix})})


def _item(
    source: str,
    source_id: str,
    price: float,
    *,
    kind: SourceKind,
    url: str,
    retailer: str | None = None,
    outbound_url: str | None = None,
    shipping: float | None = None,
) -> DealItem:
    return DealItem(
        source=source,
        source_kind=kind,
        source_id=source_id,
        url=url,
        title="NVIDIA GeForce RTX 5090 Founders Edition 32GB GDDR7",
        price=price,
        shipping=shipping,
        total_price=round(price + (shipping or 0.0), 2),
        retailer=retailer,
        outbound_url=outbound_url,
    )


def slickdeals(price: float, source_id: str = "18912345", retailer: str | None = "Best Buy", outbound: str = BESTBUY_URL) -> DealItem:
    return _item(
        "slickdeals", source_id, price, kind=SourceKind.AGGREGATOR,
        url=f"https://slickdeals.net/f/{source_id}-nvidia-geforce-rtx-5090-founders-edition",
        retailer=retailer, outbound_url=outbound,
    )


def reddit(price: float, source_id: str = "t3_1g2h3j", retailer: str | None = None, outbound: str = BESTBUY_URL) -> DealItem:
    return _item(
        "reddit", source_id, price, kind=SourceKind.AGGREGATOR,
        url=f"https://www.reddit.com/r/buildapcsales/comments/{source_id[3:]}/gpu_nvidia_rtx_5090_founders_edition/",
        retailer=retailer, outbound_url=outbound,
    )


def retail(price: float, source_id: str = "bestbuy:6614151", retailer: str = "Best Buy", url: str = BESTBUY_URL) -> DealItem:
    return _item("retail", source_id, price, kind=SourceKind.RETAIL, url=url, retailer=retailer)


def fb(price: float, source_id: str) -> DealItem:
    return _item(
        "fb_marketplace", source_id, price, kind=SourceKind.LOCAL,
        url=f"https://www.facebook.com/marketplace/item/{source_id}/",
    )


def ebay(price: float, source_id: str) -> DealItem:
    return _item(
        "ebay", source_id, price, kind=SourceKind.MARKETPLACE, url=f"https://www.ebay.com/itm/{source_id}",
    )


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
    if REDIS_SERVER.exists():
        return str(REDIS_SERVER)
    return shutil.which("redis-server")


def _stop_redis(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def _spawn_redis(binary: str, port: int, workdir: Path) -> subprocess.Popen[bytes] | None:
    """Throw-away, persistence-free redis-server in ``workdir``; None if it does not come up."""
    proc = subprocess.Popen(
        [
            binary, "--port", str(port), "--bind", "127.0.0.1", "--dir", str(workdir), "--save", "",
            "--appendonly", "no", "--protected-mode", "no", "--daemonize", "no", "--loglevel", "warning",
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


@pytest.fixture(scope="module")
def redis_port(tmp_path_factory: pytest.TempPathFactory) -> Iterator[int]:
    binary = _redis_binary()
    if binary is None:
        pytest.skip("redis-server binary not available")
    workdir = tmp_path_factory.mktemp("dedup-redis")
    proc: subprocess.Popen[bytes] | None = None
    port = 0
    for _ in range(3):  # the free port may be grabbed between probing and binding
        port = _free_port()
        proc = _spawn_redis(binary, port, workdir)
        if proc is not None:
            break
    if proc is None:
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


# --------------------------------------------------------------------------- backend harness


class FakeClock:
    def __init__(self, start: float = 1_780_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@dataclass
class Backend:
    name: str
    make: Callable[[AppConfig], Deduplicator]
    advance: Callable[[float], Awaitable[None]]  # let `seconds` of TTL time pass
    redis: aioredis.Redis | None = None


async def _redis_backend(request: pytest.FixtureRequest, name: str) -> AsyncIterator[Backend]:
    if name == "fakeredis":
        client = fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer())
    else:
        port = request.getfixturevalue("redis_port")  # skips only the real-server variant
        client = aioredis.Redis(host="127.0.0.1", port=port, socket_timeout=5.0)
        await client.flushdb()
    try:
        yield Backend(name, lambda cfg: RedisDeduplicator(client, cfg), asyncio.sleep, client)
    finally:
        await client.aclose()


@pytest.fixture(params=["memory", "fakeredis", "redis"])
async def backend(request: pytest.FixtureRequest) -> AsyncIterator[Backend]:
    """The behavioural suite's subject: every test using it runs on all three backends."""
    if request.param == "memory":
        clock = FakeClock()

        async def advance_clock(seconds: float) -> None:
            clock.advance(seconds)

        yield Backend("memory", lambda cfg: MemoryDeduplicator(cfg, clock=clock), advance_clock)
        return
    async for harness in _redis_backend(request, request.param):
        yield harness


@pytest.fixture(params=["fakeredis", "redis"])
async def redis_backend(request: pytest.FixtureRequest) -> AsyncIterator[Backend]:
    """Redis-only checks (key TTLs, script cache, multiple instances on one store)."""
    async for harness in _redis_backend(request, request.param):
        yield harness


async def statuses(dedup: Deduplicator, *claims: tuple[DealItem, str]) -> list[DedupStatus]:
    return [(await dedup.claim(item, key)).status for item, key in claims]


# --------------------------------------------------------------------------- pure helpers


def test_price_bucket_is_geometric_and_safe() -> None:
    assert price_bucket(0.0, 0.02) == 0
    assert price_bucket(-5.0, 0.02) == 0
    assert price_bucket(float("nan"), 0.02) == 0
    assert price_bucket(1.0, 0.02) == 0
    assert price_bucket(1000.0, 0.02) == math.floor(math.log(1000.0) / math.log(1.02))
    assert price_bucket(1.02 ** 100 * 1.0001, 0.02) == 100
    prices = [1.0, 9.99, 99.99, 549.0, 999.99, 1999.0, 25_000.0]
    buckets = [price_bucket(p, 0.02) for p in prices]
    assert buckets == sorted(buckets) and len(set(buckets)) == len(buckets)
    # a 1 % move crosses at most one boundary of a 2 % grid
    for p in (123.45, 999.99, 1499.0, 1999.0):
        assert price_bucket(p * 1.01, 0.02) - price_bucket(p, 0.02) in (0, 1)


@pytest.mark.parametrize(
    ("retailer", "url", "expected"),
    [
        ("Best Buy", "https://slickdeals.net/f/1", "bestbuy"),
        ("bestbuy.com", "https://slickdeals.net/f/1", "bestbuy"),
        ("www.BestBuy.com", "https://slickdeals.net/f/1", "bestbuy"),
        ("BestBuy", "https://slickdeals.net/f/1", "bestbuy"),
        (None, BESTBUY_URL, "bestbuy"),
        (None, "https://bestbuy.ca/en-ca/product/123", "bestbuy"),
        ("B&H", "https://www.reddit.com/r/buildapcsales/", "bhphotovideo"),
        ("B&H Photo", "https://slickdeals.net/f/1", "bhphotovideo"),
        (None, "https://www.bhphotovideo.com/c/product/1873146-REG/", "bhphotovideo"),
        ("Amazon.com", "https://slickdeals.net/f/1", "amazon"),
        (None, "https://www.amazon.co.uk/dp/B0DT7L98J1", "amazon"),
        (None, "https://amzn.to/4abcdEF", "amazon"),
        ("Micro Center", "https://slickdeals.net/f/1", "microcenter"),
        ("Sam's Club", "https://slickdeals.net/f/1", "samsclub"),
        ("The Home Depot", "https://slickdeals.net/f/1", "homedepot"),
        ("Newegg", NEWEGG_URL, "newegg"),
        # deal communities / marketplaces are not stores
        ("eBay", "https://www.ebay.com/itm/1", None),
        (None, "https://www.ebay.com/itm/1", None),
        (None, "https://slickdeals.net/f/18912345-some-deal", None),
        (None, "https://www.reddit.com/r/buildapcsales/comments/abc/x/", None),
        ("!!!", BESTBUY_URL, "bestbuy"),  # unusable name falls back to the link
    ],
)
def test_cluster_identity_normalises_store(retailer: str | None, url: str, expected: str | None) -> None:
    item = _item("slickdeals", "1", 100.0, kind=SourceKind.AGGREGATOR, url=url, retailer=retailer)
    assert cluster_identity(item) == expected


def test_cluster_identity_sources_agree_on_the_same_store() -> None:
    ids = {
        cluster_identity(slickdeals(999.99)),
        cluster_identity(reddit(999.99)),  # no retailer, only the outbound link
        cluster_identity(reddit(999.99, retailer="Best Buy")),
        cluster_identity(retail(999.99)),
        cluster_identity(retail(999.99, retailer="bestbuy.com")),
    }
    assert ids == {"bestbuy"}


def test_cluster_identity_never_for_local_or_marketplace() -> None:
    assert cluster_identity(fb(800.0, "1")) is None
    assert cluster_identity(ebay(800.0, "1")) is None
    local_with_store = _item(
        "craigslist", "7788", 800.0, kind=SourceKind.LOCAL, url="https://austin.craigslist.org/sop/d/x/7788.html",
        retailer="Best Buy", outbound_url=BESTBUY_URL,
    )
    assert cluster_identity(local_with_store) is None


def test_build_deduplicator_picks_backend(base_config: AppConfig) -> None:
    assert isinstance(build_deduplicator(base_config), MemoryDeduplicator)
    client = fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer())
    assert isinstance(build_deduplicator(base_config, client), RedisDeduplicator)


def test_config_ttls_are_converted_to_milliseconds(base_config: AppConfig) -> None:
    dedup = MemoryDeduplicator(base_config)
    assert dedup.listing_ttl_ms == int(base_config.dedup.listing_ttl_hours * 3_600_000)
    assert dedup.cluster_ttl_ms == int(base_config.dedup.cluster_ttl_hours * 3_600_000)
    tiny = MemoryDeduplicator(with_dedup(base_config, listing_ttl_hours=1e-9, cluster_ttl_hours=0.25 / 3600))
    assert tiny.listing_ttl_ms == 1  # never 0: PX 0 is an error in Redis
    assert tiny.cluster_ttl_ms == 250


# --------------------------------------------------------------------------- listing state machine


async def test_listing_state_machine(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)  # min_drop_pct 5 %, min_drop_abs $10
    first = await dedup.claim(slickdeals(1234.56), PRODUCT)
    assert first.status is DedupStatus.NEW and first.should_alert
    assert first.previous_price is None

    same = await dedup.claim(slickdeals(1234.56), PRODUCT)
    assert same.status is DedupStatus.DUPLICATE and not same.should_alert
    assert same.previous_price == 1234.56  # bit-exact: prices travel as strings through Lua

    up = await dedup.claim(slickdeals(1299.99), PRODUCT)
    assert up.status is DedupStatus.DUPLICATE and up.previous_price == 1234.56

    small = await dedup.claim(slickdeals(round(1234.56 * 0.97, 2)), PRODUCT)  # -3 %
    assert small.status is DedupStatus.DUPLICATE and small.previous_price == 1234.56

    drop = await dedup.claim(slickdeals(1111.1), PRODUCT)  # -10 %, -$123.46
    assert drop.status is DedupStatus.PRICE_DROP and drop.should_alert
    assert drop.previous_price == 1234.56

    after = await dedup.claim(slickdeals(1111.1), PRODUCT)
    assert after.status is DedupStatus.DUPLICATE and after.previous_price == 1111.1

    deeper = await dedup.claim(slickdeals(999.99), PRODUCT)
    assert deeper.status is DedupStatus.PRICE_DROP and deeper.previous_price == 1111.1


async def test_price_drop_needs_both_relative_and_absolute_threshold(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    assert (await dedup.claim(retail(100.0, "shop:cable"), "cable")).status is DedupStatus.NEW
    pct_only = await dedup.claim(retail(92.0, "shop:cable"), "cable")  # -8 % but only -$8
    assert pct_only.status is DedupStatus.DUPLICATE and pct_only.previous_price == 100.0
    both = await dedup.claim(retail(89.99, "shop:cable"), "cable")
    assert both.status is DedupStatus.PRICE_DROP and both.previous_price == 100.0

    assert (await dedup.claim(retail(4000.0, "shop:tv"), "tv")).status is DedupStatus.NEW
    abs_only = await dedup.claim(retail(3850.0, "shop:tv"), "tv")  # -$150 but only -3.75 %
    assert abs_only.status is DedupStatus.DUPLICATE


async def test_exact_thresholds_count_as_a_drop(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    await dedup.claim(retail(200.0, "shop:ssd"), "ssd")
    exact = await dedup.claim(retail(190.0, "shop:ssd"), "ssd")  # exactly -5 % and exactly -$10
    assert exact.status is DedupStatus.PRICE_DROP and exact.previous_price == 200.0


async def test_price_increase_keeps_last_alerted_price(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    await dedup.claim(retail(1000.0), PRODUCT)
    assert (await dedup.claim(retail(1200.0), PRODUCT)).previous_price == 1000.0
    rebound = await dedup.claim(retail(1050.0), PRODUCT)  # far below 1200 but above what was alerted
    assert rebound.status is DedupStatus.DUPLICATE and rebound.previous_price == 1000.0
    drop = await dedup.claim(retail(940.0), PRODUCT)
    assert drop.status is DedupStatus.PRICE_DROP and drop.previous_price == 1000.0


async def test_total_price_including_shipping_is_claimed(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    item = _item(
        "retail", "shopify:9", 950.0, kind=SourceKind.RETAIL, url="https://store.example.com/p/9",
        retailer="Example", shipping=49.99,
    )
    await dedup.claim(item, PRODUCT)
    again = await dedup.claim(item, PRODUCT)
    assert again.previous_price == 999.99


async def test_custom_prefix_and_hash_tag(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(with_prefix(base_config, "radar:test:"))
    decision = await dedup.claim(slickdeals(999.99), PRODUCT)
    bucket = price_bucket(999.99, base_config.dedup.price_bucket_pct)
    assert decision.keys == [
        "radar:test:{alerts}:l:slickdeals:18912345",
        f"radar:test:{{alerts}}:c:{PRODUCT}:bestbuy:{bucket}",
    ]
    assert len({key_slot(k.encode()) for k in decision.keys}) == 1  # one Redis Cluster slot
    if backend.redis is not None:
        assert await backend.redis.exists(*decision.keys) == 2
        assert await backend.redis.hget(decision.keys[0], "p") == b"999.99"


async def test_redis_ttls_follow_config(redis_backend: Backend, base_config: AppConfig) -> None:
    backend = redis_backend
    assert backend.redis is not None
    dedup = backend.make(base_config)
    decision = await dedup.claim(slickdeals(999.99), PRODUCT)
    listing_ttl = await backend.redis.pttl(decision.keys[0])
    cluster_ttl = await backend.redis.pttl(decision.keys[1])
    assert base_config.dedup.listing_ttl_hours * 3_600_000 - 5_000 < listing_ttl <= base_config.dedup.listing_ttl_hours * 3_600_000
    assert base_config.dedup.cluster_ttl_hours * 3_600_000 - 5_000 < cluster_ttl <= base_config.dedup.cluster_ttl_hours * 3_600_000


# --------------------------------------------------------------------------- cross-source clusters


async def test_same_deal_from_another_source_is_cross_source_duplicate(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    first = await dedup.claim(slickdeals(999.99), PRODUCT)
    assert first.status is DedupStatus.NEW

    via_reddit = await dedup.claim(reddit(999.99), PRODUCT)
    assert via_reddit.status is DedupStatus.CROSS_SOURCE_DUPLICATE and not via_reddit.should_alert
    assert via_reddit.previous_price == 999.99
    assert via_reddit.keys == [first.keys[1]]  # the cluster key that matched
    assert via_reddit.rollback == {}

    via_retailer = await dedup.claim(retail(999.99), PRODUCT)
    assert via_retailer.status is DedupStatus.CROSS_SOURCE_DUPLICATE

    # the suppressed listing was not recorded: it stays a cross-source duplicate
    again = await dedup.claim(reddit(999.99), PRODUCT)
    assert again.status is DedupStatus.CROSS_SOURCE_DUPLICATE
    if backend.redis is not None:
        assert await backend.redis.exists(dedup.listing_redis_key("reddit:t3_1g2h3j")) == 0

    # the original listing itself is a plain duplicate
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.DUPLICATE


async def test_different_retailer_or_product_is_new(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.NEW
    newegg = reddit(999.99, source_id="t3_newegg", outbound=NEWEGG_URL)
    assert (await dedup.claim(newegg, PRODUCT)).status is DedupStatus.NEW
    other_product = reddit(999.99, source_id="t3_other")
    assert (await dedup.claim(other_product, "gpu_rtx_5080:founders")).status is DedupStatus.NEW


async def test_neighbour_bucket_is_still_a_duplicate(backend: Backend, base_config: AppConfig) -> None:
    pct = base_config.dedup.price_bucket_pct
    boundary = (1 + pct) ** (price_bucket(1000.0, pct) + 1)
    low, high = round(boundary * 0.995, 2), round(boundary * 1.005, 2)  # ~1 % apart, straddling a boundary
    assert price_bucket(high, pct) == price_bucket(low, pct) + 1
    dedup = backend.make(base_config)

    assert (await dedup.claim(slickdeals(low), PRODUCT)).status is DedupStatus.NEW
    upper = await dedup.claim(reddit(high), PRODUCT)  # bucket + 1
    assert upper.status is DedupStatus.CROSS_SOURCE_DUPLICATE and upper.previous_price == low

    assert (await dedup.claim(slickdeals(high, source_id="2"), "p2")).status is DedupStatus.NEW
    lower = await dedup.claim(reddit(low, source_id="t3_2"), "p2")  # bucket - 1
    assert lower.status is DedupStatus.CROSS_SOURCE_DUPLICATE and lower.previous_price == high

    far = round(low * 0.95, 2)  # >= 2 buckets away: a genuinely different price
    assert price_bucket(far, pct) <= price_bucket(low, pct) - 2
    assert (await dedup.claim(retail(far), PRODUCT)).status is DedupStatus.NEW


async def test_cross_source_disabled(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(with_dedup(base_config, cross_source=False))
    first = await dedup.claim(slickdeals(999.99), PRODUCT)
    assert first.status is DedupStatus.NEW
    assert first.keys == [dedup.listing_redis_key("slickdeals:18912345")]
    assert (await dedup.claim(reddit(999.99), PRODUCT)).status is DedupStatus.NEW
    assert (await dedup.claim(retail(999.99), PRODUCT)).status is DedupStatus.NEW


async def test_local_and_marketplace_items_never_cluster(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    assert await statuses(dedup, (fb(850.0, "111"), PRODUCT), (fb(850.0, "222"), PRODUCT)) == [DedupStatus.NEW] * 2
    assert await statuses(dedup, (ebay(850.0, "333"), PRODUCT), (ebay(850.0, "444"), PRODUCT)) == [DedupStatus.NEW] * 2
    at_ebay = [slickdeals(850.0, source_id=s, retailer="eBay", outbound="https://www.ebay.com/itm/9") for s in ("5", "6")]
    assert await statuses(dedup, *((i, PRODUCT) for i in at_ebay)) == [DedupStatus.NEW] * 2
    decision = await dedup.claim(fb(850.0, "777"), PRODUCT)
    assert decision.keys == [dedup.listing_redis_key("fb_marketplace:777")]
    assert (await dedup.claim(fb(850.0, "111"), PRODUCT)).status is DedupStatus.DUPLICATE


# --------------------------------------------------------------------------- rollback


async def test_rollback_of_new_allows_reclaim(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    first = await dedup.claim(slickdeals(999.99), PRODUCT)
    assert first.status is DedupStatus.NEW
    json.dumps(first.model_dump(mode="json"))  # the undo state rides along in Alert payloads
    await dedup.rollback(first)
    if backend.redis is not None:
        assert await backend.redis.exists(*first.keys) == 0

    # the cluster key is gone too: another source may alert now ...
    other = await dedup.claim(reddit(999.99), PRODUCT)
    assert other.status is DedupStatus.NEW
    # ... and the rolled-back listing is then a cross-source duplicate of it
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.CROSS_SOURCE_DUPLICATE

    await dedup.rollback(other)
    again = await dedup.claim(slickdeals(999.99), PRODUCT)
    assert again.status is DedupStatus.NEW


async def test_rollback_after_newer_claim_keeps_the_newer_one(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    new = await dedup.claim(slickdeals(1000.0), PRODUCT)
    drop = await dedup.claim(slickdeals(850.0), PRODUCT)
    assert drop.status is DedupStatus.PRICE_DROP
    await dedup.rollback(new)  # stale: the PRICE_DROP claim owns the listing now
    kept = await dedup.claim(slickdeals(850.0), PRODUCT)
    assert kept.status is DedupStatus.DUPLICATE and kept.previous_price == 850.0
    assert (await dedup.claim(reddit(850.0), PRODUCT)).status is DedupStatus.CROSS_SOURCE_DUPLICATE

    # NEW (A) -> rollback(A) -> NEW (B) -> rollback(A) again must not undo B
    a = await dedup.claim(retail(500.0, "shop:a"), "monitor")
    await dedup.rollback(a)
    b = await dedup.claim(retail(500.0, "shop:a"), "monitor")
    assert b.status is DedupStatus.NEW
    await dedup.rollback(a)
    assert (await dedup.claim(retail(500.0, "shop:a"), "monitor")).status is DedupStatus.DUPLICATE
    assert (await dedup.claim(reddit(500.0, source_id="t3_mon"), "monitor")).status is DedupStatus.CROSS_SOURCE_DUPLICATE


async def test_rollback_of_price_drop_restores_previous_state(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    await dedup.claim(slickdeals(1000.0), PRODUCT)
    drop = await dedup.claim(slickdeals(850.0), PRODUCT)
    assert drop.status is DedupStatus.PRICE_DROP
    await dedup.rollback(drop)
    if backend.redis is not None:
        listing = dedup.listing_redis_key("slickdeals:18912345")
        assert await backend.redis.hget(listing, "p") == b"1000.0"
        assert await backend.redis.pttl(listing) > (base_config.dedup.listing_ttl_hours * 3_600_000) - 5_000

    # the cluster key of the dropped price is gone, the one of the original price is not
    assert (await dedup.claim(retail(1000.0), PRODUCT)).status is DedupStatus.CROSS_SOURCE_DUPLICATE
    assert (await dedup.claim(reddit(850.0), PRODUCT)).status is DedupStatus.NEW
    # the listing is back at its previously alerted price
    redo = await dedup.claim(slickdeals(850.0), PRODUCT)
    assert redo.status is DedupStatus.PRICE_DROP and redo.previous_price == 1000.0


async def test_rollback_of_price_drop_restores_overwritten_cluster_owner(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    await dedup.claim(retail(1000.0), PRODUCT)  # retailer listing alerted at 1000
    assert (await dedup.claim(reddit(850.0), PRODUCT)).status is DedupStatus.NEW  # reddit owns the 850 cluster
    drop = await dedup.claim(retail(850.0), PRODUCT)  # the retailer listing drops: re-alert takes the cluster
    assert drop.status is DedupStatus.PRICE_DROP
    await dedup.rollback(drop)
    third = await dedup.claim(slickdeals(850.0), PRODUCT)
    assert third.status is DedupStatus.CROSS_SOURCE_DUPLICATE  # reddit's ownership was restored
    if backend.redis is not None:
        value = await backend.redis.get(drop.keys[1])
        assert value is not None and value.endswith(b"|850.0|reddit:t3_1g2h3j")


async def test_rollback_is_noop_for_non_alerting_and_repeated_rollbacks(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    new = await dedup.claim(slickdeals(999.99), PRODUCT)
    dup = await dedup.claim(slickdeals(999.99), PRODUCT)
    cross = await dedup.claim(reddit(999.99), PRODUCT)
    await dedup.rollback(dup)
    await dedup.rollback(cross)
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.DUPLICATE
    await dedup.rollback(new)
    await dedup.rollback(new)  # second rollback of the same claim: nothing left to undo
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.NEW


async def test_rollback_of_foreign_claim_is_ignored(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    mine = await dedup.claim(slickdeals(999.99), PRODUCT)
    foreign_backend = "memory" if backend.name != "memory" else "redis"
    forged = DedupDecision(status=DedupStatus.NEW, keys=mine.keys, rollback={**mine.rollback, "backend": foreign_backend})
    await dedup.rollback(forged)
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.DUPLICATE
    await dedup.rollback(DedupDecision(status=DedupStatus.NEW))  # no undo state at all
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.DUPLICATE


# --------------------------------------------------------------------------- TTL expiry


async def test_listing_ttl_expiry_allows_realert(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(with_dedup(base_config, listing_ttl_hours=0.3 / 3600, cluster_ttl_hours=0.3 / 3600))
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.NEW
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.DUPLICATE
    await backend.advance(0.7)
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.NEW


async def test_cluster_ttl_expiry_is_independent_of_listing_ttl(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(with_dedup(base_config, listing_ttl_hours=1.0, cluster_ttl_hours=0.3 / 3600))
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.NEW
    assert (await dedup.claim(reddit(999.99), PRODUCT)).status is DedupStatus.CROSS_SOURCE_DUPLICATE
    await backend.advance(0.7)
    assert (await dedup.claim(reddit(999.99), PRODUCT)).status is DedupStatus.NEW
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.DUPLICATE


async def test_price_drop_refreshes_ttl_but_duplicate_does_not(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(with_dedup(base_config, listing_ttl_hours=0.6 / 3600))
    assert (await dedup.claim(retail(1000.0, "shop:a"), "a")).status is DedupStatus.NEW
    assert (await dedup.claim(retail(1000.0, "shop:b"), "b")).status is DedupStatus.NEW
    await backend.advance(0.35)
    assert (await dedup.claim(retail(850.0, "shop:a"), "a")).status is DedupStatus.PRICE_DROP  # TTL refreshed
    assert (await dedup.claim(retail(1000.0, "shop:b"), "b")).status is DedupStatus.DUPLICATE  # TTL untouched
    await backend.advance(0.35)  # past the first claims' expiry, within the refreshed one
    assert (await dedup.claim(retail(850.0, "shop:a"), "a")).status is DedupStatus.DUPLICATE
    assert (await dedup.claim(retail(1000.0, "shop:b"), "b")).status is DedupStatus.NEW


async def test_own_stale_cluster_entry_is_not_a_cross_source_duplicate(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(with_dedup(base_config, listing_ttl_hours=0.3 / 3600, cluster_ttl_hours=1.0))
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.NEW
    await backend.advance(0.7)  # listing expired, its cluster key has not
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.NEW
    assert (await dedup.claim(reddit(999.99), PRODUCT)).status is DedupStatus.CROSS_SOURCE_DUPLICATE


# --------------------------------------------------------------------------- concurrency


async def test_concurrent_claims_yield_exactly_one_new(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    results = await asyncio.gather(*(dedup.claim(slickdeals(999.99), PRODUCT) for _ in range(50)))
    counts = {status: sum(r.status is status for r in results) for status in DedupStatus}
    assert counts[DedupStatus.NEW] == 1
    assert counts[DedupStatus.DUPLICATE] == 49


async def test_concurrent_cross_source_claims_yield_exactly_one_new(backend: Backend, base_config: AppConfig) -> None:
    dedup = backend.make(base_config)
    items = [reddit(999.99, source_id=f"t3_{i:05d}") for i in range(25)] + [
        slickdeals(round(999.99 * (1 + (i % 5 - 2) * 0.002), 2), source_id=str(1000 + i)) for i in range(25)
    ]
    results = await asyncio.gather(*(dedup.claim(item, PRODUCT) for item in items))
    statuses_ = [r.status for r in results]
    assert statuses_.count(DedupStatus.NEW) == 1
    assert statuses_.count(DedupStatus.CROSS_SOURCE_DUPLICATE) == 49


async def test_concurrent_claims_across_deduplicator_instances(redis_backend: Backend, base_config: AppConfig) -> None:
    backend = redis_backend
    assert backend.redis is not None
    workers = [backend.make(base_config) for _ in range(5)]  # five processor nodes, one Redis
    results = await asyncio.gather(*(workers[i % 5].claim(retail(999.99), PRODUCT) for i in range(50)))
    assert [r.status for r in results].count(DedupStatus.NEW) == 1


# --------------------------------------------------------------------------- memory specifics


async def test_memory_purges_expired_entries(base_config: AppConfig) -> None:
    clock = FakeClock()
    dedup = MemoryDeduplicator(
        with_dedup(base_config, listing_ttl_hours=25 / 3600, cluster_ttl_hours=10 / 3600), clock=clock, purge_interval=30.0,
    )
    for i in range(20):
        await dedup.claim(slickdeals(500.0 + i * 50, source_id=str(i)), f"p{i}")
    assert len(dedup) == 20 and dedup.cluster_count == 20
    clock.advance(26)  # all expired, but the purge interval has not elapsed: entries linger
    await dedup.claim(fb(100.0, "a"), PRODUCT)
    assert len(dedup) == 21 and dedup.cluster_count == 20
    clock.advance(5)  # 31 s since the last purge
    await dedup.claim(fb(100.0, "b"), PRODUCT)
    assert len(dedup) == 2 and dedup.cluster_count == 0  # only the two live fb listings remain
    clock.advance(60)
    assert dedup.purge_expired() == 2 and len(dedup) == 0


async def test_memory_close_clears_state(base_config: AppConfig) -> None:
    dedup = MemoryDeduplicator(base_config)
    await dedup.claim(slickdeals(999.99), PRODUCT)
    await dedup.close()
    assert len(dedup) == 0 and dedup.cluster_count == 0


async def test_metrics_count_decisions_and_rollbacks(base_config: AppConfig) -> None:
    metrics = Metrics()
    dedup = build_deduplicator(base_config, metrics=metrics)
    new = await dedup.claim(slickdeals(999.99), PRODUCT)
    await dedup.claim(slickdeals(999.99), PRODUCT)
    await dedup.claim(reddit(999.99), PRODUCT)
    await dedup.rollback(new)
    await dedup.rollback(new)
    decisions = metrics.counter("dedup_decisions_total", labelnames=("backend", "status"))
    rollbacks = metrics.counter("dedup_rollbacks_total", labelnames=("backend", "result"))
    assert decisions.value(backend="memory", status="new") == 1
    assert decisions.value(backend="memory", status="duplicate") == 1
    assert decisions.value(backend="memory", status="cross_source_duplicate") == 1
    assert rollbacks.value(backend="memory", result="undone") == 1
    assert rollbacks.value(backend="memory", result="superseded") == 1


async def test_non_finite_price_is_rejected(base_config: AppConfig) -> None:
    dedup = MemoryDeduplicator(base_config)
    item = slickdeals(999.99).model_copy(update={"total_price": float("inf")})
    with pytest.raises(ValueError):
        await dedup.claim(item, PRODUCT)


# --------------------------------------------------------------------------- redis specifics


async def test_redis_with_decoded_responses(base_config: AppConfig) -> None:
    client = fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer(), decode_responses=True)
    try:
        dedup = RedisDeduplicator(client, base_config)
        new = await dedup.claim(slickdeals(1234.56), PRODUCT)
        assert new.status is DedupStatus.NEW
        dup = await dedup.claim(slickdeals(1234.56), PRODUCT)
        assert dup.status is DedupStatus.DUPLICATE and dup.previous_price == 1234.56
        drop = await dedup.claim(slickdeals(1099.0), PRODUCT)
        assert drop.status is DedupStatus.PRICE_DROP and drop.previous_price == 1234.56
        await dedup.rollback(drop)
        assert await client.hget(dedup.listing_redis_key("slickdeals:18912345"), "p") == "1234.56"
    finally:
        await client.aclose()


async def test_redis_script_survives_script_flush(redis_backend: Backend, base_config: AppConfig) -> None:
    backend = redis_backend
    assert backend.redis is not None
    dedup = backend.make(base_config)
    new = await dedup.claim(slickdeals(999.99), PRODUCT)
    await backend.redis.script_flush()  # e.g. fail-over to a replica with an empty script cache
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.DUPLICATE
    await backend.redis.script_flush()
    await dedup.rollback(new)
    assert (await dedup.claim(slickdeals(999.99), PRODUCT)).status is DedupStatus.NEW


async def test_redis_errors_propagate_for_pipeline_fallback(base_config: AppConfig) -> None:
    client = aioredis.Redis(host="127.0.0.1", port=_free_port(), socket_connect_timeout=0.5, socket_timeout=0.5)
    try:
        dedup = RedisDeduplicator(client, base_config)
        with pytest.raises(RedisConnectionError):
            await dedup.claim(slickdeals(999.99), PRODUCT)
    finally:
        await client.aclose()


async def test_real_redis_claim_latency(rclient: aioredis.Redis, base_config: AppConfig) -> None:
    dedup = RedisDeduplicator(rclient, base_config)
    for i in range(20):  # warm-up: connection + script load
        await dedup.claim(slickdeals(999.99, source_id=f"warm{i}"), "warm")
    samples: list[float] = []
    for i in range(300):
        item = slickdeals(500.0 + i, source_id=str(i)) if i % 2 else reddit(500.0 + i, source_id=f"t3_{i}")
        for _ in range(2):  # NEW (or CROSS) then DUPLICATE
            started = time.perf_counter()
            await dedup.claim(item, f"lat{i % 7}")
            samples.append((time.perf_counter() - started) * 1000.0)
    p50 = statistics.median(samples)
    p99 = sorted(samples)[int(len(samples) * 0.99) - 1]
    print(f"redis claim latency: p50={p50:.3f} ms p99={p99:.3f} ms n={len(samples)}")
    assert p50 < 5.0, f"claim p50 {p50:.3f} ms"
