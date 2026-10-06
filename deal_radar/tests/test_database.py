"""Tests for deal_radar.db (models, Database, Recorder, connect_redis).

All database tests run against a temporary SQLite file (aiosqlite) — the same code path
production single-node deployments use. PostgreSQL-specific SQL is verified by compiling
the statements with the PostgreSQL dialect. ``connect_redis`` is exercised against a real
``redis-server`` spawned on a random local port (skipped when the binary is missing).
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import redis.exceptions
from sqlalchemy import exc as sa_exc
from sqlalchemy import make_url, select
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.schema import CreateTable

from deal_radar.core.metrics import Metrics
from deal_radar.db.database import (
    ALERTS,
    LISTINGS,
    SNAPSHOTS,
    Database,
    Recorder,
    SnapshotRecord,
    _sqlite_file_path,
    alert_upsert_statement,
    build_snapshot_rows,
    classify_record,
    connect_redis,
    is_transient_error,
    json_safe,
    listing_upsert_statement,
    price_history_query,
    redact_url,
)
from deal_radar.db.models import Listing, UTCDateTime, db_text, ensure_utc
from deal_radar.engine.types import (
    Alert,
    Condition,
    DealItem,
    DedupDecision,
    DedupStatus,
    DispatchReport,
    DispatchResult,
    FilterResult,
    Location,
    RiskSignal,
    ScoreResult,
    SellerInfo,
    Severity,
    SourceKind,
    utcnow,
)

T0 = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- builders


def make_item(
    source_id: str = "315012345678",
    *,
    source: str = "ebay",
    price: float = 1000.0,
    shipping: float | None = None,
    condition: Condition = Condition.NEW,
    title: str | None = None,
    kind: SourceKind = SourceKind.MARKETPLACE,
    seller: str | None = "gpu_flipper_tx",
    location: Location | None = None,
    images: Sequence[str] | None = None,
    retailer: str | None = None,
) -> DealItem:
    return DealItem(
        source=source,
        source_kind=kind,
        source_id=source_id,
        url=f"https://www.ebay.com/itm/{source_id}",
        title=title or f"NVIDIA GeForce RTX 4090 Founders Edition 24GB GDDR6X #{source_id}",
        description="Barely used, original box, receipt available.",
        price=price,
        shipping=shipping,
        total_price=round(price + (shipping or 0.0), 2),
        condition=condition,
        seller=SellerInfo(name=seller, feedback_score=152, feedback_pct=99.6) if seller else None,
        location=location if location is not None else Location(city="Austin", region="TX", postal_code="78701"),
        image_urls=list(images) if images is not None else [f"https://i.ebayimg.com/images/g/{source_id}/s-l1600.jpg"],
        retailer=retailer,
    )


def accepted_fr(profile: str = "gpu_rtx_4090", variant: str | None = "fe", category: str = "gpu") -> FilterResult:
    return FilterResult(
        accepted=True,
        profile_id=profile,
        variant_id=variant,
        category=category,
        match_strength=1.0,
        matched_terms=["rtx 4090", "founders"],
        risk_signals=[RiskSignal(code="shipping_only_risk", probability=0.05, detail="ships only")],
        elapsed_us=41.0,
    )


def rejected_fr(code: str = "box_only", profile: str | None = "gpu_rtx_4090", variant: str | None = "fe") -> FilterResult:
    return FilterResult(
        accepted=False,
        profile_id=profile,
        variant_id=variant,
        category="gpu" if profile else None,
        match_strength=0.9 if profile else 0.0,
        reject_code=code,
        reject_detail="box only",
    )


def make_score(
    score: float = 72.5,
    *,
    risk: float = 0.05,
    rejected: bool = False,
    reason: str | None = None,
    severity: Severity | None = Severity.HIGH,
    market_price: float | None = 1650.0,
    discount_pct: float | None = 0.39,
    is_price_error: bool = False,
) -> ScoreResult:
    return ScoreResult(
        score=score,
        severity=None if rejected else severity,
        rejected=rejected,
        reject_reason=reason,
        market_price=market_price,
        market_basis="blend",
        discount_pct=discount_pct,
        robust_z=3.1,
        iqr_position=0.6,
        history_n=40,
        confidence=0.81,
        risk=risk,
        components={"D": 0.9, "S_stat": 0.7, "T": 1.0, "O": 0.86, "C": 0.81, "R": risk},
        is_price_error=is_price_error,
        explain=["39% below market $1,650"],
    )


def make_alert(item: DealItem, *, created_at: datetime | None = None, update: bool = False) -> Alert:
    return Alert(
        item=item,
        profile_id="gpu_rtx_4090",
        profile_name="RTX 4090",
        variant_id="fe",
        category="gpu",
        severity=Severity.CRITICAL,
        score=make_score(91.0, severity=Severity.CRITICAL, market_price=1818.0, discount_pct=0.45, is_price_error=True),
        dedup=DedupDecision(status=DedupStatus.PRICE_DROP if update else DedupStatus.NEW, previous_price=1100.0 if update else None),
        created_at=created_at or T0,
        pipeline_ms=12.4,
        ingest_lag_ms=4500.0,
        mention=True,
    )


def make_report(alert: Alert, *, ok: bool = True) -> DispatchReport:
    return DispatchReport(
        alert_id=alert.alert_id,
        results=[
            DispatchResult(target="discord:gpu", ok=ok, status=200 if ok else 500, latency_ms=118.0, message_id="1290" if ok else None),
            DispatchResult(target="telegram:main", ok=False, status=429, latency_ms=850.0, attempts=3, error="rate limited"),
        ],
    )


def rec(item: DealItem, fr: FilterResult | None, score: ScoreResult | None, at: datetime) -> SnapshotRecord:
    return SnapshotRecord(item=item, filter_result=fr, score=score, observed_at=at)


# --------------------------------------------------------------------------- fixtures / helpers


# Optional: also run every `db`-based test against a real PostgreSQL, e.g.
# DEAL_RADAR_TEST_PG_URL=postgresql+asyncpg://postgres@127.0.0.1:5432/postgres (tables are truncated!)
PG_TEST_URL = os.environ.get("DEAL_RADAR_TEST_PG_URL", "").strip()


@pytest.fixture(params=["sqlite", "postgresql"] if PG_TEST_URL else ["sqlite"])
async def db(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Database]:
    if request.param == "postgresql":
        database = Database(PG_TEST_URL)
        await database.connect()
        async with database.engine.begin() as conn:
            await conn.exec_driver_sql("TRUNCATE listings, price_snapshots, alerts RESTART IDENTITY CASCADE")
    else:
        database = Database(f"sqlite+aiosqlite:///{tmp_path}/data/nested/dealradar.db")
        await database.connect()
    try:
        yield database
    finally:
        await database.close()


async def fetch_listing(db: Database, item: DealItem) -> dict[str, Any]:
    async with db.engine.connect() as conn:
        row = (await conn.execute(select(LISTINGS).where(LISTINGS.c.id == item.fingerprint))).mappings().one()
    return dict(row)


async def fetch_snapshots(db: Database, item: DealItem | None = None) -> list[dict[str, Any]]:
    stmt = select(SNAPSHOTS).order_by(SNAPSHOTS.c.id)
    if item is not None:
        stmt = stmt.where(SNAPSHOTS.c.listing_id == item.fingerprint)
    async with db.engine.connect() as conn:
        return [dict(r) for r in (await conn.execute(stmt)).mappings()]


async def wait_until(predicate: Callable[[], bool], timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met before timeout")
        await asyncio.sleep(0.005)


# --------------------------------------------------------------------------- connect / lifecycle


async def test_connect_creates_parent_dir_and_applies_sqlite_pragmas(tmp_path: Path) -> None:
    db_path = tmp_path / "a" / "b" / "c.db"
    database = Database(f"sqlite+aiosqlite:///{db_path}")
    assert not database.connected
    await database.connect()
    await database.connect()  # idempotent
    try:
        assert db_path.exists()
        async with database.engine.connect() as conn:
            pragmas = {}
            for name in ("journal_mode", "synchronous", "busy_timeout", "foreign_keys"):
                pragmas[name] = (await conn.exec_driver_sql(f"PRAGMA {name}")).scalar()
        assert str(pragmas["journal_mode"]).lower() == "wal"
        assert pragmas["synchronous"] == 1  # NORMAL
        assert pragmas["busy_timeout"] == 5000
        assert pragmas["foreign_keys"] == 1
        assert await database.counts() == {"listings": 0, "snapshots": 0, "alerts": 0}
        assert await database.ping() is True
    finally:
        await database.close()
        await database.close()  # idempotent
    assert await database.ping() is False


async def test_in_memory_sqlite_works() -> None:
    database = Database("sqlite+aiosqlite:///:memory:")
    await database.connect()
    try:
        await database.write_snapshots([rec(make_item(), accepted_fr(), make_score(), T0)])
        assert (await database.counts())["snapshots"] == 1
    finally:
        await database.close()


def test_rejects_unsupported_database_url() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        Database("mysql+aiomysql://user:pw@localhost/deals")


async def test_methods_require_connect(tmp_path: Path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path}/x.db")
    with pytest.raises(RuntimeError, match="connect"):
        await database.write_snapshots([rec(make_item(), accepted_fr(), make_score(), T0)])
    # Empty batches are a no-op even before connect.
    await database.write_snapshots([])
    await database.write_alerts([])


# --------------------------------------------------------------------------- snapshots / listings


async def test_write_snapshots_accepted_and_rejected(db: Database) -> None:
    good = make_item("1001", price=999.99, shipping=25.0)
    bad = make_item("1002", price=150.0, title="RTX 4090 BOX ONLY no card")
    await db.write_snapshots(
        [
            rec(good, accepted_fr(), make_score(88.0, risk=0.1), T0),
            rec(bad, rejected_fr("box_only"), None, T0),
        ]
    )
    assert await db.counts() == {"listings": 2, "snapshots": 2, "alerts": 0}

    g = await fetch_listing(db, good)
    assert g["listing_key"] == "ebay:1001"
    assert g["source"] == "ebay" and g["source_id"] == "1001"
    assert g["status"] == "accepted"
    assert g["profile_id"] == "gpu_rtx_4090" and g["variant_id"] == "fe"
    assert g["product_key"] == "gpu_rtx_4090:fe" and g["category"] == "gpu"
    assert g["condition"] == "new"
    assert g["seller_name"] == "gpu_flipper_tx"
    assert g["location_text"] == "Austin, TX, 78701"
    assert g["image_url"] == "https://i.ebayimg.com/images/g/1001/s-l1600.jpg"
    assert g["first_price"] == g["last_price"] == g["min_price"] == pytest.approx(1024.99)
    assert g["last_score"] == pytest.approx(88.0)
    assert g["first_seen"] == g["last_seen"] == T0

    b = await fetch_listing(db, bad)
    assert b["status"] == "rejected"
    assert b["last_score"] is None
    assert b["product_key"] == "gpu_rtx_4090:fe"

    snaps = {s["listing_id"]: s for s in await fetch_snapshots(db)}
    sg, sb = snaps[good.fingerprint], snaps[bad.fingerprint]
    assert sg["accepted"] is True and sg["reject_code"] is None
    assert sg["price"] == pytest.approx(999.99) and sg["shipping"] == pytest.approx(25.0)
    assert sg["total_price"] == pytest.approx(1024.99)
    assert sg["condition_class"] == "new" and sg["source"] == "ebay"
    assert sg["score"] == pytest.approx(88.0) and sg["risk"] == pytest.approx(0.1)
    assert sb["accepted"] is False and sb["reject_code"] == "box_only"
    assert sb["score"] is None and sb["risk"] is None
    assert sb["product_key"] == "gpu_rtx_4090:fe"


async def test_scorer_gates_and_unclassified_records(db: Database) -> None:
    scam = make_item("2001", price=300.0)
    pricey = make_item("2002", price=2600.0)
    unknown = make_item("2003", price=500.0, condition=Condition.REFURBISHED, location=Location(text="Round Rock, TX"))
    no_profile = make_item("2004", price=50.0, title="Mystery box of cables and stuff")
    await db.write_snapshots(
        [
            rec(scam, accepted_fr(), make_score(40.0, risk=0.97, rejected=True, reason="scam_risk"), T0),
            rec(pricey, accepted_fr(), make_score(0.0, risk=0.05, rejected=True, reason="above_ceiling"), T0),
            rec(unknown, None, None, T0),
            rec(no_profile, rejected_fr("no_profile_match", profile=None, variant=None), None, T0),
        ]
    )
    snaps = {s["listing_id"]: s for s in await fetch_snapshots(db)}

    assert (await fetch_listing(db, scam))["status"] == "gated"
    assert snaps[scam.fingerprint]["accepted"] is False
    assert snaps[scam.fingerprint]["reject_code"] == "scam_risk"

    # above_ceiling is still a genuine market price -> kept for the price history.
    assert (await fetch_listing(db, pricey))["status"] == "gated"
    assert snaps[pricey.fingerprint]["accepted"] is True
    assert snaps[pricey.fingerprint]["reject_code"] == "above_ceiling"

    u = await fetch_listing(db, unknown)
    assert u["status"] == "unclassified" and u["product_key"] is None
    assert u["location_text"] == "Round Rock, TX"
    assert snaps[unknown.fingerprint]["accepted"] is False
    assert snaps[unknown.fingerprint]["condition_class"] == "refurb"

    n = await fetch_listing(db, no_profile)
    assert n["status"] == "rejected" and n["product_key"] is None
    assert snaps[no_profile.fingerprint]["reject_code"] == "no_profile_match"
    assert snaps[no_profile.fingerprint]["product_key"] is None


def test_classify_record() -> None:
    item = make_item()
    assert classify_record(rec(item, accepted_fr(), make_score(), T0)) == ("accepted", True, None)
    assert classify_record(rec(item, accepted_fr(), None, T0)) == ("accepted", True, None)
    assert classify_record(rec(item, rejected_fr("rental"), None, T0)) == ("rejected", False, "rental")
    assert classify_record(rec(item, None, None, T0)) == ("unclassified", False, None)
    gate = make_score(rejected=True, reason="out_of_stock")
    assert classify_record(rec(item, accepted_fr(), gate, T0)) == ("gated", False, "out_of_stock")


async def test_reobserving_a_listing_updates_last_and_min_price(db: Database) -> None:
    item = make_item("3001", price=1000.0)
    await db.write_snapshots([rec(item, accepted_fr(), make_score(60.0), T0)])
    await db.write_snapshots([rec(make_item("3001", price=900.0), accepted_fr(), make_score(75.0), T0 + timedelta(hours=1))])
    edited = make_item("3001", price=950.0, title="RTX 4090 FE - price adjusted", seller=None, images=[])
    await db.write_snapshots([rec(edited, accepted_fr(), make_score(70.0), T0 + timedelta(hours=2))])

    row = await fetch_listing(db, item)
    assert row["first_price"] == pytest.approx(1000.0)
    assert row["last_price"] == pytest.approx(950.0)
    assert row["min_price"] == pytest.approx(900.0)
    assert row["first_seen"] == T0
    assert row["last_seen"] == T0 + timedelta(hours=2)
    assert row["last_score"] == pytest.approx(70.0)
    assert row["title"] == "RTX 4090 FE - price adjusted"
    # Descriptive fields missing from the newer observation keep the known value.
    assert row["seller_name"] == "gpu_flipper_tx"
    assert row["image_url"] == "https://i.ebayimg.com/images/g/3001/s-l1600.jpg"
    assert len(await fetch_snapshots(db, item)) == 3

    # A later rejection flips the status and clears last_score, keeps the classification.
    await db.write_snapshots(
        [rec(make_item("3001", price=120.0), rejected_fr("no_profile_match", profile=None, variant=None), None, T0 + timedelta(hours=3))]
    )
    row = await fetch_listing(db, item)
    assert row["status"] == "rejected"
    assert row["last_score"] is None
    assert row["last_price"] == pytest.approx(120.0)
    assert row["min_price"] == pytest.approx(120.0)
    assert row["product_key"] == "gpu_rtx_4090:fe"
    assert await db.counts() == {"listings": 1, "snapshots": 4, "alerts": 0}


async def test_same_listing_twice_in_one_batch_and_out_of_order_writes(db: Database) -> None:
    t1, t2 = T0 + timedelta(hours=1), T0 + timedelta(hours=2)
    batch = [
        rec(make_item("4001", price=950.0, title="newest title"), accepted_fr(), make_score(70.0), t2),
        rec(make_item("4001", price=1000.0, title="oldest title"), accepted_fr(), make_score(50.0), T0),
        rec(make_item("4001", price=900.0, title="middle title"), accepted_fr(), make_score(80.0), t1),
    ]
    await db.write_snapshots(batch)
    row = await fetch_listing(db, batch[0].item)
    assert (row["first_seen"], row["first_price"]) == (T0, pytest.approx(1000.0))
    assert (row["last_seen"], row["last_price"]) == (t2, pytest.approx(950.0))
    assert row["min_price"] == pytest.approx(900.0)
    assert row["title"] == "newest title"
    assert row["last_score"] == pytest.approx(70.0)

    # A late (older) observation moves first_* back but never regresses last_*.
    early = T0 - timedelta(days=2)
    await db.write_snapshots([rec(make_item("4001", price=1100.0, title="stale title"), accepted_fr(), make_score(10.0), early)])
    row = await fetch_listing(db, batch[0].item)
    assert (row["first_seen"], row["first_price"]) == (early, pytest.approx(1100.0))
    assert (row["last_seen"], row["last_price"]) == (t2, pytest.approx(950.0))
    assert row["title"] == "newest title" and row["last_score"] == pytest.approx(70.0)
    assert row["min_price"] == pytest.approx(900.0)
    assert await db.counts() == {"listings": 1, "snapshots": 4, "alerts": 0}


async def test_long_values_are_clipped_to_column_widths(db: Database) -> None:
    item = make_item("5001", seller="s" * 400, retailer="r" * 400, location=Location(text="l" * 400))
    await db.write_snapshots([rec(item, rejected_fr("x" * 200), None, T0)])
    row = await fetch_listing(db, item)
    assert len(row["seller_name"]) == 255 and len(row["retailer"]) == 255 and len(row["location_text"]) == 255
    assert len((await fetch_snapshots(db, item))[0]["reject_code"]) == 64


# --------------------------------------------------------------------------- hostile input
# Each of these used to fail the whole (Recorder: 200-record) transaction: PostgreSQL rejects
# NUL in text and \u0000 in jsonb, no driver can encode lone surrogates, orjson rejects
# integers outside 64 bits, SQLite stores NaN as NULL (NOT NULL violation) and alerts.id
# was VARCHAR(64).


def test_db_text_and_json_safe() -> None:
    assert db_text(None) is None
    assert db_text("plain ascii") == "plain ascii"
    assert db_text("café \U0001f600") == "café \U0001f600"  # valid non-ASCII untouched
    assert db_text("a\x00b") == "ab"
    assert db_text("lone \ud83d here") == "lone � here"
    assert db_text("split pair 😀") == "split pair \U0001f600"  # halves re-joined
    assert db_text("abcdef", 3) == "abc"
    assert db_text("a\x00bcdef", 3) == "abc"  # sanitised before clipping
    doc = {"s": "x\x00", "n": [1, 2**70, -(2**70), 1.5, float("nan"), float("inf")], "b": True, "z": None, 3: (b"\xff", {1})}
    safe = json_safe(doc)
    assert safe == {"s": "x", "n": [1, str(2**70), str(-(2**70)), 1.5, None, None], "b": True, "z": None, "3": ["b'\\xff'", [1]]}
    clean = {"a": [1, "b", {"c": 2.5, "d": None, "e": False}]}
    assert json_safe(clean) == clean


async def test_hostile_text_is_sanitised_not_fatal(db: Database) -> None:
    nul = make_item("14001", title="RTX 4090\x00 FE", seller="bob\x00by", location=Location(text="Austin\x00"))
    sur = make_item("14002", title="RTX 4090 \ud83d deal 😀")
    await db.write_snapshots([rec(nul, accepted_fr(), make_score(), T0), rec(sur, accepted_fr(), make_score(), T0)])
    n = await fetch_listing(db, nul)
    assert (n["title"], n["seller_name"], n["location_text"]) == ("RTX 4090 FE", "bobby", "Austin")
    assert (await fetch_listing(db, sur))["title"] == "RTX 4090 � deal \U0001f600"
    assert (await db.counts())["snapshots"] == 2

    item = make_item("14003", title="RTX\x00 4090").model_copy(
        update={"extra": {"bad": "x\ud83d", "huge": 2**70, "nested": [{"k\x00": "v\x00"}]}}
    )
    alert = make_alert(item)
    report = DispatchReport(
        alert_id=alert.alert_id,
        results=[DispatchResult(target="discord:gpu", ok=False, status=502, error="bad gateway\x00")],
        suppressed_reason="flood\x00guard",
    )
    await db.write_alert(alert, report)
    [row] = await db.recent_alerts()
    assert row["payload"]["item"]["title"] == "RTX 4090"
    assert row["payload"]["item"]["extra"] == {"bad": "x�", "huge": str(2**70), "nested": [{"k": "v"}]}
    assert row["results"][0]["error"] == "bad gateway"
    assert row["suppressed_reason"] == "floodguard"


async def test_non_finite_prices_are_skipped_not_fatal(db: Database, caplog: pytest.LogCaptureFixture) -> None:
    nan_item = make_item("14101").model_copy(update={"price": float("nan"), "total_price": float("nan")})
    inf_item = make_item("14102").model_copy(update={"total_price": float("inf")})
    good = make_item("14103").model_copy(update={"shipping": float("nan")})
    with caplog.at_level(logging.WARNING, logger="deal_radar.db"):
        await db.write_snapshots(
            [
                rec(nan_item, accepted_fr(), make_score(), T0),
                rec(inf_item, accepted_fr(), make_score(), T0),
                rec(good, accepted_fr(), make_score(risk=float("nan")), T0),
            ]
        )
        await db.write_snapshots([rec(nan_item, accepted_fr(), make_score(), T0)])  # nothing valid: no-op
    assert await db.counts() == {"listings": 1, "snapshots": 1, "alerts": 0}
    [snapshot] = await fetch_snapshots(db)
    assert snapshot["listing_id"] == good.fingerprint
    assert snapshot["shipping"] is None and snapshot["risk"] is None
    assert sum("non-finite price" in r.getMessage() for r in caplog.records) == 3

    alert = make_alert(good).model_copy(update={"ingest_lag_ms": float("nan"), "pipeline_ms": float("inf")})
    await db.write_alert(alert, make_report(alert))
    [row] = await db.recent_alerts()
    assert row["ingest_lag_ms"] is None and row["pipeline_ms"] == 0.0


async def test_long_alert_ids_are_stored(db: Database) -> None:
    alert = make_alert(make_item("14201")).model_copy(update={"alert_id": "x" * 300})
    await db.write_alert(alert, make_report(alert))
    assert [r["alert_id"] for r in await db.recent_alerts()] == ["x" * 300]


def test_listing_rows_are_upserted_in_primary_key_order() -> None:
    # Two nodes upserting the same listings in different orders deadlock on PostgreSQL;
    # a global lock order (sorted primary keys) makes that impossible.
    records = [rec(make_item(f"15{i:03d}"), accepted_fr(), make_score(), T0) for i in range(50)]
    rows, snapshots = build_snapshot_rows(list(reversed(records)))
    ids = [r["id"] for r in rows]
    assert ids == sorted(ids) and len(ids) == 50
    assert [s["listing_id"] for s in snapshots] == [r.item.fingerprint for r in reversed(records)]


async def test_concurrent_writers_with_opposite_order_do_not_deadlock(db: Database) -> None:
    other = Database(db.url.render_as_string(hide_password=False))
    await other.connect()
    try:
        items = [make_item(f"16{i:03d}") for i in range(150)]
        for round_ in range(4):
            batch = [rec(item, accepted_fr(), make_score(), T0 + timedelta(minutes=round_)) for item in items]
            await asyncio.gather(db.write_snapshots(batch), other.write_snapshots(list(reversed(batch))))
    finally:
        await other.close()
    assert await db.counts() == {"listings": 150, "snapshots": 150 * 2 * 4, "alerts": 0}


# --------------------------------------------------------------------------- alerts


async def test_alert_roundtrip_and_idempotent_rewrite(db: Database) -> None:
    item = make_item("6001", price=999.0)
    alert = make_alert(item, created_at=T0)
    await db.write_alert(alert, make_report(alert))

    [row] = await db.recent_alerts()
    assert row["alert_id"] == alert.alert_id
    assert row["listing_id"] == item.fingerprint
    assert row["product_key"] == "gpu_rtx_4090:fe"
    assert row["severity"] == "critical"
    assert row["score"] == pytest.approx(91.0)
    assert row["price"] == pytest.approx(999.0)
    assert row["market_price"] == pytest.approx(1818.0)
    assert row["discount_pct"] == pytest.approx(0.45)
    assert row["is_price_error"] is True and row["is_update"] is False
    assert row["previous_price"] is None
    assert row["targets"] == ["discord:gpu", "telegram:main"]
    assert row["results"][0] == make_report(alert).results[0].model_dump(mode="json")
    assert row["results"][1]["error"] == "rate limited"
    assert row["ok"] is True
    assert row["suppressed_reason"] is None
    assert row["pipeline_ms"] == pytest.approx(12.4) and row["ingest_lag_ms"] == pytest.approx(4500.0)
    assert datetime.fromisoformat(row["created_at"]) == T0
    assert row["payload"] == alert.model_dump(mode="json")
    assert Alert.model_validate(row["payload"]) == alert

    # Re-writing the same alert id updates the delivery outcome instead of failing.
    failed = make_report(alert, ok=False)
    await db.write_alert(alert, failed.model_copy(update={"suppressed_reason": "flood_guard"}))
    [row] = await db.recent_alerts()
    assert row["ok"] is False
    assert row["suppressed_reason"] == "flood_guard"
    assert row["results"][0]["status"] == 500
    assert (await db.counts())["alerts"] == 1


async def test_recent_alerts_newest_first_with_limit(db: Database) -> None:
    alerts = [make_alert(make_item(f"70{i}"), created_at=T0 + timedelta(minutes=i), update=i == 2) for i in range(4)]
    await db.write_alerts([(a, make_report(a)) for a in alerts])
    rows = await db.recent_alerts(limit=3)
    assert [r["alert_id"] for r in rows] == [alerts[3].alert_id, alerts[2].alert_id, alerts[1].alert_id]
    assert rows[1]["is_update"] is True and rows[1]["previous_price"] == pytest.approx(1100.0)
    assert await db.recent_alerts(limit=0) == []
    assert (await db.counts())["alerts"] == 4


# --------------------------------------------------------------------------- price history


async def test_load_price_history_semantics(db: Database) -> None:
    h = timedelta(hours=1)
    a1 = make_item("8001", price=1000.0)
    a2 = make_item("8002", price=1100.0)
    old = make_item("8003", price=800.0)
    rej = make_item("8004", price=200.0)
    scam = make_item("8005", price=300.0)
    used = make_item("8006", price=700.0, condition=Condition.USED)
    risky = make_item("8007", price=1050.0)
    await db.write_snapshots(
        [
            rec(a1, accepted_fr(), make_score(), T0 + 1 * h),
            rec(make_item("8001", price=950.0), accepted_fr(), make_score(), T0 + 3 * h),  # latest for a1
            rec(a2, accepted_fr(), make_score(), T0 + 2 * h),
            rec(make_item("8002", price=90.0), rejected_fr("box_only"), None, T0 + 4 * h),  # rejected later
            rec(old, accepted_fr(), make_score(), T0 - timedelta(days=1)),  # before `since`
            rec(rej, rejected_fr("rental"), None, T0 + 1 * h),
            rec(scam, accepted_fr(), make_score(risk=0.97, rejected=True, reason="scam_risk"), T0 + 1 * h),
            rec(used, accepted_fr(), make_score(), T0 + 2 * h),
            rec(risky, accepted_fr(), make_score(risk=0.6), T0 + 5 * h),
        ]
    )
    rows = await db.load_price_history(T0)
    assert rows == [
        ("gpu_rtx_4090:fe", "new", "ebay:8002", 1100.0, T0 + 2 * h, "ebay"),
        ("gpu_rtx_4090:fe", "new", "ebay:8001", 950.0, T0 + 3 * h, "ebay"),
        ("gpu_rtx_4090:fe", "new", "ebay:8007", 1050.0, T0 + 5 * h, "ebay"),
        ("gpu_rtx_4090:fe", "used", "ebay:8006", 700.0, T0 + 2 * h, "ebay"),
    ]
    for row in rows:
        assert row[4].tzinfo is not None and row[4].utcoffset() == timedelta(0)

    # max_risk mirrors scoring.history_max_risk.
    filtered = await db.load_price_history(T0, max_risk=0.5)
    assert "ebay:8007" not in [r[2] for r in filtered] and len(filtered) == 3

    # `since` excludes older snapshots, including an older observation of a1.
    later = await db.load_price_history(T0 + 2 * h + timedelta(minutes=30))
    assert [r[2] for r in later] == ["ebay:8001", "ebay:8007"]

    # `since` given in another timezone is compared in UTC.
    tz_minus5 = timezone(timedelta(hours=-5))
    assert await db.load_price_history((T0 + 2 * h + timedelta(minutes=30)).astimezone(tz_minus5)) == later

    assert await db.load_price_history(T0, max_per_key=0) == []


async def test_load_price_history_caps_most_recent_per_key(db: Database) -> None:
    records = []
    for i in range(6):
        records.append(
            rec(make_item(f"90{i}", source="reddit", price=800.0 + i), accepted_fr("gpu_rtx_4080", None), make_score(), T0 + timedelta(hours=i))
        )
    records.append(rec(make_item("999", price=1500.0), accepted_fr(), make_score(), T0))  # another key, unaffected by cap
    await db.write_snapshots(records)

    rows = await db.load_price_history(T0 - timedelta(days=1), max_per_key=3)
    capped = [r for r in rows if r[0] == "gpu_rtx_4080"]
    assert [(r[2], r[3]) for r in capped] == [("reddit:903", 803.0), ("reddit:904", 804.0), ("reddit:905", 805.0)]
    assert [r[4] for r in capped] == sorted(r[4] for r in capped)  # chronological per key
    assert [r[0] for r in rows if r[0] != "gpu_rtx_4080"] == ["gpu_rtx_4090:fe"]
    assert all(r[5] == "reddit" for r in capped)


# --------------------------------------------------------------------------- datetimes


async def test_datetimes_round_trip_as_aware_utc(db: Database) -> None:
    naive = datetime(2026, 9, 2, 8, 30, 15, 123456)  # treated as UTC
    plus2 = datetime(2026, 9, 2, 12, 0, tzinfo=timezone(timedelta(hours=2)))  # == 10:00 UTC
    item = make_item("10001")
    await db.write_snapshots([rec(item, accepted_fr(), make_score(), naive), rec(make_item("10001", price=990.0), accepted_fr(), make_score(), plus2)])

    row = await fetch_listing(db, item)
    assert row["first_seen"] == naive.replace(tzinfo=timezone.utc)
    assert row["first_seen"].tzinfo is timezone.utc
    assert row["last_seen"] == datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc)
    assert row["last_seen"].tzinfo is timezone.utc
    assert row["first_seen"].microsecond == 123456

    # ORM access returns aware datetimes too.
    async with AsyncSession(db.engine) as session:
        listing = await session.get(Listing, item.fingerprint)
        assert listing is not None
        assert listing.last_seen.tzinfo is timezone.utc
        assert listing.min_price == pytest.approx(990.0)

    alert = make_alert(item, created_at=plus2)
    await db.write_alert(alert, make_report(alert))
    [a] = await db.recent_alerts()
    assert a["created_at"] == "2026-09-02T10:00:00+00:00"


def test_utc_datetime_type_processing() -> None:
    col_type = UTCDateTime()
    aware = datetime(2026, 1, 1, 5, tzinfo=timezone(timedelta(hours=-3)))
    assert col_type.process_bind_param(aware, sqlite.dialect()) == datetime(2026, 1, 1, 8)
    assert col_type.process_bind_param(aware, postgresql.dialect()) == datetime(2026, 1, 1, 8, tzinfo=timezone.utc)
    assert col_type.process_bind_param(None, sqlite.dialect()) is None
    assert col_type.process_result_value(datetime(2026, 1, 1, 8), sqlite.dialect()) == datetime(2026, 1, 1, 8, tzinfo=timezone.utc)
    with pytest.raises(TypeError):
        col_type.process_bind_param("2026-01-01", sqlite.dialect())  # type: ignore[arg-type]
    assert ensure_utc(datetime(2026, 1, 1)).tzinfo is timezone.utc


# --------------------------------------------------------------------------- prune / counts


async def test_prune_deletes_expired_rows_in_chunks(db: Database) -> None:
    now = utcnow()
    ancient = now - timedelta(days=200)
    recent = now - timedelta(days=1)
    gone = make_item("11001")
    kept = make_item("11002")
    await db.write_snapshots(
        [
            rec(gone, accepted_fr(), make_score(), ancient),
            rec(make_item("11001", price=990.0), accepted_fr(), make_score(), ancient + timedelta(hours=1)),
            rec(kept, accepted_fr(), make_score(), ancient),  # old snapshot of a still-live listing
            rec(make_item("11002", price=980.0), accepted_fr(), make_score(), recent),
        ]
    )
    old_alert = make_alert(gone, created_at=ancient)
    new_alert = make_alert(kept, created_at=recent)
    await db.write_alerts([(old_alert, make_report(old_alert)), (new_alert, make_report(new_alert))])
    assert await db.counts() == {"listings": 2, "snapshots": 4, "alerts": 2}

    deleted = await db.prune(120, chunk_size=1)  # chunk_size=1 exercises the batching loop
    assert deleted == 3 + 1 + 1  # 3 snapshots, 1 alert, 1 listing
    assert await db.counts() == {"listings": 1, "snapshots": 1, "alerts": 1}
    remaining = await fetch_snapshots(db)
    assert remaining[0]["listing_id"] == kept.fingerprint and remaining[0]["observed_at"] == recent
    assert [a["alert_id"] for a in await db.recent_alerts()] == [new_alert.alert_id]
    assert await db.prune(120) == 0

    with pytest.raises(ValueError):
        await db.prune(0)


async def test_listing_delete_cascades_to_snapshots(db: Database) -> None:
    item = make_item("12001")
    await db.write_snapshots([rec(item, accepted_fr(), make_score(), T0), rec(make_item("12001"), accepted_fr(), make_score(), T0 + timedelta(hours=1))])
    async with db.engine.begin() as conn:
        await conn.execute(LISTINGS.delete().where(LISTINGS.c.id == item.fingerprint))
    assert await db.counts() == {"listings": 0, "snapshots": 0, "alerts": 0}


# --------------------------------------------------------------------------- PostgreSQL SQL


def test_postgres_statements_compile() -> None:
    pg = postgresql.dialect()
    upsert = str(listing_upsert_statement("postgresql").compile(dialect=pg))
    assert upsert.startswith("INSERT INTO listings")
    assert "ON CONFLICT (id) DO UPDATE SET" in upsert
    assert "CASE WHEN (excluded.min_price < listings.min_price) THEN excluded.min_price ELSE listings.min_price END" in upsert
    assert "excluded.last_seen >= listings.last_seen" in upsert
    assert "coalesce(excluded.product_key, listings.product_key)" in upsert
    assert "source_id =" not in upsert and "listing_key =" not in upsert  # identity columns never change

    alert_sql = str(alert_upsert_statement("postgresql").compile(dialect=pg))
    assert "ON CONFLICT (id) DO UPDATE SET" in alert_sql and "payload = excluded.payload" in alert_sql

    history_sql = str(price_history_query(T0, 400).compile(dialect=pg))
    assert "row_number() OVER (PARTITION BY price_snapshots.listing_id ORDER BY price_snapshots.observed_at DESC" in history_sql
    assert "row_number() OVER (PARTITION BY per_listing.product_key, per_listing.condition_class" in history_sql
    assert "JOIN listings ON listings.id = per_key.listing_id" in history_sql

    snapshots_ddl = str(CreateTable(SNAPSHOTS).compile(dialect=pg))
    assert "BIGSERIAL" in snapshots_ddl and "TIMESTAMP WITH TIME ZONE" in snapshots_ddl
    assert "ON DELETE CASCADE" in snapshots_ddl
    alerts_ddl = str(CreateTable(ALERTS).compile(dialect=pg))
    assert "payload JSONB NOT NULL" in alerts_ddl and "targets JSONB NOT NULL" in alerts_ddl
    assert "id TEXT NOT NULL" in alerts_ddl  # Alert.alert_id has no length limit

    sqlite_sql = str(listing_upsert_statement("sqlite").compile(dialect=sqlite.dialect()))
    assert "ON CONFLICT (id) DO UPDATE SET" in sqlite_sql
    assert "INTEGER NOT NULL" in str(CreateTable(SNAPSHOTS).compile(dialect=sqlite.dialect()))

    with pytest.raises(ValueError):
        listing_upsert_statement("mysql")


def test_redact_url() -> None:
    assert redact_url("redis://:s3cret@redis.internal:6379/0") == "redis://:***@redis.internal:6379/0"
    assert redact_url("redis://user:s3cret@host/1") == "redis://user:***@host/1"
    assert redact_url("redis://localhost:6379/0") == "redis://localhost:6379/0"
    # redis-py also reads credentials from the query string.
    assert redact_url("redis://cache:6379/0?password=s3cret&db=1") == "redis://cache:6379/0?password=***&db=1"
    assert redact_url("unix:///run/redis.sock?db=0&password=s3cret") == "unix:///run/redis.sock?db=0&password=***"
    db = Database("postgresql+asyncpg://dealradar:hunter2@db:5432/deals")
    assert "hunter2" not in redact_url(db.url)


def test_sqlite_file_path_detection(tmp_path: Path) -> None:
    assert _sqlite_file_path(make_url("sqlite+aiosqlite://")) is None
    assert _sqlite_file_path(make_url("sqlite+aiosqlite:///:memory:")) is None
    assert _sqlite_file_path(make_url("sqlite+aiosqlite:///file:mem1?mode=memory&cache=shared&uri=true")) is None
    assert _sqlite_file_path(make_url(f"sqlite+aiosqlite:///{tmp_path}/x/y.db")) == tmp_path / "x" / "y.db"
    assert _sqlite_file_path(make_url(f"sqlite+aiosqlite:///file:{tmp_path}/z.db?uri=true")) == tmp_path / "z.db"


def test_is_transient_error() -> None:
    def dbapi_error(sqlstate: str) -> sa_exc.DBAPIError:
        orig = type("PgError", (Exception,), {"sqlstate": sqlstate})("boom")
        return sa_exc.DBAPIError("INSERT ...", {}, orig)

    assert is_transient_error(ConnectionRefusedError())
    assert is_transient_error(TimeoutError())
    assert is_transient_error(sa_exc.OperationalError("INSERT ...", {}, Exception("database is locked")))
    assert is_transient_error(sa_exc.InterfaceError("INSERT ...", {}, Exception("connection is closed")))
    assert is_transient_error(dbapi_error("40P01"))  # deadlock_detected
    assert is_transient_error(dbapi_error("08006"))  # connection_failure
    assert not is_transient_error(dbapi_error("22021"))  # NUL byte / bad encoding: poison record
    assert not is_transient_error(sa_exc.IntegrityError("INSERT ...", {}, Exception("NOT NULL constraint failed")))
    assert not is_transient_error(UnicodeEncodeError("utf-8", "\ud83d", 0, 1, "surrogates not allowed"))
    assert not is_transient_error(TypeError("Integer exceeds 64-bit range"))


# --------------------------------------------------------------------------- Recorder


class FakeDB:
    """Records batches; can fail the first ``fail_times`` calls or block forever."""

    def __init__(self, fail_times: int = 0, block: bool = False) -> None:
        self.fail_times = fail_times
        self.block = block
        self.calls = 0
        self.snapshot_batches: list[list[SnapshotRecord]] = []
        self.alert_batches: list[list[tuple[Alert, DispatchReport]]] = []

    async def _maybe_fail(self) -> None:
        self.calls += 1
        if self.block:
            await asyncio.Event().wait()
        if self.fail_times > 0:
            self.fail_times -= 1
            raise ConnectionError("database is unavailable")

    async def write_snapshots(self, records: Sequence[SnapshotRecord]) -> None:
        await self._maybe_fail()
        self.snapshot_batches.append(list(records))

    async def write_alerts(self, alerts: Sequence[tuple[Alert, DispatchReport]]) -> None:
        await self._maybe_fail()
        self.alert_batches.append(list(alerts))

    @property
    def snapshot_sizes(self) -> list[int]:
        return [len(b) for b in self.snapshot_batches]


def snap(i: int) -> SnapshotRecord:
    return rec(make_item(f"r{i}"), accepted_fr(), make_score(), T0 + timedelta(seconds=i))


async def test_recorder_flushes_full_batches_immediately() -> None:
    fake, metrics = FakeDB(), Metrics()
    recorder = Recorder(fake, batch_size=3, flush_seconds=60.0, metrics=metrics)  # type: ignore[arg-type]
    await recorder.start()
    for i in range(7):
        recorder.record(snap(i))
    await wait_until(lambda: fake.snapshot_sizes == [3, 3])
    await asyncio.sleep(0.05)
    assert fake.snapshot_sizes == [3, 3]  # the 7th waits for the flush window
    assert recorder.pending == 1
    await recorder.stop()
    assert fake.snapshot_sizes == [3, 3, 1]
    assert [r.item.source_id for b in fake.snapshot_batches for r in b] == [f"r{i}" for i in range(7)]
    assert metrics.counter("db_records_written_total", labelnames=("kind",)).value(kind="snapshot") == 7
    assert not recorder.running


async def test_recorder_flushes_after_flush_seconds() -> None:
    fake = FakeDB()
    recorder = Recorder(fake, batch_size=1000, flush_seconds=0.05)  # type: ignore[arg-type]
    await recorder.start()
    started = time.monotonic()
    recorder.record(snap(1))
    recorder.record(snap(2))
    await wait_until(lambda: fake.snapshot_sizes == [2])
    assert time.monotonic() - started >= 0.04
    recorder.record(snap(3))
    await wait_until(lambda: fake.snapshot_sizes == [2, 1])
    await recorder.stop()
    assert fake.snapshot_sizes == [2, 1]


async def test_recorder_stop_flushes_snapshots_and_alerts() -> None:
    fake = FakeDB()
    recorder = Recorder(fake, batch_size=500, flush_seconds=60.0)  # type: ignore[arg-type]
    await recorder.start()
    alert = make_alert(make_item("a1"))
    for i in range(5):
        recorder.record(snap(i))
    recorder.record_alert(alert, make_report(alert))
    started = time.monotonic()
    await recorder.stop()
    assert time.monotonic() - started < 1.0  # does not wait for the 60s flush window
    assert fake.snapshot_sizes == [5]
    assert [a.alert_id for a, _ in fake.alert_batches[0]] == [alert.alert_id]
    assert recorder.pending == 0


async def test_recorder_queue_full_drops_without_blocking() -> None:
    fake, metrics = FakeDB(), Metrics()
    recorder = Recorder(fake, batch_size=10, flush_seconds=60.0, metrics=metrics, queue_max=2)  # type: ignore[arg-type]
    dropped = metrics.counter("db_records_dropped_total", labelnames=("kind", "reason"))
    for i in range(5):
        assert recorder.record(snap(i)) is None  # never raises, never blocks (not even started)
    alert = make_alert(make_item("q"))
    recorder.record_alert(alert, make_report(alert))
    assert recorder.pending == 2
    assert dropped.value(kind="snapshot", reason="queue_full") == 3
    assert dropped.value(kind="alert", reason="queue_full") == 1

    await recorder.stop()  # never started: stop still drains the queue
    assert fake.snapshot_sizes == [2]
    recorder.record(snap(99))  # after stop -> dropped as closed
    assert dropped.value(kind="snapshot", reason="closed") == 1
    assert recorder.pending == 0
    with pytest.raises(RuntimeError):
        await recorder.start()


async def test_recorder_retries_failed_flush_once() -> None:
    fake, metrics = FakeDB(fail_times=1), Metrics()
    recorder = Recorder(fake, batch_size=2, flush_seconds=60.0, metrics=metrics, retry_delay=0.0)  # type: ignore[arg-type]
    await recorder.start()
    recorder.record(snap(1))
    recorder.record(snap(2))
    await wait_until(lambda: fake.snapshot_sizes == [2])
    await recorder.stop()
    assert fake.calls == 2
    assert metrics.counter("db_flush_failures_total", labelnames=("kind",)).value(kind="snapshot") == 1
    assert metrics.counter("db_records_written_total", labelnames=("kind",)).value(kind="snapshot") == 2
    assert metrics.counter("db_records_dropped_total", labelnames=("kind", "reason")).value(kind="snapshot", reason="write_error") == 0


async def test_recorder_drops_batch_after_second_failure_and_keeps_running(caplog: pytest.LogCaptureFixture) -> None:
    fake, metrics = FakeDB(fail_times=2), Metrics()
    recorder = Recorder(fake, batch_size=3, flush_seconds=60.0, metrics=metrics, retry_delay=0.0)  # type: ignore[arg-type]
    await recorder.start()
    with caplog.at_level(logging.WARNING, logger="deal_radar.db"):
        for i in range(3):
            recorder.record(snap(i))
        await wait_until(lambda: fake.calls == 2)
        await asyncio.sleep(0.01)
    dropped = metrics.counter("db_records_dropped_total", labelnames=("kind", "reason"))
    assert dropped.value(kind="snapshot", reason="write_error") == 3
    assert fake.snapshot_batches == []
    assert any(r.levelno == logging.ERROR and "dropping batch" in r.getMessage() for r in caplog.records)

    # The recorder survives and persists later records.
    for i in range(3, 6):
        recorder.record(snap(i))
    await wait_until(lambda: fake.snapshot_sizes == [3])
    await recorder.stop()
    assert metrics.counter("db_records_written_total", labelnames=("kind",)).value(kind="snapshot") == 3


async def test_recorder_stop_timeout_drops_inflight_and_queued() -> None:
    fake, metrics = FakeDB(block=True), Metrics()
    recorder = Recorder(fake, batch_size=2, flush_seconds=60.0, metrics=metrics)  # type: ignore[arg-type]
    await recorder.start()
    for i in range(5):
        recorder.record(snap(i))
    await wait_until(lambda: fake.calls == 1)
    await recorder.stop(timeout=0.1)
    dropped = metrics.counter("db_records_dropped_total", labelnames=("kind", "reason"))
    assert dropped.value(kind="snapshot", reason="cancelled") == 2  # the in-flight batch
    assert dropped.value(kind="snapshot", reason="shutdown_timeout") == 3
    assert recorder.pending == 0


async def test_recorder_stop_timeout_counts_inflight_alerts_of_a_mixed_batch() -> None:
    fake, metrics = FakeDB(block=True), Metrics()
    recorder = Recorder(fake, batch_size=3, flush_seconds=60.0, metrics=metrics)  # type: ignore[arg-type]
    await recorder.start()
    alert = make_alert(make_item("mix"))
    recorder.record(snap(1))
    recorder.record(snap(2))
    recorder.record_alert(alert, make_report(alert))  # fills the batch: 2 snapshots + 1 alert in flight
    await wait_until(lambda: fake.calls == 1)
    await recorder.stop(timeout=0.1)
    dropped = metrics.counter("db_records_dropped_total", labelnames=("kind", "reason"))
    assert dropped.value(kind="snapshot", reason="cancelled") == 2
    assert dropped.value(kind="alert", reason="cancelled") == 1  # used to vanish uncounted


class PoisonDB(FakeDB):
    """Rejects every batch that contains a poison record, like a constraint/encoding error would."""

    def __init__(self, poison: set[str], error: Exception, *, outage_after: int | None = None) -> None:
        super().__init__()
        self.poison = poison
        self.error = error
        self.outage_after = outage_after  # calls after which the database "goes away"

    async def write_snapshots(self, records: Sequence[SnapshotRecord]) -> None:
        self.calls += 1
        if self.outage_after is not None and self.calls > self.outage_after:
            raise ConnectionRefusedError("database went away")
        if any(r.item.source_id in self.poison for r in records):
            raise self.error
        self.snapshot_batches.append(list(records))


async def test_recorder_isolates_poison_records_instead_of_dropping_the_batch(caplog: pytest.LogCaptureFixture) -> None:
    error = sa_exc.IntegrityError("INSERT INTO listings ...", {}, Exception("NOT NULL constraint failed"))
    fake, metrics = PoisonDB({"r3", "r6"}, error), Metrics()
    recorder = Recorder(fake, batch_size=8, flush_seconds=60.0, metrics=metrics, retry_delay=0.0)  # type: ignore[arg-type]
    await recorder.start()
    with caplog.at_level(logging.WARNING, logger="deal_radar.db"):
        for i in range(8):
            recorder.record(snap(i))
        await recorder.stop()
    written = sorted(r.item.source_id for b in fake.snapshot_batches for r in b)
    assert written == ["r0", "r1", "r2", "r4", "r5", "r7"]
    dropped = metrics.counter("db_records_dropped_total", labelnames=("kind", "reason"))
    assert dropped.value(kind="snapshot", reason="write_error") == 2
    assert metrics.counter("db_records_written_total", labelnames=("kind",)).value(kind="snapshot") == 6
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "isolated" in errors[0].getMessage()
    assert errors[0].__dict__["dropped"] == 2 and errors[0].__dict__["written"] == 6
    assert fake.calls <= 2 + 2 * 8  # retry + bounded bisection


async def test_recorder_stops_isolating_when_the_database_goes_away() -> None:
    error = sa_exc.DataError("INSERT ...", {}, Exception("invalid byte sequence"))
    fake, metrics = PoisonDB({"r1"}, error, outage_after=2), Metrics()
    recorder = Recorder(fake, batch_size=8, flush_seconds=60.0, metrics=metrics, retry_delay=0.0)  # type: ignore[arg-type]
    await recorder.start()
    for i in range(8):
        recorder.record(snap(i))
    await recorder.stop()
    assert fake.calls == 3  # attempt, retry, first half hits the outage -> stop bisecting
    assert fake.snapshot_batches == []
    dropped = metrics.counter("db_records_dropped_total", labelnames=("kind", "reason"))
    assert dropped.value(kind="snapshot", reason="write_error") == 8


async def test_recorder_with_real_database(db: Database) -> None:
    recorder = Recorder(db, batch_size=2, flush_seconds=0.05)
    await recorder.start()
    item = make_item("13001")
    recorder.record(rec(item, accepted_fr(), make_score(), T0))
    recorder.record(rec(make_item("13002"), rejected_fr("rental"), None, T0))
    recorder.record(rec(make_item("13001", price=900.0), accepted_fr(), make_score(), T0 + timedelta(minutes=5)))
    alert = make_alert(item)
    recorder.record_alert(alert, make_report(alert))
    await recorder.stop()
    assert await db.counts() == {"listings": 2, "snapshots": 3, "alerts": 1}
    assert (await fetch_listing(db, item))["min_price"] == pytest.approx(900.0)


def test_recorder_validates_arguments() -> None:
    with pytest.raises(ValueError):
        Recorder(FakeDB(), batch_size=0, flush_seconds=1.0)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Recorder(FakeDB(), batch_size=1, flush_seconds=0)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- connect_redis

REDIS_SERVER = shutil.which("redis-server") or "/usr/bin/redis-server"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def redis_server_url(tmp_path: Path) -> Iterator[str]:
    if not os.path.exists(REDIS_SERVER):
        pytest.skip("redis-server binary not available")
    port = _free_port()
    proc = subprocess.Popen(
        [REDIS_SERVER, "--port", str(port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no", "--dir", str(tmp_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                if proc.poll() is not None or time.monotonic() > deadline:
                    pytest.skip("redis-server failed to start")
                time.sleep(0.02)
        yield f"redis://127.0.0.1:{port}/0"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


async def test_connect_redis_returns_pooled_bytes_client(redis_server_url: str) -> None:
    client = await connect_redis(redis_server_url, max_connections=7, socket_timeout=1.5)
    try:
        await client.set("dr:test", "value")
        assert await client.get("dr:test") == b"value"  # decode_responses=False
        pool = client.connection_pool
        assert pool.max_connections == 7
        kwargs = pool.connection_kwargs
        assert kwargs["socket_timeout"] == 1.5
        assert kwargs["socket_connect_timeout"] == 1.5
        assert kwargs["health_check_interval"] == 30
        assert kwargs.get("decode_responses", False) is False
    finally:
        await client.aclose()


async def test_connect_redis_over_unix_socket() -> None:
    """``unix://`` URLs are allowed by the config; TCP-only options must not be passed for them."""
    if not os.path.exists(REDIS_SERVER):
        pytest.skip("redis-server binary not available")
    sock_dir = tempfile.mkdtemp(prefix="drrs")  # short path: AF_UNIX paths are limited to ~100 bytes
    sock = os.path.join(sock_dir, "redis.sock")
    proc = subprocess.Popen(
        [REDIS_SERVER, "--port", "0", "--unixsocket", sock, "--save", "", "--appendonly", "no", "--dir", sock_dir],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 10
        while not os.path.exists(sock):
            if proc.poll() is not None or time.monotonic() > deadline:
                pytest.skip("redis-server failed to start")
            await asyncio.sleep(0.02)
        client = await connect_redis(f"unix://{sock}?db=0", max_connections=4, socket_timeout=1.0)
        try:
            await client.set("dr:unix", "1")
            assert await client.get("dr:unix") == b"1"
            assert client.connection_pool.max_connections == 4
        finally:
            await client.aclose()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        shutil.rmtree(sock_dir, ignore_errors=True)


async def test_connect_redis_raises_when_unreachable() -> None:
    port = _free_port()  # nothing listens here
    with pytest.raises(redis.exceptions.ConnectionError):
        await connect_redis(f"redis://127.0.0.1:{port}/0", max_connections=2, socket_timeout=0.5)
