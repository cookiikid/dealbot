"""Tests for the Discord, Telegram and WebSocket channels.

Everything runs offline: Discord and the Telegram Bot API are replaced by aiohttp
``TestServer`` fakes that capture each request and replay scripted responses, and
the WebSocket hub is exercised with a real aiohttp client against a local server.
"""

from __future__ import annotations

import asyncio
import logging
import re
import socket
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import aiohttp
import pytest
from aiohttp import WSCloseCode, WSMsgType, web
from aiohttp.test_utils import TestServer
from pydantic import SecretStr

from deal_radar.config_schema import DiscordTarget, TelegramChat, WebSocketSection
from deal_radar.core.backoff import BackoffPolicy
from deal_radar.core.http import HttpClient, NetworkSettings, json_dumps
from deal_radar.core.metrics import Metrics
from deal_radar.dispatchers.base import SEVERITY_COLOR, facts, headline, links
from deal_radar.dispatchers.discord import (
    BUTTON_LABEL_LIMIT,
    BUTTON_URL_LIMIT,
    DESCRIPTION_LIMIT,
    EMBED_TOTAL_LIMIT,
    FIELD_NAME_LIMIT,
    FIELD_VALUE_LIMIT,
    FIELDS_LIMIT,
    FOOTER_LIMIT,
    TITLE_LIMIT,
    ZWSP,
    DiscordDispatcher,
    _enforce_total,
    build_discord_payload,
    embed_length,
    escape_markdown,
    minimal_payload,
    scrub_alert,
    scrub_surrogates,
    truncate,
    webhook_url,
)
from deal_radar.dispatchers.telegram import (
    CAPTION_LIMIT,
    MESSAGE_LIMIT,
    TelegramDispatcher,
    build_inline_keyboard,
    build_plain_message,
    build_telegram_message,
    fit_escaped,
    is_silent,
    text_length,
)
from deal_radar.dispatchers.websocket import WebSocketHub, _Client
from deal_radar.engine.types import (
    Alert,
    Condition,
    DealItem,
    DedupDecision,
    DedupStatus,
    Location,
    RiskSignal,
    ScoreResult,
    SellerInfo,
    Severity,
    SourceKind,
    VisionResult,
    VisionVerdict,
)

T0 = datetime(2026, 10, 6, 12, 30, 15, tzinfo=timezone.utc)
DISCORD_TOKEN = "dWh0b2tlbi1zZWNyZXQtdmFsdWUtMDEyMzQ1Njc4OWFiY2RlZg"
TELEGRAM_TOKEN = "7123456789:AAHsecretTelegramTokenValue_0123456789xyz"
IMAGE = "https://i.ebayimg.com/images/g/abcAAOSw/s-l1600.jpg"


# --------------------------------------------------------------------------- fixtures / builders


def make_item(**overrides: Any) -> DealItem:
    base: dict[str, Any] = {
        "source": "ebay",
        "source_kind": SourceKind.MARKETPLACE,
        "source_id": "306012345678",
        "url": "https://www.ebay.com/itm/306012345678",
        "title": "NVIDIA GeForce RTX 4090 Founders Edition 24GB GDDR6X",
        "price": 999.99,
        "shipping": 10.0,
        "total_price": 1009.99,
        "condition": Condition.USED,
        "seller": SellerInfo(name="gpu_trader", feedback_score=812, feedback_pct=99.8),
        "location": Location(city="Austin", region="TX", distance_miles=12),
        "image_urls": [IMAGE],
        "node_id": "vm-1",
    }
    base.update(overrides)
    return DealItem(**base)


def make_alert(
    item: DealItem | None = None,
    *,
    severity: Severity = Severity.HIGH,
    mention: bool = False,
    is_price_error: bool = False,
    explain: list[str] | None = None,
    risk: list[RiskSignal] | None = None,
    profile_name: str = "RTX 4090",
    update: bool = False,
    vision: VisionResult | None = None,
) -> Alert:
    signals = risk if risk is not None else [RiskSignal(code="low_feedback", probability=0.2, origin="seller")]
    score = ScoreResult(
        score=88.4,
        severity=severity,
        market_price=1818.0,
        market_basis="blend",
        discount_pct=0.45,
        history_n=14,
        confidence=0.82,
        risk=0.2,
        risk_signals=signals,
        is_price_error=is_price_error,
        explain=explain if explain is not None else ["market $1,818 (blend of 14 sales + config)", "robust z 3.1, below Q1"],
        components={"D": 0.9, "C": 0.82},
    )
    return Alert(
        item=item or make_item(),
        profile_id="gpu_rtx_4090",
        profile_name=profile_name,
        variant_id="fe",
        category="gpu",
        severity=severity,
        score=score,
        vision=vision,
        dedup=DedupDecision(
            status=DedupStatus.PRICE_DROP if update else DedupStatus.NEW,
            previous_price=1199.0 if update else None,
        ),
        created_at=T0,
        pipeline_ms=11.7,
        ingest_lag_ms=4200.0,
        mention=mention,
    )


@dataclass
class Captured:
    path: str
    query: dict[str, str]
    json: Any
    at: float


@dataclass
class FakeAPI:
    """Scripted HTTP fake: records every POST and replays queued responses."""

    default: tuple[int, Any, dict[str, str]]
    requests: list[Captured] = field(default_factory=list)
    responses: deque[tuple[int, Any, dict[str, str]]] = field(default_factory=deque)
    server: TestServer | None = None

    def queue(self, status: int, body: Any = None, headers: dict[str, str] | None = None) -> None:
        self.responses.append((status, body, headers or {}))

    async def handler(self, request: web.Request) -> web.StreamResponse:
        try:
            payload = await request.json()
        except ValueError:
            payload = None
        self.requests.append(Captured(request.path, dict(request.query), payload, time.monotonic()))
        status, body, headers = self.responses.popleft() if self.responses else self.default
        if status == 204 or body is None:
            return web.Response(status=status, headers=headers)
        if isinstance(body, str):
            return web.Response(status=status, text=body, content_type="text/html", headers=headers)
        return web.Response(status=status, text=json_dumps(body), content_type="application/json", headers=headers)

    def url(self, path: str) -> str:
        assert self.server is not None
        return str(self.server.make_url(path))


async def _start_fake(default: tuple[int, Any, dict[str, str]]) -> FakeAPI:
    fake = FakeAPI(default=default)
    app = web.Application()
    app.router.add_post("/{tail:.*}", fake.handler)
    fake.server = TestServer(app)
    await fake.server.start_server(access_log=None)  # the fake's access log would print the token itself
    return fake


@pytest.fixture
async def discord_api():
    fake = await _start_fake((200, {"id": "1290000000000000001", "channel_id": "42"}, {}))
    yield fake
    assert fake.server is not None
    await fake.server.close()


@pytest.fixture
async def telegram_api():
    fake = await _start_fake((200, {"ok": True, "result": {"message_id": 4242, "chat": {"id": -100123}}}, {}))
    yield fake
    assert fake.server is not None
    await fake.server.close()


@pytest.fixture
async def http():
    client = HttpClient.create(NetworkSettings(trust_env=False, retry=BackoffPolicy(max_attempts=1, base_delay=0, max_delay=0)))
    yield client
    await client.close()


def _discord_cfg(fake: FakeAPI | None = None, **overrides: Any) -> DiscordTarget:
    url = fake.url(f"/api/webhooks/1234567890/{DISCORD_TOKEN}") if fake else f"https://discord.com/api/webhooks/1/{DISCORD_TOKEN}"
    data: dict[str, Any] = {"webhook_url": SecretStr(url), "mention_role_id": "987654321098765432"}
    data.update(overrides)
    return DiscordTarget(**data)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# --------------------------------------------------------------------------- Discord: payload


def test_discord_payload_structure() -> None:
    alert = make_alert(severity=Severity.CRITICAL, is_price_error=True)
    cfg = _discord_cfg(avatar_url="https://example.com/avatar.png")
    payload = build_discord_payload(alert, cfg, mention=False)

    assert payload["username"] == "DealRadar"
    assert payload["avatar_url"] == "https://example.com/avatar.png"
    assert payload["content"] == ""
    assert payload["allowed_mentions"] == {"parse": []}
    embed = payload["embeds"][0]
    assert embed["title"] == escape_markdown(headline(alert))
    assert "PRICE ERROR" in embed["title"]
    assert embed["url"] == alert.item.best_url
    assert embed["color"] == SEVERITY_COLOR[Severity.CRITICAL]
    assert [f["name"] for f in embed["fields"]] == [f.name for f in facts(alert)]
    assert all(f["inline"] is True for f in embed["fields"])
    assert embed["fields"][0]["value"] == "$999.99 + $10 ship"
    assert embed["thumbnail"] == {"url": IMAGE}
    assert embed["footer"] == {"text": f"DealRadar · vm-1 · {alert.alert_id[:8]}"}
    assert datetime.fromisoformat(embed["timestamp"]) == T0
    assert "**RTX 4090** · fe · gpu" in embed["description"]
    assert "low\\_feedback 0.20" in embed["description"]
    assert "• market $1,818 (blend of 14 sales + config)" in embed["description"]

    rows = payload["components"]
    assert len(rows) == 1 and rows[0]["type"] == 1
    buttons = rows[0]["components"]
    assert all(b["type"] == 2 and b["style"] == 5 for b in buttons)
    assert [(b["label"], b["url"]) for b in buttons] == links(alert)


def test_discord_payload_node_id_override_and_markdown_escaping() -> None:
    item = make_item(title="**RTX 4090** _mint_ ~~$1500~~ `fe` @everyone", node_id=None)
    alert = make_alert(item)
    payload = build_discord_payload(alert, _discord_cfg(), mention=False, node_id="gcp-processor")
    embed = payload["embeds"][0]
    assert embed["footer"]["text"] == f"DealRadar · gcp-processor · {alert.alert_id[:8]}"
    assert "\\*\\*RTX 4090\\*\\* \\_mint\\_ \\~\\~$1500\\~\\~ \\`fe\\`" in embed["title"]
    # @everyone is inert because nothing may be parsed as a mention.
    assert payload["allowed_mentions"] == {"parse": []}


def test_discord_mentions() -> None:
    alert = make_alert(mention=True)
    role = "987654321098765432"

    pinged = build_discord_payload(alert, _discord_cfg(), mention=True)
    assert pinged["content"] == f"<@&{role}>"
    assert pinged["allowed_mentions"] == {"parse": [], "roles": [role]}

    quiet = build_discord_payload(alert, _discord_cfg(), mention=False)
    assert quiet["content"] == "" and quiet["allowed_mentions"] == {"parse": []}

    no_role = build_discord_payload(alert, _discord_cfg(mention_role_id=None), mention=True)
    assert no_role["content"] == "" and no_role["allowed_mentions"] == {"parse": []}

    bogus = build_discord_payload(alert, _discord_cfg(mention_role_id="@here"), mention=True)
    assert bogus["content"] == "" and bogus["allowed_mentions"] == {"parse": []}


def test_discord_no_link_buttons_when_disabled() -> None:
    payload = build_discord_payload(make_alert(), _discord_cfg(link_buttons=False), mention=False)
    assert "components" not in payload


def test_discord_payload_limits_with_absurd_input() -> None:
    long_url = "https://store.example.com/p/" + "x" * 3000
    item = make_item(
        title="RTX 4090 🚨 " + "A" * 20_000,
        retailer="R" * 5000,
        seller=SellerInfo(name="s" * 5000, feedback_score=1, feedback_pct=50.0),
        location=Location(text="L" * 5000, distance_miles=3),
        outbound_url=long_url,
        image_urls=["https://img.example.com/" + "i" * 3000],
    )
    alert = make_alert(
        item,
        mention=True,
        profile_name="P" * 10_000,
        explain=["E" * 3000 for _ in range(10)],
        risk=[RiskSignal(code="r" * 900, probability=0.5), RiskSignal(code="q" * 900, probability=0.4)],
        vision=VisionResult(verdict=VisionVerdict.GENUINE, confidence=0.9),
        update=True,
    )
    cfg = _discord_cfg(username="U" * 500)
    payload = build_discord_payload(alert, cfg, mention=True)

    assert len(payload["username"]) <= 80
    assert len(payload["content"]) <= 2000
    embed = payload["embeds"][0]
    assert 0 < len(embed["title"]) <= TITLE_LIMIT
    assert len(embed.get("description", "")) <= DESCRIPTION_LIMIT
    assert len(embed["fields"]) <= FIELDS_LIMIT
    for f in embed["fields"]:
        assert 0 < len(f["name"]) <= FIELD_NAME_LIMIT
        assert 0 < len(f["value"]) <= FIELD_VALUE_LIMIT
    assert len(embed["footer"]["text"]) <= FOOTER_LIMIT
    total = (
        len(embed["title"])
        + len(embed.get("description", ""))
        + len(embed["footer"]["text"])
        + sum(len(f["name"]) + len(f["value"]) for f in embed["fields"])
    )
    assert total <= EMBED_TOTAL_LIMIT
    assert embed_length(embed) <= EMBED_TOTAL_LIMIT
    # the description is sacrificed before any fact
    assert len(embed["description"]) < DESCRIPTION_LIMIT
    assert [f["name"] for f in embed["fields"]] == [f.name for f in facts(alert)]
    # URLs that Discord would reject are left out instead of failing the whole message
    assert "url" not in embed
    assert "thumbnail" not in embed
    for row in payload["components"]:
        assert len(row["components"]) <= 5
        for button in row["components"]:
            assert len(button["label"]) <= BUTTON_LABEL_LIMIT
            assert len(button["url"]) <= BUTTON_URL_LIMIT
            assert button["url"] != long_url


def test_discord_enforce_total_drops_trailing_fields_after_description() -> None:
    embed: dict[str, Any] = {
        "title": "t" * TITLE_LIMIT,
        "description": "d" * DESCRIPTION_LIMIT,
        "fields": [{"name": f"n{i}", "value": "v" * FIELD_VALUE_LIMIT, "inline": True} for i in range(FIELDS_LIMIT)],
        "footer": {"text": "f" * 100},
    }
    _enforce_total(embed)
    assert embed_length(embed) <= EMBED_TOTAL_LIMIT
    assert "description" not in embed
    assert embed["fields"][0]["name"] == "n0"
    assert len(embed["fields"]) < FIELDS_LIMIT


def test_discord_truncate_counts_utf16_units() -> None:
    assert truncate("abc", 3) == "abc"
    cut = truncate("🚨" * 10, 7)
    assert cut.endswith("…") and len(cut.encode("utf-16-le")) // 2 <= 7
    assert truncate("hello world", 0) == ""


def test_discord_webhook_url_query() -> None:
    url = webhook_url("https://discord.com/api/webhooks/1/tok?thread_id=1", with_components=True, thread_id="555")
    assert url == "https://discord.com/api/webhooks/1/tok?wait=true&with_components=true&thread_id=555"
    plain = webhook_url("https://discord.com/api/webhooks/1/tok", with_components=False, thread_id=None)
    assert plain == "https://discord.com/api/webhooks/1/tok?wait=true"


# --------------------------------------------------------------------------- Discord: delivery


async def test_discord_send_success(discord_api: FakeAPI, http: HttpClient) -> None:
    metrics = Metrics()
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api, thread_id="555"), http, timeout=3.0, metrics=metrics)
    assert dispatcher.target == "discord:gpu" and dispatcher.configured
    alert = make_alert(mention=True)
    result = await dispatcher.send(alert)

    assert result.ok and result.status == 200 and result.attempts == 1
    assert result.message_id == "1290000000000000001"
    assert result.target == "discord:gpu"
    [req] = discord_api.requests
    assert req.path == f"/api/webhooks/1234567890/{DISCORD_TOKEN}"
    assert req.query == {"wait": "true", "with_components": "true", "thread_id": "555"}
    assert req.json["content"] == "<@&987654321098765432>"
    assert req.json["embeds"][0]["title"] == escape_markdown(headline(alert))
    assert metrics.counter("discord_requests_total", "", ("target", "status")).value(target="discord:gpu", status=200) == 1


async def test_discord_204_without_body_is_ok(discord_api: FakeAPI, http: HttpClient) -> None:
    discord_api.queue(204)
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api, link_buttons=False), http, timeout=3.0)
    result = await dispatcher.send(make_alert())
    assert result.ok and result.status == 204 and result.message_id is None
    assert discord_api.requests[0].query == {"wait": "true"}


async def test_discord_429_retry_after_body_then_success(discord_api: FakeAPI, http: HttpClient) -> None:
    discord_api.queue(429, {"message": "You are being rate limited.", "retry_after": 0.15, "global": False}, {"Retry-After": "1"})
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api), http, timeout=3.0)
    result = await dispatcher.send(make_alert())

    assert result.ok and result.attempts == 2 and result.status == 200
    first, second = discord_api.requests
    # the float body hint (0.15 s) wins over the rounded header (1 s)
    assert 0.14 <= second.at - first.at < 0.9


async def test_discord_429_retry_after_header_only(discord_api: FakeAPI, http: HttpClient) -> None:
    discord_api.queue(429, "<html>cloudflare says slow down</html>", {"Retry-After": "0"})
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api), http, timeout=3.0)
    result = await dispatcher.send(make_alert())
    assert result.ok and result.attempts == 2


async def test_discord_429_beyond_budget_fails_fast_and_blocks_webhook(discord_api: FakeAPI, http: HttpClient) -> None:
    discord_api.queue(429, {"message": "You are being rate limited.", "retry_after": 30.0, "global": False})
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api), http, timeout=1.0)
    started = time.monotonic()
    result = await dispatcher.send(make_alert())
    assert not result.ok and result.status == 429 and result.attempts == 1
    assert "rate limited" in (result.error or "")
    assert time.monotonic() - started < 0.9
    assert dispatcher.blocked_for > 25

    again = await dispatcher.send(make_alert())
    assert not again.ok and "rate limited" in (again.error or "")
    assert len(discord_api.requests) == 1  # no request while the webhook bucket is exhausted


async def test_discord_bucket_exhausted_headers_delay_next_send(discord_api: FakeAPI, http: HttpClient) -> None:
    exhausted = {"X-RateLimit-Limit": "5", "X-RateLimit-Remaining": "0", "X-RateLimit-Reset-After": "0.3"}
    discord_api.queue(200, {"id": "1"}, exhausted)
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api), http, timeout=3.0)
    assert (await dispatcher.send(make_alert())).ok
    second = await dispatcher.send(make_alert())
    assert second.ok
    first_req, second_req = discord_api.requests
    assert second_req.at - first_req.at >= 0.28


@pytest.mark.parametrize("status", [401, 403, 404])
async def test_discord_dead_webhook_disables_target(discord_api: FakeAPI, http: HttpClient, status: int) -> None:
    discord_api.queue(status, {"message": "Unknown Webhook", "code": 10015})
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api), http, timeout=3.0)
    result = await dispatcher.send(make_alert())
    assert not result.ok and result.status == status and result.attempts == 1
    assert "Unknown Webhook" in (result.error or "")
    assert dispatcher.configured is False and dispatcher.disabled_reason

    again = await dispatcher.send(make_alert())
    assert not again.ok and (again.error or "").startswith("disabled")
    assert len(discord_api.requests) == 1


async def test_discord_400_retries_once_with_minimal_payload(discord_api: FakeAPI, http: HttpClient) -> None:
    discord_api.queue(400, {"message": "Invalid Form Body", "code": 50035, "errors": {"embeds": {"0": {"thumbnail": {}}}}})
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api), http, timeout=3.0)
    result = await dispatcher.send(make_alert())
    assert result.ok and result.attempts == 2
    first, second = discord_api.requests
    assert "components" in first.json and "thumbnail" in first.json["embeds"][0]
    assert "components" not in second.json
    assert "thumbnail" not in second.json["embeds"][0] and "url" not in second.json["embeds"][0]
    assert "with_components" not in second.query
    assert second.json["embeds"][0]["fields"] == first.json["embeds"][0]["fields"]


async def test_discord_400_twice_fails_without_more_retries(discord_api: FakeAPI, http: HttpClient) -> None:
    discord_api.queue(400, {"message": "Invalid Form Body", "code": 50035})
    discord_api.queue(400, {"message": "Invalid Form Body", "code": 50035})
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api), http, timeout=3.0)
    result = await dispatcher.send(make_alert())
    assert not result.ok and result.status == 400 and result.attempts == 2
    assert dispatcher.configured  # a bad payload is not a dead webhook


async def test_discord_server_error_is_retried(discord_api: FakeAPI, http: HttpClient) -> None:
    discord_api.queue(502, "<html>bad gateway</html>")
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api), http, timeout=3.0)
    result = await dispatcher.send(make_alert())
    assert result.ok and result.attempts == 2


async def test_discord_unconfigured_never_sends(http: HttpClient) -> None:
    dispatcher = DiscordDispatcher("gpu", DiscordTarget(), http, timeout=1.0)
    assert dispatcher.configured is False
    result = await dispatcher.send(make_alert())
    assert not result.ok and result.error == "not configured"


async def test_discord_connection_error_never_raises_and_redacts(http: HttpClient, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    url = f"http://127.0.0.1:{_free_port()}/api/webhooks/1/{DISCORD_TOKEN}"
    dispatcher = DiscordDispatcher("gpu", DiscordTarget(webhook_url=SecretStr(url)), http, timeout=1.0)
    result = await dispatcher.send(make_alert())
    assert not result.ok and result.status is None and result.attempts >= 1
    assert DISCORD_TOKEN not in (result.error or "")
    for record in caplog.records:
        assert DISCORD_TOKEN not in record.getMessage() and DISCORD_TOKEN not in str(record.__dict__)


async def test_discord_send_notice(discord_api: FakeAPI, http: HttpClient) -> None:
    dispatcher = DiscordDispatcher("ops", _discord_cfg(discord_api), http, timeout=3.0)
    result = await dispatcher.send_notice("Source blocked", "fb_marketplace hit a login wall @everyone")
    assert result.ok
    [req] = discord_api.requests
    assert req.query == {"wait": "true"}
    assert req.json["allowed_mentions"] == {"parse": []}
    assert "components" not in req.json
    embed = req.json["embeds"][0]
    assert embed["title"] == "Source blocked"
    assert embed["description"] == "fb_marketplace hit a login wall @everyone"
    assert embed["color"] == 0x607D8B and embed["footer"] == {"text": "DealRadar notice"}


# --------------------------------------------------------------------------- Telegram: formatting

_TAG = re.compile(r'<(/?)(b|i|a)((?: href="[^"<>]*")?)>')
_ENTITY = re.compile(r"&(?:lt|gt|amp|quot|#\d+|#x[0-9a-fA-F]+);")


def assert_wellformed(markup: str) -> None:
    """Only <b>/<i>/<a href> tags, balanced, and no bare &, < or >."""
    stack: list[str] = []
    for match in _TAG.finditer(markup):
        closing, name = match.group(1), match.group(2)
        if closing:
            assert stack and stack.pop() == name, f"unbalanced </{name}> in {markup!r}"
        else:
            stack.append(name)
    assert not stack, f"unclosed tags {stack} in {markup!r}"
    stripped = _ENTITY.sub("", _TAG.sub("", markup))
    assert "<" not in stripped and ">" not in stripped and "&" not in stripped, stripped


def test_telegram_message_html_escaping() -> None:
    item = make_item(
        title='RTX 4080 <Super> & "Ti" <script>alert(1)</script> 5 > 4',
        seller=SellerInfo(name="<b>bob</b> & co", feedback_score=3),
        url="https://www.ebay.com/itm/1?a=1&b=2",
    )
    alert = make_alert(item, risk=[RiskSignal(code="payment_red_flag", probability=0.4)])
    msg = build_telegram_message(alert, max_len=MESSAGE_LIMIT)

    assert_wellformed(msg)
    first_line = msg.split("\n", 1)[0]
    assert first_line.startswith("<b>") and first_line.endswith("</b>")
    assert '&lt;Super&gt; &amp; "Ti" &lt;script&gt;alert(1)&lt;/script&gt; 5 &gt; 4' in first_line
    assert "<script>" not in msg
    assert "<b>Seller:</b> &lt;b&gt;bob&lt;/b&gt; &amp; co (3)" in msg
    assert "⚠️ <b>Risk:</b> payment_red_flag 0.40" in msg
    assert '<a href="https://www.ebay.com/itm/1?a=1&amp;b=2">Open listing</a>' in msg
    for fact in facts(alert):
        assert f"<b>{fact.name}:</b>" in msg


@pytest.mark.parametrize("max_len", [3, 8, 9, 20, 64, 100, 200, 333, 512, 700, CAPTION_LIMIT, MESSAGE_LIMIT])
def test_telegram_message_budget_never_cuts_markup(max_len: int) -> None:
    item = make_item(title="&<>" * 150 + " RTX 🚨🚨 " + "Ω" * 400, seller=SellerInfo(name="&" * 300, feedback_score=1))
    alert = make_alert(item, risk=[RiskSignal(code="a&b<c>", probability=0.9)])
    msg = build_telegram_message(alert, max_len=max_len)
    assert text_length(msg) <= max_len
    assert_wellformed(msg)
    if max_len >= CAPTION_LIMIT:
        assert "<b>Price:</b>" in msg and "<b>Discount:</b>" in msg


def test_telegram_caption_keeps_key_facts_and_drops_low_priority_lines() -> None:
    alert = make_alert(make_item(title="RTX 4090 FE " + "word " * 120))
    caption = build_telegram_message(alert, max_len=CAPTION_LIMIT)
    full = build_telegram_message(alert, max_len=MESSAGE_LIMIT)
    assert text_length(caption) <= CAPTION_LIMIT
    for name in ("Price", "Market", "Discount", "Score"):
        assert f"<b>{name}:</b>" in caption
    assert len(full) >= len(caption)
    # display order is preserved even when lines are dropped
    positions = [caption.index(f"<b>{n}:</b>") for n in ("Price", "Market", "Discount", "Score")]
    assert positions == sorted(positions)


def test_telegram_fit_escaped_and_plain_variant() -> None:
    assert fit_escaped("a<b&c", 100) == "a&lt;b&amp;c"
    cut = fit_escaped("a<b&c", 9)
    assert cut == "a&lt;b…"
    assert fit_escaped("&&&&", 6) == "&amp;…"
    plain = build_plain_message(make_alert(make_item(title="RTX <4090> & co")), max_len=MESSAGE_LIMIT)
    assert plain.startswith(headline(make_alert(make_item(title="RTX <4090> & co"))))
    assert "<b>" not in plain and "&amp;" not in plain


def test_telegram_inline_keyboard_two_per_row() -> None:
    item = make_item(
        source="slickdeals",
        source_kind=SourceKind.AGGREGATOR,
        outbound_url="https://www.bestbuy.com/site/1.p",
        url="https://slickdeals.net/f/1",
    )
    alert = make_alert(item)
    keyboard = build_inline_keyboard(alert)
    assert [len(row) for row in keyboard] == [2, 2]
    buttons = [b for row in keyboard for b in row]
    assert [(b["text"], b["url"]) for b in buttons] == links(alert)
    assert buttons[0] == {"text": "Buy now", "url": "https://www.bestbuy.com/site/1.p"}


def test_telegram_silent_logic() -> None:
    chat = TelegramChat(chat_id="1", silent_below=Severity.HIGH)
    assert is_silent(make_alert(severity=Severity.MEDIUM), chat) is True
    assert is_silent(make_alert(severity=Severity.MEDIUM, mention=True), chat) is False
    assert is_silent(make_alert(severity=Severity.HIGH), chat) is False
    assert is_silent(make_alert(severity=Severity.CRITICAL), chat) is False
    loud = TelegramChat(chat_id="1", silent_below=Severity.MEDIUM)
    assert is_silent(make_alert(severity=Severity.MEDIUM), loud) is False
    quiet = TelegramChat(chat_id="1", silent_below=Severity.CRITICAL)
    assert is_silent(make_alert(severity=Severity.HIGH), quiet) is True


# --------------------------------------------------------------------------- Telegram: delivery


def _telegram(
    fake: FakeAPI | None, http: HttpClient, *, timeout: float = 3.0, token: str | None = TELEGRAM_TOKEN, **chat: Any
) -> TelegramDispatcher:
    data: dict[str, Any] = {"chat_id": "-100123"}
    data.update(chat)
    api_base = fake.url("/") if fake else f"http://127.0.0.1:{_free_port()}"
    return TelegramDispatcher(
        "main", TelegramChat(**data), SecretStr(token) if token else None, api_base, http, timeout=timeout, metrics=Metrics()
    )


async def test_telegram_send_photo_with_caption(telegram_api: FakeAPI, http: HttpClient) -> None:
    dispatcher = _telegram(telegram_api, http, message_thread_id=7)
    assert dispatcher.target == "telegram:main" and dispatcher.configured
    alert = make_alert(severity=Severity.MEDIUM)
    result = await dispatcher.send(alert)

    assert result.ok and result.message_id == "4242" and result.status == 200
    [req] = telegram_api.requests
    assert req.path == f"/bot{TELEGRAM_TOKEN}/sendPhoto"
    body = req.json
    assert body["chat_id"] == "-100123" and body["message_thread_id"] == 7
    assert body["photo"] == IMAGE and body["parse_mode"] == "HTML"
    assert text_length(body["caption"]) <= CAPTION_LIMIT
    assert_wellformed(body["caption"])
    assert body["disable_notification"] is True  # medium < silent_below(high)
    assert body["reply_markup"]["inline_keyboard"] == build_inline_keyboard(alert)


async def test_telegram_photo_400_falls_back_to_send_message(telegram_api: FakeAPI, http: HttpClient) -> None:
    bad_photo = "Bad Request: wrong file identifier/HTTP URL specified"
    telegram_api.queue(400, {"ok": False, "error_code": 400, "description": bad_photo})
    dispatcher = _telegram(telegram_api, http)
    alert = make_alert(severity=Severity.CRITICAL)
    result = await dispatcher.send(alert)

    assert result.ok and result.attempts == 2
    first, second = telegram_api.requests
    assert first.path.endswith("/sendPhoto") and second.path.endswith("/sendMessage")
    assert second.json["text"] == build_telegram_message(alert, max_len=MESSAGE_LIMIT)
    assert second.json["parse_mode"] == "HTML"
    assert second.json["link_preview_options"] == {"is_disabled": False, "url": alert.item.best_url, "prefer_small_media": True}
    assert second.json["disable_notification"] is False
    assert dispatcher.configured


async def test_telegram_send_message_without_photo(telegram_api: FakeAPI, http: HttpClient) -> None:
    dispatcher = _telegram(telegram_api, http, send_photos=False)
    result = await dispatcher.send(make_alert(make_item(image_urls=[])))
    assert result.ok
    [req] = telegram_api.requests
    assert req.path.endswith("/sendMessage") and "message_thread_id" not in req.json
    assert text_length(req.json["text"]) <= MESSAGE_LIMIT


async def test_telegram_silent_flag_in_payload(telegram_api: FakeAPI, http: HttpClient) -> None:
    dispatcher = _telegram(telegram_api, http, send_photos=False)
    await dispatcher.send(make_alert(severity=Severity.MEDIUM))
    await dispatcher.send(make_alert(severity=Severity.MEDIUM, mention=True))
    await dispatcher.send(make_alert(severity=Severity.HIGH))
    assert [r.json["disable_notification"] for r in telegram_api.requests] == [True, False, False]


async def test_telegram_429_retry_after_then_success(telegram_api: FakeAPI, http: HttpClient) -> None:
    telegram_api.queue(
        429, {"ok": False, "error_code": 429, "description": "Too Many Requests: retry after 1", "parameters": {"retry_after": 1}}
    )
    dispatcher = _telegram(telegram_api, http, send_photos=False)
    result = await dispatcher.send(make_alert())
    assert result.ok and result.attempts == 2
    first, second = telegram_api.requests
    assert second.at - first.at >= 0.95
    assert first.path == second.path


async def test_telegram_429_beyond_budget_fails_fast(telegram_api: FakeAPI, http: HttpClient) -> None:
    telegram_api.queue(
        429,
        {"ok": False, "error_code": 429, "description": "Too Many Requests: retry after 40", "parameters": {"retry_after": 40}},
    )
    dispatcher = _telegram(telegram_api, http, timeout=1.0, send_photos=False)
    started = time.monotonic()
    result = await dispatcher.send(make_alert())
    assert not result.ok and result.status == 429 and "rate limited" in (result.error or "")
    assert time.monotonic() - started < 0.9
    assert len(telegram_api.requests) == 1


async def test_telegram_parse_error_falls_back_to_plain_text(telegram_api: FakeAPI, http: HttpClient) -> None:
    parse_error = "Bad Request: can't parse entities: unsupported start tag at byte offset 12"
    telegram_api.queue(400, {"ok": False, "error_code": 400, "description": parse_error})
    dispatcher = _telegram(telegram_api, http, send_photos=False)
    alert = make_alert()
    result = await dispatcher.send(alert)
    assert result.ok and result.attempts == 2
    plain = telegram_api.requests[1].json
    assert "parse_mode" not in plain and "reply_markup" not in plain
    assert plain["text"] == build_plain_message(alert, max_len=MESSAGE_LIMIT)


async def test_telegram_follows_supergroup_migration(telegram_api: FakeAPI, http: HttpClient) -> None:
    telegram_api.queue(
        400,
        {
            "ok": False,
            "error_code": 400,
            "description": "Bad Request: group chat was upgraded to a supergroup chat",
            "parameters": {"migrate_to_chat_id": -1009876543210},
        },
    )
    dispatcher = _telegram(telegram_api, http, send_photos=False)
    result = await dispatcher.send(make_alert())
    assert result.ok and result.attempts == 2
    assert telegram_api.requests[1].json["chat_id"] == "-1009876543210"
    assert dispatcher.chat_id == "-1009876543210"
    assert telegram_api.requests[1].path.endswith("/sendMessage")


@pytest.mark.parametrize(
    ("status", "description"),
    [
        (401, "Unauthorized"),
        (403, "Forbidden: bot was blocked by the user"),
        (404, "Not Found"),
        (400, "Bad Request: chat not found"),
    ],
)
async def test_telegram_dead_target_disabled(telegram_api: FakeAPI, http: HttpClient, status: int, description: str) -> None:
    telegram_api.queue(status, {"ok": False, "error_code": status, "description": description})
    dispatcher = _telegram(telegram_api, http)
    result = await dispatcher.send(make_alert())
    assert not result.ok and result.status == status and result.attempts == 1
    assert dispatcher.configured is False
    again = await dispatcher.send(make_alert())
    assert not again.ok and (again.error or "").startswith("disabled")
    assert len(telegram_api.requests) == 1


async def test_telegram_unconfigured(http: HttpClient) -> None:
    no_token = _telegram(None, http, token=None)
    assert no_token.configured is False
    result = await no_token.send(make_alert())
    assert not result.ok and result.error == "not configured"
    no_chat = TelegramDispatcher("main", TelegramChat(), SecretStr(TELEGRAM_TOKEN), "https://api.telegram.org", http, timeout=1.0)
    assert no_chat.configured is False
    assert (await no_chat.send(make_alert())).error == "not configured"


async def test_telegram_send_notice(telegram_api: FakeAPI, http: HttpClient) -> None:
    dispatcher = _telegram(telegram_api, http)
    result = await dispatcher.send_notice("Source <blocked>", "fb & co")
    assert result.ok
    [req] = telegram_api.requests
    assert req.path.endswith("/sendMessage")
    assert req.json["text"] == "<b>Source &lt;blocked&gt;</b>\nfb &amp; co"
    assert req.json["disable_notification"] is False


async def test_telegram_token_never_logged(telegram_api: FakeAPI, http: HttpClient, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    results = []
    # server errors until the budget runs out
    for _ in range(6):
        telegram_api.queue(500, {"ok": False, "error_code": 500, "description": "Internal Server Error"})
    results.append(await _telegram(telegram_api, http, timeout=1.0, send_photos=False).send(make_alert()))
    # bad request on every step
    for _ in range(3):
        telegram_api.queue(400, {"ok": False, "error_code": 400, "description": "Bad Request: something odd"})
    results.append(await _telegram(telegram_api, http).send(make_alert()))
    # unauthorized -> disabled
    telegram_api.queue(401, {"ok": False, "error_code": 401, "description": "Unauthorized"})
    results.append(await _telegram(telegram_api, http).send(make_alert()))
    # connection refused (nothing listens on the port)
    results.append(await _telegram(None, http, timeout=1.0).send(make_alert()))
    # non-JSON garbage
    telegram_api.queue(502, "<html>bad gateway</html>")
    telegram_api.queue(400, "<html>nope</html>")
    telegram_api.queue(400, "<html>nope</html>")
    telegram_api.queue(400, "<html>nope</html>")
    results.append(await _telegram(telegram_api, http).send(make_alert()))

    assert not any(r.ok for r in results)
    for r in results:
        assert TELEGRAM_TOKEN not in (r.error or "")
    assert caplog.records, "expected delivery failures to be logged"
    for record in caplog.records:
        assert TELEGRAM_TOKEN not in record.getMessage()
        assert TELEGRAM_TOKEN not in str(record.__dict__)
    assert TELEGRAM_TOKEN not in caplog.text


# --------------------------------------------------------------------------- WebSocket hub


@pytest.fixture
async def ws_env():
    servers: list[TestServer] = []
    hubs: list[WebSocketHub] = []
    session = aiohttp.ClientSession()

    async def make(**kwargs: Any) -> tuple[WebSocketHub, TestServer]:
        cfg = WebSocketSection(**{"path": "/ws", "max_clients": 5, "recent_buffer": 5, **kwargs.pop("cfg", {})})
        hub = WebSocketHub(cfg, **kwargs)
        app = web.Application()
        hub.attach(app)
        server = TestServer(app)
        await server.start_server(access_log=None)
        servers.append(server)
        hubs.append(hub)
        return hub, server

    yield make, session
    await session.close()
    for hub in hubs:
        await hub.close()
    for server in servers:
        await server.close()


async def _wait_for(predicate: Any, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


async def test_ws_hello_replay_then_live(ws_env: Any) -> None:
    make, session = ws_env
    hub, server = await make()
    history = [make_alert() for _ in range(3)]
    for alert in history:
        result = await hub.send(alert)
        assert result.ok and result.message_id is None and result.target == "websocket"
    assert hub.buffered == 3

    async with session.ws_connect(server.make_url("/ws?replay=2")) as ws:
        hello = await ws.receive_json(timeout=2)
        assert hello["type"] == "hello" and hello["replay"] == 2 and hello["buffered"] == 3 and hello["protocol"] == 1
        replayed = [await ws.receive_json(timeout=2) for _ in range(2)]
        assert [m["alert"]["alert_id"] for m in replayed] == [history[1].alert_id, history[2].alert_id]
        assert all(m["type"] == "alert" and m["replay"] is True for m in replayed)

        live = make_alert(severity=Severity.CRITICAL)
        result = await hub.send(live)
        assert result.ok
        message = await ws.receive_json(timeout=2)
        assert message["type"] == "alert" and "replay" not in message
        assert message["alert"] == live.model_dump(mode="json")
        assert hub.client_count == 1
    await _wait_for(lambda: hub.client_count == 0)


async def test_ws_default_replay_is_whole_buffer_and_zero_disables(ws_env: Any) -> None:
    make, session = ws_env
    hub, server = await make(cfg={"recent_buffer": 3})
    alerts = [make_alert() for _ in range(5)]
    for alert in alerts:
        await hub.send(alert)
    assert hub.buffered == 3

    async with session.ws_connect(server.make_url("/ws")) as ws:
        hello = await ws.receive_json(timeout=2)
        assert hello["replay"] == 3
        ids = [(await ws.receive_json(timeout=2))["alert"]["alert_id"] for _ in range(3)]
        assert ids == [a.alert_id for a in alerts[2:]]

    async with session.ws_connect(server.make_url("/ws?replay=0")) as ws:
        hello = await ws.receive_json(timeout=2)
        assert hello["replay"] == 0
        await ws.send_str('{"type":"ping"}')
        assert (await ws.receive_json(timeout=2))["type"] == "pong"  # nothing replayed before the pong

    async with session.get(server.make_url("/ws?replay=abc"), headers={"Connection": "Upgrade"}) as resp:
        assert resp.status == 400


async def test_ws_auth_required_when_token_configured(ws_env: Any) -> None:
    make, session = ws_env
    hub, server = await make(auth_token=SecretStr("s3cret-token"))

    for url, headers in (("/ws", {}), ("/ws?token=wrong", {}), ("/ws", {"Authorization": "Bearer nope"})):
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            await session.ws_connect(server.make_url(url), headers=headers)
        assert exc.value.status == 401

    async with session.ws_connect(server.make_url("/ws?token=s3cret-token")) as ws:
        assert (await ws.receive_json(timeout=2))["type"] == "hello"
    async with session.ws_connect(server.make_url("/ws"), headers={"Authorization": "Bearer s3cret-token"}) as ws:
        assert (await ws.receive_json(timeout=2))["type"] == "hello"


async def test_ws_max_clients_enforced(ws_env: Any) -> None:
    make, session = ws_env
    hub, server = await make(cfg={"max_clients": 1})
    first = await session.ws_connect(server.make_url("/ws"))
    assert (await first.receive_json(timeout=2))["type"] == "hello"
    with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
        await session.ws_connect(server.make_url("/ws"))
    assert exc.value.status == 503
    await first.close()
    await _wait_for(lambda: hub.client_count == 0)
    async with session.ws_connect(server.make_url("/ws")) as again:
        assert (await again.receive_json(timeout=2))["type"] == "hello"


async def test_ws_plain_http_request_rejected(ws_env: Any) -> None:
    make, session = ws_env
    _, server = await make()
    async with session.get(server.make_url("/ws")) as resp:
        assert resp.status == 400
        assert (await resp.json())["error"] == "websocket upgrade required"


class _StuckSocket:
    """A peer whose socket buffer is full: writes never complete."""

    def __init__(self) -> None:
        self.closed = False
        self.close_codes: list[int] = []

    async def send_str(self, data: str) -> None:
        await asyncio.sleep(3600)

    async def close(self, *, code: int, message: bytes) -> bool:
        self.close_codes.append(code)
        self.closed = True
        return True


class _ClosedSocket:
    closed = True

    async def send_str(self, data: str) -> None:  # pragma: no cover - must never be called
        raise AssertionError("write to a closed socket")

    async def close(self, *, code: int, message: bytes) -> bool:  # pragma: no cover
        raise AssertionError("closing an already closed socket")


async def test_ws_slow_and_closed_clients_dropped_without_blocking(ws_env: Any) -> None:
    make, session = ws_env
    hub, server = await make(send_timeout=0.2)
    async with session.ws_connect(server.make_url("/ws")) as ws:
        assert (await ws.receive_json(timeout=2))["type"] == "hello"
        await _wait_for(lambda: all(c.backlog is None for c in hub._clients))
        stuck = _Client(ws=_StuckSocket(), peer="stuck", backlog=None)
        gone = _Client(ws=_ClosedSocket(), peer="gone", backlog=None)
        hub._clients.update({stuck, gone})
        assert hub.client_count == 3

        alert = make_alert()
        started = time.monotonic()
        result = await hub.send(alert)
        elapsed = time.monotonic() - started
        assert result.ok and elapsed < 1.0
        assert stuck not in hub._clients and gone not in hub._clients
        assert hub.client_count == 1
        message = await ws.receive_json(timeout=2)
        assert message["alert"]["alert_id"] == alert.alert_id

        await _wait_for(lambda: stuck.ws.close_codes == [WSCloseCode.POLICY_VIOLATION])
        dropped = hub.metrics.counter("ws_dropped_total", "", ("reason",))
        assert dropped.value(reason="slow") == 1 and dropped.value(reason="closed") == 1


async def test_ws_live_alerts_queued_during_replay_arrive_in_order(ws_env: Any) -> None:
    make, session = ws_env
    hub, server = await make()
    old = make_alert()
    await hub.send(old)
    async with session.ws_connect(server.make_url("/ws")) as ws:
        # Fire live alerts immediately; they may land while the replay is being written.
        live = [make_alert() for _ in range(3)]
        for alert in live:
            await hub.send(alert)
        frames = [await ws.receive_json(timeout=2) for _ in range(5)]
    assert frames[0]["type"] == "hello"
    assert [f["alert"]["alert_id"] for f in frames[1:]] == [old.alert_id, *(a.alert_id for a in live)]


async def test_ws_send_without_clients_is_ok_and_buffer_capped(ws_env: Any) -> None:
    make, _ = ws_env
    hub, _ = await make(cfg={"recent_buffer": 2})
    for _ in range(4):
        result = await hub.send(make_alert())
        assert result.ok and result.message_id is None and result.error is None
    assert hub.buffered == 2 and hub.client_count == 0
    assert hub.configured
    assert (await hub.send_notice("t", "m")).ok


async def test_ws_notice_broadcast(ws_env: Any) -> None:
    make, session = ws_env
    hub, server = await make()
    async with session.ws_connect(server.make_url("/ws")) as ws:
        await ws.receive_json(timeout=2)
        await _wait_for(lambda: all(c.backlog is None for c in hub._clients))
        await hub.send_notice("Source blocked", "fb_marketplace checkpoint")
        notice = await ws.receive_json(timeout=2)
        assert notice["type"] == "notice" and notice["title"] == "Source blocked"
    assert hub.buffered == 0  # notices are not replayed


async def test_ws_close_sends_going_away_and_refuses_new_clients(ws_env: Any) -> None:
    make, session = ws_env
    hub, server = await make()
    ws = await session.ws_connect(server.make_url("/ws"))
    assert (await ws.receive_json(timeout=2))["type"] == "hello"
    closer = asyncio.create_task(hub.close())
    message = await ws.receive(timeout=2)
    await asyncio.wait_for(closer, 3)
    assert message.type is WSMsgType.CLOSE and message.data == WSCloseCode.GOING_AWAY
    assert ws.close_code == WSCloseCode.GOING_AWAY
    assert hub.client_count == 0
    with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
        await session.ws_connect(server.make_url("/ws"))
    assert exc.value.status == 503


# --------------------------------------------------------------------------- regression tests (adversarial review)


class _ScriptedHttp:
    """Wraps a real HttpClient and raises scripted exceptions before delegating.

    Each entry is a factory ``url -> exception`` so a test can make the exception
    message quote the (secret-bearing) request URL, as some aiohttp errors do.
    """

    def __init__(self, inner: HttpClient, *errors: Any) -> None:
        self.inner = inner
        self.errors = deque(errors)
        self.calls = 0

    async def post_json(self, url: str, payload: Any, **kwargs: Any) -> Any:
        self.calls += 1
        if self.errors:
            raise self.errors.popleft()(url)
        return await self.inner.post_json(url, payload, **kwargs)


def _assert_no_secret(secret: str, result_error: str | None, caplog: pytest.LogCaptureFixture) -> None:
    assert secret not in (result_error or "")
    for record in caplog.records:
        assert secret not in record.getMessage() and secret not in str(record.__dict__)


@pytest.mark.parametrize(
    "url",
    [
        f"https://[discord.com/api/webhooks/1/{DISCORD_TOKEN}",  # urlsplit raises ValueError
        f"not a url at all {DISCORD_TOKEN}",  # yarl percent-encodes it into the aiohttp error
        f"ftp://discord.com/api/webhooks/1/{DISCORD_TOKEN}",
    ],
)
async def test_discord_malformed_webhook_url_disables_target_without_leaking(
    http: HttpClient, caplog: pytest.LogCaptureFixture, url: str
) -> None:
    caplog.set_level(logging.DEBUG)
    dispatcher = DiscordDispatcher("gpu", DiscordTarget(webhook_url=SecretStr(url)), http, timeout=1.0)
    assert dispatcher.configured is False  # the router skips it instead of failing every alert
    result = await dispatcher.send(make_alert())  # must not raise
    assert not result.ok and (result.error or "").startswith("disabled: invalid webhook URL")
    notice = await dispatcher.send_notice("t", "m")
    assert not notice.ok
    _assert_no_secret(DISCORD_TOKEN, result.error, caplog)


async def test_discord_unexpected_error_is_redacted(
    http: HttpClient, discord_api: FakeAPI, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    scripted = _ScriptedHttp(http, lambda url: RuntimeError(f"boom while posting to {url}"))
    dispatcher = DiscordDispatcher("gpu", _discord_cfg(discord_api), scripted, timeout=2.0)  # type: ignore[arg-type]
    result = await dispatcher.send(make_alert())
    assert not result.ok and "unexpected error" in (result.error or "") and result.attempts == 1
    assert "<webhook>" in (result.error or "")
    _assert_no_secret(DISCORD_TOKEN, result.error, caplog)
    crashed = [r for r in caplog.records if r.getMessage() == "discord send crashed"]
    assert crashed and "RuntimeError" in crashed[0].__dict__["traceback"] and crashed[0].exc_info is None
    assert discord_api.requests == []


def test_discord_redact_catches_normalised_urls() -> None:
    url = f"https://discord.com/api/webhooks/1234567890/{DISCORD_TOKEN}"
    dispatcher = DiscordDispatcher("gpu", DiscordTarget(webhook_url=SecretStr(url)), None, timeout=1.0)  # type: ignore[arg-type]
    for leaked in (
        f"POST https://DISCORD.COM/api/webhooks/1234567890/{DISCORD_TOKEN}?wait=true failed",  # host case changed
        f"see /api/webhooks/999/{DISCORD_TOKEN[:-4]}zzzz",  # some other webhook's token
    ):
        redacted = dispatcher._redact(leaked)
        assert DISCORD_TOKEN not in redacted and DISCORD_TOKEN[:-4] not in redacted
        assert "/webhooks/" in redacted and "<redacted>" in redacted


_CONNECT_TIMEOUT = getattr(aiohttp, "ConnectionTimeoutError", None)
_READ_TIMEOUT = getattr(aiohttp, "SocketTimeoutError", None)


@pytest.mark.skipif(_CONNECT_TIMEOUT is None, reason="aiohttp < 3.10 has no ConnectionTimeoutError")
async def test_connect_timeout_is_retried_but_read_timeout_is_not(
    http: HttpClient, discord_api: FakeAPI, telegram_api: FakeAPI
) -> None:
    assert _CONNECT_TIMEOUT is not None and _READ_TIMEOUT is not None
    # Connect timeout: nothing was sent, so a retry cannot duplicate the message.
    scripted = _ScriptedHttp(http, lambda url: _CONNECT_TIMEOUT("Connection timeout to host"))
    discord = DiscordDispatcher("gpu", _discord_cfg(discord_api), scripted, timeout=3.0)  # type: ignore[arg-type]
    result = await discord.send(make_alert())
    assert result.ok and result.attempts == 2 and len(discord_api.requests) == 1

    scripted = _ScriptedHttp(http, lambda url: _CONNECT_TIMEOUT("Connection timeout to host"))
    telegram = TelegramDispatcher(
        "main", TelegramChat(chat_id="1", send_photos=False), SecretStr(TELEGRAM_TOKEN), telegram_api.url("/"),
        scripted, timeout=3.0,  # type: ignore[arg-type]
    )
    result = await telegram.send(make_alert())
    assert result.ok and result.attempts == 2 and len(telegram_api.requests) == 1

    # Read timeout: the message may already be posted, so no retry (no duplicate alert).
    scripted = _ScriptedHttp(http, lambda url: _READ_TIMEOUT("Timeout on reading data from socket"))
    discord = DiscordDispatcher("gpu", _discord_cfg(discord_api), scripted, timeout=3.0)  # type: ignore[arg-type]
    result = await discord.send(make_alert())
    assert not result.ok and result.attempts == 1 and scripted.calls == 1


async def test_tiny_timeout_still_attempts_delivery(discord_api: FakeAPI, telegram_api: FakeAPI, http: HttpClient) -> None:
    discord = DiscordDispatcher("gpu", _discord_cfg(discord_api), http, timeout=0.2)
    assert (await discord.send(make_alert())).ok
    telegram = _telegram(telegram_api, http, timeout=0.2, send_photos=False)
    assert (await telegram.send(make_alert())).ok


def test_scrub_surrogates_identity_and_replacement() -> None:
    clean = {"a": ["x", "ü🚨"], "b": 1}
    assert scrub_surrogates(clean) is clean
    dirty = {"a": ["x", "RTX \ud83d 4090"], "k\udc00": "v"}
    scrubbed = scrub_surrogates(dirty)
    assert scrubbed == {"a": ["x", "RTX \ufffd 4090"], "k\ufffd": "v"}
    alert = make_alert()
    assert scrub_alert(alert) is alert


async def test_lone_surrogates_are_delivered_on_every_channel(
    discord_api: FakeAPI, telegram_api: FakeAPI, http: HttpClient, ws_env: Any
) -> None:
    # A Playwright-scraped title cut in the middle of an emoji keeps half a surrogate pair.
    item = make_item(title="RTX 4090 \ud83d FE", seller=SellerInfo(name="bob\udc00", feedback_score=3))
    alert = make_alert(item)

    discord = await DiscordDispatcher("gpu", _discord_cfg(discord_api), http, timeout=3.0).send(alert)
    assert discord.ok, discord.error
    assert "RTX 4090 \ufffd FE" in discord_api.requests[0].json["embeds"][0]["title"]

    telegram = await _telegram(telegram_api, http, send_photos=False).send(alert)
    assert telegram.ok, telegram.error
    assert "\ufffd" in telegram_api.requests[0].json["text"]
    assert (await _telegram(telegram_api, http).send_notice("blocked \ud83d", "x")).ok

    make, session = ws_env
    hub, server = await make()
    async with session.ws_connect(server.make_url("/ws?replay=0")) as ws:
        assert (await ws.receive_json(timeout=2))["type"] == "hello"
        await _wait_for(lambda: all(c.backlog is None for c in hub._clients))
        result = await hub.send(alert)
        assert result.ok and hub.buffered == 1
        frame = await ws.receive_json(timeout=2)
        assert frame["alert"]["item"]["title"] == "RTX 4090 \ufffd FE"
        assert frame["alert"]["alert_id"] == alert.alert_id
        assert (await hub.send_notice("t \ud83d", "m")).ok


def test_discord_block_markdown_in_listing_text_is_escaped() -> None:
    item = make_item(
        seller=SellerInfo(name="# HUGE HEADER"),
        location=Location(text="> quoted\n- bullet\n1. first\n-# subtext"),
    )
    embed = build_discord_payload(make_alert(item), _discord_cfg(), mention=False)["embeds"][0]
    fields = {f["name"]: f["value"] for f in embed["fields"]}
    assert fields["Seller"] == "\\# HUGE HEADER"
    assert fields["Location"].split("\n") == ["\\> quoted", "\\- bullet", "1\\. first", "\\-# subtext"]
    # Markers that Discord would not render as block syntax are left alone.
    assert escape_markdown("-5% off") == "-5% off" and escape_markdown("#1 deal") == "#1 deal"


def test_discord_whitespace_only_values_and_bad_item_urls() -> None:
    item = make_item(
        location=Location(text="   "),
        image_urls=["https://[broken/img.jpg"],
        outbound_url="https://[broken/deal",
    )
    alert = make_alert(item)
    payload = build_discord_payload(alert, _discord_cfg(), mention=False)  # must not raise
    embed = payload["embeds"][0]
    location = next(f for f in embed["fields"] if f["name"] == "Location")
    assert location["value"] == ZWSP  # Discord rejects blank field values with a 400
    assert "thumbnail" not in embed and "url" not in embed
    assert all("[broken" not in b["url"] for row in payload["components"] for b in row["components"])
    message = build_telegram_message(alert, max_len=MESSAGE_LIMIT)  # must not raise either
    assert_wellformed(message)
    assert all("[broken" not in b["url"] for row in build_inline_keyboard(alert) for b in row)


def test_discord_refused_username_falls_back_to_webhook_name() -> None:
    cfg = _discord_cfg(username="Discord Deals", avatar_url="https://example.com/a.png")
    payload = build_discord_payload(make_alert(), cfg, mention=False)
    assert "username" not in payload and payload["avatar_url"] == "https://example.com/a.png"
    assert build_discord_payload(make_alert(), _discord_cfg(username="Clyde"), mention=False).get("username") is None
    full = build_discord_payload(make_alert(), _discord_cfg(avatar_url="https://example.com/a.png"), mention=True)
    degraded = minimal_payload(full)
    assert "username" not in degraded and "avatar_url" not in degraded and "components" not in degraded
    assert degraded["content"] == "<@&987654321098765432>"


async def test_ws_handshake_completing_during_close_is_closed(ws_env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    make, session = ws_env
    hub, server = await make()
    original = web.WebSocketResponse.prepare

    async def prepare_then_shutdown(self: web.WebSocketResponse, request: web.Request) -> Any:
        prepared = await original(self, request)
        await hub.close()  # shutdown lands while this handshake is in flight
        return prepared

    monkeypatch.setattr(web.WebSocketResponse, "prepare", prepare_then_shutdown)
    async with session.ws_connect(server.make_url("/ws")) as ws:
        message = await ws.receive(timeout=2)
        assert message.type is WSMsgType.CLOSE and message.data == WSCloseCode.GOING_AWAY
    assert hub.client_count == 0
    assert hub.metrics.counter("ws_rejected_total", "", ("reason",)).value(reason="shutting_down") == 1
    await hub.close()  # idempotent


async def test_secrets_padded_with_whitespace_still_work(discord_api: FakeAPI, telegram_api: FakeAPI, http: HttpClient) -> None:
    # Docker secrets and .env files commonly leave a trailing newline on the value.
    webhook = SecretStr(discord_api.url(f"/api/webhooks/1234567890/{DISCORD_TOKEN}") + "\n")
    discord = DiscordDispatcher("gpu", DiscordTarget(webhook_url=webhook, thread_id=" 555 "), http, timeout=3.0)
    assert discord.configured
    assert (await discord.send(make_alert())).ok
    [sent] = discord_api.requests
    assert sent.path == f"/api/webhooks/1234567890/{DISCORD_TOKEN}" and sent.query["thread_id"] == "555"

    chat = TelegramChat(chat_id=" -100123\n", send_photos=False)
    telegram = TelegramDispatcher("main", chat, SecretStr(f" {TELEGRAM_TOKEN}\n"), telegram_api.url("/"), http, timeout=3.0)
    assert (await telegram.send(make_alert())).ok
    [sent] = telegram_api.requests
    assert sent.path == f"/bot{TELEGRAM_TOKEN}/sendMessage" and sent.json["chat_id"] == "-100123"
