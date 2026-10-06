"""Discord webhook channel.

Design decisions
----------------
* **One embed, every limit enforced locally.** Discord rejects the *whole* message
  with a 400 when any embed limit is exceeded (title 256, description 4096, 25 fields,
  field name 256 / value 1024, footer 2048, 6000 characters across the embed), and a
  rejected alert is a lost deal. :func:`build_discord_payload` therefore truncates
  every piece and then enforces the 6000 total by shrinking the description first
  (explanations are the least important text), then the trailing fields. Lengths are
  measured in UTF-16 code units, which is never smaller than Discord's own count, so
  emoji-heavy titles cannot slip over a limit.
* **Identical facts on every channel.** Title, fields and buttons come from the shared
  ``headline()`` / ``facts()`` / ``links()`` helpers in :mod:`dispatchers.base`.
* **No accidental pings.** ``allowed_mentions.parse`` is always empty; a role is pinged
  only when the router asked for a mention *and* ``mention_role_id`` is a snowflake,
  and then only that role is whitelisted. Listing titles containing ``@everyone``
  therefore render as text.
* **Link buttons** are sent as one action row (type 1) of link buttons (type 2,
  style 5). Non-application webhooks only honour components with
  ``?with_components=true``, which is added when ``link_buttons`` is enabled.
* **Own retry loop instead of HttpClient retries.** Discord's 429 carries
  ``retry_after`` (float seconds) in the JSON body, which the generic client cannot
  see, so requests go out with ``retry=False`` and every status is inspected here. The
  loop is bounded by the dispatcher's ``timeout`` budget: a server hint that does not
  fit into the remaining budget fails fast instead of stalling the alert fan-out.
* **Per-webhook rate-limit state.** ``X-RateLimit-Remaining: 0`` plus
  ``X-RateLimit-Reset-After`` (and every 429) block the *next* send on this webhook
  until the bucket resets, so a burst of alerts queues politely instead of collecting
  429s. Discord bans IPs that produce 10,000 invalid (401/403/429) requests in 10
  minutes, hence also:
* **Dead webhooks are disabled.** 401/403/404 mean the webhook was deleted or its
  token is wrong; the dispatcher flips ``configured`` to ``False`` (the router then
  skips it) and stops sending until the process restarts with a fixed config.
* **Graceful degradation.** A 400 (e.g. an image URL Discord refuses) is retried once
  with a minimal payload: no buttons, no thumbnail, no embed URL.
* **Secrets.** The webhook URL *is* the credential; it is never logged and is
  redacted from every error string.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import aiohttp

from deal_radar.config_schema import DiscordTarget
from deal_radar.core.backoff import parse_retry_after
from deal_radar.core.http import HttpClient, json_loads
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.dispatchers.base import SEVERITY_COLOR, Dispatcher, facts, headline, links, risk_summary
from deal_radar.engine.types import Alert, DispatchResult, utcnow

log = get_logger("dispatch.discord")

# --------------------------------------------------------------------------- Discord limits

CONTENT_LIMIT = 2000
USERNAME_LIMIT = 80
TITLE_LIMIT = 256
DESCRIPTION_LIMIT = 4096
FIELDS_LIMIT = 25
FIELD_NAME_LIMIT = 256
FIELD_VALUE_LIMIT = 1024
FOOTER_LIMIT = 2048
AUTHOR_LIMIT = 256
EMBED_TOTAL_LIMIT = 6000
BUTTON_LABEL_LIMIT = 80
BUTTON_URL_LIMIT = 512
BUTTONS_PER_ROW = 5
ACTION_ROWS_LIMIT = 5
URL_LIMIT = 2048

EXPLAIN_LINES = 6  # how many ScoreResult.explain lines make it into the description
MIN_FIELD_VALUE = 16  # below this a truncated field is useless; drop it instead
NOTICE_COLOR = 0x607D8B
ZWSP = "​"  # Discord rejects empty field names/values

# --------------------------------------------------------------------------- delivery policy

ALL_STATUSES = range(100, 600)  # inspect every status ourselves (see module docstring)
OK_STATUSES = frozenset({200, 204})
DISABLE_STATUSES = frozenset({401, 403, 404})
RETRY_STATUSES = frozenset({500, 502, 503, 504, 520, 521, 522, 523, 524})
MAX_ATTEMPTS = 5
MIN_ATTEMPT_SECONDS = 0.25  # never start a request with less budget than this
DEFAULT_RETRY_AFTER = 1.0  # 429 without any hint (e.g. Cloudflare HTML page)
SERVER_ERROR_BASE_DELAY = 0.25


# --------------------------------------------------------------------------- text helpers


def text_length(text: str) -> int:
    """Length in UTF-16 code units (>= code points; conservative for every Discord limit)."""
    return len(text) + sum(1 for ch in text if ord(ch) > 0xFFFF)


def truncate(text: str, limit: int, ellipsis: str = "…") -> str:
    """Cut ``text`` so that its UTF-16 length is <= ``limit`` (ellipsis included)."""
    if text_length(text) <= limit:
        return text
    if limit <= 0:
        return ""
    budget = limit - text_length(ellipsis)
    if budget <= 0:
        return ellipsis[:limit]
    used = 0
    cut = 0
    for index, ch in enumerate(text):
        width = 2 if ord(ch) > 0xFFFF else 1
        if used + width > budget:
            cut = index
            break
        used += width
    return text[:cut].rstrip() + ellipsis


_MD_SPECIAL = re.compile(r"([\\*_~`|\[\]])")


def escape_markdown(text: str) -> str:
    """Neutralise Discord markdown in untrusted listing text (titles, seller names...)."""
    return _MD_SPECIAL.sub(r"\\\1", text)


def _valid_url(url: str | None, limit: int = URL_LIMIT) -> bool:
    if not url or len(url) > limit or any(ch.isspace() for ch in url):
        return False
    parts = urlsplit(url)
    return parts.scheme in ("http", "https") and bool(parts.netloc)


def _iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()


def _snowflake(value: str | None) -> str | None:
    value = (value or "").strip()
    return value if value.isdigit() else None


# --------------------------------------------------------------------------- payload building


def embed_length(embed: Mapping[str, Any]) -> int:
    """Characters Discord counts toward the 6000 embed total."""
    total = text_length(embed.get("title", "")) + text_length(embed.get("description", ""))
    total += text_length((embed.get("footer") or {}).get("text", ""))
    total += text_length((embed.get("author") or {}).get("name", ""))
    for field in embed.get("fields", ()):
        total += text_length(field.get("name", "")) + text_length(field.get("value", ""))
    return total


def _enforce_total(embed: dict[str, Any], limit: int = EMBED_TOTAL_LIMIT) -> None:
    """Shrink description first, then trailing fields, then the title until the embed fits."""
    over = embed_length(embed) - limit
    if over <= 0:
        return
    description = embed.get("description")
    if description:
        keep = text_length(description) - over
        if keep > 1:
            embed["description"] = truncate(description, keep)
        else:
            del embed["description"]
        over = embed_length(embed) - limit
    fields: list[dict[str, Any]] = embed.get("fields", [])
    while over > 0 and fields:
        last = fields[-1]
        keep = text_length(last["value"]) - over
        if keep >= MIN_FIELD_VALUE:
            last["value"] = truncate(last["value"], keep)
        else:
            fields.pop()
        over = embed_length(embed) - limit
    if not fields:
        embed.pop("fields", None)
    if over > 0:
        title = embed.get("title", "")
        embed["title"] = truncate(title, max(1, text_length(title) - over))


def _description(alert: Alert) -> str:
    lines: list[str] = []
    profile = f"**{escape_markdown(alert.profile_name)}**"
    if alert.variant_id:
        profile += f" · {escape_markdown(alert.variant_id)}"
    profile += f" · {escape_markdown(alert.category)}"
    lines.append(profile)
    risk = risk_summary(alert)
    if risk:
        lines.append(f"⚠️ Risk: {escape_markdown(risk)}")
    for line in alert.score.explain[:EXPLAIN_LINES]:
        line = line.strip()
        if line:
            lines.append(f"• {escape_markdown(line)}")
    return "\n".join(lines)


def build_embed(alert: Alert, *, node_id: str | None = None) -> dict[str, Any]:
    """The alert embed with every Discord limit enforced."""
    item = alert.item
    embed: dict[str, Any] = {
        "title": truncate(escape_markdown(headline(alert)), TITLE_LIMIT),
        "color": SEVERITY_COLOR[alert.severity],
        "timestamp": _iso(alert.created_at),
    }
    if _valid_url(item.best_url):
        embed["url"] = item.best_url
    description = truncate(_description(alert), DESCRIPTION_LIMIT)
    if description:
        embed["description"] = description
    fields = [
        {
            "name": truncate(fact.name, FIELD_NAME_LIMIT) or ZWSP,
            "value": truncate(escape_markdown(fact.value), FIELD_VALUE_LIMIT) or ZWSP,
            "inline": fact.inline,
        }
        for fact in facts(alert)[:FIELDS_LIMIT]
    ]
    if fields:
        embed["fields"] = fields
    if _valid_url(item.primary_image):
        embed["thumbnail"] = {"url": item.primary_image}
    footer = " · ".join(part for part in ("DealRadar", node_id or item.node_id, alert.alert_id[:8]) if part)
    embed["footer"] = {"text": truncate(footer, FOOTER_LIMIT)}
    _enforce_total(embed)
    return embed


def link_button_rows(alert: Alert) -> list[dict[str, Any]]:
    """Action rows of link buttons (component type 1 containing type-2/style-5 buttons)."""
    buttons: list[dict[str, Any]] = []
    seen: set[str] = set()
    for label, url in links(alert):
        if not _valid_url(url, BUTTON_URL_LIMIT) or url in seen:
            continue
        seen.add(url)
        buttons.append({"type": 2, "style": 5, "label": truncate(label, BUTTON_LABEL_LIMIT) or "Open", "url": url})
    rows = [{"type": 1, "components": buttons[i : i + BUTTONS_PER_ROW]} for i in range(0, len(buttons), BUTTONS_PER_ROW)]
    return rows[:ACTION_ROWS_LIMIT]


def _base_payload(cfg: DiscordTarget) -> dict[str, Any]:
    payload: dict[str, Any] = {"username": truncate(cfg.username.strip(), USERNAME_LIMIT) or "DealRadar"}
    if _valid_url(cfg.avatar_url):
        payload["avatar_url"] = cfg.avatar_url
    return payload


def build_discord_payload(alert: Alert, cfg: DiscordTarget, *, mention: bool, node_id: str | None = None) -> dict[str, Any]:
    """Webhook execute payload for ``alert`` (pure; used by the dispatcher and tests)."""
    role = _snowflake(cfg.mention_role_id) if mention else None
    payload = _base_payload(cfg)
    payload["content"] = f"<@&{role}>" if role else ""
    payload["allowed_mentions"] = {"parse": [], "roles": [role]} if role else {"parse": []}
    payload["embeds"] = [build_embed(alert, node_id=node_id)]
    if cfg.link_buttons:
        rows = link_button_rows(alert)
        if rows:
            payload["components"] = rows
    return payload


def minimal_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Degraded copy for a retry after a 400: no components, thumbnail or embed URL."""
    out = {k: v for k, v in payload.items() if k != "components"}
    embeds = []
    for embed in payload.get("embeds", ()):
        embeds.append({k: v for k, v in embed.items() if k not in ("thumbnail", "url", "image")})
    if embeds:
        out["embeds"] = embeds
    return out


def build_notice_payload(title: str, message: str, cfg: DiscordTarget) -> dict[str, Any]:
    """Operator notice: one plain embed, never pings anybody."""
    embed: dict[str, Any] = {
        "title": truncate(escape_markdown(title.strip()) or "Notice", TITLE_LIMIT),
        "description": truncate(message.strip() or ZWSP, DESCRIPTION_LIMIT),
        "color": NOTICE_COLOR,
        "timestamp": _iso(utcnow()),
        "footer": {"text": "DealRadar notice"},
    }
    _enforce_total(embed)
    payload = _base_payload(cfg)
    payload.update({"content": "", "allowed_mentions": {"parse": []}, "embeds": [embed]})
    return payload


def webhook_url(base: str, *, with_components: bool, thread_id: str | None) -> str:
    """``base`` + ``wait=true`` (+ ``with_components`` / ``thread_id``), preserving other params."""
    parts = urlsplit(base)
    managed = {"wait", "with_components"} | ({"thread_id"} if thread_id else set())
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in managed]
    query.append(("wait", "true"))
    if with_components:
        query.append(("with_components", "true"))
    if thread_id:
        query.append(("thread_id", thread_id))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


# --------------------------------------------------------------------------- response helpers


def _decode(data: Any) -> Any:
    if not isinstance(data, str) or not data.strip():
        return None
    try:
        return json_loads(data)
    except ValueError:
        return None


def _retry_after(body: Any, headers: Mapping[str, str]) -> float:
    if isinstance(body, dict):
        value = body.get("retry_after")
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
            return float(value)
    hinted = parse_retry_after(headers.get("Retry-After"))
    if hinted is not None:
        return hinted
    reset_after = _float_header(headers, "X-RateLimit-Reset-After")
    return reset_after if reset_after is not None else DEFAULT_RETRY_AFTER


def _float_header(headers: Mapping[str, str], name: str) -> float | None:
    raw = headers.get(name)
    if raw is None:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value >= 0 and value == value else None


def _error_text(body: Any, raw: Any) -> str:
    if isinstance(body, dict):
        message = body.get("message") or ""
        code = body.get("code")
        detail = f"{message} (code {code})" if code is not None else str(message)
        errors = body.get("errors")
        if errors:
            detail += f" {str(errors)[:300]}"
        return detail.strip()
    return str(raw or "")[:200].strip()


@dataclass(slots=True)
class _Attempt:
    payload: dict[str, Any]
    degraded: bool = False


# --------------------------------------------------------------------------- dispatcher


class DiscordDispatcher(Dispatcher):
    """Posts alerts to one Discord webhook (``target = discord:<name>``)."""

    def __init__(
        self,
        name: str,
        cfg: DiscordTarget,
        http: HttpClient,
        *,
        timeout: float,
        metrics: Metrics | None = None,
        node_id: str | None = None,
    ) -> None:
        self.name = name
        self.cfg = cfg
        self.http = http
        self.timeout = timeout
        self.node_id = node_id
        self.target = f"discord:{name}"
        self.metrics = metrics or Metrics()
        self._disabled_reason: str | None = None
        self._blocked_until = 0.0  # monotonic; per-webhook bucket exhausted / 429
        self._role_warned = False
        self._requests = self.metrics.counter("discord_requests_total", "Discord webhook requests", ("target", "status"))
        self._rate_limited = self.metrics.counter("discord_rate_limited_total", "Discord 429 responses", ("target",))
        if cfg.mention_role_id and _snowflake(cfg.mention_role_id) is None:
            log.warning("discord mention_role_id is not a numeric role id; mentions disabled", extra={"target": self.target})

    # ------------------------------------------------------------------ state

    @property
    def configured(self) -> bool:
        return self.cfg.configured and self._disabled_reason is None

    @property
    def disabled_reason(self) -> str | None:
        return self._disabled_reason

    @property
    def blocked_for(self) -> float:
        """Seconds until the per-webhook rate limit allows the next request."""
        return max(0.0, self._blocked_until - time.monotonic())

    def _secret(self) -> str | None:
        return self.cfg.webhook_url.get_secret_value() if self.cfg.webhook_url is not None else None

    def _redact(self, text: str) -> str:
        secret = self._secret()
        if not secret:
            return text
        text = text.replace(secret, "<webhook>")
        token = urlsplit(secret).path.rstrip("/").rsplit("/", 1)[-1]
        return text.replace(token, "<redacted>") if len(token) >= 8 else text

    def _note_rate_headers(self, headers: Mapping[str, str]) -> None:
        remaining = headers.get("X-RateLimit-Remaining")
        if remaining is None or remaining.strip() != "0":
            return
        reset_after = _float_header(headers, "X-RateLimit-Reset-After")
        if reset_after:
            self._blocked_until = max(self._blocked_until, time.monotonic() + reset_after)

    def _disable(self, status: int, detail: str) -> None:
        if self._disabled_reason is None:
            self._disabled_reason = f"HTTP {status}: {detail}"[:300]
            log.error(
                "discord webhook rejected credentials; target disabled until restart",
                extra={"target": self.target, "status": status, "detail": detail[:200]},
            )

    # ------------------------------------------------------------------ public API

    async def send(self, alert: Alert) -> DispatchResult:
        try:
            payload = build_discord_payload(alert, self.cfg, mention=alert.mention, node_id=self.node_id)
        except Exception as exc:  # formatting bugs must never take the pipeline down
            log.exception("discord payload build failed", extra={"target": self.target, "alert_id": alert.alert_id})
            return DispatchResult(target=self.target, ok=False, error=f"payload build failed: {exc!r}"[:300])
        return await self._deliver(payload, alert_id=alert.alert_id)

    async def send_notice(self, title: str, message: str) -> DispatchResult:
        try:
            payload = build_notice_payload(title, message, self.cfg)
        except Exception as exc:
            return DispatchResult(target=self.target, ok=False, error=f"payload build failed: {exc!r}"[:300])
        return await self._deliver(payload, alert_id=None)

    async def close(self) -> None:
        """Nothing to release: the shared HttpClient owns the connection pool."""

    # ------------------------------------------------------------------ delivery

    async def _deliver(self, payload: dict[str, Any], *, alert_id: str | None) -> DispatchResult:
        started = time.monotonic()
        deadline = started + self.timeout
        secret = self._secret()
        if secret is None:
            return DispatchResult(target=self.target, ok=False, error="not configured")
        if self._disabled_reason is not None:
            return DispatchResult(target=self.target, ok=False, error=f"disabled: {self._disabled_reason}")

        attempt = _Attempt(payload)
        attempts = 0
        status: int | None = None
        error: str | None = None
        server_errors = 0

        def _result(ok: bool, message_id: str | None = None) -> DispatchResult:
            return DispatchResult(
                target=self.target,
                ok=ok,
                status=status,
                latency_ms=round((time.monotonic() - started) * 1000.0, 3),
                attempts=max(1, attempts),
                message_id=message_id,
                error=None if ok else (self._redact(error or "delivery failed"))[:500],
            )

        while attempts < MAX_ATTEMPTS:
            wait = self._blocked_until - time.monotonic()
            if wait > 0:
                if time.monotonic() + wait + MIN_ATTEMPT_SECONDS > deadline:
                    error = f"rate limited for {wait:.2f}s (exceeds dispatch budget)"
                    break
                await asyncio.sleep(wait)
            remaining = deadline - time.monotonic()
            if remaining < MIN_ATTEMPT_SECONDS:
                error = error or "dispatch budget exhausted"
                break
            attempts += 1
            url = webhook_url(secret, with_components="components" in attempt.payload, thread_id=self.cfg.thread_id)
            try:
                resp = await self.http.post_json(
                    url,
                    attempt.payload,
                    expected=ALL_STATUSES,
                    parse="text",
                    retry=False,
                    rate_limit=False,
                    timeout=remaining,
                )
            except asyncio.CancelledError:
                raise
            except aiohttp.ClientConnectorError as exc:
                # The TCP/TLS connection was never established, so nothing was posted:
                # safe to retry without risking a duplicate message.
                status = None
                error = f"connection failed: {exc!r}"
                delay = SERVER_ERROR_BASE_DELAY * (2 ** (attempts - 1))
                if time.monotonic() + delay + MIN_ATTEMPT_SECONDS <= deadline:
                    await asyncio.sleep(delay)
                    continue
                break
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                # Ambiguous: Discord may have posted the message; do not risk a duplicate.
                status = None
                error = f"request failed: {exc!r}"
                break
            except Exception as exc:  # never raise from send()
                status = None
                error = f"unexpected error: {exc!r}"
                log.exception("discord send crashed", extra={"target": self.target, "alert_id": alert_id})
                break

            status = resp.status
            self._requests.inc(target=self.target, status=status)
            self._note_rate_headers(resp.headers)
            body = _decode(resp.data)

            if status in OK_STATUSES:
                message_id = str(body["id"]) if isinstance(body, dict) and body.get("id") is not None else None
                if attempt.degraded:
                    log.info("discord accepted degraded payload", extra={"target": self.target, "alert_id": alert_id})
                return _result(True, message_id)

            detail = _error_text(body, resp.data)
            if status == 429:
                retry_after = _retry_after(body, resp.headers)
                self._blocked_until = max(self._blocked_until, time.monotonic() + retry_after)
                self._rate_limited.inc(target=self.target)
                scope = body.get("global") if isinstance(body, dict) else None
                error = f"rate limited (retry_after={retry_after:.2f}s{', global' if scope else ''})"
                log.warning(
                    "discord rate limited",
                    extra={"target": self.target, "retry_after_s": round(retry_after, 3), "global": bool(scope)},
                )
                continue  # the gate at the top of the loop waits or fails fast
            if status in DISABLE_STATUSES:
                self._disable(status, detail)
                error = f"HTTP {status}: {detail}"
                break
            if status == 400 and not attempt.degraded:
                log.warning(
                    "discord rejected payload; retrying with minimal payload",
                    extra={"target": self.target, "alert_id": alert_id, "detail": detail[:300]},
                )
                attempt = _Attempt(minimal_payload(attempt.payload), degraded=True)
                error = f"HTTP 400: {detail}"
                continue
            if status in RETRY_STATUSES:
                server_errors += 1
                error = f"HTTP {status}: {detail}"
                delay = SERVER_ERROR_BASE_DELAY * (2 ** (server_errors - 1))
                if time.monotonic() + delay + MIN_ATTEMPT_SECONDS <= deadline:
                    await asyncio.sleep(delay)
                    continue
                break
            error = f"HTTP {status}: {detail}"
            break

        log.warning(
            "discord delivery failed",
            extra={
                "target": self.target,
                "alert_id": alert_id,
                "status": status,
                "attempts": attempts,
                "error": self._redact(error or "")[:300],
            },
        )
        return _result(False)


__all__ = [
    "DiscordDispatcher",
    "build_discord_payload",
    "build_embed",
    "build_notice_payload",
    "embed_length",
    "escape_markdown",
    "link_button_rows",
    "minimal_payload",
    "text_length",
    "truncate",
    "webhook_url",
]
