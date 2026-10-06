"""Telegram Bot API channel.

Design decisions
----------------
* **HTML parse mode, escaped by construction.** Telegram rejects the whole message
  (400 "can't parse entities") when a stray ``<`` or ``&`` appears outside a tag, and
  listing titles are full of them ("RTX 4080 <$900 & free ship"). Every dynamic string
  is escaped with :func:`html.escape`; href attributes are escaped with quotes too.
* **Never cut inside a tag or entity.** Messages are assembled from complete,
  pre-escaped lines chosen by priority (headline → key facts → risk → other facts →
  links). Only the headline itself may be truncated, and it is truncated on the
  *plain* text, one escaped character at a time, so the result is always well-formed.
  The budget is applied to the raw HTML (markup included, measured in UTF-16 code
  units), which is always >= what Telegram counts after parsing — so a caption built
  for 1024 can never be refused for length.
* **Photo first, text fallback.** With an image and ``send_photos`` the alert goes out
  as ``sendPhoto`` (URL fetched by Telegram) with the caption; Telegram cannot fetch
  many marketplace CDN URLs (signed/expiring, hot-link protected), and a 400 there
  falls back to ``sendMessage`` with a large link preview of the deal page.
  A 400 on ``sendMessage`` (e.g. an entity Telegram dislikes or a button URL it
  refuses) is retried once as plain text without markup, so the alert still lands.
* **Silent vs. loud.** ``disable_notification`` is set for severities below
  ``silent_below`` unless the router asked for a mention.
* **429 / migration.** ``parameters.retry_after`` is honoured within the dispatch
  timeout budget (and blocks the next send to this chat); ``migrate_to_chat_id``
  (group upgraded to a supergroup) is followed automatically.
* **Dead targets are disabled.** 401/404 (bad token), 403 (bot blocked/kicked) and
  "chat not found" flip ``configured`` to ``False`` so the router stops hammering.
* **The bot token lives in the URL path** (``/bot<token>/sendMessage``). URLs are
  never logged and the token is redacted from every error string.
* **Unencodable text.** Lone UTF-16 surrogates (possible in Playwright-scraped titles)
  are replaced before formatting (:func:`dispatchers.discord.scrub_alert`), otherwise
  the shared ``links()`` and the JSON encoder would refuse the whole alert.
"""

from __future__ import annotations

import asyncio
import html
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import aiohttp
from pydantic import SecretStr

from deal_radar.config_schema import TelegramChat
from deal_radar.core.backoff import parse_retry_after
from deal_radar.core.http import HttpClient, json_loads
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.dispatchers.base import Dispatcher, facts, headline, links, risk_summary
from deal_radar.dispatchers.discord import NOT_SENT_ERRORS, scrub_alert, scrub_surrogates
from deal_radar.engine.types import Alert, DispatchResult

log = get_logger("dispatch.telegram")

MESSAGE_LIMIT = 4096
CAPTION_LIMIT = 1024
BUTTONS_PER_ROW = 2
BUTTON_TEXT_LIMIT = 64
BUTTON_URL_LIMIT = 2048
KEY_FACTS = 4  # Price, Market, Discount, Score: kept even in tight captions
MIN_HEADLINE = 120  # headline budget never shrinks below this to make room for facts

ALL_STATUSES = range(100, 600)
DISABLE_STATUSES = frozenset({401, 403, 404})
RETRY_STATUSES = frozenset({500, 502, 503, 504})
MAX_ATTEMPTS = 6
MIN_ATTEMPT_SECONDS = 0.25
DEFAULT_RETRY_AFTER = 1.0
SERVER_ERROR_BASE_DELAY = 0.25


# --------------------------------------------------------------------------- text helpers


def text_length(text: str) -> int:
    """Length in UTF-16 code units, the unit Telegram uses for its limits."""
    return len(text) + sum(1 for ch in text if ord(ch) > 0xFFFF)


def escape(text: str) -> str:
    """Escape ``&``, ``<`` and ``>`` for Telegram HTML text."""
    return html.escape(text, quote=False)


def escape_attr(text: str) -> str:
    return html.escape(text, quote=True)


def fit_escaped(text: str, budget: int, ellipsis: str = "…") -> str:
    """Escaped ``text``, truncated on the plain text so no entity is ever split."""
    full = escape(text)
    if text_length(full) <= budget:
        return full
    limit = budget - text_length(ellipsis)
    if limit < 0:
        return ""
    pieces: list[str] = []
    used = 0
    for ch in text:
        piece = escape(ch)
        width = text_length(piece)
        if used + width > limit:
            break
        pieces.append(piece)
        used += width
    return "".join(pieces).rstrip() + ellipsis


def _valid_url(url: str | None, limit: int = BUTTON_URL_LIMIT) -> bool:
    if not url or len(url) > limit or any(ch.isspace() for ch in url):
        return False
    try:
        parts = urlsplit(url)
    except ValueError:  # e.g. "https://[broken" (unbalanced IPv6 bracket)
        return False
    return parts.scheme in ("http", "https") and bool(parts.netloc)


def _truncate_plain(text: str, limit: int) -> str:
    if text_length(text) <= limit:
        return text
    if limit <= 0:
        return ""
    out: list[str] = []
    used = 0
    for ch in text:
        width = 2 if ord(ch) > 0xFFFF else 1
        if used + width > limit - 1:
            break
        out.append(ch)
        used += width
    return "".join(out).rstrip() + "…"


# --------------------------------------------------------------------------- message building


@dataclass(frozen=True, slots=True)
class _Line:
    order: int  # display position
    priority: int  # lower = kept first when the budget is tight
    text: str  # complete, escaped HTML (or plain text for the plain variant)


def _candidate_lines(alert: Alert, *, markup: bool) -> list[_Line]:
    lines: list[_Line] = []
    order = 1
    for index, fact in enumerate(facts(alert)):
        text = f"<b>{escape(fact.name)}:</b> {escape(fact.value)}" if markup else f"{fact.name}: {fact.value}"
        lines.append(_Line(order, 1 if index < KEY_FACTS else 3, text))
        order += 1
    risk = risk_summary(alert)
    if risk:
        lines.append(_Line(order, 2, f"⚠️ <b>Risk:</b> {escape(risk)}" if markup else f"⚠️ Risk: {risk}"))
        order += 1
    pairs = [(label, url) for label, url in links(alert) if _valid_url(url)]
    if pairs:
        if markup:
            text = " · ".join(f'<a href="{escape_attr(url)}">{escape(label)}</a>' for label, url in pairs)
        else:
            text = "\n".join(f"{label}: {url}" for label, url in pairs)
        lines.append(_Line(order, 4, text))
    return lines


def _assemble(head: str, candidates: list[_Line], max_len: int) -> str:
    budget = max_len - text_length(head)
    chosen: list[_Line] = []
    for line in sorted(candidates, key=lambda ln: (ln.priority, ln.order)):
        cost = text_length(line.text) + 1  # "\n" separator
        if cost <= budget:
            chosen.append(line)
            budget -= cost
    chosen.sort(key=lambda ln: ln.order)
    return "\n".join([head, *(line.text for line in chosen)])


def _compose(alert: Alert, max_len: int, *, markup: bool) -> str:
    candidates = _candidate_lines(alert, markup=markup)
    plain_head = headline(alert)
    fit = fit_escaped if markup else _truncate_plain
    wrapper = text_length("<b></b>") if markup else 0
    if max_len <= wrapper:
        return fit(plain_head, max_len)
    # The headline may not crowd out the key facts (a title full of "&" triples in
    # size once escaped), but it always keeps a readable minimum.
    key_cost = sum(text_length(line.text) + 1 for line in candidates if line.priority == 1)
    head_budget = max(max_len - wrapper - key_cost, min(max_len - wrapper, MIN_HEADLINE))
    head_text = fit(plain_head, head_budget)
    head = f"<b>{head_text}</b>" if markup else head_text
    return _assemble(head, candidates, max_len)


def build_telegram_message(alert: Alert, *, max_len: int) -> str:
    """HTML-formatted alert of at most ``max_len`` UTF-16 units *including markup*."""
    return _compose(alert, max_len, markup=True)


def build_plain_message(alert: Alert, *, max_len: int = MESSAGE_LIMIT) -> str:
    """Markup-free variant used when Telegram refuses the HTML message."""
    return _compose(alert, max_len, markup=False)


def build_inline_keyboard(alert: Alert) -> list[list[dict[str, str]]]:
    """URL buttons (primary action first), at most two per row."""
    buttons: list[dict[str, str]] = []
    seen: set[str] = set()
    for label, url in links(alert):
        if not _valid_url(url) or url in seen:
            continue
        seen.add(url)
        buttons.append({"text": _truncate_plain(label, BUTTON_TEXT_LIMIT) or "Open", "url": url})
    return [buttons[i : i + BUTTONS_PER_ROW] for i in range(0, len(buttons), BUTTONS_PER_ROW)]


def is_silent(alert: Alert, chat: TelegramChat) -> bool:
    """``disable_notification``: below ``silent_below`` and not a mention route."""
    return not alert.mention and alert.severity.rank < chat.silent_below.rank


# --------------------------------------------------------------------------- response helpers


def _decode(data: Any) -> Any:
    if not isinstance(data, str) or not data.strip():
        return None
    try:
        return json_loads(data)
    except ValueError:
        return None


def _parameters(body: Any) -> dict[str, Any]:
    if isinstance(body, dict) and isinstance(body.get("parameters"), dict):
        return body["parameters"]
    return {}


def _retry_after(body: Any, headers: Mapping[str, str]) -> float:
    value = _parameters(body).get("retry_after")
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
        return float(value)
    hinted = parse_retry_after(headers.get("Retry-After"))
    return hinted if hinted is not None else DEFAULT_RETRY_AFTER


def _description(body: Any, raw: Any) -> str:
    if isinstance(body, dict):
        return str(body.get("description") or "")[:300]
    return str(raw or "")[:200].strip()


@dataclass(slots=True)
class _Step:
    method: str  # "sendPhoto" | "sendMessage"
    payload: dict[str, Any]
    label: str  # "photo" | "html" | "plain" | "notice"


# --------------------------------------------------------------------------- dispatcher


class TelegramDispatcher(Dispatcher):
    """Sends alerts to one Telegram chat (``target = telegram:<name>``)."""

    def __init__(
        self,
        name: str,
        chat: TelegramChat,
        bot_token: SecretStr | None,
        api_base: str,
        http: HttpClient,
        *,
        timeout: float,
        metrics: Metrics | None = None,
    ) -> None:
        self.name = name
        self.chat = chat
        self.http = http
        self.timeout = timeout
        self.api_base = api_base.rstrip("/")
        self.target = f"telegram:{name}"
        self.metrics = metrics or Metrics()
        self._token = bot_token
        self._chat_id: str | None = (chat.chat_id or "").strip() or None  # env values may carry a newline
        self._disabled_reason: str | None = None
        self._blocked_until = 0.0  # monotonic; per-chat flood control
        self._requests = self.metrics.counter(
            "telegram_requests_total", "Telegram Bot API requests", ("target", "method", "status")
        )
        self._fallbacks = self.metrics.counter("telegram_fallbacks_total", "Telegram degraded deliveries", ("target", "kind"))

    # ------------------------------------------------------------------ state

    @property
    def configured(self) -> bool:
        return self._token is not None and self._chat_id is not None and self._disabled_reason is None

    @property
    def disabled_reason(self) -> str | None:
        return self._disabled_reason

    @property
    def chat_id(self) -> str | None:
        return self._chat_id

    def _token_value(self) -> str:
        # Stripped: Docker secrets / .env values often end with a newline.
        return self._token.get_secret_value().strip() if self._token is not None else ""

    def _redact(self, text: str) -> str:
        token = self._token_value()
        return text.replace(token, "<redacted>") if token else text

    def _disable(self, status: int, detail: str) -> None:
        if self._disabled_reason is None:
            self._disabled_reason = f"HTTP {status}: {detail}"[:300]
            log.error(
                "telegram rejected the bot or chat; target disabled until restart",
                extra={"target": self.target, "status": status, "detail": self._redact(detail)[:200]},
            )

    # ------------------------------------------------------------------ payloads

    def _common(self, *, silent: bool) -> dict[str, Any]:
        out: dict[str, Any] = {"chat_id": self._chat_id, "disable_notification": silent}
        if self.chat.message_thread_id is not None:
            out["message_thread_id"] = self.chat.message_thread_id
        return out

    def plan(self, alert: Alert) -> list[_Step]:
        """Ordered delivery attempts for ``alert``: each step is the fallback of the previous."""
        silent = is_silent(alert, self.chat)
        keyboard = build_inline_keyboard(alert)
        markup = {"reply_markup": {"inline_keyboard": keyboard}} if keyboard else {}
        steps: list[_Step] = []
        photo = alert.item.primary_image
        if self.chat.send_photos and _valid_url(photo):
            payload = self._common(silent=silent)
            payload.update(
                {
                    "photo": photo,
                    "caption": build_telegram_message(alert, max_len=CAPTION_LIMIT),
                    "parse_mode": "HTML",
                    **markup,
                }
            )
            steps.append(_Step("sendPhoto", payload, "photo"))
        message = self._common(silent=silent)
        message.update({"text": build_telegram_message(alert, max_len=MESSAGE_LIMIT), "parse_mode": "HTML", **markup})
        best_url = alert.item.best_url
        if _valid_url(best_url):
            message["link_preview_options"] = {"is_disabled": False, "url": best_url, "prefer_small_media": True}
        steps.append(_Step("sendMessage", message, "html"))
        plain = self._common(silent=silent)
        plain["text"] = build_plain_message(alert, max_len=MESSAGE_LIMIT)
        steps.append(_Step("sendMessage", plain, "plain"))
        return steps

    # ------------------------------------------------------------------ public API

    async def send(self, alert: Alert) -> DispatchResult:
        try:
            steps = self.plan(scrub_alert(alert))
        except Exception as exc:  # formatting bugs must never take the pipeline down
            log.exception("telegram payload build failed", extra={"target": self.target, "alert_id": alert.alert_id})
            return DispatchResult(target=self.target, ok=False, error=f"payload build failed: {exc!r}"[:300])
        return await self._deliver(steps, alert_id=alert.alert_id)

    async def send_notice(self, title: str, message: str) -> DispatchResult:
        title, message = scrub_surrogates(title), scrub_surrogates(message)
        text = f"<b>{fit_escaped(title.strip() or 'Notice', 256)}</b>\n{fit_escaped(message.strip(), MESSAGE_LIMIT - 300)}"
        payload = self._common(silent=False)
        payload.update({"text": text, "parse_mode": "HTML", "link_preview_options": {"is_disabled": True}})
        plain = self._common(silent=False)
        plain["text"] = _truncate_plain(f"{title}\n{message}", MESSAGE_LIMIT)
        return await self._deliver([_Step("sendMessage", payload, "notice"), _Step("sendMessage", plain, "plain")], alert_id=None)

    async def close(self) -> None:
        """Nothing to release: the shared HttpClient owns the connection pool."""

    # ------------------------------------------------------------------ delivery

    async def _deliver(self, steps: list[_Step], *, alert_id: str | None) -> DispatchResult:
        started = time.monotonic()
        deadline = started + self.timeout
        # Scaled down for tiny budgets so the first attempt always goes out.
        min_attempt = min(MIN_ATTEMPT_SECONDS, self.timeout / 2)
        if self._token is None or self._chat_id is None:
            return DispatchResult(target=self.target, ok=False, error="not configured")
        if self._disabled_reason is not None:
            return DispatchResult(target=self.target, ok=False, error=f"disabled: {self._disabled_reason}")
        token = self._token_value()

        index = 0
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
                error=None if ok else self._redact(error or "delivery failed")[:500],
            )

        while index < len(steps) and attempts < MAX_ATTEMPTS:
            step = steps[index]
            wait = self._blocked_until - time.monotonic()
            if wait > 0:
                if time.monotonic() + wait + min_attempt > deadline:
                    error = f"rate limited for {wait:.2f}s (exceeds dispatch budget)"
                    break
                await asyncio.sleep(wait)
            remaining = deadline - time.monotonic()
            if remaining < min_attempt:
                error = error or "dispatch budget exhausted"
                break
            attempts += 1
            step.payload["chat_id"] = self._chat_id  # may have changed after a migration
            try:
                resp = await self.http.post_json(
                    f"{self.api_base}/bot{token}/{step.method}",
                    step.payload,
                    expected=ALL_STATUSES,
                    parse="text",
                    retry=False,
                    timeout=remaining,
                )
            except asyncio.CancelledError:
                raise
            except NOT_SENT_ERRORS as exc:
                # Nothing reached Telegram (connect/TLS failed): safe to retry, no duplicate.
                status = None
                error = f"connection failed: {type(exc).__name__}: {exc}"
                delay = SERVER_ERROR_BASE_DELAY * (2 ** (attempts - 1))
                if time.monotonic() + delay + min_attempt <= deadline:
                    await asyncio.sleep(delay)
                    continue
                break
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                status = None
                error = f"request failed: {type(exc).__name__}: {exc}"
                break
            except Exception as exc:  # never raise from send()
                status = None
                error = f"unexpected error: {type(exc).__name__}: {exc}"
                log.error(
                    "telegram send crashed",
                    extra={"target": self.target, "alert_id": alert_id, "error": self._redact(error)[:300]},
                )
                break

            status = resp.status
            self._requests.inc(target=self.target, method=step.method, status=status)
            body = _decode(resp.data)
            ok = isinstance(body, dict) and body.get("ok") is True
            if 200 <= status < 300 and ok:
                result = body.get("result") if isinstance(body, dict) else None
                message_id = result.get("message_id") if isinstance(result, dict) else None
                if step.label != steps[0].label:
                    self._fallbacks.inc(target=self.target, kind=step.label)
                    log.info(
                        "telegram delivered via fallback",
                        extra={"target": self.target, "alert_id": alert_id, "via": step.label},
                    )
                return _result(True, str(message_id) if message_id is not None else None)

            detail = _description(body, resp.data)
            error = f"HTTP {status}: {detail}" if detail else f"HTTP {status}"
            params = _parameters(body)
            if status == 429:
                retry_after = _retry_after(body, resp.headers)
                self._blocked_until = max(self._blocked_until, time.monotonic() + retry_after)
                log.warning("telegram rate limited", extra={"target": self.target, "retry_after_s": retry_after})
                error = f"rate limited (retry_after={retry_after:.2f}s)"
                continue
            migrate_to = params.get("migrate_to_chat_id")
            if status == 400 and migrate_to is not None and str(migrate_to) != self._chat_id:
                log.warning(
                    "telegram chat migrated to a supergroup; update chat_id in the config",
                    extra={"target": self.target, "new_chat_id": str(migrate_to)},
                )
                self._chat_id = str(migrate_to)
                continue
            if status in DISABLE_STATUSES or (status == 400 and "chat not found" in detail.lower()):
                self._disable(status, detail)
                break
            if status == 400 and index + 1 < len(steps):
                log.warning(
                    "telegram rejected request; falling back",
                    extra={"target": self.target, "alert_id": alert_id, "method": step.method, "detail": self._redact(detail)},
                )
                index += 1
                continue
            if status in RETRY_STATUSES:
                server_errors += 1
                delay = SERVER_ERROR_BASE_DELAY * (2 ** (server_errors - 1))
                if time.monotonic() + delay + min_attempt <= deadline:
                    await asyncio.sleep(delay)
                    continue
            break

        log.warning(
            "telegram delivery failed",
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
    "CAPTION_LIMIT",
    "MESSAGE_LIMIT",
    "TelegramDispatcher",
    "build_inline_keyboard",
    "build_plain_message",
    "build_telegram_message",
    "escape",
    "fit_escaped",
    "is_silent",
]
