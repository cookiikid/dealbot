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
* **No accidental pings.** ``allowed_mentions`` is sent explicitly on every payload
  (alerts, degraded retries and notices) with ``parse`` empty; a role is pinged only
  when the router asked for a mention *and* ``mention_role_id`` is a snowflake, and
  then only that role is whitelisted. Listing titles containing ``@everyone``,
  ``@here`` or ``<@id>`` therefore render as inert text.
* **Scraped text is sanitised** (:func:`sanitize_text`) before it is formatted: C0/C1
  control characters, zero-width and other invisible characters, and bidi overrides
  (which can visually reverse a price) are stripped (a zero-width joiner survives
  only inside emoji sequences); single-line contexts (title, button labels) fold
  newlines and tabs to spaces. Markdown is then escaped (:func:`escape_markdown`),
  including ``[`` / ``]``, so a title like ``[Claim](https://evil)`` can never render
  as a masked link in the title, description or fields.
* **Versioned API URL.** The URL copied from Discord's UI is unversioned
  (``https://discord.com/api/webhooks/<id>/<token>``), and older ones use
  ``discordapp.com`` or the ``ptb.`` / ``canary.`` hosts. :func:`canonical_webhook_url`
  rebuilds every Discord-hosted webhook URL onto
  ``https://discord.com/api/v10/webhooks/<id>/<token>`` (dropping a ``/slack`` or
  ``/github`` suffix, because the payload is native). A Discord-hosted URL that is
  not a webhook (e.g. a channel link) disables the target; any other host (a
  self-hosted relay) is used verbatim.
* **Link buttons** are sent as one action row (type 1) of link buttons (type 2,
  style 5). Non-application webhooks silently drop components unless the request
  carries ``?with_components=true``, which is added whenever the payload has
  components. The ``IS_COMPONENTS_V2`` flag (``1 << 15``) is never set: V2 messages
  cannot carry ``embeds`` or ``content``, and Discord would reject the alert.
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
  with a minimal payload: no buttons, no thumbnail, no embed URL, no username/avatar
  override (Discord refuses e.g. usernames containing "discord"). Whitespace-only
  field values (which Discord rejects) become a zero-width space, and lone UTF-16
  surrogates (Playwright-scraped titles can carry them; they are not valid UTF-8 so
  the JSON encoder refuses them) are replaced by U+FFFD before formatting.
* **Secrets.** The webhook URL *is* the credential; it is never logged and is
  redacted from every error string (also in percent-encoded form). A webhook URL
  that is not an http(s) URL disables the target at construction with an error that
  does not echo it.
"""

from __future__ import annotations

import asyncio
import re
import time
import traceback
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

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

USERNAME_LIMIT = 80
TITLE_LIMIT = 256
DESCRIPTION_LIMIT = 4096
FIELDS_LIMIT = 25
FIELD_NAME_LIMIT = 256
FIELD_VALUE_LIMIT = 1024
FOOTER_LIMIT = 2048
EMBED_TOTAL_LIMIT = 6000
BUTTON_LABEL_LIMIT = 80
BUTTON_URL_LIMIT = 512
BUTTONS_PER_ROW = 5
ACTION_ROWS_LIMIT = 5
URL_LIMIT = 2048

EXPLAIN_LINES = 6  # how many ScoreResult.explain lines make it into the description
MIN_FIELD_VALUE = 16  # below this a truncated field is useless; drop it instead
NOTICE_COLOR = 0x607D8B
ZWSP = "\u200b"  # Discord rejects empty field names/values
FORBIDDEN_USERNAME_PARTS = ("discord", "clyde")  # webhook username overrides containing these are refused (400)

# --------------------------------------------------------------------------- webhook URL

DISCORD_API_VERSION = 10
DISCORD_WEBHOOK_HOSTS = frozenset(
    {
        "discord.com",
        "www.discord.com",
        "ptb.discord.com",
        "canary.discord.com",
        "discordapp.com",
        "www.discordapp.com",
        "ptb.discordapp.com",
        "canary.discordapp.com",
    }
)
_WEBHOOK_EXECUTE_PATH = re.compile(r"/api(?:/v\d{1,2})?/webhooks/(\d{1,20})/([A-Za-z0-9_-]{1,200})(?:/(?:slack|github))?/?")

# --------------------------------------------------------------------------- delivery policy

ALL_STATUSES = range(100, 600)  # inspect every status ourselves (see module docstring)
OK_STATUSES = frozenset({200, 204})
DISABLE_STATUSES = frozenset({401, 403, 404})
RETRY_STATUSES = frozenset({500, 502, 503, 504, 520, 521, 522, 523, 524})
MAX_ATTEMPTS = 5
MIN_ATTEMPT_SECONDS = 0.25  # never start a request with less budget than this
DEFAULT_RETRY_AFTER = 1.0  # 429 without any hint (e.g. Cloudflare HTML page)
SERVER_ERROR_BASE_DELAY = 0.25
# Failures raised before any byte of the request left the machine: retrying them can
# never post a duplicate message. (ConnectionTimeoutError exists since aiohttp 3.10.)
NOT_SENT_ERRORS: tuple[type[BaseException], ...] = (aiohttp.ClientConnectorError,) + tuple(
    cls for cls in (getattr(aiohttp, "ConnectionTimeoutError", None),) if cls is not None
)


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
# Block-level markers only count at the start of a line: headings ("# ", "-# "),
# quotes ("> ", ">>> "), bullets ("- ") and ordered lists ("1. ").
_MD_LINE_MARKER = re.compile(r"(?m)^([ \t]*)([#>-])(?=[ \t#>-])")
_MD_ORDERED_LIST = re.compile(r"(?m)^([ \t]*\d+)\.(?=[ \t])")


def escape_markdown(text: str) -> str:
    """Neutralise Discord markdown in untrusted listing text (titles, seller names...)."""
    text = _MD_SPECIAL.sub(r"\\\1", text)
    text = _MD_LINE_MARKER.sub(r"\1\\\2", text)
    return _MD_ORDERED_LIST.sub(r"\1\\.", text)


# C0/C1 controls (tab/newline/CR are folded separately), soft hyphen, invisible
# joiners/separators, bidi marks/embeddings/overrides/isolates, Hangul fillers (blank
# "names"), BOM, interlinear annotation marks and Unicode tag characters.
_INVISIBLE = re.compile(
    "[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f\u00ad\u034f\u061c\u115f\u1160\u17b4\u17b5\u180e"
    "\u200b\u200c\u200e\u200f\u202a-\u202e\u2060-\u2064\u2066-\u206f\u3164\ufeff\uffa0\ufff9-\ufffb"
    "\U000e0000-\U000e007f]"
)
_EMOJI_CLASS = "[\u2190-\u2bff\u2600-\u27bf\ufe0f\U0001f000-\U0001faff]"
# A zero-width joiner is only legitimate inside an emoji sequence (e.g. U+1F468 ZWJ U+1F4BB).
_STRAY_ZWJ = re.compile(f"(?<!{_EMOJI_CLASS})\u200d|\u200d(?!{_EMOJI_CLASS})")
_LINE_BREAKS = re.compile(r"\r\n?|[\u2028\u2029\x85]")


def sanitize_text(text: str, *, single_line: bool = False) -> str:
    """Strip control and invisible characters from scraped text before formatting.

    Newlines are normalised to ``\\n`` (or folded to spaces with ``single_line``) and
    tabs become spaces. The result is safe to pass to :func:`escape_markdown`.
    """
    if not text:
        return ""
    text = _LINE_BREAKS.sub("\n", text)
    text = _STRAY_ZWJ.sub("", _INVISIBLE.sub("", text)).replace("\t", " ")
    if single_line:
        text = " ".join(part.strip() for part in text.split("\n") if part.strip())
    return text


def _md(text: str, *, single_line: bool = False) -> str:
    """Untrusted text -> sanitised and markdown-escaped."""
    return escape_markdown(sanitize_text(text, single_line=single_line))


_SURROGATE = re.compile("[\ud800-\udfff]")
_WEBHOOK_PATH = re.compile(r"(/webhooks/\d+/)[^/?#\s'\"<>]+")  # token segment of any webhook URL in a message


def scrub_surrogates(value: Any) -> Any:
    """``value`` with lone UTF-16 surrogates replaced by U+FFFD in every nested string.

    Lone surrogates are not encodable as UTF-8, so ``quote_plus`` (inside the shared
    ``links()``) and the JSON encoder both refuse them; one in a scraped title would
    otherwise fail the alert on every channel. Returns the *same* object when nothing
    needed replacing, so callers can detect the (rare) dirty case by identity.
    """
    if isinstance(value, str):
        if value.isascii() or _SURROGATE.search(value) is None:
            return value
        return _SURROGATE.sub("\ufffd", value)
    if isinstance(value, dict):
        out: dict[Any, Any] = {}
        changed = False
        for key, item in value.items():
            new_key, new_item = scrub_surrogates(key), scrub_surrogates(item)
            changed = changed or new_key is not key or new_item is not item
            out[new_key] = new_item
        return out if changed else value
    if isinstance(value, (list, tuple)):
        items = [scrub_surrogates(item) for item in value]
        return items if any(new is not old for new, old in zip(items, value, strict=True)) else value
    return value


def scrub_alert(alert: Alert) -> Alert:
    """``alert`` itself, or a sanitised copy when any of its strings holds a lone surrogate."""
    data = alert.model_dump()
    clean = scrub_surrogates(data)
    return alert if clean is data else Alert.model_validate(clean)


def _valid_url(url: str | None, limit: int = URL_LIMIT) -> bool:
    if not url or len(url) > limit or any(ch.isspace() for ch in url):
        return False
    try:
        parts = urlsplit(url)
    except ValueError:  # e.g. "https://[broken" (unbalanced IPv6 bracket)
        return False
    return parts.scheme in ("http", "https") and bool(parts.netloc)


def _field_text(text: str, limit: int) -> str:
    """Truncated field name/value; Discord rejects empty *and* whitespace-only ones."""
    out = truncate(text, limit)
    return out if out.strip() else ZWSP


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
    profile = f"**{_md(alert.profile_name, single_line=True)}**"
    if alert.variant_id:
        profile += f" · {_md(alert.variant_id, single_line=True)}"
    profile += f" · {_md(alert.category, single_line=True)}"
    lines.append(profile)
    risk = risk_summary(alert)
    if risk:
        lines.append(f"⚠️ Risk: {_md(risk, single_line=True)}")
    for line in alert.score.explain[:EXPLAIN_LINES]:
        line = sanitize_text(line, single_line=True)
        if line:
            lines.append(f"• {escape_markdown(line)}")
    return "\n".join(lines)


def build_embed(alert: Alert, *, node_id: str | None = None) -> dict[str, Any]:
    """The alert embed with every Discord limit enforced."""
    item = alert.item
    embed: dict[str, Any] = {
        "title": truncate(_md(headline(alert), single_line=True), TITLE_LIMIT),
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
            "name": _field_text(sanitize_text(fact.name, single_line=True), FIELD_NAME_LIMIT),
            "value": _field_text(_md(fact.value), FIELD_VALUE_LIMIT),
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
        text = truncate(sanitize_text(label, single_line=True), BUTTON_LABEL_LIMIT)
        buttons.append({"type": 2, "style": 5, "label": text or "Open", "url": url})
    rows = [{"type": 1, "components": buttons[i : i + BUTTONS_PER_ROW]} for i in range(0, len(buttons), BUTTONS_PER_ROW)]
    return rows[:ACTION_ROWS_LIMIT]


def username_allowed(username: str) -> bool:
    """Discord refuses webhook username overrides containing "discord" or "clyde"."""
    lowered = username.lower()
    return not any(part in lowered for part in FORBIDDEN_USERNAME_PARTS)


def _base_payload(cfg: DiscordTarget) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    username = truncate(cfg.username.strip(), USERNAME_LIMIT) or "DealRadar"
    if username_allowed(username):  # else the webhook's own name is used
        payload["username"] = username
    if _valid_url(cfg.avatar_url):
        payload["avatar_url"] = cfg.avatar_url
    return payload


def build_discord_payload(alert: Alert, cfg: DiscordTarget, *, mention: bool, node_id: str | None = None) -> dict[str, Any]:
    """Webhook execute payload for ``alert`` (pure; used by the dispatcher and tests).

    ``allowed_mentions`` is always explicit and ``flags`` is never set (no
    ``IS_COMPONENTS_V2``: it forbids ``embeds``), see the module docstring.
    """
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
    """Degraded copy for a retry after a 400: no components, thumbnail, embed URL or identity override."""
    out = {k: v for k, v in payload.items() if k not in ("components", "username", "avatar_url")}
    embeds = []
    for embed in payload.get("embeds", ()):
        embeds.append({k: v for k, v in embed.items() if k not in ("thumbnail", "url", "image")})
    if embeds:
        out["embeds"] = embeds
    return out


def build_notice_payload(title: str, message: str, cfg: DiscordTarget) -> dict[str, Any]:
    """Operator notice: one plain embed, never pings anybody."""
    embed: dict[str, Any] = {
        "title": truncate(_md(title, single_line=True) or "Notice", TITLE_LIMIT),
        "description": truncate(sanitize_text(message).strip() or ZWSP, DESCRIPTION_LIMIT),
        "color": NOTICE_COLOR,
        "timestamp": _iso(utcnow()),
        "footer": {"text": "DealRadar notice"},
    }
    _enforce_total(embed)
    payload = _base_payload(cfg)
    payload.update({"content": "", "allowed_mentions": {"parse": []}, "embeds": [embed]})
    return scrub_surrogates(payload)


def canonical_webhook_url(url: str) -> str:
    """Rebuild a Discord webhook URL onto ``https://discord.com/api/v10/webhooks/<id>/<token>``.

    Accepts the unversioned UI copy, older ``/api/vN`` URLs and the ``discordapp.com``
    / ``ptb.`` / ``canary.`` hosts; the query (e.g. ``thread_id``) is kept. URLs on any
    other host are returned unchanged (self-hosted relays). Raises ``ValueError`` for
    a non-http(s) URL or a Discord URL that is not a webhook; the message never echoes
    the URL, which is the credential.
    """
    if not _valid_url(url, limit=max(URL_LIMIT, len(url))):
        raise ValueError("invalid webhook URL: expected an http(s) URL")
    parts = urlsplit(url)
    if (parts.hostname or "").lower() not in DISCORD_WEBHOOK_HOSTS:
        return url
    match = _WEBHOOK_EXECUTE_PATH.fullmatch(parts.path)
    if match is None:
        raise ValueError("invalid webhook URL: expected https://discord.com/api/webhooks/<id>/<token>")
    webhook_id, token = match.groups()
    path = f"/api/v{DISCORD_API_VERSION}/webhooks/{webhook_id}/{token}"
    return urlunsplit(("https", "discord.com", path, parts.query, ""))


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
        self._requests = self.metrics.counter("discord_requests_total", "Discord webhook requests", ("target", "status"))
        self._rate_limited = self.metrics.counter("discord_rate_limited_total", "Discord 429 responses", ("target",))
        self._thread_id = (cfg.thread_id or "").strip() or None
        self._url: str | None = None  # canonical execute URL (v10); None when unset or invalid
        secret = self._secret()
        if secret is not None:
            try:
                self._url = canonical_webhook_url(secret)
            except ValueError:
                # Never echo the URL: it is the credential. The router skips unconfigured targets.
                self._disabled_reason = "invalid webhook URL (expected https://discord.com/api/webhooks/<id>/<token>)"
                log.error("discord webhook_url is not a webhook URL; target disabled", extra={"target": self.target})
        self._secrets = self._secret_forms()
        if cfg.mention_role_id and _snowflake(cfg.mention_role_id) is None:
            log.warning("discord mention_role_id is not a numeric role id; mentions disabled", extra={"target": self.target})
        if not username_allowed(cfg.username):
            log.warning(
                "discord refuses webhook usernames containing 'discord' or 'clyde'; using the webhook's own name",
                extra={"target": self.target},
            )

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
        # Stripped: Docker secrets / .env values often end with a newline.
        return self.cfg.webhook_url.get_secret_value().strip() if self.cfg.webhook_url is not None else None

    def _secret_forms(self) -> list[tuple[str, str]]:
        """(needle, replacement) pairs: the URLs and the token, raw and percent-encoded."""
        secret = self._secret()
        if not secret:
            return []
        forms: list[tuple[str, str]] = []
        for url in dict.fromkeys(u for u in (secret, self._url) if u):
            forms += [(url, "<webhook>"), (quote(url, safe=":/?&="), "<webhook>")]
        for url in dict.fromkeys(u for u in (self._url, secret) if u):
            tail = url.split("?", 1)[0].split("#", 1)[0].rstrip("/").rsplit("/", 1)[-1]
            if len(tail) >= 8:  # the token; also catches it inside URLs normalised by yarl
                forms += [(tail, "<redacted>"), (quote(tail, safe=""), "<redacted>")]
        return forms

    def _redact(self, text: str) -> str:
        for needle, replacement in self._secrets:
            text = text.replace(needle, replacement)
        return _WEBHOOK_PATH.sub(r"\1<redacted>", text)

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
            payload = build_discord_payload(scrub_alert(alert), self.cfg, mention=alert.mention, node_id=self.node_id)
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
        # Scaled down for tiny budgets so the first attempt always goes out.
        min_attempt = min(MIN_ATTEMPT_SECONDS, self.timeout / 2)
        if self._secret() is None:
            return DispatchResult(target=self.target, ok=False, error="not configured")
        if self._disabled_reason is not None or self._url is None:
            return DispatchResult(target=self.target, ok=False, error=f"disabled: {self._disabled_reason or 'invalid webhook URL'}")
        base_url = self._url

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
                if time.monotonic() + wait + min_attempt > deadline:
                    error = f"rate limited for {wait:.2f}s (exceeds dispatch budget)"
                    break
                await asyncio.sleep(wait)
            remaining = deadline - time.monotonic()
            if remaining < min_attempt:
                error = error or "dispatch budget exhausted"
                break
            attempts += 1
            url = webhook_url(base_url, with_components="components" in attempt.payload, thread_id=self._thread_id)
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
            except NOT_SENT_ERRORS as exc:
                # The TCP/TLS connection was never established, so nothing was posted:
                # safe to retry without risking a duplicate message.
                status = None
                error = f"connection failed: {exc!r}"
                delay = SERVER_ERROR_BASE_DELAY * (2 ** (attempts - 1))
                if time.monotonic() + delay + min_attempt <= deadline:
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
                # No log.exception: the traceback could quote the webhook URL; log a redacted copy.
                log.error(
                    "discord send crashed",
                    extra={
                        "target": self.target,
                        "alert_id": alert_id,
                        "error": self._redact(error)[:300],
                        "traceback": self._redact("".join(traceback.format_exception(exc)))[-4000:],
                    },
                )
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
                if time.monotonic() + delay + min_attempt <= deadline:
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
    "NOT_SENT_ERRORS",
    "DiscordDispatcher",
    "build_discord_payload",
    "build_embed",
    "build_notice_payload",
    "canonical_webhook_url",
    "embed_length",
    "escape_markdown",
    "link_button_rows",
    "minimal_payload",
    "sanitize_text",
    "scrub_alert",
    "scrub_surrogates",
    "text_length",
    "truncate",
    "username_allowed",
    "webhook_url",
]
