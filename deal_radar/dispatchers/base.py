"""Dispatcher interface + shared alert formatting used by every channel.

All channels render the same facts in the same order so a human scanning Discord
on a desktop and Telegram on a phone reads identical numbers:

    headline:  "🚨 PRICE ERROR · RTX 4090 FE — $999.99 (-45% vs $1,818)"
    facts:     price, market, discount, score, condition, source, location, lag
    links:     Buy / View listing, eBay sold comps, Google Shopping compare
"""

from __future__ import annotations

import abc
import time
from dataclasses import dataclass
from urllib.parse import quote_plus

from deal_radar.core.logs import get_logger
from deal_radar.engine.types import Alert, DispatchResult, Severity

log = get_logger("dispatch")

SEVERITY_EMOJI = {Severity.CRITICAL: "🚨", Severity.HIGH: "🔥", Severity.MEDIUM: "💡"}
SEVERITY_COLOR = {Severity.CRITICAL: 0xE53935, Severity.HIGH: 0xFB8C00, Severity.MEDIUM: 0x1E88E5}


class Dispatcher(abc.ABC):
    """A delivery channel. ``target`` is the routing name, e.g. ``discord:gpu``."""

    target: str

    @abc.abstractmethod
    async def send(self, alert: Alert) -> DispatchResult:
        """Deliver an alert. Must not raise for delivery failures: return ``ok=False``."""

    async def send_notice(self, title: str, message: str) -> DispatchResult:
        """Deliver an operator notice (source blocked, circuit open...). Default: unsupported."""
        return DispatchResult(target=self.target, ok=False, error="notices not supported")

    @property
    def configured(self) -> bool:
        """False when credentials are missing; the router skips unconfigured targets."""
        return True

    async def close(self) -> None:
        """Release resources (connections, websockets)."""


# --------------------------------------------------------------------------- formatting


def money(value: float | None, currency: str = "USD") -> str:
    if value is None:
        return "?"
    symbol = "$" if currency == "USD" else f"{currency} "
    if abs(value - round(value)) < 0.005:
        return f"{symbol}{value:,.0f}"
    return f"{symbol}{value:,.2f}"


def pct(value: float | None) -> str:
    if value is None:
        return "?"
    return f"{value * 100:.0f}%"


def headline(alert: Alert) -> str:
    """One-line summary used as Discord embed title, Telegram first line and console log."""
    item = alert.item
    tag = "PRICE ERROR" if alert.score.is_price_error else alert.severity.value.upper()
    prefix = "PRICE DROP · " if alert.is_update else ""
    discount = alert.score.discount_pct
    vs = ""
    if discount is not None and alert.score.market_price:
        vs = f" (-{pct(discount)} vs {money(alert.score.market_price, item.currency)})" if discount > 0 else ""
    return f"{SEVERITY_EMOJI[alert.severity]} {tag} · {prefix}{money(item.total_price, item.currency)}{vs} — {item.title}"


@dataclass(frozen=True, slots=True)
class Fact:
    name: str
    value: str
    inline: bool = True


def facts(alert: Alert) -> list[Fact]:
    """Ordered key facts shared by all channels."""
    item = alert.item
    score = alert.score
    out = [
        Fact("Price", money(item.price, item.currency) + (f" + {money(item.shipping, item.currency)} ship" if item.shipping else "")),
        Fact("Market", money(score.market_price, item.currency) + (f" ({score.market_basis})" if score.market_price else "")),
        Fact("Discount", pct(score.discount_pct) if score.discount_pct is not None else "?"),
        Fact("Score", f"{score.score:.0f}/100 · conf {score.confidence:.2f} · risk {score.risk:.2f}"),
        Fact("Condition", item.condition.value.replace("_", " ")),
        Fact("Source", f"{item.source}" + (f" · {item.retailer}" if item.retailer else "")),
    ]
    if alert.is_update and alert.dedup.previous_price is not None:
        out.append(Fact("Was", money(alert.dedup.previous_price, item.currency)))
    if item.location is not None:
        loc = item.location.text or ", ".join(p for p in (item.location.city, item.location.region) if p)
        if item.location.distance_miles is not None:
            loc = f"{loc} ({item.location.distance_miles:.0f} mi)" if loc else f"{item.location.distance_miles:.0f} mi"
        if loc:
            out.append(Fact("Location", loc))
    if item.seller is not None and (item.seller.feedback_score is not None or item.seller.name):
        seller = item.seller.name or "?"
        if item.seller.feedback_score is not None:
            seller += f" ({item.seller.feedback_score}"
            seller += f", {item.seller.feedback_pct:.1f}%)" if item.seller.feedback_pct is not None else ")"
        out.append(Fact("Seller", seller))
    if alert.vision is not None and alert.vision.verdict.value not in ("skipped",):
        out.append(Fact("Vision", f"{alert.vision.verdict.value} ({alert.vision.confidence:.2f})"))
    lag = alert.ingest_lag_ms
    out.append(Fact("Latency", f"pipeline {alert.pipeline_ms:.0f} ms" + (f" · listed {_human_lag(lag)} ago" if lag is not None else "")))
    return out


def risk_summary(alert: Alert, limit: int = 3) -> str | None:
    signals = sorted(alert.score.risk_signals, key=lambda s: s.probability, reverse=True)[:limit]
    if not signals:
        return None
    return ", ".join(f"{s.code} {s.probability:.2f}" for s in signals)


def links(alert: Alert) -> list[tuple[str, str]]:
    """(label, url) pairs: primary action first."""
    item = alert.item
    out = [("Buy now" if item.outbound_url or item.source in ("retail",) else "Open listing", item.best_url)]
    if item.outbound_url and item.outbound_url != item.url:
        out.append(("Discussion", item.url))
    query = quote_plus(alert.profile_name if len(item.title) > 90 else item.title)
    out.append(("eBay sold", f"https://www.ebay.com/sch/i.html?_nkw={query}&LH_Sold=1&LH_Complete=1"))
    out.append(("Compare", f"https://www.google.com/search?tbm=shop&q={query}"))
    return out


def _human_lag(ms: float) -> str:
    seconds = max(0.0, ms / 1000.0)
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.0f}m"
    return f"{seconds / 3600:.1f}h"


class ConsoleDispatcher(Dispatcher):
    """Logs alerts; always available (dry-run mode routes everything here)."""

    target = "console"

    async def send(self, alert: Alert) -> DispatchResult:
        started = time.perf_counter()
        log.warning(
            headline(alert),
            extra={
                "alert_id": alert.alert_id,
                "profile": alert.product_key,
                "score": round(alert.score.score, 1),
                "url": alert.item.best_url,
                "facts": {f.name: f.value for f in facts(alert)},
            },
        )
        return DispatchResult(target=self.target, ok=True, latency_ms=(time.perf_counter() - started) * 1000)

    async def send_notice(self, title: str, message: str) -> DispatchResult:
        log.warning(f"[notice] {title}: {message}")
        return DispatchResult(target=self.target, ok=True)


__all__ = [
    "ConsoleDispatcher",
    "Dispatcher",
    "Fact",
    "SEVERITY_COLOR",
    "SEVERITY_EMOJI",
    "facts",
    "headline",
    "links",
    "money",
    "pct",
    "risk_summary",
]
