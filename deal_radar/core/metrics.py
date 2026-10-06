"""Zero-dependency metrics registry with Prometheus text exposition.

Everything runs on one asyncio event loop, so the collectors are intentionally
lock-free. ``Metrics.render()`` produces the Prometheus 0.0.4 text format served on
``/metrics``; ``Metrics.snapshot()`` returns a JSON-friendly dict for ``/status``.
"""

from __future__ import annotations

import math
import time
from collections.abc import Iterable, Sequence

LabelKey = tuple[tuple[str, str], ...]

DEFAULT_LATENCY_BUCKETS_MS: tuple[float, ...] = (
    0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000,
)


def _label_key(labelnames: Sequence[str], labels: dict[str, object]) -> LabelKey:
    unknown = set(labels) - set(labelnames)
    if unknown:
        raise ValueError(f"unknown label(s) {sorted(unknown)}; expected {list(labelnames)}")
    return tuple((name, str(labels.get(name, ""))) for name in labelnames)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_labels(key: LabelKey, extra: Iterable[tuple[str, str]] = ()) -> str:
    pairs = [*key, *extra]
    if not pairs:
        return ""
    return "{" + ",".join(f'{k}="{_escape(v)}"' for k, v in pairs) + "}"


def _fmt(value: float) -> str:
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if value == int(value) and abs(value) < 1e15:
        return str(int(value))
    return repr(float(value))


class _Collector:
    kind = "untyped"

    def __init__(self, name: str, documentation: str, labelnames: Sequence[str] = ()) -> None:
        self.name = name
        self.documentation = documentation
        self.labelnames = tuple(labelnames)

    def header(self) -> list[str]:
        return [f"# HELP {self.name} {self.documentation}", f"# TYPE {self.name} {self.kind}"]


class Counter(_Collector):
    kind = "counter"

    def __init__(self, name: str, documentation: str, labelnames: Sequence[str] = ()) -> None:
        super().__init__(name, documentation, labelnames)
        self._values: dict[LabelKey, float] = {}

    def inc(self, amount: float = 1.0, **labels: object) -> None:
        if amount < 0:
            raise ValueError("counters can only increase")
        key = _label_key(self.labelnames, labels)
        self._values[key] = self._values.get(key, 0.0) + amount

    def value(self, **labels: object) -> float:
        return self._values.get(_label_key(self.labelnames, labels), 0.0)

    def render(self) -> list[str]:
        return [*self.header(), *(f"{self.name}{_format_labels(k)} {_fmt(v)}" for k, v in self._values.items())]

    def snapshot(self) -> dict[str, float]:
        return {_format_labels(k) or "_": v for k, v in self._values.items()}


class Gauge(_Collector):
    kind = "gauge"

    def __init__(self, name: str, documentation: str, labelnames: Sequence[str] = ()) -> None:
        super().__init__(name, documentation, labelnames)
        self._values: dict[LabelKey, float] = {}

    def set(self, value: float, **labels: object) -> None:
        self._values[_label_key(self.labelnames, labels)] = float(value)

    def inc(self, amount: float = 1.0, **labels: object) -> None:
        key = _label_key(self.labelnames, labels)
        self._values[key] = self._values.get(key, 0.0) + amount

    def dec(self, amount: float = 1.0, **labels: object) -> None:
        self.inc(-amount, **labels)

    def value(self, **labels: object) -> float:
        return self._values.get(_label_key(self.labelnames, labels), 0.0)

    def render(self) -> list[str]:
        return [*self.header(), *(f"{self.name}{_format_labels(k)} {_fmt(v)}" for k, v in self._values.items())]

    def snapshot(self) -> dict[str, float]:
        return {_format_labels(k) or "_": v for k, v in self._values.items()}


class Histogram(_Collector):
    kind = "histogram"

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Sequence[str] = (),
        buckets: Sequence[float] = DEFAULT_LATENCY_BUCKETS_MS,
    ) -> None:
        super().__init__(name, documentation, labelnames)
        ordered = sorted(float(b) for b in buckets)
        if not ordered or ordered[-1] != math.inf:
            ordered.append(math.inf)
        self.buckets = tuple(ordered)
        self._counts: dict[LabelKey, list[int]] = {}
        self._sums: dict[LabelKey, float] = {}

    def observe(self, value: float, **labels: object) -> None:
        key = _label_key(self.labelnames, labels)
        counts = self._counts.get(key)
        if counts is None:
            counts = [0] * len(self.buckets)
            self._counts[key] = counts
            self._sums[key] = 0.0
        for i, bound in enumerate(self.buckets):
            if value <= bound:
                counts[i] += 1
                break
        self._sums[key] += value

    def count(self, **labels: object) -> int:
        return sum(self._counts.get(_label_key(self.labelnames, labels), []))

    def quantile(self, q: float, **labels: object) -> float | None:
        """Bucket-interpolated quantile estimate (same method as PromQL histogram_quantile)."""
        counts = self._counts.get(_label_key(self.labelnames, labels))
        if not counts:
            return None
        total = sum(counts)
        if total == 0:
            return None
        rank = q * total
        cumulative = 0
        lower = 0.0
        for bound, c in zip(self.buckets, counts):
            if cumulative + c >= rank and c > 0:
                if math.isinf(bound):
                    return lower
                return lower + (bound - lower) * ((rank - cumulative) / c)
            cumulative += c
            lower = bound if not math.isinf(bound) else lower
        return lower

    def render(self) -> list[str]:
        lines = self.header()
        for key, counts in self._counts.items():
            cumulative = 0
            for bound, c in zip(self.buckets, counts):
                cumulative += c
                lines.append(f"{self.name}_bucket{_format_labels(key, [('le', _fmt(bound))])} {cumulative}")
            lines.append(f"{self.name}_sum{_format_labels(key)} {_fmt(self._sums[key])}")
            lines.append(f"{self.name}_count{_format_labels(key)} {cumulative}")
        return lines

    def snapshot(self) -> dict[str, dict[str, float | None]]:
        out: dict[str, dict[str, float | None]] = {}
        for key in self._counts:
            labels = dict(key)
            out[_format_labels(key) or "_"] = {
                "count": float(self.count(**labels)),
                "sum": self._sums[key],
                "p50": self.quantile(0.5, **labels),
                "p95": self.quantile(0.95, **labels),
                "p99": self.quantile(0.99, **labels),
            }
        return out


class Metrics:
    """Get-or-create registry. Re-registering a name returns the existing collector."""

    def __init__(self, namespace: str = "dealradar") -> None:
        self.namespace = namespace
        self._collectors: dict[str, _Collector] = {}
        self.started_at = time.time()

    def _full(self, name: str) -> str:
        return f"{self.namespace}_{name}" if self.namespace else name

    def _get_or_create(self, cls: type, name: str, documentation: str, labelnames: Sequence[str], **kw: object):
        full = self._full(name)
        existing = self._collectors.get(full)
        if existing is not None:
            if not isinstance(existing, cls):
                raise TypeError(f"metric {full} already registered as {existing.kind}")
            return existing
        collector = cls(full, documentation, labelnames, **kw)
        self._collectors[full] = collector
        return collector

    def counter(self, name: str, documentation: str = "", labelnames: Sequence[str] = ()) -> Counter:
        return self._get_or_create(Counter, name, documentation or name, labelnames)

    def gauge(self, name: str, documentation: str = "", labelnames: Sequence[str] = ()) -> Gauge:
        return self._get_or_create(Gauge, name, documentation or name, labelnames)

    def histogram(
        self,
        name: str,
        documentation: str = "",
        labelnames: Sequence[str] = (),
        buckets: Sequence[float] = DEFAULT_LATENCY_BUCKETS_MS,
    ) -> Histogram:
        return self._get_or_create(Histogram, name, documentation or name, labelnames, buckets=buckets)

    def render(self) -> str:
        lines: list[str] = []
        for collector in self._collectors.values():
            lines.extend(collector.render())  # type: ignore[attr-defined]
        uptime = f"{self._full('uptime_seconds')}"
        lines.append(f"# HELP {uptime} Seconds since process start")
        lines.append(f"# TYPE {uptime} gauge")
        lines.append(f"{uptime} {_fmt(round(time.time() - self.started_at, 3))}")
        return "\n".join(lines) + "\n"

    def snapshot(self) -> dict[str, object]:
        return {name: c.snapshot() for name, c in self._collectors.items()}  # type: ignore[attr-defined]


__all__ = ["Counter", "DEFAULT_LATENCY_BUCKETS_MS", "Gauge", "Histogram", "Metrics"]
