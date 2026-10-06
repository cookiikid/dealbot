"""Robust, explainable price-anomaly scoring on top of a depreciation-aware price history.

This module is the mathematical core of DealRadar. :class:`AnomalyScorer` turns a
classified listing (:class:`DealItem` + :class:`FilterResult`, optionally a
:class:`VisionResult` from the local GPU model) into a 0-100 score, a severity, a
noisy-OR scam risk and a list of human-readable reasons. :class:`PriceHistory` learns
the market from the listings the scorer accepts. Everything is pure CPU, allocation
-light and cached, so a score costs tens of microseconds (budget: < 1 ms).

Symbols
-------
==============  ============================================================================
``P``           total price of the listing (``price + shipping``; unknown shipping = 0)
``band``        the profile's static price band for the matched variant
                (``floor < target <= ceiling`` plus ``reference_{new,used,refurb}``)
``class``       condition market class of the item: ``new | refurb | used | parts``. Every
                (product, class) pair has its own history, so used cards never move the
                new-card market (``history_key = "<profile>[:<variant>]|<class>"``).
``x_i, t_i``    historical sample price / observation time (one sample per listing)
``s_i``         evidence weight of the sample's source (``scoring.source_history_weights``,
                default 0.7): a retailer's price is stronger evidence than a FB asking price
``h, W``        ``history_half_life_days``, ``history_window_days``
``n0``          ``reference_prior_strength`` (pseudo-sample size of the config reference)
``λ, γ``        ``confidence_floor``, ``risk_exponent``
==============  ============================================================================

1. Price history (time-decayed, weighted, robust)
-------------------------------------------------
Sample weight::

    w_i = s_i · 2^(-(t_now - t_i) / h)

Samples older than ``W`` are pruned and each key keeps at most
``history_max_samples`` samples (oldest dropped first). A re-observed listing replaces
its own sample (a listing is one piece of evidence however often it is polled).

*Weighted quantile* ``Q(q)``: sort the samples by price, give sample ``i`` the
plotting position ``p_i = (C_i - w_i/2) / Σw`` where ``C_i`` is the cumulative weight
up to and including ``i`` (with equal weights this is Hazen's ``(i - ½)/n``), and
interpolate linearly between the two adjacent order statistics whose positions bracket
``q`` (clamped to the extremes outside ``[p_1, p_n]``). Then::

    median m = Q(½)      q1 = Q(¼)      q3 = Q(¾)      IQR = q3 - q1
    MAD      = weighted median of |x_i - m|       (same weights)
    n_eff    = (Σ w_i)² / Σ w_i²                   (Kish effective sample size)

Every ``w_i`` shares the factor ``2^(-t_now/h)``, and all of the statistics above are
invariant to a common scale of the weights. They are therefore *independent of*
``t_now``: they are cached per key (weights are computed relative to the newest
sample, which also avoids float overflow) and invalidated only when a sample is
added, replaced or pruned. ``t_now`` only decides which samples fall out of the window.

2. Market reference ``M`` (shrinkage toward the curated reference)
------------------------------------------------------------------
With ``stats`` = the history of ``(product, class)`` and ``ref = band.reference_for(class)``::

    enough   = stats.n >= min_samples_for_stats
    c_hist   = 1 - exp(-n_eff / n0)                     (0 when not enough)
    M        = c_hist · m + (1 - c_hist) · ref          basis "blend"   (enough, ref)
             = m                                        basis "history" (enough, no ref)
             = ref                                      basis "config"  (not enough)
             = m                                        basis "history" (no ref, n >= 2)
             = undefined                                basis "none"

3. Opportunity ``O`` (how good is the price?)
---------------------------------------------
::

    Δ      = (M - P) / M                                relative discount vs market
    D      = clamp(Δ / discount_saturation, 0, 1)
    z      = (m - P) / max(1.4826 · MAD, 0.02 · m)      robust z-score   (enough stats only)
    Zc     = clamp((z - z_low) / (z_high - z_low), 0, 1)
    I      = clamp((q1 - P) / (3 · IQR), 0, 1)          (I = Zc when IQR = 0)
    S_stat = (Zc + I) / 2                               (S_stat = D without enough stats)
    T      = 1 if P <= target; 0 if P >= ceiling; else (ceiling - P) / (ceiling - target)
    O      = w_d · D + w_s · S_stat + w_t · T           (weights sum to 1)

4. Confidence ``C`` (how much do we believe the inputs?)
--------------------------------------------------------
::

    c_src   = reliability(source)       (+ (1 - c_src) · vision_trust_boost when the vision
                                          model says GENUINE with confidence >= 0.6)
    c_match = FilterResult.match_strength
    c_ref   = 0.5 + 0.5 · c_hist  (enough history) | 0.5 (config reference only)
              | 0.35 (thin history, no reference)  | 0.25 (no reference at all)
    C       = c_src · c_match · c_ref

5. Risk ``R`` (noisy-OR over independent scam / quality evidence)
-----------------------------------------------------------------
::

    R = 1 - Π_i (1 - p_i)

over the text-filter signals plus: ``bait_price`` (P < floor), ``placeholder_price``
(the asking price is within $0.005 of a keyboard-mash value such as 1234/9999 on a
seller-priced source, i.e. LOCAL or MARKETPLACE, or P <= $5 for a product whose floor
exceeds $50 on any source), ``extreme_discount_low_trust`` (Δ >= threshold on a source
whose *base* reliability is below ``low_trust_reliability``), ``low_feedback``
(MARKETPLACE seller below ``min_seller_feedback``), ``poor_feedback_pct`` (seller
positive-feedback % below the minimum; ignored for sellers with zero feedback, whose
percentage is meaningless), ``no_image`` (LOCAL listing without photos),
``vision_<verdict>`` for a negative vision verdict (table probability when the model
confidence >= ``vision.negative_min_confidence``, else ``table · confidence · ½``) and
``vision_unverified`` when the vision backend failed. Probabilities come from
``scoring.risk_probabilities``; a code reported twice counts once (max probability).

6. Final score
--------------
::

    S = clamp(100 · O · (λ + (1 - λ) · C) · (1 - R)^γ, 0, 100)

7. Gates, price errors and severity
-----------------------------------
* Hard gates (score still computed for analytics, ``severity = None``,
  ``rejected = True``), first match wins: the text filter rejected the item
  (its ``reject_code``), ``P <= 0`` -> ``non_positive_price``, ``P > ceiling`` ->
  ``above_ceiling``, ``R >= reject_risk`` -> ``scam_risk``, ``in_stock is False`` ->
  ``out_of_stock``.
* Price error: ``Δ >= price_error_discount`` and base ``c_src >=
  price_error_min_reliability`` and ``R <= price_error_max_risk`` and class in
  {new, refurb} -> ``is_price_error``, severity forced to CRITICAL and
  ``S <- max(S, severity.critical)`` so the number agrees with the severity
  (``components["S_model"]`` keeps the un-lifted model score).
* Severity: ``min = profile.min_score or severity.medium``; no alert when ``S < min``;
  otherwise ``S >= critical`` -> CRITICAL, ``>= high`` -> HIGH,
  ``>= max(min, medium)`` -> MEDIUM.

Rationale
---------
* **Robust statistics.** The median has a 50 % breakdown point and the IQR 25 %: a
  handful of $150 "box only" scams or "$9999 don't lowball me" anchors cannot drag the
  market estimate, whereas a single outlier moves a mean/stdev arbitrarily far.
  ``1.4826 · MAD`` is a consistent estimator of σ under normality, so ``z`` reads like a
  classical z-score; the ``0.02 · m`` floor keeps the scale sane when many identical
  prices make ``MAD = 0`` (no division by zero, no infinite z for a one-cent change).
* **Exponential time decay** models hardware depreciation and market drift (new
  generations, launches, tariffs): with ``h = 14`` days a two-week-old price counts
  half, and the window bounds memory and staleness.
* **Kish effective sample size.** Thirty month-old FB asking prices are weaker evidence
  than their raw count suggests; ``n_eff`` is the size of an equally-weighted sample
  with the same variance of the weighted mean, so it measures real evidence.
* **Shrinkage** (credibility weighting): a cold key leans on the curated reference;
  as evidence accumulates ``c_hist`` -> 1 and the learned median takes over
  (``n0 = 8``: n_eff 2 -> 0.22, 8 -> 0.63, 24 -> 0.95). The exponential form saturates
  smoothly and never overshoots.
* **Three opportunity views.** ``D`` is the relative discount (saturating at 45 % off,
  beyond which a listing is a price error or a scam and other terms decide), ``S_stat``
  says how *unusual* the price is given the observed dispersion (20 % off in a tight
  market is rarer than in a noisy one), and ``T`` ties the score to the buyer's own
  target/ceiling.
* **Noisy-OR** treats each signal as an independent cause that alone could make the
  listing bad: monotone, order-independent, bounded by 1 however many weak signals fire.
* **(1 - R)^γ** punishes risk super-linearly (γ = 2: R = 0.3 keeps 49 %, R = 0.6 keeps
  16 %), so a great price with moderate scam risk falls below the alert thresholds
  instead of paging someone.
* **λ confidence floor**: low confidence scales a score down, never to zero; a genuine
  $900 RTX 4090 on Craigslist is still worth seeing (λ = 0.4 keeps 40 % of the
  opportunity of the least trusted listing).
* **Price errors** are a retail phenomenon: only trusted sources selling new/refurb
  stock qualify, and seller-typed placeholder prices are not checked on retailer feeds
  because a retailer's "$999" is a real price, not a keyboard mash.
"""

from __future__ import annotations

import heapq
import math
import time
from bisect import bisect_right
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from deal_radar.config_schema import AppConfig, PriceBand, Profile, SourcesSection
from deal_radar.core.logs import get_logger
from deal_radar.engine.types import (
    DealItem,
    FilterResult,
    RiskSignal,
    ScoreResult,
    Severity,
    SourceKind,
    VisionResult,
    VisionVerdict,
)

log = get_logger("deal_radar.engine.anomaly")

# --------------------------------------------------------------------------- constants

MAD_TO_SIGMA = 1.4826  # consistency constant: 1.4826·MAD estimates σ for normal data
MIN_SCALE_FRACTION = 0.02  # robust-z scale floor as a share of the median (MAD = 0 guard)
IQR_SPAN = 3.0  # (q1 - P) / (3·IQR) saturates three IQRs below the first quartile
GENUINE_MIN_CONFIDENCE = 0.6  # GENUINE vision verdicts below this give no trust boost
DEFAULT_RELIABILITY = 0.7  # c_src of a source without configuration
C_REF_CONFIG = 0.5  # c_ref when only the config reference is available
C_REF_THIN_HISTORY = 0.35  # c_ref for a history below min_samples and no reference
C_REF_NONE = 0.25  # c_ref without any reference
PLACEHOLDER_TOLERANCE = 0.005  # $ distance to a placeholder value that counts as a match
PLACEHOLDER_LOW_PRICE = 5.0  # "$1"-style prices ...
PLACEHOLDER_LOW_FLOOR = 50.0  # ... are placeholders for products whose floor exceeds this
SELLER_PRICED_KINDS = frozenset({SourceKind.LOCAL, SourceKind.MARKETPLACE})
PRICE_ERROR_CLASSES = frozenset({"new", "refurb"})
REQUIRED_COMPONENTS = ("D", "S_stat", "Zc", "I", "T", "O", "C", "c_src", "c_match", "c_ref", "R")

_SECONDS_PER_DAY = 86_400.0
_LN2 = math.log(2.0)


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return low if value < low else high if value > high else value


def _epoch(ts: datetime | float | int) -> float:
    """Seconds since the epoch; naive datetimes are taken as UTC (SQLite returns them)."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.timestamp()
    if isinstance(ts, bool) or not isinstance(ts, (int, float)):
        raise TypeError(f"timestamp must be a datetime or epoch seconds, got {type(ts).__name__}")
    return float(ts)


def _money(value: float) -> str:
    return f"${value:,.0f}" if abs(value) >= 100 else f"${value:,.2f}"


def history_key(product_key: str, market_class: str) -> str:
    """History bucket of a product (``profile[:variant]``) in one condition class."""
    return f"{product_key}|{market_class}"


def weighted_quantiles(values: Sequence[float], weights: Sequence[float], qs: Sequence[float]) -> list[float]:
    """Weighted quantiles of *values* (sorted ascending) with interpolation.

    Sample ``i`` sits at plotting position ``(C_i - w_i/2) / Σw`` (Hazen's ``(i - ½)/n``
    for equal weights); a quantile between two positions is interpolated linearly
    between the adjacent order statistics and clamped to the extremes outside them.
    Zero weights are allowed (such samples never pull a quantile towards themselves
    except as an interpolation end point); the total weight must be positive.
    """
    n = len(values)
    if n == 0:
        raise ValueError("weighted_quantiles needs at least one value")
    if n == 1:
        return [float(values[0])] * len(qs)
    total = 0.0
    positions: list[float] = []
    for w in weights:
        positions.append(total + 0.5 * w)
        total += w
    if total <= 0:
        raise ValueError("weighted_quantiles needs a positive total weight")
    inv = 1.0 / total
    positions = [p * inv for p in positions]
    first, last = positions[0], positions[-1]
    out: list[float] = []
    for q in qs:
        if q <= first:
            out.append(float(values[0]))
            continue
        if q >= last:
            out.append(float(values[-1]))
            continue
        j = bisect_right(positions, q)  # positions[j-1] <= q < positions[j]
        lo, hi = positions[j - 1], positions[j]
        x_lo, x_hi = values[j - 1], values[j]
        frac = (q - lo) / (hi - lo) if hi > lo else 0.0
        out.append(x_lo + frac * (x_hi - x_lo))
    return out


# --------------------------------------------------------------------------- history


@dataclass(frozen=True, slots=True)
class HistoryStats:
    """Time-decayed weighted summary of one ``(product, condition class)`` history."""

    n: int  # raw number of samples (listings) in the window
    n_eff: float  # Kish effective sample size (Σw)² / Σw²
    median: float
    q1: float
    q3: float
    mad: float  # weighted median absolute deviation from the median
    iqr: float  # q3 - q1
    min: float
    max: float


class _Bucket:
    """Samples of one history key plus the cached statistics."""

    __slots__ = ("samples", "oldest", "newest", "cached")

    def __init__(self) -> None:
        # listing_key -> (price, epoch seconds, source weight)
        self.samples: dict[str, tuple[float, float, float]] = {}
        self.oldest = math.inf  # lower bound of the oldest timestamp (exact after a prune)
        self.newest = -math.inf  # exact newest timestamp
        self.cached: HistoryStats | None = None


class PriceHistory:
    """In-memory, per-key price history with exponential time decay.

    One sample per ``listing_key`` (a re-observed listing replaces its sample unless the
    update is older than what is stored, e.g. out-of-order bus delivery). Window pruning
    on :meth:`observe`/:meth:`load` is relative to the newest sample of the key, so the
    structure never depends on the wall clock; :meth:`stats` additionally prunes against
    ``now``. Statistics are cached per key and invalidated on every change.
    """

    def __init__(
        self,
        *,
        half_life_days: float,
        window_days: float,
        max_samples: int,
        source_weights: Mapping[str, float] | None = None,
        default_source_weight: float = 0.7,
    ) -> None:
        if half_life_days <= 0 or window_days <= 0:
            raise ValueError("half_life_days and window_days must be positive")
        if max_samples < 1:
            raise ValueError("max_samples must be >= 1")
        if default_source_weight < 0:
            raise ValueError("default_source_weight must be >= 0")
        self.half_life_days = float(half_life_days)
        self.window_days = float(window_days)
        self.max_samples = int(max_samples)
        self.default_source_weight = float(default_source_weight)
        self._source_weights = {k: float(v) for k, v in (source_weights or {}).items()}
        self._decay = _LN2 / (self.half_life_days * _SECONDS_PER_DAY)  # w ∝ exp(-decay · age)
        self._window_s = self.window_days * _SECONDS_PER_DAY
        self._buckets: dict[str, _Bucket] = {}

    # ------------------------------------------------------------------ public API

    def source_weight(self, source: str) -> float:
        """Evidence weight ``s_i`` of a source (0 disables learning from it)."""
        return self._source_weights.get(source, self.default_source_weight)

    def observe(self, key: str, listing_key: str, price: float, ts: datetime, source: str) -> bool:
        """Add or replace the sample of ``listing_key``; returns True when it was stored."""
        if not self._insert(key, listing_key, price, ts, source):
            return False
        bucket = self._buckets[key]
        self._enforce(key, bucket, bucket.newest - self._window_s)
        return listing_key in bucket.samples

    def stats(self, key: str, now: datetime | None = None) -> HistoryStats | None:
        """Time-decayed weighted statistics of *key*; ``None`` when it has no samples."""
        bucket = self._buckets.get(key)
        if bucket is None:
            return None
        now_s = time.time() if now is None else _epoch(now)
        if not self._enforce(key, bucket, now_s - self._window_s):
            return None
        if bucket.cached is None:
            bucket.cached = self._compute(bucket)
        return bucket.cached

    def weighted_samples(self, key: str, now: datetime | None = None) -> list[tuple[float, float]]:
        """``(price, weight)`` pairs with absolute weights ``s_i · 2^(-age/h)`` at *now*.

        Oldest first. Ages are clamped at zero for samples stamped after *now* (clock
        skew between collector nodes). Mainly for diagnostics and the ``/status`` API.
        """
        bucket = self._buckets.get(key)
        if bucket is None:
            return []
        now_s = time.time() if now is None else _epoch(now)
        if not self._enforce(key, bucket, now_s - self._window_s):
            return []
        ordered = sorted(bucket.samples.values(), key=lambda s: s[1])
        return [(price, sw * math.exp(-self._decay * max(0.0, now_s - t))) for price, t, sw in ordered]

    def load(self, rows: Iterable[tuple[str, str, float, datetime, str]]) -> int:
        """Warm start from persisted ``(key, listing_key, price, ts, source)`` rows.

        Malformed, non-positive, zero-weight and stale rows are skipped. Returns the
        number of loaded samples retained after window pruning and the size cap.
        """
        applied: dict[str, set[str]] = {}
        skipped = 0
        for row in rows:
            try:
                key, listing_key, price, ts, source = row
                stored = self._insert(str(key), str(listing_key), price, ts, str(source))
            except (TypeError, ValueError) as exc:
                skipped += 1
                log.debug("skipping malformed history row", extra={"error": str(exc)})
                continue
            if stored:
                applied.setdefault(str(key), set()).add(str(listing_key))
            else:
                skipped += 1
        retained = 0
        for key, listing_keys in applied.items():
            bucket = self._buckets.get(key)
            if bucket is None:
                continue
            if self._enforce(key, bucket, bucket.newest - self._window_s):
                retained += sum(1 for lk in listing_keys if lk in bucket.samples)
        if skipped:
            log.debug("history warm start skipped rows", extra={"skipped": skipped, "retained": retained})
        return retained

    def clear(self) -> None:
        self._buckets.clear()

    def __len__(self) -> int:
        return sum(len(b.samples) for b in self._buckets.values())

    def __contains__(self, key: object) -> bool:
        return key in self._buckets

    def keys(self) -> list[str]:
        return list(self._buckets)

    # ------------------------------------------------------------------ internals

    def _insert(self, key: str, listing_key: str, price: float, ts: datetime | float, source: str) -> bool:
        price_f = float(price)
        if not math.isfinite(price_f) or price_f <= 0:
            return False
        weight = self.source_weight(source)
        if not (weight > 0 and math.isfinite(weight)):
            return False
        t = _epoch(ts)
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = self._buckets[key] = _Bucket()
        else:
            previous = bucket.samples.get(listing_key)
            if previous is not None and previous[1] > t:
                return False  # older than what we already know about this listing
        bucket.samples[listing_key] = (price_f, t, weight)
        if t < bucket.oldest:
            bucket.oldest = t
        if t > bucket.newest:
            bucket.newest = t
        bucket.cached = None
        return True

    def _enforce(self, key: str, bucket: _Bucket, cutoff: float) -> bool:
        """Apply the window (samples older than *cutoff*) and the size cap.

        Returns False (and drops the key) when the bucket ends up empty.
        """
        samples = bucket.samples
        changed = False
        if bucket.oldest < cutoff:
            stale = [lk for lk, s in samples.items() if s[1] < cutoff]
            for lk in stale:
                del samples[lk]
            changed = bool(stale)
            bucket.oldest = min((s[1] for s in samples.values()), default=math.inf)
        excess = len(samples) - self.max_samples
        if excess > 0:
            if excess == 1:
                del samples[min(samples, key=lambda lk: samples[lk][1])]
            else:
                for lk, _ in heapq.nsmallest(excess, samples.items(), key=lambda kv: kv[1][1]):
                    del samples[lk]
            bucket.oldest = min((s[1] for s in samples.values()), default=math.inf)
            changed = True
        if not samples:
            del self._buckets[key]
            return False
        if changed:
            bucket.cached = None
        return True

    def _compute(self, bucket: _Bucket) -> HistoryStats:
        # Weights relative to the newest sample: the common factor 2^(-(t_now - t_newest)/h)
        # cancels in every normalised statistic, and exp() never overflows.
        anchor = bucket.newest
        decay = self._decay
        exp = math.exp
        pairs = sorted((price, sw * exp(decay * (t - anchor))) for price, t, sw in bucket.samples.values())
        values = [p for p, _ in pairs]
        weights = [w for _, w in pairs]
        total = math.fsum(weights)
        sum_sq = math.fsum(w * w for w in weights)
        n_eff = (total * total / sum_sq) if sum_sq > 0 else 0.0
        q1, median, q3 = weighted_quantiles(values, weights, (0.25, 0.5, 0.75))
        deviations = sorted((abs(p - median), w) for p, w in pairs)
        mad = weighted_quantiles([d for d, _ in deviations], [w for _, w in deviations], (0.5,))[0]
        return HistoryStats(
            n=len(values),
            n_eff=n_eff,
            median=median,
            q1=q1,
            q3=q3,
            mad=mad,
            iqr=max(0.0, q3 - q1),
            min=values[0],
            max=values[-1],
        )


# --------------------------------------------------------------------------- scorer


class AnomalyScorer:
    """Scores listings against the learned market and the profile's price band."""

    def __init__(self, config: AppConfig, history: PriceHistory | None = None) -> None:
        self.config = config
        sc = config.scoring
        self._sc = sc
        self.history: PriceHistory = (
            history
            if history is not None
            else PriceHistory(
                half_life_days=sc.history_half_life_days,
                window_days=sc.history_window_days,
                max_samples=sc.history_max_samples,
                source_weights=sc.source_history_weights,
            )
        )
        self._profiles: dict[str, Profile] = {p.id: p for p in config.profiles}
        self._bands: dict[tuple[str, str | None], PriceBand] = {}
        self._reliability: dict[str, float] = {}
        self._placeholders: tuple[float, ...] = tuple(sorted(float(v) for v in sc.placeholder_prices))
        self._risk_p: dict[str, float] = dict(sc.risk_probabilities)
        self._z_span = sc.z_high - sc.z_low
        self._source_names = frozenset(SourcesSection.model_fields)

    # ------------------------------------------------------------------ helpers

    def reliability(self, source: str) -> float:
        """``c_src``: ``config.sources.<name>.reliability`` (0.7 for unknown sources)."""
        cached = self._reliability.get(source)
        if cached is None:
            value = DEFAULT_RELIABILITY
            if source in self._source_names:
                rel = getattr(getattr(self.config.sources, source, None), "reliability", None)
                if isinstance(rel, (int, float)) and math.isfinite(rel):
                    value = _clamp(float(rel))
            cached = self._reliability[source] = value
        return cached

    def band(self, profile: Profile, variant_id: str | None) -> PriceBand:
        """``profile.band_for(variant)``, memoised (merging re-validates a model)."""
        key = (profile.id, variant_id)
        band = self._bands.get(key)
        if band is None:
            band = self._bands[key] = profile.band_for(variant_id)
        return band

    def severity_for(self, score: float, profile: Profile | None = None) -> Severity | None:
        """Map a score to a severity honouring ``profile.min_score``."""
        th = self._sc.severity
        floor = profile.min_score if profile is not None and profile.min_score is not None else th.medium
        if score < floor:
            return None
        if score >= th.critical:
            return Severity.CRITICAL
        if score >= th.high:
            return Severity.HIGH
        if score >= max(floor, th.medium):
            return Severity.MEDIUM
        return None

    def _is_placeholder(self, price: float) -> bool:
        values = self._placeholders
        i = bisect_right(values, price)
        return (i > 0 and price - values[i - 1] <= PLACEHOLDER_TOLERANCE) or (
            i < len(values) and values[i] - price <= PLACEHOLDER_TOLERANCE
        )

    def _p(self, code: str, default: float = 0.5) -> float:
        return self._risk_p.get(code, default)

    # ------------------------------------------------------------------ scoring

    def score(
        self,
        item: DealItem,
        fr: FilterResult,
        vision: VisionResult | None = None,
        *,
        now: datetime | None = None,
    ) -> ScoreResult:
        sc = self._sc
        profile = self._profiles.get(fr.profile_id) if fr.profile_id else None
        if profile is None or fr.product_key is None:
            reason = fr.reject_code or "unknown_profile"
            return ScoreResult(
                score=0.0,
                rejected=True,
                reject_reason=reason,
                explain=[f"rejected: {reason} (no scorable profile {fr.profile_id!r})"],
            )
        band = self.band(profile, fr.variant_id)
        market_class = item.condition.market_class
        price = item.total_price if math.isfinite(item.total_price) else 0.0
        explain: list[str] = []

        # -- market reference M (shrinkage of the learned median toward the config) ----
        stats = self.history.stats(history_key(fr.product_key, market_class), now)
        ref = band.reference_for(market_class)
        enough = stats is not None and stats.n >= sc.min_samples_for_stats
        c_hist = 0.0
        market: float | None
        if stats is not None and enough:
            c_hist = 1.0 - math.exp(-stats.n_eff / sc.reference_prior_strength)
            c_ref = 0.5 + 0.5 * c_hist
            if ref:
                market, basis = c_hist * stats.median + (1.0 - c_hist) * ref, "blend"
            else:
                market, basis = stats.median, "history"
        elif ref:
            market, basis, c_ref = ref, "config", C_REF_CONFIG
        elif stats is not None and stats.n >= 2:
            market, basis, c_ref = stats.median, "history", C_REF_THIN_HISTORY
        else:
            market, basis, c_ref = None, "none", C_REF_NONE

        if market is not None:
            if basis == "blend" and stats is not None:
                explain.append(
                    f"market {_money(market)} = {c_hist:.2f}·median {_money(stats.median)} + "
                    f"{1 - c_hist:.2f}·ref {_money(ref or 0.0)} (n={stats.n}, n_eff={stats.n_eff:.1f})"
                )
            elif basis == "history" and stats is not None:
                explain.append(f"market {_money(market)} = history median (n={stats.n}, n_eff={stats.n_eff:.1f})")
            else:
                n_hist = stats.n if stats is not None else 0
                explain.append(f"market {_money(market)} = config reference ({market_class}; history n={n_hist})")
        else:
            explain.append("no market reference")

        # -- opportunity --------------------------------------------------------------
        delta: float | None = (market - price) / market if market is not None and market > 0 else None
        d_term = _clamp(delta / sc.discount_saturation) if delta is not None else 0.0
        if delta is not None:
            if delta >= 0:
                explain.append(f"{delta:.0%} below market (D={d_term:.2f})")
            else:
                explain.append(f"{-delta:.0%} above market")

        robust_z: float | None = None
        iqr_position: float | None = None
        if stats is not None and enough:
            scale = max(MAD_TO_SIGMA * stats.mad, MIN_SCALE_FRACTION * stats.median)
            robust_z = (stats.median - price) / scale if scale > 0 else 0.0
            zc = _clamp((robust_z - sc.z_low) / self._z_span)
            if stats.iqr > 0:
                iqr_position = (stats.q1 - price) / stats.iqr
                i_term = _clamp(iqr_position / IQR_SPAN)
            else:
                i_term = zc
            s_stat = (zc + i_term) / 2.0
            explain.append(
                f"robust z {robust_z:.1f} (MAD {_money(stats.mad)}), "
                + (f"{iqr_position:.1f} IQR below Q1 {_money(stats.q1)}" if iqr_position is not None else "IQR 0")
                + f" -> S_stat={s_stat:.2f}"
            )
        else:
            zc = i_term = 0.0
            s_stat = d_term
            n_hist = stats.n if stats is not None else 0
            explain.append(f"thin history (n={n_hist} < {sc.min_samples_for_stats}): S_stat falls back to D")

        if price <= band.target:
            t_term = 1.0
        elif price >= band.ceiling:
            t_term = 0.0
        else:
            t_term = (band.ceiling - price) / (band.ceiling - band.target)
        explain.append(f"target {_money(band.target)} / ceiling {_money(band.ceiling)} (T={t_term:.2f})")

        w = sc.weights
        opportunity = w.discount * d_term + w.statistical * s_stat + w.target * t_term

        # -- confidence ---------------------------------------------------------------
        c_src_base = self.reliability(item.source)
        c_src = c_src_base
        if vision is not None and vision.verdict is VisionVerdict.GENUINE and vision.confidence >= GENUINE_MIN_CONFIDENCE:
            c_src = c_src_base + (1.0 - c_src_base) * sc.vision_trust_boost
            explain.append(f"vision confirmed genuine ({vision.confidence:.2f}): source trust {c_src_base:.2f} -> {c_src:.2f}")
        c_match = _clamp(fr.match_strength)
        confidence = c_src * c_match * c_ref
        explain.append(f"confidence {confidence:.2f} = src {c_src:.2f} × match {c_match:.2f} × ref {c_ref:.2f}")

        # -- risk (noisy-OR) ----------------------------------------------------------
        signals = self._risk_signals(item, fr, vision, band, price, delta, c_src_base)
        survival = 1.0
        for sig in signals:
            survival *= 1.0 - sig.probability
        risk = _clamp(1.0 - survival)
        if signals:
            explain.append(
                f"risk {risk:.2f}: " + ", ".join(f"{s.code} {s.probability:.2f}" for s in signals)
            )

        # -- final score --------------------------------------------------------------
        lam = sc.confidence_floor
        penalty = (1.0 - risk) ** sc.risk_exponent
        model_score = round(_clamp(100.0 * opportunity * (lam + (1.0 - lam) * confidence) * penalty, 0.0, 100.0), 2)
        final_score = model_score

        # -- gates --------------------------------------------------------------------
        reject_reason: str | None = None
        if not fr.accepted:
            reject_reason = fr.reject_code or "filter_rejected"
        elif price <= 0:
            reject_reason = "non_positive_price"
        elif price > band.ceiling:
            reject_reason = "above_ceiling"
        elif risk >= sc.reject_risk:
            reject_reason = "scam_risk"
        elif item.in_stock is False:
            reject_reason = "out_of_stock"

        price_error = (
            delta is not None
            and delta >= sc.price_error_discount
            and c_src_base >= sc.price_error_min_reliability
            and risk <= sc.price_error_max_risk
            and market_class in PRICE_ERROR_CLASSES
        )

        severity: Severity | None = None
        if reject_reason is not None:
            explain.append(f"rejected: {reject_reason}" + (" (would be a price error)" if price_error else ""))
            price_error = False
        elif price_error:
            final_score = max(model_score, float(sc.severity.critical))
            severity = Severity.CRITICAL
            explain.append(f"PRICE ERROR: {delta:.0%} below market on a trusted source ({c_src_base:.2f})")
        else:
            severity = self.severity_for(final_score, profile)

        components = {
            "D": round(d_term, 4),
            "S_stat": round(s_stat, 4),
            "Zc": round(zc, 4),
            "I": round(i_term, 4),
            "T": round(t_term, 4),
            "O": round(opportunity, 4),
            "C": round(confidence, 4),
            "c_src": round(c_src, 4),
            "c_match": round(c_match, 4),
            "c_ref": round(c_ref, 4),
            "R": round(risk, 4),
            "c_hist": round(c_hist, 4),
            "penalty": round(penalty, 4),
            "S_model": model_score,
        }
        return ScoreResult(
            score=final_score,
            severity=severity,
            rejected=reject_reason is not None,
            reject_reason=reject_reason,
            market_price=round(market, 2) if market is not None else None,
            market_basis=basis,
            discount_pct=round(delta, 4) if delta is not None else None,
            robust_z=round(robust_z, 3) if robust_z is not None else None,
            iqr_position=round(iqr_position, 3) if iqr_position is not None else None,
            history_n=stats.n if stats is not None else 0,
            confidence=round(confidence, 4),
            risk=round(risk, 4),
            components=components,
            risk_signals=signals,
            is_price_error=price_error,
            explain=explain,
        )

    def _risk_signals(
        self,
        item: DealItem,
        fr: FilterResult,
        vision: VisionResult | None,
        band: PriceBand,
        price: float,
        delta: float | None,
        c_src_base: float,
    ) -> list[RiskSignal]:
        sc = self._sc
        found: list[RiskSignal] = list(fr.risk_signals)

        if price < band.floor:
            found.append(
                RiskSignal(
                    code="bait_price",
                    probability=self._p("bait_price"),
                    detail=f"{_money(price)} < floor {_money(band.floor)}",
                    origin="price",
                )
            )
        if (item.source_kind in SELLER_PRICED_KINDS and (self._is_placeholder(item.price) or self._is_placeholder(price))) or (
            0 < item.price <= PLACEHOLDER_LOW_PRICE and band.floor > PLACEHOLDER_LOW_FLOOR
        ):
            found.append(
                RiskSignal(
                    code="placeholder_price",
                    probability=self._p("placeholder_price"),
                    detail=f"asking price {_money(item.price)} looks like a placeholder",
                    origin="price",
                )
            )
        if delta is not None and delta >= sc.extreme_discount_threshold and c_src_base < sc.low_trust_reliability:
            found.append(
                RiskSignal(
                    code="extreme_discount_low_trust",
                    probability=self._p("extreme_discount_low_trust"),
                    detail=f"{delta:.0%} below market on a low-trust source ({c_src_base:.2f})",
                    origin="price",
                )
            )

        seller = item.seller
        if seller is not None:
            fb_score = seller.feedback_score
            if item.source_kind is SourceKind.MARKETPLACE and fb_score is not None and fb_score < sc.min_seller_feedback:
                found.append(
                    RiskSignal(
                        code="low_feedback",
                        probability=self._p("low_feedback"),
                        detail=f"seller feedback {fb_score} < {sc.min_seller_feedback}",
                        origin="seller",
                    )
                )
            pct = seller.feedback_pct
            if pct is not None and pct < sc.min_seller_feedback_pct and (fb_score is None or fb_score > 0):
                found.append(
                    RiskSignal(
                        code="poor_feedback_pct",
                        probability=self._p("poor_feedback_pct"),
                        detail=f"seller positive feedback {pct:.1f}% < {sc.min_seller_feedback_pct:.1f}%",
                        origin="seller",
                    )
                )

        if item.source_kind is SourceKind.LOCAL and not item.image_urls:
            found.append(RiskSignal(code="no_image", probability=self._p("no_image"), detail="no photos", origin="listing"))

        if vision is not None:
            if vision.verdict.is_negative:
                code = f"vision_{vision.verdict.value}"
                table = self._p(code)
                certain = vision.confidence >= self.config.vision.negative_min_confidence
                prob = table if certain else table * vision.confidence * 0.5
                found.append(
                    RiskSignal(
                        code=code,
                        probability=_clamp(prob),
                        detail=f"vision {vision.verdict.value} ({vision.confidence:.2f}, {vision.model or 'model'})",
                        origin="vision",
                    )
                )
            elif vision.verdict is VisionVerdict.ERROR:
                found.append(
                    RiskSignal(
                        code="vision_unverified",
                        probability=self._p("vision_unverified"),
                        detail=f"vision unavailable: {vision.error or 'error'}",
                        origin="vision",
                    )
                )

        # The same evidence reported twice (e.g. by the text filter and here) counts once.
        best: dict[str, RiskSignal] = {}
        for sig in found:
            current = best.get(sig.code)
            if current is None or sig.probability > current.probability:
                best[sig.code] = sig
        return list(best.values())

    # ------------------------------------------------------------------ learning

    def observe(self, item: DealItem, fr: FilterResult, result: ScoreResult) -> bool:
        """Feed a scored listing into the price history; returns True when stored.

        Only accepted, non-rejected (``above_ceiling`` excepted: an expensive listing is
        still a market price), low-risk (``risk <= history_max_risk``) items are learned.
        """
        product_key = fr.product_key
        if not fr.accepted or product_key is None:
            return False
        if result.rejected and result.reject_reason != "above_ceiling":
            return False
        if result.risk > self._sc.history_max_risk:
            return False
        price = item.total_price
        if not math.isfinite(price) or price <= 0:
            return False
        return self.history.observe(
            history_key(product_key, item.condition.market_class),
            item.listing_key,
            price,
            item.received_at,
            item.source,
        )


__all__ = [
    "AnomalyScorer",
    "HistoryStats",
    "PriceHistory",
    "history_key",
    "weighted_quantiles",
]
