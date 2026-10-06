"""Deterministic text classification: product identification + scam/noise rules.

The text filter is the first CPU stage after normalisation and runs for *every*
listing a source emits, so it is built to be fast, explainable and boring:

* **Compile once.** Every regex from ``config.yaml`` is compiled in ``__init__``.
  The patterns of a rule group, of a profile's ``any``/``none`` lists and of a
  variant's ``match`` list are joined into ONE alternation ``(?:p1)|(?:p2)|...``
  so a single C-level scan replaces N Python-level calls. Patterns that would
  change meaning when concatenated (numbered back-references, named groups,
  leading global inline flags) are compiled on their own. ``all`` lists stay
  separate by definition (every one of them must match).
* **Case folding instead of IGNORECASE.** Config regexes are case-insensitive by
  contract, but ``re.IGNORECASE`` disables ``sre``'s literal/charset prefix scan and
  is 2-8x slower on long descriptions. Each pattern is therefore lower-cased once
  (escape sequences such as ``\\S``/``\\W``/``\\B`` are left untouched) and matched,
  without IGNORECASE, against a lower-cased copy of the listing made once per
  evaluation. Spans are mapped back onto the original text for ``reject_detail``.
  The few constructs for which this is not equivalent (scoped ``(?-i:...)``,
  ``\\x``/``\\u``/``\\N`` escapes, the locale flag) keep IGNORECASE on the original text.
* **Pre-indexed rule groups.** The reject/risk groups applicable to every
  ``(product category, source kind)`` pair are resolved at start-up, so
  :meth:`TextFilter.evaluate` never re-checks ``categories``/``source_kinds``.
* **Pure.** ``evaluate`` reads only the item and immutable compiled state. It has
  no side effects (no logging, metrics or caches) and is safe to call from any
  number of pipeline workers.

Evaluation order (the first rejection wins):

1. ``title_too_short`` when the stripped title is shorter than
   ``filters.min_title_length``.
2. Product identification over the enabled profiles (``match.any/all/none`` on
   ``match.field``). Candidates are ranked by ``priority`` (desc) then config order.
   The winner's first matching variant (``variant.field``) is selected; a
   ``variant_required`` profile without a matching variant yields to the next
   candidate, else ``variant_unknown``. Nothing matched -> ``no_profile_match``.
3. Condition gates: ``FOR_PARTS`` -> ``for_parts``; condition outside
   ``profile.conditions`` -> ``condition_not_allowed``.
4. ``reject`` groups in config order (respecting ``field``, ``categories``,
   ``source_kinds`` and ``negatable``): first match -> ``reject_code=<group>``,
   ``reject_detail=<matched text>``.
5. ``risk`` groups: one :class:`RiskSignal` per matching group (origin ``text``).
6. Title/price mismatch: the first explicit ``$`` price in the title vs
   ``item.price``; a ratio outside ``[0.5, 2.0]`` adds ``title_price_mismatch``
   (origin ``listing``).

``match_strength`` starts at 1.0, loses 0.15 when the profile has variants but none
matched and 0.1 when another profile of equal priority also matched; it is clamped
to ``[0.3, 1]``.

Negation (``negatable`` groups only) is evaluated per match, not by masking:

* A match is ignored when a configured negation term (multi-word terms allowed,
  e.g. ``free of``) ends within the previous ``negation_window_words`` words of
  the *same clause*. Clause boundaries are ``. ! ? ; ,`` (a ``.``/``,`` followed by a
  digit is a decimal/thousands separator, not a boundary), newlines and the words
  ``but``/``however``, so "no box, cracked screen" still rejects while "no dead
  pixels" does not.
* A negation term only negates what it governs: when one of a small set of
  *scope enders* sits between the term and the match ("no **box** cracked screen",
  "no **returns** untested", "no issues **except** dead pixels") the term is
  attached to that word and does not negate the match.
* Form-style answers directly after the match are negations too:
  "Dead pixels: none", "Burn-in: no", "Mining: never".
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass

from deal_radar.config_schema import AppConfig, Profile, RuleGroup
from deal_radar.core.logs import get_logger
from deal_radar.engine.types import Condition, DealItem, FilterResult, RiskSignal, SourceKind

log = get_logger(__name__)

# Constructs whose meaning changes (or that fail to compile) once a pattern is
# embedded in a larger alternation: numbered back-references (group numbers
# shift), named groups (duplicates collide) and leading global inline flags.
_UNJOINABLE = re.compile(r"\\[1-9]|\(\?P[<=]|^\(\?[aiLmsux]+\)")

# Constructs for which "lower-case the pattern, drop IGNORECASE, match lower-cased
# text" is NOT equivalent to IGNORECASE matching: scoped case-sensitivity
# ("(?-i:...)"), the locale flag, code-point escapes and named characters.
_CASE_UNSAFE = re.compile(r"\(\?[aiLmsux]*-[imsx]*i|\(\?[aimsux]*L|\\[xuUN0]")

# ASCII-only lower-casing, used when str.lower() would change the text length
# (a handful of Unicode code points) so match offsets stay valid on both strings.
_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")

# Word tokens for the negation window (run on lower-cased text). Decimals and
# thousands ("4.0", "1,500") and contractions ("isn't", "doesn’t") stay one token so
# "PCIe 4.0" never yields a stray "0" negation and "isn't" can be configured as a term.
_WORD = re.compile(r"[^\W_]+(?:[.,'’][^\W_]+)*")

# Clause boundaries for the negation window (run on lower-cased text).
_CLAUSE = re.compile(r"[!?;\n]|[.,](?!\d)|\bbut\b|\bhowever\b")

# Words that close the scope of a preceding negation term: in "no box cracked
# screen" the "no" governs "box", not "cracked".
_NEGATION_SCOPE_ENDERS = frozenset(
    {
        "box", "boxes", "packaging", "package", "manual", "manuals", "receipt", "warranty",
        "return", "returns", "refund", "refunds", "trade", "trades", "lowball", "lowballs",
        "lowballers", "hold", "holds", "scam", "scams", "scammers", "shipping", "delivery",
        "offers", "accessories", "cable", "cables", "stand", "remote",
        "except", "besides", "although", "though",
    }
)

# First explicit dollar price in a title: "$1,199", "$1199.99", "US$ 900", "$1.2k".
_TITLE_PRICE = re.compile(r"(?<![\w$])(?:US)?\$\s?(\d{1,3}(?:,\d{3})+|\d+)(\.\d{1,2})?(?:\s?([kK])\b)?")

# Accepted ratio item.price / title price before "title_price_mismatch" fires.
_TITLE_PRICE_RATIO = (0.5, 2.0)

_TITLE_FIELD = "title"


def extract_title_price(text: str) -> float | None:
    """First explicit ``$`` price in a title (``"... - $1199 ($1599 - $400)"`` -> ``1199.0``).

    Same contract as ``engine.normalizer.extract_title_price``; kept local so the
    hot path has no import-time dependency on the normalizer module.
    """
    if not text or "$" not in text:
        return None
    m = _TITLE_PRICE.search(text)
    if m is None:
        return None
    whole, cents, kilo = m.groups()
    value = float(whole.replace(",", "") + (cents or ""))
    if kilo:
        value *= 1000.0
    return round(value, 2)


# --------------------------------------------------------------------------- compiled patterns


def _lower(text: str) -> str:
    """Lower-case ``text`` without changing its length (offsets must map 1:1)."""
    low = text.lower()
    return low if len(low) == len(text) else text.translate(_ASCII_LOWER)


def _fold_pattern(pattern: str) -> str:
    """Lower-case a regex's literals, leaving every backslash escape untouched."""
    out: list[str] = []
    i, n = 0, len(pattern)
    while i < n:
        ch = pattern[i]
        if ch == "\\" and i + 1 < n:
            out.append(pattern[i : i + 2])
            i += 2
            continue
        out.append(ch.lower())
        i += 1
    return "".join(out)


def _compile_joined(sources: Sequence[str], flags: int) -> list[re.Pattern[str]]:
    joinable = [p for p in sources if not _UNJOINABLE.search(p)]
    regexes: list[re.Pattern[str]] = []
    if joinable:
        try:
            regexes.append(re.compile("|".join(f"(?:{p})" for p in joinable), flags))
        except re.error:
            # config_schema compiled each pattern already, so this is defensive: never
            # let one odd pattern take the whole filter down.
            regexes.extend(re.compile(p, flags) for p in joinable)
    regexes.extend(re.compile(p, flags) for p in sources if _UNJOINABLE.search(p))
    return regexes


class _PatternSet:
    """Patterns compiled into as few regexes as possible (normally one).

    ``search``/``finditer`` take the original text and its :func:`_lower` copy; match
    offsets are valid on both because the two strings have the same length.
    """

    __slots__ = ("patterns", "_folded", "_cased", "_single")

    def __init__(self, patterns: Sequence[str]) -> None:
        self.patterns: tuple[str, ...] = tuple(patterns)
        safe = [_fold_pattern(p) for p in self.patterns if not _CASE_UNSAFE.search(p)]
        unsafe = [p for p in self.patterns if _CASE_UNSAFE.search(p)]
        self._folded: tuple[re.Pattern[str], ...] = tuple(_compile_joined(safe, 0))
        self._cased: tuple[re.Pattern[str], ...] = tuple(_compile_joined(unsafe, re.IGNORECASE))
        self._single: re.Pattern[str] | None = (
            self._folded[0] if len(self._folded) == 1 and not self._cased else None
        )

    def search(self, text: str, low: str) -> re.Match[str] | None:
        """Leftmost match of any pattern."""
        if self._single is not None:
            return self._single.search(low)
        best: re.Match[str] | None = None
        for rx, haystack in self._pairs(text, low):
            m = rx.search(haystack)
            if m is not None and (best is None or m.start() < best.start()):
                best = m
        return best

    def finditer(self, text: str, low: str) -> Iterator[re.Match[str]]:
        """All matches, in text order."""
        if self._single is not None:
            return self._single.finditer(low)
        matches = [m for rx, haystack in self._pairs(text, low) for m in rx.finditer(haystack)]
        matches.sort(key=lambda m: m.start())
        return iter(matches)

    def _pairs(self, text: str, low: str) -> Iterator[tuple[re.Pattern[str], str]]:
        for rx in self._folded:
            yield rx, low
        for rx in self._cased:
            yield rx, text


def _pattern_set(patterns: Sequence[str]) -> _PatternSet | None:
    return _PatternSet(patterns) if patterns else None


@dataclass(frozen=True, slots=True)
class _Haystacks:
    """The two fields rules can look at, each with its lower-cased twin."""

    title: str
    title_low: str
    text: str
    text_low: str

    def pick(self, on_title: bool) -> tuple[str, str]:
        return (self.title, self.title_low) if on_title else (self.text, self.text_low)


@dataclass(frozen=True, slots=True)
class _Variant:
    id: str
    on_title: bool
    patterns: _PatternSet


@dataclass(frozen=True, slots=True)
class _Profile:
    profile: Profile
    order: int  # position in config.profiles (tie-breaker)
    on_title: bool
    any: _PatternSet | None
    all: tuple[_PatternSet, ...]
    none: _PatternSet | None
    variants: tuple[_Variant, ...]
    conditions: frozenset[Condition]

    @property
    def priority(self) -> int:
        return self.profile.priority


@dataclass(frozen=True, slots=True)
class _Group:
    name: str
    reject: bool
    on_title: bool
    negatable: bool
    probability: float
    categories: frozenset[str]  # empty = every category
    source_kinds: frozenset[SourceKind]  # empty = every source kind
    patterns: _PatternSet

    def applies(self, category: str | None, kind: SourceKind) -> bool:
        if self.categories and category not in self.categories:
            return False
        return not self.source_kinds or kind in self.source_kinds


@dataclass(frozen=True, slots=True)
class _Identification:
    profile: _Profile
    variant_id: str | None
    terms: tuple[str, ...]
    strength: float


# --------------------------------------------------------------------------- filter


class TextFilter:
    """Deterministic, precompiled text classifier (see the module docstring for semantics)."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        filters = config.filters
        self._min_title = filters.min_title_length
        self._window = filters.negation_window_words

        terms = [tuple(_WORD.findall(_lower(t).replace("’", "'"))) for t in filters.negation_terms]
        terms = [t for t in terms if t]
        self._neg_single = frozenset(t[0] for t in terms if len(t) == 1)
        self._neg_multi: tuple[tuple[str, ...], ...] = tuple(t for t in terms if len(t) > 1)
        longest = max((len(t) for t in terms), default=1)
        # Generous character budget for the words the window may need to inspect.
        self._lookback = 40 * (self._window + longest)
        self._trailing_neg: re.Pattern[str] | None = None
        if terms:
            alts = "|".join(r"\s+".join(re.escape(w) for w in t) for t in terms)
            self._trailing_neg = re.compile(rf"\s*[:=]\s*(?:{alts})(?![\w'’])")

        self._profiles: tuple[_Profile, ...] = tuple(
            sorted(
                (_compile_profile(p, i) for i, p in enumerate(config.profiles) if p.enabled),
                key=lambda cp: (-cp.priority, cp.order),
            )
        )
        self._groups: tuple[_Group, ...] = tuple(_compile_group(name, g) for name, g in filters.rules.items())
        categories: set[str | None] = {None, *(p.category for p in config.profiles)}
        self._index: dict[tuple[str | None, SourceKind], tuple[tuple[_Group, ...], tuple[_Group, ...]]] = {
            (category, kind): self._select_groups(category, kind) for category in categories for kind in SourceKind
        }
        self._mismatch_p = config.scoring.risk_probabilities.get("title_price_mismatch", 0.45)
        log.debug(
            "text filter compiled",
            extra={"profiles": len(self._profiles), "rule_groups": len(self._groups), "negation_terms": len(terms)},
        )

    # ------------------------------------------------------------------ public

    @property
    def rule_names(self) -> list[str]:
        """Rule group names in evaluation order."""
        return [g.name for g in self._groups]

    def evaluate(self, item: DealItem) -> FilterResult:
        """Classify one item. Pure: no I/O, no state mutation."""
        started = time.perf_counter_ns()
        title = item.title

        # 1. title length ------------------------------------------------------
        if len(title.strip()) < self._min_title:
            return _finish(started, FilterResult(accepted=False, reject_code="title_too_short", reject_detail=title.strip()))

        title_low = _lower(title)
        if item.description:
            hay = _Haystacks(title, title_low, f"{title}\n{item.description}", f"{title_low}\n{_lower(item.description)}")
        else:
            hay = _Haystacks(title, title_low, title, title_low)

        # 2. product identification -------------------------------------------
        ident, unknown = self._identify(hay)
        if ident is None:
            if unknown is not None:  # variant_required profile, variant not recognisable
                profile = unknown.profile.profile
                return _finish(
                    started,
                    FilterResult(
                        accepted=False,
                        reject_code="variant_unknown",
                        reject_detail=f"{profile.id}: no variant matched",
                        profile_id=profile.id,
                        category=profile.category,
                        match_strength=unknown.strength,
                        matched_terms=list(unknown.terms),
                    ),
                )
            return _finish(started, FilterResult(accepted=False, reject_code="no_profile_match"))

        profile = ident.profile.profile
        base = {
            "profile_id": profile.id,
            "variant_id": ident.variant_id,
            "category": profile.category,
            "match_strength": ident.strength,
            "matched_terms": list(ident.terms),
        }

        # 3. condition gates ---------------------------------------------------
        if item.condition is Condition.FOR_PARTS:
            return _finish(started, FilterResult(accepted=False, reject_code="for_parts", reject_detail="for_parts", **base))
        if item.condition not in ident.profile.conditions:
            return _finish(
                started,
                FilterResult(accepted=False, reject_code="condition_not_allowed", reject_detail=item.condition.value, **base),
            )

        reject_groups, risk_groups = self._groups_for(profile.category, item.source_kind)

        # 4. reject groups -----------------------------------------------------
        for group in reject_groups:
            detail = self._group_match(group, hay)
            if detail is not None:
                return _finish(started, FilterResult(accepted=False, reject_code=group.name, reject_detail=detail, **base))

        # 5. risk groups -------------------------------------------------------
        signals: list[RiskSignal] = []
        for group in risk_groups:
            detail = self._group_match(group, hay)
            if detail is not None:
                signals.append(RiskSignal(code=group.name, probability=group.probability, detail=detail, origin="text"))

        # 6. title / price mismatch -------------------------------------------
        title_price = extract_title_price(title)
        if title_price is not None and title_price > 0 and item.price > 0:
            ratio = item.price / title_price
            if not _TITLE_PRICE_RATIO[0] <= ratio <= _TITLE_PRICE_RATIO[1]:
                signals.append(
                    RiskSignal(
                        code="title_price_mismatch",
                        probability=self._mismatch_p,
                        detail=f"title says ${title_price:,.2f}, listed at ${item.price:,.2f}",
                        origin="listing",
                    )
                )

        return _finish(started, FilterResult(accepted=True, risk_signals=signals, **base))

    # ------------------------------------------------------------------ identification

    def _identify(self, hay: _Haystacks) -> tuple[_Identification | None, _Identification | None]:
        """Return ``(winner, variant_unknown_candidate)``; at most one of them is set."""
        candidates: list[tuple[_Profile, tuple[str, ...]]] = []
        for cp in self._profiles:
            terms = _match_profile(cp, hay)
            if terms is not None:
                candidates.append((cp, terms))
        if not candidates:
            return None, None

        unknown: _Identification | None = None
        for cp, terms in candidates:
            variant_id: str | None = None
            variant_term: str | None = None
            for variant in cp.variants:
                text, low = hay.pick(variant.on_title)
                m = variant.patterns.search(text, low)
                if m is not None:
                    variant_id, variant_term = variant.id, text[m.start() : m.end()]
                    break
            strength = 1.0
            if cp.variants and variant_id is None:
                strength -= 0.15
            if any(other is not cp and other.priority == cp.priority for other, _ in candidates):
                strength -= 0.1
            ident = _Identification(
                profile=cp,
                variant_id=variant_id,
                terms=_unique((*terms, variant_term) if variant_term else terms),
                strength=round(min(1.0, max(0.3, strength)), 4),
            )
            if variant_id is None and cp.profile.variant_required:
                if unknown is None:
                    unknown = ident
                continue
            return ident, None
        return None, unknown

    # ------------------------------------------------------------------ rule groups

    def _select_groups(self, category: str | None, kind: SourceKind) -> tuple[tuple[_Group, ...], tuple[_Group, ...]]:
        applicable = [g for g in self._groups if g.applies(category, kind)]
        return tuple(g for g in applicable if g.reject), tuple(g for g in applicable if not g.reject)

    def _groups_for(self, category: str | None, kind: SourceKind) -> tuple[tuple[_Group, ...], tuple[_Group, ...]]:
        found = self._index.get((category, kind))
        return found if found is not None else self._select_groups(category, kind)

    def _group_match(self, group: _Group, hay: _Haystacks) -> str | None:
        """Matched (original-case) text of the first non-negated match, else ``None``."""
        text, low = hay.pick(group.on_title)
        if not group.negatable:
            m = group.patterns.search(text, low)
            return None if m is None else _detail(text, m)
        for m in group.patterns.finditer(text, low):
            if not self._negated(low, m.start(), m.end()):
                return _detail(text, m)
        return None

    def _negated(self, low: str, start: int, end: int) -> bool:
        """True when the match ``low[start:end]`` is governed by a negation term."""
        if self._trailing_neg is not None and self._trailing_neg.match(low, end) is not None:
            return True
        lo = max(0, start - self._lookback)
        segment = low[lo:start]
        boundary_end = 0
        for b in _CLAUSE.finditer(segment):
            boundary_end = b.end()
        words = _WORD.findall(segment[boundary_end:].replace("’", "'"))
        if lo > 0 and boundary_end == 0 and words:
            words = words[1:]  # the window was clipped mid-text: first token may be partial
        n = len(words)
        for last in range(n - 1, max(0, n - self._window) - 1, -1):  # a term must END inside the window
            if words[last] in self._neg_single and not _scope_closed(words, last):
                return True
            for term in self._neg_multi:
                k = len(term)
                if last - k + 1 >= 0 and tuple(words[last - k + 1 : last + 1]) == term and not _scope_closed(words, last):
                    return True
        return False


# --------------------------------------------------------------------------- helpers


def _detail(text: str, m: re.Match[str]) -> str:
    return text[m.start() : m.end()].strip()


def _scope_closed(words: Sequence[str], term_end: int) -> bool:
    """A scope ender between the negation term and the match detaches the negation."""
    return any(w in _NEGATION_SCOPE_ENDERS for w in words[term_end + 1 :])


def _unique(terms: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for term in terms:
        term = term.strip()
        key = term.lower()
        if term and key not in seen:
            seen.add(key)
            out.append(term)
    return tuple(out)


def _match_profile(cp: _Profile, hay: _Haystacks) -> tuple[str, ...] | None:
    text, low = hay.pick(cp.on_title)
    terms: list[str] = []
    if cp.any is not None:
        m = cp.any.search(text, low)
        if m is None:
            return None
        terms.append(text[m.start() : m.end()])
    for required in cp.all:
        m = required.search(text, low)
        if m is None:
            return None
        terms.append(text[m.start() : m.end()])
    if cp.none is not None and cp.none.search(text, low) is not None:
        return None
    return tuple(terms)


def _compile_profile(profile: Profile, order: int) -> _Profile:
    rules = profile.match
    return _Profile(
        profile=profile,
        order=order,
        on_title=rules.field == _TITLE_FIELD,
        any=_pattern_set(rules.any),
        all=tuple(_PatternSet([p]) for p in rules.all),
        none=_pattern_set(rules.none),
        variants=tuple(
            _Variant(id=v.id, on_title=v.field == _TITLE_FIELD, patterns=_PatternSet(v.match)) for v in profile.variants
        ),
        conditions=frozenset(profile.conditions),
    )


def _compile_group(name: str, group: RuleGroup) -> _Group:
    return _Group(
        name=name,
        reject=group.action == "reject",
        on_title=group.field == _TITLE_FIELD,
        negatable=group.negatable,
        probability=group.probability,
        categories=frozenset(group.categories),
        source_kinds=frozenset(group.source_kinds),
        patterns=_PatternSet(group.patterns),
    )


def _finish(started_ns: int, result: FilterResult) -> FilterResult:
    result.elapsed_us = round((time.perf_counter_ns() - started_ns) / 1000.0, 3)
    return result


__all__ = ["TextFilter", "extract_title_price"]
