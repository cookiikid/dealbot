"""Deterministic text classification: product identification + scam/noise rules.

The text filter is the first CPU stage after normalisation and runs for *every*
listing a source emits, so it is built to be fast, explainable and boring.

Semantics (evaluation order, the first rejection wins)
-------------------------------------------------------
1. ``title_too_short`` when the stripped title is shorter than
   ``filters.min_title_length``.
2. Product identification over the enabled profiles (``match.any/all/none`` on
   ``match.field``). Candidates are ranked by ``priority`` (desc) then config order.
   The winner's first matching variant (``variant.field``) is selected; a
   ``variant_required`` profile without a matching variant yields to the next
   candidate, else ``variant_unknown``. Nothing matched -> ``no_profile_match``.
3. Condition gates: ``FOR_PARTS`` -> ``for_parts``; a condition outside
   ``profile.conditions`` -> ``condition_not_allowed``.
4. ``reject`` groups in config order (respecting ``field``, ``categories``,
   ``source_kinds`` and ``negatable``): first match -> ``reject_code=<group>``,
   ``reject_detail=<matched text>``.
5. ``risk`` groups: one :class:`RiskSignal` per matching group (origin ``text``).
6. Title/price mismatch: the first explicit price in the title
   (:func:`deal_radar.engine.normalizer.extract_title_price`, which ignores
   "$50 off"-style amounts) vs ``item.price``; a ratio outside ``[0.5, 2.0]`` adds
   ``title_price_mismatch`` (origin ``listing``).

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

Performance design
------------------
* **Compile once, join per group.** The patterns of a rule group, of a profile's
  ``any``/``none`` lists and of a variant's ``match`` list are joined into ONE
  alternation ``(?:p1)|(?:p2)|...``. Patterns that would change meaning when
  concatenated (numbered back-references, named groups, leading global inline
  flags) are compiled on their own; ``all`` lists stay separate by definition.
* **Case folding instead of IGNORECASE.** ``re.IGNORECASE`` disables sre's
  literal/charset prefix scan and is 2-8x slower. Each pattern is lower-cased once
  (backslash escapes untouched) and matched without IGNORECASE against a lower-cased
  copy of the listing; spans map 1:1 back onto the original text. Constructs where
  that is not equivalent (scoped ``(?-i:...)``, ``\\x``/``\\u``/``\\N`` escapes, the
  locale flag) keep IGNORECASE on the original text.
* **Literal anchor gate.** A regex led by ``\\b`` or a look-behind pays ~15 ns per
  character even on irrelevant text, and descriptions run to 5000 characters. For
  every pattern set the filter derives, from the regex parse tree and
  conservatively, literal *anchors*: strings one of which every match must contain
  ("box only", "not working", "zelle"). All anchors are compiled into greedy trie
  regexes scanned once per listing; only pattern sets whose anchors occur are run.
  Anchors that provably start a word are matched with a leading space so the
  scan only engages at word starts. Pattern sets without a provable anchor always
  run, so the gate can only skip work, never change a result.
* **Pre-indexed rule groups.** Applicable reject/risk groups per
  ``(category, source kind)`` are resolved at start-up.
* **Pure.** ``evaluate`` reads only the item and immutable compiled state: no I/O,
  logging, metrics or caches; safe to call from any number of pipeline workers.
"""

from __future__ import annotations

import functools
import re
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from deal_radar.config_schema import AppConfig, Profile, RuleGroup
from deal_radar.core.logs import get_logger
from deal_radar.engine.normalizer import extract_title_price
from deal_radar.engine.types import Condition, DealItem, FilterResult, RiskSignal, SourceKind

try:  # CPython 3.11+: the regex parser used to derive literal anchors (an optimisation only)
    from re import _constants as _sre_c
    from re import _parser as _sre_p
except ImportError:  # pragma: no cover - other implementations: every pattern set simply runs ungated
    _sre_c = None
    _sre_p = None

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

# Accepted ratio item.price / title price before "title_price_mismatch" fires.
_TITLE_PRICE_RATIO = (0.5, 2.0)

_TITLE_FIELD = "title"


# --------------------------------------------------------------------------- case folding


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


# --------------------------------------------------------------------------- gate text
#
# The gate scans a "gate form" of the lower-cased listing: every non-word character
# becomes a space and the text is padded with a space on both sides, so a word
# start is always preceded by a space. Anchors are mapped the same way and their
# whitespace runs collapsed to one space, which the trie regexes match as " +"
# (collapsing the text itself would cost more than the whole scan).

_ASCII_NON_WORD = "".join(c if (c.isalnum() or c == "_") else " " for c in map(chr, range(128)))
_NON_WORD_RUN = re.compile(r"\W+")
_WS_RUN = re.compile(r"\s+")
_SPACE_RUN = re.compile(r" {2,}")


def _gate_form(text: str) -> str:
    mapped = text.translate(_ASCII_NON_WORD) if text.isascii() else _NON_WORD_RUN.sub(" ", text)
    return _WS_RUN.sub(" ", mapped)


def _gate_text(low: str) -> str:
    mapped = low.translate(_ASCII_NON_WORD) if low.isascii() else _NON_WORD_RUN.sub(" ", low)
    return f" {mapped} "


def _is_word_char(ch: str) -> bool:
    return ch.isalnum() or ch == "_"


# --------------------------------------------------------------------------- anchor derivation

_MAX_ANCHORS = 256  # per literal run; beyond this a run is cut and restarted


def _anchor_constants() -> dict[str, Any] | None:
    if _sre_c is None or _sre_p is None:
        return None
    names = (
        "LITERAL", "IN", "RANGE", "CATEGORY", "CATEGORY_SPACE", "CATEGORY_WORD", "CATEGORY_NOT_WORD", "SUBPATTERN", "BRANCH",
        "AT", "AT_BOUNDARY", "AT_BEGINNING", "AT_BEGINNING_STRING", "ASSERT", "ASSERT_NOT",
    )
    try:
        found: dict[str, Any] = {name: getattr(_sre_c, name) for name in names}
    except AttributeError:  # pragma: no cover - unexpected interpreter internals
        return None
    found["REPEATS"] = tuple(
        getattr(_sre_c, n) for n in ("MAX_REPEAT", "MIN_REPEAT", "POSSESSIVE_REPEAT") if hasattr(_sre_c, n)
    )
    found["ATOMIC_GROUP"] = getattr(_sre_c, "ATOMIC_GROUP", object())
    found["ALIGNING_AT"] = (found["AT_BOUNDARY"], found["AT_BEGINNING"], found["AT_BEGINNING_STRING"])
    found["NON_WORD_CATEGORIES"] = (found["CATEGORY_SPACE"], found["CATEGORY_NOT_WORD"])
    return found


_C = _anchor_constants()


def _sub_items(node: Any) -> list[tuple[Any, Any]]:
    return list(node.data) if hasattr(node, "data") else list(node)


def _class_chars(items: Sequence[tuple[Any, Any]]) -> set[str] | None:
    """Characters of a small positive class; ``\\s``/``\\W`` become " " (gate form)."""
    assert _C is not None
    chars: set[str] = set()
    for op, av in items:
        if op is _C["LITERAL"]:
            chars.add(chr(av))
        elif op is _C["RANGE"] and av[1] - av[0] < 10:
            chars.update(chr(c) for c in range(av[0], av[1] + 1))
        elif op is _C["CATEGORY"] and av in _C["NON_WORD_CATEGORIES"]:
            chars.add(" ")
        else:
            return None
    return chars if 0 < len(chars) <= 10 else None


def _anchor_seq(items: Sequence[tuple[Any, Any]]) -> tuple[set[str], bool]:
    """Literal prefixes every match of ``items`` starts with, and whether they are complete."""
    assert _C is not None
    prefixes = {""}
    for op, av in items:
        if op is _C["AT"] or op is _C["ASSERT"] or op is _C["ASSERT_NOT"]:
            continue
        step = _anchor_node(op, av)
        if step is None:
            return prefixes, False
        strings, complete = step
        combined = {p + s for p in prefixes for s in strings}
        if len(combined) > _MAX_ANCHORS:
            return prefixes, False
        prefixes = combined
        if not complete:
            return prefixes, False
    return prefixes, True


def _anchor_node(op: Any, av: Any) -> tuple[set[str], bool] | None:
    """Prefix strings of one consuming node (``None`` when it starts with a non-literal)."""
    assert _C is not None
    if op is _C["LITERAL"]:
        return {chr(av)}, True
    if op is _C["IN"]:
        chars = _class_chars(av)
        return (chars, True) if chars is not None else None
    if op is _C["SUBPATTERN"]:
        _group, add_flags, del_flags, sub = av
        if add_flags or del_flags:
            return None
        return _anchor_seq(_sub_items(sub))
    if op is _C["ATOMIC_GROUP"]:
        return _anchor_seq(_sub_items(av))
    if op is _C["BRANCH"]:
        out: set[str] = set()
        complete = True
        for branch in av[1]:
            strings, done = _anchor_seq(_sub_items(branch))
            out |= strings
            complete = complete and done
        return (out, complete) if len(out) <= _MAX_ANCHORS else None
    if op in _C["REPEATS"]:
        low, high, sub = av
        strings, done = _anchor_seq(_sub_items(sub))
        if done and strings and all(not _gate_form(s).strip() for s in strings):
            # \s+, \W*, [\s.-]? ...: in gate form any such run is exactly one space.
            return ({" "} if low >= 1 and "" not in strings else {"", " "}), True
        if low == 0:
            return ({""} | strings, True) if high == 1 and done else ({""}, False)
        if low == 1 and high == 1:
            return strings, done
        return strings, False
    return None


def _nested_required(op: Any, av: Any, aligned: bool) -> set[str] | None:
    """Required literals of a compound node that could not be used as a plain prefix."""
    assert _C is not None
    if op is _C["SUBPATTERN"]:
        _group, add_flags, del_flags, sub = av
        return None if add_flags or del_flags else _required(_sub_items(sub), aligned)
    if op is _C["ATOMIC_GROUP"]:
        return _required(_sub_items(av), aligned)
    if op is _C["BRANCH"]:
        out: set[str] = set()
        for branch in av[1]:
            found = _required(_sub_items(branch), aligned)
            if found is None:
                return None  # one alternative needs no literal at all
            out |= found
        return out if len(out) <= _MAX_ANCHORS else None
    if op in _C["REPEATS"] and av[0] >= 1:
        return _required(_sub_items(av[2]), aligned)
    return None


def _behind_is_non_word(av: Any, negative: bool) -> bool:
    """Whether a look-behind proves the previous character is not a word character.

    ``(?<![\\w.$-])`` (negative, class containing ``\\w``) and ``(?<=\\s)`` (positive,
    class of non-word characters only) both do.
    """
    assert _C is not None
    direction, sub = av
    items = _sub_items(sub)
    if direction != -1 or len(items) != 1 or items[0][0] is not _C["IN"]:
        return False
    members = items[0][1]
    if negative:
        return any(op is _C["CATEGORY"] and arg is _C["CATEGORY_WORD"] for op, arg in members)
    chars = _class_chars(members)
    return chars is not None and all(not _is_word_char(ch) for ch in chars)


def _ends_non_word(items: Sequence[tuple[Any, Any]]) -> bool:
    """Whether every match of ``items`` ends with a non-word character (``...\\s+``)."""
    assert _C is not None
    consuming = [(op, av) for op, av in items if op is not _C["AT"] and op is not _C["ASSERT"] and op is not _C["ASSERT_NOT"]]
    if not consuming:
        return False
    node = _anchor_node(*consuming[-1])
    return node is not None and node[1] and bool(node[0]) and all(s and not _is_word_char(s[-1]) for s in node[0])


def _aligned_after(op: Any, av: Any, aligned_before: bool) -> bool:
    """Word-start knowledge after a non-literal item such as ``(?:[\\w-]+\\s+){0,2}``."""
    assert _C is not None
    if op in _C["REPEATS"]:
        low, _high, sub = av
        ends = _ends_non_word(_sub_items(sub))
        return ends and (aligned_before or low >= 1)
    return False


def _ends_aligned(strings: set[str], aligned_before: bool) -> bool:
    """Whether the character after a literal step is known to follow a non-word char."""
    result = True
    for s in strings:
        result = result and (aligned_before if s == "" else not _is_word_char(s[-1]))
    return result


def _required(items: Sequence[tuple[Any, Any]], aligned: bool = False) -> set[str] | None:
    """Most selective gate-form literal set R such that every match contains a member of R.

    Consecutive literal items form *runs* (prefix sets from :func:`_anchor_node`); a
    non-literal item (``\\d``, ``.*``, ``[^\\n]*``...) closes the current run. Every
    run, every positive look-around and every compound node is a candidate. A run
    that starts right after ``\\b``/``^`` or a non-word character is a word start and
    gets a leading space. ``aligned`` tells whether the first item starts a word.
    """
    assert _C is not None
    candidates: list[set[str]] = []
    run: set[str] = {""}
    run_aligned = False

    def close() -> None:
        nonlocal run
        if run != {""}:
            candidates.append({(" " + s) if run_aligned else s for s in run})
        run = {""}

    for op, av in items:
        if op is _C["AT"]:
            if av in _C["ALIGNING_AT"] and run == {""}:
                aligned = True
            elif run == {""}:
                aligned = False
            continue
        if op is _C["ASSERT"]:  # positive look-around: its content must be in the text too
            inner = _required(_sub_items(av[1]), aligned if av[0] == 1 else False)
            if inner:
                candidates.append(inner)
            if run == {""} and _behind_is_non_word(av, negative=False):
                aligned = True
            continue
        if op is _C["ASSERT_NOT"]:
            if run == {""} and _behind_is_non_word(av, negative=True):
                aligned = True
            continue
        node = _anchor_node(op, av)
        if node is None:
            close()
            nested = _nested_required(op, av, aligned)
            if nested:
                candidates.append(nested)
            aligned = _aligned_after(op, av, aligned)
            continue
        strings, complete = node
        if run == {""}:
            run_aligned = aligned
        combined = {p + s for p in run for s in strings}
        if len(combined) > _MAX_ANCHORS:
            close()
            run_aligned = aligned
            combined = set(strings)
        run = combined
        aligned = _ends_aligned(strings, aligned)
        if not complete:
            close()
            nested = _nested_required(op, av, run_aligned)
            if nested:
                candidates.append(nested)
            aligned = False
    close()

    best: set[str] | None = None
    best_key = (0, 0)
    for candidate in candidates:
        forms = {_gate_form(c) for c in candidate}
        if not all(any(_is_word_char(ch) for ch in f) for f in forms):
            continue  # some match may contain no word character at all here
        shortest = min(len(f.strip()) for f in forms)
        # Word-start anchors are both rarer and cheaper to scan for.
        key = (shortest + (2 if all(f.startswith(" ") for f in forms) else 0), -len(forms))
        if best is None or key > best_key:
            best, best_key = forms, key
    return best


@functools.lru_cache(maxsize=4096)
def _literal_anchors(pattern: str) -> frozenset[str] | None:
    """Gate-form anchors one of which occurs in every match of ``pattern``, else ``None``."""
    if _C is None or _sre_p is None:
        return None
    try:
        found = _required(_sub_items(_sre_p.parse(pattern, 0)))
    except Exception:  # noqa: BLE001 - optimisation only: anything unexpected disables gating
        return None
    return frozenset(found) if found else None


def _set_anchors(patterns: Sequence[str]) -> frozenset[str] | None:
    out: set[str] = set()
    for pattern in patterns:
        anchors = _literal_anchors(pattern)
        if anchors is None:
            return None
        out |= anchors
    return frozenset(out) if out else None


# sre tries alternatives in order, so branches are ordered by how often a word starts
# with that character in listing text (cheaper failed attempts on ordinary words).
_BRANCH_ORDER = {ch: i for i, ch in enumerate("tscapbmwfhoidlrneguvkyjqzx0123456789 ")}


def _trie_regex(words: Iterable[str]) -> str:
    """Greedy trie alternation: at any position it matches the LONGEST word starting there."""
    trie: dict[str | None, Any] = {}
    for word in words:
        node = trie
        for ch in word:
            node = node.setdefault(ch, {})
        node[None] = True

    def render(node: dict[str | None, Any]) -> str:
        keys = sorted((k for k in node if k is not None), key=lambda k: (_BRANCH_ORDER.get(k, len(_BRANCH_ORDER)), k))
        branches = [(" +" if ch == " " else re.escape(ch)) + render(node[ch]) for ch in keys]
        if not branches:
            return ""
        body = branches[0] if len(branches) == 1 else "(?:" + "|".join(branches) + ")"
        return f"(?:{body})?" if None in node else body

    return render(trie)


# --------------------------------------------------------------------------- compiled patterns


class _PatternSet:
    """Patterns compiled into as few regexes as possible (normally one).

    ``search``/``finditer`` take the original text, its :func:`_lower` copy (offsets
    are valid on both because the strings have the same length) and the gate's hit
    set: a set with ``anchors`` that is not in ``hits`` cannot match and is skipped.
    """

    __slots__ = ("patterns", "anchors", "_folded", "_cased", "_single")

    def __init__(self, patterns: Sequence[str]) -> None:
        self.patterns: tuple[str, ...] = tuple(patterns)
        safe = [_fold_pattern(p) for p in self.patterns if not _CASE_UNSAFE.search(p)]
        unsafe = [p for p in self.patterns if _CASE_UNSAFE.search(p)]
        self._folded: tuple[re.Pattern[str], ...] = tuple(_compile_joined(safe, 0))
        self._cased: tuple[re.Pattern[str], ...] = tuple(_compile_joined(unsafe, re.IGNORECASE))
        self._single: re.Pattern[str] | None = (
            self._folded[0] if len(self._folded) == 1 and not self._cased else None
        )
        self.anchors: frozenset[str] | None = None if unsafe else _set_anchors(safe)

    def search(self, text: str, low: str, hits: set[_PatternSet]) -> re.Match[str] | None:
        """Leftmost match of any pattern."""
        if self.anchors is not None and self not in hits:
            return None
        if self._single is not None:
            return self._single.search(low)
        best: re.Match[str] | None = None
        for rx, haystack in self._pairs(text, low):
            m = rx.search(haystack)
            if m is not None and (best is None or m.start() < best.start()):
                best = m
        return best

    def finditer(self, text: str, low: str, hits: set[_PatternSet]) -> Iterator[re.Match[str]]:
        """All matches, in text order."""
        if self.anchors is not None and self not in hits:
            return iter(())
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


class _AnchorGate:
    """One literal scan per listing that tells which anchored pattern sets may match.

    Exact: each trie regex is greedy, so at a position it reports the LONGEST anchor
    starting there; every other anchor starting at that position is a prefix of it
    (prefix closure below), and the scan resumes one character after each hit start
    so overlapping anchors are found too. ``scan`` therefore returns precisely the
    pattern sets with at least one anchor present in the gate text. Word-start
    anchors (leading space) are scanned with a space-prefixed regex, which sre
    searches with a fast literal-prefix loop.
    """

    __slots__ = ("_regexes", "_sets_for", "anchor_count")

    def __init__(self, pattern_sets: Iterable[_PatternSet]) -> None:
        owners: dict[str, set[_PatternSet]] = {}
        for ps in pattern_sets:
            for anchor in ps.anchors or ():
                owners.setdefault(anchor, set()).add(ps)
        self.anchor_count = len(owners)
        self._sets_for: dict[str, frozenset[_PatternSet]] = {}
        for anchor in owners:
            closure: set[_PatternSet] = set()
            for i in range(1, len(anchor) + 1):
                closure |= owners.get(anchor[:i], set())
            self._sets_for[anchor] = frozenset(closure)
        word_start = [a[1:] for a in owners if a.startswith(" ")]
        anywhere = [a for a in owners if not a.startswith(" ")]
        regexes: list[re.Pattern[str]] = []
        if word_start:
            regexes.append(re.compile(" (?:" + _trie_regex(word_start) + ")"))
        if anywhere:
            regexes.append(re.compile(_trie_regex(anywhere)))
        self._regexes: tuple[re.Pattern[str], ...] = tuple(regexes)

    def scan(self, low: str) -> set[_PatternSet]:
        hits: set[_PatternSet] = set()
        if not self._regexes:
            return hits
        text = _gate_text(low)
        sets_for = self._sets_for
        for regex in self._regexes:
            pos = 0
            while True:
                m = regex.search(text, pos)
                if m is None:
                    break
                key = m.group()
                if "  " in key:  # the gate text keeps whitespace runs; anchors are single-spaced
                    key = _SPACE_RUN.sub(" ", key)
                hits |= sets_for[key]
                pos = m.start() + 1
        return hits


# --------------------------------------------------------------------------- compiled config


@dataclass(frozen=True, slots=True)
class _Haystacks:
    """The two fields rules can look at (each with its lower-cased twin) + gate hits."""

    title: str
    title_low: str
    text: str
    text_low: str
    hits: set[_PatternSet]

    def pick(self, on_title: bool) -> tuple[str, str]:
        return (self.title, self.title_low) if on_title else (self.text, self.text_low)

    def search(self, ps: _PatternSet, on_title: bool) -> tuple[str, re.Match[str] | None]:
        text, low = self.pick(on_title)
        return text, ps.search(text, low, self.hits)


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

    def pattern_sets(self) -> list[tuple[_PatternSet, bool]]:
        """Every compiled pattern set with whether it only ever looks at the title."""
        sets = [(ps, self.on_title) for ps in (self.any, self.none, *self.all) if ps is not None]
        sets.extend((v.patterns, v.on_title) for v in self.variants)
        return sets


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
    """Deterministic, precompiled text classifier (see the module docstring for semantics).

    ``literal_gate=False`` disables the anchor gate (every pattern set always runs).
    Results are identical either way; the switch exists for verification and debugging.
    """

    def __init__(self, config: AppConfig, *, literal_gate: bool = True) -> None:
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
            alts = "|".join(r"\W+".join(re.escape(w).replace("'", "['’]") for w in t) for t in terms)
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

        # Two gates: title-only pattern sets are gated by a scan of the (short) title,
        # the rest by a scan of title + description. Keeping title anchors out of the
        # text gate keeps the per-character cost on long descriptions low.
        pattern_sets = [(g.patterns, g.on_title) for g in self._groups]
        for cp in self._profiles:
            pattern_sets.extend(cp.pattern_sets())
        if not literal_gate:
            for ps, _ in pattern_sets:
                ps.anchors = None
        self._title_gate = _AnchorGate(ps for ps, on_title in pattern_sets if on_title)
        self._text_gate = _AnchorGate(ps for ps, on_title in pattern_sets if not on_title)
        log.debug(
            "text filter compiled",
            extra={
                "profiles": len(self._profiles),
                "rule_groups": len(self._groups),
                "negation_terms": len(terms),
                "pattern_sets": len(pattern_sets),
                "gated_pattern_sets": sum(1 for ps, _ in pattern_sets if ps.anchors is not None),
                "anchors": self._title_gate.anchor_count + self._text_gate.anchor_count,
            },
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
            text, text_low = f"{title}\n{item.description}", f"{title_low}\n{_lower(item.description)}"
        else:
            text, text_low = title, title_low
        hits = self._title_gate.scan(title_low)
        hits |= self._text_gate.scan(text_low)
        hay = _Haystacks(title, title_low, text, text_low, hits)

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
        base: dict[str, Any] = {
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
                text, m = hay.search(variant.patterns, variant.on_title)
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
        if not group.negatable:
            text, m = hay.search(group.patterns, group.on_title)
            return None if m is None else _detail(text, m)
        text, low = hay.pick(group.on_title)
        for m in group.patterns.finditer(text, low, hay.hits):
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
    terms: list[str] = []
    if cp.any is not None:
        text, m = hay.search(cp.any, cp.on_title)
        if m is None:
            return None
        terms.append(text[m.start() : m.end()])
    for required in cp.all:
        text, m = hay.search(required, cp.on_title)
        if m is None:
            return None
        terms.append(text[m.start() : m.end()])
    if cp.none is not None and hay.search(cp.none, cp.on_title)[1] is not None:
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


__all__ = ["TextFilter"]
