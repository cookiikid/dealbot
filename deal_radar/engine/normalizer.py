"""Listing normalization: :class:`RawListing` -> :class:`DealItem`.

Every source hands the engine loosely structured data ("US $1,299.99", "1.299,99 €",
"$800 OBO", "Open-Box Excellent", eBay condition id "3000", tracking-laden URLs,
HTML-escaped titles...). This module turns it into the one canonical
:class:`DealItem` schema that every later stage relies on, so price/condition
parsing lives in exactly one battle-tested place. (``DealItem`` and ``RawListing``
are defined in :mod:`deal_radar.engine.types` and re-exported here.)

Design decisions
----------------
* **Precision over recall.** A wrong price is worse than no price: it produces a
  fake "90 % off" alert. Every parser therefore returns ``None`` when the input is
  ambiguous (``"$1,250k"``), malformed (``"1,2,3"``), negative or absurd
  (``> 10,000,000``), and :meth:`Normalizer.normalize` raises
  :class:`NormalizationError` instead of guessing.
* **One numeric grammar.** Prices are tokenised by one regex family (sign,
  currency prefix/suffix, digit run, ``k`` multiplier) and interpreted by
  :func:`_to_number`, which resolves US (``1,299.99``), European (``1.299,99``),
  Swiss (``1'299.50``) and space-grouped (``1 299,99``) notations: a separator
  followed by exactly three digits is a thousands separator, otherwise decimal.
* **Currency markers beat bare numbers.** In a price field ``"2 for $50"`` means
  $50, so the first currency-marked amount wins over unmarked numbers. A bare
  ``$`` is USD unless the listing itself says it is priced in another dollar
  currency; ``C$``/``A$``/``CAD``... are always explicit.
* **Title prices are only explicit money.** :func:`extract_title_price` ignores
  every number without ``$``/``USD`` (model numbers, VRAM, refresh rates, years,
  percentages), skips amounts labelled as discounts/fees ("$50 off", "$20
  shipping", "(-$100 rebate)") and understands Slickdeals/buildapcsales
  breakdowns: ``"$1199 ($1599 - $400)"`` -> 1199, ``"($449 - $50 = $399)"`` -> 399.
* **Condition is conservative.** Structured values (eBay condition ids/enums,
  retailer strings) are authoritative and free text may only *downgrade* them
  ("New" + "like new" in the title -> USED). Without a structured value the source
  kind sets the default (retail/aggregator -> NEW, local/marketplace -> USED);
  retail text can downgrade it, local text upgrades to NEW only on explicit "brand
  new / sealed / BNIB / NIB / new in box" wording, and negated hints ("never
  used", "not refurbished") are ignored. Lower classes are the *safe* direction:
  they compare the price against a lower market reference, so they can only
  shrink an apparent discount.
* **URLs are canonical but functional.** Tracking parameters (``utm_*``,
  ``fbclid``, ``gclid``, eBay ``_trksid``/``hash``, Facebook ``tracking``...) and
  fragments are removed; parameters that select the page/offer (eBay ``var``,
  Best Buy ``skuId``, Amazon ``th``/``psc``/``smid``) are kept byte-for-byte (no
  re-encoding). Affiliate attribution the operator can configure (eBay Partner
  Network ``campid``/``mkcid``/..., Amazon ``tag``) is preserved. Amazon product
  pages collapse to ``/dp/<ASIN>``. Image URLs are never rewritten beyond
  scheme/host case because CDN signatures (Facebook ``oh``/``oe``) live in the query.
* **Hot path.** :meth:`Normalizer.normalize` targets < 50 us for a typical
  listing. CPython's regex engine is slow on wide alternations that start with
  assertions, so hot patterns start with literals and rare context checks
  (word boundaries, "like new" exclusions, negation) run in Python only on the few
  matches; price scanning in free text jumps between ``$``/``USD`` anchors; ASCII
  text skips Unicode normalization; plain prices (``"1,299.99"``) bypass the
  tokenizer.
"""

from __future__ import annotations

import html
import math
import re
import unicodedata
from datetime import datetime, timezone
from collections.abc import Iterator
from typing import TYPE_CHECKING, NamedTuple
from urllib.parse import unquote, urlsplit, urlunsplit

from deal_radar.core.logs import get_logger
from deal_radar.engine.types import Condition, DealItem, RawListing, SourceKind, utcnow

if TYPE_CHECKING:  # pragma: no cover
    from deal_radar.config_schema import AppConfig

log = get_logger("engine.normalizer")

MAX_PRICE = 10_000_000.0  # anything above is garbage ("$99999999999"), never a real listing
MAX_TITLE_LEN = 300
MAX_DESCRIPTION_LEN = 5000
MAX_IMAGES = 8
_CONDITION_TEXT_CHARS = 1500  # description prefix scanned for condition hints (bounded cost)


class NormalizationError(Exception):
    """A listing cannot be turned into a trustworthy :class:`DealItem`.

    ``code`` is a stable machine code (``no_price``, ``negative_price``,
    ``unsupported_currency``, ``bad_url``, ``empty_title``) suitable for metric
    labels; ``detail`` is a short human explanation.
    """

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


# =========================================================================== money

# Dollar prefixes. Single-letter prefixes must touch the "$" ("A $50 gift card" is
# not Australian dollars); only eBay's "US $" may contain a space.
_DOLLAR_PREFIX_CODES: dict[str, str] = {
    "": "USD",
    "US": "USD",
    "C": "CAD",
    "CA": "CAD",
    "A": "AUD",
    "AU": "AUD",
    "NZ": "NZD",
    "HK": "HKD",
    "S": "SGD",
    "SG": "SGD",
    "MX": "MXN",
    "NT": "TWD",
    "R": "BRL",
}
_DOLLAR_CODES = frozenset({"USD", "CAD", "AUD", "NZD", "HKD", "SGD", "MXN", "TWD"})
_SYMBOL_CODES: dict[str, str] = {
    "\u20ac": "EUR",
    "\u00a3": "GBP",
    "\u00a5": "JPY",
    "\u20b9": "INR",
    "\u20a9": "KRW",
    "\u20bd": "RUB",
    "\u20ba": "TRY",
    "\u20aa": "ILS",
    "\u20b1": "PHP",
}
_ISO_CODES = frozenset(
    {
        "USD", "CAD", "AUD", "NZD", "HKD", "SGD", "MXN", "TWD", "BRL", "EUR", "GBP", "JPY", "CNY",
        "CHF", "INR", "KRW", "RUB", "TRY", "ILS", "PHP", "SEK", "NOK", "DKK", "PLN", "CZK", "HUF", "ZAR",
    }
)

_SP = "[ \u00a0]"  # a space inside a money expression (regular or no-break)
_DOLLAR = rf"(?:(?<![A-Za-z])(?:US{_SP}?|CA|AU|NZ|HK|SG|MX|NT|C|A|S|R))?\$"
_CODE_ALT = "|".join(sorted(_ISO_CODES))
_CODE_PREFIX = rf"(?<![A-Za-z])(?i:{_CODE_ALT})(?![A-Za-z])(?:{_SP}?\$)?"
_CODE_SUFFIX = rf"{_SP}?(?<![A-Za-z])(?i:{_CODE_ALT})(?![A-Za-z])"
_SYMBOLS = "[" + "".join(_SYMBOL_CODES) + "]"

# A digit run; separators only count when another digit follows ("$1,299," ends at 9).
_NUM_RUN = r"\d(?:\d|[,.'\u00a0\u202f\u2009](?=\d))*"
# Regular-space thousands grouping ("1 299,99 EUR"); only accepted in price fields.
_NUM_SPACED = r"\d{1,3}(?:[ ]\d{3}(?!\d))+(?:[.,]\d{1,2}(?!\d))?"
# A sign only when attached to the amount and preceded by a boundary: "-5", "(-$100)".
# "$800-$900" and "Title - $549" are ranges/separators, not negatives.
_NEG = r"(?P<neg>(?<![^\s(\[{:/])[-\u2212])?"
_KILO = r"[kK](?![A-Za-z0-9])"


def _build_money_regex(*, prefix: str, suffix: str, number: str, plain: bool) -> re.Pattern[str]:
    """Tokenizer for (sign, currency prefix, number, k-multiplier, currency suffix)."""
    tail = rf"(?P<post>{suffix})" + ("?" if plain else "")
    return re.compile(
        rf"""{_NEG}
        (?:
            (?P<pre>{prefix}){_SP}{{0,2}}(?P<neg2>[-\u2212])?(?P<n1>{number})(?P<k1>{_KILO})?
          |
            (?<![\w.,'\u2019$])(?P<n2>{number})(?P<k2>{_KILO})?{tail}
        )""",
        re.VERBOSE,
    )


# Price fields: any currency, prefix or suffix, unmarked numbers allowed.
_FIELD_RE = _build_money_regex(
    prefix=rf"{_DOLLAR}|{_CODE_PREFIX}|{_SYMBOLS}",
    suffix=rf"{_SP}?\$(?!{_SP}?[\d$])|{_SP}?{_SYMBOLS}(?!{_SP}?\d)|{_CODE_SUFFIX}(?!{_SP}?\d)",
    number=rf"(?:{_NUM_SPACED}|{_NUM_RUN})",
    plain=True,
)
# Free text (titles/descriptions): only explicit dollar/USD amounts.
_TEXT_RE = _build_money_regex(
    prefix=rf"{_DOLLAR}|(?<![A-Za-z])(?i:usd)(?![A-Za-z])(?:{_SP}?\$)?|(?<![A-Za-z])(?:CAD|AUD|NZD|HKD|SGD|MXN){_SP}?\$",
    suffix=rf"\$(?![\d$])|{_SP}?(?<![A-Za-z])(?i:usd)(?![A-Za-z])(?!{_SP}?\d)",
    number=_NUM_RUN,
    plain=False,
)
_TEXT_ANCHOR = re.compile(r"\$|[uU][sS][dD]")
_PREFIX_LOOKBACK = 6  # "-US $" / "-CAD $": a prefix match starts at most this far before "$"
_ANCHOR_WINDOW = 48  # chars after an anchor searched for its amount
_RUN_SEPARATORS = ",.'   "

# Hot path for clean US-style fields: "1299", "1,299.99", "$1,299.99", "US $1,299.99".
_US_PRICE = re.compile(r"\s*(US ?\$|\$)?[ ]?((?:\d{1,3}(?:,\d{3})+|\d{1,8})(?:\.\d{1,2})?)\s*")
_FREE_ITEM = re.compile(
    r"\bfree\b(?!\s*(?:shipping|ship\b|delivery|returns?|s&h|s/h|gifts?\b|install\w*|pick[\s-]?up\s+in\s+store|trial))",
    re.IGNORECASE,
)
_FREE_SHIPPING = re.compile(
    r"^\W*(?:free|gratis|included|incl\b)|\bfree\s+(?:standard\s+|ground\s+|2[\s-]day\s+)?(?:shipping|delivery|s&h|s/h|ship)\b"
    r"|^\W*(?:shipping|delivery)\s*:?\s*(?:free|included)\b",
    re.IGNORECASE,
)
_KEYCAP = re.compile(r"[0-9#*]\ufe0f?\u20e3")

# Amounts in free text that are labelled as something other than the item price.
_SKIP_AFTER = re.compile(
    r"\s*(?:off\b|rebates?\b|discounts?\b|savings?\b|coupons?\b|promo\b|gift\s*cards?\b|gc\b|e?credits?\b"
    r"|cash\s*back\b|cashback\b|rewards?\b|back\b|mir\b|shipping\b(?!\s+(?:included|incl))|s&h\b|s/h\b"
    r"|tax\b|fees?\b|deposit\b|bonus\b|trade[\s-]?in\b|value\b)",
    re.IGNORECASE,
)
_SKIP_BEFORE = re.compile(
    r"\b(?:save|saving|savings|extra|additional|discount|rebate|credit)\s*(?:of\s+|up\s+to\s+|an?\s+)?$",
    re.IGNORECASE,
)
_DASHES = frozenset("-\u2212\u2013\u2014")


class _Candidate(NamedTuple):
    start: int
    end: int
    value: float | None  # None when malformed/ambiguous/over the cap
    marker: str | None  # raw currency marker text ("US $", "CAD", "$"...)
    negative: bool


class _Money(NamedTuple):
    amount: float | None
    currency: str | None
    negative: bool  # amounts were found but every one carried a minus sign
    bare_dollar: bool  # currency came from a bare "$" (may be another dollar currency)


_NO_MONEY = _Money(None, None, False, False)


def _valid_groups(groups: list[str]) -> bool:
    if not groups or not 1 <= len(groups[0]) <= 3:
        return False
    return all(len(g) == 3 for g in groups[1:])


def _to_number(run: str, kilo: bool) -> float | None:
    """Interpret one digit run ("1.299,99", "1,299.99", "1'299", "1,2" + k)."""
    s = run
    for ch in "'\u00a0\u202f\u2009 ":
        if ch in s:
            s = s.replace(ch, "")
    dots = s.count(".")
    commas = s.count(",")
    if dots and commas:
        dec = "." if s.rfind(".") > s.rfind(",") else ","
        if s.count(dec) > 1:
            return None
        int_part, frac = s.split(dec)
        groups = int_part.split("," if dec == "." else ".")
        if not _valid_groups(groups):
            return None
        num = "".join(groups) + "." + frac
    elif dots or commas:
        parts = s.split("." if dots else ",")
        if len(parts) > 2:
            if kilo or not _valid_groups(parts):
                return None
            num = "".join(parts)
        else:
            head, tail = parts
            if len(tail) == 3 and head != "0" and len(head) <= 3:
                if kilo:
                    return None  # "1,250k": 1.25k or 1,250k? refuse to guess
                num = head + tail
            else:
                num = head + "." + tail
    else:
        num = s
    try:
        value = float(num)
    except ValueError:
        return None
    if kilo:
        value = round(value * 1000.0, 6)
    if not math.isfinite(value) or value > MAX_PRICE:
        return None
    return value


def _candidate(m: re.Match[str]) -> _Candidate:
    if m.group("n1") is not None:
        run, kilo, marker = m.group("n1"), m.group("k1") is not None, m.group("pre")
    else:
        run, kilo, marker = m.group("n2"), m.group("k2") is not None, m.group("post")
    negative = m.group("neg") is not None or m.group("neg2") is not None
    return _Candidate(m.start(), m.end(), _to_number(run, kilo), marker, negative)


def _match_floor(text: str, anchor: int, floor: int) -> int:
    """Earliest index a money match containing the anchor at ``anchor`` can start.

    Suffix forms ("1,299.99$", "1500 USD") start at the digit run right before the
    anchor (plus an optional sign); prefix forms ("-US $5", "-CAD $5") start at most
    ``_PREFIX_LOOKBACK`` characters before it.
    """
    i = anchor
    if i > floor and text[i - 1] in "  ":
        i -= 1
    j = i
    while j > floor and (text[j - 1].isdigit() or text[j - 1] in _RUN_SEPARATORS):
        j -= 1
    if j == i:
        return max(floor, anchor - _PREFIX_LOOKBACK)
    return max(floor, j - 1)


def _iter_text_candidates(text: str) -> Iterator[_Candidate]:
    """$/USD amounts in free text, left to right, tokenizing only next to anchors.

    Lazy, so callers that only need the first price stop scanning early.
    """
    pos = 0
    n = len(text)
    while True:
        anchor = _TEXT_ANCHOR.search(text, pos)
        if anchor is None:
            return
        # The window end only bounds work for an anchor without an amount ("$$$");
        # any real amount is far shorter than _ANCHOR_WINDOW characters.
        m = _TEXT_RE.search(text, _match_floor(text, anchor.start(), pos), min(n, anchor.end() + _ANCHOR_WINDOW))
        if m is None:
            pos = anchor.end()
            continue
        yield _candidate(m)
        pos = m.end()


def _currency_from_marker(marker: str | None) -> tuple[str | None, bool]:
    """Map a currency marker to (ISO code, is_bare_dollar)."""
    if not marker:
        return None, False
    m = marker.replace(" ", "").replace("\u00a0", "").upper()
    if m.endswith("$"):
        head = m[:-1]
        if head in _ISO_CODES:
            return head, False
        return _DOLLAR_PREFIX_CODES.get(head), head == ""
    if m in _ISO_CODES:
        return m, False
    return _SYMBOL_CODES.get(m), False


def _parse_money_detail(value: str | float | int | None) -> _Money:
    if value is None or isinstance(value, bool):
        return _NO_MONEY
    if not isinstance(value, str):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return _NO_MONEY
        if not math.isfinite(number):
            return _NO_MONEY
        if number < 0:
            return _Money(None, None, True, False)
        if number > MAX_PRICE:
            return _NO_MONEY
        return _Money(number, None, False, False)

    simple = _US_PRICE.fullmatch(value)
    if simple is not None:
        amount = float(simple.group(2).replace(",", ""))
        if amount > MAX_PRICE:
            return _NO_MONEY
        marker = simple.group(1)
        if marker is None:
            return _Money(amount, None, False, False)
        return _Money(amount, "USD", False, marker == "$")

    text = value.strip()
    if not text:
        return _NO_MONEY
    if "\u20e3" in text:
        text = _KEYCAP.sub(" ", text)
    lead = text.lstrip(" \t\"'*~!:([")
    if lead[:4].lower() == "free" and _FREE_ITEM.match(lead):  # "Free", "FREE - pick up only"
        return _Money(0.0, None, False, False)

    candidates = [_candidate(m) for m in _FIELD_RE.finditer(text)]
    chosen: _Candidate | None = None
    for cand in candidates:  # first valid currency-marked amount
        if cand.marker and cand.value is not None and not cand.negative:
            chosen = cand
            break
    if chosen is None:
        for cand in candidates:  # else the first valid plain number
            if not cand.marker and cand.value is not None and not cand.negative:
                chosen = cand
                break
    if chosen is None:
        if any(c.negative for c in candidates):
            return _Money(None, None, True, False)
        if not candidates and _FREE_ITEM.search(text):
            return _Money(0.0, None, False, False)
        return _NO_MONEY
    currency, bare = _currency_from_marker(chosen.marker)
    return _Money(chosen.value, currency, False, bare)


def parse_money(text: str | float | int | None) -> tuple[float | None, str | None]:
    """Parse a price field into ``(amount, ISO currency or None)``.

    ``"US $1,299.99"`` -> ``(1299.99, "USD")``; ``"1.299,99 €"`` -> ``(1299.99, "EUR")``;
    ``"C$1,100"`` -> ``(1100.0, "CAD")``; ``"$1.2k"`` -> ``(1200.0, "USD")``;
    ``"1299 OBO"`` -> ``(1299.0, None)``; ``"$800-$900"`` -> ``(800.0, "USD")``;
    ``"Free"`` -> ``(0.0, None)``. Unparseable ("Ask", "Contact for price"),
    ambiguous (``"$1,250k"``), negative, NaN or > 10,000,000 -> ``(None, None)``.
    The currency is ``None`` when the text carries no symbol or ISO code.
    """
    money = _parse_money_detail(text)
    if money.amount is None:
        return None, None
    return money.amount, money.currency


def parse_price(value: str | float | int | None) -> float | None:
    """Amount part of :func:`parse_money` (see there for the accepted formats)."""
    return _parse_money_detail(value).amount


# ----------------------------------------------------------------- prices in free text


def _enclosing_parens(text: str, pos: int) -> tuple[int, int] | None:
    open_idx = text.rfind("(", 0, pos)
    if open_idx == -1 or text.find(")", open_idx, pos) != -1:
        return None
    close_idx = text.find(")", pos)
    if close_idx == -1 or text.find("(", pos, close_idx) != -1:
        return None
    return open_idx, close_idx


def _is_skipped(text: str, cand: _Candidate) -> bool:
    if cand.negative:
        return True  # "(-$100 rebate)": a deduction, not a price
    if _SKIP_AFTER.match(text, cand.end):
        return True
    return _SKIP_BEFORE.search(text, max(0, cand.start - 28), cand.start) is not None


def _breakdown_value(text: str, first: _Candidate, cands: list[_Candidate], paren: tuple[int, int]) -> float:
    """Resolve "($1599 - $400)" -> 1199 and "($449 - $50 = $399)" -> 399."""
    base = first.value or 0.0
    group = [c for c in cands if c.start >= first.start and c.end <= paren[1] and c.value is not None]
    for prev, cand in zip(group, group[1:]):
        if text[prev.end : cand.start].strip().endswith("=") and cand.value is not None:
            return cand.value
    result = base
    for prev, cand in zip(group, group[1:]):
        gap = text[prev.end : cand.start].strip()
        if (gap in _DASHES) if gap else cand.negative:
            result -= cand.value or 0.0
        else:
            break
    if 0 < result < base:
        return round(result, 2)
    return base


def _find_text_price(text: str) -> tuple[float, str | None, bool] | None:
    """First explicit $/USD price in free text -> (amount, currency, bare_dollar)."""
    if not text:
        return None
    cands = _iter_text_candidates(text)
    first: _Candidate | None = None
    for cand in cands:
        if cand.value is not None and not _is_skipped(text, cand):
            first = cand
            break
    if first is None:
        return None
    value = first.value or 0.0
    paren = _enclosing_parens(text, first.start)
    if paren is not None:
        group = [first]
        for cand in cands:  # the rest of the parenthesised breakdown only
            if cand.start > paren[1]:
                break
            group.append(cand)
        value = _breakdown_value(text, first, group, paren)
    currency, bare = _currency_from_marker(first.marker)
    return value, currency, bare


def extract_title_price(text: str) -> float | None:
    """First explicit ``$``/``USD`` price in a title or post body.

    Bare numbers (model numbers ``4090``, ``24GB``, ``240Hz``, years, ``20%``) are
    never prices. Amounts labelled as discounts or fees ("$50 off", "+$20 shipping",
    "(-$100 rebate)") are skipped. ``"... - $1199 ($1599 - $400)"`` -> ``1199.0``;
    a breakdown that is the only price is resolved (``"($449 - $50 = $399)"`` ->
    ``399.0``). Rental rates (``"$2/hr"``) are still returned; the text filter
    rejects rentals.
    """
    found = _find_text_price(text)
    return found[0] if found is not None else None


# =========================================================================== condition

# eBay condition ids (Browse/Trading API). 1750 "New with defects" is treated as open
# box; 2750 "Like New" is a used item.
_EBAY_CONDITION_IDS: dict[int, Condition] = {
    1000: Condition.NEW,
    1500: Condition.OPEN_BOX,
    1750: Condition.OPEN_BOX,
    2000: Condition.REFURBISHED,
    2010: Condition.REFURBISHED,
    2020: Condition.REFURBISHED,
    2030: Condition.REFURBISHED,
    2500: Condition.REFURBISHED,
    2750: Condition.USED,
    2990: Condition.USED,
    3000: Condition.USED,
    3010: Condition.USED,
    4000: Condition.USED,
    5000: Condition.USED,
    6000: Condition.USED,
    7000: Condition.FOR_PARTS,
}

# Structured condition strings, normalised to lowercase words separated by one space
# ("Open-Box Excellent" -> "open box excellent", "USED_VERY_GOOD" -> "used very good").
# Order matters: the first matching rule wins.
_RAW_CONDITION_RULES: tuple[tuple[Condition, re.Pattern[str]], ...] = (
    (Condition.FOR_PARTS, re.compile(r"\b(?:for parts|parts only|parts or repair|not working|non working|salvage)\b")),
    (Condition.REFURBISHED, re.compile(r"\b(?:refurb\w*|renewed|reconditioned|remanufactured|re ?certified)\b")),
    (
        Condition.OPEN_BOX,
        re.compile(r"\b(?:open ?box|opened box|new other|new with defects|new without box|new no box|damaged box)\b"),
    ),
    (
        Condition.USED,
        re.compile(r"(?<!never )(?<!not )\bused\b|\b(?:pre ?owned|second ?hand|like new|gently used|lightly used|normal wear)\b"),
    ),
    (Condition.NEW, re.compile(r"\b(?:new|brand new|bnib|nib|nisb|nwt|bnwt|sealed|factory sealed|unopened)\b")),
    # Bare grades ("Very Good", "Excellent", Craigslist "fair") only describe used items.
    (Condition.USED, re.compile(r"\b(?:very good|good|acceptable|fair|excellent|mint|poor|worn|collectible)\b")),
)
_NON_WORD = re.compile(r"[^0-9a-z]+")

# Free-text hints over *lowercased* text. A bare literal alternation (which sre scans
# fast) finds trigger positions; _HINT_RE is then matched *anchored* there, and the
# word-start, exclusion and negation checks run in Python on those few positions.
# Specific alternatives precede the plain "new" fallback.
_TRIGGER_WORDS = (
    "for", "parts", "refurb", "renewed", "recond", "reman", "recert", "open", "box", "new", "used", "pre",
    "second", "like", "brand", "bnib", "nib", "nisb", "bnwt", "nwt", "factory", "sealed", "unopened",
)
# Each literal is followed by a lookbehind asserting it starts a word; it only runs after
# the literal matched, so scanning stays fast and mid-word hits ("geforce") never surface.
_HINT_TRIGGER = re.compile("|".join(rf"{w}(?<![a-z0-9_]{w})" for w in _TRIGGER_WORDS))
_HINT_RE = re.compile(
    r"""
      (?P<parts>for[\s-]+parts(?:\s+(?:or|and|/)\s+(?:repair|not\s+working))?|parts(?:[\s-]+only|\s*/\s*repair))\b
    | (?P<refurb>refurb(?:ished)?|renewed|reconditioned|remanufactured|recertified)\b
    | (?P<open>open[\s-]?box(?:ed)?|opened|box\s+(?:was\s+|has\s+been\s+)?opened|new\s+other|new\s+with\s+defects)\b
    | (?P<used>used|pre[\s-]?owned|second[\s-]?hand|like[\s-]new)\b
    | (?P<new>brand[\s-]new|b?nib|nisb|b?nwt|new\s+in\s+(?:the\s+)?(?:sealed\s+)?box|factory[\s-]sealed|new\s+sealed|sealed|unopened)\b
    | (?P<plain_new>new)\b
    """,
    re.VERBOSE,
)
_BE_BEFORE = re.compile(r"\bbe\s$")  # "can be used"
_RE_BEFORE = re.compile(r"\bre[\s-]$")  # "re-sealed"
_NEW_EXCLUDE_BEFORE = re.compile(r"\b(?:like|near|almost|as|pretty|basically)[\s-]$")
_NEW_EXCLUDE_AFTER = re.compile(r"[\s-]*(?:to\s+me|thermal|pads?|paste|fans?|cooler|price|listing|account|in\s+town|ish)\b")
_NEGATION = re.compile(r"\b(?:not|no|never|isn'?t|wasn'?t|aren'?t|without|non)\b[^.!?;,\n]{0,20}$")
_CLAUSE_BREAK = re.compile(r"[.!?;,\n]")

# Lower = safer (smaller market reference -> smaller apparent discount).
_SAFETY_RANK = {
    Condition.FOR_PARTS: 0,
    Condition.USED: 1,
    Condition.UNKNOWN: 1,
    Condition.REFURBISHED: 2,
    Condition.OPEN_BOX: 3,
    Condition.NEW: 4,
}
_HINT_CLASSES = {
    "parts": Condition.FOR_PARTS,
    "refurb": Condition.REFURBISHED,
    "open": Condition.OPEN_BOX,
    "used": Condition.USED,
    "new": Condition.NEW,
    "plain_new": Condition.NEW,
}


def _condition_from_raw(raw: str | int | None) -> Condition | None:
    if raw is None or isinstance(raw, bool):
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.isdigit():
        if len(text) != 4:
            return None
        cid = int(text)
        if cid in _EBAY_CONDITION_IDS:
            return _EBAY_CONDITION_IDS[cid]
        if 1000 <= cid < 1500:
            return Condition.NEW
        if 1500 <= cid < 2000:
            return Condition.OPEN_BOX
        if 2000 <= cid < 2750:
            return Condition.REFURBISHED
        if 2750 <= cid < 7000:
            return Condition.USED
        return None
    norm = _NON_WORD.sub(" ", text.lower()).strip()
    for condition, pattern in _RAW_CONDITION_RULES:
        if pattern.search(norm):
            return condition
    return None


def _negated(low: str, start: int) -> bool:
    """True when a negation word precedes ``start`` within the same clause."""
    window = low[max(0, start - 28) : start]
    last_break = None
    for last_break in _CLAUSE_BREAK.finditer(window):
        pass
    if last_break is not None:
        window = window[last_break.end() :]
    return _NEGATION.search(window) is not None


def _text_hints(low: str, title_end: int) -> set[Condition]:
    hints: set[Condition] = set()
    pos = 0
    while True:
        trigger = _HINT_TRIGGER.search(low, pos)
        if trigger is None:
            return hints
        start = trigger.start()
        pos = start + 1
        if start and low[start - 1].isalnum():
            continue  # not at a word start for non-ASCII letters ("\u00e9new")
        m = _HINT_RE.match(low, start)
        if m is None:
            continue
        pos = m.end()
        kind = m.lastgroup or ""
        if kind == "used" and low.startswith("used", start) and _BE_BEFORE.search(low, max(0, start - 4), start):
            continue
        if kind == "new" and low.startswith("sealed", start) and _RE_BEFORE.search(low, max(0, start - 4), start):
            continue
        if kind == "plain_new":
            # A bare "new" is trusted in the title only ("new thermal pads" is noise).
            if start >= title_end or _NEW_EXCLUDE_BEFORE.search(low, max(0, start - 10), start):
                continue
            if _NEW_EXCLUDE_AFTER.match(low, m.end()):
                continue
        if _negated(low, start):
            continue
        hints.add(_HINT_CLASSES[kind])


def _safest(base: Condition, hints: set[Condition]) -> Condition:
    """Apply downgrade-only hints to a base class."""
    best = base
    for hint in hints:
        if hint is not Condition.NEW and _SAFETY_RANK[hint] < _SAFETY_RANK[best]:
            best = hint
    return best


def _coerce_kind(kind: SourceKind | str) -> SourceKind | None:
    if isinstance(kind, SourceKind):
        return kind
    try:
        return SourceKind(str(kind).lower())
    except ValueError:
        return None


def parse_condition(raw: str | None, kind: SourceKind, text: str = "") -> Condition:
    """Classify an item's condition from a structured value, listing text and source kind.

    * ``raw`` (eBay id "3000", "Open-Box Excellent", "USED_GOOD"...) is authoritative;
      title hints may only downgrade it ("New" + "like new" in the title -> USED).
    * Without a recognisable ``raw``: RETAIL/AGGREGATOR default to NEW and are
      downgraded by open-box / refurbished / used / for-parts wording;
      LOCAL/MARKETPLACE default to USED and become NEW only on explicit "brand new /
      sealed / BNIB / NIB / new in box" wording (or a bare "new" in the title),
      capped to OPEN_BOX/REFURBISHED when the text also says so and overridden by
      any "used"/"like new" wording.
    * Negated hints ("never used", "not refurbished") are ignored. The first line of
      ``text`` is treated as the title.
    """
    source_kind = _coerce_kind(kind)
    low = text.lower() if text else ""
    title_end = low.find("\n")
    if title_end == -1:
        title_end = len(low)
    base = _condition_from_raw(raw)
    if base is not None:
        if base is Condition.FOR_PARTS or not low:
            return base
        return _safest(base, _text_hints(low[:title_end], title_end))

    hints = _text_hints(low, title_end) if low else set()
    if Condition.FOR_PARTS in hints:
        return Condition.FOR_PARTS
    if source_kind in (SourceKind.RETAIL, SourceKind.AGGREGATOR):
        return _safest(Condition.NEW, hints)
    if source_kind in (SourceKind.LOCAL, SourceKind.MARKETPLACE):
        if Condition.USED in hints:
            return Condition.USED
        if Condition.NEW in hints:
            if Condition.REFURBISHED in hints:
                return Condition.REFURBISHED
            if Condition.OPEN_BOX in hints:
                return Condition.OPEN_BOX
            return Condition.NEW
        return Condition.USED
    # Unknown kind: only an explicit textual statement decides (the safest one wins).
    if not hints:
        return Condition.UNKNOWN
    return min(hints, key=lambda c: _SAFETY_RANK[c])


# =========================================================================== URLs

_TRACKING_PARAMS = frozenset(
    {
        "fbclid", "gclid", "gclsrc", "dclid", "gbraid", "wbraid", "msclkid", "yclid", "twclid", "ttclid",
        "li_fat_id", "igshid", "igsh", "mc_cid", "mc_eid", "_ga", "_gl", "_hsenc", "_hsmi", "hsctatracking",
        "mkt_tok", "spm", "scm", "ref", "ref_", "irclickid", "irgwc", "clickid", "click_id", "s_kwcid",
        "ef_id", "_branch_match_id", "_bta_tid", "_bta_c", "wickedid", "rb_clickid", "_openstat", "ncid",
        "sr_share", "cmpid", "icid", "rdt", "srsltid", "cjevent", "cjdata", "zanpid", "ranmid", "raneaa",
        "ransiteid", "share_id", "si", "epik", "trk", "trkcampaign", "mibextid", "pk_campaign", "pk_kwd",
        "pk_keyword", "pk_source", "pk_medium", "pk_content", "pk_cid",
    }
)
_TRACKING_PREFIXES = (
    "utm_", "pf_rd_", "pd_rd_", "hsa_", "mtm_", "_hs", "__hs", "oly_", "vero_", "trk_", "ref_", "cm_",
    "__cft__", "__tn__", "__xts__",
)
_HOST_TRACKING_PARAMS: tuple[tuple[str, frozenset[str]], ...] = (
    (
        "ebay.",
        frozenset({"_trkparms", "_trksid", "hash", "amdata", "itmmeta", "itmprp", "sspagename", "_from", "nordt", "_ul", "ul_noapp"}),
    ),
    ("bestbuy.", frozenset({"loc", "acampid", "mpid", "cmp", "lid"})),
    (
        "facebook.",
        frozenset({"referral_code", "referral_story_type", "tracking", "rdid", "share_url", "notif_id", "notif_t", "acontext", "aref", "sfnsn"}),
    ),
    ("slickdeals.", frozenset({"src", "attrsrc"})),
    (
        "walmart.",
        frozenset(
            {
                "athbdg", "athcpid", "athpgid", "athznid", "athieid", "athstid", "athguid", "athwpid", "athtvid",
                "athancid", "athena", "adsredirect", "wmlspartner", "affiliates_ad_id", "veh", "sourceid", "wl13",
            }
        ),
    ),
    ("target.", frozenset({"clkid", "afid", "cpng", "lnk", "dpid"})),
)
_AMAZON_HOST = re.compile(r"(?:^|\.)amazon\.(?:com|ca|co\.uk|de|fr|it|es|co\.jp|com\.au|com\.mx|in|nl|se|pl|sg)$")
_AMAZON_ASIN = re.compile(r"/(?:dp|gp/product|gp/aw/d|exec/obidos/asin|o/asin)/([A-Z0-9]{10})(?=[/?]|$)", re.IGNORECASE)
_AMAZON_KEEP = frozenset({"th", "psc", "smid", "m", "tag", "aod"})


def _host_tracking(host: str) -> frozenset[str]:
    for marker, params in _HOST_TRACKING_PARAMS:
        if marker in host:
            return params
    return frozenset()


def _is_tracking(key: str, host_specific: frozenset[str]) -> bool:
    k = key.lower()
    return k in _TRACKING_PARAMS or k in host_specific or k.startswith(_TRACKING_PREFIXES)


def _clean_netloc(scheme: str, netloc: str) -> tuple[str, str]:
    """Return (netloc, host) with credentials, default port and trailing dot removed."""
    if "@" in netloc:
        netloc = netloc.rsplit("@", 1)[1]  # never keep credentials
    netloc = netloc.lower()
    if netloc.startswith("["):  # IPv6 literal
        end = netloc.find("]")
        host, rest = (netloc, "") if end == -1 else (netloc[: end + 1], netloc[end + 1 :])
        port = rest[1:] if rest.startswith(":") else ""
    else:
        host, _, port = netloc.partition(":")
        host = host.rstrip(".")
    if port and not ((scheme == "http" and port == "80") or (scheme == "https" and port == "443")):
        return f"{host}:{port}", host
    return host, host


def _canonicalize(url: str) -> tuple[str, bool]:
    """(canonical URL, is a usable http(s) URL with a host)."""
    if not url:
        return "", False
    u = url.strip().strip("<>").strip()
    if u.startswith("//"):
        u = "https:" + u
    elif u[:4].lower() == "www.":
        u = "https://" + u
    if " " in u:
        u = u.replace(" ", "%20")
    try:
        parts = urlsplit(u)
    except ValueError:
        return u, False
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https") or not parts.netloc:
        return u, False
    netloc, host = _clean_netloc(scheme, parts.netloc)
    if not host or host == "[]":
        return u, False
    path = parts.path or "/"
    query = parts.query

    if _AMAZON_HOST.search(host):
        m = _AMAZON_ASIN.search(path)
        if m:
            path = f"/dp/{m.group(1).upper()}"
        if query:
            query = "&".join(seg for seg in query.split("&") if unquote(seg.split("=", 1)[0]).lower() in _AMAZON_KEEP)
    elif query:
        host_specific = _host_tracking(host)
        query = "&".join(
            seg for seg in query.split("&") if seg and not _is_tracking(unquote(seg.split("=", 1)[0]), host_specific)
        )
    return urlunsplit((scheme, netloc, path, query, "")), True


def canonical_url(url: str) -> str:
    """Canonical form of a listing URL.

    Lowercases scheme and host, drops credentials, default ports, fragments and
    tracking parameters (``utm_*``, ``fbclid``, ``gclid``, ``mc_cid``, ``_ga``,
    ``spm``, ``ref_``, ``igshid``, eBay ``_trksid``/``hash``...), keeps functional
    parameters verbatim and in order (eBay ``var``, Best Buy ``skuId``...), and
    collapses Amazon product URLs to ``/dp/<ASIN>``. Never raises: input that is not
    an absolute http(s) URL is returned stripped (callers validate the scheme).
    """
    return _canonicalize(url)[0]


_WHITESPACE = re.compile(r"\s")


def _image_url(url: object) -> str | None:
    """Validate an image URL without touching its query (CDN signatures live there)."""
    if not isinstance(url, str):
        return None
    u = url.strip()
    if u.startswith("//"):
        u = "https:" + u
    head, sep, rest = u.partition("://")
    if not sep or head.lower() not in ("http", "https"):
        return None
    host, slash, tail = rest.partition("/")
    if not host or _WHITESPACE.search(host):
        return None
    if " " in tail:
        tail = tail.replace(" ", "%20")
    return f"{head.lower()}://{host.lower()}{slash}{tail}"


# =========================================================================== text

_TAG_DROP_BLOCKS = re.compile(r"<(script|style|noscript)\b[^>]*>.*?</\1\s*>", re.IGNORECASE | re.DOTALL)
_TAG_COMMENT = re.compile(r"<!--.*?-->|<!\[CDATA\[|\]\]>|<![A-Za-z][^>]*>", re.DOTALL)
_TAG_BREAK = re.compile(r"<\s*/?\s*(?:br|p|div|li|tr|h[1-6]|hr|ul|ol|table|blockquote)\b[^<>]*>", re.IGNORECASE)
_TAG_ANY = re.compile(r"</?[A-Za-z][^<>]*>")
_ENTITY = re.compile(r"&(?:#\d{1,7}|#[xX][0-9a-fA-F]{1,6}|[A-Za-z][A-Za-z0-9]{1,31});")
_CONTROL = re.compile(r"[\x00-\x08\x0e-\x1f\x7f-\x9f]")
_HSPACE = re.compile(r"[^\S\n]+")
_NEWLINES = re.compile(r" ?\n\s*")
_ALNUM = re.compile(r"[^\W_]")

# Trademark-ish signs are removed *before* NFKC so "RTX™" does not become "RTXTM".
_TRADEMARKS = re.compile("[\u2122\u00ae\u00a9\u2120]")
_INVISIBLE = re.compile("[\u200b-\u200f\u2060\ufeff\u00ad\u180e\u202a-\u202e\u2066-\u2069\ufe0e\ufe0f]")
_SINGLE_QUOTES = re.compile("[\u2018\u2019\u201a\u201b\u2032\u2035\u02bc\u02bb\u00b4]")
_DOUBLE_QUOTES = re.compile("[\u201c\u201d\u201e\u201f\u2033\u2036\u00ab\u00bb]")
_FANCY_DASHES = re.compile("[\u2010-\u2015\u2212\ufe58\ufe63]")


def _strip_tags(text: str) -> str:
    if "<s" in text or "<S" in text or "<n" in text or "<N" in text:
        text = _TAG_DROP_BLOCKS.sub(" ", text)
    if "<!" in text or "]]>" in text:
        text = _TAG_COMMENT.sub(" ", text)
    text = _TAG_BREAK.sub("\n", text)
    return _TAG_ANY.sub("", text)


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    if max_len <= 0:
        return ""
    cut = text[:max_len]
    if text[max_len].isspace():
        return cut.rstrip()
    idx = max(cut.rfind(" "), cut.rfind("\n"))
    if idx > 0 and idx >= max_len * 0.6:
        return cut[:idx].rstrip()
    return cut


def clean_text(text: str, max_len: int | None = None, *, keep_newlines: bool = True) -> str:
    """Make listing text safe and uniform for regex matching and display.

    HTML entities are unescaped (twice for double-escaped feeds), tags removed
    (block tags become line breaks, ``<script>``/``<style>`` bodies dropped), text
    is NFKC-normalised (after removing trademark signs so "RTX™" stays "RTX"),
    typographic quotes and dashes become ASCII, zero-width/bidi/control characters
    are removed and whitespace is collapsed; single ``\\n`` line breaks are kept
    unless ``keep_newlines=False``. With ``max_len`` the result is cut at a word
    boundary when one exists in the last 40 % of the budget, else hard-cut; no
    ellipsis is added.
    """
    if not text:
        return ""
    if not isinstance(text, str):
        text = str(text)
    if "<" in text:
        text = _strip_tags(text)
    for _ in range(2):
        if "&" not in text or not _ENTITY.search(text):
            break
        text = html.unescape(text)
        if "<" in text:
            text = _strip_tags(text)
    if "\r" in text:
        text = text.replace("\r\n", "\n").replace("\r", "\n")
    if not text.isascii():
        text = _TRADEMARKS.sub("", text)
        text = _SINGLE_QUOTES.sub("'", text)  # before NFKC: it turns U+00B4 into a combining mark
        text = unicodedata.normalize("NFKC", text)
        text = _INVISIBLE.sub("", text)
        text = _SINGLE_QUOTES.sub("'", text)
        text = _DOUBLE_QUOTES.sub('"', text)
        text = _FANCY_DASHES.sub("-", text)
    text = _CONTROL.sub("", text)
    if keep_newlines:
        text = _HSPACE.sub(" ", text)
        if "\n" in text:
            text = _NEWLINES.sub("\n", text)
        text = text.strip()
    else:
        text = " ".join(text.split())
    if max_len is not None:
        text = _truncate(text, max_len)
    return text


# =========================================================================== normalizer


def _normalize_currency_code(value: str | None) -> str | None:
    if not value or not isinstance(value, str):
        return None
    v = value.strip()
    if not v:
        return None
    if v in _SYMBOL_CODES:
        return _SYMBOL_CODES[v]
    upper = v.upper().replace(" ", "")
    if upper.endswith("$"):
        head = upper[:-1]
        if head in _ISO_CODES:
            return head
        if head in _DOLLAR_PREFIX_CODES:
            return _DOLLAR_PREFIX_CODES[head]
    return upper


def _parse_shipping(value: str | float | int | None) -> float | None:
    """Shipping cost; ``None`` = unknown ("Calculated", "Local pickup", garbage, negative)."""
    if value is None:
        return None
    if isinstance(value, str) and _FREE_SHIPPING.search(value):
        return 0.0
    return _parse_money_detail(value).amount


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class Normalizer:
    """Turns :class:`RawListing` objects into validated :class:`DealItem` objects.

    Rules: price falls back to the first explicit ``$`` amount in the title, then
    the description, when ``raw.price`` is missing/unparseable (``extra
    ["price_origin"]`` records the fallback); ``total_price = price + (shipping or
    0)``; the currency must be in ``filters.allowed_currencies``; URLs must be
    http(s) (an invalid ``outbound_url`` is dropped, an invalid ``url`` is an
    error); images are deduplicated, http(s) only, at most 8; the title is cleaned
    and cut to 300 chars, the description to 5000; ``posted_at`` becomes tz-aware
    UTC (naive = UTC); prices are rounded to cents. Stateless after construction,
    so one instance can be shared by all pipeline workers.
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.allowed_currencies = frozenset(c.strip().upper() for c in config.filters.allowed_currencies)

    def normalize(self, raw: RawListing) -> DealItem:
        """Normalize one listing; raises :class:`NormalizationError` when it is unusable."""
        title = clean_text(raw.title, MAX_TITLE_LEN, keep_newlines=False)
        if not _ALNUM.search(title):
            raise NormalizationError("empty_title", f"{raw.listing_key}: title {raw.title[:60]!r}")

        url, url_ok = _canonicalize(raw.url)
        if not url_ok:
            raise NormalizationError("bad_url", f"{raw.listing_key}: url {raw.url[:120]!r}")
        outbound: str | None = None
        if raw.outbound_url:
            outbound, outbound_ok = _canonicalize(raw.outbound_url)
            if not outbound_ok:
                log.debug(
                    "dropping invalid outbound_url",
                    extra={"listing": raw.listing_key, "outbound_url": raw.outbound_url[:120]},
                )
                outbound = None

        description = clean_text(raw.description, MAX_DESCRIPTION_LEN) if raw.description else ""

        # ---- price & currency
        money = _parse_money_detail(raw.price)
        if money.negative:
            raise NormalizationError("negative_price", f"{raw.listing_key}: price {raw.price!r}")
        extra = raw.extra
        amount, marker_currency, bare = money.amount, money.currency, money.bare_dollar
        if amount is None:
            origin = "title"
            found = _find_text_price(title)
            if found is None and description:
                origin = "description"
                found = _find_text_price(description)
            if found is None:
                raise NormalizationError("no_price", f"{raw.listing_key}: price {raw.price!r} and no $ amount in text")
            amount, marker_currency, bare = found
            extra = {**raw.extra, "price_origin": origin}

        listing_currency = _normalize_currency_code(raw.currency) or "USD"
        if marker_currency is None or (bare and listing_currency in _DOLLAR_CODES):
            currency = listing_currency
        else:
            currency = marker_currency
        if currency not in self.allowed_currencies:
            raise NormalizationError("unsupported_currency", f"{raw.listing_key}: {currency}")

        price = round(amount, 2)
        shipping = _parse_shipping(raw.shipping)
        if shipping is not None:
            shipping = round(shipping, 2)
        total = round(price + (shipping or 0.0), 2)
        list_price = parse_price(raw.list_price)
        if list_price is not None:
            list_price = round(list_price, 2) if list_price > 0 else None

        # ---- condition. Retail/aggregator descriptions mention other offers
        # ("also available refurbished"), so only their titles are used.
        if raw.source_kind in (SourceKind.RETAIL, SourceKind.AGGREGATOR) or not description:
            condition_text = title
        else:
            condition_text = f"{title}\n{description[:_CONDITION_TEXT_CHARS]}"
        condition = parse_condition(raw.condition, raw.source_kind, condition_text)

        # ---- images
        images: list[str] = []
        for candidate in raw.image_urls:
            img = _image_url(candidate)
            if img is None or img in images:
                continue
            images.append(img)
            if len(images) >= MAX_IMAGES:
                break

        retailer = clean_text(raw.retailer, 120, keep_newlines=False) if raw.retailer else ""
        sku = raw.sku.strip() if raw.sku else ""

        return DealItem(
            source=raw.source,
            source_kind=raw.source_kind,
            source_id=raw.source_id,
            url=url,
            title=title,
            description=description,
            price=price,
            currency=currency,
            shipping=shipping,
            total_price=total,
            list_price=list_price,
            condition=condition,
            seller=raw.seller,
            location=raw.location,
            image_urls=images,
            posted_at=_as_utc(raw.posted_at),
            in_stock=raw.in_stock,
            quantity=raw.quantity,
            retailer=retailer or None,
            sku=sku or None,
            outbound_url=outbound,
            query=raw.query,
            profile_hint=raw.profile_hint,
            extra=extra,
            received_at=_as_utc(raw.received_at) or utcnow(),
            normalized_at=utcnow(),
            node_id=raw.node_id,
        )


__all__ = [
    "MAX_DESCRIPTION_LEN",
    "MAX_IMAGES",
    "MAX_PRICE",
    "MAX_TITLE_LEN",
    "DealItem",
    "NormalizationError",
    "Normalizer",
    "RawListing",
    "canonical_url",
    "clean_text",
    "extract_title_price",
    "parse_condition",
    "parse_money",
    "parse_price",
]
