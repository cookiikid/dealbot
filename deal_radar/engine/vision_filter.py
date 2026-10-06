"""Photo validation of marketplace candidates with a local vision-language model.

Local peer-to-peer listings (FB Marketplace, OfferUp, Craigslist, r/hardwareswap)
are where most "too good to be true" prices live: empty boxes, coolers without a
PCB, cracked OLED panels, screenshots of someone else's listing. Text rules catch
the honest ones ("box only"); the photos catch the rest. :class:`VisionFilter`
asks a small VLM served by Ollama (or any OpenAI-compatible server: vLLM,
llama.cpp, LM Studio) on the user's RTX 3060 what each photo actually shows.

Flow of :meth:`VisionFilter.check`
----------------------------------
1. Per image, look up the answer cache (in-process LRU, then Redis when available)
   keyed by ``sha1(prompt version + model + profile id + image url)``. The profile
   id is part of the key because the prompt is profile specific.
2. Cache miss: download through the shared :class:`HttpClient` (browser identity,
   ``max_bytes`` cap, small retry policy), reject non-image content types, then
   decode / EXIF-orient / flatten / downscale to ``resize_max_side`` and re-encode
   as JPEG in ``asyncio.to_thread`` (Pillow is CPU bound and must not block the loop).
   Unusable images (404, HTML error page, oversized, corrupt) are skipped and the
   next listing photo is tried instead, up to ``max_images + 2`` candidates.
3. One model call per image, bounded by a semaphore of ``max_concurrency`` (the
   GPU is the scarce resource), ``timeout_seconds`` per call, no retries and no
   politeness rate limit (local endpoint: fail fast, never queue behind a backoff).
4. The model answers :data:`VISION_SCHEMA` (Ollama ``format`` / OpenAI
   ``response_format``). Answers are parsed defensively: prose or markdown fences
   around the JSON, Python-literal dicts, percent/word confidences and outputs
   truncated by ``num_predict`` are all tolerated.
5. Category → :class:`VisionVerdict`, then aggregation over the images:

   * a *defect* verdict (DAMAGED, PARTS_ONLY) with confidence >=
     ``negative_min_confidence`` always wins (highest confidence first);
   * an *absence* verdict (BOX_ONLY, SCREENSHOT, RECEIPT, STOCK_PHOTO, UNRELATED)
     with enough confidence wins unless another photo of the same listing is
     GENUINE with at least the same threshold. Sellers routinely add a photo of
     the retail box next to photos of the card; "this photo only shows the box"
     must not veto a listing whose first photo shows the actual item;
   * otherwise GENUINE if any image is genuine, else UNCERTAIN (low-confidence
     negatives therefore end up UNCERTAIN, never negative).

6. Two-stage cascade: an UNCERTAIN aggregate is re-asked to ``escalation_model``
   (when configured) reusing the already prepared images. If the escalation fails
   the primary UNCERTAIN result is returned rather than an error.

``check`` never raises: backend failures (timeouts, HTTP errors, an open circuit)
become an ``ERROR`` verdict and ``on_error`` policy is applied by the pipeline;
listings whose photos are all unusable get ``SKIPPED`` (the backend was healthy,
there was simply nothing to look at). A :class:`CircuitBreaker` (3 consecutive
failures → open for 30 s, doubling up to 5 min) turns a dead GPU box into
microsecond ``ERROR`` results instead of a ``timeout_seconds`` stall per listing;
an open circuit also skips the image downloads.

Performance (RTX 3060 12 GB, Ollama, Q4 weights)
------------------------------------------------
* A VLM call is image-encoder + prefill of the visual tokens + decode of the
  answer. Qwen2.5-VL turns every 28x28 pixel block into one token, so a raw 12 MP
  phone photo is thousands of visual tokens and several seconds of prefill alone;
  at <= 672 px the same photo is ~400-600 tokens. Downscaling is the main latency
  lever, followed by a short answer: ``num_predict`` ~100-160 caps the decode
  tail (the JSON answer is ~40-80 tokens, ``reason`` is limited to 160 chars).
* With that, small VLMs (qwen2.5vl:3b, gemma3:4b, minicpm-v) answer in roughly
  0.3-1.5 s per image on a 3060; 7B-class models in roughly 1-3 s. JPEG ``draft``
  mode lets Pillow decode large JPEGs at 1/2-1/8 scale, so preprocessing stays at
  a few ms, and the re-encoded ~50-100 KB payload is negligible over Tailscale.
* ``keep_alive`` keeps the weights resident; a cold load costs several seconds
  (logged and counted from Ollama's ``load_duration``). Changing ``num_ctx`` per
  request would force a reload, so it is deliberately never sent. :meth:`warmup`
  preloads the model(s) at startup.
* ``max_concurrency`` 2 matches what one 3060 serves without inflating per-call
  latency (set ``OLLAMA_NUM_PARALLEL`` accordingly); extra parallelism mostly
  queues on the GPU and pushes calls into their timeout.
* The pipeline only calls :meth:`check` for listings that :meth:`applies_to` and
  whose *preliminary* score already reaches ``vision.min_prelim_score``. Text
  filtering + scoring cost < 1 ms; a photo check costs ~1 s of a single GPU. Most
  listings are overpriced or rejected by text rules and never need the GPU, which
  keeps its queue empty for the few real candidates. The threshold sits below the
  alert threshold because a GENUINE verdict raises source trust (and with it the
  score) while a negative verdict adds risk.
* Cascade: the small model handles the clear cases in ~0.5-1 s; only UNCERTAIN
  aggregates (including low-confidence negatives) pay for the larger model. When
  both models do not fit in VRAM together, escalation also pays a model swap, so
  keep the escalation model for the genuinely ambiguous minority.
* Answers are cached per image for ``cache_ttl_seconds``: re-observations of the
  same listing (price drops, other workers, restarts with Redis) cost nothing.
"""

from __future__ import annotations

import ast
import asyncio
import base64
import hashlib
import io
import math
import re
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import aiohttp
from PIL import Image, ImageOps
from redis.exceptions import RedisError

from deal_radar.config_schema import AppConfig, Profile
from deal_radar.core.backoff import BackoffPolicy, RetryExhausted
from deal_radar.core.http import HttpClient, HttpStatusError, ResponseTooLarge, json_dumps, json_loads
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.core.ratelimit import CircuitBreaker
from deal_radar.engine.types import DealItem, VisionResult, VisionVerdict

if TYPE_CHECKING:  # pragma: no cover
    from redis.asyncio import Redis

log = get_logger("vision")

# --------------------------------------------------------------------------- answer schema

VISION_CATEGORIES: tuple[str, ...] = (
    "genuine",
    "box_only",
    "parts_only",
    "damaged",
    "screenshot",
    "receipt",
    "stock_photo",
    "unrelated",
    "uncertain",
)
REASON_MAX_CHARS = 160

# Property order matters: grammar-constrained decoding emits keys in schema order, so
# the cheap booleans and the decision come first and the free-text reason last.
VISION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "item_visible": {"type": "boolean"},
        "category": {"type": "string", "enum": list(VISION_CATEGORIES)},
        "damage_visible": {"type": "boolean"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "reason": {"type": "string", "maxLength": REASON_MAX_CHARS},
    },
    "required": ["item_visible", "category", "damage_visible", "confidence", "reason"],
    "additionalProperties": False,
}

# Every category name is also a VisionVerdict value.
CATEGORY_VERDICTS: dict[str, VisionVerdict] = {name: VisionVerdict(name) for name in VISION_CATEGORIES}

_CATEGORY_ALIASES: dict[str, str] = {
    "authentic": "genuine", "real": "genuine", "ok": "genuine", "valid": "genuine", "intact": "genuine",
    "item": "genuine", "genuine_item": "genuine", "legit": "genuine",
    "box": "box_only", "packaging": "box_only", "empty_box": "box_only", "box_only_no_item": "box_only",
    "parts": "parts_only", "part": "parts_only", "for_parts": "parts_only", "partial": "parts_only",
    "incomplete": "parts_only", "accessory": "parts_only", "accessories": "parts_only",
    "broken": "damaged", "cracked": "damaged", "damage": "damaged", "defective": "damaged",
    "screen": "screenshot", "screen_photo": "screenshot", "photo_of_screen": "screenshot", "screencap": "screenshot",
    "invoice": "receipt", "document": "receipt", "paperwork": "receipt", "order_confirmation": "receipt",
    "stock": "stock_photo", "stock_image": "stock_photo", "render": "stock_photo", "marketing": "stock_photo",
    "catalog": "stock_photo", "product_render": "stock_photo",
    "other": "unrelated", "irrelevant": "unrelated", "wrong_item": "unrelated", "not_related": "unrelated",
    "unknown": "uncertain", "unsure": "uncertain", "unclear": "uncertain", "none": "uncertain", "cannot_tell": "uncertain",
}

_WORD_CONFIDENCE: dict[str, float] = {
    "very_high": 0.95, "certain": 0.95, "high": 0.85, "medium": 0.6, "moderate": 0.6, "low": 0.3, "very_low": 0.1,
}

DEFAULT_CONFIDENCE = 0.5  # model omitted / garbled the confidence
_DAMAGE_FLAG_DISCOUNT = 0.8  # "genuine" + damage_visible: damage is a side judgement, trust it a bit less
_CONTRADICTION_DISCOUNT = 0.5  # answers whose fields contradict their category become UNCERTAIN

_DEFECT_VERDICTS = frozenset({VisionVerdict.DAMAGED, VisionVerdict.PARTS_ONLY})

# --------------------------------------------------------------------------- tuning constants

PROMPT_VERSION = "v1"  # part of the cache key: bump when the prompt changes meaningfully
MEMORY_CACHE_ENTRIES = 4096
IMAGE_FETCH_TIMEOUT_SECONDS = 5.0
IMAGE_FETCH_POLICY = BackoffPolicy(max_attempts=2, base_delay=0.15, max_delay=0.5, max_total_seconds=6.0)
IMAGE_ACCEPT = "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"
MIN_IMAGE_SIDE = 32  # tracking pixels / icons
MAX_IMAGE_PIXELS = 40_000_000  # refuse decompression bombs before decoding
EXTRA_IMAGE_CANDIDATES = 2  # how many further photos to try when some are unusable
REDIS_TIMEOUT_SECONDS = 0.5
REDIS_BACKOFF_SECONDS = 30.0
BREAKER_FAILURES = 3
BREAKER_RECOVERY_SECONDS = 30.0
BREAKER_MAX_SECONDS = 300.0
COLD_LOAD_LOG_MS = 1000.0

# Failures of the health / warmup probes (never raised to the caller).
_PROBE_ERRORS: tuple[type[BaseException], ...] = (
    TimeoutError,
    HttpStatusError,
    RetryExhausted,
    ResponseTooLarge,
    aiohttp.ClientError,
    OSError,
    ValueError,
)
_GENERIC_BINARY_TYPES = frozenset({"", "application/octet-stream", "binary/octet-stream", "application/binary"})
_NON_RASTER_IMAGE_TYPES = frozenset({"image/svg+xml"})


# --------------------------------------------------------------------------- prompt


_CATEGORY_KIND: dict[str, str] = {
    "gpu": "gpu",
    "workstation_gpu": "gpu",
    "graphics_card": "gpu",
    "monitor": "monitor",
    "tv": "tv",
    "television": "tv",
    "prebuilt": "prebuilt",
    "desktop": "prebuilt",
    "camera": "camera",
    "handheld": "handheld",
    "laptop": "laptop",
}

# Fallback keyword detection for categories not listed above. Order matters: a
# "prebuilt with RTX 4090" or an "RTX laptop" must not be treated as a bare GPU.
_KIND_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("prebuilt", re.compile(r"\b(prebuilt|pre-built|desktop pc|gaming pc|tower)\b", re.I)),
    ("laptop", re.compile(r"\b(laptop|notebook|macbook)\b", re.I)),
    ("handheld", re.compile(r"\b(handheld|steam deck|rog ally|legion go)\b", re.I)),
    ("tv", re.compile(r"\b(tv|television)\b", re.I)),
    ("monitor", re.compile(r"\b(monitor|display)\b", re.I)),
    ("camera", re.compile(r"\b(camera|mirrorless|dslr)\b", re.I)),
    ("gpu", re.compile(r"\b(gpu|graphics card|geforce|radeon|rtx|gtx|quadro)\b", re.I)),
)

_KIND_RULES: dict[str, str] = {
    "gpu": (
        "GPU rules: genuine needs the graphics card itself - its cooler with fans or a blower, the backplate "
        "or PCB, the PCIe gold-finger connector. A cooler or shroud without the PCB, or a bare PCB, is parts_only. "
        "A photo of a monitor or of a GPU-Z/benchmark screen is screenshot. A melted or burnt power connector, "
        "a cracked PCB or broken fan blades is damaged."
    ),
    "monitor": (
        "Monitor rules: genuine needs the monitor with its panel visible. Any crack, spider-web pattern, coloured "
        "lines, dead or black areas or pressure blotches on the panel is damaged. Only the stand, a bare panel or "
        "the electronics board is parts_only."
    ),
    "tv": (
        "TV rules: genuine needs the TV with its screen visible. Any crack, spider-web pattern, coloured lines, "
        "dead or black areas or pressure blotches on the screen is damaged. Only the stand, the remote or a "
        "connection box is parts_only."
    ),
    "prebuilt": (
        "Prebuilt PC rules: genuine needs a complete desktop computer - a populated case with motherboard, cooler "
        "and graphics card, or the whole closed tower. An empty case, a lone graphics card or another single "
        "component, or only peripherals is parts_only. A photo of a desktop screen or a spec list is screenshot."
    ),
    "camera": (
        "Camera rules: genuine needs the camera body. A lens alone, a battery, charger or other accessories "
        "without the body is parts_only. A cracked rear screen, a broken lens mount or heavy dents is damaged."
    ),
    "handheld": (
        "Handheld rules: genuine needs the handheld console itself. A cracked or shattered screen or broken "
        "sticks or buttons is damaged. Only the case, dock or accessories is parts_only."
    ),
    "laptop": (
        "Laptop rules: genuine needs the laptop itself. A cracked screen, a broken hinge or missing keys is "
        "damaged. Only the charger or a bare motherboard is parts_only."
    ),
    "generic": "Rules: genuine needs the advertised item itself, complete and intact.",
}


def prompt_kind(profile: Profile) -> str:
    """Which category-specific rule block applies to ``profile``."""
    kind = _CATEGORY_KIND.get(profile.category)
    if kind is not None:
        return kind
    haystack = f"{profile.category.replace('_', ' ')} {profile.name} {profile.vision_hint or ''}"
    for name, pattern in _KIND_PATTERNS:
        if pattern.search(haystack):
            return name
    return "generic"


def build_prompt(profile: Profile) -> str:
    """Compact, category-aware instruction for one listing photo.

    Text tokens are cheap next to ~500 visual tokens, but every token still costs
    prefill time, so the prompt states the categories once and asks for JSON only.
    """
    hint = profile.vision_hint or f"the {profile.category.replace('_', ' ')} itself"
    return (
        f"You check one photo from a second-hand listing for: {profile.name}.\n"
        f"A genuine photo shows {hint}.\n"
        "Pick exactly one category for THIS photo:\n"
        "genuine - the real item is clearly shown (its box or accessories next to it are fine)\n"
        "box_only - only the retail box or packaging, the item itself is not shown\n"
        "parts_only - only a part or accessory, not the complete item\n"
        "damaged - the item is shown with visible damage\n"
        "screenshot - a screenshot or a photo of a screen, web page or spec sheet\n"
        "receipt - a receipt, invoice, order confirmation or other document\n"
        "stock_photo - an official marketing render or catalog image, not a photo of a real unit\n"
        "unrelated - shows a different kind of object\n"
        "uncertain - you cannot tell\n"
        f"{_KIND_RULES[prompt_kind(profile)]}\n"
        "Reply with one JSON object only, no markdown, no extra text:\n"
        '{"item_visible": true|false, "category": "<category>", "damage_visible": true|false, '
        '"confidence": <0.0-1.0>, "reason": "<at most 15 words>"}'
    )


# --------------------------------------------------------------------------- answer parsing

_FENCE = re.compile(r"```[A-Za-z0-9_-]*\s*(.*?)```", re.S)
_TRAILING_COMMA = re.compile(r",\s*([}\]])")
_NUMBER = re.compile(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)")
_SALVAGE: dict[str, re.Pattern[str]] = {
    "category": re.compile(r"[\"']?category[\"']?\s*[:=]\s*[\"']?([A-Za-z][A-Za-z_ -]*)", re.I),
    "confidence": re.compile(r"[\"']?confidence[\"']?\s*[:=]\s*[\"']?([0-9.]+\s*%?|[A-Za-z][A-Za-z_ ]*)", re.I),
    "item_visible": re.compile(r"[\"']?item_visible[\"']?\s*[:=]\s*[\"']?(true|false|yes|no)", re.I),
    "damage_visible": re.compile(r"[\"']?damage_visible[\"']?\s*[:=]\s*[\"']?(true|false|yes|no)", re.I),
    "reason": re.compile(r"[\"']?reason[\"']?\s*[:=]\s*[\"']([^\"']*)", re.I),
}
_MAX_LITERAL_EVAL_CHARS = 8000


def _matching_brace(text: str, start: int) -> int | None:
    """Index of the ``}`` closing the object opened at ``start`` (string-aware)."""
    depth = 0
    in_string: str | None = None
    escaped = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_string is not None:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_string:
                in_string = None
            continue
        if ch in ('"', "'"):
            in_string = ch
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i
    return None


def _loads_lenient(raw: str) -> Any:
    try:
        return json_loads(raw)
    except ValueError:
        pass
    fixed = _TRAILING_COMMA.sub(r"\1", raw.replace("“", '"').replace("”", '"'))
    try:
        return json_loads(fixed)
    except ValueError:
        pass
    if len(raw) <= _MAX_LITERAL_EVAL_CHARS:  # Python-literal dicts: {'category': 'genuine', 'item_visible': True}
        try:
            return ast.literal_eval(raw)
        except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
            return None
    return None


_ANSWER_KEYS = ("category", "verdict", "label")
_MAX_OBJECTS_SCANNED = 16


def _looks_like_answer(obj: Mapping[str, Any]) -> bool:
    if any(k in obj for k in _ANSWER_KEYS):
        return True
    return any(isinstance(v, Mapping) and any(k in v for k in _ANSWER_KEYS) for v in obj.values())


def _first_object(text: str) -> dict[str, Any] | None:
    """First balanced JSON object in ``text``, preferring one that carries an answer."""
    first: dict[str, Any] | None = None
    start = text.find("{")
    scanned = 0
    while start != -1 and scanned < _MAX_OBJECTS_SCANNED:
        end = _matching_brace(text, start)
        if end is None:
            start = text.find("{", start + 1)
            continue
        scanned += 1
        obj = _loads_lenient(text[start : end + 1])
        if isinstance(obj, dict):
            if _looks_like_answer(obj):
                return obj
            if first is None:
                first = obj
            start = text.find("{", end + 1)  # skip past this whole object, including nested ones
        else:
            start = text.find("{", start + 1)
    return first


def _salvage_fields(text: str) -> dict[str, Any] | None:
    """Recover fields from a truncated / malformed answer (e.g. cut off by ``num_predict``)."""
    found: dict[str, Any] = {}
    for key, pattern in _SALVAGE.items():
        match = pattern.search(text)
        if match:
            found[key] = match.group(1).strip()
    return found if "category" in found else None


def extract_json_object(content: Any) -> dict[str, Any] | None:
    """First JSON object in a model answer, tolerating prose, fences and truncation."""
    if isinstance(content, Mapping):
        return dict(content)
    if not isinstance(content, str):
        return None
    text = content.strip()
    if not text:
        return None
    if text.startswith("{"):
        obj = _loads_lenient(text)
        if isinstance(obj, dict):
            return obj
    fallback: dict[str, Any] | None = None
    for chunk in [m.group(1) for m in _FENCE.finditer(text)] + [text]:
        obj = _first_object(chunk)
        if obj is not None and _looks_like_answer(obj):
            return obj
        fallback = fallback or obj
    salvaged = _salvage_fields(text)
    return salvaged if salvaged is not None else fallback


def normalize_category(value: Any) -> str:
    """Map a free-form category to one of :data:`VISION_CATEGORIES` (default ``uncertain``)."""
    if not isinstance(value, str):
        return "uncertain"
    key = re.sub(r"[^a-z]+", "_", value.strip().lower()).strip("_")
    if key in CATEGORY_VERDICTS:
        return key
    alias = _CATEGORY_ALIASES.get(key)
    if alias is not None:
        return alias
    for name in VISION_CATEGORIES:
        if key.startswith(name):
            return name
    return "uncertain"


def coerce_confidence(value: Any) -> float | None:
    """0..1 confidence from numbers, numeric strings, percentages or words."""
    if value is None or isinstance(value, bool):
        return None
    percent = False
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return None
        word = re.sub(r"[^a-z]+", "_", text).strip("_")
        if word in _WORD_CONFIDENCE:
            return _WORD_CONFIDENCE[word]
        match = _NUMBER.search(text)
        if match is None:
            return None
        number = float(match.group())
        percent = "%" in text
    else:
        return None
    if not math.isfinite(number):
        return None
    if percent or 1.0 < number <= 100.0:
        number /= 100.0
    return min(1.0, max(0.0, number))


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "yes", "y", "1"):
            return True
        if text in ("false", "no", "n", "0"):
            return False
    return None


def _pick(obj: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in obj:
            return obj[key]
    return None


def interpret_answer(obj: Mapping[str, Any]) -> dict[str, Any]:
    """Normalise a parsed answer into ``item_visible/category/damage_visible/confidence/reason``."""
    if not any(k in obj for k in ("category", "verdict", "label", "class")):
        for value in obj.values():  # {"answer": {...}} / {"result": {...}}
            if isinstance(value, Mapping) and any(k in value for k in ("category", "verdict", "label")):
                obj = value
                break
    reason = _pick(obj, "reason", "explanation", "why", "rationale")
    confidence = coerce_confidence(_pick(obj, "confidence", "score", "probability", "certainty"))
    return {
        "item_visible": _coerce_bool(_pick(obj, "item_visible", "visible", "item_present")),
        "category": normalize_category(_pick(obj, "category", "verdict", "label", "class")),
        "damage_visible": _coerce_bool(_pick(obj, "damage_visible", "damaged", "damage")),
        "confidence": DEFAULT_CONFIDENCE if confidence is None else confidence,
        "reason": (str(reason).strip() if reason is not None else "")[:REASON_MAX_CHARS],
    }


def verdict_for(fields: Mapping[str, Any]) -> tuple[VisionVerdict, float]:
    """Category → verdict, with consistency checks against the boolean fields."""
    category = fields.get("category", "uncertain")
    confidence = float(fields.get("confidence", DEFAULT_CONFIDENCE))
    item_visible = fields.get("item_visible")
    if category == "genuine":
        if fields.get("damage_visible") is True:
            return VisionVerdict.DAMAGED, confidence * _DAMAGE_FLAG_DISCOUNT
        if item_visible is False:
            return VisionVerdict.UNCERTAIN, confidence * _CONTRADICTION_DISCOUNT
        return VisionVerdict.GENUINE, confidence
    if category == "box_only" and item_visible is True:
        # "the box, but the item is visible" is usually item + box: not proof of a box-only listing.
        return VisionVerdict.UNCERTAIN, confidence * _CONTRADICTION_DISCOUNT
    return CATEGORY_VERDICTS.get(category, VisionVerdict.UNCERTAIN), confidence


# --------------------------------------------------------------------------- per-image answer


@dataclass(slots=True)
class ImageAnswer:
    url: str
    model: str
    category: str
    verdict: VisionVerdict
    confidence: float
    item_visible: bool | None = None
    damage_visible: bool | None = None
    reason: str = ""
    model_ms: float = 0.0
    cached: bool = False
    image_size: str | None = None
    parse_error: str | None = None

    @classmethod
    def from_fields(cls, url: str, model: str, fields: Mapping[str, Any], **extra: Any) -> ImageAnswer:
        verdict, confidence = verdict_for(fields)
        return cls(
            url=url,
            model=model,
            category=str(fields.get("category", "uncertain")),
            verdict=verdict,
            confidence=round(min(1.0, max(0.0, confidence)), 4),
            item_visible=fields.get("item_visible"),
            damage_visible=fields.get("damage_visible"),
            reason=str(fields.get("reason", ""))[:REASON_MAX_CHARS],
            **extra,
        )

    def to_cache(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "verdict": self.verdict.value,
            "confidence": self.confidence,
            "item_visible": self.item_visible,
            "damage_visible": self.damage_visible,
            "reason": self.reason,
            "model_ms": self.model_ms,
        }

    @classmethod
    def from_cache(cls, url: str, model: str, data: Any) -> ImageAnswer | None:
        if not isinstance(data, Mapping):
            return None
        try:
            verdict = VisionVerdict(data["verdict"])
            confidence = float(data["confidence"])
        except (KeyError, TypeError, ValueError):
            return None
        if not 0.0 <= confidence <= 1.0:
            return None
        return cls(
            url=url,
            model=model,
            category=str(data.get("category") or verdict.value),
            verdict=verdict,
            confidence=confidence,
            item_visible=_coerce_bool(data.get("item_visible")),
            damage_visible=_coerce_bool(data.get("damage_visible")),
            reason=str(data.get("reason") or "")[:REASON_MAX_CHARS],
            model_ms=float(data.get("model_ms") or 0.0),
            cached=True,
        )

    def summary(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "url": self.url,
            "model": self.model,
            "category": self.category,
            "verdict": self.verdict.value,
            "confidence": round(self.confidence, 3),
            "item_visible": self.item_visible,
            "damage_visible": self.damage_visible,
            "reason": self.reason,
            "cached": self.cached,
            "model_ms": round(self.model_ms, 1),
        }
        if self.image_size:
            out["image"] = self.image_size
        if self.parse_error:
            out["parse_error"] = self.parse_error
        return out


def aggregate_answers(
    answers: Sequence[ImageAnswer], negative_min_confidence: float
) -> tuple[VisionVerdict, float, ImageAnswer | None]:
    """Combine per-image answers into one listing verdict (see module docstring)."""
    if not answers:
        return VisionVerdict.UNCERTAIN, 0.0, None

    def conf(a: ImageAnswer) -> float:
        return a.confidence

    strong = [a for a in answers if a.verdict.is_negative and a.confidence >= negative_min_confidence]
    defects = [a for a in strong if a.verdict in _DEFECT_VERDICTS]
    if defects:
        top = max(defects, key=conf)
        return top.verdict, top.confidence, top
    genuine = [a for a in answers if a.verdict is VisionVerdict.GENUINE]
    best_genuine = max(genuine, key=conf) if genuine else None
    if strong and not (best_genuine is not None and best_genuine.confidence >= negative_min_confidence):
        top = max(strong, key=conf)
        return top.verdict, top.confidence, top
    if best_genuine is not None:
        return VisionVerdict.GENUINE, best_genuine.confidence, best_genuine
    top = max(answers, key=conf)
    return VisionVerdict.UNCERTAIN, top.confidence, top


# --------------------------------------------------------------------------- image preparation


class ImageRejected(Exception):
    """A listing photo that cannot be used (``reason`` is a short metric-friendly code)."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True, slots=True)
class PreparedImage:
    b64: str  # base64 JPEG, no data: prefix
    width: int
    height: int
    source_width: int
    source_height: int
    source_bytes: int
    jpeg_bytes: int
    source_format: str | None = None
    prep_ms: float = 0.0

    @property
    def size_label(self) -> str:
        return f"{self.width}x{self.height}"


def _flatten_to_rgb(img: Image.Image) -> Image.Image:
    if img.mode == "RGB":
        return img
    if img.mode in ("RGBA", "LA", "PA") or (img.mode == "P" and "transparency" in img.info):
        rgba = img.convert("RGBA")
        background = Image.new("RGB", rgba.size, (255, 255, 255))  # transparent PNGs would otherwise turn black
        background.paste(rgba, mask=rgba.getchannel("A"))
        return background
    return img.convert("RGB")


def prepare_image(data: bytes, *, max_side: int, quality: int) -> PreparedImage:
    """Decode, EXIF-orient, flatten and downscale one photo to a base64 JPEG.

    CPU bound (tens of ms for a 12 MP JPEG without draft mode): call it through
    ``asyncio.to_thread``. Raises :class:`ImageRejected`.
    """
    started = time.perf_counter()
    try:
        with Image.open(io.BytesIO(data)) as img:
            source_format = img.format
            src_w, src_h = img.size
            if src_w < MIN_IMAGE_SIDE or src_h < MIN_IMAGE_SIDE:
                raise ImageRejected("too_small", f"{src_w}x{src_h}")
            if src_w * src_h > MAX_IMAGE_PIXELS:
                raise ImageRejected("too_many_pixels", f"{src_w}x{src_h}")
            # JPEG only: decode directly at 1/2..1/8 scale (DCT scaling), still >= max_side.
            img.draft("RGB", (max_side, max_side))
            oriented = ImageOps.exif_transpose(img)
            rgb = _flatten_to_rgb(oriented)
            rgb.thumbnail((max_side, max_side), Image.Resampling.BICUBIC, reducing_gap=2.0)
            out = io.BytesIO()
            rgb.save(out, format="JPEG", quality=quality, optimize=False)
            width, height = rgb.size
    except ImageRejected:
        raise
    except Exception as exc:  # Pillow raises a zoo of types on corrupt input (OSError, SyntaxError, ValueError...)
        raise ImageRejected("decode_error", f"{type(exc).__name__}: {exc}") from exc
    jpeg = out.getvalue()
    return PreparedImage(
        b64=base64.b64encode(jpeg).decode("ascii"),
        width=width,
        height=height,
        source_width=src_w,
        source_height=src_h,
        source_bytes=len(data),
        jpeg_bytes=len(jpeg),
        source_format=source_format,
        prep_ms=round((time.perf_counter() - started) * 1000.0, 3),
    )


def _site(host: str) -> str:
    """Approximate registrable domain (last two labels) for Sec-Fetch-Site."""
    labels = [part for part in host.lower().split(".") if part]
    return ".".join(labels[-2:])


def image_request_headers(image_url: str, page_url: str | None) -> dict[str, str]:
    """Headers a browser sends when a listing page loads one of its photos via ``<img>``.

    Combined with ``fetch_mode="no-cors"`` (which drops the navigation-only headers),
    this mirrors a real subresource load: ``Sec-Fetch-Dest: image``, the
    same-site/cross-site relation to the listing page and an origin-only ``Referer``
    (the default ``strict-origin-when-cross-origin`` policy).
    """
    image_host = urlsplit(image_url).hostname or ""
    page = urlsplit(page_url) if page_url else None
    page_host = page.hostname if page is not None else None
    if not page_host or page is None or page.scheme not in ("http", "https"):
        return {"Sec-Fetch-Dest": "image", "Sec-Fetch-Site": "cross-site"}
    if page_host == image_host:
        relation = "same-origin"
    elif _site(page_host) == _site(image_host):
        relation = "same-site"
    else:
        relation = "cross-site"
    return {"Sec-Fetch-Dest": "image", "Sec-Fetch-Site": relation, "Referer": f"{page.scheme}://{page.netloc}/"}


def candidate_urls(urls: Sequence[str]) -> list[str]:
    """Unique http(s) image URLs in listing order."""
    seen: set[str] = set()
    out: list[str] = []
    for url in urls:
        if not isinstance(url, str):
            continue
        url = url.strip()
        if not url.lower().startswith(("http://", "https://")) or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


# --------------------------------------------------------------------------- internals


class _BackendFailure(Exception):
    """Model call failed (timeout, HTTP error, open circuit, malformed response)."""


@dataclass(slots=True)
class _Skip:
    reason: str


@dataclass(slots=True)
class _Failure:
    message: str


@dataclass(slots=True)
class _Pass:
    model: str
    answers: list[ImageAnswer] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class _ImageStore:
    """Per-check memo so the escalation pass reuses the images the primary pass prepared."""

    def __init__(self, fetch: Callable[[str], Any]) -> None:
        self._fetch = fetch
        self._done: dict[str, PreparedImage | ImageRejected] = {}

    async def get(self, url: str) -> PreparedImage | ImageRejected:
        hit = self._done.get(url)
        if hit is None:
            try:
                hit = await self._fetch(url)
            except ImageRejected as exc:
                hit = exc
            self._done[url] = hit
        return hit


class _TTLCache:
    """Small LRU with per-entry expiry (monotonic clock)."""

    def __init__(self, max_entries: int, clock: Callable[[], float] = time.monotonic) -> None:
        self.max_entries = max_entries
        self._clock = clock
        self._data: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()

    def get(self, key: str) -> dict[str, Any] | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        expires, value = entry
        if expires <= self._clock():
            del self._data[key]
            return None
        self._data.move_to_end(key)
        return value

    def put(self, key: str, value: dict[str, Any], ttl: float) -> None:
        self._data[key] = (self._clock() + ttl, value)
        self._data.move_to_end(key)
        while len(self._data) > self.max_entries:
            self._data.popitem(last=False)

    def clear(self) -> None:
        self._data.clear()

    def __len__(self) -> int:
        return len(self._data)


def _short(text: Any, limit: int = 200) -> str:
    value = str(text or "").replace("\n", " ").strip()
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _model_available(model: str, names: set[str]) -> bool:
    if model in names:
        return True
    if ":" not in model and f"{model}:latest" in names:
        return True
    return model.endswith(":latest") and model[: -len(":latest")] in names


def _model_names(data: Any) -> set[str]:
    names: set[str] = set()
    if not isinstance(data, Mapping):
        return names
    for key in ("models", "data"):  # Ollama /api/tags | OpenAI /v1/models (llama.cpp returns both)
        entries = data.get(key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, Mapping):
                for field_name in ("name", "model", "id"):
                    value = entry.get(field_name)
                    if isinstance(value, str) and value:
                        names.add(value)
            elif isinstance(entry, str):
                names.add(entry)
    return names


# --------------------------------------------------------------------------- filter


class VisionFilter:
    """Validates listing photos against the profile with a local VLM. See module docstring."""

    def __init__(
        self,
        config: AppConfig,
        http: HttpClient,
        *,
        metrics: Metrics | None = None,
        redis: Redis | None = None,
        breaker: CircuitBreaker | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.cfg = config.vision
        self.http = http
        self.metrics = metrics or Metrics()
        self.redis = redis
        self.breaker = breaker or CircuitBreaker(
            BREAKER_FAILURES, BREAKER_RECOVERY_SECONDS, max_timeout=BREAKER_MAX_SECONDS, clock=clock
        )
        self._clock = clock
        self._sem = asyncio.Semaphore(self.cfg.max_concurrency)
        self._memory = _TTLCache(MEMORY_CACHE_ENTRIES, clock=clock)
        self._redis_prefix = f"{config.storage.redis_key_prefix}vision:"
        self._redis_retry_at = 0.0
        self._closed = False
        self._circuit_state = CircuitBreaker.CLOSED
        base = self.cfg.base_url.rstrip("/")
        self._chat_url = f"{base}/api/chat" if self.cfg.backend == "ollama" else f"{base}/v1/chat/completions"
        self._models_url = f"{base}/api/tags" if self.cfg.backend == "ollama" else f"{base}/v1/models"

        m = self.metrics
        self._m_checks = m.counter("vision_checks_total", "Vision checks by final verdict", ("verdict",))
        self._m_check_ms = m.histogram("vision_check_ms", "Vision check wall time (ms)", ())
        self._m_calls = m.counter("vision_model_calls_total", "Vision model calls", ("model", "outcome"))
        self._m_model_ms = m.histogram("vision_model_ms", "Vision model call latency (ms)", ("model",))
        self._m_images = m.counter("vision_images_total", "Listing photos prepared or skipped", ("outcome",))
        self._m_prep_ms = m.histogram("vision_image_prep_ms", "Photo decode + downscale time (ms)", ())
        self._m_cache = m.counter("vision_cache_total", "Vision answer cache lookups", ("layer", "result"))
        self._m_escalations = m.counter("vision_escalations_total", "UNCERTAIN results escalated", ("outcome",))
        self._m_cold = m.counter("vision_cold_loads_total", "Model calls that paid a model load", ("model",))
        self._m_circuit = m.gauge("vision_circuit_open", "1 while the vision backend circuit is open", ())

    # ------------------------------------------------------------------ public API

    def applies_to(self, item: DealItem, profile: Profile) -> bool:
        """Whether ``item`` should get a photo check under ``profile``."""
        cfg = self.cfg
        if not cfg.enabled or profile.vision == "off" or not item.image_urls:
            return False
        if profile.vision == "required":
            return True
        return item.source_kind in cfg.apply_to_kinds or item.source in cfg.apply_to_sources

    async def check(self, item: DealItem, profile: Profile) -> VisionResult:
        """Validate the listing photos. Never raises; backend failures give an ``ERROR`` verdict."""
        started = time.perf_counter()
        try:
            result = await self._check(item, profile, started)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # last-resort guard: a vision bug must never kill a pipeline worker
            log.exception("vision check failed", extra={"listing": item.listing_key, "profile": profile.id})
            result = self._result(VisionVerdict.ERROR, started, error=f"internal: {type(exc).__name__}: {_short(exc)}")
        self._m_checks.inc(verdict=result.verdict.value)
        self._m_check_ms.observe(result.latency_ms)
        log.debug(
            "vision verdict",
            extra={
                "listing": item.listing_key,
                "profile": profile.id,
                "verdict": result.verdict.value,
                "confidence": result.confidence,
                "vision_model": result.model,
                "latency_ms": result.latency_ms,
                "cached": result.cached,
                "images": result.images_checked,
            },
        )
        return result

    async def health(self) -> bool:
        """Backend reachable and (for Ollama) the configured model pulled. Never raises."""
        cfg = self.cfg
        try:
            resp = await self.http.get_json(
                self._models_url,
                headers=self._auth_headers(),
                timeout=min(5.0, cfg.timeout_seconds),
                retry=False,
                rate_limit=False,
            )
        except asyncio.CancelledError:
            raise
        except _PROBE_ERRORS as exc:
            log.warning("vision backend unreachable", extra={"url": self._models_url, "error": _short(repr(exc))})
            return False
        names = _model_names(resp.data)
        wanted = [cfg.model] + ([cfg.escalation_model] if cfg.escalation_model else [])
        missing = [m for m in wanted if not _model_available(m, names)]
        if missing:
            log.warning(
                "vision model(s) not listed by backend",
                extra={"missing": missing, "available": sorted(names)[:20], "backend": cfg.backend},
            )
        if cfg.backend == "ollama":
            # Ollama answers 404 for models that are not pulled; the escalation model is optional.
            return cfg.model not in missing
        # OpenAI-compatible single-model servers (llama.cpp) often ignore the model name.
        return True

    async def warmup(self) -> bool:
        """Load the model(s) into VRAM ahead of the first real check (Ollama only). Never raises.

        Ollama loads a model on an empty ``/api/chat`` request and keeps it for
        ``keep_alive``; this moves the multi-second cold load off the alert path.
        """
        if self.cfg.backend != "ollama":
            return await self.health()
        ok = True
        models = [self.cfg.model] + ([self.cfg.escalation_model] if self.cfg.escalation_model else [])
        for model in models:
            try:
                await self.http.post_json(
                    self._chat_url,
                    {"model": model, "messages": [], "keep_alive": self.cfg.keep_alive},
                    headers=self._auth_headers(),
                    timeout=max(60.0, self.cfg.timeout_seconds),
                    retry=False,
                    rate_limit=False,
                )
            except asyncio.CancelledError:
                raise
            except _PROBE_ERRORS as exc:
                log.warning("vision warmup failed", extra={"vision_model": model, "error": _short(repr(exc))})
                ok = False
        return ok

    async def close(self) -> None:
        """Drop cached answers. The HTTP client and Redis connection are shared and not closed here."""
        self._closed = True
        self._memory.clear()

    # ------------------------------------------------------------------ check internals

    async def _check(self, item: DealItem, profile: Profile, started: float) -> VisionResult:
        cfg = self.cfg
        if self._closed:
            return self._result(VisionVerdict.ERROR, started, error="vision filter closed")
        if not cfg.enabled or profile.vision == "off":
            return self._result(VisionVerdict.SKIPPED, started, error="vision disabled")
        urls = candidate_urls(item.image_urls)
        if not urls:
            return self._result(VisionVerdict.SKIPPED, started, error="no images")

        prompt = build_prompt(profile)
        images = _ImageStore(lambda url: self._fetch_image(url, item.url))
        primary = await self._run_pass(
            cfg.model, profile, prompt, urls[: cfg.max_images + EXTRA_IMAGE_CANDIDATES], images, want=cfg.max_images
        )
        details: dict[str, Any] = {"prompt_kind": prompt_kind(profile)}
        if primary.skipped:
            details["skipped"] = [{"url": u, "reason": r} for u, r in primary.skipped.items()]
        if primary.errors:
            details["errors"] = primary.errors
        if not primary.answers:
            details["answers"] = []
            if primary.errors:
                return self._result(VisionVerdict.ERROR, started, error="; ".join(dict.fromkeys(primary.errors)), details=details)
            return self._result(VisionVerdict.SKIPPED, started, error="no usable images", details=details)

        verdict, confidence, decisive = aggregate_answers(primary.answers, cfg.negative_min_confidence)
        answers = list(primary.answers)
        final = primary
        details["escalated"] = False
        if verdict is VisionVerdict.UNCERTAIN and cfg.escalation_model and cfg.escalation_model != cfg.model:
            escalation = await self._run_pass(
                cfg.escalation_model, profile, prompt, [a.url for a in primary.answers], images, want=len(primary.answers)
            )
            answers.extend(escalation.answers)
            if escalation.answers:
                details["escalated"] = True
                details["primary_verdict"] = verdict.value
                details["primary_confidence"] = round(confidence, 3)
                verdict, confidence, decisive = aggregate_answers(escalation.answers, cfg.negative_min_confidence)
                final = escalation
                self._m_escalations.inc(outcome="answered")
            else:
                details["escalation_error"] = "; ".join(dict.fromkeys(escalation.errors)) or "no answers"
                self._m_escalations.inc(outcome="failed")

        details["answers"] = [a.summary() for a in answers]
        if decisive is not None and decisive.reason:
            details["reason"] = decisive.reason
        return self._result(
            verdict,
            started,
            confidence=confidence,
            model=final.model,
            images_checked=len(final.answers),
            cached=all(a.cached for a in answers),
            details=details,
        )

    async def _run_pass(
        self,
        model: str,
        profile: Profile,
        prompt: str,
        urls: Sequence[str],
        images: _ImageStore,
        *,
        want: int,
    ) -> _Pass:
        """Answer up to ``want`` photos with ``model``, falling back to later photos when one is unusable."""
        result = _Pass(model)
        queue = list(urls)
        while queue and len(result.answers) < want and not result.errors:
            batch, queue = queue[: want - len(result.answers)], queue[want - len(result.answers) :]
            outcomes = await asyncio.gather(*(self._answer(model, profile, prompt, url, images) for url in batch))
            for url, outcome in zip(batch, outcomes, strict=True):
                if isinstance(outcome, ImageAnswer):
                    result.answers.append(outcome)
                elif isinstance(outcome, _Skip):
                    result.skipped[url] = outcome.reason
                else:
                    result.errors.append(outcome.message)
        return result

    async def _answer(
        self, model: str, profile: Profile, prompt: str, url: str, images: _ImageStore
    ) -> ImageAnswer | _Skip | _Failure:
        key = self._cache_key(model, profile.id, url)
        cached = await self._cache_get(key, url, model)
        if cached is not None:
            return cached
        if self.breaker.state == CircuitBreaker.OPEN:
            # Fail fast: no download, no request while the backend is known to be down.
            self._m_calls.inc(model=model, outcome="circuit_open")
            return _Failure("circuit_open")
        image = await images.get(url)
        if isinstance(image, ImageRejected):
            log.debug("vision image skipped", extra={"url": url, "reason": image.reason, "detail": _short(image.detail)})
            return _Skip(image.reason)
        try:
            answer = await self._ask_model(model, prompt, url, image)
        except _BackendFailure as exc:
            return _Failure(str(exc))
        if answer.parse_error is None:
            await self._cache_put(key, answer)
        return answer

    async def _fetch_image(self, url: str, page_url: str | None = None) -> PreparedImage:
        cfg = self.cfg
        try:
            resp = await self.http.get_bytes(
                url,
                max_bytes=cfg.max_image_bytes,
                browser_identity=True,
                fetch_mode="no-cors",  # an <img> subresource load, not a navigation
                headers=image_request_headers(url, page_url),
                accept=IMAGE_ACCEPT,
                policy=IMAGE_FETCH_POLICY,
                timeout=IMAGE_FETCH_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except ResponseTooLarge as exc:
            self._m_images.inc(outcome="too_large")
            raise ImageRejected("too_large", str(exc)) from exc
        except HttpStatusError as exc:
            self._m_images.inc(outcome="http_error")
            raise ImageRejected(f"http_{exc.status}", _short(exc.body)) from exc
        except (TimeoutError, RetryExhausted, aiohttp.ClientError, OSError, ValueError) as exc:
            self._m_images.inc(outcome="fetch_error")
            raise ImageRejected("fetch_error", f"{type(exc).__name__}: {_short(exc)}") from exc
        content_type = str(resp.headers.get("Content-Type", "")).split(";", 1)[0].strip().lower()
        if content_type in _NON_RASTER_IMAGE_TYPES or (
            not content_type.startswith("image/") and content_type not in _GENERIC_BINARY_TYPES
        ):
            self._m_images.inc(outcome="not_image")
            raise ImageRejected("not_image", content_type)
        data = resp.data
        if not isinstance(data, (bytes, bytearray)) or not data:
            self._m_images.inc(outcome="empty")
            raise ImageRejected("empty")
        try:
            prepared = await asyncio.to_thread(prepare_image, bytes(data), max_side=cfg.resize_max_side, quality=cfg.jpeg_quality)
        except ImageRejected as exc:
            self._m_images.inc(outcome=exc.reason)
            raise
        self._m_images.inc(outcome="ok")
        self._m_prep_ms.observe(prepared.prep_ms)
        return prepared

    # ------------------------------------------------------------------ model backend

    def _auth_headers(self) -> dict[str, str] | None:
        if self.cfg.api_key is None:
            return None
        return {"Authorization": f"Bearer {self.cfg.api_key.get_secret_value()}"}

    def _payload(self, model: str, prompt: str, image: PreparedImage) -> dict[str, Any]:
        cfg = self.cfg
        if cfg.backend == "ollama":
            return {
                "model": model,
                "messages": [{"role": "user", "content": prompt, "images": [image.b64]}],
                "stream": False,
                "format": VISION_SCHEMA,
                "keep_alive": cfg.keep_alive,
                "options": {"temperature": 0, "num_predict": cfg.num_predict},
            }
        return {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image.b64}"}},
                    ],
                }
            ],
            "stream": False,
            "temperature": 0,
            "max_tokens": cfg.num_predict,
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": "listing_photo_check", "schema": VISION_SCHEMA},
            },
        }

    def _extract_content(self, data: Any) -> tuple[Any, dict[str, Any]]:
        """(answer content or None for a malformed response, backend timing stats)."""
        stats: dict[str, Any] = {}
        if not isinstance(data, Mapping):
            return None, stats
        if self.cfg.backend == "ollama":
            for key in ("total_duration", "load_duration", "prompt_eval_duration", "eval_duration"):
                value = data.get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    stats[key.replace("_duration", "_ms")] = round(value / 1e6, 1)  # Ollama reports nanoseconds
            for key in ("prompt_eval_count", "eval_count", "done_reason"):
                if data.get(key) is not None:
                    stats[key] = data[key]
            message = data.get("message")
            content = message.get("content") if isinstance(message, Mapping) else None
            if content is None:
                content = data.get("response")  # /api/generate-shaped proxies
            return content, stats
        choices = data.get("choices")
        if not isinstance(choices, list) or not choices or not isinstance(choices[0], Mapping):
            return None, stats
        first = choices[0]
        if first.get("finish_reason") is not None:
            stats["finish_reason"] = first["finish_reason"]
        usage = data.get("usage")
        if isinstance(usage, Mapping):
            for key in ("prompt_tokens", "completion_tokens"):
                if usage.get(key) is not None:
                    stats[key] = usage[key]
        message = first.get("message")
        if not isinstance(message, Mapping):
            return None, stats
        content = message.get("parsed") or message.get("content")
        if isinstance(content, list):  # content-part arrays from some servers
            content = "".join(str(p.get("text", "")) for p in content if isinstance(p, Mapping))
        if not content and isinstance(message.get("reasoning_content"), str):
            content = message["reasoning_content"]  # reasoning servers that put everything there
        return ("" if content is None else content), stats

    def _fail(self, model: str, outcome: str, message: str, started: float) -> _BackendFailure:
        elapsed = (time.perf_counter() - started) * 1000.0
        self.breaker.record_failure()
        self._sync_circuit()
        self._m_calls.inc(model=model, outcome=outcome)
        self._m_model_ms.observe(elapsed, model=model)
        log.warning(
            "vision backend call failed",
            extra={"vision_model": model, "outcome": outcome, "error": message, "elapsed_ms": round(elapsed, 1)},
        )
        return _BackendFailure(message)

    async def _ask_model(self, model: str, prompt: str, url: str, image: PreparedImage) -> ImageAnswer:
        cfg = self.cfg
        payload = self._payload(model, prompt, image)
        async with self._sem:  # the GPU is the bottleneck: bound in-flight inferences
            if not self.breaker.allow():
                self._m_calls.inc(model=model, outcome="circuit_open")
                raise _BackendFailure("circuit_open")
            started = time.perf_counter()
            try:
                resp = await self.http.post_json(
                    self._chat_url,
                    payload,
                    headers=self._auth_headers(),
                    timeout=cfg.timeout_seconds,
                    rate_limit=False,
                    retry=False,
                )
            except asyncio.CancelledError:
                if self.breaker.state != CircuitBreaker.CLOSED:
                    self.breaker.record_failure()  # never leave a half-open trial dangling
                    self._sync_circuit()
                raise
            except TimeoutError as exc:  # before OSError: TimeoutError subclasses it
                raise self._fail(model, "timeout", f"timeout after {cfg.timeout_seconds:g}s", started) from exc
            except HttpStatusError as exc:
                raise self._fail(model, "http_error", f"HTTP {exc.status}: {_short(exc.body)}", started) from exc
            except (RetryExhausted, ResponseTooLarge, aiohttp.ClientError, OSError, ValueError) as exc:
                raise self._fail(model, "transport_error", f"{type(exc).__name__}: {_short(exc)}", started) from exc
        elapsed = (time.perf_counter() - started) * 1000.0
        content, stats = self._extract_content(resp.data)
        if content is None:
            raise self._fail(model, "bad_response", f"unexpected response shape: {_short(json_dumps(resp.data))}", started)
        self.breaker.record_success()
        self._sync_circuit()
        self._m_model_ms.observe(elapsed, model=model)
        load_ms = stats.get("load_ms")
        if isinstance(load_ms, (int, float)) and load_ms >= COLD_LOAD_LOG_MS:
            self._m_cold.inc(model=model)
            log.info("vision model cold load", extra={"vision_model": model, "load_ms": load_ms, "keep_alive": cfg.keep_alive})

        parsed = extract_json_object(content)
        extra = {"model_ms": round(elapsed, 3), "image_size": image.size_label}
        if parsed is None:
            self._m_calls.inc(model=model, outcome="unparseable")
            log.info("vision answer unparseable", extra={"vision_model": model, "content": _short(content), **stats})
            return ImageAnswer(
                url=url,
                model=model,
                category="uncertain",
                verdict=VisionVerdict.UNCERTAIN,
                confidence=0.0,
                reason=_short(content, REASON_MAX_CHARS),
                parse_error="unparseable answer",
                **extra,
            )
        self._m_calls.inc(model=model, outcome="ok")
        answer = ImageAnswer.from_fields(url, model, interpret_answer(parsed), **extra)
        log.debug(
            "vision answer",
            extra={"vision_model": model, "category": answer.category, "verdict": answer.verdict.value,
                   "confidence": answer.confidence, "model_ms": answer.model_ms, **stats},
        )
        return answer

    def _sync_circuit(self) -> None:
        state = self.breaker.state
        is_open = state == CircuitBreaker.OPEN
        self._m_circuit.set(1.0 if is_open else 0.0)
        if state != self._circuit_state:
            if is_open:
                log.warning(
                    "vision circuit opened",
                    extra={
                        "retry_in_s": round(self.breaker.seconds_until_retry(), 1),
                        "failures": self.breaker.consecutive_failures,
                    },
                )
            elif state == CircuitBreaker.CLOSED:
                log.info("vision circuit closed")
            self._circuit_state = state

    # ------------------------------------------------------------------ cache

    def _cache_key(self, model: str, profile_id: str, url: str) -> str:
        return hashlib.sha1(f"{PROMPT_VERSION}\x1f{model}\x1f{profile_id}\x1f{url}".encode()).hexdigest()

    async def _cache_get(self, key: str, url: str, model: str) -> ImageAnswer | None:
        ttl = self.cfg.cache_ttl_seconds
        if ttl <= 0:
            return None
        data = self._memory.get(key)
        if data is not None:
            answer = ImageAnswer.from_cache(url, model, data)
            if answer is not None:
                self._m_cache.inc(layer="memory", result="hit")
                return answer
        self._m_cache.inc(layer="memory", result="miss")
        raw = await self._redis_call("get", key)
        if raw is None:
            if self.redis is not None:
                self._m_cache.inc(layer="redis", result="miss")
            return None
        try:
            data = json_loads(raw if isinstance(raw, (bytes, bytearray)) else str(raw))
        except (ValueError, TypeError):
            data = None
        answer = ImageAnswer.from_cache(url, model, data)
        if answer is None:
            self._m_cache.inc(layer="redis", result="corrupt")
            return None
        self._m_cache.inc(layer="redis", result="hit")
        self._memory.put(key, answer.to_cache(), ttl)
        return answer

    async def _cache_put(self, key: str, answer: ImageAnswer) -> None:
        ttl = self.cfg.cache_ttl_seconds
        if ttl <= 0:
            return
        data = answer.to_cache()
        self._memory.put(key, data, ttl)
        await self._redis_call("set", key, json_dumps(data), ttl)

    async def _redis_call(self, op: str, key: str, value: str | None = None, ttl: int = 0) -> Any:
        """Best-effort Redis GET/SET; a failing Redis is bypassed for a while instead of slowing every check."""
        if self.redis is None or self._clock() < self._redis_retry_at:
            return None
        redis_key = self._redis_prefix + key
        try:
            if op == "get":
                return await asyncio.wait_for(self.redis.get(redis_key), REDIS_TIMEOUT_SECONDS)
            await asyncio.wait_for(self.redis.set(redis_key, value, ex=max(1, int(ttl))), REDIS_TIMEOUT_SECONDS)
            return None
        except asyncio.CancelledError:
            raise
        except (TimeoutError, RedisError, OSError) as exc:
            self._redis_retry_at = self._clock() + REDIS_BACKOFF_SECONDS
            self._m_cache.inc(layer="redis", result="error")
            log.warning("vision cache redis error; bypassing redis", extra={"op": op, "error": _short(repr(exc))})
            return None

    # ------------------------------------------------------------------ results

    def _result(
        self,
        verdict: VisionVerdict,
        started: float,
        *,
        confidence: float = 0.0,
        model: str | None = None,
        images_checked: int = 0,
        cached: bool = False,
        details: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> VisionResult:
        return VisionResult(
            verdict=verdict,
            confidence=round(min(1.0, max(0.0, confidence)), 4),
            model=model or self.cfg.model,
            latency_ms=round((time.perf_counter() - started) * 1000.0, 3),
            images_checked=images_checked,
            cached=cached,
            details=details or {},
            error=error,
        )


__all__ = [
    "CATEGORY_VERDICTS",
    "VISION_CATEGORIES",
    "VISION_SCHEMA",
    "ImageAnswer",
    "ImageRejected",
    "PreparedImage",
    "VisionFilter",
    "aggregate_answers",
    "build_prompt",
    "candidate_urls",
    "coerce_confidence",
    "extract_json_object",
    "image_request_headers",
    "interpret_answer",
    "normalize_category",
    "prepare_image",
    "prompt_kind",
    "verdict_for",
]
