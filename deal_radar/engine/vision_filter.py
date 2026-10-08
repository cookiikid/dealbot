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
   Only web photo formats are decoded (JPEG/MPO, PNG, WebP, GIF, BMP, AVIF when
   Pillow has it): listing photos are attacker-controlled bytes and must not reach
   the long tail of Pillow decoders (TIFF, PCX, SGI, ...). Unusable images (404,
   HTML error page, oversized, corrupt) are skipped and the next listing photo is
   tried instead, up to ``max_images + 2`` candidates. Photo downloads bypass the
   per-host politeness budgets: ``images.craigslist.org`` / ``images.offerup.com``
   would otherwise inherit the 0.3 req/s budgets meant for the sites' search pages
   and stall a two-photo check for seconds, while a browser rendering one listing
   loads a dozen photos from those CDNs at once.
3. Content dedupe: the answer is also cached under the digest of the normalised
   image (``sha1`` of the downscaled JPEG). Marketplace CDN URLs carry rotating
   signatures (fbcdn ``oh``/``oe``), so the same photo regularly comes back under a
   new URL; it then costs one download instead of a GPU call, and the new URL is
   learnt so the next observation skips the download too. The digest is exact on
   purpose: a perceptual near-match could hand one listing the verdict of a
   different unit photographed the same way (an empty box vs. a full one).
4. One model call per image, bounded by a semaphore of ``max_concurrency`` (the
   GPU is the scarce resource), ``timeout_seconds`` per call, no retries and no
   politeness rate limit (local endpoint: fail fast, never queue behind a backoff).
5. The model answers :data:`VISION_SCHEMA` (Ollama ``format`` / OpenAI
   ``response_format``). Answers are parsed defensively: prose or markdown fences
   around the JSON, Python-literal dicts, percent/word confidences (a number wins
   over a word: ``"0.9 (high)"`` is 0.9), echoed option lists
   (``"genuine|box_only"`` is UNCERTAIN) and outputs truncated by ``num_predict``
   are all tolerated.
6. Category → :class:`VisionVerdict`, then aggregation over the images:

   * DAMAGED with confidence >= ``negative_min_confidence`` always wins (highest
     confidence first): damage seen on any photo is a property of the item;
   * any other negative verdict (PARTS_ONLY, BOX_ONLY, SCREENSHOT, RECEIPT,
     STOCK_PHOTO, UNRELATED) with enough confidence wins unless another photo of
     the same listing is GENUINE with at least the same threshold. Sellers
     routinely add a photo of the retail box, a close-up of the backplate or the
     GPU inside a prebuilt next to photos of the whole item; "this photo only
     shows the box / a part" must not veto a listing whose first photo shows the
     complete item;
   * otherwise GENUINE if any image is genuine, else UNCERTAIN (low-confidence
     negatives therefore end up UNCERTAIN, never negative).

7. Two-stage cascade: an UNCERTAIN aggregate is re-asked to ``escalation_model``
   (when configured) reusing the already prepared images. If the escalation fails
   the primary UNCERTAIN result is returned rather than an error.

``check`` never raises: backend failures (timeouts, HTTP errors, an open circuit)
become an ``ERROR`` verdict and ``on_error`` policy is applied by the pipeline. A
failure on one photo (including an unexpected bug) is recorded in
``details["errors"]`` without discarding the answers of the other photos.
Listings whose photos are all unusable get ``SKIPPED`` (the backend was healthy,
there was simply nothing to look at), except under ``vision: required`` profiles:
there an unverifiable listing is ``ERROR`` so the scorer adds its
``vision_unverified`` risk instead of treating the check as not applicable.

A :class:`CircuitBreaker` (3 consecutive failures → open for 30 s, doubling up to
5 min) turns a dead GPU box into microsecond ``ERROR`` results instead of a
``timeout_seconds`` stall per listing; an open circuit also skips the image
downloads. After the recovery timeout exactly one check runs the half-open trial;
concurrent checks keep failing fast (no download) until it has settled, and every
outcome of the trial settles the breaker - an answer closes it; a timeout, HTTP
error, malformed response, unexpected exception or cancellation re-opens it.
HTTP 503 (``OLLAMA_MAX_QUEUE`` overflow) and 429 are backpressure from a healthy
server: outcome ``busy``, never counted as a breaker failure while the circuit is
closed; a busy reply to the half-open trial proves nothing either way, so the
trial slot is released by re-opening (the breaker API has no neutral release).

Performance (RTX 3060 12 GB, Ollama, Q4 weights)
------------------------------------------------
* A VLM call is image-encoder + prefill of the visual tokens + decode of the
  answer. Qwen3-VL turns every 32x32 pixel block into one visual token (16 px
  patches merged 2x2; Qwen2.5-VL: 28x28), so a raw 12 MP phone photo is ~10k
  visual tokens and seconds of prefill alone; at the default ``resize_max_side``
  of 512 a 4:3 photo is ~200 tokens. Downscaling is the main latency lever,
  followed by a short answer: ``num_predict`` ~100-160 caps the decode tail (the
  JSON answer is ~40-80 tokens, ``reason`` is limited to 160 chars).
* With that, 2026 measurements put ``qwen3-vl:4b-instruct`` (the default model) at
  ~1.2 s per image for the JSON answer on 8-12 GB GPUs such as the 3060; larger
  7-8B VLMs cost a multiple of that, and reasoning ("thinking") builds ~5x more,
  hence ``think: false``. Sub-200 ms per image is only reachable with
  encoder-only classifiers (CLIP/SigLIP-style embeddings + a trained head), not
  with a generative VLM; this module trades that latency for zero-shot,
  category-aware judgement with a stated reason. JPEG ``draft`` mode lets Pillow
  decode large JPEGs at 1/2-1/8 scale, so preprocessing stays at a few ms, and
  the re-encoded ~30-80 KB payload is negligible over Tailscale.
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
* Cascade: the small model handles the clear cases in ~1.2 s per image; only
  UNCERTAIN aggregates (including low-confidence negatives) pay for the larger
  model. When both models do not fit in VRAM together, escalation also pays a
  model swap, so keep the escalation model for the genuinely ambiguous minority.
* Answers are cached per image URL and per image content for
  ``cache_ttl_seconds``: re-observations of the same listing (price drops, other
  workers, restarts with Redis, re-signed CDN URLs) cost no GPU time.
* ``keep_alive`` values without a unit ("-1", "3600") are sent as JSON numbers
  (seconds; negative = keep loaded forever): Ollama parses *strings* with Go's
  ``time.ParseDuration``, which rejects a bare number with HTTP 400.
* ``base_url`` may be given with the API prefix many guides show
  (``http://host:1234/v1`` for LM Studio / vLLM, ``http://host:11434/api``); the
  prefix is stripped before the endpoint paths are appended.
"""

from __future__ import annotations

import ast
import asyncio
import base64
import hashlib
import io
import ipaddress
import math
import re
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import SplitResult, urlsplit, urlunsplit

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

# Verdicts that hold for the whole item whichever photo shows them. PARTS_ONLY is not
# one: a close-up of the backplate or the GPU inside a prebuilt is a detail shot of a
# complete item, so a confident GENUINE photo of the same listing overrides it.
_ITEM_PROPERTY_VERDICTS = frozenset({VisionVerdict.DAMAGED})

# --------------------------------------------------------------------------- tuning constants

PROMPT_VERSION = "v2"  # part of the cache key: bump when the prompt changes meaningfully
MEMORY_CACHE_ENTRIES = 4096
IMAGE_FETCH_TIMEOUT_SECONDS = 5.0
IMAGE_FETCH_POLICY = BackoffPolicy(max_attempts=2, base_delay=0.15, max_delay=0.5, max_total_seconds=6.0)
BUSY_STATUSES = frozenset({429, 503})  # backpressure from a healthy server, not a backend fault


def _web_image_formats() -> tuple[str, ...]:
    """Pillow decoders a listing photo can legitimately need (MPO: multi-picture camera JPEGs)."""
    Image.init()
    return tuple(f for f in ("JPEG", "MPO", "PNG", "WEBP", "GIF", "BMP", "AVIF") if f in Image.OPEN)


WEB_IMAGE_FORMATS: tuple[str, ...] = _web_image_formats()
# Advertise AVIF only when this Pillow build can decode it (CDNs negotiate on Accept).
IMAGE_ACCEPT = ("image/avif," if "AVIF" in WEB_IMAGE_FORMATS else "") + "image/webp,image/apng,image/*,*/*;q=0.8"
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
        "genuine - the real item is clearly shown (its box or accessories next to it are fine); a close-up "
        "of one area of the item (ports, backplate, label, serial number) is also genuine\n"
        "box_only - only the retail box or packaging, the item itself is not shown\n"
        "parts_only - only a part or accessory separated from the item, not the complete item\n"
        "damaged - the item is shown broken, cracked, burnt or with missing pieces; dust and normal wear "
        "such as light scratches or scuffs are not damage\n"
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
_NUMBER = re.compile(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[-+]?\d+)?", re.I)
# Separators of an echoed option list ("genuine|box_only|parts_only", "box_only/genuine",
# "genuine or box_only"): small models sometimes copy the schema instead of choosing.
_OPTION_SEPARATORS = re.compile(r"\s*(?:[|/,;]|\bor\b|(?<=[a-z])_or_(?=[a-z]))\s*", re.I)
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


def _category_token(text: str) -> str | None:
    """One category for one free-form token, or None when it names none."""
    key = re.sub(r"[^a-z]+", "_", text.strip().lower()).strip("_")
    if not key:
        return None
    if key in CATEGORY_VERDICTS:
        return key
    alias = _CATEGORY_ALIASES.get(key)
    if alias is not None:
        return alias
    for name in VISION_CATEGORIES:
        if key.startswith(name):
            return name
    return None


def normalize_category(value: Any) -> str:
    """Map a free-form category to one of :data:`VISION_CATEGORIES` (default ``uncertain``).

    An answer naming several different categories ("genuine|box_only|parts_only")
    is an echoed option list, not a decision, and becomes ``uncertain``.
    """
    if not isinstance(value, str):
        return "uncertain"
    named = {c for c in map(_category_token, _OPTION_SEPARATORS.split(value)) if c is not None}
    if len(named) > 1:
        return "uncertain"
    if named:
        return named.pop()
    return _category_token(value) or "uncertain"


def coerce_confidence(value: Any) -> float | None:
    """0..1 confidence from numbers, numeric strings, percentages or words.

    A number in the text wins over a word next to it: ``"0.9 (high)"`` is 0.9 and
    ``"low (0.2)"`` is 0.2; words alone map through a small table.
    """
    if value is None or isinstance(value, bool):
        return None
    percent = False
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return None
        match = _NUMBER.search(text)
        if match is None:
            return _WORD_CONFIDENCE.get(re.sub(r"[^a-z]+", "_", text).strip("_"))
        try:
            number = float(match.group())
        except (ValueError, OverflowError):
            return None
        percent = text[match.end() :].lstrip().startswith("%")
    else:
        return None
    if not math.isfinite(number):
        return None
    if percent or 1.0 < number <= 100.0:
        number /= 100.0
    return min(1.0, max(0.0, number))


def _is_number(value: Any) -> bool:
    """A finite int/float that is not a bool."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


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
        """Rebuild a cached answer; None for anything this module would not have written.

        Redis is shared with other workers and versions: an entry with a field of the
        wrong type or a verdict that is not an answer category (``error``,
        ``skipped``) is ignored rather than trusted or allowed to raise.
        """
        if not isinstance(data, Mapping):
            return None
        verdict_value = data.get("verdict")
        category = data.get("category", verdict_value)
        if not (isinstance(verdict_value, str) and verdict_value in CATEGORY_VERDICTS):
            return None
        if not (isinstance(category, str) and category in CATEGORY_VERDICTS):
            return None
        confidence = data.get("confidence")
        if not _is_number(confidence) or not 0.0 <= confidence <= 1.0:
            return None
        model_ms = data.get("model_ms", 0.0)
        if model_ms is None:
            model_ms = 0.0
        if not _is_number(model_ms) or model_ms < 0:
            return None
        reason = data.get("reason", "")
        if reason is None:
            reason = ""
        item_visible = data.get("item_visible")
        damage_visible = data.get("damage_visible")
        if (
            not isinstance(reason, str)
            or not (item_visible is None or isinstance(item_visible, bool))
            or not (damage_visible is None or isinstance(damage_visible, bool))
        ):
            return None
        return cls(
            url=url,
            model=model,
            category=category,
            verdict=CATEGORY_VERDICTS[verdict_value],
            confidence=float(confidence),
            item_visible=item_visible,
            damage_visible=damage_visible,
            reason=reason[:REASON_MAX_CHARS],
            model_ms=float(model_ms),
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
    item_defects = [a for a in strong if a.verdict in _ITEM_PROPERTY_VERDICTS]
    if item_defects:
        top = max(item_defects, key=conf)
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
    digest: str = ""  # sha1 hex of the normalised JPEG: the content cache key

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
        # Restrict the decoders: listing photos are attacker-controlled bytes (see module docstring).
        with Image.open(io.BytesIO(data), formats=WEB_IMAGE_FORMATS) as img:
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
        digest=hashlib.sha1(jpeg).hexdigest(),
    )


_DEFAULT_PORTS: dict[str, int] = {"http": 80, "https": 443}


def _site(host: str) -> str:
    """Approximate registrable domain (last two labels) for Sec-Fetch-Site; IP literals are their own site."""
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        pass
    labels = [part for part in host.lower().split(".") if part]
    return ".".join(labels[-2:])


def _origin(parts: SplitResult) -> tuple[str, str, int] | None:
    """(scheme, host, effective port) of an http(s) URL, None when it has no usable origin."""
    scheme = parts.scheme.lower()
    if scheme not in _DEFAULT_PORTS or not parts.hostname:
        return None
    try:
        port = parts.port
    except ValueError:  # out-of-range / non-numeric port
        return None
    return scheme, parts.hostname, port or _DEFAULT_PORTS[scheme]


def _serialize_origin(origin: tuple[str, str, int]) -> str:
    scheme, host, port = origin
    netloc = f"[{host}]" if ":" in host else host
    if port != _DEFAULT_PORTS[scheme]:
        netloc += f":{port}"
    return f"{scheme}://{netloc}"


def image_request_headers(image_url: str, page_url: str | None) -> dict[str, str]:
    """Headers a browser sends when a listing page loads one of its photos via ``<img>``.

    Combined with ``fetch_mode="no-cors"`` (which drops the navigation-only headers),
    this mirrors a real subresource load: ``Sec-Fetch-Dest: image``, the
    (schemeful) same-origin/same-site/cross-site relation to the listing page and the
    ``Referer`` of the default ``strict-origin-when-cross-origin`` policy: the full
    page URL (without credentials or fragment) for same-origin loads, only the origin
    for cross-origin loads, nothing for an https page loading an http photo.
    """
    image_origin = _origin(urlsplit(image_url))
    page = urlsplit(page_url) if page_url else None
    page_origin = _origin(page) if page is not None else None
    if page is None or page_origin is None or image_origin is None:
        return {"Sec-Fetch-Dest": "image", "Sec-Fetch-Site": "cross-site"}
    if page_origin == image_origin:
        relation = "same-origin"
    elif page_origin[0] == image_origin[0] and _site(page_origin[1]) == _site(image_origin[1]):
        relation = "same-site"
    else:
        relation = "cross-site"
    headers = {"Sec-Fetch-Dest": "image", "Sec-Fetch-Site": relation}
    if relation == "same-origin":
        headers["Referer"] = _serialize_origin(page_origin) + urlunsplit(("", "", page.path or "/", page.query, ""))
    elif not (page_origin[0] == "https" and image_origin[0] == "http"):
        headers["Referer"] = _serialize_origin(page_origin) + "/"
    return headers


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


_INT_SECONDS = re.compile(r"[-+]?\d+")
_FLOAT_SECONDS = re.compile(r"[-+]?(?:\d+\.\d*|\.\d+|\d+(?:\.\d*)?e[-+]?\d+)", re.I)


def ollama_keep_alive(value: str | float) -> str | int | float:
    """``keep_alive`` as Ollama accepts it: unit-less numbers become JSON numbers (seconds).

    Ollama parses a *string* with Go's ``time.ParseDuration``, so ``"-1"`` or ``"3600"``
    (no unit) is an HTTP 400, while the numbers ``-1`` (keep loaded) and ``3600`` work.
    Duration strings ("30m", "-1m", "1h") are passed through unchanged.
    """
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    text = str(value).strip()
    if _INT_SECONDS.fullmatch(text):
        return int(text)
    if _FLOAT_SECONDS.fullmatch(text):
        number = float(text)
        if math.isfinite(number):
            return number
    return text


def api_base(base_url: str, backend: str) -> str:
    """``base_url`` without an API prefix copied from a guide ("/v1", Ollama's "/api").

    The endpoint paths (``/api/chat``, ``/v1/chat/completions``) are appended to the
    result. Ollama also serves its OpenAI-compatible API under ``/v1``, so both
    prefixes are stripped for it; an OpenAI-compatible server keeps a non-``/v1``
    prefix (Open WebUI serves ``/api/chat/completions``).
    """
    base = base_url.strip().rstrip("/")
    suffixes = ("/api", "/v1") if backend == "ollama" else ("/v1",)
    for suffix in suffixes:
        if base.lower().endswith(suffix):
            return base[: -len(suffix)]
    return base


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
        # True while one check owns the half-open trial: from before its download until
        # its model call settled the breaker. Concurrent checks fail fast meanwhile.
        self._trial_reserved = False
        self._keep_alive = ollama_keep_alive(self.cfg.keep_alive)
        base = api_base(self.cfg.base_url, self.cfg.backend)
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
        try:
            return await self._health()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # e.g. RuntimeError("Session is closed") during shutdown
            log.warning("vision health probe failed", extra={"url": self._models_url, "error": _short(repr(exc))})
            return False

    async def _health(self) -> bool:
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
                    {"model": model, "messages": [], "keep_alive": self._keep_alive},
                    headers=self._auth_headers(),
                    timeout=max(60.0, self.cfg.timeout_seconds),
                    retry=False,
                    rate_limit=False,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # expected probe errors and e.g. a closed session alike
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
        # Nothing to look at: not applicable, unless the profile demands a photo check, in
        # which case the listing is unverified (ERROR adds the scorer's vision_unverified risk).
        unusable = VisionVerdict.ERROR if profile.vision == "required" else VisionVerdict.SKIPPED
        urls = candidate_urls(item.image_urls)
        if not urls:
            return self._result(unusable, started, error="no images")

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
            return self._result(unusable, started, error="no usable images", details=details)

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
        """One photo's answer. Never raises (except cancellation): a bug in one photo's
        path becomes a :class:`_Failure` instead of discarding the other photos' answers."""
        try:
            return await self._answer_photo(model, profile, prompt, url, images)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("vision photo check failed", exc_info=True, extra={"url": url, "vision_model": model})
            return _Failure(f"internal: {type(exc).__name__}: {_short(exc)}")

    async def _answer_photo(
        self, model: str, profile: Profile, prompt: str, url: str, images: _ImageStore
    ) -> ImageAnswer | _Skip | _Failure:
        url_key = self._cache_key(model, profile.id, url)
        cached = await self._cache_get(url_key, url, model)
        if cached is not None:
            return cached
        state = self.breaker.state
        if state == CircuitBreaker.OPEN or (state == CircuitBreaker.HALF_OPEN and self._trial_reserved):
            # Fail fast: no download, no request while the backend is known to be down or
            # while another check's half-open trial is still deciding.
            self._m_calls.inc(model=model, outcome="circuit_open")
            return _Failure("circuit_open")
        trial = state == CircuitBreaker.HALF_OPEN
        if trial:
            self._trial_reserved = True
        try:
            image = await images.get(url)
            if isinstance(image, ImageRejected):
                log.debug("vision image skipped", extra={"url": url, "reason": image.reason, "detail": _short(image.detail)})
                return _Skip(image.reason)
            content_key = self._cache_key(model, profile.id, f"content:{image.digest}") if image.digest else None
            if content_key is not None:
                known = await self._cache_get(content_key, url, model, layer="content")
                if known is not None:
                    await self._cache_put(url_key, known)  # learn the new URL: no download next time
                    return known
            try:
                answer = await self._ask_model(model, prompt, url, image)
            except _BackendFailure as exc:
                return _Failure(str(exc))
            if answer.parse_error is None:
                await self._cache_put(url_key, answer)
                if content_key is not None:
                    await self._cache_put(content_key, answer)
            return answer
        finally:
            if trial:
                self._trial_reserved = False

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
                rate_limit=False,  # photo CDNs: never queue behind a site's politeness budget
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
                "keep_alive": self._keep_alive,
                # Reasoning builds (qwen3.5, *-thinking) think by default: 6.4 s vs 1.2 s per
                # image in 2026 benchmarks. The verdict needs no chain of thought.
                "think": False,
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

    def _busy(self, model: str, status: int, trial: bool, started: float) -> _BackendFailure:
        """HTTP 503 (``OLLAMA_MAX_QUEUE`` overflow) / 429: backpressure, not a backend fault.

        Never a breaker failure while the circuit is closed: this photo simply goes
        unverified. A busy reply to the half-open trial proves nothing either way, but
        the trial slot must be released, and :class:`CircuitBreaker` can only release
        it by settling: re-open and keep backing off rather than wedge half-open.
        """
        if trial:
            self.breaker.record_failure()
            self._sync_circuit()
        self._m_calls.inc(model=model, outcome="busy")
        self._m_model_ms.observe((time.perf_counter() - started) * 1000.0, model=model)
        log.info("vision backend busy", extra={"vision_model": model, "status": status, "half_open_trial": trial})
        return _BackendFailure(f"backend_busy (HTTP {status})")

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
            # No await between reading the state and allow(): ``trial`` is exact.
            trial = self.breaker.state == CircuitBreaker.HALF_OPEN
            if not self.breaker.allow():
                self._m_calls.inc(model=model, outcome="circuit_open")
                raise _BackendFailure("circuit_open")
            # From here on every outcome must settle the breaker (record_success or
            # record_failure); a half-open trial left unsettled wedges it for good.
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
                content, stats = self._extract_content(resp.data)
            except asyncio.CancelledError:
                if trial:
                    self.breaker.record_failure()  # never leave a half-open trial dangling
                    self._sync_circuit()
                raise
            except TimeoutError as exc:  # before OSError: TimeoutError subclasses it
                raise self._fail(model, "timeout", f"timeout after {cfg.timeout_seconds:g}s", started) from exc
            except HttpStatusError as exc:
                if exc.status in BUSY_STATUSES:
                    raise self._busy(model, exc.status, trial, started) from exc
                raise self._fail(model, "http_error", f"HTTP {exc.status}: {_short(exc.body)}", started) from exc
            except (RetryExhausted, ResponseTooLarge, aiohttp.ClientError, OSError, ValueError) as exc:
                raise self._fail(model, "transport_error", f"{type(exc).__name__}: {_short(exc)}", started) from exc
            except Exception as exc:  # e.g. RuntimeError("Session is closed"): still settles the breaker
                raise self._fail(model, "error", f"{type(exc).__name__}: {_short(exc)}", started) from exc
            if content is None:
                raise self._fail(model, "bad_response", f"unexpected response shape: {_short(repr(resp.data))}", started)
            self.breaker.record_success()
            self._sync_circuit()
        elapsed = (time.perf_counter() - started) * 1000.0
        self._m_model_ms.observe(elapsed, model=model)
        load_ms = stats.get("load_ms")
        if isinstance(load_ms, (int, float)) and load_ms >= COLD_LOAD_LOG_MS:
            self._m_cold.inc(model=model)
            log.info("vision model cold load", extra={"vision_model": model, "load_ms": load_ms, "keep_alive": self._keep_alive})

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

    async def _cache_get(self, key: str, url: str, model: str, *, layer: str | None = None) -> ImageAnswer | None:
        """Cached answer for ``key`` (memory, then Redis), rebuilt for ``url``.

        Metrics: URL lookups are reported per layer (``memory`` / ``redis``); content
        lookups (``layer="content"``) are reported once, as that single layer.
        """
        ttl = self.cfg.cache_ttl_seconds
        if ttl <= 0:
            return None
        answer = ImageAnswer.from_cache(url, model, self._memory.get(key))
        if answer is not None:
            self._m_cache.inc(layer=layer or "memory", result="hit")
            return answer
        if layer is None:
            self._m_cache.inc(layer="memory", result="miss")
        raw = await self._redis_call("get", key)
        if raw is None:
            if layer is not None:
                self._m_cache.inc(layer=layer, result="miss")
            elif self.redis is not None:
                self._m_cache.inc(layer="redis", result="miss")
            return None
        try:
            data = json_loads(raw if isinstance(raw, (bytes, bytearray)) else str(raw))
        except (ValueError, TypeError):
            data = None
        answer = ImageAnswer.from_cache(url, model, data)
        if answer is None:
            self._m_cache.inc(layer=layer or "redis", result="corrupt")
            return None
        self._m_cache.inc(layer=layer or "redis", result="hit")
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
    "WEB_IMAGE_FORMATS",
    "ImageAnswer",
    "ImageRejected",
    "PreparedImage",
    "VisionFilter",
    "aggregate_answers",
    "api_base",
    "build_prompt",
    "candidate_urls",
    "coerce_confidence",
    "extract_json_object",
    "image_request_headers",
    "interpret_answer",
    "normalize_category",
    "ollama_keep_alive",
    "prepare_image",
    "prompt_kind",
    "verdict_for",
]
