"""Tests for engine/vision_filter.py against an emulated Ollama / OpenAI-compatible backend.

One aiohttp test server plays three roles: an image CDN (Pillow-generated JPEG/PNG
photos plus broken ones), Ollama (``/api/chat``, ``/api/tags``) and an
OpenAI-compatible server (``/v1/chat/completions``, ``/v1/models``). The fake model
"looks" at the base64 image it receives: every test photo is a solid colour and the
scripted answer is picked by the colour nearest to the decoded image's mean, so the
tests also prove that the right (downscaled) image reached the right model.
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import socket
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fakeredis
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from PIL import Image
from redis.exceptions import ConnectionError as RedisConnectionError

from deal_radar.config_schema import AppConfig, Profile, VisionSection, load_config
from deal_radar.core.http import HttpClient, NetworkSettings
from deal_radar.core.metrics import Metrics
from deal_radar.core.ratelimit import CircuitBreaker
from deal_radar.engine.types import DealItem, SourceKind, VisionVerdict
from deal_radar.engine.vision_filter import (
    CATEGORY_VERDICTS,
    VISION_CATEGORIES,
    VISION_SCHEMA,
    ImageAnswer,
    ImageRejected,
    VisionFilter,
    aggregate_answers,
    build_prompt,
    candidate_urls,
    coerce_confidence,
    extract_json_object,
    image_request_headers,
    interpret_answer,
    normalize_category,
    prepare_image,
    prompt_kind,
    verdict_for,
)

CONFIG_PATH = Path(__file__).parents[1] / "config.yaml"
SMALL = "qwen2.5vl:3b"
LARGE = "qwen2.5vl:7b"

PALETTE: dict[str, tuple[int, int, int]] = {
    "red": (220, 30, 30),
    "green": (30, 190, 60),
    "blue": (30, 60, 220),
    "yellow": (235, 215, 40),
    "magenta": (200, 40, 200),
}

# --------------------------------------------------------------------------- helpers


def make_image(color: str, size: tuple[int, int] = (1600, 1200), fmt: str = "JPEG", mode: str = "RGB") -> bytes:
    fill: tuple[int, ...] = PALETTE[color] + ((255,) if mode == "RGBA" else ())
    img = Image.new(mode, size, fill)
    buf = io.BytesIO()
    img.save(buf, format=fmt)
    return buf.getvalue()


def nearest_color(b64: str) -> tuple[str, tuple[int, int], str]:
    with Image.open(io.BytesIO(base64.b64decode(b64))) as img:
        size, fmt = img.size, img.format
        mean = img.convert("RGB").resize((1, 1), Image.Resampling.BOX).getpixel((0, 0))
    name = min(PALETTE, key=lambda n: sum((a - b) ** 2 for a, b in zip(PALETTE[n], mean, strict=True)))
    return name, size, fmt or ""


def answer(category: str, confidence: Any = 0.9, *, item_visible: bool | None = None, damage_visible: bool = False,
           reason: str = "looks fine") -> dict[str, Any]:
    return {
        "item_visible": category in ("genuine", "damaged") if item_visible is None else item_visible,
        "category": category,
        "damage_visible": damage_visible,
        "confidence": confidence,
        "reason": reason,
    }


@dataclass
class Backend:
    """Mutable script + recorder for the fake image CDN and model servers."""

    images: dict[str, tuple[int, str, bytes]] = field(default_factory=dict)  # name -> (status, content type, body)
    answers: dict[tuple[str, str], Any] = field(default_factory=dict)  # (model or "*", colour) -> answer
    fail_status: int | None = None
    fail_models: dict[str, int] = field(default_factory=dict)
    delay: float = 0.0
    tags: list[str] = field(default_factory=lambda: [SMALL, LARGE])
    chat: list[dict[str, Any]] = field(default_factory=list)  # {"path","model","color","size","payload","headers"}
    image_hits: dict[str, int] = field(default_factory=dict)
    image_headers: list[dict[str, str]] = field(default_factory=list)
    inflight: int = 0
    max_inflight: int = 0

    def script(self, model: str, color: str) -> Any:
        return self.answers.get((model, color), self.answers.get(("*", color), answer("uncertain", 0.2)))


def default_images() -> dict[str, tuple[int, str, bytes]]:
    return {
        "red.jpg": (200, "image/jpeg", make_image("red")),
        "blue.jpg": (200, "image/jpeg", make_image("blue")),
        "green.jpg": (200, "image/jpeg", make_image("green", (1024, 768))),
        "yellow.png": (200, "image/png", make_image("yellow", (900, 900), fmt="PNG")),
        "magenta.webp": (200, "image/webp", make_image("magenta", (640, 480), fmt="WEBP")),
        "octet.jpg": (200, "application/octet-stream", make_image("red", (700, 500))),
        "page.html": (200, "text/html; charset=utf-8", b"<html><body>Image not found</body></html>"),
        "corrupt.jpg": (200, "image/jpeg", b"\xff\xd8\xff\xe0" + b"definitely not a jpeg" * 20),
        "big.jpg": (200, "image/jpeg", b"\xff\xd8" + bytes(range(256)) * 200),  # 51 KB > max_image_bytes in tests
        "icon.svg": (200, "image/svg+xml", b"<svg xmlns='http://www.w3.org/2000/svg'/>"),
    }


@pytest.fixture
async def backend() -> AsyncIterator[tuple[TestServer, Backend]]:
    state = Backend(images=default_images())

    async def image(request: web.Request) -> web.Response:
        name = request.match_info["name"]
        state.image_hits[name] = state.image_hits.get(name, 0) + 1
        state.image_headers.append(dict(request.headers))
        if name not in state.images:
            return web.Response(status=404, text="not found")
        status, ctype, body = state.images[name]
        return web.Response(status=status, body=body, headers={"Content-Type": ctype})

    async def _model_call(request: web.Request, payload: dict[str, Any], image_b64: str | None) -> tuple[int, Any]:
        model = payload.get("model", "")
        record: dict[str, Any] = {"path": request.path, "model": model, "payload": payload, "headers": dict(request.headers)}
        if image_b64:
            record["color"], record["size"], record["format"] = nearest_color(image_b64)
        state.chat.append(record)
        state.inflight += 1
        state.max_inflight = max(state.max_inflight, state.inflight)
        try:
            if state.delay:
                await asyncio.sleep(state.delay)
        finally:
            state.inflight -= 1
        status = state.fail_models.get(model) or state.fail_status
        if status:
            return status, {"error": "CUDA error: out of memory"}
        if not image_b64:
            return 200, ""
        scripted = state.script(model, record["color"])
        return 200, scripted if isinstance(scripted, str) else json.dumps(scripted)

    async def ollama_chat(request: web.Request) -> web.Response:
        payload = await request.json()
        images = payload["messages"][0].get("images") if payload.get("messages") else None
        status, content = await _model_call(request, payload, images[0] if images else None)
        if status != 200:
            return web.json_response(content, status=status)
        return web.json_response(
            {
                "model": payload["model"],
                "created_at": "2026-10-06T12:00:00.000Z",
                "message": {"role": "assistant", "content": content},
                "done": True,
                "done_reason": "stop" if payload.get("messages") else "load",
                "total_duration": 812_000_000,
                "load_duration": 12_000_000,
                "prompt_eval_count": 612,
                "prompt_eval_duration": 301_000_000,
                "eval_count": 41,
                "eval_duration": 420_000_000,
            }
        )

    async def ollama_tags(request: web.Request) -> web.Response:
        return web.json_response(
            {
                "models": [
                    {"name": t, "model": t, "size": 3_200_000_000, "digest": "abc", "details": {"format": "gguf"}}
                    for t in state.tags
                ]
            }
        )

    async def openai_chat(request: web.Request) -> web.Response:
        payload = await request.json()
        image_b64 = None
        for part in payload["messages"][0]["content"]:
            if part.get("type") == "image_url":
                image_b64 = part["image_url"]["url"].split(",", 1)[1]
        status, content = await _model_call(request, payload, image_b64)
        if status != 200:
            return web.json_response({"error": {"message": content["error"]}}, status=status)
        return web.json_response(
            {
                "id": "chatcmpl-1",
                "object": "chat.completion",
                "created": 1_790_000_000,
                "model": payload["model"],
                "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 640, "completion_tokens": 44, "total_tokens": 684},
            }
        )

    async def openai_models(request: web.Request) -> web.Response:
        models = [{"id": t, "object": "model", "owned_by": "local"} for t in state.tags]
        return web.json_response({"object": "list", "data": models})

    app = web.Application()
    app.router.add_get("/img/{name}", image)
    app.router.add_post("/api/chat", ollama_chat)
    app.router.add_get("/api/tags", ollama_tags)
    app.router.add_post("/v1/chat/completions", openai_chat)
    app.router.add_get("/v1/models", openai_models)
    server = TestServer(app)
    await server.start_server()
    yield server, state
    await server.close()


@pytest.fixture
async def http() -> AsyncIterator[HttpClient]:
    client = HttpClient.create(NetworkSettings(trust_env=False))
    yield client
    await client.close()


@pytest.fixture(scope="module")
def base_config() -> AppConfig:
    return load_config(CONFIG_PATH, env={})


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


def configure(base: AppConfig, server: TestServer, **vision: Any) -> AppConfig:
    settings: dict[str, Any] = {"enabled": True, "base_url": str(server.make_url("/")), "model": SMALL}
    settings.update(vision)
    return base.model_copy(update={"vision": VisionSection(**settings)})


def make_item(
    server: TestServer | None, *images: str, kind: SourceKind = SourceKind.LOCAL, source: str = "fb_marketplace"
) -> DealItem:
    urls = [str(server.make_url(f"/img/{name}")) for name in images] if server is not None else list(images)
    return DealItem(
        source=source,
        source_kind=kind,
        source_id="1234567890",
        url="https://www.facebook.com/marketplace/item/1234567890/",
        title="RTX 4090 Founders Edition - barely used",
        price=1100.0,
        total_price=1100.0,
        image_urls=urls,
    )


def gpu_profile(config: AppConfig) -> Profile:
    return config.profile("rtx_4090")


def chat_models(state: Backend) -> list[str]:
    return [c["model"] for c in state.chat if "color" in c]


# --------------------------------------------------------------------------- pure functions


def test_schema_captures_the_answer_fields() -> None:
    props = VISION_SCHEMA["properties"]
    assert VISION_SCHEMA["type"] == "object"
    assert list(props) == ["item_visible", "category", "damage_visible", "confidence", "reason"]
    assert set(VISION_SCHEMA["required"]) == set(props)
    assert VISION_SCHEMA["additionalProperties"] is False
    assert props["item_visible"]["type"] == props["damage_visible"]["type"] == "boolean"
    assert props["category"]["enum"] == list(VISION_CATEGORIES)
    assert props["confidence"] == {"type": "number", "minimum": 0, "maximum": 1}
    assert props["reason"]["maxLength"] == 160
    json.dumps(VISION_SCHEMA)  # must be JSON-serialisable for both backends
    assert CATEGORY_VERDICTS["box_only"] is VisionVerdict.BOX_ONLY
    assert all(CATEGORY_VERDICTS[c].value == c for c in VISION_CATEGORIES)


def test_prompt_is_category_aware(base_config: AppConfig) -> None:
    gpu = build_prompt(base_config.profile("rtx_4090"))
    assert "NVIDIA GeForce RTX 4090 24GB" in gpu
    assert base_config.profile("rtx_4090").vision_hint in gpu
    assert "PCB" in gpu and "shroud" in gpu and "screenshot" in gpu
    assert "JSON" in gpu and '"category"' in gpu
    monitor = build_prompt(base_config.profile("oled_4k_monitor"))
    assert "Monitor rules" in monitor and "crack" in monitor and "lines" in monitor
    tv = build_prompt(base_config.profile("lg_oled_tv"))
    assert "TV rules" in tv and "crack" in tv
    prebuilt = build_prompt(base_config.profile("prebuilt_flagship"))
    assert "empty case" in prebuilt and "Prebuilt PC rules" in prebuilt
    assert prompt_kind(base_config.profile("rtx_a6000")) == "gpu"  # workstation_gpu
    assert prompt_kind(base_config.profile("sony_a7iv")) == "camera"
    assert prompt_kind(base_config.profile("steam_deck_oled")) == "handheld"
    for profile in base_config.profiles:
        assert len(build_prompt(profile)) < 2000  # compact: prefill time matters


def test_prompt_kind_keyword_fallback() -> None:
    def profile(category: str, name: str, hint: str | None = None) -> Profile:
        return Profile(
            id="custom",
            name=name,
            category=category,
            match={"any": ["widget"]},
            price={"floor": 10, "target": 50, "ceiling": 100, "reference_new": 120},
            vision_hint=hint,
        )

    assert prompt_kind(profile("gaming_laptop", "Razer Blade 16 RTX 4090 laptop")) == "laptop"
    assert prompt_kind(profile("misc_gpu_box", "Some RTX card")) == "gpu"
    assert prompt_kind(profile("system", "Prebuilt gaming PC with RTX 5090")) == "prebuilt"
    generic = profile("misc", "Widget 3000")
    assert prompt_kind(generic) == "generic"
    assert "the misc itself" in build_prompt(generic)


@pytest.mark.parametrize(
    ("content", "expected_category"),
    [
        ('{"item_visible": true, "category": "genuine", "damage_visible": false, "confidence": 0.9, "reason": "ok"}', "genuine"),
        ('Sure! Here is my answer:\n```json\n{"category": "box_only", "confidence": 0.95}\n```\nHope it helps.', "box_only"),
        ('Answer: {"category": "genuine", "reason": "fan {left} and } ok", "confidence": 0.8} -- done', "genuine"),
        ("{'item_visible': False, 'category': 'box_only', 'confidence': '90%'}", "box_only"),
        ('{"category": "damaged", "confidence": 0.7,}', "damaged"),
        (  # truncated by num_predict
            '{"item_visible": true, "category": "damaged", "damage_visible": true, "confidence": 0.82, "reason": "crack across',
            "damaged",
        ),
        ("```\n{\"category\": \"screenshot\"}\n```", "screenshot"),
        ('{"note": "x"} then {"category": "receipt", "confidence": 0.9}', "receipt"),
    ],
)
def test_extract_json_object_tolerates_wrapping(content: str, expected_category: str) -> None:
    obj = extract_json_object(content)
    assert obj is not None
    assert interpret_answer(obj)["category"] == expected_category


def test_extract_json_object_edge_cases() -> None:
    assert extract_json_object("I cannot help with that.") is None
    assert extract_json_object("") is None
    assert extract_json_object(None) is None
    assert extract_json_object({"category": "genuine"}) == {"category": "genuine"}
    nested = extract_json_object('{"result": {"category": "parts_only", "confidence": "high"}}')
    assert nested is not None
    fields = interpret_answer(nested)
    assert fields["category"] == "parts_only" and fields["confidence"] == 0.85


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.8, 0.8),
        ("0.8", 0.8),
        ("85%", 0.85),
        (" 85 % ", 0.85),
        (85, 0.85),
        ("high", 0.85),
        ("Very High", 0.95),
        ("Low", 0.3),
        ("confidence: 0.7", 0.7),
        (1, 1.0),
        (150, 1.0),
        (-0.2, 0.0),
        (True, None),
        (None, None),
        ("n/a", None),
        (float("nan"), None),
        ([0.5], None),
    ],
)
def test_coerce_confidence(value: Any, expected: float | None) -> None:
    result = coerce_confidence(value)
    if expected is None:
        assert result is None
    else:
        assert result == pytest.approx(expected)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("genuine", "genuine"),
        ("Box Only", "box_only"),
        ("box-only", "box_only"),
        ("BROKEN", "damaged"),
        ("stock photo", "stock_photo"),
        ("screenshot of a listing", "screenshot"),
        ("Invoice", "receipt"),
        ("parts", "parts_only"),
        ("???", "uncertain"),
        (None, "uncertain"),
        (3, "uncertain"),
    ],
)
def test_normalize_category(raw: Any, expected: str) -> None:
    assert normalize_category(raw) == expected


def test_interpret_answer_defaults_and_coercion() -> None:
    fields = interpret_answer({"label": "Genuine", "visible": "yes", "damaged": "no", "explanation": "x" * 500})
    assert fields == {
        "item_visible": True,
        "category": "genuine",
        "damage_visible": False,
        "confidence": 0.5,  # missing -> neutral default
        "reason": "x" * 160,
    }


def test_verdict_for_consistency_rules() -> None:
    assert verdict_for(interpret_answer(answer("genuine", 0.9))) == (VisionVerdict.GENUINE, 0.9)
    verdict, conf = verdict_for(interpret_answer(answer("genuine", 0.9, damage_visible=True)))
    assert verdict is VisionVerdict.DAMAGED and conf == pytest.approx(0.72)
    verdict, conf = verdict_for(interpret_answer(answer("genuine", 0.9, item_visible=False)))
    assert verdict is VisionVerdict.UNCERTAIN and conf == pytest.approx(0.45)
    verdict, _ = verdict_for(interpret_answer(answer("box_only", 0.9, item_visible=True)))
    assert verdict is VisionVerdict.UNCERTAIN  # item next to its box is not a box-only listing
    assert verdict_for(interpret_answer(answer("box_only", 0.9)))[0] is VisionVerdict.BOX_ONLY
    assert verdict_for(interpret_answer(answer("parts_only", 0.8)))[0] is VisionVerdict.PARTS_ONLY
    assert verdict_for(interpret_answer(answer("stock_photo", 0.8)))[0] is VisionVerdict.STOCK_PHOTO


def _ans(verdict: VisionVerdict, confidence: float, url: str = "u") -> ImageAnswer:
    return ImageAnswer(url=url, model=SMALL, category=verdict.value, verdict=verdict, confidence=confidence)


def test_aggregate_answers_rules() -> None:
    G, B, D, S, P = (VisionVerdict.GENUINE, VisionVerdict.BOX_ONLY, VisionVerdict.DAMAGED, VisionVerdict.SCREENSHOT,
                     VisionVerdict.PARTS_ONLY)
    assert aggregate_answers([], 0.6)[:2] == (VisionVerdict.UNCERTAIN, 0.0)
    assert aggregate_answers([_ans(G, 0.8)], 0.6)[:2] == (G, 0.8)
    # defect evidence wins even next to a genuine photo
    assert aggregate_answers([_ans(G, 0.9), _ans(D, 0.7)], 0.6)[:2] == (D, 0.7)
    assert aggregate_answers([_ans(G, 0.9), _ans(P, 0.65)], 0.6)[:2] == (P, 0.65)
    # "this photo only shows the box" does not veto a confidently genuine photo of the same listing
    assert aggregate_answers([_ans(G, 0.9), _ans(B, 0.95)], 0.6)[:2] == (G, 0.9)
    assert aggregate_answers([_ans(G, 0.5), _ans(B, 0.95)], 0.6)[:2] == (B, 0.95)
    # highest-confidence negative wins
    assert aggregate_answers([_ans(B, 0.7), _ans(S, 0.9)], 0.6)[:2] == (S, 0.9)
    # low-confidence negatives are never negative
    verdict, conf, decisive = aggregate_answers([_ans(B, 0.4)], 0.6)
    assert verdict is VisionVerdict.UNCERTAIN and conf == 0.4 and decisive is not None
    assert aggregate_answers([_ans(D, 0.5), _ans(G, 0.3)], 0.6)[0] is G


def test_image_request_headers() -> None:
    fb = image_request_headers("https://scontent-lax3-1.xx.fbcdn.net/v/t45/123.jpg?oe=1", "https://www.facebook.com/marketplace/item/1/")
    assert fb == {"Sec-Fetch-Dest": "image", "Sec-Fetch-Site": "cross-site", "Referer": "https://www.facebook.com/"}
    cl = image_request_headers("https://images.craigslist.org/00a_abc_600x450.jpg", "https://sfbay.craigslist.org/sfc/sys/d/1.html")
    assert cl["Sec-Fetch-Site"] == "same-site" and cl["Referer"] == "https://sfbay.craigslist.org/"
    same = image_request_headers("https://example.com/a.jpg", "https://example.com/item/1")
    assert same["Sec-Fetch-Site"] == "same-origin"
    assert image_request_headers("https://cdn.example.com/a.jpg", None) == {"Sec-Fetch-Dest": "image", "Sec-Fetch-Site": "cross-site"}


def test_candidate_urls_dedupes_and_filters() -> None:
    urls = ["https://a/1.jpg", "https://a/1.jpg", "ftp://x/2.jpg", "data:image/png;base64,xx", " http://b/3.png ", ""]
    assert candidate_urls(urls) == ["https://a/1.jpg", "http://b/3.png"]


# --------------------------------------------------------------------------- image preparation


def test_prepare_image_downscales_large_jpeg() -> None:
    data = make_image("red", (2000, 1500))
    prepared = prepare_image(data, max_side=672, quality=85)
    assert (prepared.source_width, prepared.source_height) == (2000, 1500)
    assert (prepared.width, prepared.height) == (672, 504)
    with Image.open(io.BytesIO(base64.b64decode(prepared.b64))) as out:
        assert out.format == "JPEG" and out.size == (672, 504) and out.mode == "RGB"
    assert prepared.jpeg_bytes == len(base64.b64decode(prepared.b64))
    assert prepared.size_label == "672x504"


def test_prepare_image_flattens_transparency_and_never_upscales() -> None:
    img = Image.new("RGBA", (1200, 300), (0, 0, 0, 0))  # fully transparent -> must become white, not black
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    prepared = prepare_image(buf.getvalue(), max_side=672, quality=90)
    assert (prepared.width, prepared.height) == (672, 168)
    with Image.open(io.BytesIO(base64.b64decode(prepared.b64))) as out:
        assert min(out.convert("RGB").getpixel((10, 10))) > 240
    small = prepare_image(make_image("blue", (300, 200), fmt="PNG"), max_side=672, quality=85)
    assert (small.width, small.height) == (300, 200)
    palette = Image.new("P", (900, 600), 3)
    buf = io.BytesIO()
    palette.save(buf, format="GIF")
    gif = prepare_image(buf.getvalue(), max_side=448, quality=85)
    assert (gif.width, gif.height) == (448, 299)


def test_prepare_image_applies_exif_orientation() -> None:
    img = Image.new("RGB", (800, 400), PALETTE["green"])
    exif = Image.Exif()
    exif[0x0112] = 6  # rotate 90 degrees clockwise on display
    buf = io.BytesIO()
    img.save(buf, format="JPEG", exif=exif)
    prepared = prepare_image(buf.getvalue(), max_side=672, quality=85)
    assert (prepared.width, prepared.height) == (336, 672)


def test_prepare_image_rejects_garbage_and_tiny_images() -> None:
    with pytest.raises(ImageRejected) as err:
        prepare_image(b"<html>nope</html>", max_side=672, quality=85)
    assert err.value.reason == "decode_error"
    with pytest.raises(ImageRejected) as err:
        prepare_image(make_image("red", (10, 10)), max_side=672, quality=85)
    assert err.value.reason == "too_small"
    truncated = make_image("red", (1200, 900))[:400]
    with pytest.raises(ImageRejected) as err:
        prepare_image(truncated, max_side=672, quality=85)
    assert err.value.reason == "decode_error"


# --------------------------------------------------------------------------- applies_to


def test_applies_to_rules(base_config: AppConfig) -> None:
    http = object.__new__(HttpClient)  # applies_to never touches the network
    profile = gpu_profile(base_config)
    local = make_item(None, "https://cdn.example.com/a.jpg")
    retail = make_item(None, "https://cdn.example.com/a.jpg", kind=SourceKind.RETAIL, source="retail")
    no_images = make_item(None)

    enabled = base_config.model_copy(update={"vision": VisionSection(enabled=True)})
    vf = VisionFilter(enabled, http)  # type: ignore[arg-type]
    assert vf.applies_to(local, profile)
    assert not vf.applies_to(retail, profile)
    assert not vf.applies_to(no_images, profile)
    assert not vf.applies_to(local, profile.model_copy(update={"vision": "off"}))
    assert vf.applies_to(retail, profile.model_copy(update={"vision": "required"}))
    assert not vf.applies_to(no_images, profile.model_copy(update={"vision": "required"}))

    by_source = base_config.model_copy(update={"vision": VisionSection(enabled=True, apply_to_sources=["retail"])})
    assert VisionFilter(by_source, http).applies_to(retail, profile)  # type: ignore[arg-type]
    marketplace = make_item(None, "https://i.ebayimg.com/x.jpg", kind=SourceKind.MARKETPLACE, source="ebay")
    by_kind = base_config.model_copy(
        update={"vision": VisionSection(enabled=True, apply_to_kinds=[SourceKind.LOCAL, SourceKind.MARKETPLACE])}
    )
    assert VisionFilter(by_kind, http).applies_to(marketplace, profile)  # type: ignore[arg-type]

    disabled = base_config.model_copy(update={"vision": VisionSection(enabled=False)})
    vf_off = VisionFilter(disabled, http)  # type: ignore[arg-type]
    assert not vf_off.applies_to(local, profile)
    assert not vf_off.applies_to(retail, profile.model_copy(update={"vision": "required"}))


# --------------------------------------------------------------------------- Ollama backend


async def test_ollama_genuine_and_request_shape(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.92, reason="card with three fans visible")
    metrics = Metrics()
    config = configure(base_config, server)
    vf = VisionFilter(config, http, metrics=metrics)
    profile = gpu_profile(config)

    result = await vf.check(make_item(server, "red.jpg"), profile)

    assert result.verdict is VisionVerdict.GENUINE
    assert result.confidence == pytest.approx(0.92)
    assert result.model == SMALL and result.images_checked == 1 and result.cached is False
    assert result.error is None and result.latency_ms > 0
    assert result.details["reason"] == "card with three fans visible"
    assert result.details["answers"][0]["image"] == "672x504"

    assert len(state.chat) == 1
    call = state.chat[0]
    assert call["path"] == "/api/chat"
    payload = call["payload"]
    assert payload["model"] == SMALL
    assert payload["stream"] is False
    assert payload["format"] == VISION_SCHEMA
    assert payload["keep_alive"] == "30m"
    assert payload["options"] == {"temperature": 0, "num_predict": 160}
    message = payload["messages"][0]
    assert message["role"] == "user"
    assert profile.name in message["content"] and "PCB" in message["content"]
    assert len(message["images"]) == 1 and not message["images"][0].startswith("data:")
    # the model received the downscaled re-encoded JPEG, not the 1600x1200 original
    assert call["color"] == "red" and call["size"] == (672, 504) and call["format"] == "JPEG"
    # image fetched once, looking like an <img> load from the listing page
    assert state.image_hits == {"red.jpg": 1}
    sent = state.image_headers[0]
    assert "Mozilla/5.0" in sent["User-Agent"]
    assert "image/" in sent["Accept"]
    if "Sec-Fetch-Dest" in sent:  # Chromium / Firefox / Safari identities all send Fetch Metadata
        assert sent["Sec-Fetch-Dest"] == "image" and sent["Sec-Fetch-Mode"] == "no-cors"
        assert sent["Sec-Fetch-Site"] == "cross-site"
    assert sent["Referer"] == "https://www.facebook.com/"
    assert "Upgrade-Insecure-Requests" not in sent and "Sec-Fetch-User" not in sent

    assert metrics.counter("vision_checks_total", labelnames=("verdict",)).value(verdict="genuine") == 1
    assert metrics.counter("vision_model_calls_total", labelnames=("model", "outcome")).value(model=SMALL, outcome="ok") == 1
    VisionResultJson = result.model_dump(mode="json")  # details must be JSON-friendly for alerts / DB
    json.dumps(VisionResultJson)


async def test_box_only_with_high_confidence_is_negative(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "blue")] = answer("box_only", 0.95, reason="only the retail box is shown")
    vf = VisionFilter(configure(base_config, server), http)
    result = await vf.check(make_item(server, "blue.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.BOX_ONLY
    assert result.is_negative and result.confidence == pytest.approx(0.95)
    assert result.details["reason"] == "only the retail box is shown"


async def test_negative_with_low_confidence_is_not_negative(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "blue")] = answer("box_only", 0.4)
    vf = VisionFilter(configure(base_config, server), http)
    result = await vf.check(make_item(server, "blue.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.UNCERTAIN
    assert not result.is_negative
    assert result.details["escalated"] is False  # no escalation model configured


async def test_second_image_damaged_wins(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.9)
    state.answers[("*", "yellow")] = answer("damaged", 0.88, damage_visible=True, reason="melted 12VHPWR connector")
    vf = VisionFilter(configure(base_config, server), http)
    result = await vf.check(make_item(server, "red.jpg", "yellow.png", "blue.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.DAMAGED
    assert result.confidence == pytest.approx(0.88)
    assert result.images_checked == 2  # max_images = 2: the third photo is not inspected
    assert sorted(c["color"] for c in state.chat) == ["red", "yellow"]
    assert "blue.jpg" not in state.image_hits
    assert result.details["reason"] == "melted 12VHPWR connector"
    assert {a["verdict"] for a in result.details["answers"]} == {"genuine", "damaged"}


async def test_box_photo_next_to_genuine_photo_stays_genuine(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.9)
    state.answers[("*", "blue")] = answer("box_only", 0.95)
    vf = VisionFilter(configure(base_config, server), http)
    result = await vf.check(make_item(server, "red.jpg", "blue.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.GENUINE and result.images_checked == 2


async def test_uncertain_escalates_to_larger_model(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[(SMALL, "green")] = answer("uncertain", 0.3, item_visible=False)
    state.answers[(LARGE, "green")] = answer("genuine", 0.91, reason="blower-style card, PCB visible")
    metrics = Metrics()
    vf = VisionFilter(configure(base_config, server, escalation_model=LARGE), http, metrics=metrics)
    result = await vf.check(make_item(server, "green.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.GENUINE
    assert result.model == LARGE
    assert result.confidence == pytest.approx(0.91)
    assert result.details["escalated"] is True
    assert result.details["primary_verdict"] == "uncertain"
    assert chat_models(state) == [SMALL, LARGE]
    assert state.image_hits == {"green.jpg": 1}  # the prepared image is reused for the escalation
    assert [a["model"] for a in result.details["answers"]] == [SMALL, LARGE]
    assert metrics.counter("vision_escalations_total", labelnames=("outcome",)).value(outcome="answered") == 1


async def test_escalation_failure_keeps_primary_uncertain(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[(SMALL, "green")] = answer("uncertain", 0.3)
    state.fail_models[LARGE] = 500
    vf = VisionFilter(configure(base_config, server, escalation_model=LARGE), http)
    result = await vf.check(make_item(server, "green.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.UNCERTAIN
    assert result.model == SMALL and result.error is None
    assert "500" in result.details["escalation_error"]
    assert result.details["escalated"] is False


async def test_genuine_is_not_escalated(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.8)
    vf = VisionFilter(configure(base_config, server, escalation_model=LARGE), http)
    result = await vf.check(make_item(server, "red.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.GENUINE and chat_models(state) == [SMALL]


async def test_fenced_json_and_percent_confidence(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = (
        "Sure! Here is the analysis:\n```json\n"
        '{"item_visible": true, "category": "Genuine", "damage_visible": false, "confidence": "85%", '
        '"reason": "card with fans and backplate"}\n```\nLet me know if you need more.'
    )
    vf = VisionFilter(configure(base_config, server), http)
    result = await vf.check(make_item(server, "red.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.GENUINE
    assert result.confidence == pytest.approx(0.85)
    assert result.details["reason"] == "card with fans and backplate"


async def test_unparseable_answer_is_uncertain_not_a_backend_failure(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = "I'm sorry, I can't determine that from the image."
    vf = VisionFilter(configure(base_config, server), http)
    result = await vf.check(make_item(server, "red.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.UNCERTAIN and result.error is None
    assert result.details["answers"][0]["parse_error"] == "unparseable answer"
    assert vf.breaker.consecutive_failures == 0
    # unparseable answers are not cached: the next check asks again
    await vf.check(make_item(server, "red.jpg"), gpu_profile(base_config))
    assert len(state.chat) == 2


async def test_backend_500_gives_error_verdict(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.fail_status = 500
    metrics = Metrics()
    vf = VisionFilter(configure(base_config, server), http, metrics=metrics)
    result = await vf.check(make_item(server, "red.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.ERROR
    assert result.error is not None and "500" in result.error
    assert result.confidence == 0.0 and result.images_checked == 0
    assert len(state.chat) == 1  # retry=False: fail fast on the local endpoint
    calls = metrics.counter("vision_model_calls_total", labelnames=("model", "outcome"))
    assert calls.value(model=SMALL, outcome="http_error") == 1
    assert metrics.counter("vision_checks_total", labelnames=("verdict",)).value(verdict="error") == 1


async def test_timeout_gives_error_verdict(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.delay = 0.6
    state.answers[("*", "red")] = answer("genuine", 0.9)
    vf = VisionFilter(configure(base_config, server, timeout_seconds=0.15), http)
    result = await vf.check(make_item(server, "red.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.ERROR
    assert result.error is not None and "timeout" in result.error
    assert result.latency_ms < 550


async def test_circuit_opens_and_fails_fast_then_recovers(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.fail_status = 503
    clock = FakeClock()
    metrics = Metrics()
    vf = VisionFilter(configure(base_config, server), http, metrics=metrics, clock=clock)
    profile = gpu_profile(base_config)
    item = make_item(server, "red.jpg")

    for _ in range(3):  # BREAKER_FAILURES consecutive failures open the circuit
        assert (await vf.check(item, profile)).verdict is VisionVerdict.ERROR
    assert len(state.chat) == 3
    assert vf.breaker.state == CircuitBreaker.OPEN
    assert metrics.gauge("vision_circuit_open").value() == 1.0
    hits_before = dict(state.image_hits)

    fast = await vf.check(item, profile)
    assert fast.verdict is VisionVerdict.ERROR and fast.error == "circuit_open"
    assert len(state.chat) == 3  # backend not called
    assert state.image_hits == hits_before  # nor the image CDN
    assert fast.latency_ms < 50

    # after the recovery timeout one trial call is allowed; success closes the circuit
    state.fail_status = None
    state.answers[("*", "red")] = answer("genuine", 0.9)
    clock.now += 31
    recovered = await vf.check(item, profile)
    assert recovered.verdict is VisionVerdict.GENUINE
    assert vf.breaker.state == CircuitBreaker.CLOSED
    assert metrics.gauge("vision_circuit_open").value() == 0.0


async def test_cancelled_half_open_trial_does_not_wedge_the_breaker(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    clock = FakeClock()
    breaker = CircuitBreaker(1, 10.0, clock=clock)
    vf = VisionFilter(configure(base_config, server), http, breaker=breaker, clock=clock)
    profile = gpu_profile(base_config)
    state.fail_status = 500
    await vf.check(make_item(server, "red.jpg"), profile)
    assert breaker.state == CircuitBreaker.OPEN
    clock.now += 11
    assert breaker.state == CircuitBreaker.HALF_OPEN

    state.fail_status = None
    state.delay = 5.0
    task = asyncio.create_task(vf.check(make_item(server, "red.jpg"), profile))
    for _ in range(200):
        if len(state.chat) == 2:
            break
        await asyncio.sleep(0.01)
    assert len(state.chat) == 2  # the half-open trial is in flight
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert breaker.state == CircuitBreaker.OPEN  # re-opened, not stuck half-open with a dangling trial
    clock.now += 100
    state.delay = 0.0
    state.answers[("*", "red")] = answer("genuine", 0.9)
    assert (await vf.check(make_item(server, "red.jpg"), profile)).verdict is VisionVerdict.GENUINE


async def test_memory_cache_hit_skips_download_and_backend(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.9)
    metrics = Metrics()
    vf = VisionFilter(configure(base_config, server), http, metrics=metrics)
    item = make_item(server, "red.jpg")
    first = await vf.check(item, gpu_profile(base_config))
    second = await vf.check(item, gpu_profile(base_config))
    assert first.cached is False and second.cached is True
    assert second.verdict is VisionVerdict.GENUINE and second.confidence == pytest.approx(0.9)
    assert second.images_checked == 1
    assert len(state.chat) == 1 and state.image_hits == {"red.jpg": 1}
    assert metrics.counter("vision_cache_total", labelnames=("layer", "result")).value(layer="memory", result="hit") == 1
    # a different profile uses a different prompt and therefore a different cache entry
    await vf.check(item, base_config.profile("rtx_5090"))
    assert len(state.chat) == 2


async def test_cache_disabled_with_zero_ttl(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.9)
    vf = VisionFilter(configure(base_config, server, cache_ttl_seconds=0), http)
    item = make_item(server, "red.jpg")
    await vf.check(item, gpu_profile(base_config))
    assert (await vf.check(item, gpu_profile(base_config))).cached is False
    assert len(state.chat) == 2


async def test_cache_expires_after_ttl(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.9)
    clock = FakeClock()
    vf = VisionFilter(configure(base_config, server, cache_ttl_seconds=60), http, clock=clock)
    item = make_item(server, "red.jpg")
    await vf.check(item, gpu_profile(base_config))
    clock.now += 61
    assert (await vf.check(item, gpu_profile(base_config))).cached is False
    assert len(state.chat) == 2


async def test_escalated_answers_are_cached_too(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[(SMALL, "green")] = answer("uncertain", 0.2)
    state.answers[(LARGE, "green")] = answer("damaged", 0.8, damage_visible=True)
    vf = VisionFilter(configure(base_config, server, escalation_model=LARGE), http)
    item = make_item(server, "green.jpg")
    first = await vf.check(item, gpu_profile(base_config))
    second = await vf.check(item, gpu_profile(base_config))
    assert first.verdict is second.verdict is VisionVerdict.DAMAGED
    assert second.cached is True and second.model == LARGE
    assert len(state.chat) == 2


async def test_redis_cache_is_shared_between_instances(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "blue")] = answer("box_only", 0.93)
    redis = fakeredis.FakeAsyncRedis()
    config = configure(base_config, server)
    item = make_item(server, "blue.jpg")
    first = await VisionFilter(config, http, redis=redis).check(item, gpu_profile(config))
    keys = await redis.keys("dr:vision:*")
    assert len(keys) == 1
    ttl = await redis.ttl(keys[0])
    assert 0 < ttl <= 86_400

    other_node = VisionFilter(config, http, redis=redis)  # fresh in-memory cache
    second = await other_node.check(item, gpu_profile(config))
    assert first.verdict is second.verdict is VisionVerdict.BOX_ONLY
    assert second.cached is True and second.confidence == pytest.approx(0.93)
    assert len(state.chat) == 1
    await redis.aclose()


class BrokenRedis:
    def __init__(self) -> None:
        self.calls = 0

    async def get(self, key: str) -> bytes | None:
        self.calls += 1
        raise RedisConnectionError("connection refused")

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.calls += 1
        raise RedisConnectionError("connection refused")


async def test_redis_failure_is_bypassed(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.9)
    broken = BrokenRedis()
    metrics = Metrics()
    vf = VisionFilter(configure(base_config, server), http, metrics=metrics, redis=broken)
    result = await vf.check(make_item(server, "red.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.GENUINE
    assert broken.calls == 1  # GET failed -> redis bypassed for a while (no SET attempt)
    await vf.check(make_item(server, "red.jpg"), gpu_profile(base_config.model_copy()))
    assert broken.calls == 1
    assert metrics.counter("vision_cache_total", labelnames=("layer", "result")).value(layer="redis", result="error") == 1


async def test_corrupt_cache_entry_is_ignored(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.9)
    redis = fakeredis.FakeAsyncRedis()
    config = configure(base_config, server)
    vf = VisionFilter(config, http, redis=redis)
    await vf.check(make_item(server, "red.jpg"), gpu_profile(config))
    for key in await redis.keys("dr:vision:*"):
        await redis.set(key, b"{not json")
    fresh = VisionFilter(config, http, redis=redis)
    result = await fresh.check(make_item(server, "red.jpg"), gpu_profile(config))
    assert result.verdict is VisionVerdict.GENUINE and result.cached is False
    assert len(state.chat) == 2
    await redis.aclose()


# --------------------------------------------------------------------------- unusable images


async def test_unusable_images_are_skipped_without_model_calls(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    metrics = Metrics()
    vf = VisionFilter(configure(base_config, server, max_image_bytes=10_000), http, metrics=metrics)
    item = make_item(server, "page.html", "corrupt.jpg", "missing.jpg", "big.jpg")
    result = await vf.check(item, gpu_profile(base_config))
    assert result.verdict is VisionVerdict.SKIPPED
    assert result.error == "no usable images"
    assert state.chat == []
    reasons = {entry["reason"] for entry in result.details["skipped"]}
    assert reasons == {"not_image", "decode_error", "http_404", "too_large"}
    images = metrics.counter("vision_images_total", labelnames=("outcome",))
    assert images.value(outcome="not_image") == 1 and images.value(outcome="too_large") == 1


async def test_svg_is_not_a_photo(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, _state = backend
    vf = VisionFilter(configure(base_config, server), http)
    result = await vf.check(make_item(server, "icon.svg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.SKIPPED
    assert result.details["skipped"] == [{"url": str(server.make_url("/img/icon.svg")), "reason": "not_image"}]


async def test_falls_back_to_next_photo_when_one_is_unusable(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.9)
    state.answers[("*", "magenta")] = answer("genuine", 0.7)
    vf = VisionFilter(configure(base_config, server, max_images=1), http)
    result = await vf.check(make_item(server, "page.html", "octet.jpg", "magenta.webp"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.GENUINE
    assert result.images_checked == 1 and result.confidence == pytest.approx(0.9)
    # octet-stream is accepted when the bytes decode; the webp was never needed
    assert [c["color"] for c in state.chat] == ["red"]
    assert "magenta.webp" not in state.image_hits
    assert result.details["skipped"][0]["reason"] == "not_image"


async def test_webp_and_png_are_converted_to_jpeg(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "magenta")] = answer("genuine", 0.9)
    state.answers[("*", "yellow")] = answer("genuine", 0.9)
    vf = VisionFilter(configure(base_config, server, resize_max_side=448), http)
    result = await vf.check(make_item(server, "magenta.webp", "yellow.png"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.GENUINE and result.images_checked == 2
    sizes = {c["color"]: (c["size"], c["format"]) for c in state.chat}
    assert sizes == {"magenta": ((448, 336), "JPEG"), "yellow": ((448, 448), "JPEG")}


# --------------------------------------------------------------------------- OpenAI-compatible backend


async def test_openai_backend_request_shape(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "red")] = answer("genuine", 0.88)
    config = configure(base_config, server, backend="openai", api_key="sk-local-test", num_predict=128)
    vf = VisionFilter(config, http)
    result = await vf.check(make_item(server, "red.jpg"), gpu_profile(config))
    assert result.verdict is VisionVerdict.GENUINE and result.confidence == pytest.approx(0.88)

    call = state.chat[0]
    assert call["path"] == "/v1/chat/completions"
    assert call["headers"]["Authorization"] == "Bearer sk-local-test"
    payload = call["payload"]
    assert payload["model"] == SMALL
    assert payload["temperature"] == 0 and payload["max_tokens"] == 128 and payload["stream"] is False
    assert payload["response_format"] == {
        "type": "json_schema",
        "json_schema": {"name": "listing_photo_check", "schema": VISION_SCHEMA},
    }
    parts = payload["messages"][0]["content"]
    assert payload["messages"][0]["role"] == "user"
    assert parts[0]["type"] == "text" and gpu_profile(config).name in parts[0]["text"]
    assert parts[1]["type"] == "image_url"
    assert parts[1]["image_url"]["url"].startswith("data:image/jpeg;base64,")
    assert call["size"] == (672, 504)


async def test_openai_backend_without_api_key_sends_no_auth(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.answers[("*", "blue")] = answer("stock_photo", 0.7)
    vf = VisionFilter(configure(base_config, server, backend="openai"), http)
    result = await vf.check(make_item(server, "blue.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.STOCK_PHOTO
    assert "Authorization" not in state.chat[0]["headers"]


# --------------------------------------------------------------------------- concurrency, health, lifecycle


async def test_model_calls_are_bounded_by_max_concurrency(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    state.delay = 0.1
    for color in PALETTE:
        state.answers[("*", color)] = answer("genuine", 0.9)
    vf = VisionFilter(configure(base_config, server, max_concurrency=1, max_images=2), http)
    profile = gpu_profile(base_config)
    items = [make_item(server, "red.jpg", "blue.jpg"), make_item(server, "green.jpg", "yellow.png")]
    results = await asyncio.gather(*(vf.check(i, profile) for i in items))
    assert all(r.verdict is VisionVerdict.GENUINE for r in results)
    assert len(state.chat) == 4 and state.max_inflight == 1

    state.chat.clear()
    state.max_inflight = 0
    vf2 = VisionFilter(configure(base_config, server, max_concurrency=2, max_images=2, cache_ttl_seconds=0), http)
    await vf2.check(make_item(server, "red.jpg", "blue.jpg"), profile)
    assert state.max_inflight == 2


async def test_health_ollama(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    assert await VisionFilter(configure(base_config, server), http).health() is True
    assert await VisionFilter(configure(base_config, server, model="qwen2.5vl"), http).health() is False
    state.tags = ["qwen2.5vl:latest"]
    assert await VisionFilter(configure(base_config, server, model="qwen2.5vl"), http).health() is True
    # missing escalation model only warns
    state.tags = [SMALL]
    assert await VisionFilter(configure(base_config, server, escalation_model=LARGE), http).health() is True


async def test_health_openai(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    assert await VisionFilter(configure(base_config, server, backend="openai"), http).health() is True
    state.tags = ["some-other-model"]  # llama.cpp style single-model servers ignore the name
    assert await VisionFilter(configure(base_config, server, backend="openai"), http).health() is True


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def test_unreachable_backend(http: HttpClient, base_config: AppConfig, backend) -> None:
    server, _state = backend
    dead = f"http://127.0.0.1:{_free_port()}"
    config = base_config.model_copy(update={"vision": VisionSection(enabled=True, base_url=dead, model=SMALL)})
    vf = VisionFilter(config, http)
    assert await vf.health() is False
    assert await vf.warmup() is False
    result = await vf.check(make_item(server, "red.jpg"), gpu_profile(config))
    assert result.verdict is VisionVerdict.ERROR and result.error
    assert vf.breaker.consecutive_failures == 1


async def test_warmup_preloads_models(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    vf = VisionFilter(configure(base_config, server, escalation_model=LARGE, keep_alive="1h"), http)
    assert await vf.warmup() is True
    assert [c["model"] for c in state.chat] == [SMALL, LARGE]
    assert all(c["payload"]["messages"] == [] and c["payload"]["keep_alive"] == "1h" for c in state.chat)
    openai = VisionFilter(configure(base_config, server, backend="openai"), http)
    assert await openai.warmup() is True  # falls back to a health probe


async def test_skipped_when_disabled_without_images_or_profile_off(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, state = backend
    profile = gpu_profile(base_config)
    disabled = VisionFilter(configure(base_config, server, enabled=False), http)
    assert (await disabled.check(make_item(server, "red.jpg"), profile)).verdict is VisionVerdict.SKIPPED
    vf = VisionFilter(configure(base_config, server), http)
    assert (await vf.check(make_item(server), profile)).verdict is VisionVerdict.SKIPPED
    off = profile.model_copy(update={"vision": "off"})
    assert (await vf.check(make_item(server, "red.jpg"), off)).verdict is VisionVerdict.SKIPPED
    assert state.chat == [] and state.image_hits == {}


async def test_close_and_internal_errors_never_raise(backend, http: HttpClient, base_config: AppConfig, monkeypatch) -> None:
    server, state = backend
    vf = VisionFilter(configure(base_config, server), http)

    async def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("unexpected bug")

    monkeypatch.setattr(vf, "_run_pass", boom)
    result = await vf.check(make_item(server, "red.jpg"), gpu_profile(base_config))
    assert result.verdict is VisionVerdict.ERROR and "unexpected bug" in (result.error or "")

    other = VisionFilter(configure(base_config, server), http)
    await other.close()
    closed = await other.check(make_item(server, "red.jpg"), gpu_profile(base_config))
    assert closed.verdict is VisionVerdict.ERROR and closed.error == "vision filter closed"
    assert state.chat == []


async def test_malformed_backend_response_counts_as_failure(backend, http: HttpClient, base_config: AppConfig) -> None:
    server, _state = backend
    config = configure(base_config, server, backend="openai")
    vf = VisionFilter(config, http)

    async def fake_post_json(url: str, payload: Any, **kwargs: Any) -> Any:
        from deal_radar.core.http import HttpResponse

        return HttpResponse(200, url, {}, {"object": "error", "message": "model crashed"})

    vf.http = type("H", (), {"post_json": staticmethod(fake_post_json), "get_bytes": http.get_bytes})()  # type: ignore[assignment]
    result = await vf.check(make_item(server, "red.jpg"), gpu_profile(config))
    assert result.verdict is VisionVerdict.ERROR and "unexpected response shape" in (result.error or "")
    assert vf.breaker.consecutive_failures == 1
