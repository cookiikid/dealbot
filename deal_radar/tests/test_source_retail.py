"""Tests for the retail endpoint ingestor (sources/retail_endpoints.py).

All HTTP is offline. ``aioresponses`` patches ``aiohttp.ClientSession``, and the shared
``HttpClient`` (retries, conditional cache, cache-bust, identities) runs unmodified on
top of it. The fixtures under ``fixtures/retail/`` mirror real upstream payloads:

* ``newegg_productrealtime_14-137-866.json`` is a ProductRealtime response recorded
  verbatim from www.newegg.com on 2026-10-06.
* The Shopify ``.js`` and ``products.json`` fixtures copy the key sets and value types
  of live storefront responses (Ajax API cents vs products.json decimal strings).
* The Best Buy and RedSky fixtures follow the documented and researched response
  shapes, including the HUMAN/PerimeterX block body Target returns with HTTP 435.
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import logging
import random
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import aioresponses.core as aioresponses_core
import pytest
from aiohttp import ClientResponse
from aioresponses import CallbackResult, aioresponses
from pydantic import SecretStr
from yarl import URL

from deal_radar.config_schema import (
    AppConfig,
    BestBuyEndpoint,
    GenericJsonEndpoint,
    NeweggEndpoint,
    ShopifyEndpoint,
    TargetEndpoint,
    load_config,
)
from deal_radar.core.backoff import BackoffPolicy
from deal_radar.core.http import HttpClient, NetworkSettings
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import RawListing, SourceKind
from deal_radar.sources import retail_endpoints as retail
from deal_radar.sources.base import IngestorContext, SourceAuthError, SourceBlocked, SourceError
from deal_radar.sources.registry import load_ingestor_class
from deal_radar.sources.retail_endpoints import (
    BESTBUY_SHOW_FIELDS,
    NEWEGG_MIN_INTERVAL_SECONDS,
    BestBuyAdapter,
    GenericJsonAdapter,
    NeweggAdapter,
    PayloadError,
    RetailEndpointError,
    RetailIngestor,
    ShopifyAdapter,
    TargetAdapter,
    bestbuy_products_url,
    compile_path,
    condition_word,
    is_bot_wall_json,
    newegg_product_url,
    normalize_store_url,
    parse_bestbuy_products,
    parse_generic_items,
    parse_newegg_realtime,
    parse_redsky_summaries,
    parse_shopify_product_js,
    parse_shopify_products_json,
    parse_timestamp,
    redact,
    redsky_in_stock,
    resolve_path,
    to_bool,
    to_float,
)

FIXTURES = Path(__file__).parent / "fixtures" / "retail"
CONFIG_PATH = Path(__file__).parents[1] / "config.yaml"
BB_KEY = "BBYk3yS3cr3tValue42"
TARGET_KEY = "9f36aeafbe60771e321a7cc95a78140772ab3e96"
STORE = "https://gpu-shop.example.com"
BB_RE = re.compile(r"^https://api\.bestbuy\.com/v1/products\(sku%20in\([0-9,]+\)\)\?.*$")
REDSKY_RE = re.compile(r"^https://redsky\.target\.com/redsky_aggregations/v1/web/product_summary_with_fulfillment_v1\?.*$")
NEWEGG_RE = re.compile(r"^https://www\.newegg\.com/product/api/ProductRealtime\?.*$")
CF_CHALLENGE_HTML = (
    "<!DOCTYPE html><html lang=\"en-US\"><head><title>Just a moment...</title></head>"
    "<body><div>Verifying your connection...</div></body></html>"
)


# --------------------------------------------------------------------------- helpers


class _CompatClientResponse(ClientResponse):
    """aioresponses 0.7.9 predates aiohttp 3.14's required ``stream_writer`` argument."""

    def __init__(self, method: str, url: URL, **kwargs: Any) -> None:
        if _NEEDS_STREAM_WRITER:
            kwargs.setdefault("stream_writer", SimpleNamespace(output_size=0))
        super().__init__(method, url, **kwargs)


_NEEDS_STREAM_WRITER = "stream_writer" in inspect.signature(ClientResponse.__init__).parameters


@pytest.fixture(autouse=True)
def _aioresponses_compat(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(aioresponses_core, "ClientResponse", _CompatClientResponse)


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def make_http() -> HttpClient:
    return HttpClient.create(
        NetworkSettings(trust_env=False, retry=BackoffPolicy(max_attempts=2, base_delay=0, max_delay=0))
    )


@pytest.fixture
async def http():
    client = make_http()
    yield client
    await client.close()


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t


def bb_endpoint(**kw: Any) -> dict[str, Any]:
    base = {"name": "bby_gpus", "adapter": "bestbuy", "api_key": BB_KEY, "skus": ["6614151"], "profile_hint": "rtx_5090"}
    base.update(kw)
    return base


def shop_endpoint(**kw: Any) -> dict[str, Any]:
    base = {
        "name": "gpu_shop",
        "adapter": "shopify",
        "retailer": "GPU Shop",
        "store_url": STORE + "/",
        "handles": ["nvidia-geforce-rtx-5090-founders-edition"],
    }
    base.update(kw)
    return base


def make_config(endpoints: list[dict[str, Any]], **retail_kw: Any) -> AppConfig:
    section = {"enabled": True, "jitter_pct": 0.0, "poll_interval_seconds": 20, "endpoints": endpoints}
    section.update(retail_kw)
    return AppConfig.model_validate({"sources": {"retail": section}})


def make_ingestor(
    config: AppConfig,
    http: HttpClient,
    *,
    clock: FakeClock | None = None,
    notices: list[tuple[str, str]] | None = None,
    metrics: Metrics | None = None,
) -> RetailIngestor:
    async def notify(title: str, message: str) -> None:
        if notices is not None:
            notices.append((title, message))

    ctx = IngestorContext(
        http=http,
        metrics=metrics or Metrics(),
        config=config,
        node_id="test",
        notify=notify,
        rng=random.Random(7),
    )
    return RetailIngestor(config.sources.retail, ctx, clock=clock or FakeClock())


def requests_to(m: aioresponses, pattern: re.Pattern[str] | str) -> list[tuple[URL, Any]]:
    """All recorded (url, call) pairs whose URL matches ``pattern`` (in request order per URL)."""
    out = []
    for (_method, url), calls in m.requests.items():
        if (pattern.match(str(url)) if isinstance(pattern, re.Pattern) else str(url).startswith(pattern)):
            out.extend((url, call) for call in calls)
    return out


def shopify_product(pid: int, title: str, price: str) -> dict[str, Any]:
    return {
        "id": pid,
        "title": title,
        "handle": f"product-{pid}",
        "updated_at": "2026-10-05T22:11:20-07:00",
        "vendor": "Vendor",
        "product_type": "Graphics Cards",
        "variants": [{"id": pid * 10, "title": "Default Title", "price": price, "available": True, "compare_at_price": None}],
        "images": [],
    }


# =========================================================================== value helpers


def test_to_float_handles_numbers_strings_and_garbage() -> None:
    assert to_float(799.99) == 799.99
    assert to_float(2500, 100) == 25.0
    assert to_float("25.00") == 25.0
    assert to_float("$1,299.99") == 1299.99
    assert to_float("209999", 100) == 2099.99
    for bad in (None, True, "", "  ", "abc", "1.299,99 €", float("nan"), float("inf"), -5, [1]):
        assert to_float(bad) is None, bad


def test_to_bool_understands_availability_words() -> None:
    assert to_bool(True) is True and to_bool(False) is False
    assert to_bool(3) is True and to_bool(0) is False
    for word in ("in_stock", "In Stock", "available", "TRUE", "yes", "1", "LIMITED_STOCK"):
        assert to_bool(word) is True, word
    for word in ("out_of_stock", "Sold Out", "unavailable", "false", "0", "discontinued"):
        assert to_bool(word) is False, word
    assert to_bool("maybe") is None and to_bool(None) is None


def test_parse_timestamp_requires_an_offset() -> None:
    assert parse_timestamp("2026-10-05T09:15:00-05:00") == datetime(2026, 10, 5, 14, 15, tzinfo=UTC)
    assert parse_timestamp("2026-10-05T14:15:00Z") == datetime(2026, 10, 5, 14, 15, tzinfo=UTC)
    assert parse_timestamp("2026-10-05T14:15:00") is None  # naive: zone unknown, never guessed
    assert parse_timestamp("not a date") is None and parse_timestamp(None) is None


def test_condition_word_maps_retailer_vocabularies() -> None:
    assert condition_word("New") == "New"
    assert condition_word("Refurbished") == "Refurbished"
    assert condition_word("pre-owned") == "Used"
    assert condition_word("Open-Box Excellent") == "Open Box"
    assert condition_word("certified") == "Refurbished"
    assert condition_word("") is None and condition_word(None) is None


def test_redact_masks_known_secrets_and_key_query_params() -> None:
    text = (
        f"HTTP 400 for https://api.bestbuy.com/v1/products(sku%20in(1))?apiKey={BB_KEY}&format=json "
        f"echo={BB_KEY} other https://redsky.target.com/x?key=abcdef123456&tcins=1 token=%3Fkey%3Dzzz"
    )
    out = redact(text, [BB_KEY])
    assert BB_KEY not in out
    assert "abcdef123456" not in out
    assert "apiKey=***" in out and "key=***" in out
    assert "format=json" in out and "tcins=1" in out
    assert redact("plain text", [None, "", "abc"]) == "plain text"


# =========================================================================== JSON paths


def test_compile_and_resolve_dotted_paths_with_indices() -> None:
    doc = {"data": {"items": [{"images": [{"url": "u0"}, {"url": "u1"}]}], "count": 1}, "list": [[1, 2], [3]]}
    assert compile_path("data.items[0].images[-1].url") == ("data", "items", 0, "images", -1, "url")
    assert resolve_path(doc, "data.items[0].images[1].url") == "u1"
    assert resolve_path(doc, "data.items[0].images[-1].url") == "u1"
    assert resolve_path(doc, "data.items.0.images.0.url") == "u0"
    assert resolve_path(doc, "list[0][1]") == 2
    assert resolve_path(doc, "") is doc
    assert resolve_path(doc, "data.items[5].images") is None
    assert resolve_path(doc, "data.count.nope") is None
    assert resolve_path([{"a": 1}], "[0].a") == 1
    for bad in ("a..b", "a[x]", "a[0", ".a"):
        with pytest.raises(ValueError):
            compile_path(bad)


# =========================================================================== Best Buy parsing


def test_parse_bestbuy_products_maps_price_stock_condition_and_dates() -> None:
    endpoint = BestBuyEndpoint(name="bby_gpus", api_key=SecretStr(BB_KEY), skus=["6614151"], profile_hint="rtx_5090")
    skipped: list[tuple[str, str]] = []
    listings = parse_bestbuy_products(load("bestbuy_products_gpus.json"), endpoint, lambda r, i: skipped.append((r, i)))
    assert [r.source_id for r in listings] == ["bby_gpus:6614151", "bby_gpus:6614153", "bby_gpus:6575404"]
    assert skipped == [("no_price", "6600001")]

    fe, sold_out, refurb = listings
    assert fe.source == "retail" and fe.source_kind is SourceKind.RETAIL
    assert fe.retailer == "Best Buy" and fe.profile_hint == "rtx_5090"
    assert fe.title == "NVIDIA - GeForce RTX 5090 32GB GDDR7 Graphics Card - Dark Gun Metal"
    assert fe.price == 1999.99 and fe.list_price == 1999.99 and fe.currency == "USD"
    assert fe.in_stock is True and fe.condition == "New" and fe.sku == "6614151"
    assert fe.url == "https://api.bestbuy.com/click/-/6614151/pdp"
    assert fe.image_urls == ["https://pisces.bbystatic.com/image2/BestBuy_US/images/products/6614/6614151_sd.jpg"]
    assert fe.extra["endpoint"] == "bby_gpus" and fe.extra["adapter"] == "bestbuy"
    # Naive Best Buy timestamps are kept verbatim but never guessed into posted_at.
    assert fe.posted_at is None and fe.extra["price_update_date"] == "2026-10-05T23:00:41"
    assert fe.extra["add_to_cart_url"] == "https://api.bestbuy.com/click/-/6614151/cart"

    assert sold_out.in_stock is False and sold_out.price == 899.99 and sold_out.list_price == 999.99
    assert sold_out.extra["orderable"] == "SoldOut" and sold_out.extra["price_restriction"] == "MAP"
    # The later of priceUpdateDate / onlineAvailabilityUpdateDate (both carry offsets here).
    assert sold_out.posted_at == datetime(2026, 10, 5, 14, 15, tzinfo=UTC)

    # onlineAvailability false but orderable "Available" still counts as buyable.
    assert refurb.in_stock is True and refurb.condition == "Refurbished"
    assert refurb.url == "https://www.bestbuy.com/site/6575404.p?skuId=6575404"


def test_parse_bestbuy_error_and_shape_problems() -> None:
    endpoint = BestBuyEndpoint(name="b", api_key=SecretStr(BB_KEY), skus=["1"])
    with pytest.raises(PayloadError, match="unable to validate"):
        parse_bestbuy_products({"errorCode": "403", "errorMessage": "We were unable to validate your API Key."}, endpoint)
    with pytest.raises(PayloadError):
        parse_bestbuy_products([1, 2], endpoint)
    payload = {"products": ["junk", {"sku": 5, "salePrice": 3}, {"sku": 6, "name": "x", "salePrice": 10}]}
    out = parse_bestbuy_products(payload, endpoint)
    assert [r.sku for r in out] == ["6"] and out[0].in_stock is None


def test_bestbuy_url_uses_in_operator() -> None:
    assert bestbuy_products_url(["1", "2", "3"]) == "https://api.bestbuy.com/v1/products(sku in(1,2,3))"
    assert str(URL(bestbuy_products_url(["1", "2"]))).endswith("/v1/products(sku%20in(1,2))")


# =========================================================================== Shopify parsing


def test_parse_shopify_product_js_converts_cents_and_builds_variants() -> None:
    endpoint = ShopifyEndpoint(name="gpu_shop", retailer="GPU Shop", store_url=STORE, handles=["x"], profile_hint="rtx_5090")
    listings = parse_shopify_product_js(load("shopify_product_rtx5090.js.json"), endpoint, STORE)
    assert len(listings) == 2
    base, protected = listings
    assert base.source_id == "gpu_shop:gpu-shop.example.com:44011122233301"
    assert base.title == "NVIDIA GeForce RTX 5090 Founders Edition"  # "Default Title" is not appended
    assert base.price == 1999.99  # 199999 cents
    assert base.list_price is None  # compare_at_price 0 means "no compare-at price"
    assert base.in_stock is True and base.quantity == 3 and base.sku == "900-1G144-2530-000"
    assert base.url == f"{STORE}/products/nvidia-geforce-rtx-5090-founders-edition?variant=44011122233301"
    assert base.image_urls[0] == "https://cdn.shopify.com/s/files/1/0000/0001/files/rtx5090-front.png?v=1774646345"
    assert base.description == ""  # marketing copy is not fed to the text rules
    assert base.extra["vendor"] == "NVIDIA" and base.extra["product_type"] == "Graphics Cards"
    assert base.retailer == "GPU Shop" and base.profile_hint == "rtx_5090"
    assert base.condition is None  # Shopify has no condition: normalizer default (NEW for retail) applies

    assert protected.title == "NVIDIA GeForce RTX 5090 Founders Edition - With 3-Year Protection"
    assert protected.price == 2099.99 and protected.list_price == 2199.99
    assert protected.in_stock is False and protected.quantity is None
    # Variant image first, protocol-relative URLs made absolute.
    assert protected.image_urls[0] == "https://cdn.shopify.com/s/files/1/0000/0001/files/rtx5090-protection.png?v=1774646345"
    assert len(protected.image_urls) == 3


def test_parse_shopify_products_json_decimal_strings_and_missing_available() -> None:
    endpoint = ShopifyEndpoint(name="gpu_shop", retailer="GPU Shop", store_url=STORE, collection="gpus")
    skipped: list[tuple[str, str]] = []
    listings, count, first_id = parse_shopify_products_json(
        load("shopify_collection_gpus_page1.json"), endpoint, STORE, lambda r, i: skipped.append((r, i))
    )
    assert count == 2 and first_id == "7340901859408"
    assert skipped == [("no_price", "42146889039961")]
    msi, gigabyte = listings
    assert msi.price == 999.99 and msi.list_price == 1199.99 and msi.in_stock is True
    assert msi.posted_at == datetime(2026, 10, 6, 5, 11, 20, tzinfo=UTC)
    assert msi.image_urls == ["https://cdn.shopify.com/s/files/1/1104/4168/files/msi-5080-ventus.png?v=1774646345"]
    assert gigabyte.title == "Gigabyte Radeon RX 9070 XT Gaming OC - 16GB"
    assert gigabyte.price == 649.0
    assert gigabyte.list_price is None  # "0.00" compare-at is treated as absent
    assert gigabyte.in_stock is None  # theme omits variants[].available
    with pytest.raises(PayloadError):
        parse_shopify_products_json({"product": {}}, endpoint, STORE)


def test_normalize_store_url() -> None:
    assert normalize_store_url("gpu-shop.example.com/") == "https://gpu-shop.example.com"
    assert normalize_store_url("https://gpu-shop.example.com/collections/all") == "https://gpu-shop.example.com"
    with pytest.raises(ValueError):
        normalize_store_url("ftp://x")


# =========================================================================== Target parsing


def test_parse_redsky_summaries_prices_and_availability() -> None:
    endpoint = TargetEndpoint(name="target_consoles", api_key=SecretStr(TARGET_KEY), tcins=["93954446"], store_id="1375")
    skipped: list[tuple[str, str]] = []
    listings = parse_redsky_summaries(load("redsky_summary_with_fulfillment.json"), endpoint, lambda r, i: skipped.append((r, i)))
    assert [r.source_id for r in listings] == ["target_consoles:93954446", "target_consoles:89981234", "target_consoles:12345678"]
    assert skipped == [("missing_id_or_title", "87654321")]
    switch, tv, hidden = listings
    assert switch.title == "Nintendo Switch 2 Console &#38; Mario Kart World Bundle"  # normalizer unescapes
    assert switch.price == 449.99 and switch.list_price == 499.99 and switch.in_stock is True
    assert switch.retailer == "Target" and switch.condition == "New"
    assert switch.url.endswith("/-/A-93954446") and len(switch.image_urls) == 2
    assert switch.extra["price_type"] == "sale" and switch.extra["is_marketplace"] is False
    assert switch.extra["store_id"] == "1375"
    assert tv.price == 1499.99 and tv.list_price == 2499.99 and tv.in_stock is False
    # Non-numeric price text goes to the normalizer, which will reject it; availability unknown.
    assert hidden.price == "See price in cart" and hidden.in_stock is None
    assert hidden.url == "https://www.target.com/p/-/A-12345678"


def test_parse_redsky_error_payload_and_bot_wall_detection() -> None:
    endpoint = TargetEndpoint(name="t", api_key=SecretStr(TARGET_KEY), tcins=["93954446"])
    with pytest.raises(PayloadError, match="No product found"):
        parse_redsky_summaries({"errors": [{"message": "No product found with tcin 1"}], "data": {}}, endpoint)
    pdp = {"tcin": "1", "item": {"product_description": {"title": "PDP shape"}}, "price": {"current_retail": 5}}
    single = {"data": {"product": pdp}}
    assert parse_redsky_summaries(single, endpoint)[0].price == 5.0
    assert is_bot_wall_json(load("redsky_block_435.json")) is True
    assert is_bot_wall_json(load("redsky_summary_with_fulfillment.json")) is False
    assert redsky_in_stock({"shipping_options": {"availability_status": "PRE_ORDER_SELLABLE"}}) is True
    assert redsky_in_stock({"store_options": [{"in_store_only": {"availability_status": "LIMITED_STOCK"}}]}) is True
    assert redsky_in_stock({"shipping_options": {"availability_status": "DISCONTINUED"}}) is False
    assert redsky_in_stock({}) is None and redsky_in_stock(None) is None


# =========================================================================== Newegg parsing


def test_parse_newegg_recorded_productrealtime() -> None:
    endpoint = NeweggEndpoint(name="newegg_gpus", item_numbers=["14-137-866"], profile_hint="rtx_4070_ti")
    (listing,) = parse_newegg_realtime(load("newegg_productrealtime_14-137-866.json"), endpoint)
    assert listing.source_id == "newegg_gpus:14-137-866"
    assert listing.title == "MSI Gaming GeForce RTX 4070 Ti Graphics Card RTX 4070 Ti GAMING SLIM WHITE 12G"
    assert listing.price == 799.99 and listing.list_price == 829.99
    assert listing.in_stock is False and listing.quantity == 0
    assert listing.condition == "New" and listing.seller is None  # sold by Newegg itself
    assert listing.shipping == 0.0  # ItemTagFlags.FreeShipping == 1
    assert listing.url == "https://www.newegg.com/p/N82E16814137866"
    assert listing.image_urls[0] == "https://c1.neweggimages.com/ProductImageOriginal/14-137-866-02.png"
    assert len(listing.image_urls) == 6
    assert listing.retailer == "Newegg" and listing.profile_hint == "rtx_4070_ti"
    assert listing.extra["promotion"] == "OUT OF STOCK" and listing.extra["price_in_cart"] is False
    assert listing.extra["subcategory"] == "GPUs / Video Graphics Cards"


def test_parse_newegg_open_box_marketplace_and_missing_item() -> None:
    endpoint = NeweggEndpoint(name="ne", item_numbers=["9SIA1234567890"])
    payload = copy.deepcopy(load("newegg_productrealtime_14-137-866.json"))
    main = payload["MainItem"]
    main.update({"Item": "9SIA1234567890", "FinalPrice": 649.5, "Instock": True, "Stock": 2})
    main.update({"ItemTagFlags": {}, "ShippingCharge": 14.99})
    main["Feature"]["IsOpenBoxed"] = True
    main["Seller"]["SellerName"] = "Some Marketplace Seller"
    (listing,) = parse_newegg_realtime(payload, endpoint)
    assert listing.condition == "Open Box" and listing.in_stock is True and listing.quantity == 2
    assert listing.price == 649.5 and listing.shipping == 14.99
    assert listing.seller is not None and listing.seller.name == "Some Marketplace Seller"
    assert listing.extra["marketplace_seller"] == "Some Marketplace Seller"
    assert listing.url == "https://www.newegg.com/p/9SIA1234567890"

    skipped: list[tuple[str, str]] = []
    deactivated = {"MainItem": None, "IsForceDeactiveItem": True}
    assert parse_newegg_realtime(deactivated, endpoint, lambda r, i: skipped.append((r, i))) == []
    assert skipped == [("no_main_item", "")]
    hidden = copy.deepcopy(load("newegg_productrealtime_14-137-866.json"))
    hidden["MainItem"].update({"FinalPrice": 0, "UnitCost": 0})
    assert parse_newegg_realtime(hidden, endpoint) == []
    assert newegg_product_url("14-137-866") == "https://www.newegg.com/p/N82E16814137866"


# =========================================================================== generic JSON parsing


def generic_endpoint(**kw: Any) -> GenericJsonEndpoint:
    data: dict[str, Any] = {
        "name": "custom_api",
        "adapter": "json",
        "retailer": "Example",
        "url": "https://api.example.com/v1/products?category=gpu",
        "items_path": "data.products",
        "fields": {
            "id": "sku",
            "title": "name",
            "price": "pricing.current",
            "list_price": "pricing.regular",
            "in_stock": "inventory.status",
            "url": "links.web",
            "image": "images[0].url",
        },
        "price_divisor": 100,
    }
    data.update(kw)
    return GenericJsonEndpoint.model_validate(data)


GENERIC_PAYLOAD = {
    "data": {
        "products": [
            {
                "sku": "A-100",
                "name": "Radeon RX 9070 XT 16GB",
                "pricing": {"current": 59999, "regular": 69999},
                "inventory": {"status": "IN_STOCK"},
                "links": {"web": "/p/A-100"},
                "images": [{"url": "https://img.example.com/a100.jpg"}, {"url": "https://img.example.com/a100b.jpg"}],
            },
            {
                "sku": 200,
                "name": "GeForce RTX 5070 12GB",
                "pricing": {"current": "54900"},
                "inventory": {"status": "sold out"},
                "images": [],
            },
            {"sku": "C-300", "name": "No price item", "pricing": {}},
            "garbage",
        ]
    }
}


def test_parse_generic_items_maps_fields_paths_and_divisor() -> None:
    endpoint = generic_endpoint(url_template="https://store.example.com/p/{id}", condition="open_box")
    skipped: list[tuple[str, str]] = []
    listings = parse_generic_items(GENERIC_PAYLOAD, endpoint, on_skip=lambda r, i: skipped.append((r, i)))
    assert skipped == [("no_price", "C-300"), ("not_an_object", "")]
    first, second = listings
    assert first.source_id == "custom_api:A-100" and first.price == 599.99 and first.list_price == 699.99
    assert first.in_stock is True and first.url == "https://api.example.com/p/A-100"
    assert first.image_urls == ["https://img.example.com/a100.jpg"]
    assert first.condition == "Open Box"  # endpoint default condition
    assert second.source_id == "custom_api:200" and second.price == 549.0 and second.in_stock is False
    assert second.url == "https://store.example.com/p/200" and second.image_urls == []
    assert first.extra == {"endpoint": "custom_api", "adapter": "json"}


def test_parse_generic_items_fallbacks_and_errors() -> None:
    endpoint = generic_endpoint(price_divisor=1, items_path="", fields={"id": "id", "title": "t", "price": "p", "condition": "c"})
    items = [{"id": 1, "t": "Thing", "p": "$1,299.99", "c": "Refurbished"}, {"id": 2, "t": "Other", "p": "Ask"}]
    listings = parse_generic_items(items, endpoint)
    assert listings[0].price == 1299.99 and listings[0].condition == "Refurbished"
    assert listings[1].price == "Ask"  # non-numeric text is left to the normalizer
    assert listings[0].url == endpoint.url and listings[0].extra["url_fallback"] is True
    single = generic_endpoint(items_path="data.product", fields={"id": "id", "title": "t", "price": "p"}, price_divisor=1)
    assert len(parse_generic_items({"data": {"product": {"id": 9, "t": "One", "p": 5}}}, single)) == 1
    with pytest.raises(PayloadError):
        parse_generic_items({"data": {"products": "nope"}}, generic_endpoint())
    with pytest.raises(ValueError, match="malformed"):
        GenericJsonAdapter(generic_endpoint(fields={"id": "a[x]", "title": "t", "price": "p"}))


# =========================================================================== Best Buy over HTTP


async def test_bestbuy_batches_skus_and_sends_documented_query(http: HttpClient) -> None:
    skus = ["6614151", "6614153", "6575404", "bad-sku"]
    endpoint = BestBuyEndpoint(name="bby", api_key=SecretStr(BB_KEY), skus=skus, batch_size=2)
    adapter = BestBuyAdapter(endpoint)
    payload = load("bestbuy_products_gpus.json")
    with aioresponses() as m:
        m.get(BB_RE, payload={**payload, "products": payload["products"][:2]})
        m.get(BB_RE, payload={**payload, "products": payload["products"][2:3]})
        listings = await adapter.fetch(http)
        calls = requests_to(m, BB_RE)
    assert adapter.skus == ["6614151", "6614153", "6575404"]  # invalid SKU dropped before it reaches the path
    assert len(calls) == 2
    paths = sorted(url.raw_path for url, _ in calls)
    assert paths == ["/v1/products(sku%20in(6575404))", "/v1/products(sku%20in(6614151,6614153))"]
    url, call = calls[0]
    assert url.query["apiKey"] == BB_KEY and url.query["format"] == "json" and url.query["pageSize"] == "100"
    assert url.query["show"].split(",") == list(BESTBUY_SHOW_FIELDS)
    assert "sec-ch-ua" not in {k.lower() for k in call.kwargs["headers"]}  # official API: no browser disguise
    assert sorted(r.sku for r in listings) == ["6575404", "6614151", "6614153"]


async def test_bestbuy_403_is_auth_failure_and_never_leaks_the_key(http: HttpClient, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    config = make_config([bb_endpoint()])
    ingestor = make_ingestor(config, http)
    body = {"errorCode": "403", "errorMessage": f"We were unable to validate your API Key. (apiKey={BB_KEY})"}
    with aioresponses() as m:
        m.get(BB_RE, status=403, payload=body)
        with pytest.raises(SourceAuthError) as info:
            await ingestor.run_once()
        assert len(requests_to(m, BB_RE)) == 1  # 403 is never retried
    message = str(info.value)
    assert "403" in message and "quota" in message
    assert BB_KEY not in message
    state = ingestor.endpoints[0]
    assert state.state == "blocked" and BB_KEY not in (state.last_error or "")
    assert all(BB_KEY not in record.getMessage() and BB_KEY not in str(record.__dict__) for record in caplog.records)


async def test_bestbuy_400_echoing_the_url_is_redacted(http: HttpClient) -> None:
    adapter = BestBuyAdapter(BestBuyEndpoint(name="bby", api_key=SecretStr(BB_KEY), skus=["1"]))
    echo = f"Couldn't understand '/v1/products(sku in(1))?apiKey={BB_KEY}&format=json'"
    with aioresponses() as m:
        m.get(BB_RE, status=400, body=echo, content_type="text/plain")
        with pytest.raises(RetailEndpointError) as info:
            await adapter.fetch(http)
    assert info.value.status == 400 and not info.value.blocked
    assert BB_KEY not in str(info.value) and "apiKey=***" in str(info.value)


async def test_bestbuy_server_errors_exhaust_retries_without_leaking(http: HttpClient) -> None:
    adapter = BestBuyAdapter(BestBuyEndpoint(name="bby", api_key=SecretStr(BB_KEY), skus=["1"]))
    with aioresponses() as m:
        m.get(BB_RE, status=503, body="busy", repeat=True)
        with pytest.raises(RetailEndpointError) as info:
            await adapter.fetch(http)
        assert len(requests_to(m, BB_RE)) == 2  # BackoffPolicy(max_attempts=2)
    assert "503" in str(info.value) and BB_KEY not in str(info.value)


async def test_bestbuy_missing_key_is_reported_without_a_request(http: HttpClient) -> None:
    config = make_config([bb_endpoint(api_key=None)], enabled=False)
    ingestor = make_ingestor(config, http)
    with aioresponses() as m:
        with pytest.raises(SourceAuthError, match="api_key is not configured"):
            await ingestor.poll()
        assert not m.requests


# =========================================================================== Shopify over HTTP


async def test_shopify_conditional_get_304_skips_parse_and_replays(http: HttpClient, monkeypatch: pytest.MonkeyPatch) -> None:
    metrics = Metrics()
    config = make_config([shop_endpoint()])
    clock = FakeClock()
    ingestor = make_ingestor(config, http, clock=clock, metrics=metrics)
    parses = 0
    real_parse = retail.parse_shopify_product_js

    def counting_parse(*args: Any, **kwargs: Any) -> list[RawListing]:
        nonlocal parses
        parses += 1
        return real_parse(*args, **kwargs)

    monkeypatch.setattr(retail, "parse_shopify_product_js", counting_parse)
    url = f"{STORE}/products/nvidia-geforce-rtx-5090-founders-edition.js"
    etag = 'W/"page_cache:11044168:ProductDetailsController:720bc786862ec87c1d7c478b1d595f0e"'
    with aioresponses() as m:
        body = json.dumps(load("shopify_product_rtx5090.js.json"))
        m.get(url, body=body, content_type="text/javascript", headers={"ETag": etag})
        m.get(url, status=304, body="", headers={"ETag": etag.removeprefix("W/")})
        first = await ingestor.run_once()
        clock.t += 25
        second_raw = await ingestor.poll()
        calls = requests_to(m, url)
    assert parses == 1  # the 304 body was never parsed
    assert len(first) == 2
    assert "If-None-Match" not in calls[0][1].kwargs["headers"]
    assert calls[1][1].kwargs["headers"]["If-None-Match"] == etag
    # The replay keeps poll() complete ("everything currently visible") with fresh receive times.
    assert [r.source_id for r in second_raw] == [r.source_id for r in first]
    assert all(b.received_at >= a.received_at for a, b in zip(first, second_raw, strict=True))
    assert ingestor.select_changed(second_raw) == []  # unchanged -> nothing emitted downstream
    assert metrics.counter("retail_not_modified_total", labelnames=("endpoint",)).value(endpoint="gpu_shop") == 1
    # Shopify gets a coherent browser identity sent as an XHR (CORS) fetch.
    headers = calls[0][1].kwargs["headers"]
    assert headers["Sec-Fetch-Mode"] == "cors" and "Mozilla/5.0" in headers["User-Agent"]


async def test_cache_bust_param_is_opt_in_per_endpoint(http: HttpClient) -> None:
    busted = ShopifyAdapter(ShopifyEndpoint.model_validate(shop_endpoint(name="busted", cache_bust=True, cache_bust_param="cb")))
    plain = ShopifyAdapter(ShopifyEndpoint.model_validate(shop_endpoint(name="plain", conditional=False)))
    pattern = re.compile(re.escape(f"{STORE}/products/nvidia-geforce-rtx-5090-founders-edition.js") + r".*")
    with aioresponses() as m:
        m.get(pattern, payload=load("shopify_product_rtx5090.js.json"), repeat=True)
        await busted.fetch(http)
        await busted.fetch(http)
        await plain.fetch(http)
        urls = [url for url, _ in requests_to(m, pattern)]
    busted_urls = [u for u in urls if "cb" in u.query]
    plain_urls = [u for u in urls if not u.query]
    assert len(busted_urls) == 2 and len(plain_urls) == 1
    assert busted_urls[0].query["cb"] != busted_urls[1].query["cb"]  # a fresh nonce every request
    assert busted_urls[0].query["cb"].isdigit()


async def test_shopify_cloudflare_challenge_429_blocks_without_retry(http: HttpClient) -> None:
    endpoint = ShopifyEndpoint.model_validate(shop_endpoint(handles=["a", "b", "c"]))
    adapter = ShopifyAdapter(endpoint, semaphore=asyncio.Semaphore(1))
    pattern = re.compile(re.escape(STORE) + r"/products/.*\.js")
    with aioresponses() as m:
        challenge = {"cf-mitigated": "challenge"}
        m.get(pattern, status=429, body=CF_CHALLENGE_HTML, content_type="text/html", headers=challenge, repeat=True)
        with pytest.raises(RetailEndpointError) as info:
            await adapter.fetch(http)
        calls = requests_to(m, pattern)
    assert info.value.blocked and info.value.status == 429
    assert "Cloudflare managed challenge" in str(info.value)
    assert len(calls) == 1  # no retry, and the remaining handles were not hammered


async def test_shopify_html_page_with_200_is_a_bot_wall(http: HttpClient) -> None:
    adapter = ShopifyAdapter(ShopifyEndpoint.model_validate(shop_endpoint()))
    with aioresponses() as m:
        m.get(re.compile(re.escape(STORE) + ".*"), status=200, body=CF_CHALLENGE_HTML, content_type="text/html")
        with pytest.raises(RetailEndpointError, match="bot wall") as info:
            await adapter.fetch(http)
    assert info.value.blocked


async def test_shopify_collection_paginates_until_short_page(http: HttpClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retail, "SHOPIFY_PAGE_LIMIT", 2)
    endpoint = ShopifyEndpoint.model_validate(shop_endpoint(handles=[], collection="graphics-cards", max_pages=5))
    adapter = ShopifyAdapter(endpoint)
    page = re.compile(re.escape(f"{STORE}/collections/graphics-cards/products.json") + r"\?.*")
    with aioresponses() as m:
        m.get(page, payload={"products": [shopify_product(1, "Card One", "100.00"), shopify_product(2, "Card Two", "200.00")]})
        m.get(page, payload={"products": [shopify_product(3, "Card Three", "300.00")]})
        listings = await adapter.fetch(http)
        calls = requests_to(m, page)
    assert [r.price for r in listings] == [100.0, 200.0, 300.0]
    assert sorted(url.query["page"] for url, _ in calls) == ["1", "2"]  # stopped after the short page
    assert {url.query["limit"] for url, _ in calls} == {"2"}


async def test_shopify_pagination_stops_when_store_repeats_page_one(http: HttpClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retail, "SHOPIFY_PAGE_LIMIT", 1)
    adapter = ShopifyAdapter(ShopifyEndpoint.model_validate(shop_endpoint(handles=[], collection=None, max_pages=4)))
    page = re.compile(re.escape(f"{STORE}/products.json") + r"\?.*")
    with aioresponses() as m:
        m.get(page, payload={"products": [shopify_product(1, "Same Card", "100.00")]}, repeat=True)
        listings = await adapter.fetch(http)
        calls = requests_to(m, page)
    assert len(listings) == 1 and len(calls) == 2


async def test_shopify_one_missing_handle_does_not_fail_the_endpoint(http: HttpClient) -> None:
    handles = ["nvidia-geforce-rtx-5090-founders-edition", "gone"]
    adapter = ShopifyAdapter(ShopifyEndpoint.model_validate(shop_endpoint(handles=handles)))
    with aioresponses() as m:
        m.get(f"{STORE}/products/nvidia-geforce-rtx-5090-founders-edition.js", payload=load("shopify_product_rtx5090.js.json"))
        m.get(f"{STORE}/products/gone.js", status=404, body="Not Found", content_type="text/plain")
        listings = await adapter.fetch(http)
    assert len(listings) == 2


# =========================================================================== Target / Newegg / generic over HTTP


async def test_target_batches_tcins_and_sends_store_context(http: HttpClient) -> None:
    tcins = ["93954446", "89981234", "x1"]
    endpoint = TargetEndpoint(name="tgt", api_key=SecretStr(TARGET_KEY), tcins=tcins, store_id="1375", zip_code="10001")
    adapter = TargetAdapter(endpoint, rng=random.Random(1))
    with aioresponses() as m:
        m.get(REDSKY_RE, payload=load("redsky_summary_with_fulfillment.json"))
        listings = await adapter.fetch(http)
        ((url, _),) = requests_to(m, REDSKY_RE)
    assert url.query["tcins"] == "93954446,89981234" and "tcin" not in url.query
    assert url.query["key"] == TARGET_KEY and url.query["channel"] == "WEB"
    assert url.query["store_id"] == url.query["required_store_id"] == url.query["scheduled_delivery_store_id"] == "1375"
    assert url.query["has_required_store_id"] == "true" and url.query["zip"] == "10001"
    assert re.fullmatch(r"[0-9A-F]{32}", url.query["visitor_id"])
    assert len(listings) == 3


async def test_target_perimeterx_435_is_blocked_and_cools_down(http: HttpClient) -> None:
    config = make_config(
        [{"name": "tgt", "adapter": "target_redsky", "api_key": TARGET_KEY, "tcins": ["93954446"], "poll_interval_seconds": 30}],
        cooldown_seconds=300,
    )
    clock = FakeClock()
    notices: list[tuple[str, str]] = []
    ingestor = make_ingestor(config, http, clock=clock, notices=notices)
    with aioresponses() as m:
        m.get(REDSKY_RE, status=435, payload=load("redsky_block_435.json"))
        with pytest.raises(SourceBlocked) as info:
            await ingestor.poll()
        assert len(requests_to(m, REDSKY_RE)) == 1
    assert "435" in str(info.value) and TARGET_KEY not in str(info.value)
    assert info.value.cooldown_seconds == pytest.approx(300)
    assert ingestor.endpoints[0].state == "blocked"
    assert notices == []  # the whole source is blocked: the base loop sends the one notice
    # Still cooling down 100 s later: the endpoint is skipped, nothing is requested.
    clock.t += 100
    with aioresponses() as m:
        assert await ingestor.poll() == []
        assert not m.requests


async def test_newly_blocked_endpoint_notifies_once_while_others_keep_working(http: HttpClient) -> None:
    config = make_config(
        [
            {"name": "tgt", "adapter": "target_redsky", "api_key": TARGET_KEY, "tcins": ["93954446"], "poll_interval_seconds": 20},
            shop_endpoint(poll_interval_seconds=20),
        ],
        cooldown_seconds=30,
    )
    clock = FakeClock(0.0)
    notices: list[tuple[str, str]] = []
    ingestor = make_ingestor(config, http, clock=clock, notices=notices)
    with aioresponses() as m:
        m.get(REDSKY_RE, status=435, payload=load("redsky_block_435.json"), repeat=True)
        m.get(re.compile(re.escape(STORE) + ".*"), payload=load("shopify_product_rtx5090.js.json"), repeat=True)
        assert len(await ingestor.poll()) == 2
        clock.t = 30.0
        assert len(await ingestor.poll()) == 2
    states = {s.name: s for s in ingestor.endpoints}
    assert states["tgt"].consecutive_blocks == 2
    assert states["tgt"].next_due == pytest.approx(30.0 + 60.0)  # 30 s cooldown doubled
    assert len(notices) == 1
    title, message = notices[0]
    assert title == "retail endpoint tgt blocked" and "435" in message and TARGET_KEY not in message


async def test_target_block_json_with_200_and_retired_aggregation(http: HttpClient) -> None:
    adapter = TargetAdapter(TargetEndpoint(name="tgt", api_key=SecretStr(TARGET_KEY), tcins=["93954446"]))
    with aioresponses() as m:
        m.get(REDSKY_RE, status=200, payload=load("redsky_block_435.json"))
        with pytest.raises(RetailEndpointError, match="PerimeterX") as blocked:
            await adapter.fetch(http)
        m.get(REDSKY_RE, status=410, body="Gone", content_type="text/plain")
        with pytest.raises(RetailEndpointError, match="retired") as gone:
            await adapter.fetch(http)
    assert blocked.value.blocked and not gone.value.blocked
    assert TARGET_KEY not in str(blocked.value) + str(gone.value)


async def test_newegg_requests_each_item_and_defaults_to_60s(http: HttpClient) -> None:
    config = make_config([{"name": "ne", "adapter": "newegg", "item_numbers": ["14-137-866", "N82E16814137866"]}])
    ingestor = make_ingestor(config, http)
    assert ingestor.endpoints[0].interval == NEWEGG_MIN_INTERVAL_SECONDS
    payload = load("newegg_productrealtime_14-137-866.json")
    with aioresponses() as m:
        m.get(NEWEGG_RE, payload=payload, repeat=True)
        listings = await ingestor.poll()
        calls = requests_to(m, NEWEGG_RE)
    assert sorted(url.query["ItemNumber"] for url, _ in calls) == ["14-137-866", "N82E16814137866"]
    assert len(listings) == 2 and listings[0].price == 799.99
    explicit = make_config([{"name": "ne", "adapter": "newegg", "item_numbers": ["14-137-866"], "poll_interval_seconds": 90}])
    assert make_ingestor(explicit, http).endpoints[0].interval == 90


async def test_generic_post_endpoint_sends_body_params_and_headers(http: HttpClient) -> None:
    endpoint = generic_endpoint(
        method="POST",
        url="https://api.example.com/v1/search",
        params={"region": "us"},
        body={"query": "gpu", "page": 1},
        headers={"X-Api-Version": "2"},
        url_template="https://store.example.com/p/{id}",
    )
    adapter = GenericJsonAdapter(endpoint)
    with aioresponses() as m:
        m.post("https://api.example.com/v1/search?region=us", payload=GENERIC_PAYLOAD)
        listings = await adapter.fetch(http)
        ((_url, call),) = requests_to(m, "https://api.example.com/v1/search")
    assert call.kwargs["json"] == {"query": "gpu", "page": 1}
    assert call.kwargs["headers"]["X-Api-Version"] == "2"
    assert "If-None-Match" not in call.kwargs["headers"]
    assert [r.source_id for r in listings] == ["custom_api:A-100", "custom_api:200"]


# =========================================================================== scheduling, failures, concurrency


async def test_per_endpoint_schedule_runs_only_due_endpoints(http: HttpClient) -> None:
    config = make_config(
        [
            shop_endpoint(name="fast", poll_interval_seconds=10),
            shop_endpoint(name="slow", store_url="https://slow-shop.example.com", poll_interval_seconds=30),
        ]
    )
    clock = FakeClock(1000.0)
    ingestor = make_ingestor(config, http, clock=clock)
    product = load("shopify_product_rtx5090.js.json")
    fast = re.compile(re.escape(STORE) + ".*")
    slow = re.compile(re.escape("https://slow-shop.example.com") + ".*")
    with aioresponses() as m:
        m.get(fast, payload=product, repeat=True)
        m.get(slow, payload=product, repeat=True)
        first = await ingestor.poll()
        assert {r.extra["endpoint"] for r in first} == {"fast", "slow"}
        assert ingestor.next_interval() == pytest.approx(10)

        clock.t = 1004.0  # nothing due yet
        assert await ingestor.poll() == []
        assert ingestor.next_interval() == pytest.approx(6)

        clock.t = 1010.0
        second = await ingestor.poll()
        assert {r.extra["endpoint"] for r in second} == {"fast"}
        assert ingestor.next_interval() == pytest.approx(10)

        clock.t = 1030.0
        third = await ingestor.poll()
        assert {r.extra["endpoint"] for r in third} == {"fast", "slow"}
        assert len(requests_to(m, fast)) == 3 and len(requests_to(m, slow)) == 2

        clock.t = 1039.8  # due in 0.2 s: within the slack it runs now ...
        assert {r.extra["endpoint"] for r in await ingestor.poll()} == {"fast"}
        clock.t = 1049.5  # ... and next_interval never goes below one second
        assert ingestor.next_interval() == pytest.approx(1.0)
    status = {s["endpoint"]: s for s in ingestor.endpoint_status()}
    assert status["fast"]["polls"] == 4 and status["slow"]["polls"] == 2 and status["fast"]["state"] == "ok"


async def test_jitter_spreads_due_times(http: HttpClient) -> None:
    config = make_config([shop_endpoint(poll_interval_seconds=100)], jitter_pct=0.2)
    clock = FakeClock(0.0)
    ingestor = make_ingestor(config, http, clock=clock)
    seen = set()
    with aioresponses() as m:
        m.get(re.compile(re.escape(STORE) + ".*"), payload=load("shopify_product_rtx5090.js.json"), repeat=True)
        for _ in range(4):
            clock.t = ingestor.endpoints[0].next_due
            await ingestor.poll()
            gap = ingestor.endpoints[0].next_due - clock.t
            assert 80.0 <= gap <= 120.0
            seen.add(round(gap, 6))
    assert len(seen) > 1


async def test_partial_failure_returns_healthy_endpoints(http: HttpClient) -> None:
    config = make_config([shop_endpoint(name="ok_shop"), shop_endpoint(name="broken", store_url="https://broken.example.com")])
    clock = FakeClock()
    ingestor = make_ingestor(config, http, clock=clock)
    with aioresponses() as m:
        m.get(re.compile(re.escape(STORE) + ".*"), payload=load("shopify_product_rtx5090.js.json"))
        m.get(re.compile(re.escape("https://broken.example.com") + ".*"), status=500, body="oops", repeat=True)
        fresh = await ingestor.run_once()
    assert {r.extra["endpoint"] for r in fresh} == {"ok_shop"} and len(fresh) == 2
    states = {s.name: s for s in ingestor.endpoints}
    assert states["ok_shop"].state == "ok" and states["broken"].state == "error"
    assert states["broken"].consecutive_failures == 1 and "500" in (states["broken"].last_error or "")
    assert ingestor.health.state == "ok"  # the poll as a whole succeeded


async def test_all_due_endpoints_failing_fails_the_poll_and_backs_off(http: HttpClient) -> None:
    endpoints = [shop_endpoint(name="a"), shop_endpoint(name="b", store_url="https://b.example.com")]
    config = make_config(endpoints, cooldown_seconds=300)
    clock = FakeClock(0.0)
    ingestor = make_ingestor(config, http, clock=clock)
    with aioresponses() as m:
        m.get(re.compile(r"https://.*"), status=502, body="bad gateway", repeat=True)
        with pytest.raises(SourceError, match="all 2 due retail endpoint"):
            await ingestor.poll()
        clock.t = 20.0
        with pytest.raises(SourceError):
            await ingestor.poll()
    # Exponential per-endpoint backoff: 20 s after the first failure, 40 s after the second.
    assert all(s.consecutive_failures == 2 for s in ingestor.endpoints)
    assert all(s.next_due == pytest.approx(60.0) for s in ingestor.endpoints)


async def test_blocked_endpoint_with_partial_results_still_emits(http: HttpClient) -> None:
    config = make_config([shop_endpoint(handles=["nvidia-geforce-rtx-5090-founders-edition", "zz-blocked"])], max_concurrency=1)
    ingestor = make_ingestor(config, http)
    with aioresponses() as m:
        m.get(f"{STORE}/products/nvidia-geforce-rtx-5090-founders-edition.js", payload=load("shopify_product_rtx5090.js.json"))
        blocked = {"Retry-After": "120"}
        m.get(f"{STORE}/products/zz-blocked.js", status=429, body=CF_CHALLENGE_HTML, content_type="text/html", headers=blocked)
        listings = await ingestor.poll()
    assert len(listings) == 2
    state = ingestor.endpoints[0]
    assert state.state == "blocked" and state.next_due == pytest.approx(1000.0 + 120.0)


async def test_max_concurrency_bounds_in_flight_requests(http: HttpClient) -> None:
    handles = [f"card-{i}" for i in range(6)]
    config = make_config([shop_endpoint(handles=handles)], max_concurrency=2)
    ingestor = make_ingestor(config, http)
    in_flight = 0
    peak = 0
    product = load("shopify_product_rtx5090.js.json")

    async def slow(url: URL, **kwargs: Any) -> CallbackResult:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.02)
        in_flight -= 1
        return CallbackResult(payload=product)

    with aioresponses() as m:
        m.get(re.compile(re.escape(STORE) + ".*"), callback=slow, repeat=True)
        listings = await ingestor.poll()
    assert peak == 2
    assert len(listings) == 12


async def test_signature_emits_on_price_change_and_stock_flip(http: HttpClient) -> None:
    config = make_config([shop_endpoint(conditional=False)])
    clock = FakeClock()
    ingestor = make_ingestor(config, http, clock=clock)
    product = load("shopify_product_rtx5090.js.json")
    flipped = copy.deepcopy(product)
    flipped["variants"][1]["available"] = True
    cheaper = copy.deepcopy(flipped)
    cheaper["variants"][0]["price"] = 149999
    url = f"{STORE}/products/nvidia-geforce-rtx-5090-founders-edition.js"
    with aioresponses() as m:
        for payload in (product, product, flipped, cheaper):
            m.get(url, payload=payload)
        emitted = []
        for _ in range(4):
            emitted.append(await ingestor.run_once())
            clock.t += 30
    assert [len(batch) for batch in emitted] == [2, 0, 1, 1]
    assert emitted[2][0].in_stock is True and emitted[3][0].price == 1499.99
    assert ingestor.signature(emitted[3][0]) == ("1499.99", True)
    assert all(r.node_id == "test" for batch in emitted for r in batch)


async def test_endpoint_without_work_is_skipped(http: HttpClient) -> None:
    config = make_config([bb_endpoint(skus=["not-a-sku"]), shop_endpoint()])
    ingestor = make_ingestor(config, http)
    assert [s.name for s in ingestor.endpoints] == ["gpu_shop"]
    empty = make_ingestor(make_config([]), http)
    assert await empty.poll() == []
    assert 17.0 <= empty.next_interval() <= 23.0  # falls back to the source interval


async def test_registry_and_shipped_config_build_the_ingestor(http: HttpClient) -> None:
    assert load_ingestor_class("retail") is RetailIngestor
    config = load_config(CONFIG_PATH, env={"RETAIL_ENABLED": "true", "BESTBUY_API_KEY": BB_KEY})
    ingestor = make_ingestor(config, http)
    assert ingestor.name == "retail" and ingestor.kind is SourceKind.RETAIL
    (state,) = ingestor.endpoints
    assert isinstance(state.adapter, BestBuyAdapter) and state.interval == 15
    assert state.adapter.redact(f"x?apiKey={BB_KEY}") == "x?apiKey=***"
    with aioresponses() as m:
        m.get(BB_RE, payload=load("bestbuy_products_gpus.json"))
        fresh = await ingestor.run_once()
    assert {r.profile_hint for r in fresh} == {"rtx_5090"}
    assert all(isinstance(r, RawListing) and r.received_at <= datetime.now(UTC) + timedelta(seconds=1) for r in fresh)
    assert isinstance(NeweggAdapter(NeweggEndpoint(name="n", item_numbers=["14-137-866"])).interval(20), float)
