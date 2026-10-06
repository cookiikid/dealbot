"""Source registry: maps config keys to ingestor classes, imported lazily.

Lazy imports keep a cloud node that never runs a browser from importing Playwright,
and let ``--check-config`` run on machines without optional system dependencies.
"""

from __future__ import annotations

import importlib

from deal_radar.config_schema import AppConfig
from deal_radar.sources.base import BaseIngestor, IngestorContext

SOURCE_CLASSES: dict[str, tuple[str, str]] = {
    "ebay": ("deal_radar.sources.ebay_api", "EbayIngestor"),
    "reddit": ("deal_radar.sources.reddit_stream", "RedditIngestor"),
    "slickdeals": ("deal_radar.sources.slickdeals_rss", "SlickdealsIngestor"),
    "retail": ("deal_radar.sources.retail_endpoints", "RetailIngestor"),
    "fb_marketplace": ("deal_radar.sources.fb_marketplace", "FbMarketplaceIngestor"),
    "offerup": ("deal_radar.sources.offerup", "OfferUpIngestor"),
    "craigslist": ("deal_radar.sources.craigslist", "CraigslistIngestor"),
}


def load_ingestor_class(name: str) -> type[BaseIngestor]:
    try:
        module_name, class_name = SOURCE_CLASSES[name]
    except KeyError as exc:
        raise KeyError(f"unknown source {name!r}; known: {sorted(SOURCE_CLASSES)}") from exc
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    if not (isinstance(cls, type) and issubclass(cls, BaseIngestor)):
        raise TypeError(f"{module_name}.{class_name} is not a BaseIngestor")
    return cls


def enabled_source_names(config: AppConfig) -> list[str]:
    """Sources enabled in config *and* assigned to this node (respecting roles/nodes)."""
    return [name for name, _ in config.sources.items() if config.source_enabled_here(name)]


def build_ingestors(config: AppConfig, ctx: IngestorContext, names: list[str] | None = None) -> list[BaseIngestor]:
    ingestors: list[BaseIngestor] = []
    for name in names if names is not None else enabled_source_names(config):
        cls = load_ingestor_class(name)
        ingestors.append(cls(getattr(config.sources, name), ctx))
    return ingestors


__all__ = ["SOURCE_CLASSES", "build_ingestors", "enabled_source_names", "load_ingestor_class"]
