"""Tests for config_schema: the shipped config, env interpolation and fail-fast validation."""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from deal_radar.config_schema import (
    AppConfig,
    ConfigError,
    PriceBand,
    interpolate_env,
    load_config,
    load_dotenv,
)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"


@pytest.fixture(scope="module")
def raw_config() -> dict:
    return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))


def _validate(data: dict, env: dict | None = None) -> AppConfig:
    return AppConfig.model_validate(interpolate_env(data, env or {}))


def test_shipped_config_loads_with_defaults_only() -> None:
    cfg = load_config(CONFIG_PATH, env={})
    ids = [p.id for p in cfg.profiles]
    for required in ("rtx_3090", "rtx_4090", "rtx_5090", "rtx_6000_ada", "rtx_a5000", "rtx_a6000", "oled_4k_monitor",
                     "lg_oled_tv", "prebuilt_flagship"):
        assert required in ids
    assert cfg.storage.redis_url is None and cfg.bus.backend == "memory"
    assert cfg.sources.ebay.enabled is False  # credentials-gated sources are off without env
    assert cfg.sources.reddit.enabled is False  # OAuth-only since unauthenticated access was shut down
    assert cfg.sources.slickdeals.enabled is True and not cfg.sources.slickdeals.search_feeds_from_profiles
    assert len(cfg.sources.ebay.queries) >= 5
    assert {"box_only", "damaged", "rental"} <= set(cfg.filters.rules)
    assert cfg.known_targets() >= {"console", "websocket", "discord:gpu", "telegram:main"}
    # every price band (incl. variant overrides) is internally consistent
    for profile in cfg.profiles:
        for variant in [None, *[v.id for v in profile.variants]]:
            band = profile.band_for(variant)
            assert band.floor < band.target <= band.ceiling


def test_shipped_config_with_full_env() -> None:
    env = {
        "NODE_ID": "gcp-east",
        "APP_ROLES": "collector,processor",
        "REDIS_URL": "redis://10.0.0.2:6379/0",
        "BUS_BACKEND": "redis",
        "EBAY_ENABLED": "true",
        "EBAY_CLIENT_ID": "id",
        "EBAY_CLIENT_SECRET": "TOPSECRETVALUE",
        "DISCORD_WEBHOOK_GPU": "https://discord.com/api/webhooks/1/abc",
        "TELEGRAM_BOT_TOKEN": "123:abc",
        "TELEGRAM_CHAT_ID": "-100123",
        "VISION_ENABLED": "true",
        "VISION_URL": "http://100.64.0.5:11434",
    }
    cfg = load_config(CONFIG_PATH, env=env)
    assert cfg.app.node_id == "gcp-east"
    assert cfg.bus.backend == "redis"
    assert cfg.sources.ebay.enabled and cfg.sources.ebay.client_secret.get_secret_value() == "TOPSECRETVALUE"
    assert cfg.dispatch.discord.webhooks["gpu"].configured
    assert not cfg.dispatch.discord.webhooks["displays"].configured
    assert cfg.vision.enabled and cfg.vision.base_url == "http://100.64.0.5:11434"
    assert "TOPSECRETVALUE" not in repr(cfg.sources.ebay) and "TOPSECRETVALUE" not in str(cfg.model_dump())


def test_env_interpolation_rules() -> None:
    env = {"A": "1", "EMPTY": ""}
    assert interpolate_env("${A}", env) == "1"
    assert interpolate_env("x-${A}-y", env) == "x-1-y"
    assert interpolate_env("${MISSING:-dflt}", env) == "dflt"
    assert interpolate_env("${EMPTY:-dflt}", env) == "dflt"
    assert interpolate_env({"k": ["${A}", 5]}, env) == {"k": ["1", 5]}
    with pytest.raises(ConfigError, match="MISSING"):
        interpolate_env("${MISSING}", env)
    with pytest.raises(ConfigError, match="need it"):
        interpolate_env("${MISSING:?need it}", env)


def test_dotenv_loader(tmp_path: Path) -> None:
    f = tmp_path / ".env"
    f.write_text('# comment\nexport A=1\nB="two words"\nC=3 # trailing\nD=\'q\'\n\nBROKEN\n', encoding="utf-8")
    env: dict[str, str] = {"A": "keep"}
    load_dotenv(f, env)
    assert env == {"A": "keep", "B": "two words", "C": "3", "D": "q"}
    load_dotenv(f, env, override=True)
    assert env["A"] == "1"
    assert load_dotenv(tmp_path / "missing.env", {}) == {}


def test_unknown_keys_are_rejected(raw_config: dict) -> None:
    data = copy.deepcopy(raw_config)
    data["sources"]["reddit"]["poll_intervall_seconds"] = 5
    with pytest.raises(Exception, match="poll_intervall_seconds"):
        _validate(data)


def test_bad_regex_fails_fast(raw_config: dict) -> None:
    data = copy.deepcopy(raw_config)
    data["filters"]["rules"]["box_only"]["patterns"].append("(unclosed")
    with pytest.raises(Exception, match="invalid regex"):
        _validate(data)
    data = copy.deepcopy(raw_config)
    data["profiles"][0]["match"]["any"].append("[z-a]")
    with pytest.raises(Exception, match="invalid regex"):
        _validate(data)


def test_cross_reference_validation(raw_config: dict) -> None:
    data = copy.deepcopy(raw_config)
    data["dispatch"]["routes"][0]["targets"].append("discord:nope")
    with pytest.raises(Exception, match="undefined target"):
        _validate(data)
    data = copy.deepcopy(raw_config)
    data["dispatch"]["routes"][0]["profiles"] = ["no_such_profile"]
    with pytest.raises(Exception, match="unknown profile"):
        _validate(data)
    data = copy.deepcopy(raw_config)
    data["profiles"].append(copy.deepcopy(data["profiles"][0]))
    with pytest.raises(Exception, match="duplicate profile ids"):
        _validate(data)
    data = copy.deepcopy(raw_config)
    data["bus"]["backend"] = "redis"
    with pytest.raises(Exception, match="requires storage.redis_url"):
        _validate(data)
    data = copy.deepcopy(raw_config)
    data["app"]["roles"] = "collector"
    with pytest.raises(Exception, match="collector-only node needs bus.backend=redis"):
        _validate(data)


def test_numeric_and_ordering_constraints(raw_config: dict) -> None:
    data = copy.deepcopy(raw_config)
    data["scoring"]["weights"] = {"discount": 0.5, "statistical": 0.5, "target": 0.5}
    with pytest.raises(Exception, match="sum to 1.0"):
        _validate(data)
    data = copy.deepcopy(raw_config)
    data["scoring"]["severity"] = {"critical": 50, "high": 70, "medium": 55}
    with pytest.raises(Exception, match="medium <= high <= critical"):
        _validate(data)
    data = copy.deepcopy(raw_config)
    data["profiles"][0]["price"]["target"] = data["profiles"][0]["price"]["ceiling"] + 1
    with pytest.raises(Exception, match="floor < target <= ceiling"):
        _validate(data)
    data = copy.deepcopy(raw_config)
    data["profiles"][2]["variants"][0]["price"]["floor"] = 5000  # 3090 Ti variant floor above its ceiling
    with pytest.raises(Exception, match="floor < target <= ceiling"):
        _validate(data)


def test_credentials_required_when_source_enabled(raw_config: dict) -> None:
    data = copy.deepcopy(raw_config)
    data["sources"]["ebay"]["enabled"] = True
    with pytest.raises(Exception, match="EBAY_CLIENT_ID"):
        _validate(data)
    data = copy.deepcopy(raw_config)
    data["sources"]["retail"]["enabled"] = True
    with pytest.raises(Exception, match="BESTBUY_API_KEY"):
        _validate(data)
    data = copy.deepcopy(raw_config)
    data["sources"]["reddit"]["enabled"] = True
    with pytest.raises(Exception, match="REDDIT_CLIENT_ID"):
        _validate(data)


def test_reference_price_fallbacks() -> None:
    band = PriceBand(reference_new=1000, floor=100, target=700, ceiling=900)
    assert band.reference_for("new") == 1000
    assert band.reference_for("used") == pytest.approx(800)
    assert band.reference_for("refurb") == pytest.approx(900)
    band = PriceBand(reference_used=800, floor=100, target=600, ceiling=900)
    assert band.reference_for("new") == pytest.approx(1000)
    with pytest.raises(Exception, match="at least one reference"):
        PriceBand(floor=1, target=2, ceiling=3)


def test_load_config_error_messages(tmp_path: Path) -> None:
    missing = tmp_path / "nope.yaml"
    with pytest.raises(ConfigError, match="cannot read"):
        load_config(missing)
    bad = tmp_path / "bad.yaml"
    bad.write_text("app: [unclosed", encoding="utf-8")
    with pytest.raises(ConfigError, match="invalid YAML"):
        load_config(bad)
    scalar = tmp_path / "scalar.yaml"
    scalar.write_text("42", encoding="utf-8")
    with pytest.raises(ConfigError, match="mapping"):
        load_config(scalar)
    invalid = tmp_path / "invalid.yaml"
    invalid.write_text("version: 1\nstorage:\n  database_url: mysql://x\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="storage.database_url"):
        load_config(invalid, env={})
