"""Structured logging: single-line JSON for production, readable text for terminals.

Any ``extra={...}`` keys passed to a logging call are emitted as top-level JSON fields,
so log processors (Cloud Logging, Loki) can index ``source``, ``profile``, ``score``...
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

_RESERVED = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName", "levelname",
        "levelno", "lineno", "message", "module", "msecs", "msg", "name", "pathname", "process",
        "processName", "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    }
)


class JsonFormatter(logging.Formatter):
    def __init__(self, node_id: str | None = None) -> None:
        super().__init__()
        self.node_id = node_id

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if self.node_id:
            payload["node"] = self.node_id
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-7s %(name)s: %(message)s", "%H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = {k: v for k, v in record.__dict__.items() if k not in _RESERVED and not k.startswith("_")}
        if extras:
            base += " " + " ".join(f"{k}={v}" for k, v in extras.items())
        return base


def configure_logging(level: str = "INFO", *, json_output: bool = True, node_id: str | None = None) -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(node_id) if json_output else TextFormatter())
    root.addHandler(handler)
    root.setLevel(level.upper())
    # Third-party libraries are chatty at INFO; keep them at WARNING unless debugging.
    if level.upper() != "DEBUG":
        for noisy in ("aiohttp.access", "asyncio", "sqlalchemy.engine", "urllib3"):
            logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name if name.startswith("deal_radar") else f"deal_radar.{name}")


__all__ = ["JsonFormatter", "TextFormatter", "configure_logging", "get_logger"]
