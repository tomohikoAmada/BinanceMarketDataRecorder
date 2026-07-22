"""Small structured JSON logging foundation."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import IO, Any

LOGGER_NAME = "binance_market_data_recorder"


class JsonFormatter(logging.Formatter):
    """Render one stable JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp_utc": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": getattr(record, "structured_event", "log"),
            "message": record.getMessage(),
        }
        fields = getattr(record, "structured_fields", None)
        if isinstance(fields, dict):
            payload["fields"] = fields
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def configure_logging(level: str = "INFO", *, stream: IO[str] | None = None) -> logging.Logger:
    """Configure the project logger without mutating the root logger."""

    logger = logging.getLogger(LOGGER_NAME)
    logger.handlers.clear()
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    message: str,
    **fields: object,
) -> None:
    """Emit a structured event through standard logging."""

    logger.log(
        level,
        message,
        extra={"structured_event": event, "structured_fields": fields},
    )
