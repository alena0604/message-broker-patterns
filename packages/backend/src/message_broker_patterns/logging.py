import json
import logging
from datetime import UTC, datetime
from typing import Literal

TEXT_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


class JsonLogFormatter(logging.Formatter):
    """Render a log record as a single-line JSON object.

    Keys are always emitted in the order ``timestamp``, ``level``, ``logger``,
    ``message`` — plus ``exception`` when the record carries ``exc_info``.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, str] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def init_logger(level: str = "INFO", fmt: Literal["text", "json"] | None = None) -> None:
    """Configure root logging.

    ``fmt=None`` reads ``settings.log_format`` (env var ``LOG_FORMAT``); an
    explicit ``fmt`` wins over it. ``settings`` is imported lazily because every
    script imports this module before any other project module.
    """
    from message_broker_patterns.config.settings import settings

    resolved = fmt if fmt is not None else settings.log_format
    formatter = JsonLogFormatter() if resolved == "json" else logging.Formatter(TEXT_FORMAT)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=[handler],
        force=True,
    )
