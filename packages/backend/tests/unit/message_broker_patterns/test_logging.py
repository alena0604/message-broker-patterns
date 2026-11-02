import json
import logging
from collections.abc import Generator
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from message_broker_patterns.config.settings import Settings, settings
from message_broker_patterns.logging import TEXT_FORMAT, JsonLogFormatter, init_logger

DEFAULT_TEXT_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


@pytest.fixture()
def isolated_root_logger() -> Generator[None, None, None]:
    """Snapshot/restore root handlers + level.

    ``init_logger`` calls ``basicConfig(force=True)``, which tears down whatever
    handlers pytest installed. Without this fixture that teardown leaks into
    unrelated tests.
    """
    handlers = logging.root.handlers[:]
    level = logging.root.level
    yield
    logging.root.handlers[:] = handlers
    logging.root.setLevel(level)


def _record(
    msg: str = "hello",
    args: tuple[object, ...] = (),
    *,
    level: int = logging.INFO,
    name: str = "message_broker_patterns.demo",
    exc_info: tuple | None = None,
) -> logging.LogRecord:
    return logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=42,
        msg=msg,
        args=args,
        exc_info=exc_info,
    )


def _boom_exc_info() -> tuple:
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        return sys.exc_info()


def _root_formatter() -> logging.Formatter:
    assert len(logging.root.handlers) == 1, logging.root.handlers
    formatter = logging.root.handlers[0].formatter
    assert formatter is not None
    return formatter


# --- JsonLogFormatter ---------------------------------------------------------


def test_json_formatter_emits_exact_four_keys_in_order() -> None:
    # Arrange
    record = _record("relayed 3 entries", name="message_broker_patterns.outbox_pattern.relay")

    # Act
    parsed = json.loads(JsonLogFormatter().format(record))

    # Assert
    assert list(parsed) == ["timestamp", "level", "logger", "message"]
    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "message_broker_patterns.outbox_pattern.relay"
    assert parsed["message"] == "relayed 3 entries"


def test_json_formatter_timestamp_is_timezone_aware_utc() -> None:
    # Arrange
    record = _record()

    # Act
    parsed = json.loads(JsonLogFormatter().format(record))
    stamp = datetime.fromisoformat(parsed["timestamp"])

    # Assert
    assert stamp.tzinfo is not None
    assert stamp.utcoffset() == UTC.utcoffset(None)


def test_json_formatter_interpolates_percent_args() -> None:
    # Arrange
    record = _record("consumed %d", (5,))

    # Act
    parsed = json.loads(JsonLogFormatter().format(record))

    # Assert
    assert parsed["message"] == "consumed 5"


def test_json_formatter_renders_exception_on_one_line() -> None:
    # Arrange
    record = _record("handler failed", level=logging.ERROR, exc_info=_boom_exc_info())

    # Act
    rendered = JsonLogFormatter().format(record)
    parsed = json.loads(rendered)

    # Assert
    assert "\n" not in rendered
    assert "ValueError" in parsed["exception"]
    assert "boom" in parsed["exception"]


def test_json_formatter_omits_exception_key_when_absent() -> None:
    # Arrange
    record = _record()

    # Act
    parsed = json.loads(JsonLogFormatter().format(record))

    # Assert
    assert "exception" not in parsed


def test_json_formatter_round_trips_newlines_and_quotes() -> None:
    # Arrange
    message = 'line one\nline "two"'
    record = _record(message)

    # Act
    rendered = JsonLogFormatter().format(record)
    parsed = json.loads(rendered)

    # Assert
    assert "\n" not in rendered
    assert parsed["message"] == message


def test_json_formatter_keeps_non_ascii_unescaped() -> None:
    # Arrange
    record = _record("café")

    # Act
    rendered = JsonLogFormatter().format(record)

    # Assert
    assert "café" in rendered
    assert "\\u" not in rendered
    assert json.loads(rendered)["message"] == "café"


# --- init_logger --------------------------------------------------------------


def test_init_logger_defaults_to_text_formatter(
    isolated_root_logger: None, mocker: MockerFixture
) -> None:
    # Arrange — LOG_FORMAT unset means settings keeps its "text" default.
    mocker.patch.object(settings, "log_format", "text")

    # Act
    init_logger()

    # Assert
    formatter = _root_formatter()
    assert not isinstance(formatter, JsonLogFormatter)
    assert formatter._style._fmt == DEFAULT_TEXT_FORMAT


def test_init_logger_json_installs_json_formatter(isolated_root_logger: None) -> None:
    # Arrange / Act
    init_logger(fmt="json")

    # Assert
    assert isinstance(_root_formatter(), JsonLogFormatter)


def test_init_logger_reads_log_format_setting(
    isolated_root_logger: None, mocker: MockerFixture
) -> None:
    # Arrange
    mocker.patch.object(settings, "log_format", "json")

    # Act
    init_logger()

    # Assert
    assert isinstance(_root_formatter(), JsonLogFormatter)


def test_init_logger_explicit_fmt_beats_setting(
    isolated_root_logger: None, mocker: MockerFixture
) -> None:
    # Arrange
    mocker.patch.object(settings, "log_format", "json")

    # Act
    init_logger(fmt="text")

    # Assert
    formatter = _root_formatter()
    assert not isinstance(formatter, JsonLogFormatter)
    assert formatter._style._fmt == DEFAULT_TEXT_FORMAT


@pytest.mark.parametrize(
    ("level", "expected"),
    [("WARNING", logging.WARNING), ("ERROR", logging.ERROR), ("INFO", logging.INFO)],
)
def test_init_logger_sets_root_level(
    isolated_root_logger: None, mocker: MockerFixture, level: str, expected: int
) -> None:
    # Arrange
    mocker.patch.object(settings, "log_format", "text")

    # Act
    init_logger(level)

    # Assert
    assert logging.root.level == expected


def test_init_logger_last_call_wins(isolated_root_logger: None) -> None:
    # Arrange
    init_logger(fmt="json")

    # Act
    init_logger(fmt="text")

    # Assert
    assert not isinstance(_root_formatter(), JsonLogFormatter)


def test_text_format_constant_matches_documented_default() -> None:
    # Assert
    assert TEXT_FORMAT == DEFAULT_TEXT_FORMAT


# --- settings -----------------------------------------------------------------


def test_settings_log_format_defaults_to_text(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.delenv("LOG_FORMAT", raising=False)

    # Act
    loaded = Settings(_env_file=None)

    # Assert
    assert loaded.log_format == "text"


def test_settings_rejects_invalid_log_format(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setenv("LOG_FORMAT", "xml")

    # Act / Assert
    with pytest.raises(ValidationError, match="log_format"):
        Settings(_env_file=None)
