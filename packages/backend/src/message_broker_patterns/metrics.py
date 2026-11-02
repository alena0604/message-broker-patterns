"""Thread-safe in-memory metrics registry shared across pattern demos.

Patterns increment named counters; the demo metrics HTTP server snapshots the
registry and serves it as JSON. Intentionally process-local and dependency-free
— this is an educational instrumentation layer, not a production metrics system.
"""

import logging
import re
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

METRIC_PREFIX = "mbp_"
FRESHNESS_METRIC = "mbp_pattern_last_updated_timestamp_seconds"
FRESHNESS_HELP = "Unix timestamp of the last counter update for the pattern."

_INVALID_NAME_CHARS = re.compile(r"[^a-zA-Z0-9_]")


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class PatternMetrics:
    """Counters and metadata for a single messaging pattern."""

    label: str
    counters: dict[str, int] = field(default_factory=dict)
    last_updated: str = field(default_factory=_now_iso)


class MetricsRegistry:
    """Thread-safe registry of per-pattern counters."""

    def __init__(self) -> None:
        self._patterns: dict[str, PatternMetrics] = {}
        self._lock = threading.Lock()

    def register(self, pattern_id: str, label: str) -> PatternMetrics:
        """Register a pattern, returning the existing entry if already present."""
        with self._lock:
            existing = self._patterns.get(pattern_id)
            if existing is not None:
                return existing
            metrics = PatternMetrics(label=label)
            self._patterns[pattern_id] = metrics
            return metrics

    def increment(self, pattern_id: str, key: str, amount: int = 1) -> None:
        """Increment a counter, creating the pattern entry if it is missing."""
        with self._lock:
            metrics = self._patterns.get(pattern_id)
            if metrics is None:
                metrics = PatternMetrics(label=pattern_id)
                self._patterns[pattern_id] = metrics
            metrics.counters[key] = metrics.counters.get(key, 0) + amount
            metrics.last_updated = _now_iso()

    def snapshot(self) -> list[dict]:
        """Return a JSON-serialisable snapshot of all registered patterns."""
        with self._lock:
            return [
                {
                    "id": pattern_id,
                    "label": metrics.label,
                    "counters": dict(metrics.counters),
                    "last_updated": metrics.last_updated,
                }
                for pattern_id, metrics in self._patterns.items()
            ]


REGISTRY = MetricsRegistry()


def _sanitise_metric_name(key: str) -> str:
    """Coerce a counter key into Prometheus' ``[a-zA-Z_:][a-zA-Z0-9_:]*`` grammar."""
    sanitised = _INVALID_NAME_CHARS.sub("_", key)
    if sanitised[:1].isdigit():
        sanitised = f"_{sanitised}"
    return sanitised


def _counter_metric_name(key: str) -> str:
    """``mbp_<sanitised key>_total`` — never double-suffixed."""
    sanitised = _sanitise_metric_name(key)
    if sanitised.endswith("_total"):
        return f"{METRIC_PREFIX}{sanitised}"
    return f"{METRIC_PREFIX}{sanitised}_total"


def _escape_label_value(value: str) -> str:
    """Escape a label value per the exposition spec: ``\\``, ``"`` and newline."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _escape_help(text: str) -> str:
    """Escape HELP text: only ``\\`` and newline are special there."""
    return text.replace("\\", "\\\\").replace("\n", "\\n")


def _freshness_seconds(pattern_id: str, last_updated: object) -> float | None:
    """Unix seconds for a snapshot's ``last_updated``, or ``None`` plus a warning.

    A malformed timestamp must never take a whole scrape down, so the caller
    drops that one gauge sample and keeps rendering every other pattern.
    """
    if isinstance(last_updated, str):
        try:
            return datetime.fromisoformat(last_updated).timestamp()
        except (ValueError, OverflowError, OSError):
            pass
    logger.warning(
        "Skipping freshness gauge for pattern %r: unparseable last_updated %r",
        pattern_id,
        last_updated,
    )
    return None


def render_prometheus(snapshot: list[dict]) -> str:
    """Render a registry snapshot in Prometheus text exposition format.

    Pure by design: the snapshot is an argument, never read from ``REGISTRY``,
    so the renderer is unit-testable against synthetic snapshots. Counters
    become ``mbp_<key>_total{pattern="<id>"}``; every pattern also gets an
    ``mbp_pattern_last_updated_timestamp_seconds`` gauge, which is the only
    signal that catches a loop that died with plausible counters frozen.
    """
    # (metric name) -> (pattern id) -> value. Accumulating into a dict is what
    # makes two keys that sanitise alike sum into one sample rather than emit a
    # duplicate — repeated ``name{labels}`` lines are invalid exposition.
    counters: dict[str, dict[str, int]] = {}
    raw_keys: dict[str, set[str]] = {}
    gauges: dict[str, float] = {}

    for entry in snapshot:
        pattern_id = str(entry.get("id", ""))
        for key, value in (entry.get("counters") or {}).items():
            name = _counter_metric_name(key)
            samples = counters.setdefault(name, {})
            samples[pattern_id] = samples.get(pattern_id, 0) + int(value)
            raw_keys.setdefault(name, set()).add(key)
        seconds = _freshness_seconds(pattern_id, entry.get("last_updated"))
        if seconds is not None:
            gauges[pattern_id] = seconds

    blocks: dict[str, tuple[str, str, dict[str, str]]] = {
        name: (
            f'Counter "{min(raw_keys[name])}" reported by message-broker-patterns pattern demos.',
            "counter",
            {pattern_id: str(value) for pattern_id, value in samples.items()},
        )
        for name, samples in counters.items()
    }
    if gauges:
        blocks[FRESHNESS_METRIC] = (
            FRESHNESS_HELP,
            "gauge",
            {pattern_id: repr(value) for pattern_id, value in gauges.items()},
        )

    lines: list[str] = []
    for name in sorted(blocks):
        help_text, metric_type, samples = blocks[name]
        lines.append(f"# HELP {name} {_escape_help(help_text)}")
        lines.append(f"# TYPE {name} {metric_type}")
        lines.extend(
            f'{name}{{pattern="{_escape_label_value(pattern_id)}"}} {samples[pattern_id]}'
            for pattern_id in sorted(samples)
        )

    if not lines:
        return ""
    return "\n".join(lines) + "\n"
