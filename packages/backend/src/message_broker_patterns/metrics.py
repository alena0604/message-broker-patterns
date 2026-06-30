"""Thread-safe in-memory metrics registry shared across pattern demos.

Patterns increment named counters; the demo metrics HTTP server snapshots the
registry and serves it as JSON. Intentionally process-local and dependency-free
— this is an educational instrumentation layer, not a production metrics system.
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


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
