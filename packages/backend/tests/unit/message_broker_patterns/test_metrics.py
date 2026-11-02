import logging
import threading
from datetime import datetime

import pytest

from message_broker_patterns.metrics import (
    REGISTRY,
    MetricsRegistry,
    PatternMetrics,
    render_prometheus,
)

STAMP = "2026-08-31T19:00:00.123456+00:00"


def _entry(
    pattern_id: str,
    label: str,
    counters: dict[str, int],
    last_updated: str = STAMP,
) -> dict:
    return {
        "id": pattern_id,
        "label": label,
        "counters": counters,
        "last_updated": last_updated,
    }


def _help_names(rendered: str) -> list[str]:
    return [line.split()[2] for line in rendered.splitlines() if line.startswith("# HELP ")]


def _type_names(rendered: str) -> list[str]:
    return [line.split()[2] for line in rendered.splitlines() if line.startswith("# TYPE ")]


def _sample_lines(rendered: str) -> list[str]:
    return [line for line in rendered.splitlines() if not line.startswith("#") and line]


def _sample_series(rendered: str) -> list[str]:
    """Return the ``name{labels}`` part of every sample line (value stripped)."""
    return [line.rsplit(" ", 1)[0] for line in _sample_lines(rendered)]


def _sample_value(rendered: str, series: str) -> str:
    values = [line.rsplit(" ", 1)[1] for line in _sample_lines(rendered) if line.startswith(series)]
    assert len(values) == 1, f"expected exactly one sample for {series}, got {values}"
    return values[0]


ALL_PATTERNS_SNAPSHOT = [
    _entry(
        "competing_consumers",
        "Competing Consumers",
        {"messages_published": 15, "messages_consumed": 12, "consumer_joins": 2},
    ),
    _entry(
        "event_sourcing",
        "Event Sourcing",
        {"events_appended": 9, "read_model_events_applied": 9},
    ),
    _entry(
        "transactional_outbox",
        "Transactional Outbox",
        {"orders_inserted": 4, "entries_relayed": 4},
    ),
    _entry(
        "saga",
        "Saga (Choreography)",
        {"sagas_started": 3, "sagas_completed": 2, "sagas_cancelled": 1},
    ),
    _entry(
        "priority_queue",
        "Priority Queue",
        {
            "tickets_published": 6,
            "tickets_consumed": 6,
            "high_published": 2,
            "high_consumed": 2,
            "normal_published": 2,
            "normal_consumed": 2,
            "low_published": 2,
            "low_consumed": 2,
        },
    ),
    _entry(
        "scatter_gather",
        "Scatter-Gather",
        {
            "requests_scattered": 5,
            "messages_published": 15,
            "responses_published": 10,
            "responses_gathered": 10,
            "partial_gathers": 5,
            "service_failures": 5,
        },
    ),
    _entry(
        "dlq",
        "Dead Letter Queue",
        {
            "payments_published": 8,
            "payments_processed": 6,
            "moved_to_dlq": 2,
            "skipped_duplicate": 1,
        },
    ),
    _entry(
        "claim_check",
        "Claim Check",
        {
            "payloads_stored": 3,
            "claim_checks_published": 3,
            "payloads_retrieved": 3,
            "payloads_processed": 3,
            "payloads_deleted": 3,
            "broker_bytes_saved": 300_000,
        },
    ),
]


def test_register_is_idempotent() -> None:
    registry = MetricsRegistry()

    first = registry.register("competing_consumers", "Competing Consumers")
    second = registry.register("competing_consumers", "Different Label")

    assert first is second
    assert isinstance(first, PatternMetrics)
    assert first.label == "Competing Consumers"


def test_increment_creates_missing_entry() -> None:
    registry = MetricsRegistry()

    registry.increment("saga", "sagas_started")

    snapshot = registry.snapshot()
    assert len(snapshot) == 1
    assert snapshot[0]["id"] == "saga"
    assert snapshot[0]["counters"]["sagas_started"] == 1


def test_increment_accumulates() -> None:
    registry = MetricsRegistry()
    registry.register("saga", "Saga")

    registry.increment("saga", "sagas_started")
    registry.increment("saga", "sagas_started", amount=4)

    snapshot = registry.snapshot()
    assert snapshot[0]["counters"]["sagas_started"] == 5


def test_snapshot_structure() -> None:
    registry = MetricsRegistry()
    registry.register("event_sourcing", "Event Sourcing")
    registry.increment("event_sourcing", "events_appended", amount=2)

    snapshot = registry.snapshot()

    assert isinstance(snapshot, list)
    entry = snapshot[0]
    assert set(entry.keys()) == {"id", "label", "counters", "last_updated"}
    assert entry["id"] == "event_sourcing"
    assert entry["label"] == "Event Sourcing"
    assert entry["counters"] == {"events_appended": 2}
    assert isinstance(entry["last_updated"], str)
    # ISO-format UTC string ends with +00:00
    assert entry["last_updated"].endswith("+00:00")


def test_concurrent_increments_are_thread_safe() -> None:
    registry = MetricsRegistry()
    registry.register("competing_consumers", "Competing Consumers")

    thread_count = 16
    increments_per_thread = 1000

    def worker() -> None:
        for _ in range(increments_per_thread):
            registry.increment("competing_consumers", "messages_consumed")

    threads = [threading.Thread(target=worker) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    snapshot = registry.snapshot()
    assert snapshot[0]["counters"]["messages_consumed"] == thread_count * increments_per_thread


def test_render_prometheus_ignores_module_registry() -> None:
    REGISTRY.register("registry_only_pattern", "Registry Only")
    REGISTRY.increment("registry_only_pattern", "registry_only_counter")

    rendered = render_prometheus([_entry("dlq", "Dead Letter Queue", {"moved_to_dlq": 3})])

    assert "registry_only" not in rendered
    assert 'mbp_moved_to_dlq_total{pattern="dlq"} 3' in rendered


def test_render_prometheus_matches_documented_example() -> None:
    rendered = render_prometheus([_entry("dlq", "Dead Letter Queue", {"moved_to_dlq": 3})])

    expected_gauge = datetime.fromisoformat(STAMP).timestamp()
    assert rendered == (
        '# HELP mbp_moved_to_dlq_total Counter "moved_to_dlq" reported by '
        "message-broker-patterns pattern demos.\n"
        "# TYPE mbp_moved_to_dlq_total counter\n"
        'mbp_moved_to_dlq_total{pattern="dlq"} 3\n'
        "# HELP mbp_pattern_last_updated_timestamp_seconds Unix timestamp of the last "
        "counter update for the pattern.\n"
        "# TYPE mbp_pattern_last_updated_timestamp_seconds gauge\n"
        f'mbp_pattern_last_updated_timestamp_seconds{{pattern="dlq"}} {expected_gauge!r}\n'
    )


def test_render_prometheus_all_patterns_has_one_help_and_type_per_metric() -> None:
    rendered = render_prometheus(ALL_PATTERNS_SNAPSHOT)

    help_names = _help_names(rendered)
    type_names = _type_names(rendered)
    assert help_names == sorted(set(help_names))
    assert type_names == help_names
    series = _sample_series(rendered)
    assert len(series) == len(set(series)), "duplicate name{labels} sample lines"
    assert "mbp_pattern_last_updated_timestamp_seconds" in help_names


def test_render_prometheus_shared_counter_key_shares_one_block() -> None:
    rendered = render_prometheus(ALL_PATTERNS_SNAPSHOT)

    assert _help_names(rendered).count("mbp_messages_published_total") == 1
    assert _type_names(rendered).count("mbp_messages_published_total") == 1
    assert 'mbp_messages_published_total{pattern="competing_consumers"} 15' in rendered
    assert 'mbp_messages_published_total{pattern="scatter_gather"} 15' in rendered


@pytest.mark.parametrize(
    ("counter_key", "expected_name"),
    [
        ("messages-published", "mbp_messages_published_total"),
        ("messages.published", "mbp_messages_published_total"),
        ("weird key!", "mbp_weird_key__total"),
        ("2fast", "mbp__2fast_total"),
        ("retries_total", "mbp_retries_total"),
    ],
)
def test_render_prometheus_sanitises_counter_names(counter_key: str, expected_name: str) -> None:
    rendered = render_prometheus([_entry("dlq", "Dead Letter Queue", {counter_key: 7})])

    assert f'{expected_name}{{pattern="dlq"}} 7' in rendered
    assert f"# TYPE {expected_name} counter" in rendered


def test_render_prometheus_merges_colliding_keys_into_one_sample() -> None:
    rendered = render_prometheus([_entry("dlq", "Dead Letter Queue", {"a-b": 2, "a.b": 5})])

    series = _sample_series(rendered)
    assert series.count('mbp_a_b_total{pattern="dlq"}') == 1
    assert _sample_value(rendered, 'mbp_a_b_total{pattern="dlq"}') == "7"


def test_render_prometheus_escapes_label_values() -> None:
    rendered = render_prometheus([_entry('we"ird\\id', "Weird", {"messages_published": 1})])

    assert 'mbp_messages_published_total{pattern="we\\"ird\\\\id"} 1' in rendered


def test_render_prometheus_emits_gauge_for_zero_counter_pattern() -> None:
    rendered = render_prometheus([_entry("saga", "Saga (Choreography)", {})])

    expected_gauge = datetime.fromisoformat(STAMP).timestamp()
    assert _sample_lines(rendered) == [
        f'mbp_pattern_last_updated_timestamp_seconds{{pattern="saga"}} {expected_gauge!r}'
    ]
    assert "_total" not in rendered


def test_render_prometheus_skips_unparseable_last_updated(caplog: pytest.LogCaptureFixture) -> None:
    snapshot = [
        _entry("dlq", "Dead Letter Queue", {"moved_to_dlq": 1}, last_updated="not-a-timestamp"),
        _entry("saga", "Saga (Choreography)", {"sagas_started": 2}),
    ]

    with caplog.at_level(logging.WARNING, logger="message_broker_patterns.metrics"):
        rendered = render_prometheus(snapshot)

    assert 'mbp_moved_to_dlq_total{pattern="dlq"} 1' in rendered
    assert 'mbp_sagas_started_total{pattern="saga"} 2' in rendered
    assert 'mbp_pattern_last_updated_timestamp_seconds{pattern="dlq"}' not in rendered
    assert 'mbp_pattern_last_updated_timestamp_seconds{pattern="saga"}' in rendered
    assert any(record.levelno == logging.WARNING for record in caplog.records)


def test_render_prometheus_is_deterministic_and_sorted() -> None:
    first = render_prometheus(ALL_PATTERNS_SNAPSHOT)
    second = render_prometheus(ALL_PATTERNS_SNAPSHOT)

    assert first == second
    assert _help_names(first) == sorted(_help_names(first))
    published = [
        line for line in _sample_lines(first) if line.startswith("mbp_messages_published_total{")
    ]
    assert published == sorted(published)


def test_render_prometheus_counter_values_render_as_integers() -> None:
    rendered = render_prometheus([_entry("dlq", "Dead Letter Queue", {"moved_to_dlq": 3})])

    assert _sample_value(rendered, 'mbp_moved_to_dlq_total{pattern="dlq"}') == "3"


def test_render_prometheus_ends_with_trailing_newline() -> None:
    rendered = render_prometheus(ALL_PATTERNS_SNAPSHOT)

    assert rendered.endswith("\n")
    assert not rendered.endswith("\n\n")


def test_render_prometheus_empty_snapshot_is_empty_string() -> None:
    assert render_prometheus([]) == ""
