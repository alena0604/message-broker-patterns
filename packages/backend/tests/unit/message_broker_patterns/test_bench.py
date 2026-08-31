import asyncio
import csv
import io
import json
import re

import pytest
from pytest_mock import MockerFixture

from message_broker_patterns.bench import (
    BenchResult,
    DepthSample,
    LatencySummary,
    Window,
    add_bench_args,
    emit_json,
    latency_csv,
    new_run_id,
    percentile,
    queue_depth_csv,
    run_bench,
    show_bench_progress,
    write_csv_files,
)

MODULE = "message_broker_patterns.bench"


class FakeClock:
    """A patchable stand-in for :func:`bench.now` — time only moves when told to.

    Lets the windowing, throughput and percentile assertions be exact instead of
    approximate, the same trick ``chaos.sleep`` enables for latency tests.
    """

    def __init__(self, start: float = 0.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture()
def clock(mocker: MockerFixture) -> FakeClock:
    fake = FakeClock()
    mocker.patch(f"{MODULE}.now", fake)
    return fake


# --- percentile ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("quantile", "expected"),
    [(50, 50.0), (95, 95.0), (99, 99.0), (100, 100.0), (0, 1.0)],
)
def test_percentile_uses_nearest_rank(quantile: float, expected: float) -> None:
    values = [float(n) for n in range(1, 101)]

    assert percentile(values, quantile) == expected


def test_percentile_sorts_its_input() -> None:
    assert percentile([9.0, 1.0, 5.0], 50) == 5.0


def test_percentile_of_single_value_is_that_value() -> None:
    assert percentile([4.2], 99) == 4.2


def test_percentile_of_empty_sequence_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        percentile([], 50)


@pytest.mark.parametrize("quantile", [-1, 101])
def test_percentile_rejects_out_of_range_quantile(quantile: float) -> None:
    with pytest.raises(ValueError, match="between 0 and 100"):
        percentile([1.0], quantile)


# --- LatencySummary -----------------------------------------------------------


def test_latency_summary_converts_seconds_to_milliseconds() -> None:
    summary = LatencySummary.from_seconds([0.001, 0.002, 0.003, 0.004])

    assert summary.count == 4
    assert summary.min_ms == 1.0
    assert summary.max_ms == 4.0
    assert summary.mean_ms == 2.5
    assert summary.p50_ms == 2.0


def test_latency_summary_of_no_samples_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        LatencySummary.from_seconds([])


# --- Window -------------------------------------------------------------------


def test_window_rejects_both_knobs() -> None:
    with pytest.raises(ValueError, match="ops or seconds, not both"):
        Window(ops=10, seconds=1.0)


def test_window_rejects_neither_knob() -> None:
    with pytest.raises(ValueError, match="configure a window"):
        Window()


@pytest.mark.parametrize("ops", [0, -1])
def test_window_rejects_non_positive_ops(ops: int) -> None:
    with pytest.raises(ValueError, match="ops must be"):
        Window.of_ops(ops)


@pytest.mark.parametrize("seconds", [0.0, -1.0, float("inf"), float("nan")])
def test_window_rejects_non_finite_or_non_positive_seconds(seconds: float) -> None:
    with pytest.raises(ValueError, match="seconds must be"):
        Window.of_seconds(seconds)


def test_ops_window_closes_on_count_only() -> None:
    window = Window.of_ops(3)

    assert window.is_done(2, elapsed=9_999.0) is False
    assert window.is_done(3, elapsed=0.0) is True


def test_duration_window_closes_on_elapsed_only() -> None:
    window = Window.of_seconds(1.5)

    assert window.is_done(9_999, elapsed=1.4) is False
    assert window.is_done(0, elapsed=1.5) is True


# --- run_bench ----------------------------------------------------------------


async def test_run_bench_excludes_warmup_from_measured_window(clock: FakeClock) -> None:
    calls = 0

    async def operation() -> None:
        nonlocal calls
        calls += 1
        clock.advance(0.1 if calls <= 2 else 0.2)

    result = await run_bench("demo", operation, warmup=Window.of_ops(2), measure=Window.of_ops(3))

    assert result.warmup_ops == 2
    assert result.ops == 3
    assert result.latency.count == 3
    assert result.latency.p50_ms == pytest.approx(200.0)
    assert result.duration_s == pytest.approx(0.6)


async def test_run_bench_computes_throughput_over_the_measured_window(clock: FakeClock) -> None:
    async def operation() -> None:
        clock.advance(0.2)

    result = await run_bench("demo", operation, warmup=Window.of_ops(2), measure=Window.of_ops(3))

    assert result.throughput_ops_s == pytest.approx(5.0)


async def test_run_bench_runs_without_a_warmup(clock: FakeClock) -> None:
    async def operation() -> None:
        clock.advance(0.05)

    result = await run_bench("demo", operation, measure=Window.of_ops(2))

    assert result.warmup_ops == 0
    assert result.ops == 2
    assert result.duration_s == pytest.approx(0.1)


async def test_run_bench_duration_window_stops_on_elapsed_time(clock: FakeClock) -> None:
    async def operation() -> None:
        clock.advance(0.1)

    result = await run_bench("demo", operation, measure=Window.of_seconds(0.25))

    assert result.ops == 3


async def test_run_bench_echoes_the_knobs_it_was_given(clock: FakeClock) -> None:
    async def operation() -> None:
        clock.advance(0.1)

    result = await run_bench(
        "demo", operation, measure=Window.of_ops(1), knobs={"poll_interval": 0.5}
    )

    assert result.knobs == {"poll_interval": 0.5}
    assert result.extra == {}


async def test_run_bench_keeps_concurrency_operations_in_flight() -> None:
    in_flight = 0
    peak = 0

    async def operation() -> None:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1

    result = await run_bench("demo", operation, measure=Window.of_ops(10), concurrency=5)

    assert result.ops == 10
    assert peak == 5


async def test_run_bench_rejects_non_positive_concurrency() -> None:
    async def operation() -> None:
        return None

    with pytest.raises(ValueError, match="concurrency must be"):
        await run_bench("demo", operation, measure=Window.of_ops(1), concurrency=0)


async def test_run_bench_propagates_an_operation_failure() -> None:
    async def operation() -> None:
        raise ValueError("workload broke")

    with pytest.raises(ValueError, match="workload broke"):
        await run_bench("demo", operation, measure=Window.of_ops(1))


async def test_run_bench_samples_queue_depth_during_the_measured_window() -> None:
    depths = [3, 2, 1, 0]
    sampled: list[int] = []
    sampled_during_warmup: list[int] = []

    async def sampler() -> int:
        depth = depths[min(len(sampled), len(depths) - 1)]
        sampled.append(depth)
        return depth

    async def warmup_operation() -> None:
        sampled_during_warmup.append(len(sampled))
        await asyncio.sleep(0.001)

    async def operation() -> None:
        await asyncio.sleep(0.02)

    calls = 0

    async def dispatch() -> None:
        nonlocal calls
        calls += 1
        await (warmup_operation() if calls <= 2 else operation())

    result = await run_bench(
        "demo",
        dispatch,
        warmup=Window.of_ops(2),
        measure=Window.of_ops(4),
        sampler=sampler,
        sample_interval=0.01,
    )

    assert sampled_during_warmup == [0, 0]
    assert len(result.queue_depth) >= 2
    assert all(isinstance(sample, DepthSample) for sample in result.queue_depth)
    elapsed = [sample.elapsed_s for sample in result.queue_depth]
    assert elapsed == sorted(elapsed)
    assert result.queue_depth[-1].elapsed_s == pytest.approx(result.duration_s, abs=0.05)


async def test_run_bench_without_a_sampler_records_no_depth_series(clock: FakeClock) -> None:
    async def operation() -> None:
        clock.advance(0.01)

    result = await run_bench("demo", operation, measure=Window.of_ops(1))

    assert result.queue_depth == []


# --- emission -----------------------------------------------------------------


@pytest.fixture()
def result() -> BenchResult:
    return BenchResult(
        name="outbox",
        knobs={"poll_interval": 0.05},
        warmup_ops=2,
        ops=4,
        duration_s=0.5,
        throughput_ops_s=8.0,
        latency=LatencySummary.from_seconds([0.001, 0.002, 0.003, 0.004]),
        latencies_s=[0.001, 0.002, 0.003, 0.004],
        queue_depth=[DepthSample(0.0, 3), DepthSample(0.25, 1)],
        extra={"lost": 0},
    )


def test_emit_json_writes_one_object_for_one_result(result: BenchResult) -> None:
    stream = io.StringIO()

    emit_json(result, stream=stream)

    payload = json.loads(stream.getvalue())
    assert payload["name"] == "outbox"
    assert payload["knobs"] == {"poll_interval": 0.05}
    assert payload["warmup_ops"] == 2
    assert payload["ops"] == 4
    assert payload["throughput_ops_s"] == 8.0
    assert payload["latency_ms"]["p99"] == 4.0
    assert payload["queue_depth"] == [
        {"elapsed_s": 0.0, "depth": 3},
        {"elapsed_s": 0.25, "depth": 1},
    ]
    assert payload["extra"] == {"lost": 0}


def test_emit_json_writes_an_array_for_a_sweep(result: BenchResult) -> None:
    stream = io.StringIO()

    emit_json([result, result], stream=stream)

    payload = json.loads(stream.getvalue())
    assert isinstance(payload, list)
    assert len(payload) == 2


def test_emit_json_ends_with_a_newline(result: BenchResult) -> None:
    stream = io.StringIO()

    emit_json(result, stream=stream)

    assert stream.getvalue().endswith("\n")


def test_latency_csv_has_a_header_and_one_row_per_operation(result: BenchResult) -> None:
    rows = list(csv.reader(io.StringIO(latency_csv(result))))

    assert rows[0] == ["op", "latency_ms"]
    assert len(rows) == 5
    assert rows[1] == ["1", "1.0"]


def test_queue_depth_csv_has_a_header_and_one_row_per_sample(result: BenchResult) -> None:
    rows = list(csv.reader(io.StringIO(queue_depth_csv(result))))

    assert rows[0] == ["elapsed_s", "depth"]
    assert rows[1] == ["0.0", "3"]
    assert len(rows) == 3


def test_write_csv_files_writes_a_latency_and_a_depth_file(result: BenchResult, tmp_path) -> None:
    paths = write_csv_files([result], tmp_path)

    assert sorted(path.name for path in paths) == [
        "outbox_poll-interval-0.05_depth.csv",
        "outbox_poll-interval-0.05_latency.csv",
    ]
    assert paths[0].read_text().startswith("op,latency_ms")


def test_add_bench_args_registers_the_shared_knobs() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    add_bench_args(parser, ops=100, warmup=10, concurrency=4)

    args = parser.parse_args([])
    assert args.ops == 100
    assert args.warmup == 10
    assert args.concurrency == 4
    assert args.csv_dir is None


def test_add_bench_args_lets_the_operator_override_them() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    add_bench_args(parser, ops=100, warmup=10, concurrency=4)

    args = parser.parse_args(["--ops", "7", "--warmup", "1", "--concurrency", "2"])
    assert (args.ops, args.warmup, args.concurrency) == (7, 1, 2)


def test_add_bench_args_takes_a_per_script_sample_interval() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    add_bench_args(parser, ops=1, warmup=0, sample_interval=0.02)

    assert parser.parse_args([]).sample_interval == 0.02


# --- new_run_id ---------------------------------------------------------------


def test_new_run_id_is_short_lowercase_hex() -> None:
    run_id = new_run_id()

    assert re.fullmatch(r"[0-9a-f]{8}", run_id), run_id


def test_new_run_id_differs_across_calls() -> None:
    run_ids = {new_run_id() for _ in range(1_000)}

    assert len(run_ids) == 1_000


def test_new_run_id_is_drawn_from_uuid4(mocker: MockerFixture) -> None:
    uuid4 = mocker.patch(f"{MODULE}.uuid4")
    uuid4.return_value.hex = "0123456789abcdef" * 2

    assert new_run_id() == "01234567"
    uuid4.assert_called_once_with()


def test_show_bench_progress_enables_the_modules_info_lines() -> None:
    import logging

    bench_logger = logging.getLogger("message_broker_patterns.bench")
    previous = bench_logger.level
    try:
        bench_logger.setLevel(logging.NOTSET)

        show_bench_progress()

        assert bench_logger.isEnabledFor(logging.INFO)
    finally:
        bench_logger.setLevel(previous)
