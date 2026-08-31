"""Benchmark harness — the numbers behind "operating it at scale".

Every pattern in this repo has one knob an operator actually turns: the outbox
relay's poll interval, a scatter-gather's fan-out width and timeout budget, how
many competing consumers share a queue, how many attempts a payment gets before
it is dead-lettered. Each ``scripts/bench_<pattern>.py`` turns exactly one of
those knobs; this module is the part they all share, so the four scripts differ
only in the workload they drive.

What a bench measures here:

- **Warmup, excluded.** A first phase runs the same operation and throws the
  numbers away — connection setup, consumer-group creation and the first Redis
  round-trip are startup cost, not steady state. Only the second phase is timed.
- **Throughput** — operations completed per second of the measured window.
- **Latency distribution** — p50/p95/p99 (plus min/mean/max) over the per-op
  latencies, by nearest-rank on the sorted samples. A bench that reports a mean
  and nothing else hides the tail the pattern exists to bound.
- **Queue depth over time** — periodic samples taken through a caller-supplied
  async :data:`DepthSampler`, because "depth" is a different question per
  pattern: rows left in the outbox table, ``XLEN`` of a dead-letter stream,
  unacked entries in a consumer group, ``qsize()`` of an :class:`asyncio.Queue`.
  The harness owns the sampling cadence; the caller owns the meaning.

A phase runs until its :class:`Window` closes — a fixed operation count or a
fixed wall-clock duration, never both, and never neither (an unbounded bench
that looks armed is the same trap :class:`chaos.FirePolicy` refuses). Operations
run ``concurrency`` at a time, which is what lets a bench put a real backlog in
front of N competing consumers instead of measuring one round trip at a time.

Failures are *not* swallowed: if the workload raises, the exception propagates
and the bench dies. A harness that quietly counts errors reports a throughput
number for work that never happened.

Results are emitted as JSON on a stream (:func:`emit_json`) — the required
output — with CSV of the latency and queue-depth series available for charting
(:func:`latency_csv`, :func:`queue_depth_csv`, :func:`write_csv_files`).

Timing goes through the module-level :func:`now` (a monotonic
:func:`time.perf_counter`) rather than inline calls, so a test can patch it and
assert exact windows, throughput and percentiles — the same convention as
``chaos.sleep`` and ``entities.envelope.utc_now``.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import io
import json
import logging
import math
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

logger = logging.getLogger(__name__)

#: The unit of work a bench times. Takes no arguments — a script closes over
#: whatever workload state it needs — and its return value is ignored.
type Operation = Callable[[], Awaitable[object]]

#: Reads whatever "queue depth" means for one pattern, once, right now.
type DepthSampler = Callable[[], Awaitable[int]]

#: How often the depth sampler runs during the measured window, in seconds.
DEFAULT_SAMPLE_INTERVAL = 0.05


def now() -> float:
    """Monotonic seconds. Patchable, so a windowing test never has to wait."""
    return time.perf_counter()


def percentile(values: Sequence[float], quantile: float) -> float:
    """Return the ``quantile``-th percentile of ``values`` by nearest rank.

    Nearest rank — ``ceil(q/100 * n)``-th smallest sample — rather than an
    interpolating definition: the result is always a latency that was actually
    observed, which is what a tail number should be. No external stats
    dependency for what is three lines of sorting.
    """
    if not values:
        raise ValueError("cannot take a percentile of an empty sequence")
    if not 0 <= quantile <= 100:
        raise ValueError(f"quantile must be between 0 and 100, got {quantile!r}")
    ordered = sorted(values)
    rank = math.ceil(quantile / 100 * len(ordered))
    return ordered[max(rank - 1, 0)]


@dataclass(frozen=True)
class LatencySummary:
    """Per-operation latency distribution, in milliseconds."""

    count: int
    min_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    mean_ms: float

    @classmethod
    def from_seconds(cls, latencies: Sequence[float]) -> LatencySummary:
        """Summarise raw per-op latencies (seconds in, milliseconds out)."""
        if not latencies:
            raise ValueError("cannot summarise an empty latency sample")
        ms = [value * 1_000 for value in latencies]
        return cls(
            count=len(ms),
            min_ms=min(ms),
            p50_ms=percentile(ms, 50),
            p95_ms=percentile(ms, 95),
            p99_ms=percentile(ms, 99),
            max_ms=max(ms),
            mean_ms=sum(ms) / len(ms),
        )

    def to_dict(self) -> dict[str, float]:
        return {
            "count": self.count,
            "min": round(self.min_ms, 3),
            "p50": round(self.p50_ms, 3),
            "p95": round(self.p95_ms, 3),
            "p99": round(self.p99_ms, 3),
            "max": round(self.max_ms, 3),
            "mean": round(self.mean_ms, 3),
        }


@dataclass(frozen=True)
class DepthSample:
    """One reading of the pattern's queue depth, ``elapsed_s`` into the window."""

    elapsed_s: float
    depth: int


@dataclass(frozen=True)
class Window:
    """How long a phase runs: a fixed operation count, or a wall-clock duration.

    Exactly one of the two must be set. Defaulting to either would silently
    change what a bench measured, and defaulting to neither would run forever.
    """

    ops: int | None = None
    seconds: float | None = None

    def __post_init__(self) -> None:
        if self.ops is not None and self.seconds is not None:
            raise ValueError("configure ops or seconds, not both")
        if self.ops is None and self.seconds is None:
            raise ValueError("configure a window: ops=<n> or seconds=<s>")
        if self.ops is not None and self.ops < 1:
            raise ValueError(f"ops must be a positive count, got {self.ops!r}")
        if self.seconds is not None and (not math.isfinite(self.seconds) or self.seconds <= 0):
            raise ValueError(f"seconds must be finite and positive, got {self.seconds!r}")

    @classmethod
    def of_ops(cls, ops: int) -> Window:
        """A window that closes after ``ops`` operations have been started."""
        return cls(ops=ops)

    @classmethod
    def of_seconds(cls, seconds: float) -> Window:
        """A window that closes ``seconds`` after the phase began."""
        return cls(seconds=seconds)

    def is_done(self, started: int, elapsed: float) -> bool:
        """Whether the phase should stop admitting work.

        ``started`` is the number of operations already admitted (not completed)
        — gating on admission is what keeps a concurrent count-window from
        overshooting its target.
        """
        if self.ops is not None:
            return started >= self.ops
        assert self.seconds is not None
        return elapsed >= self.seconds

    def describe(self) -> str:
        return f"{self.ops} ops" if self.ops is not None else f"{self.seconds}s"


@dataclass
class BenchResult:
    """One bench run: the knob values that produced it, and what they cost.

    ``extra`` is deliberately caller-owned and empty by default — a place for
    the pattern-specific number a generic harness cannot know about (how many
    quotes a gather actually collected, how many payments were dead-lettered).
    """

    name: str
    knobs: dict[str, object]
    warmup_ops: int
    ops: int
    duration_s: float
    throughput_ops_s: float
    latency: LatencySummary
    latencies_s: list[float] = field(default_factory=list)
    queue_depth: list[DepthSample] = field(default_factory=list)
    extra: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """The JSON-serialisable shape :func:`emit_json` writes."""
        return {
            "name": self.name,
            "knobs": dict(self.knobs),
            "warmup_ops": self.warmup_ops,
            "ops": self.ops,
            "duration_s": round(self.duration_s, 4),
            "throughput_ops_s": round(self.throughput_ops_s, 2),
            "latency_ms": self.latency.to_dict(),
            "queue_depth": [
                {"elapsed_s": round(sample.elapsed_s, 4), "depth": sample.depth}
                for sample in self.queue_depth
            ],
            "extra": dict(self.extra),
        }

    def slug(self) -> str:
        """Filesystem-safe identifier: the bench name plus its knob values.

        Underscore separates the parts, so it is the one character scrubbed out
        of the parts themselves — ``outbox_poll-interval-0.05``.
        """

        def clean(value: object) -> str:
            return re.sub(r"[^A-Za-z0-9.]+", "-", str(value)).strip("-")

        parts = [clean(self.name), *(f"{clean(k)}-{clean(v)}" for k, v in self.knobs.items())]
        return "_".join(parts)


async def _run_phase(
    operation: Operation,
    window: Window,
    concurrency: int,
    started_at: float,
) -> tuple[int, list[float]]:
    """Drive ``operation`` ``concurrency``-at-a-time until ``window`` closes.

    Returns the number of operations run and their individual latencies. Each
    worker re-checks the window before admitting its next operation, so a
    duration window always completes the op that was in flight when it expired
    rather than reporting a truncated latency.
    """
    started = 0
    latencies: list[float] = []

    async def worker() -> None:
        nonlocal started
        while not window.is_done(started, now() - started_at):
            started += 1
            call_started_at = now()
            await operation()
            latencies.append(now() - call_started_at)

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    return started, latencies


async def _sample_depth(
    sampler: DepthSampler,
    interval: float,
    started_at: float,
    samples: list[DepthSample],
) -> None:
    """Record a depth reading every ``interval`` seconds until cancelled."""
    while True:
        samples.append(DepthSample(now() - started_at, await sampler()))
        await asyncio.sleep(interval)


async def run_bench(
    name: str,
    operation: Operation,
    *,
    measure: Window,
    warmup: Window | None = None,
    concurrency: int = 1,
    knobs: Mapping[str, object] | None = None,
    sampler: DepthSampler | None = None,
    sample_interval: float = DEFAULT_SAMPLE_INTERVAL,
) -> BenchResult:
    """Warm up, then time ``operation`` and return the measured :class:`BenchResult`.

    The warmup phase runs the identical operation and discards its latencies;
    the clock for throughput starts only once it is over. ``sampler``, when
    given, runs solely across the measured window — the depth series and the
    throughput number describe the same interval, so they can share an x-axis.
    """
    if concurrency < 1:
        raise ValueError(f"concurrency must be at least 1, got {concurrency!r}")

    warmup_ops = 0
    if warmup is not None:
        logger.info(
            "bench %s: warming up (%s, concurrency=%d)", name, warmup.describe(), concurrency
        )
        warmup_ops, _ = await _run_phase(operation, warmup, concurrency, now())

    logger.info("bench %s: measuring (%s, concurrency=%d)", name, measure.describe(), concurrency)
    samples: list[DepthSample] = []
    started_at = now()
    sampler_task: asyncio.Task[None] | None = None
    if sampler is not None:
        sampler_task = asyncio.create_task(
            _sample_depth(sampler, sample_interval, started_at, samples)
        )
    try:
        ops, latencies = await _run_phase(operation, measure, concurrency, started_at)
    finally:
        if sampler_task is not None:
            sampler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sampler_task
    duration = now() - started_at
    if sampler is not None:
        # One final reading at the closing edge, so the series covers the whole
        # window even when it ended between two ticks.
        samples.append(DepthSample(now() - started_at, await sampler()))

    result = BenchResult(
        name=name,
        knobs=dict(knobs or {}),
        warmup_ops=warmup_ops,
        ops=ops,
        duration_s=duration,
        throughput_ops_s=ops / duration if duration > 0 else 0.0,
        latency=LatencySummary.from_seconds(latencies),
        latencies_s=latencies,
        queue_depth=samples,
    )
    logger.info(
        "bench %s: %d op(s) in %.3fs — %.1f ops/s, p50=%.1fms p95=%.1fms p99=%.1fms",
        name,
        result.ops,
        result.duration_s,
        result.throughput_ops_s,
        result.latency.p50_ms,
        result.latency.p95_ms,
        result.latency.p99_ms,
    )
    return result


def emit_json(results: BenchResult | Sequence[BenchResult], *, stream: TextIO) -> None:
    """Write ``results`` to ``stream`` as JSON — one object, or an array for a sweep.

    Writes to the stream rather than printing: the caller decides where the
    machine-readable output goes (stdout for a pipe, a file for a chart), and
    library code never owns stdout.
    """
    payload: object
    if isinstance(results, BenchResult):
        payload = results.to_dict()
    else:
        payload = [result.to_dict() for result in results]
    stream.write(json.dumps(payload, indent=2) + "\n")
    stream.flush()


def _csv(header: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue()


def latency_csv(result: BenchResult) -> str:
    """The per-operation latency series as CSV: ``op,latency_ms``."""
    rows = [
        (index, round(latency * 1_000, 3))
        for index, latency in enumerate(result.latencies_s, start=1)
    ]
    return _csv(("op", "latency_ms"), rows)


def queue_depth_csv(result: BenchResult) -> str:
    """The queue-depth series as CSV: ``elapsed_s,depth``."""
    rows = [(round(sample.elapsed_s, 4), sample.depth) for sample in result.queue_depth]
    return _csv(("elapsed_s", "depth"), rows)


def write_csv_files(results: Sequence[BenchResult], directory: Path) -> list[Path]:
    """Write a latency and a queue-depth CSV per result; return the paths written."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for result in results:
        for suffix, content in (
            ("latency", latency_csv(result)),
            ("depth", queue_depth_csv(result)),
        ):
            path = directory / f"{result.slug()}_{suffix}.csv"
            path.write_text(content)
            written.append(path)
    logger.info("bench: wrote %d CSV file(s) to %s", len(written), directory)
    return written


def show_bench_progress() -> None:
    """Let this module's progress lines through a deliberately quiet root logger.

    A bench script initialises logging at WARNING — the pattern modules log a
    line per message, which at a few hundred operations costs more than the work
    being measured — but the operator still wants to see which configuration is
    running and what it scored.
    """
    logger.setLevel(logging.INFO)


def add_bench_args(
    parser: argparse.ArgumentParser,
    *,
    ops: int,
    warmup: int,
    concurrency: int = 1,
    sample_interval: float = DEFAULT_SAMPLE_INTERVAL,
) -> None:
    """Register the knobs every bench script shares, with per-script defaults.

    The defaults are the script's own — each pattern's workload has a different
    natural size — but the flag names and help text stay identical across the
    four benches so one habit works everywhere.
    """
    parser.add_argument(
        "--ops", type=int, default=ops, help=f"operations in the measured window (default: {ops})"
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=warmup,
        help=f"operations run before measuring, discarded (default: {warmup})",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=concurrency,
        help=f"operations kept in flight at once (default: {concurrency})",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=sample_interval,
        help=f"seconds between queue-depth samples (default: {sample_interval})",
    )
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=None,
        help="also write the latency and queue-depth series as CSV into this directory",
    )
