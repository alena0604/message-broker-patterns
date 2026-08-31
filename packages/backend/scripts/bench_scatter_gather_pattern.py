"""Benchmark scatter-gather against its two knobs: fan-out width and timeout budget.

Both knobs trade the same thing off. **Fan-out width** is how many airlines the
search reaches — more recipients means more quotes to choose from, but the
gather can only ever be as complete as its slowest participant. The **timeout
budget** is the deadline the aggregator refuses to blow: whatever arrived by
then is the answer, and the rest is dropped. So the number that matters here is
not throughput alone but the *partial-gather rate* — the fraction of the
expected quotes a search actually collected before its deadline.

To make that fraction mean something, the airlines are deliberately staggered:
airline *i* answers after ``i * AIRLINE_LATENCY_STEP`` seconds. Widening the
fan-out therefore adds recipients that are progressively slower, and raising the
timeout lets progressively more of them in. The stagger is fixed rather than
random so two runs of the same configuration produce the same numbers.

One operation = one full scatter → gather cycle with a fresh correlation id.
Queue depth is ``qsize()`` on the coordinator's response queue: quotes that
arrived *after* their search gave up, still sitting there waiting to be
discarded by the next gather. That backlog is what a too-tight timeout looks
like from the outside.

    uv --directory packages/backend run python scripts/bench_scatter_gather_pattern.py
    uv --directory packages/backend run python scripts/bench_scatter_gather_pattern.py \
        --fanout 10 --timeout 0.5
"""

from message_broker_patterns.logging import init_logger

# WARNING, not INFO: every airline logs a line per quote, and this bench
# publishes fan-out quotes per operation.
init_logger("WARNING")

import argparse  # noqa: E402
import asyncio  # noqa: E402
import itertools  # noqa: E402
import logging  # noqa: E402
import sys  # noqa: E402
from datetime import UTC, datetime  # noqa: E402

from message_broker_patterns.bench import (  # noqa: E402
    BenchResult,
    Window,
    add_bench_args,
    emit_json,
    run_bench,
    show_bench_progress,
    write_csv_files,
)
from message_broker_patterns.scatter_gather_pattern.aggregator import (  # noqa: E402
    RESPONSE_TOPIC,
    ScatterGatherCoordinator,
    recipient_request_topic,
)
from message_broker_patterns.scatter_gather_pattern.broker import InMemoryTopicBroker  # noqa: E402
from message_broker_patterns.scatter_gather_pattern.models import (  # noqa: E402
    DistributionStrategy,
    FlightQuote,
    SearchRequest,
)
from message_broker_patterns.scatter_gather_pattern.service import AirlineService  # noqa: E402

show_bench_progress()
logger = logging.getLogger("bench_scatter_gather")

DEPART = datetime(2026, 8, 1, 9, 30, tzinfo=UTC)

# Airline i answers after i * this many seconds. Chosen so the interesting
# timeouts (50ms, 100ms) fall between two airlines rather than on top of one —
# a deadline that lands exactly on a reply makes the collected count flap.
AIRLINE_LATENCY_STEP = 0.015

DEFAULT_FANOUTS = [5, 10]
DEFAULT_TIMEOUTS = [0.05, 0.1]
DEFAULT_OPS = 20
DEFAULT_WARMUP = 3


def _make_lookup(airline: str, price_cents: int):
    def _lookup(request: SearchRequest) -> FlightQuote:
        return FlightQuote(
            correlation_id=request.correlation_id,
            airline=airline,
            flight_number=f"{airline.upper()}{price_cents // 100}",
            price_cents=price_cents,
            depart_at=DEPART,
        )

    return _lookup


def _build_airlines(broker: InMemoryTopicBroker, fanout: int) -> list[AirlineService]:
    """Build ``fanout`` recipients, each one step slower than the last."""
    return [
        AirlineService(
            f"air{index:02d}",
            broker,
            _make_lookup(f"air{index:02d}", 10_000 + index * 100),
            recipient_request_topic(f"air{index:02d}"),
            RESPONSE_TOPIC,
            latency=index * AIRLINE_LATENCY_STEP,
        )
        for index in range(fanout)
    ]


async def bench_fanout_timeout(
    fanout: int, timeout: float, args: argparse.Namespace
) -> BenchResult:
    """Run one measured window at a given fan-out width and timeout budget."""
    broker = InMemoryTopicBroker()
    airlines = _build_airlines(broker, fanout)
    recipients = [airline.name for airline in airlines]
    coordinator = ScatterGatherCoordinator(broker)

    # One long-lived subscription instead of `scatter_gather`'s per-search one,
    # so the depth sampler can watch late quotes accumulate between searches.
    response_queue = broker.subscribe(RESPONSE_TOPIC)

    stop = asyncio.Event()
    serving = [asyncio.create_task(airline.serve(stop)) for airline in airlines]

    searches = itertools.count(1)
    collected: list[int] = []

    async def one_search() -> None:
        request = SearchRequest(
            correlation_id=f"bench-search-{next(searches):06d}",
            origin="New York (JFK)",
            destination="Los Angeles (LAX)",
            departure_date="2026-08-01",
            passengers=1,
        )
        await coordinator.scatter(
            request, DistributionStrategy.RECIPIENT_LIST, recipients=recipients
        )
        quotes = await coordinator.gather(
            response_queue, request.correlation_id, expected=fanout, timeout=timeout
        )
        collected.append(len(quotes))

    async def sample_response_backlog() -> int:
        return response_queue.qsize()

    try:
        result = await run_bench(
            "scatter-gather",
            one_search,
            warmup=Window.of_ops(args.warmup),
            measure=Window.of_ops(args.ops),
            concurrency=args.concurrency,
            knobs={"fanout": fanout, "timeout": timeout},
            sampler=sample_response_backlog,
            sample_interval=args.sample_interval,
        )
    finally:
        stop.set()
        await asyncio.gather(*serving)
        broker.unsubscribe(RESPONSE_TOPIC, response_queue)

    # Warmup searches are at the front of `collected`; only the measured window
    # counts toward the partial-gather rate.
    measured = collected[-result.ops :]
    expected_quotes = result.ops * fanout
    gathered_quotes = sum(measured)
    result.extra = {
        "expected_quotes": expected_quotes,
        "collected_quotes": gathered_quotes,
        "collected_fraction": round(gathered_quotes / expected_quotes, 4),
        "partial_gathers": sum(1 for count in measured if count < fanout),
        "quotes_per_search": round(gathered_quotes / result.ops, 2),
    }
    return result


async def main() -> None:
    args = _parse_args()
    results = [
        await bench_fanout_timeout(fanout, timeout, args)
        for fanout in args.fanout
        for timeout in args.timeout
    ]
    emit_json(results, stream=sys.stdout)
    if args.csv_dir is not None:
        write_csv_files(results, args.csv_dir)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark scatter-gather across fan-out widths and timeout budgets."
    )
    parser.add_argument(
        "--fanout",
        type=int,
        nargs="+",
        default=DEFAULT_FANOUTS,
        help=f"number of airlines scattered to (default: {DEFAULT_FANOUTS})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        nargs="+",
        default=DEFAULT_TIMEOUTS,
        help=f"gather timeout budget(s) in seconds (default: {DEFAULT_TIMEOUTS})",
    )
    add_bench_args(parser, ops=DEFAULT_OPS, warmup=DEFAULT_WARMUP)
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
