from message_broker_patterns.logging import init_logger

init_logger()

import argparse  # noqa: E402
import asyncio  # noqa: E402
import contextlib  # noqa: E402
import logging  # noqa: E402
import time  # noqa: E402
import uuid  # noqa: E402
from collections.abc import Callable, Iterable  # noqa: E402
from datetime import UTC, datetime  # noqa: E402

from message_broker_patterns.chaos import (  # noqa: E402
    BrokerUnavailableError,
    broker_unavailable,
    slow_consumer,
)
from message_broker_patterns.scatter_gather_pattern.aggregator import (  # noqa: E402
    BROADCAST_REQUEST_TOPIC,
    RESPONSE_TOPIC,
    ScatterGatherCoordinator,
    recipient_request_topic,
)
from message_broker_patterns.scatter_gather_pattern.broker import InMemoryTopicBroker  # noqa: E402
from message_broker_patterns.scatter_gather_pattern.combining import (  # noqa: E402
    cheapest,
    sort_by_price,
)
from message_broker_patterns.scatter_gather_pattern.models import (  # noqa: E402
    DistributionStrategy,
    FlightQuote,
    SearchRequest,
)
from message_broker_patterns.scatter_gather_pattern.naive import (  # noqa: E402
    AsyncQuoteLookup,
    gather_quotes_sequentially,
)
from message_broker_patterns.scatter_gather_pattern.service import AirlineService  # noqa: E402

logger = logging.getLogger("run_scatter_gather")

DEPART = datetime(2026, 8, 1, 9, 30, tzinfo=UTC)

# Three airlines quote different fares; "SlowAir" is deliberately laggy and
# "GhostAir" always errors — together they exercise the partial-failure path.
AIRLINES = {
    "BudgetJet": 18_900,
    "SkyHigh": 24_500,
    "Nimbus": 21_200,
}


def make_lookup(airline: str, price_cents: int) -> Callable[[SearchRequest], FlightQuote]:
    def _lookup(request: SearchRequest) -> FlightQuote:
        return FlightQuote(
            correlation_id=request.correlation_id,
            airline=airline,
            flight_number=f"{airline[:2].upper()}{price_cents // 100}",
            price_cents=price_cents,
            depart_at=DEPART,
        )

    return _lookup


def ghost_lookup(request: SearchRequest) -> FlightQuote:
    raise RuntimeError("inventory system offline")


def build_services(
    broker: InMemoryTopicBroker, request_topic_for: Callable[[str], str]
) -> list[AirlineService]:
    services = [
        AirlineService(
            name, broker, make_lookup(name, price), request_topic_for(name), RESPONSE_TOPIC
        )
        for name, price in AIRLINES.items()
    ]
    # A slow airline whose quote arrives after the aggregator's timeout.
    services.append(
        AirlineService(
            "SlowAir",
            broker,
            make_lookup("SlowAir", 15_000),
            request_topic_for("SlowAir"),
            RESPONSE_TOPIC,
            latency=0.5,
        )
    )
    # A broken airline that never produces a quote.
    services.append(
        AirlineService(
            "GhostAir", broker, ghost_lookup, request_topic_for("GhostAir"), RESPONSE_TOPIC
        )
    )
    return services


async def run_demo(
    label: str,
    strategy: DistributionStrategy,
    request_topic_for: Callable[[str], str],
    recipients: Iterable[str] | None,
) -> None:
    logger.info("=" * 70)
    logger.info("  %s (%s)", label, strategy.value)
    logger.info("=" * 70)

    broker = InMemoryTopicBroker()
    services = build_services(broker, request_topic_for)
    coordinator = ScatterGatherCoordinator(broker)

    request = SearchRequest(
        correlation_id=str(uuid.uuid4()),
        origin="New York (JFK)",
        destination="Los Angeles (LAX)",
        departure_date="2026-08-01",
        passengers=2,
    )
    logger.info(
        "Searching %s → %s on %s (correlation=%s)",
        request.origin,
        request.destination,
        request.departure_date,
        request.correlation_id,
    )
    logger.info(
        "5 airlines queried; expecting 3 healthy replies (SlowAir is too slow, GhostAir is offline)"
    )

    stop = asyncio.Event()
    tasks = [asyncio.create_task(service.serve(stop)) for service in services]
    try:
        # Wait for the 3 healthy airlines, giving up after 200ms — SlowAir's
        # 500ms reply never makes the cut, GhostAir never replies at all.
        quotes = await coordinator.scatter_gather(
            request, strategy, expected=5, timeout=0.2, recipients=recipients
        )
    finally:
        stop.set()
        await asyncio.gather(*tasks)

    logger.info("Gathered %d quote(s) before the deadline:", len(quotes))
    for quote in sort_by_price(quotes):
        logger.info(
            "  %-10s %-8s $%6.2f departs %s",
            quote.airline,
            quote.flight_number,
            quote.price_cents / 100,
            quote.depart_at.isoformat(),
        )
    best = cheapest(quotes)
    if best is not None:
        logger.info("Cheapest: %s at $%.2f", best.airline, best.price_cents / 100)


async def main() -> None:
    logger.info("=== Scatter-Gather Demo: Flight Search Across Airlines ===")

    # Publish-subscribe: every airline subscribes to one broadcast topic; the
    # coordinator has no idea who is listening.
    await run_demo(
        "PUBLISH-SUBSCRIBE — broadcast to a shared topic",
        DistributionStrategy.PUBLISH_SUBSCRIBE,
        lambda _airline: BROADCAST_REQUEST_TOPIC,
        recipients=None,
    )

    # Recipient list: the coordinator addresses each airline's dedicated topic
    # by name — more control, tighter coupling.
    all_airlines = [*AIRLINES.keys(), "SlowAir", "GhostAir"]
    await run_demo(
        "RECIPIENT LIST — address each airline directly",
        DistributionStrategy.RECIPIENT_LIST,
        recipient_request_topic,
        recipients=all_airlines,
    )


# --- naive baseline -----------------------------------------------------------
# The same five airlines the correct demo queries, called one `await` at a time.
# Each healthy airline is given the same 50 ms of its own API latency (an
# airline's latency, not a fault — the correct path models it the same way via
# `AirlineService(latency=...)`). The two *faults* come from chaos: SlowAir's
# 500 ms stall and GhostAir's unreachable inventory system.
NAIVE_AIRLINE_LATENCY = 0.05
NAIVE_SLOW_LATENCY = 0.5
NAIVE_DEADLINE = 0.2  # the hard deadline `ScatterGatherCoordinator` enforces


def make_async_lookup(airline: str, price_cents: int) -> AsyncQuoteLookup:
    """The naive design's in-line airline call: an await, not a published request."""
    sync_lookup = make_lookup(airline, price_cents)

    async def _lookup(request: SearchRequest) -> FlightQuote:
        await asyncio.sleep(NAIVE_AIRLINE_LATENCY)  # the airline's own API latency
        return sync_lookup(request)

    return _lookup


def recording(name: str, lookup: AsyncQuoteLookup, asked: list[str]) -> AsyncQuoteLookup:
    """Note that an airline was reached at all, so we can name the ones that never were."""

    async def _wrapped(request: SearchRequest) -> FlightQuote:
        asked.append(name)
        return await lookup(request)

    return _wrapped


def naive_request() -> SearchRequest:
    return SearchRequest(
        correlation_id=str(uuid.uuid4()),
        origin="New York (JFK)",
        destination="Los Angeles (LAX)",
        departure_date="2026-08-01",
        passengers=2,
    )


async def run_naive() -> None:
    """INTENTIONALLY BROKEN — the sequential fan-in scatter-gather replaces."""
    logger.info("=" * 70)
    logger.info("  NAIVE scatter-gather — sequential fan-in (INTENTIONALLY BROKEN)")
    logger.info("=" * 70)

    # [1] Healthy airlines only: correct results, and latency that is the SUM.
    asked: list[str] = []
    healthy = [
        recording(name, make_async_lookup(name, price), asked) for name, price in AIRLINES.items()
    ]
    started = time.perf_counter()
    quotes = await gather_quotes_sequentially(naive_request(), healthy)
    elapsed = time.perf_counter() - started
    logger.info("[1] %d healthy airlines, one await at a time", len(healthy))
    logger.info(
        "    %d quote(s) in %.3fs — %d x %.3fs paid serially (the sum, not the max)",
        len(quotes),
        elapsed,
        len(healthy),
        NAIVE_AIRLINE_LATENCY,
    )

    # [2] Add SlowAir. There is no deadline inside the gather, so the caller
    #     waits for the slowest partner however long that takes...
    asked = []
    slow = recording(
        "SlowAir",
        slow_consumer(
            make_async_lookup("SlowAir", 15_000), latency=NAIVE_SLOW_LATENCY, probability=1.0
        ),
        asked,
    )
    with_slow = [
        recording(name, make_async_lookup(name, price), asked) for name, price in AIRLINES.items()
    ] + [slow]
    started = time.perf_counter()
    quotes = await gather_quotes_sequentially(naive_request(), with_slow)
    elapsed = time.perf_counter() - started
    logger.info("[2] + SlowAir (chaos.slow_consumer, latency=%.3fs)", NAIVE_SLOW_LATENCY)
    logger.info(
        "    %d quote(s) in %.3fs — %.1fx the %.3fs deadline the correct coordinator enforces",
        len(quotes),
        elapsed,
        elapsed / NAIVE_DEADLINE,
        NAIVE_DEADLINE,
    )

    # ...and a caller that *does* impose one gets nothing at all, not even the
    # quotes that already arrived. The correct coordinator returns those.
    asked = []
    with_slow = [
        recording(name, make_async_lookup(name, price), asked) for name, price in AIRLINES.items()
    ] + [
        recording(
            "SlowAir",
            slow_consumer(
                make_async_lookup("SlowAir", 15_000), latency=NAIVE_SLOW_LATENCY, probability=1.0
            ),
            asked,
        )
    ]
    salvaged: list[FlightQuote] = []
    with contextlib.suppress(TimeoutError):
        salvaged = await asyncio.wait_for(
            gather_quotes_sequentially(naive_request(), with_slow), timeout=NAIVE_DEADLINE
        )
    # Every airline reached except the one still stalling had already answered.
    arrived_before_the_deadline = len(asked) - 1
    logger.info(
        "    a caller that DOES impose a %.3fs deadline salvages %d of the %d quote(s) that "
        "had already arrived — a sequential gather has no partial result to hand back",
        NAIVE_DEADLINE,
        len(salvaged),
        arrived_before_the_deadline,
    )

    # [3] GhostAir raises, second in line. No try/except anywhere, so one
    #     unavailable airline sinks a search three others could have answered.
    asked = []
    names = list(AIRLINES)
    with_ghost = [
        recording(names[0], make_async_lookup(names[0], AIRLINES[names[0]]), asked),
        recording(
            "GhostAir",
            broker_unavailable(make_async_lookup("GhostAir", 0), probability=1.0),
            asked,
        ),
        *(recording(name, make_async_lookup(name, AIRLINES[name]), asked) for name in names[1:]),
    ]
    logger.info("[3] + GhostAir (chaos.broker_unavailable) second in line")
    try:
        await gather_quotes_sequentially(naive_request(), with_ghost)
    except BrokerUnavailableError as exc:
        never_asked = [n for n in [names[0], "GhostAir", *names[1:]] if n not in asked]
        logger.info("    search raised %s: %s", type(exc).__name__, exc)
        logger.info("    0 quotes returned; reached: %s", asked)
        logger.info("    never even asked: %s", never_asked)

    logger.info(
        "Run without --naive: the same five airlines under one %.3fs deadline, "
        "concurrent, returning the 3 quotes that made it.",
        NAIVE_DEADLINE,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scatter-Gather flight search demo.")
    parser.add_argument(
        "--naive",
        action="store_true",
        help="run the intentionally broken sequential fan-in baseline (naive.py) instead",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_naive() if _parse_args().naive else main())
