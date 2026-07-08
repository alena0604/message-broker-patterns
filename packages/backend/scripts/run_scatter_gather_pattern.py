from message_broker_patterns.logging import init_logger

init_logger()

import asyncio  # noqa: E402
import logging  # noqa: E402
import uuid  # noqa: E402
from collections.abc import Callable, Iterable  # noqa: E402
from datetime import UTC, datetime  # noqa: E402

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
        AirlineService("GhostAir", broker, ghost_lookup, request_topic_for("GhostAir"), RESPONSE_TOPIC)
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
        "5 airlines queried; expecting 3 healthy replies "
        "(SlowAir is too slow, GhostAir is offline)"
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


if __name__ == "__main__":
    asyncio.run(main())
