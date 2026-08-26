from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from datetime import UTC, datetime

import pytest

from message_broker_patterns.scatter_gather_pattern.aggregator import (
    BROADCAST_REQUEST_TOPIC,
    RESPONSE_TOPIC,
    ScatterGatherCoordinator,
    recipient_request_topic,
)
from message_broker_patterns.scatter_gather_pattern.broker import InMemoryTopicBroker
from message_broker_patterns.scatter_gather_pattern.combining import cheapest, sort_by_price
from message_broker_patterns.scatter_gather_pattern.models import (
    DistributionStrategy,
    FlightQuote,
    SearchRequest,
)
from message_broker_patterns.scatter_gather_pattern.service import AirlineService

DEPART = datetime(2026, 8, 1, 9, 30, tzinfo=UTC)

# Three airlines that answer, one that errors ("GhostAir"), and one that is
# addressed but never replies at all ("DeadAir") — five recipients, three
# usable answers. The gather is asked for all five so it can only finish by
# timing out.
HEALTHY_AIRLINES = {"BudgetJet": 18_900, "SkyHigh": 24_500, "Nimbus": 21_200}
GHOST_AIRLINE = "GhostAir"
SILENT_AIRLINE = "DeadAir"
FLEET_SIZE = len(HEALTHY_AIRLINES) + 2

GATHER_TIMEOUT = 0.1
# Generous upper bound: the point is that the gather returns at its own
# deadline instead of blocking on the two recipients that never answer.
NO_HANG_BUDGET = 1.0

STRATEGIES = [DistributionStrategy.PUBLISH_SUBSCRIBE, DistributionStrategy.RECIPIENT_LIST]


def _make_lookup(airline: str, price_cents: int) -> Callable[[SearchRequest], FlightQuote]:
    def _lookup(request: SearchRequest) -> FlightQuote:
        return FlightQuote(
            correlation_id=request.correlation_id,
            airline=airline,
            flight_number=f"{airline[:2].upper()}{price_cents // 100}",
            price_cents=price_cents,
            depart_at=DEPART,
        )

    return _lookup


def _ghost_lookup(request: SearchRequest) -> FlightQuote:
    raise RuntimeError("inventory system offline")


def _request_topic_for(strategy: DistributionStrategy) -> Callable[[str], str]:
    if strategy is DistributionStrategy.PUBLISH_SUBSCRIBE:
        return lambda _airline: BROADCAST_REQUEST_TOPIC
    return recipient_request_topic


def _recipients(strategy: DistributionStrategy) -> Iterable[str] | None:
    if strategy is DistributionStrategy.PUBLISH_SUBSCRIBE:
        return None
    return [*HEALTHY_AIRLINES, GHOST_AIRLINE, SILENT_AIRLINE]


def _build_fleet(
    broker: InMemoryTopicBroker, strategy: DistributionStrategy
) -> tuple[list[AirlineService], asyncio.Queue[SearchRequest]]:
    """Wire up the five recipients; return the served ones plus the silent inbox.

    The silent recipient is a bare subscriber queue with nothing draining it —
    the request reaches it and no answer ever comes back.
    """
    topic_for = _request_topic_for(strategy)
    services = [
        AirlineService(name, broker, _make_lookup(name, price), topic_for(name), RESPONSE_TOPIC)
        for name, price in HEALTHY_AIRLINES.items()
    ]
    services.append(
        AirlineService(
            GHOST_AIRLINE, broker, _ghost_lookup, topic_for(GHOST_AIRLINE), RESPONSE_TOPIC
        )
    )
    silent_inbox: asyncio.Queue[SearchRequest] = broker.subscribe(topic_for(SILENT_AIRLINE))
    return services, silent_inbox


def _search(correlation_id: str) -> SearchRequest:
    return SearchRequest(
        correlation_id=correlation_id,
        origin="JFK",
        destination="LAX",
        departure_date="2026-08-01",
        passengers=2,
    )


@pytest.mark.parametrize("strategy", STRATEGIES, ids=lambda s: s.value)
async def test_partial_failure_search_completes_on_timeout_with_successful_quotes(
    strategy: DistributionStrategy,
) -> None:
    broker = InMemoryTopicBroker()
    services, silent_inbox = _build_fleet(broker, strategy)
    coordinator = ScatterGatherCoordinator(broker)
    request = _search("corr-partial")

    stop = asyncio.Event()
    tasks = [asyncio.create_task(service.serve(stop)) for service in services]
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        quotes = await coordinator.scatter_gather(
            request,
            strategy,
            FLEET_SIZE,
            GATHER_TIMEOUT,
            recipients=_recipients(strategy),
        )
    finally:
        stop.set()
        await asyncio.gather(*tasks)
    elapsed = loop.time() - started

    # The two failing recipients never answered, so the gather could only end by
    # spending its whole budget — and it did end, well inside the no-hang bound.
    assert elapsed >= GATHER_TIMEOUT
    assert elapsed < NO_HANG_BUDGET

    # The silent recipient really was addressed: its request is still unanswered.
    assert silent_inbox.qsize() == 1

    # Partial, not empty and not raised: every healthy airline's quote survived.
    assert len(quotes) == len(HEALTHY_AIRLINES)
    assert {quote.airline for quote in quotes} == set(HEALTHY_AIRLINES)
    assert all(quote.correlation_id == request.correlation_id for quote in quotes)

    # The partial result is still usable by the combining step.
    assert [quote.airline for quote in sort_by_price(quotes)] == ["BudgetJet", "Nimbus", "SkyHigh"]
    best = cheapest(quotes)
    assert best is not None
    assert best.airline == "BudgetJet"


async def test_concurrent_searches_under_partial_failure_stay_isolated() -> None:
    strategy = DistributionStrategy.PUBLISH_SUBSCRIBE
    broker = InMemoryTopicBroker()
    services, _silent_inbox = _build_fleet(broker, strategy)
    coordinator = ScatterGatherCoordinator(broker)

    stop = asyncio.Event()
    tasks = [asyncio.create_task(service.serve(stop)) for service in services]
    try:
        quotes_a, quotes_b = await asyncio.gather(
            coordinator.scatter_gather(_search("corr-A"), strategy, FLEET_SIZE, GATHER_TIMEOUT),
            coordinator.scatter_gather(_search("corr-B"), strategy, FLEET_SIZE, GATHER_TIMEOUT),
        )
    finally:
        stop.set()
        await asyncio.gather(*tasks)

    # Both searches degrade to the same partial result, each stitched back
    # together by its own correlation id.
    for correlation_id, quotes in (("corr-A", quotes_a), ("corr-B", quotes_b)):
        assert {quote.airline for quote in quotes} == set(HEALTHY_AIRLINES)
        assert {quote.correlation_id for quote in quotes} == {correlation_id}
