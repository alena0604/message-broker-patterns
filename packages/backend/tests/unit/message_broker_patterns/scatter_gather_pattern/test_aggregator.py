from __future__ import annotations

import asyncio
from collections.abc import Callable
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

DEPART = datetime(2026, 8, 1, 9, 0, tzinfo=UTC)


def _airline_lookup(name: str, price_cents: int) -> Callable[[SearchRequest], FlightQuote]:
    """Build a lookup that quotes ``price_cents`` and echoes the correlation id."""

    def _lookup(request: SearchRequest) -> FlightQuote:
        return FlightQuote(
            correlation_id=request.correlation_id,
            airline=name,
            flight_number=f"{name[:2].upper()}{price_cents}",
            price_cents=price_cents,
            depart_at=DEPART,
        )

    return _lookup


def _failing_lookup(request: SearchRequest) -> FlightQuote:
    raise RuntimeError("no availability")


# --- scatter ---------------------------------------------------------------


async def test_scatter_publish_subscribe_reaches_every_subscriber(
    broker: InMemoryTopicBroker,
    make_request: Callable[..., SearchRequest],
) -> None:
    broker.subscribe(BROADCAST_REQUEST_TOPIC)
    broker.subscribe(BROADCAST_REQUEST_TOPIC)
    coordinator = ScatterGatherCoordinator(broker)

    reached = await coordinator.scatter(make_request(), DistributionStrategy.PUBLISH_SUBSCRIBE)

    assert reached == 2


async def test_scatter_recipient_list_addresses_each_recipient(
    broker: InMemoryTopicBroker,
    make_request: Callable[..., SearchRequest],
) -> None:
    alpha = broker.subscribe(recipient_request_topic("alpha"))
    beta = broker.subscribe(recipient_request_topic("beta"))
    coordinator = ScatterGatherCoordinator(broker)
    request = make_request()

    reached = await coordinator.scatter(
        request, DistributionStrategy.RECIPIENT_LIST, recipients=["alpha", "beta"]
    )

    assert reached == 2
    assert await alpha.get() is request
    assert await beta.get() is request


async def test_scatter_recipient_list_requires_recipients(
    broker: InMemoryTopicBroker,
    make_request: Callable[..., SearchRequest],
) -> None:
    coordinator = ScatterGatherCoordinator(broker)

    with pytest.raises(ValueError, match="recipient-list"):
        await coordinator.scatter(make_request(), DistributionStrategy.RECIPIENT_LIST)


# --- gather ----------------------------------------------------------------


async def test_gather_returns_all_quotes_before_timeout(
    broker: InMemoryTopicBroker,
    make_quote: Callable[..., FlightQuote],
) -> None:
    coordinator = ScatterGatherCoordinator(broker)
    queue = broker.subscribe(RESPONSE_TOPIC)
    await broker.publish(RESPONSE_TOPIC, make_quote(airline="A", price_cents=10_000))
    await broker.publish(RESPONSE_TOPIC, make_quote(airline="B", price_cents=20_000))

    # Generous timeout: gather should return as soon as both expected quotes are
    # in hand, without waiting the full timeout out.
    collected = await coordinator.gather(queue, "corr-1", expected=2, timeout=5.0)

    assert {q.airline for q in collected} == {"A", "B"}


async def test_gather_returns_partial_result_after_timeout(
    broker: InMemoryTopicBroker,
    make_quote: Callable[..., FlightQuote],
) -> None:
    coordinator = ScatterGatherCoordinator(broker)
    queue = broker.subscribe(RESPONSE_TOPIC)
    # Only two of the three expected airlines ever answer.
    await broker.publish(RESPONSE_TOPIC, make_quote(airline="A", price_cents=10_000))
    await broker.publish(RESPONSE_TOPIC, make_quote(airline="B", price_cents=20_000))

    collected = await coordinator.gather(queue, "corr-1", expected=3, timeout=0.05)

    assert len(collected) == 2
    assert {q.airline for q in collected} == {"A", "B"}


async def test_gather_ignores_foreign_correlation_ids(
    broker: InMemoryTopicBroker,
    make_quote: Callable[..., FlightQuote],
) -> None:
    coordinator = ScatterGatherCoordinator(broker)
    queue = broker.subscribe(RESPONSE_TOPIC)
    await broker.publish(RESPONSE_TOPIC, make_quote(correlation_id="corr-2", airline="X"))
    await broker.publish(RESPONSE_TOPIC, make_quote(correlation_id="corr-1", airline="A"))
    await broker.publish(RESPONSE_TOPIC, make_quote(correlation_id="corr-1", airline="B"))

    collected = await coordinator.gather(queue, "corr-1", expected=2, timeout=0.5)

    assert {q.airline for q in collected} == {"A", "B"}
    assert all(q.correlation_id == "corr-1" for q in collected)


# --- end-to-end scatter_gather --------------------------------------------


@pytest.mark.parametrize(
    "strategy",
    [DistributionStrategy.PUBLISH_SUBSCRIBE, DistributionStrategy.RECIPIENT_LIST],
)
async def test_scatter_gather_happy_path_for_both_strategies(
    broker: InMemoryTopicBroker,
    make_request: Callable[..., SearchRequest],
    drive: Callable[..., object],
    strategy: DistributionStrategy,
) -> None:
    names = ["alpha", "beta", "gamma"]
    prices = {"alpha": 30_000, "beta": 10_000, "gamma": 20_000}
    if strategy is DistributionStrategy.PUBLISH_SUBSCRIBE:
        request_topics = {name: BROADCAST_REQUEST_TOPIC for name in names}
        recipients = None
    else:
        request_topics = {name: recipient_request_topic(name) for name in names}
        recipients = names
    services = [
        AirlineService(
            name, broker, _airline_lookup(name, prices[name]), request_topics[name], RESPONSE_TOPIC
        )
        for name in names
    ]
    coordinator = ScatterGatherCoordinator(broker)

    quotes = await drive(
        coordinator,
        make_request(correlation_id="corr-1"),
        strategy,
        services,
        expected=3,
        timeout=1.0,
        recipients=recipients,
    )

    ordered = sort_by_price(quotes)
    assert [q.airline for q in ordered] == ["beta", "gamma", "alpha"]
    best = cheapest(quotes)
    assert best is not None
    assert best.airline == "beta"


async def test_scatter_gather_returns_partial_result_on_service_failure(
    broker: InMemoryTopicBroker,
    make_request: Callable[..., SearchRequest],
    drive: Callable[..., object],
) -> None:
    # alpha answers instantly, beta errors out (never answers), gamma is so slow
    # its answer lands after the timeout — the aggregator must still return alpha.
    services = [
        AirlineService(
            "alpha",
            broker,
            _airline_lookup("alpha", 10_000),
            BROADCAST_REQUEST_TOPIC,
            RESPONSE_TOPIC,
        ),
        AirlineService("beta", broker, _failing_lookup, BROADCAST_REQUEST_TOPIC, RESPONSE_TOPIC),
        AirlineService(
            "gamma",
            broker,
            _airline_lookup("gamma", 20_000),
            BROADCAST_REQUEST_TOPIC,
            RESPONSE_TOPIC,
            latency=0.2,
        ),
    ]
    coordinator = ScatterGatherCoordinator(broker)

    quotes = await drive(
        coordinator,
        make_request(correlation_id="corr-1"),
        DistributionStrategy.PUBLISH_SUBSCRIBE,
        services,
        expected=3,
        timeout=0.05,
        recipients=None,
    )

    assert [q.airline for q in quotes] == ["alpha"]


async def test_concurrent_searches_do_not_cross_contaminate(
    broker: InMemoryTopicBroker,
    make_request: Callable[..., SearchRequest],
) -> None:
    names = ["alpha", "beta"]
    prices = {"alpha": 10_000, "beta": 20_000}
    services = [
        AirlineService(
            name,
            broker,
            _airline_lookup(name, prices[name]),
            BROADCAST_REQUEST_TOPIC,
            RESPONSE_TOPIC,
        )
        for name in names
    ]
    coordinator = ScatterGatherCoordinator(broker)
    stop = asyncio.Event()
    tasks = [asyncio.create_task(service.serve(stop)) for service in services]

    try:
        result_a, result_b = await asyncio.gather(
            coordinator.scatter_gather(
                make_request(correlation_id="search-A"),
                DistributionStrategy.PUBLISH_SUBSCRIBE,
                expected=2,
                timeout=1.0,
            ),
            coordinator.scatter_gather(
                make_request(correlation_id="search-B"),
                DistributionStrategy.PUBLISH_SUBSCRIBE,
                expected=2,
                timeout=1.0,
            ),
        )
    finally:
        stop.set()
        await asyncio.gather(*tasks)

    assert len(result_a) == 2
    assert len(result_b) == 2
    assert all(q.correlation_id == "search-A" for q in result_a)
    assert all(q.correlation_id == "search-B" for q in result_b)
