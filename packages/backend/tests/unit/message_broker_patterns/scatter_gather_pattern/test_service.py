from __future__ import annotations

import asyncio
from collections.abc import Callable

from message_broker_patterns.scatter_gather_pattern.broker import InMemoryTopicBroker
from message_broker_patterns.scatter_gather_pattern.models import FlightQuote, SearchRequest
from message_broker_patterns.scatter_gather_pattern.service import AirlineService

REQUEST_TOPIC = "flights:search:requests:alpha"
RESPONSE_TOPIC = "flights:search:responses"


async def _drain_one(response_queue: asyncio.Queue[FlightQuote]) -> FlightQuote:
    async with asyncio.timeout(1.0):
        return await response_queue.get()


async def test_serve_publishes_quote_for_received_request(
    broker: InMemoryTopicBroker,
    make_request: Callable[..., SearchRequest],
    make_quote: Callable[..., FlightQuote],
) -> None:
    expected_quote = make_quote(airline="Alpha", price_cents=12_000)

    def lookup(_request: SearchRequest) -> FlightQuote:
        return expected_quote

    service = AirlineService("Alpha", broker, lookup, REQUEST_TOPIC, RESPONSE_TOPIC)
    responses = broker.subscribe(RESPONSE_TOPIC)
    await broker.publish(REQUEST_TOPIC, make_request())

    stop = asyncio.Event()
    task = asyncio.create_task(service.serve(stop))
    quote = await _drain_one(responses)
    stop.set()
    served = await task

    assert quote == expected_quote
    assert served == 1


async def test_serve_publishes_nothing_when_lookup_raises(
    broker: InMemoryTopicBroker,
    make_request: Callable[..., SearchRequest],
) -> None:
    calls = 0

    def failing_lookup(_request: SearchRequest) -> FlightQuote:
        nonlocal calls
        calls += 1
        raise RuntimeError("no availability")

    service = AirlineService("Alpha", broker, failing_lookup, REQUEST_TOPIC, RESPONSE_TOPIC)
    responses = broker.subscribe(RESPONSE_TOPIC)
    await broker.publish(REQUEST_TOPIC, make_request())

    stop = asyncio.Event()
    task = asyncio.create_task(service.serve(stop))

    # Give the service a couple of poll cycles to consume the request and fail.
    async def _stop_once_consumed() -> None:
        while calls == 0:
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)
        stop.set()

    _, served = await asyncio.gather(_stop_once_consumed(), task)

    assert calls == 1
    assert served == 0
    assert responses.empty()


async def test_serve_stops_immediately_when_already_stopped(
    broker: InMemoryTopicBroker,
) -> None:
    def lookup(_request: SearchRequest) -> FlightQuote:
        raise AssertionError("lookup must not run when already stopped")

    service = AirlineService("Alpha", broker, lookup, REQUEST_TOPIC, RESPONSE_TOPIC)
    stop = asyncio.Event()
    stop.set()

    served = await service.serve(stop)

    assert served == 0
