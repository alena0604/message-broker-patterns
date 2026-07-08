from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from message_broker_patterns.metrics import REGISTRY
from message_broker_patterns.scatter_gather_pattern.broker import PATTERN_ID, InMemoryTopicBroker
from message_broker_patterns.scatter_gather_pattern.models import FlightQuote, SearchRequest

logger = logging.getLogger(__name__)

# An airline's "internal systems" lookup: given a search, return the fare this
# airline offers. Plain (non-async) because it models a CPU/transformation step;
# raising signals that the airline has no availability (a partial failure that
# leaves the aggregator to time out on this recipient).
QuoteLookup = Callable[[SearchRequest], FlightQuote]


class AirlineService:
    """One airline recipient in the scatter-gather flow.

    On construction it subscribes to ``request_topic`` — either the shared
    broadcast topic (publish-subscribe) or its own dedicated topic (recipient
    list); the service code is identical either way. :meth:`serve` then loops:
    receive a search, simulate an inventory lookup, and publish a quote onto the
    common response topic tagged with the request's correlation id.
    """

    def __init__(
        self,
        name: str,
        broker: InMemoryTopicBroker,
        lookup: QuoteLookup,
        request_topic: str,
        response_topic: str,
        *,
        latency: float = 0.0,
    ) -> None:
        self.name = name
        self._broker = broker
        self._lookup = lookup
        self._response_topic = response_topic
        self._latency = latency
        self._queue = broker.subscribe(request_topic)

    async def serve(self, stop_event: asyncio.Event, *, poll_interval: float = 0.01) -> int:
        """Answer searches until ``stop_event`` is set; return quotes published.

        Each search is optionally delayed by ``latency`` to model a slow airline
        (whose answer may land after the aggregator's timeout, and is then
        harmlessly ignored). If the lookup raises, the airline simply publishes
        nothing — the aggregator still returns the other airlines' quotes once it
        times out, never hanging on the missing one.

        The receive is polled with :func:`asyncio.timeout` so a set stop event is
        always observed within ``poll_interval`` even when no search arrives.
        """
        served = 0
        while not stop_event.is_set():
            try:
                async with asyncio.timeout(poll_interval):
                    request = await self._queue.get()
            except TimeoutError:
                continue
            if self._latency > 0:
                await asyncio.sleep(self._latency)
            try:
                quote = self._lookup(request)
            except Exception as exc:
                REGISTRY.increment(PATTERN_ID, "service_failures")
                logger.warning(
                    "airline=%s could not quote correlation=%s: %s",
                    self.name,
                    request.correlation_id,
                    exc,
                )
                continue
            await self._broker.publish(self._response_topic, quote)
            REGISTRY.increment(PATTERN_ID, "responses_published")
            served += 1
            logger.info(
                "airline=%s quoted %s %d for correlation=%s",
                self.name,
                quote.flight_number,
                quote.price_cents,
                request.correlation_id,
            )
        logger.info("airline=%s stopping — published %d quote(s)", self.name, served)
        return served
