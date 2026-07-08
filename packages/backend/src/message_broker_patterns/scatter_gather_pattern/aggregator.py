from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable

from message_broker_patterns.metrics import REGISTRY
from message_broker_patterns.scatter_gather_pattern.broker import PATTERN_ID, InMemoryTopicBroker
from message_broker_patterns.scatter_gather_pattern.models import (
    DistributionStrategy,
    FlightQuote,
    SearchRequest,
)

logger = logging.getLogger(__name__)

# The single topic every airline publishes its quotes onto, and the broadcast
# topic used by the publish-subscribe distribution strategy.
RESPONSE_TOPIC = "flights:search:responses"
BROADCAST_REQUEST_TOPIC = "flights:search:requests"


def recipient_request_topic(airline: str) -> str:
    """The dedicated request topic the recipient-list strategy addresses."""
    return f"flights:search:requests:{airline}"


class ScatterGatherCoordinator:
    """Scatters a search to many airlines and gathers their quotes into one answer.

    The coordinator owns the two halves of the pattern:

    * **scatter** — fan the request out, either to a known recipient list or to a
      broadcast topic (publish-subscribe).
    * **gather** — collect quotes off the shared response topic, keeping only
      those whose correlation id matches this search, until either the expected
      count arrives or the timeout expires.

    Partial failures are absorbed by the timeout: a missing or slow airline never
    stalls the gather — whatever arrived by the deadline is returned.
    """

    def __init__(
        self,
        broker: InMemoryTopicBroker,
        *,
        response_topic: str = RESPONSE_TOPIC,
        broadcast_topic: str = BROADCAST_REQUEST_TOPIC,
    ) -> None:
        self._broker = broker
        self._response_topic = response_topic
        self._broadcast_topic = broadcast_topic

    async def scatter(
        self,
        request: SearchRequest,
        strategy: DistributionStrategy,
        *,
        recipients: Iterable[str] | None = None,
    ) -> int:
        """Fan ``request`` out per ``strategy``; return how many queues it reached.

        For :attr:`DistributionStrategy.PUBLISH_SUBSCRIBE` the request is
        published once to the broadcast topic. For
        :attr:`DistributionStrategy.RECIPIENT_LIST` it is published to each named
        recipient's dedicated topic — ``recipients`` is required in that mode.
        """
        if strategy is DistributionStrategy.PUBLISH_SUBSCRIBE:
            reached = await self._broker.publish(self._broadcast_topic, request)
        else:
            if recipients is None:
                raise ValueError("recipient-list strategy requires an explicit recipients list")
            reached = 0
            for airline in recipients:
                reached += await self._broker.publish(recipient_request_topic(airline), request)
        REGISTRY.increment(PATTERN_ID, "requests_scattered")
        logger.info(
            "scattered correlation=%s via %s → %d recipient queue(s)",
            request.correlation_id,
            strategy.value,
            reached,
        )
        return reached

    async def gather(
        self,
        response_queue: asyncio.Queue[FlightQuote],
        correlation_id: str,
        expected: int,
        timeout: float,
    ) -> list[FlightQuote]:
        """Collect up to ``expected`` matching quotes off ``response_queue``.

        Returns as soon as ``expected`` quotes for ``correlation_id`` have
        arrived, or when ``timeout`` seconds have elapsed — whichever comes
        first. Quotes carrying a different correlation id are discarded (keeping
        concurrent searches isolated); they do not count toward ``expected`` and
        do not extend the deadline. On an early timeout the partial result is
        returned rather than raising.
        """
        collected: list[FlightQuote] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while len(collected) < expected:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                async with asyncio.timeout(remaining):
                    quote = await response_queue.get()
            except TimeoutError:
                break
            if quote.correlation_id != correlation_id:
                logger.debug(
                    "gather for %s ignored quote for %s",
                    correlation_id,
                    quote.correlation_id,
                )
                continue
            collected.append(quote)

        REGISTRY.increment(PATTERN_ID, "responses_gathered", len(collected))
        if len(collected) < expected:
            REGISTRY.increment(PATTERN_ID, "partial_gathers")
            logger.warning(
                "gather for %s timed out with %d/%d quote(s)",
                correlation_id,
                len(collected),
                expected,
            )
        else:
            logger.info("gather for %s complete: %d quote(s)", correlation_id, len(collected))
        return collected

    async def scatter_gather(
        self,
        request: SearchRequest,
        strategy: DistributionStrategy,
        expected: int,
        timeout: float,
        *,
        recipients: Iterable[str] | None = None,
    ) -> list[FlightQuote]:
        """Run a full scatter → gather cycle and return the collected quotes.

        Subscribes to the response topic *before* scattering so no fast reply is
        missed, then always unsubscribes — even on a timeout — so a finished
        search stops absorbing late responses.
        """
        response_queue: asyncio.Queue[FlightQuote] = self._broker.subscribe(self._response_topic)
        try:
            await self.scatter(request, strategy, recipients=recipients)
            return await self.gather(response_queue, request.correlation_id, expected, timeout)
        finally:
            self._broker.unsubscribe(self._response_topic, response_queue)
