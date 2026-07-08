from __future__ import annotations

import asyncio
import logging
from typing import Any

from message_broker_patterns.metrics import REGISTRY

logger = logging.getLogger(__name__)

# Pattern id used when reporting counters to the shared metrics registry.
PATTERN_ID = "scatter_gather"


class InMemoryTopicBroker:
    """A dependency-free, in-process fan-out message broker built on asyncio.

    Every ``subscribe`` hands back its own :class:`asyncio.Queue`; ``publish``
    copies the message into *every* subscriber queue for that topic. That
    per-subscriber copy is what makes both distribution strategies fall out of
    one primitive:

    * **Publish-subscribe** — many services subscribe to one broadcast topic;
      a single ``publish`` reaches all of them.
    * **Recipient list** — the scatterer knows each recipient's dedicated topic
      and publishes to them one by one.

    This pattern needs neither persistence nor acknowledgement, so per ADR-0002
    it uses stdlib primitives rather than Redis Streams.
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, list[asyncio.Queue[Any]]] = {}

    def subscribe(self, topic: str) -> asyncio.Queue[Any]:
        """Register a fresh queue for ``topic`` and return it to the caller.

        Subscribe *before* the matching scatter runs: ``publish`` only reaches
        queues that already exist, so a late subscriber silently misses messages.
        """
        queue: asyncio.Queue[Any] = asyncio.Queue()
        self._subscribers.setdefault(topic, []).append(queue)
        logger.debug(
            "Subscribed a queue to topic %s (now %d)", topic, len(self._subscribers[topic])
        )
        return queue

    def unsubscribe(self, topic: str, queue: asyncio.Queue[Any]) -> None:
        """Detach ``queue`` from ``topic`` so later publishes skip it.

        Idempotent: unsubscribing a queue that is already gone is a no-op. The
        aggregator calls this once a gather finishes so a completed search's
        queue stops absorbing stray late responses.
        """
        queues = self._subscribers.get(topic)
        if not queues:
            return
        try:
            queues.remove(queue)
        except ValueError:
            return
        logger.debug("Unsubscribed a queue from topic %s (now %d)", topic, len(queues))

    async def publish(self, topic: str, message: Any) -> int:
        """Fan ``message`` out to every current subscriber of ``topic``.

        Returns the number of subscriber queues it reached — ``0`` when nobody
        is listening (a broadcast into the void, not an error).
        """
        queues = list(self._subscribers.get(topic, ()))
        for queue in queues:
            await queue.put(message)
        REGISTRY.increment(PATTERN_ID, "messages_published")
        logger.debug("Published to topic %s → %d subscriber(s)", topic, len(queues))
        return len(queues)

    def subscriber_count(self, topic: str) -> int:
        """Number of live subscriber queues on ``topic`` (used by tests/demos)."""
        return len(self._subscribers.get(topic, ()))
