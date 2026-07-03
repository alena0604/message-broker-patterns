import logging

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from message_broker_patterns.metrics import REGISTRY
from message_broker_patterns.priority_queue_pattern.models import Priority, SupportTicket

logger = logging.getLogger(__name__)

# One stream per priority level. Separate streams (rather than a single stream
# with a priority field) are what make this a *priority* queue: each level is
# drained independently by its own pool of consumers, so an urgent ticket never
# waits behind a backlog of routine ones.
STREAMS: dict[Priority, str] = {
    Priority.HIGH: "support:high",
    Priority.NORMAL: "support:normal",
    Priority.LOW: "support:low",
}

# Raised by XGROUP CREATE when the group already exists. Swallowed so group
# creation is idempotent across many consumer startups.
_BUSYGROUP = "BUSYGROUP"


class PriorityQueueBroker:
    """Thin async wrapper over Redis Streams, one stream + group per priority.

    Routing is decided entirely by ``SupportTicket.priority``: ``publish`` picks
    the matching stream, and consumers attach to exactly one priority's stream.
    """

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def publish(self, ticket: SupportTicket) -> str:
        """Append a ticket to its priority's stream; return the message id."""
        stream = STREAMS[ticket.priority]
        msg_id: bytes | str = await self._client.xadd(stream, ticket.to_fields())
        decoded = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        REGISTRY.increment("priority_queue", "tickets_published")
        REGISTRY.increment("priority_queue", f"{ticket.priority.value}_published")
        logger.debug(
            "Published ticket %s (%s) → %s id=%s",
            ticket.ticket_id,
            ticket.priority.value,
            stream,
            decoded,
        )
        return decoded

    async def ensure_group(self, priority: Priority, group: str) -> bool:
        """Create the group for one priority's stream (creating the stream too).

        Idempotent: returns ``True`` when created, ``False`` when it already
        existed. Any non-BUSYGROUP ResponseError is re-raised.
        """
        stream = STREAMS[priority]
        try:
            await self._client.xgroup_create(stream, group, id="0", mkstream=True)
        except ResponseError as exc:
            if _BUSYGROUP in str(exc):
                logger.debug("Consumer group %s already exists on %s", group, stream)
                return False
            raise
        logger.debug("Created consumer group %s on stream %s", group, stream)
        return True

    async def ensure_all_groups(self, group: str) -> None:
        """Create the group on all three priority streams."""
        for priority in Priority:
            await self.ensure_group(priority, group)

    async def read_new(
        self,
        priority: Priority,
        group: str,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> list[tuple[str, dict[bytes, bytes]]]:
        """Claim up to ``count`` never-delivered messages from one priority stream.

        Blocks up to ``block_ms`` ms when the stream is empty. A ``block_ms`` of
        ``0`` (or less) means a truly non-blocking poll — no ``BLOCK`` argument
        is sent, so Redis returns immediately. (Passing ``BLOCK 0`` to Redis
        would block *forever*, which is the opposite of what a non-blocking
        strict-priority scan needs.) Returns ``(message_id, fields)`` pairs; an
        empty list on timeout or when nothing is waiting.
        """
        stream = STREAMS[priority]
        block = block_ms if block_ms > 0 else None
        results = await self._client.xreadgroup(
            group, consumer, {stream: ">"}, count=count, block=block
        )
        if not results:
            return []
        return [(msg_id.decode(), fields) for msg_id, fields in results[0][1]]

    async def ack(self, priority: Priority, group: str, message_id: str) -> int:
        """Acknowledge a processed message so it leaves the pending list."""
        acked: int = await self._client.xack(STREAMS[priority], group, message_id)
        return acked

    async def pending_count(self, priority: Priority, group: str) -> int:
        """Number of delivered-but-unacked messages for one priority's group."""
        summary = await self._client.xpending(STREAMS[priority], group)
        count: int = summary["pending"]
        return count

    async def close(self) -> None:
        await self._client.aclose()
