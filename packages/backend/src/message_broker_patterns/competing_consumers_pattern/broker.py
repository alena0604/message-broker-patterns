import logging

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

logger = logging.getLogger(__name__)

# Raised by XGROUP CREATE when the group already exists. Treated as a no-op so
# group creation is idempotent across many competing-consumer startups.
_BUSYGROUP = "BUSYGROUP"


class CompetingConsumersBroker:
    """Thin async wrapper over the Redis Streams consumer-group commands.

    A consumer group is what turns a single stream into a load balancer: every
    consumer in the group reads with ``XREADGROUP ... >`` and the broker hands
    each new message to exactly one consumer. Unacked messages stay pending and
    can be reclaimed by a surviving sibling via ``XAUTOCLAIM``/``XCLAIM``.
    """

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def publish(self, stream: str, task_fields: dict[str, str]) -> str:
        """Append a task to the stream and return its generated message id."""
        msg_id: bytes | str = await self._client.xadd(stream, task_fields)
        decoded = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        logger.debug("Published to stream %s: id=%s", stream, decoded)
        return decoded

    async def ensure_group(self, stream: str, group: str) -> bool:
        """Create the consumer group, creating the stream too if missing.

        Idempotent: returns ``True`` when the group was created and ``False``
        when it already existed (the BUSYGROUP error is swallowed). Any other
        ResponseError is re-raised.
        """
        try:
            await self._client.xgroup_create(stream, group, id="0", mkstream=True)
        except ResponseError as exc:
            if _BUSYGROUP in str(exc):
                logger.debug("Consumer group %s already exists on %s", group, stream)
                return False
            raise
        logger.debug("Created consumer group %s on stream %s", group, stream)
        return True

    async def read_new(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> list[tuple[str, dict[bytes, bytes]]]:
        """Claim up to ``count`` never-before-delivered messages for this consumer.

        Blocks up to ``block_ms`` milliseconds when the stream is empty. Returns
        ``(message_id, fields)`` pairs; an empty list on timeout.
        """
        results = await self._client.xreadgroup(
            group, consumer, {stream: ">"}, count=count, block=block_ms
        )
        if not results:
            return []
        return [(msg_id.decode(), fields) for msg_id, fields in results[0][1]]

    async def ack(self, stream: str, group: str, message_id: str) -> int:
        """Acknowledge a processed message so it leaves the pending list."""
        acked: int = await self._client.xack(stream, group, message_id)
        return acked

    async def reclaim_stale(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_ms: int,
        count: int,
    ) -> list[tuple[str, dict[bytes, bytes]]]:
        """Reclaim pending messages idle longer than ``min_idle_ms`` for this consumer.

        This is the crash-recovery path: messages a dead consumer read but never
        acked become claimable here so a surviving sibling can finish them.
        Returns ``(message_id, fields)`` pairs that are now owned by ``consumer``.
        """
        _next, claimed, _deleted = await self._client.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time=min_idle_ms,
            start_id="0",
            count=count,
        )
        return [(msg_id.decode(), fields) for msg_id, fields in claimed]

    async def pending_count(self, stream: str, group: str) -> int:
        """Number of delivered-but-unacked messages across the whole group."""
        summary = await self._client.xpending(stream, group)
        count: int = summary["pending"]
        return count

    async def close(self) -> None:
        await self._client.aclose()
