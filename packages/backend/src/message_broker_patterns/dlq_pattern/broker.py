import logging

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from message_broker_patterns.dlq_pattern.models import Payment
from message_broker_patterns.metrics import REGISTRY

logger = logging.getLogger(__name__)

# Two streams: the main pipeline every payment enters, and the dead-letter
# queue that collects poison messages once they exhaust their retry budget.
MAIN_STREAM = "payments:main"
DLQ_STREAM = "payments:dlq"

# Redis Set of payment ids that have been successfully processed. The consumer
# checks it before running a handler so a replayed payment is skipped rather
# than charged a second time — the idempotency guard.
PROCESSED_SET = "payments:processed"

# Raised by XGROUP CREATE when the group already exists. Swallowed so group
# creation is idempotent across many consumer startups.
_BUSYGROUP = "BUSYGROUP"


class DLQBroker:
    """Thin async wrapper over two Redis Streams plus an idempotency set.

    Payments enter ``payments:main``; a consumer that fails a payment
    ``max_attempts`` times moves it to ``payments:dlq`` with the failure reason
    and attempt count attached. Successfully processed payment ids are recorded
    in ``payments:processed`` so replays are deduplicated.
    """

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def publish(self, payment: Payment) -> str:
        """Append a payment to the main stream; return the message id."""
        msg_id: bytes | str = await self._client.xadd(MAIN_STREAM, payment.to_fields())
        decoded = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        REGISTRY.increment("dlq", "payments_published")
        logger.debug(
            "Published payment %s → %s id=%s",
            payment.payment_id,
            MAIN_STREAM,
            decoded,
        )
        return decoded

    async def ensure_group(self, stream: str, group: str) -> bool:
        """Create the group on one stream (creating the stream too).

        Idempotent: returns ``True`` when created, ``False`` when it already
        existed. Any non-BUSYGROUP ResponseError is re-raised.
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

    async def ensure_all_groups(self, group: str) -> None:
        """Create the group on both the main and DLQ streams."""
        for stream in (MAIN_STREAM, DLQ_STREAM):
            await self.ensure_group(stream, group)

    async def read_new(
        self,
        group: str,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> list[tuple[str, dict[bytes, bytes]]]:
        """Claim up to ``count`` never-delivered messages from the main stream.

        Blocks up to ``block_ms`` ms when the stream is empty. A ``block_ms`` of
        ``0`` (or less) means a truly non-blocking poll — no ``BLOCK`` argument
        is sent, so Redis returns immediately. Returns ``(message_id, fields)``
        pairs; an empty list on timeout or when nothing is waiting.
        """
        block = block_ms if block_ms > 0 else None
        results = await self._client.xreadgroup(
            group, consumer, {MAIN_STREAM: ">"}, count=count, block=block
        )
        if not results:
            return []
        return [(msg_id.decode(), fields) for msg_id, fields in results[0][1]]

    async def read_pending(
        self,
        group: str,
        consumer: str,
        count: int,
    ) -> list[tuple[str, dict[bytes, bytes]]]:
        """Re-read this consumer's own delivered-but-unacked backlog (id ``0``).

        Redis only redelivers a never-acked message when the consumer explicitly
        re-reads its pending entries list — a ``>`` read returns *new* messages
        only. This is how a failed payment that was intentionally left unacked
        gets retried on a later iteration until it exhausts its attempt budget.
        Returns ``(message_id, fields)`` pairs; an empty list when nothing is
        pending for this consumer.
        """
        results = await self._client.xreadgroup(
            group, consumer, {MAIN_STREAM: "0"}, count=count, block=None
        )
        if not results:
            return []
        return [(msg_id.decode(), fields) for msg_id, fields in results[0][1]]

    async def ack(self, group: str, message_id: str) -> int:
        """Acknowledge a processed main-stream message so it leaves the pending list."""
        acked: int = await self._client.xack(MAIN_STREAM, group, message_id)
        return acked

    async def move_to_dlq(
        self,
        group: str,
        message_id: str,
        payment: Payment,
        reason: str,
        attempt: int,
    ) -> str:
        """Route a poison message to the DLQ: ack it off main, add it to the DLQ.

        The DLQ entry carries the full payment fields plus the failure
        ``reason`` and the ``attempt`` count that exhausted the retry budget, so
        an operator can inspect why it failed before replaying it.
        """
        await self._client.xack(MAIN_STREAM, group, message_id)
        fields = {**payment.to_fields(), "reason": reason, "attempt": str(attempt)}
        dlq_id: bytes | str = await self._client.xadd(DLQ_STREAM, fields)
        decoded = dlq_id.decode() if isinstance(dlq_id, bytes) else dlq_id
        REGISTRY.increment("dlq", "moved_to_dlq")
        logger.warning(
            "Moved payment %s → %s id=%s (reason=%s attempt=%d)",
            payment.payment_id,
            DLQ_STREAM,
            decoded,
            reason,
            attempt,
        )
        return decoded

    async def is_processed(self, payment_id: str) -> bool:
        """Return whether this payment id is already in the processed set."""
        member: int = await self._client.sismember(PROCESSED_SET, payment_id)
        return bool(member)

    async def mark_processed(self, payment_id: str) -> None:
        """Record a payment id in the processed set (the idempotency guard)."""
        await self._client.sadd(PROCESSED_SET, payment_id)

    async def read_dlq(
        self,
        group: str,
        consumer: str,
        count: int,
    ) -> list[tuple[str, dict[bytes, bytes]]]:
        """Non-blocking claim of up to ``count`` messages from the DLQ stream."""
        results = await self._client.xreadgroup(
            group, consumer, {DLQ_STREAM: ">"}, count=count, block=None
        )
        if not results:
            return []
        return [(msg_id.decode(), fields) for msg_id, fields in results[0][1]]

    async def ack_dlq(self, group: str, message_id: str) -> int:
        """Acknowledge a processed DLQ-stream message."""
        acked: int = await self._client.xack(DLQ_STREAM, group, message_id)
        return acked

    async def close(self) -> None:
        await self._client.aclose()
