"""INTENTIONALLY INCORRECT — the inline-payload baseline the claim check replaces.

This module exists to be demonstrated *failing*. Its bug is its contract:
``tests/unit/message_broker_patterns/claim_check_pattern/test_naive.py`` fails if
the bug is repaired.

**The invariant it violates.** ``claim_check_pattern/README.md``: *"The payload
never touches the broker."* The real producer writes the bytes to storage and
publishes only a ``ClaimCheck`` — a couple of hundred bytes, whatever the payload
weighs.

**What this does instead.** It publishes the payload itself, which is what a
queue's API invites you to do: ``await broker.publish(message)``. There is no
size check anywhere in the producer, so the ceiling is discovered by the broker,
in production, on the first genuinely large file.

The ceiling here is real, not invented: :data:`MAX_MESSAGE_BYTES` is 1 MiB —
Kafka's default producer-side ``max.request.size``, sitting just under the
broker-side ``message.max.bytes`` that backs it (SQS caps at 256 KiB, RabbitMQ
degrades badly on large frames well before its own limit). Exceeding it is not a slow
publish — it is a **rejected** publish, and the message is simply gone unless the
producer has somewhere else to put it. Under the limit the cost is quieter and
worse: every byte of every payload crosses the broker twice (in and out) and sits
in its memory in between, which is how a broker ends up doing a file server's job
badly.
"""

from __future__ import annotations

import asyncio
import json
import logging

from message_broker_patterns.claim_check_pattern.models import Payload

logger = logging.getLogger(__name__)

# 1 MiB — Kafka's default producer ``max.request.size``. A real ceiling from a
# real broker; the naive producer just never checks against it.
MAX_MESSAGE_BYTES = 1_048_576


class MessageTooLargeError(RuntimeError):
    """The broker refused the message because it exceeded the size limit.

    The realistic failure mode: not a timeout, not slowness — a hard rejection
    at publish time, after the producer has already read the whole file into
    memory.
    """


def encode_inline(payload: Payload) -> bytes:
    """Serialize the *whole* payload — metadata header plus every raw byte."""
    header = json.dumps(
        {
            "content_type": payload.content_type,
            "original_name": payload.original_name,
            "size_bytes": payload.size_bytes,
        }
    ).encode("utf-8")
    return header + b"\n" + payload.data


class InlinePayloadBroker:
    """An ``asyncio.Queue`` broker that enforces a broker-realistic size limit.

    ``bytes_transferred`` accumulates every byte that crossed it, so a demo can
    put the naive number next to ``ClaimCheck.wire_size_bytes()`` and show the
    two-orders-of-magnitude gap.
    """

    def __init__(self, max_message_bytes: int = MAX_MESSAGE_BYTES) -> None:
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self.max_message_bytes = max_message_bytes
        self.bytes_transferred = 0

    async def publish(self, message: bytes) -> int:
        """Enqueue the raw message, or reject it for being over the limit."""
        if len(message) > self.max_message_bytes:
            logger.error(
                "Broker rejected a %d byte message (limit %d)",
                len(message),
                self.max_message_bytes,
            )
            raise MessageTooLargeError(
                f"message of {len(message)} bytes exceeds the broker limit "
                f"of {self.max_message_bytes} bytes"
            )
        await self._queue.put(message)
        self.bytes_transferred += len(message)
        logger.debug("Broker carried %d bytes inline", len(message))
        return len(message)

    async def get(self) -> bytes:
        """Dequeue the next raw message."""
        return await self._queue.get()

    def qsize(self) -> int:
        return self._queue.qsize()


async def publish_payload_inline(broker: InlinePayloadBroker, payload: Payload) -> int:
    """Publish the payload straight through the broker. Returns the wire size.

    Note the absence of a store, a claim id, and — crucially — any size check
    before the call. The producer finds out the payload was too big by catching
    the broker's rejection, at which point the bytes have nowhere to go.
    """
    message = encode_inline(payload)
    wire_size = await broker.publish(message)  # ← the payload IS the message.
    logger.info(
        "Published %s inline: %d bytes across the broker",
        payload.original_name,
        wire_size,
    )
    return wire_size
