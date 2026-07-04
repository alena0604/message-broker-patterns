from __future__ import annotations

import asyncio
import logging

from message_broker_patterns.claim_check_pattern.models import ClaimCheck
from message_broker_patterns.metrics import REGISTRY

logger = logging.getLogger(__name__)


class ClaimCheckBroker:
    """An in-memory broker (an ``asyncio.Queue``) that carries claim checks only.

    The whole point of the pattern is that only the small :class:`ClaimCheck`
    travels through the broker — never the heavy payload. A stdlib
    ``asyncio.Queue`` is enough to demonstrate that: this pattern does not need
    a real external broker, so it uses a stdlib primitive rather than Redis.
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[ClaimCheck] = asyncio.Queue()

    async def publish(self, claim_check: ClaimCheck) -> None:
        """Enqueue a claim check for consumers to redeem."""
        await self._queue.put(claim_check)
        wire_size = claim_check.wire_size_bytes()
        bytes_saved = max(claim_check.size_bytes - wire_size, 0)
        REGISTRY.increment("claim_check", "claim_checks_published")
        REGISTRY.increment("claim_check", "broker_bytes_saved", bytes_saved)
        logger.debug(
            "Published claim=%s (%s, %d bytes payload, %d bytes on the wire, %d bytes saved)",
            claim_check.claim_id,
            claim_check.original_name,
            claim_check.size_bytes,
            wire_size,
            bytes_saved,
        )

    async def get(self, timeout: float | None = None) -> ClaimCheck | None:
        """Dequeue the next claim check.

        With ``timeout=None`` this blocks until a claim is available. With a
        positive ``timeout`` it waits at most that many seconds and returns
        ``None`` on expiry, so a consumer loop can poll and still observe its
        stop signal instead of blocking forever.
        """
        if timeout is None:
            return await self._queue.get()
        try:
            return await asyncio.wait_for(self._queue.get(), timeout)
        except TimeoutError:
            return None

    def qsize(self) -> int:
        """Number of claim checks currently waiting in the queue."""
        return self._queue.qsize()

    def empty(self) -> bool:
        """Whether the queue currently has no waiting claim checks."""
        return self._queue.empty()
