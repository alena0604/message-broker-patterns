from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from message_broker_patterns.claim_check_pattern.broker import ClaimCheckBroker
from message_broker_patterns.claim_check_pattern.models import ClaimCheck, Payload
from message_broker_patterns.claim_check_pattern.storage import FilesystemPayloadStore
from message_broker_patterns.metrics import REGISTRY

logger = logging.getLogger(__name__)

# A handler receives the consumer id, the claim check it redeemed, and the
# fully-resolved payload fetched back out of storage. It is awaited once per
# successfully-redeemed claim.
Handler = Callable[[str, ClaimCheck, Payload], Awaitable[None]]


async def run_consumer(
    broker: ClaimCheckBroker,
    store: FilesystemPayloadStore,
    handler: Handler,
    stop_event: asyncio.Event,
    *,
    consumer_id: str = "consumer-0",
    delete_after: bool = True,
    poll_timeout: float = 0.05,
) -> int:
    """Redeem claim checks from the broker until stopped; return the count handled.

    Per claim check:

    1. Fetch the real payload bytes from ``store`` using ``claim.claim_id`` and
       rebuild the :class:`Payload` with the claim's metadata.
    2. Run ``handler`` with the resolved payload.
    3. If ``delete_after`` is set, delete the payload from storage now that it
       has been processed.

    ``delete_after`` defaults to ``True`` because claim-check storage is usually
    a temporary staging area worth reclaiming once the payload is consumed. It
    is configurable — deletion is never forced — so a consumer that wants to
    keep the payload (multiple readers, audit, reprocessing) can pass
    ``delete_after=False``.

    The broker read uses ``poll_timeout`` so an idle consumer wakes periodically
    to observe ``stop_event`` instead of blocking on an empty queue forever.
    """
    logger.info("consumer=%s redeeming claim checks (delete_after=%s)", consumer_id, delete_after)
    total_handled = 0
    while not stop_event.is_set():
        claim = await broker.get(timeout=poll_timeout)
        if claim is None:
            continue
        data = store.get(claim.claim_id)
        REGISTRY.increment("claim_check", "payloads_retrieved")
        payload = Payload(
            data=data,
            content_type=claim.content_type,
            original_name=claim.original_name,
        )
        await handler(consumer_id, claim, payload)
        REGISTRY.increment("claim_check", "payloads_processed")
        if delete_after:
            store.delete(claim.claim_id)
            REGISTRY.increment("claim_check", "payloads_deleted")
        total_handled += 1
        logger.info(
            "consumer=%s redeemed claim=%s (%s, %d bytes)%s",
            consumer_id,
            claim.claim_id,
            claim.original_name,
            claim.size_bytes,
            " → deleted" if delete_after else "",
        )
    logger.info("consumer=%s stopping — handled %d payload(s)", consumer_id, total_handled)
    return total_handled
