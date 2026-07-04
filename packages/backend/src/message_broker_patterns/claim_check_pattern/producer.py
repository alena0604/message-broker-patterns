from __future__ import annotations

import logging

from message_broker_patterns.claim_check_pattern.broker import ClaimCheckBroker
from message_broker_patterns.claim_check_pattern.models import ClaimCheck, Payload
from message_broker_patterns.claim_check_pattern.storage import FilesystemPayloadStore
from message_broker_patterns.metrics import REGISTRY

logger = logging.getLogger(__name__)


class ClaimCheckProducer:
    """Stores a large payload, then publishes only its claim check.

    The producer holds the two collaborators the pattern needs — the external
    ``store`` and the ``broker`` — and wires them together: ``publish`` writes
    the payload to storage, wraps the returned storage key in a
    :class:`ClaimCheck` with the payload's metadata, and puts *that* on the
    broker. The heavy bytes never touch the broker.
    """

    def __init__(self, broker: ClaimCheckBroker, store: FilesystemPayloadStore) -> None:
        self._broker = broker
        self._store = store

    async def publish(self, payload: Payload) -> ClaimCheck:
        """Store ``payload`` externally, publish its claim check, return the claim."""
        claim_id = self._store.put(payload.data)
        storage_path = self._store.path_for(claim_id)
        REGISTRY.increment("claim_check", "payloads_stored")
        claim = ClaimCheck(
            claim_id=claim_id,
            content_type=payload.content_type,
            original_name=payload.original_name,
            size_bytes=payload.size_bytes,
        )
        await self._broker.publish(claim)
        logger.info(
            "Checked in payload %s (%d bytes) → claim=%s path=%s",
            payload.original_name,
            payload.size_bytes,
            claim_id,
            storage_path,
        )
        return claim
