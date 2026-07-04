from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


@dataclass
class Payload:
    """A large object that is too big to travel through the broker directly.

    This is the *heavy* half of the pattern: the raw ``data`` bytes plus just
    enough metadata to reconstruct the object after it is fetched back out of
    storage. It is handed to the producer, written to external storage, and
    never published on the broker itself.
    """

    data: bytes
    content_type: str
    original_name: str

    @property
    def size_bytes(self) -> int:
        """Number of bytes in the payload — the reason it needs a claim check."""
        return len(self.data)


@dataclass
class ClaimCheck:
    """A lightweight reference token that stands in for a stored payload.

    This is the *light* half of the pattern: it carries only the storage key
    (``claim_id``) and descriptive metadata, never the bytes. It is the single
    thing published on the broker — small enough to slip under any broker size
    limit. A consumer redeems it against the store to fetch the real payload.
    """

    claim_id: str
    content_type: str
    original_name: str
    size_bytes: int
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def wire_size_bytes(self) -> int:
        """Size of this claim check if sent over the broker as JSON.

        This is what actually crosses the broker — used to report how much
        traffic the pattern saves versus sending the full payload.
        """
        encoded = {
            "claim_id": self.claim_id,
            "content_type": self.content_type,
            "original_name": self.original_name,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at.isoformat(),
        }
        return len(json.dumps(encoded).encode("utf-8"))
