from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Payment:
    """A payment flowing through the DLQ pipeline.

    ``payment_id`` is unique per payment and doubles as the idempotency key: the
    consumer records it in the processed set so a replayed payment is skipped
    rather than charged twice. A negative ``amount_cents`` is treated as
    malformed by the demo consumer and routed to the dead-letter queue.
    """

    payment_id: str
    amount_cents: int
    customer_id: str
    currency: str

    def to_fields(self) -> dict[str, str]:
        """Serialize to the flat string->string mapping Redis Streams stores."""
        return {
            "payment_id": self.payment_id,
            "amount_cents": str(self.amount_cents),
            "customer_id": self.customer_id,
            "currency": self.currency,
        }

    @classmethod
    def from_fields(cls, fields: dict[bytes, bytes]) -> Payment:
        """Reconstruct a Payment from raw Redis Stream fields (bytes->bytes)."""
        return cls(
            payment_id=fields[b"payment_id"].decode(),
            amount_cents=int(fields[b"amount_cents"].decode()),
            customer_id=fields[b"customer_id"].decode(),
            currency=fields[b"currency"].decode(),
        )
