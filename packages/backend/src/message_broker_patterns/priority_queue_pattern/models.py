from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum

logger = logging.getLogger(__name__)


class Priority(StrEnum):
    """The priority level a support ticket is routed by.

    The string value doubles as the stream suffix and the metrics-counter
    prefix, so a single enum drives routing, instrumentation, and display.
    """

    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


@dataclass
class SupportTicket:
    """A customer support ticket placed onto the priority queue."""

    ticket_id: str
    subject: str
    priority: Priority
    customer_id: str

    def to_fields(self) -> dict[str, str]:
        """Serialize to the flat string->string mapping Redis Streams stores."""
        return {
            "ticket_id": self.ticket_id,
            "subject": self.subject,
            "priority": self.priority.value,
            "customer_id": self.customer_id,
        }

    @classmethod
    def from_fields(cls, fields: dict[bytes, bytes]) -> SupportTicket:
        """Reconstruct a SupportTicket from raw Redis Stream fields (bytes->bytes)."""
        return cls(
            ticket_id=fields[b"ticket_id"].decode(),
            subject=fields[b"subject"].decode(),
            priority=Priority(fields[b"priority"].decode()),
            customer_id=fields[b"customer_id"].decode(),
        )
