from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class Order:
    order_id: str
    customer_id: str
    amount: float
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class OutboxEntry:
    order_id: str
    payload: str
    id: int | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
