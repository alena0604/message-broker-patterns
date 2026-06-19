from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum


class SagaStatus(StrEnum):
    PENDING = "pending"
    PAYMENT_PROCESSING = "payment_processing"
    PAID = "paid"
    SHIPPED = "shipped"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class Order:
    order_id: str
    customer_id: str
    amount: float
    status: SagaStatus = SagaStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
