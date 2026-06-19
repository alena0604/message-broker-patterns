from __future__ import annotations

from dataclasses import dataclass

STREAM_ORDERS = "saga:orders"
STREAM_PAYMENTS = "saga:payments"
STREAM_SHIPPING = "saga:shipping"


@dataclass(frozen=True)
class OrderCreated:
    event_type: str = "OrderCreated"
    order_id: str = ""
    customer_id: str = ""
    amount: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "event_type": self.event_type,
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "amount": self.amount,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> OrderCreated:
        return cls(
            order_id=data["order_id"],
            customer_id=data["customer_id"],
            amount=data["amount"],
        )


@dataclass(frozen=True)
class PaymentProcessed:
    event_type: str = "PaymentProcessed"
    order_id: str = ""
    transaction_id: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "event_type": self.event_type,
            "order_id": self.order_id,
            "transaction_id": self.transaction_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> PaymentProcessed:
        return cls(
            order_id=data["order_id"],
            transaction_id=data["transaction_id"],
        )


@dataclass(frozen=True)
class PaymentFailed:
    event_type: str = "PaymentFailed"
    order_id: str = ""
    reason: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "event_type": self.event_type,
            "order_id": self.order_id,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> PaymentFailed:
        return cls(
            order_id=data["order_id"],
            reason=data["reason"],
        )


@dataclass(frozen=True)
class OrderShipped:
    event_type: str = "OrderShipped"
    order_id: str = ""
    tracking_number: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "event_type": self.event_type,
            "order_id": self.order_id,
            "tracking_number": self.tracking_number,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> OrderShipped:
        return cls(
            order_id=data["order_id"],
            tracking_number=data["tracking_number"],
        )


@dataclass(frozen=True)
class OrderCancelled:
    event_type: str = "OrderCancelled"
    order_id: str = ""
    reason: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "event_type": self.event_type,
            "order_id": self.order_id,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> OrderCancelled:
        return cls(
            order_id=data["order_id"],
            reason=data["reason"],
        )
