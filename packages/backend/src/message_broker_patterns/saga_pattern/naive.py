"""INTENTIONALLY INCORRECT — the no-compensation baseline the saga replaces.

This module exists to be demonstrated *failing*. Its bug is its contract:
``tests/unit/message_broker_patterns/saga_pattern/test_naive.py`` fails if the
bug is repaired.

**The invariant it violates.** ``saga_pattern/README.md``: a distributed
transaction becomes *"a sequence of local transactions… if a step fails,
compensating transactions undo the work already done"*. The real
``OrderService.handle_payment_failed`` publishes an ``OrderCancelled``
compensation and moves the order to ``CANCELLED``.

**What this does instead.** The competent-first-attempt orchestration: call each
service directly, in order, inside one ``try``-less function. Every step's local
transaction commits in its *own* service the moment it succeeds. There is no
rollback path — no refund, no un-reserve — because there is nothing to roll back
*to*: the payment service already committed and it is behind an API, not a
shared database connection.

So a failure at step *n* leaves steps *1..n-1* permanently applied. The customer
is charged for an order that never ships, and no code anywhere will ever notice:
the exception unwinds the caller's stack, not the other services' state. Wrapping
the sequence in a database transaction cannot help — the money left the building
via HTTP.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field

from message_broker_patterns.saga_pattern.models import Order, SagaStatus

logger = logging.getLogger(__name__)


class PaymentDeclinedError(RuntimeError):
    """The card was declined — step 2 failed before any money moved."""


class ShippingUnavailableError(RuntimeError):
    """No stock / no courier — step 3 failed *after* the money already moved."""


@dataclass
class NaivePaymentService:
    """A payment service with a ``charge`` and — deliberately — no refund.

    ``ledger`` is the money actually held per order. There is no ``refund``
    method: adding one is exactly the compensating transaction this baseline is
    missing, so its absence is the thing under test.
    """

    decline_for: set[str] = field(default_factory=set)
    ledger: list[tuple[str, int]] = field(default_factory=list)

    def charge(self, order_id: str, amount_cents: int) -> str:
        if order_id in self.decline_for:
            raise PaymentDeclinedError(f"card declined for {order_id}")
        self.ledger.append((order_id, amount_cents))
        transaction_id = str(uuid.uuid4())
        logger.info("Charged %d cents for order %s (tx=%s)", amount_cents, order_id, transaction_id)
        return transaction_id

    def net_cents(self, order_id: str) -> int:
        """Money currently held for an order. A compensation would drive this to 0."""
        return sum(cents for charged_id, cents in self.ledger if charged_id == order_id)


@dataclass
class NaiveShippingService:
    """A shipping service that can refuse — the step that fails too late."""

    reject_for: set[str] = field(default_factory=set)
    shipments: list[str] = field(default_factory=list)

    def ship(self, order_id: str) -> str:
        if order_id in self.reject_for:
            raise ShippingUnavailableError(f"no stock for {order_id}")
        self.shipments.append(order_id)
        tracking_number = str(uuid.uuid4())
        logger.info("Shipped order %s (tracking=%s)", order_id, tracking_number)
        return tracking_number


@dataclass
class NaiveOrderService:
    """Holds order rows. Its own local write is step 1 of the sequence."""

    orders: dict[str, Order] = field(default_factory=dict)

    def create_order(self, order: Order) -> None:
        self.orders[order.order_id] = order
        logger.info("Order %s created (status=%s)", order.order_id, order.status)

    def get_order(self, order_id: str) -> Order | None:
        return self.orders.get(order_id)


async def place_order_without_compensation(
    order_svc: NaiveOrderService,
    payment_svc: NaivePaymentService,
    shipping_svc: NaiveShippingService,
    order: Order,
) -> str:
    """Create → charge → ship, sequentially, with no rollback of earlier steps.

    Each call commits inside its own service. When ``ship`` raises, the charge
    from the previous line stays on the customer's card and the order row stays
    in ``PAID`` forever — paid but never shipped, and never ``CANCELLED``: an
    in-between state no consumer of this system can interpret and no operator
    will be told about.
    """
    order_svc.create_order(order)  # ← step 1: local commit, order service.
    order.status = SagaStatus.PAYMENT_PROCESSING

    payment_svc.charge(order.order_id, int(order.amount * 100))  # ← step 2: money moves.
    order.status = SagaStatus.PAID

    # ← step 3 may raise. Nothing below runs, and nothing above is undone:
    #   there is no `except` that refunds, because there is no refund.
    tracking_number = shipping_svc.ship(order.order_id)
    order.status = SagaStatus.COMPLETED
    return tracking_number
