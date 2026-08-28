"""The naive no-compensation baseline must strand cross-service state.

Every assertion turns red if ``saga_pattern/naive.py`` grows a rollback path
(a refund, an un-ship, a cancel), which is precisely what the saga pattern adds.
"""

from __future__ import annotations

import pytest

from message_broker_patterns.saga_pattern.models import Order, SagaStatus
from message_broker_patterns.saga_pattern.naive import (
    NaiveOrderService,
    NaivePaymentService,
    NaiveShippingService,
    PaymentDeclinedError,
    ShippingUnavailableError,
    place_order_without_compensation,
)

AMOUNT = 42.0
AMOUNT_CENTS = 4200


def _order(order_id: str) -> Order:
    return Order(order_id=order_id, customer_id="cust-1", amount=AMOUNT)


async def test_naive_saga_leaves_customer_charged_when_shipping_fails() -> None:
    # Arrange — shipping refuses the order the customer is about to pay for.
    order_svc = NaiveOrderService()
    payment_svc = NaivePaymentService()
    shipping_svc = NaiveShippingService(reject_for={"ord-1"})
    order = _order("ord-1")

    # Act.
    with pytest.raises(ShippingUnavailableError):
        await place_order_without_compensation(order_svc, payment_svc, shipping_svc, order)

    # Assert — money taken, nothing shipped, and no compensation ran.
    # A refund would drive net_cents to 0; there is no refund.
    assert payment_svc.net_cents("ord-1") == AMOUNT_CENTS
    assert shipping_svc.shipments == []


async def test_naive_saga_strands_the_order_in_a_non_terminal_state() -> None:
    # Arrange.
    order_svc = NaiveOrderService()
    payment_svc = NaivePaymentService()
    shipping_svc = NaiveShippingService(reject_for={"ord-1"})
    order = _order("ord-1")

    # Act.
    with pytest.raises(ShippingUnavailableError):
        await place_order_without_compensation(order_svc, payment_svc, shipping_svc, order)

    # Assert — the order sits at PAID forever: never COMPLETED, never CANCELLED.
    # The real saga would have moved it to CANCELLED via a compensating event.
    stored = order_svc.get_order("ord-1")
    assert stored is not None
    assert stored.status is SagaStatus.PAID


async def test_naive_saga_is_consistent_only_when_the_failure_comes_first() -> None:
    """A step-2 failure looks fine — which is why the bug ships to production.

    Nothing has been committed downstream yet when the payment declines, so the
    naive sequence is accidentally correct here. The defect only appears once a
    *later* step fails, and that is the case the previous tests pin.
    """
    # Arrange — the payment declines, before any irreversible side effect.
    order_svc = NaiveOrderService()
    payment_svc = NaivePaymentService(decline_for={"ord-1"})
    shipping_svc = NaiveShippingService()

    # Act.
    with pytest.raises(PaymentDeclinedError):
        await place_order_without_compensation(
            order_svc, payment_svc, shipping_svc, _order("ord-1")
        )

    # Assert — no money held, nothing shipped: no inconsistency to compensate.
    assert payment_svc.net_cents("ord-1") == 0
    assert shipping_svc.shipments == []


async def test_naive_saga_happy_path_still_completes() -> None:
    # Arrange — nothing fails.
    order_svc = NaiveOrderService()
    payment_svc = NaivePaymentService()
    shipping_svc = NaiveShippingService()
    order = _order("ord-1")

    # Act.
    tracking_number = await place_order_without_compensation(
        order_svc, payment_svc, shipping_svc, order
    )

    # Assert — the baseline is not a strawman: it works until a late step fails.
    assert tracking_number
    assert order.status is SagaStatus.COMPLETED
    assert shipping_svc.shipments == ["ord-1"]
