import fakeredis.aioredis

from message_broker_patterns.saga_pattern.broker import SagaBroker
from message_broker_patterns.saga_pattern.events import (
    STREAM_ORDERS,
    OrderShipped,
    PaymentFailed,
    PaymentProcessed,
)
from message_broker_patterns.saga_pattern.models import Order, SagaStatus
from message_broker_patterns.saga_pattern.services.order_service import OrderService


async def test_create_order_sets_pending_status(
    saga_broker: SagaBroker,
) -> None:
    svc = OrderService(saga_broker)
    order = Order(order_id="o-1", customer_id="c-1", amount=10.0)
    svc.create_order(order)
    assert svc.get_order("o-1") is not None
    assert svc.get_order("o-1").status == SagaStatus.PENDING  # type: ignore[union-attr]


async def test_publish_created_sets_payment_processing(
    saga_broker: SagaBroker,
) -> None:
    svc = OrderService(saga_broker)
    order = Order(order_id="o-2", customer_id="c-1", amount=10.0)
    svc.create_order(order)
    await svc.publish_created(order)
    assert order.status == SagaStatus.PAYMENT_PROCESSING


async def test_handle_payment_processed_sets_paid(
    saga_broker: SagaBroker,
) -> None:
    svc = OrderService(saga_broker)
    order = Order(order_id="o-3", customer_id="c-1", amount=10.0)
    svc.create_order(order)
    await svc.handle_payment_processed(PaymentProcessed(order_id="o-3", transaction_id="tx-1"))
    assert svc.get_order("o-3").status == SagaStatus.PAID  # type: ignore[union-attr]


async def test_handle_payment_failed_cancels_and_publishes_compensation(
    saga_broker: SagaBroker,
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    svc = OrderService(saga_broker)
    order = Order(order_id="o-4", customer_id="c-1", amount=10.0)
    svc.create_order(order)
    await svc.handle_payment_failed(PaymentFailed(order_id="o-4", reason="insufficient funds"))
    assert svc.get_order("o-4").status == SagaStatus.CANCELLED  # type: ignore[union-attr]
    msgs = await fake_redis.xrange(STREAM_ORDERS)
    assert len(msgs) == 1
    assert msgs[0][1][b"event_type"] == b"OrderCancelled"
    assert msgs[0][1][b"order_id"] == b"o-4"


async def test_handle_order_shipped_sets_completed(
    saga_broker: SagaBroker,
) -> None:
    svc = OrderService(saga_broker)
    order = Order(order_id="o-5", customer_id="c-1", amount=10.0)
    svc.create_order(order)
    await svc.handle_order_shipped(OrderShipped(order_id="o-5", tracking_number="TRACK-1"))
    assert svc.get_order("o-5").status == SagaStatus.COMPLETED  # type: ignore[union-attr]
