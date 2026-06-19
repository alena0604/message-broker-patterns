import asyncio

import fakeredis.aioredis

from message_broker_patterns.saga_pattern.broker import SagaBroker
from message_broker_patterns.saga_pattern.events import (
    STREAM_ORDERS,
    STREAM_PAYMENTS,
    STREAM_SHIPPING,
)
from message_broker_patterns.saga_pattern.models import Order, SagaStatus
from message_broker_patterns.saga_pattern.runner import run_saga
from message_broker_patterns.saga_pattern.services.order_service import OrderService
from message_broker_patterns.saga_pattern.services.payment_service import PaymentService
from message_broker_patterns.saga_pattern.services.shipping_service import ShippingService


async def test_happy_path_saga(
    saga_broker: SagaBroker,
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    order_svc = OrderService(saga_broker)
    payment_svc = PaymentService(saga_broker)
    shipping_svc = ShippingService(saga_broker)
    order = Order(order_id="happy-1", customer_id="c-1", amount=99.99)

    stop = asyncio.Event()
    await run_saga(
        order_svc, payment_svc, shipping_svc, saga_broker, order, stop, poll_interval=0.01
    )

    assert order_svc.get_order("happy-1").status == SagaStatus.COMPLETED  # type: ignore[union-attr]

    orders_msgs = await fake_redis.xrange(STREAM_ORDERS)
    event_types = [m[1][b"event_type"].decode() for m in orders_msgs]
    assert "OrderCreated" in event_types

    payments_msgs = await fake_redis.xrange(STREAM_PAYMENTS)
    payment_types = [m[1][b"event_type"].decode() for m in payments_msgs]
    assert "PaymentProcessed" in payment_types

    shipping_msgs = await fake_redis.xrange(STREAM_SHIPPING)
    shipping_types = [m[1][b"event_type"].decode() for m in shipping_msgs]
    assert "OrderShipped" in shipping_types


async def test_compensation_saga(
    saga_broker: SagaBroker,
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    order_svc = OrderService(saga_broker)
    payment_svc = PaymentService(saga_broker)
    shipping_svc = ShippingService(saga_broker)
    order = Order(order_id="fail-1", customer_id="c-1", amount=-1.0)

    stop = asyncio.Event()
    await run_saga(
        order_svc, payment_svc, shipping_svc, saga_broker, order, stop, poll_interval=0.01
    )

    assert order_svc.get_order("fail-1").status == SagaStatus.CANCELLED  # type: ignore[union-attr]

    payments_msgs = await fake_redis.xrange(STREAM_PAYMENTS)
    payment_types = [m[1][b"event_type"].decode() for m in payments_msgs]
    assert "PaymentFailed" in payment_types

    orders_msgs = await fake_redis.xrange(STREAM_ORDERS)
    order_event_types = [m[1][b"event_type"].decode() for m in orders_msgs]
    assert "OrderCancelled" in order_event_types

    # Shipping must NOT have been invoked
    shipping_msgs = await fake_redis.xrange(STREAM_SHIPPING)
    assert shipping_msgs == []
