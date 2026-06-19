import fakeredis.aioredis

from message_broker_patterns.saga_pattern.broker import SagaBroker
from message_broker_patterns.saga_pattern.events import STREAM_PAYMENTS, OrderCreated
from message_broker_patterns.saga_pattern.services.payment_service import PaymentService


async def test_successful_payment_publishes_payment_processed(
    saga_broker: SagaBroker,
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    svc = PaymentService(saga_broker)
    event = OrderCreated(order_id="o-1", customer_id="c-1", amount="99.99")
    await svc.handle_order_created(event)
    msgs = await fake_redis.xrange(STREAM_PAYMENTS)
    assert len(msgs) == 1
    assert msgs[0][1][b"event_type"] == b"PaymentProcessed"
    assert msgs[0][1][b"order_id"] == b"o-1"
    assert b"transaction_id" in msgs[0][1]


async def test_failed_payment_publishes_payment_failed(
    saga_broker: SagaBroker,
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    svc = PaymentService(saga_broker)
    event = OrderCreated(order_id="o-2", customer_id="c-1", amount="-1.0")
    await svc.handle_order_created(event)
    msgs = await fake_redis.xrange(STREAM_PAYMENTS)
    assert len(msgs) == 1
    assert msgs[0][1][b"event_type"] == b"PaymentFailed"
    assert msgs[0][1][b"order_id"] == b"o-2"
