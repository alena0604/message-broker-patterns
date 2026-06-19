import fakeredis.aioredis

from message_broker_patterns.saga_pattern.broker import SagaBroker
from message_broker_patterns.saga_pattern.events import STREAM_SHIPPING, PaymentProcessed
from message_broker_patterns.saga_pattern.services.shipping_service import ShippingService


async def test_handle_payment_processed_publishes_order_shipped(
    saga_broker: SagaBroker,
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    svc = ShippingService(saga_broker)
    event = PaymentProcessed(order_id="o-1", transaction_id="tx-1")
    await svc.handle_payment_processed(event)
    msgs = await fake_redis.xrange(STREAM_SHIPPING)
    assert len(msgs) == 1
    assert msgs[0][1][b"event_type"] == b"OrderShipped"
    assert msgs[0][1][b"order_id"] == b"o-1"
    assert b"tracking_number" in msgs[0][1]
