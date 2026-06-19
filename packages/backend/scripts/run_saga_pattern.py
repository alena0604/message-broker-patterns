from message_broker_patterns.logging import init_logger

init_logger()

import asyncio  # noqa: E402
import uuid  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.config.settings import settings  # noqa: E402
from message_broker_patterns.saga_pattern.broker import SagaBroker  # noqa: E402
from message_broker_patterns.saga_pattern.events import (  # noqa: E402
    STREAM_ORDERS,
    STREAM_PAYMENTS,
    STREAM_SHIPPING,
)
from message_broker_patterns.saga_pattern.models import Order  # noqa: E402
from message_broker_patterns.saga_pattern.runner import run_saga  # noqa: E402
from message_broker_patterns.saga_pattern.services.order_service import OrderService  # noqa: E402
from message_broker_patterns.saga_pattern.services.payment_service import PaymentService  # noqa: E402
from message_broker_patterns.saga_pattern.services.shipping_service import ShippingService  # noqa: E402


async def run_demo(redis_client: aioredis.Redis, label: str, amount: float) -> None:
    broker = SagaBroker(redis_client)
    order_svc = OrderService(broker)
    payment_svc = PaymentService(broker)
    shipping_svc = ShippingService(broker)

    order = Order(order_id=str(uuid.uuid4()), customer_id="demo-user", amount=amount)
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"  order_id: {order.order_id}  amount: {amount}")
    print(f"{'=' * 60}")

    stop = asyncio.Event()
    await run_saga(order_svc, payment_svc, shipping_svc, broker, order, stop, poll_interval=0.1)

    final = order_svc.get_order(order.order_id)
    print(f"\nFinal order status: {final.status if final else 'unknown'}")  # type: ignore[union-attr]

    for stream, label_ in [
        (STREAM_ORDERS, "saga:orders"),
        (STREAM_PAYMENTS, "saga:payments"),
        (STREAM_SHIPPING, "saga:shipping"),
    ]:
        msgs = await redis_client.xrange(stream)
        print(f"\n{label_} ({len(msgs)} message(s)):")
        for msg_id, fields in msgs:
            event_type = fields.get(b"event_type", b"?").decode()
            order_id = fields.get(b"order_id", b"?").decode()
            print(f"  {msg_id.decode()}  {event_type}  order_id={order_id}")

    # clean up streams for next demo run
    await redis_client.delete(STREAM_ORDERS, STREAM_PAYMENTS, STREAM_SHIPPING)
    await broker.close()


async def main() -> None:
    redis_client = aioredis.from_url(settings.redis_url)

    await run_demo(redis_client, "HAPPY PATH — payment succeeds", amount=150.0)
    await run_demo(redis_client, "FAILURE PATH — payment fails (compensation)", amount=-1.0)

    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
