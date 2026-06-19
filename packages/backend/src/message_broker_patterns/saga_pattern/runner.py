import asyncio
import logging

from message_broker_patterns.saga_pattern.broker import SagaBroker
from message_broker_patterns.saga_pattern.events import (
    STREAM_ORDERS,
    STREAM_PAYMENTS,
    STREAM_SHIPPING,
    OrderCreated,
    OrderShipped,
    PaymentFailed,
    PaymentProcessed,
)
from message_broker_patterns.saga_pattern.models import Order, SagaStatus
from message_broker_patterns.saga_pattern.services.order_service import OrderService
from message_broker_patterns.saga_pattern.services.payment_service import PaymentService
from message_broker_patterns.saga_pattern.services.shipping_service import ShippingService

logger = logging.getLogger(__name__)

_TERMINAL = {SagaStatus.COMPLETED, SagaStatus.CANCELLED}


async def run_saga(
    order_svc: OrderService,
    payment_svc: PaymentService,
    shipping_svc: ShippingService,
    broker: SagaBroker,
    order: Order,
    stop_event: asyncio.Event,
    poll_interval: float = 0.05,
) -> None:
    order_svc.create_order(order)
    await order_svc.publish_created(order)

    last_ids: dict[str, str] = {
        STREAM_ORDERS: "0",
        STREAM_PAYMENTS: "0",
        STREAM_SHIPPING: "0",
    }

    logger.info("Saga runner started for order %s", order.order_id)

    while not stop_event.is_set():
        # --- saga:orders ---
        for msg_id, raw in await broker.consume(STREAM_ORDERS, last_ids[STREAM_ORDERS]):
            last_ids[STREAM_ORDERS] = msg_id
            event_type = raw.get(b"event_type", b"").decode()
            data = {k.decode(): v.decode() for k, v in raw.items()}
            if event_type == "OrderCreated":
                await payment_svc.handle_order_created(OrderCreated.from_dict(data))

        # --- saga:payments ---
        for msg_id, raw in await broker.consume(STREAM_PAYMENTS, last_ids[STREAM_PAYMENTS]):
            last_ids[STREAM_PAYMENTS] = msg_id
            event_type = raw.get(b"event_type", b"").decode()
            data = {k.decode(): v.decode() for k, v in raw.items()}
            if event_type == "PaymentProcessed":
                evt = PaymentProcessed.from_dict(data)
                await order_svc.handle_payment_processed(evt)
                await shipping_svc.handle_payment_processed(evt)
            elif event_type == "PaymentFailed":
                await order_svc.handle_payment_failed(PaymentFailed.from_dict(data))

        # --- saga:shipping ---
        for msg_id, raw in await broker.consume(STREAM_SHIPPING, last_ids[STREAM_SHIPPING]):
            last_ids[STREAM_SHIPPING] = msg_id
            event_type = raw.get(b"event_type", b"").decode()
            data = {k.decode(): v.decode() for k, v in raw.items()}
            if event_type == "OrderShipped":
                await order_svc.handle_order_shipped(OrderShipped.from_dict(data))

        # terminal state check
        current = order_svc.get_order(order.order_id)
        if current is not None and current.status in _TERMINAL:
            logger.info(
                "Saga for order %s reached terminal state: %s", order.order_id, current.status
            )
            stop_event.set()
            break

        await asyncio.sleep(poll_interval)

    logger.info("Saga runner stopped for order %s", order.order_id)
