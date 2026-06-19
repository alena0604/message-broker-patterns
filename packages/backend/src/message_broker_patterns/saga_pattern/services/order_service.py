import logging

from message_broker_patterns.saga_pattern.broker import SagaBroker
from message_broker_patterns.saga_pattern.events import (
    STREAM_ORDERS,
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    PaymentFailed,
    PaymentProcessed,
)
from message_broker_patterns.saga_pattern.models import Order, SagaStatus

logger = logging.getLogger(__name__)


class OrderService:
    def __init__(self, broker: SagaBroker) -> None:
        self._orders: dict[str, Order] = {}
        self._broker = broker

    def create_order(self, order: Order) -> None:
        self._orders[order.order_id] = order
        logger.info("Order %s created (status: %s)", order.order_id, order.status)

    async def publish_created(self, order: Order) -> None:
        order.status = SagaStatus.PAYMENT_PROCESSING
        event = OrderCreated(
            order_id=order.order_id,
            customer_id=order.customer_id,
            amount=str(order.amount),
        )
        await self._broker.publish(STREAM_ORDERS, event.event_type, event.to_dict())
        logger.info("Published OrderCreated for order %s", order.order_id)

    async def handle_payment_processed(self, event: PaymentProcessed) -> None:
        order = self._orders.get(event.order_id)
        if order is None:
            logger.warning("OrderService: unknown order %s", event.order_id)
            return
        order.status = SagaStatus.PAID
        logger.info("Order %s marked PAID (tx: %s)", order.order_id, event.transaction_id)

    async def handle_payment_failed(self, event: PaymentFailed) -> None:
        order = self._orders.get(event.order_id)
        if order is None:
            logger.warning("OrderService: unknown order %s", event.order_id)
            return
        order.status = SagaStatus.CANCELLED
        compensation = OrderCancelled(order_id=order.order_id, reason=event.reason)
        await self._broker.publish(STREAM_ORDERS, compensation.event_type, compensation.to_dict())
        logger.info("Order %s CANCELLED (compensation published)", order.order_id)

    async def handle_order_shipped(self, event: OrderShipped) -> None:
        order = self._orders.get(event.order_id)
        if order is None:
            logger.warning("OrderService: unknown order %s", event.order_id)
            return
        order.status = SagaStatus.COMPLETED
        logger.info("Order %s COMPLETED (tracking: %s)", order.order_id, event.tracking_number)

    def get_order(self, order_id: str) -> Order | None:
        return self._orders.get(order_id)
