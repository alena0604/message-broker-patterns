import logging
import uuid

from message_broker_patterns.saga_pattern.broker import SagaBroker
from message_broker_patterns.saga_pattern.events import (
    STREAM_SHIPPING,
    OrderShipped,
    PaymentProcessed,
)

logger = logging.getLogger(__name__)


class ShippingService:
    def __init__(self, broker: SagaBroker) -> None:
        self._broker = broker

    async def handle_payment_processed(self, event: PaymentProcessed) -> None:
        shipped = OrderShipped(
            order_id=event.order_id,
            tracking_number=str(uuid.uuid4()),
        )
        await self._broker.publish(STREAM_SHIPPING, shipped.event_type, shipped.to_dict())
        logger.info("Order %s SHIPPED (tracking: %s)", event.order_id, shipped.tracking_number)
