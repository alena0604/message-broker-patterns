import logging
import uuid

from message_broker_patterns.saga_pattern.broker import SagaBroker
from message_broker_patterns.saga_pattern.events import (
    STREAM_PAYMENTS,
    OrderCreated,
    PaymentFailed,
    PaymentProcessed,
)

logger = logging.getLogger(__name__)


class PaymentService:
    def __init__(self, broker: SagaBroker) -> None:
        self._broker = broker

    async def handle_order_created(self, event: OrderCreated) -> None:
        amount = float(event.amount)
        if amount <= 0:
            failed = PaymentFailed(
                order_id=event.order_id,
                reason=f"Invalid amount: {event.amount}",
            )
            await self._broker.publish(STREAM_PAYMENTS, failed.event_type, failed.to_dict())
            logger.info("Payment FAILED for order %s: %s", event.order_id, failed.reason)
        else:
            processed = PaymentProcessed(
                order_id=event.order_id,
                transaction_id=str(uuid.uuid4()),
            )
            await self._broker.publish(STREAM_PAYMENTS, processed.event_type, processed.to_dict())
            logger.info(
                "Payment PROCESSED for order %s (tx: %s)",
                event.order_id,
                processed.transaction_id,
            )
