import asyncio
import logging
from collections.abc import Awaitable, Callable

from message_broker_patterns.dlq_pattern.broker import DLQBroker
from message_broker_patterns.dlq_pattern.models import Payment
from message_broker_patterns.metrics import REGISTRY

logger = logging.getLogger(__name__)

# A handler receives the consumer id plus the payment. It is awaited once per
# delivery; raising signals a processing failure that counts toward the retry
# budget.
Handler = Callable[[str, Payment], Awaitable[None]]


async def run_idempotent_consumer(
    broker: DLQBroker,
    consumer_id: str,
    group: str,
    handler: Handler,
    stop_event: asyncio.Event,
    *,
    max_attempts: int = 3,
    count: int = 10,
    block_ms: int = 100,
    idle_sleep: float = 0.01,
) -> int:
    """Run one idempotent consumer against ``payments:main`` until stopped.

    Per message:

    1. Idempotency check — if the payment id is already in the processed set,
       ack and skip (it was handled by an earlier delivery or run).
    2. Run the handler. On success, record the payment id and ack.
    3. On failure, bump an in-memory attempt counter. While attempts remain the
       message is left unacked so the broker redelivers it; once the counter
       reaches ``max_attempts`` the message is moved to the DLQ.

    The attempt counter is an in-memory ``{msg_id: int}`` dict — ephemeral and
    correct for a single consumer run. Returns the number of payments
    successfully processed (excluding skipped duplicates and DLQ moves).
    """
    await broker.ensure_all_groups(group)
    logger.info("consumer=%s joined idempotent pipeline (group=%s)", consumer_id, group)
    attempts: dict[str, int] = {}
    total_processed = 0
    while not stop_event.is_set():
        batch = await broker.read_new(group, consumer_id, count, block_ms)
        if not batch:
            # No new work — retry this consumer's unacked backlog. A ``>`` read
            # never redelivers, so failed-but-unacked payments only come back
            # via an explicit pending re-read.
            batch = await broker.read_pending(group, consumer_id, count)
        if not batch:
            await asyncio.sleep(idle_sleep)
            continue
        for msg_id, fields in batch:
            payment = Payment.from_fields(fields)
            if await broker.is_processed(payment.payment_id):
                await broker.ack(group, msg_id)
                REGISTRY.increment("dlq", "skipped_duplicate")
                logger.info(
                    "consumer=%s skipped duplicate %s msg=%s",
                    consumer_id,
                    payment.payment_id,
                    msg_id,
                )
                continue
            try:
                await handler(consumer_id, payment)
            except Exception as exc:
                attempt = attempts.get(msg_id, 0) + 1
                attempts[msg_id] = attempt
                if attempt < max_attempts:
                    logger.warning(
                        "consumer=%s failed %s msg=%s attempt=%d/%d — will redeliver: %s",
                        consumer_id,
                        payment.payment_id,
                        msg_id,
                        attempt,
                        max_attempts,
                        exc,
                    )
                    continue
                await broker.move_to_dlq(group, msg_id, payment, type(exc).__name__, attempt)
                attempts.pop(msg_id, None)
                logger.warning(
                    "consumer=%s exhausted retries for %s msg=%s → DLQ",
                    consumer_id,
                    payment.payment_id,
                    msg_id,
                )
                continue
            await broker.mark_processed(payment.payment_id)
            await broker.ack(group, msg_id)
            attempts.pop(msg_id, None)
            REGISTRY.increment("dlq", "payments_processed")
            total_processed += 1
            logger.info(
                "consumer=%s processed payment=%s msg=%s",
                consumer_id,
                payment.payment_id,
                msg_id,
            )
    logger.info("consumer=%s stopping — processed %d payment(s)", consumer_id, total_processed)
    return total_processed
