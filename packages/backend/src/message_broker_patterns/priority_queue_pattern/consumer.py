import asyncio
import logging
from collections.abc import Awaitable, Callable

from message_broker_patterns.metrics import REGISTRY
from message_broker_patterns.priority_queue_pattern.broker import PriorityQueueBroker
from message_broker_patterns.priority_queue_pattern.models import Priority, SupportTicket

logger = logging.getLogger(__name__)

# A handler receives the consumer id that processed the ticket plus the ticket
# itself. It is awaited once per successfully-claimed message.
Handler = Callable[[str, SupportTicket], Awaitable[None]]


async def run_consumer(
    broker: PriorityQueueBroker,
    priority: Priority,
    consumer_id: str,
    group: str,
    handler: Handler,
    stop_event: asyncio.Event,
    *,
    count: int = 10,
    block_ms: int = 100,
    idle_sleep: float = 0.01,
) -> int:
    """Run one consumer dedicated to a single priority's queue until stopped.

    Each iteration claims new messages from this priority's stream, runs the
    handler, then acks. A consumer only ever touches its own priority — that
    dedication is what gives high-priority tickets their own throughput.

    ``idle_sleep`` is a short cooperative yield taken when a read finds no work,
    so a consumer can't hot-spin and starve siblings or the stop signal (the
    in-memory test double resolves blocking reads inline without yielding).
    Returns the total number of tickets this consumer processed.
    """
    await broker.ensure_group(priority, group)
    logger.info("consumer=%s joined %s queue (group=%s)", consumer_id, priority.value, group)
    total_handled = 0
    while not stop_event.is_set():
        batch = await broker.read_new(priority, group, consumer_id, count, block_ms)
        if batch:
            for msg_id, fields in batch:
                ticket = SupportTicket.from_fields(fields)
                await handler(consumer_id, ticket)
                await broker.ack(priority, group, msg_id)
                REGISTRY.increment("priority_queue", "tickets_consumed")
                REGISTRY.increment("priority_queue", f"{priority.value}_consumed")
                total_handled += 1
                logger.info(
                    "consumer=%s processed ticket=%s (%s) msg=%s",
                    consumer_id,
                    ticket.ticket_id,
                    ticket.priority.value,
                    msg_id,
                )
        else:
            await asyncio.sleep(idle_sleep)
    logger.info("consumer=%s stopping — handled %d ticket(s)", consumer_id, total_handled)
    return total_handled
