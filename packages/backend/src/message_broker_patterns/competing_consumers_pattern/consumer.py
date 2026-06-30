import asyncio
import logging
from collections.abc import Awaitable, Callable

from message_broker_patterns.competing_consumers_pattern.broker import (
    CompetingConsumersBroker,
)
from message_broker_patterns.competing_consumers_pattern.models import Task
from message_broker_patterns.metrics import REGISTRY

logger = logging.getLogger(__name__)

# A handler receives the consumer id that processed the task plus the task
# itself. It is awaited once per successfully-claimed message.
Handler = Callable[[str, Task], Awaitable[None]]


async def _handle_batch(
    broker: CompetingConsumersBroker,
    stream: str,
    group: str,
    consumer_id: str,
    batch: list[tuple[str, dict[bytes, bytes]]],
    handler: Handler,
) -> int:
    """Process each message in ``batch``, ack on success. Returns count handled."""
    handled = 0
    for message_id, fields in batch:
        task = Task.from_fields(fields)
        await handler(consumer_id, task)
        await broker.ack(stream, group, message_id)
        REGISTRY.increment("competing_consumers", "messages_consumed")
        handled += 1
        logger.info(
            "consumer=%s handled task=%s msg=%s",
            consumer_id,
            task.task_id,
            message_id,
        )
    return handled


async def run_consumer(
    broker: CompetingConsumersBroker,
    stream: str,
    group: str,
    consumer_id: str,
    handler: Handler,
    stop_event: asyncio.Event,
    *,
    count: int = 10,
    block_ms: int = 100,
    reclaim_min_idle_ms: int = 5_000,
    idle_sleep: float = 0.01,
) -> int:
    """Run one competing consumer until ``stop_event`` is set.

    Each iteration first reclaims messages abandoned by crashed siblings
    (pending longer than ``reclaim_min_idle_ms``), then claims new messages with
    ``XREADGROUP ... >``. Every successfully handled message is acked. Returns
    the total number of messages this consumer processed.

    ``idle_sleep`` is a short cooperative yield taken when a sweep finds no work.
    Against real Redis the ``XREADGROUP BLOCK`` already parks the consumer, but
    some clients (notably the in-memory test double) resolve a blocking read
    inline without yielding to the event loop — the sleep guarantees sibling
    consumers and the stop signal always get a turn, instead of one consumer
    hot-spinning the loop.
    """
    await broker.ensure_group(stream, group)
    REGISTRY.increment("competing_consumers", "consumer_joins")
    logger.info("consumer=%s joined group=%s on stream=%s", consumer_id, group, stream)

    total_handled = 0
    while not stop_event.is_set():
        did_work = False

        reclaimed = await broker.reclaim_stale(
            stream, group, consumer_id, reclaim_min_idle_ms, count
        )
        if reclaimed:
            did_work = True
            logger.info("consumer=%s reclaimed %d stale message(s)", consumer_id, len(reclaimed))
            total_handled += await _handle_batch(
                broker, stream, group, consumer_id, reclaimed, handler
            )

        batch = await broker.read_new(stream, group, consumer_id, count, block_ms)
        if batch:
            did_work = True
            total_handled += await _handle_batch(broker, stream, group, consumer_id, batch, handler)

        if not did_work:
            await asyncio.sleep(idle_sleep)

    logger.info("consumer=%s stopping — handled %d message(s) total", consumer_id, total_handled)
    return total_handled
