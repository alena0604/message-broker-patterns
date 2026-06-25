from message_broker_patterns.logging import init_logger

init_logger()

import asyncio  # noqa: E402
import logging  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.competing_consumers_pattern.broker import (  # noqa: E402
    CompetingConsumersBroker,
)
from message_broker_patterns.competing_consumers_pattern.consumer import (  # noqa: E402
    run_consumer,
)
from message_broker_patterns.competing_consumers_pattern.models import Task  # noqa: E402
from message_broker_patterns.config.settings import settings  # noqa: E402

logger = logging.getLogger("run_competing_consumers")

STREAM = "tasks:work"
GROUP = "workers"
NUM_TASKS = 30
NUM_CONSUMERS = 3


async def _demo_load_balancing(broker: CompetingConsumersBroker) -> None:
    """Fast producer + N competing consumers; show which consumer handled what."""
    logger.info("=== Demo 1: load balancing across %d consumers ===", NUM_CONSUMERS)

    # consumer_id -> list of task_ids it handled, to show the distribution.
    handled_by: dict[str, list[str]] = {f"worker-{i}": [] for i in range(NUM_CONSUMERS)}

    # Fast producer: push all tasks up front so consumers compete for the backlog.
    for i in range(NUM_TASKS):
        await broker.publish(STREAM, Task(f"task-{i}", f"payload-{i}").to_fields())
    logger.info("producer pushed %d tasks onto stream %s", NUM_TASKS, STREAM)

    def make_handler(consumer_id: str):
        async def handler(cid: str, task: Task) -> None:
            # Simulate a little work so the broker load-balances across consumers.
            await asyncio.sleep(0.01)
            handled_by[consumer_id].append(task.task_id)

        return handler

    stop = asyncio.Event()

    async def _stop_when_drained() -> None:
        while await broker.pending_count(STREAM, GROUP) > 0 or sum(
            len(v) for v in handled_by.values()
        ) < NUM_TASKS:
            await asyncio.sleep(0.02)
        stop.set()

    await asyncio.gather(
        *(
            run_consumer(
                broker,
                STREAM,
                GROUP,
                consumer_id,
                make_handler(consumer_id),
                stop,
                count=4,
                block_ms=50,
            )
            for consumer_id in handled_by
        ),
        _stop_when_drained(),
    )

    logger.info("--- distribution ---")
    for consumer_id, ids in handled_by.items():
        logger.info("%s handled %d task(s): %s", consumer_id, len(ids), sorted(ids))
    total = sum(len(v) for v in handled_by.values())
    unique = {tid for ids in handled_by.values() for tid in ids}
    logger.info(
        "processed %d task(s), %d unique → exactly-once: %s",
        total,
        len(unique),
        total == len(unique) == NUM_TASKS,
    )


async def _demo_crash_recovery(broker: CompetingConsumersBroker) -> None:
    """Show a crashed consumer's in-flight message reclaimed by a sibling."""
    logger.info("=== Demo 2: crash recovery via XAUTOCLAIM ===")
    crash_stream = "tasks:crash"
    crash_group = "workers"
    await broker.ensure_group(crash_stream, crash_group)
    await broker.publish(crash_stream, Task("orphan-1", "needs-recovery").to_fields())

    # A consumer reads the message and then "crashes" — never acks it.
    in_flight = await broker.read_new(
        crash_stream, crash_group, "doomed-worker", count=10, block_ms=50
    )
    logger.info(
        "doomed-worker read %d message(s) then crashed (no ack); pending=%d",
        len(in_flight),
        await broker.pending_count(crash_stream, crash_group),
    )

    handled: list[tuple[str, Task]] = []

    async def handler(consumer_id: str, task: Task) -> None:
        handled.append((consumer_id, task))

    stop = asyncio.Event()

    async def _stop_when_reclaimed() -> None:
        while not handled:
            await asyncio.sleep(0.02)
        stop.set()

    # The survivor reclaims anything idle for >0ms — i.e. the orphaned message.
    await asyncio.gather(
        run_consumer(
            broker,
            crash_stream,
            crash_group,
            "survivor-worker",
            handler,
            stop,
            block_ms=50,
            reclaim_min_idle_ms=0,
        ),
        _stop_when_reclaimed(),
    )

    consumer_id, task = handled[0]
    logger.info(
        "%s reclaimed and processed orphaned task=%s; pending=%d",
        consumer_id,
        task.task_id,
        await broker.pending_count(crash_stream, crash_group),
    )


async def main() -> None:
    redis_client = aioredis.from_url(settings.redis_url)
    broker = CompetingConsumersBroker(redis_client)
    # Clean any leftovers from a previous run so the demo is reproducible.
    await redis_client.delete(STREAM, "tasks:crash")
    await broker.ensure_group(STREAM, GROUP)

    await _demo_load_balancing(broker)
    await _demo_crash_recovery(broker)

    await redis_client.delete(STREAM, "tasks:crash")
    await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
