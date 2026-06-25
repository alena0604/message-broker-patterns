import asyncio

import fakeredis.aioredis

from message_broker_patterns.competing_consumers_pattern.broker import (
    CompetingConsumersBroker,
)
from message_broker_patterns.competing_consumers_pattern.consumer import run_consumer
from message_broker_patterns.competing_consumers_pattern.models import Task

STREAM = "test:cc:consumer"
GROUP = "workers"


async def test_run_consumer_handles_and_acks_messages(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(STREAM, GROUP)
    await broker.publish(STREAM, Task("t-1", "p1").to_fields())
    await broker.publish(STREAM, Task("t-2", "p2").to_fields())

    handled: list[tuple[str, Task]] = []

    async def handler(consumer_id: str, task: Task) -> None:
        handled.append((consumer_id, task))

    stop = asyncio.Event()

    async def _stop_when_drained() -> None:
        while await broker.pending_count(STREAM, GROUP) > 0 or len(handled) < 2:
            await asyncio.sleep(0.01)
        stop.set()

    total, _ = await asyncio.gather(
        run_consumer(broker, STREAM, GROUP, "c1", handler, stop, block_ms=10, count=10),
        _stop_when_drained(),
    )

    assert total == 2
    assert {t.task_id for _, t in handled} == {"t-1", "t-2"}
    assert await broker.pending_count(STREAM, GROUP) == 0


async def test_run_consumer_returns_immediately_when_stopped(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    stop = asyncio.Event()
    stop.set()  # set before the loop body runs

    async def handler(consumer_id: str, task: Task) -> None:
        raise AssertionError("handler must not run when already stopped")

    total = await run_consumer(broker, STREAM, GROUP, "c1", handler, stop, block_ms=10)

    assert total == 0


async def test_run_consumer_reclaims_stale_message_from_crashed_sibling(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(STREAM, GROUP)
    await broker.publish(STREAM, Task("t-orphan", "po").to_fields())
    # "crashed" consumer reads but never acks.
    await broker.read_new(STREAM, GROUP, "dead-consumer", count=10, block_ms=10)
    assert await broker.pending_count(STREAM, GROUP) == 1

    handled: list[Task] = []

    async def handler(consumer_id: str, task: Task) -> None:
        handled.append(task)

    stop = asyncio.Event()

    async def _stop_when_reclaimed() -> None:
        while not handled:
            await asyncio.sleep(0.01)
        stop.set()

    # min_idle 0 → the orphaned message is reclaimable on the first sweep.
    total, _ = await asyncio.gather(
        run_consumer(
            broker,
            STREAM,
            GROUP,
            "survivor",
            handler,
            stop,
            block_ms=10,
            reclaim_min_idle_ms=0,
        ),
        _stop_when_reclaimed(),
    )

    assert total == 1
    assert handled == [Task("t-orphan", "po")]
    assert await broker.pending_count(STREAM, GROUP) == 0
