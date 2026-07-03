import asyncio

import fakeredis.aioredis

from message_broker_patterns.competing_consumers_pattern.broker import (
    CompetingConsumersBroker,
)
from message_broker_patterns.competing_consumers_pattern.consumer import run_consumer
from message_broker_patterns.competing_consumers_pattern.models import Task

STREAM = "integration:cc:tasks"
GROUP = "workers"


async def test_competing_consumers_process_each_message_exactly_once(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(STREAM, GROUP)

    total_tasks = 30
    for i in range(total_tasks):
        await broker.publish(STREAM, Task(f"t-{i}", f"payload-{i}").to_fields())

    # consumer_id -> task_ids it handled
    handled_by: dict[str, list[str]] = {"c1": [], "c2": [], "c3": []}
    lock = asyncio.Lock()

    def make_handler(consumer_id: str):
        async def handler(cid: str, task: Task) -> None:
            # small await so the event loop interleaves the three consumers
            await asyncio.sleep(0)
            async with lock:
                handled_by[consumer_id].append(task.task_id)

        return handler

    stop = asyncio.Event()

    async def _stop_when_drained() -> None:
        while (
            await broker.pending_count(STREAM, GROUP) > 0
            or sum(len(v) for v in handled_by.values()) < total_tasks
        ):
            await asyncio.sleep(0.01)
        stop.set()

    results = await asyncio.gather(
        run_consumer(broker, STREAM, GROUP, "c1", make_handler("c1"), stop, count=4, block_ms=10),
        run_consumer(broker, STREAM, GROUP, "c2", make_handler("c2"), stop, count=4, block_ms=10),
        run_consumer(broker, STREAM, GROUP, "c3", make_handler("c3"), stop, count=4, block_ms=10),
        _stop_when_drained(),
    )

    all_handled = [tid for ids in handled_by.values() for tid in ids]
    expected = {f"t-{i}" for i in range(total_tasks)}

    # exactly once: no duplicates, every task processed
    assert len(all_handled) == total_tasks
    assert set(all_handled) == expected

    # disjoint: each task_id belongs to exactly one consumer's list
    seen: set[str] = set()
    for ids in handled_by.values():
        consumer_set = set(ids)
        assert consumer_set.isdisjoint(seen), "a task was processed by two consumers"
        seen |= consumer_set

    # genuine load balancing: every consumer got at least one message
    assert all(len(ids) > 0 for ids in handled_by.values())

    # the three run_consumer return values (handled counts) sum to total
    consumer_totals = results[:3]
    assert sum(consumer_totals) == total_tasks

    assert await broker.pending_count(STREAM, GROUP) == 0


async def test_crashed_consumer_message_is_reclaimed_by_sibling(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(STREAM, GROUP)
    await broker.publish(STREAM, Task("t-crash", "needs-recovery").to_fields())

    # --- a consumer reads the message and then "crashes" before acking ---
    in_flight = await broker.read_new(STREAM, GROUP, "crashed-consumer", count=10, block_ms=10)
    assert len(in_flight) == 1
    assert await broker.pending_count(STREAM, GROUP) == 1  # held, unacked

    # --- a surviving sibling reclaims and finishes the orphaned message ---
    handled: list[tuple[str, Task]] = []

    async def handler(consumer_id: str, task: Task) -> None:
        handled.append((consumer_id, task))

    stop = asyncio.Event()

    async def _stop_when_reclaimed() -> None:
        while not handled:
            await asyncio.sleep(0.01)
        stop.set()

    await asyncio.gather(
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

    assert handled == [("survivor", Task("t-crash", "needs-recovery"))]
    assert await broker.pending_count(STREAM, GROUP) == 0
