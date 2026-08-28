"""The naive bare-XREAD workers must each process EVERY message.

Switching ``run_bare_read_consumer`` to ``XREADGROUP`` would split the work
1x-across-N; the duplication test then never reaches its expected count and fails
on the timeout. That is the intended red.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable

import fakeredis.aioredis

from message_broker_patterns.competing_consumers_pattern.models import Task
from message_broker_patterns.competing_consumers_pattern.naive import run_bare_read_consumer

STREAM = "tasks:work"
WORKERS = 3
TASKS = 6


async def _publish_tasks(client: fakeredis.aioredis.FakeRedis, total: int) -> None:
    for index in range(total):
        await client.xadd(STREAM, Task(task_id=f"task-{index}", payload="work").to_fields())


async def _wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    """Poll until ``predicate()`` is true, or fail the test by timing out."""
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


async def test_naive_competing_consumers_each_process_every_message(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    # Arrange — 6 tasks on the stream, 3 workers doing bare reads.
    await _publish_tasks(fake_redis, TASKS)
    handled: list[tuple[str, str]] = []

    async def handler(consumer_id: str, task: Task) -> None:
        handled.append((consumer_id, task.task_id))

    stop = asyncio.Event()
    workers = [
        asyncio.create_task(
            run_bare_read_consumer(fake_redis, STREAM, f"worker-{index}", handler, stop)
        )
        for index in range(WORKERS)
    ]

    # Act — wait for the full N*M blow-up, then stop cleanly.
    try:
        await _wait_until(lambda: len(handled) >= WORKERS * TASKS)
    finally:
        stop.set()
        totals = await asyncio.gather(*workers)

    # Assert — no work-splitting at all: every worker did the whole stream.
    assert totals == [TASKS] * WORKERS
    assert Counter(task_id for _consumer, task_id in handled) == {
        f"task-{index}": WORKERS for index in range(TASKS)
    }


async def test_naive_competing_consumers_create_no_group_to_balance_or_redeliver(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    # Arrange — one worker, one task.
    await _publish_tasks(fake_redis, 1)
    handled: list[str] = []

    async def handler(consumer_id: str, task: Task) -> None:
        handled.append(task.task_id)

    stop = asyncio.Event()
    worker = asyncio.create_task(
        run_bare_read_consumer(fake_redis, STREAM, "worker-0", handler, stop)
    )

    # Act.
    try:
        await _wait_until(lambda: bool(handled))
    finally:
        stop.set()
        await worker

    # Assert — the stream has no consumer group, so there is no pending-entries
    # list: nothing a surviving sibling could ever reclaim from a dead worker.
    assert await fake_redis.xinfo_groups(STREAM) == []
