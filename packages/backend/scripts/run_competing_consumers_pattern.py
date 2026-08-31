from message_broker_patterns.logging import init_logger

init_logger()

import argparse  # noqa: E402
import asyncio  # noqa: E402
import logging  # noqa: E402
from collections import Counter  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.chaos import ConsumerCrashError, crash_before_ack  # noqa: E402
from message_broker_patterns.competing_consumers_pattern.broker import (  # noqa: E402
    CompetingConsumersBroker,
)
from message_broker_patterns.competing_consumers_pattern.consumer import (  # noqa: E402
    run_consumer,
)
from message_broker_patterns.competing_consumers_pattern.models import Task  # noqa: E402
from message_broker_patterns.competing_consumers_pattern.naive import (  # noqa: E402
    run_bare_read_consumer,
)
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
        while (
            await broker.pending_count(STREAM, GROUP) > 0
            or sum(len(v) for v in handled_by.values()) < NUM_TASKS
        ):
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


# --- naive baseline -----------------------------------------------------------
# The same 30 tasks and 3 workers, but every worker does a bare XREAD from its
# own cursor instead of joining a consumer group. Demo 1 needs no injected fault
# — the duplication is what a group-less read *is*. Demo 2 does: chaos supplies
# the mid-task crash whose message a group would have left pending for a sibling.
NAIVE_STREAM = "tasks:naive"
NAIVE_CRASH_STREAM = "tasks:naive-crash"
NAIVE_CRASH_TASKS = 3


async def _naive_demo_duplication(client: aioredis.Redis) -> None:
    """N bare-read workers each process every message: N*M side effects for M tasks."""
    logger.info("=== NAIVE Demo 1: %d bare-XREAD workers, no consumer group ===", NUM_CONSUMERS)

    for index in range(NUM_TASKS):
        await client.xadd(NAIVE_STREAM, Task(f"task-{index}", f"payload-{index}").to_fields())
    logger.info("producer pushed %d tasks onto stream %s", NUM_TASKS, NAIVE_STREAM)

    handled_by: dict[str, list[str]] = {f"worker-{i}": [] for i in range(NUM_CONSUMERS)}

    def make_handler(consumer_id: str):
        async def handler(cid: str, task: Task) -> None:
            await asyncio.sleep(0.01)
            handled_by[consumer_id].append(task.task_id)

        return handler

    stop = asyncio.Event()

    async def _stop_when_every_worker_has_the_whole_stream() -> None:
        while sum(len(v) for v in handled_by.values()) < NUM_TASKS * NUM_CONSUMERS:
            await asyncio.sleep(0.02)
        stop.set()

    await asyncio.gather(
        *(
            run_bare_read_consumer(
                client, NAIVE_STREAM, consumer_id, make_handler(consumer_id), stop
            )
            for consumer_id in handled_by
        ),
        _stop_when_every_worker_has_the_whole_stream(),
    )

    logger.info("--- distribution ---")
    for consumer_id, ids in handled_by.items():
        logger.info("%s handled %d task(s)", consumer_id, len(ids))
    total = sum(len(v) for v in handled_by.values())
    per_task = Counter(tid for ids in handled_by.values() for tid in ids)
    unique = len(per_task)
    logger.info(
        "%d execution(s) for %d unique task(s) — every task ran %dx (exactly-once: %s)",
        total,
        unique,
        max(per_task.values()),
        total == unique == NUM_TASKS,
    )
    logger.info(
        "a 4th worker would make it %d: bare XREAD is a broadcast, so workers multiply "
        "the work instead of sharing it",
        NUM_TASKS * (NUM_CONSUMERS + 1),
    )


async def _naive_demo_no_reclaim(client: aioredis.Redis) -> None:
    """A crashed bare-read worker leaves nothing behind — there is no pending list."""
    logger.info("=== NAIVE Demo 2: crash recovery without a consumer group ===")
    for index in range(1, NAIVE_CRASH_TASKS + 1):
        await client.xadd(NAIVE_CRASH_STREAM, Task(f"crash-{index}", "needs-recovery").to_fields())

    executions: list[tuple[str, str]] = []

    async def handler(consumer_id: str, task: Task) -> None:
        executions.append((consumer_id, task.task_id))

    # chaos kills the worker on its first task, *after* the work is done — the
    # exact moment a consumer group would leave the message pending for a peer.
    doomed_handler = crash_before_ack(handler, on_call=1)
    stop = asyncio.Event()
    try:
        await run_bare_read_consumer(
            client, NAIVE_CRASH_STREAM, "doomed-worker", doomed_handler, stop
        )
    except ConsumerCrashError as exc:
        logger.info("doomed-worker died mid-task (chaos.crash_before_ack): %s", exc)

    groups = await client.xinfo_groups(NAIVE_CRASH_STREAM)
    logger.info(
        "consumer groups on %s: %s — no pending-entries list exists, so there is nothing "
        "for a sibling to XAUTOCLAIM",
        NAIVE_CRASH_STREAM,
        groups,
    )

    # The only "recovery" a bare read offers: another worker replays the stream
    # from 0 — redoing the task the dead worker had already completed.
    survivor_stop = asyncio.Event()

    async def _stop_when_replayed() -> None:
        while len([1 for cid, _ in executions if cid == "survivor-worker"]) < NAIVE_CRASH_TASKS:
            await asyncio.sleep(0.02)
        survivor_stop.set()

    await asyncio.gather(
        run_bare_read_consumer(
            client, NAIVE_CRASH_STREAM, "survivor-worker", handler, survivor_stop
        ),
        _stop_when_replayed(),
    )

    done_twice = sorted(
        task_id for task_id, count in Counter(t for _, t in executions).items() if count > 1
    )
    logger.info(
        "survivor-worker replayed the stream from 0: %d execution(s) for %d task(s); "
        "re-ran %s, which doomed-worker had already completed",
        len(executions),
        NAIVE_CRASH_TASKS,
        done_twice,
    )
    logger.info("there was never a way to ask for just the orphan — that is what a group is for")


async def run_naive() -> None:
    """INTENTIONALLY BROKEN — bare XREAD per worker, no consumer group."""
    client = aioredis.from_url(settings.redis_url)
    await client.delete(NAIVE_STREAM, NAIVE_CRASH_STREAM)

    await _naive_demo_duplication(client)
    await _naive_demo_no_reclaim(client)

    logger.info(
        "Run without --naive: the same %d tasks across %d consumers in one group — "
        "%d executions, exactly-once, and a crashed worker's message reclaimed via XAUTOCLAIM.",
        NUM_TASKS,
        NUM_CONSUMERS,
        NUM_TASKS,
    )
    await client.delete(NAIVE_STREAM, NAIVE_CRASH_STREAM)
    await client.aclose()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Competing Consumers work-queue demo.")
    parser.add_argument(
        "--naive",
        action="store_true",
        help="run the intentionally broken bare-XREAD baseline (naive.py) instead",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_naive() if _parse_args().naive else main())
