"""Benchmark competing consumers against its one knob: how many consumers compete.

Adding consumers to a consumer group is the pattern's whole scaling story, and
the only honest way to tell it is to measure it: one group, one stream, a fixed
backlog of in-flight work, and ``--consumers`` workers sharing it. Each handler
does a fixed ``WORK_SECONDS`` of "work", so a single consumer is capped at
roughly ``1 / WORK_SECONDS`` messages per second and every consumer added should
buy close to that again — until the broker round trip, not the handler, becomes
the limit.

One operation = publish one task and wait until a consumer has handled it, so
the latency recorded is a real publish-to-handled time and not a fire-and-forget
``XADD``. ``--concurrency`` operations are kept in flight, which is what gives
the consumers a backlog to compete over; with one operation at a time every
consumer but one would sit idle and the knob would look useless.

Queue depth is the group's pending-entries count (``XPENDING``) — messages
delivered to a consumer but not yet acked. That, not ``XLEN``, is the backlog an
operator watches: a stream's length only ever grows, since acking a message does
not remove it from the log.

    uv --directory packages/backend run python scripts/bench_competing_consumers_pattern.py
    uv --directory packages/backend run python scripts/bench_competing_consumers_pattern.py \
        --consumers 5 --ops 300
"""

from message_broker_patterns.logging import init_logger

# WARNING, not INFO: the consumer logs a line per handled task.
init_logger("WARNING")

import argparse  # noqa: E402
import asyncio  # noqa: E402
import itertools  # noqa: E402
import logging  # noqa: E402
import sys  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.bench import (  # noqa: E402
    BenchResult,
    Window,
    add_bench_args,
    emit_json,
    run_bench,
    show_bench_progress,
    write_csv_files,
)
from message_broker_patterns.competing_consumers_pattern.broker import (  # noqa: E402
    CompetingConsumersBroker,
)
from message_broker_patterns.competing_consumers_pattern.consumer import (  # noqa: E402
    run_consumer,
)
from message_broker_patterns.competing_consumers_pattern.models import Task  # noqa: E402
from message_broker_patterns.config.settings import settings  # noqa: E402

show_bench_progress()
logger = logging.getLogger("bench_competing_consumers")

# A stream and group of this bench's own, so a run never disturbs the demo.
STREAM = "bench:tasks:work"
GROUP = "bench_workers"

# Simulated per-task work. Small enough to keep the bench short, large enough
# that the handler — not Redis — is what a consumer is busy with.
WORK_SECONDS = 0.005

DEFAULT_CONSUMERS = [1, 2, 4]
DEFAULT_OPS = 300
DEFAULT_WARMUP = 40
DEFAULT_CONCURRENCY = 20


async def bench_consumer_count(consumers: int, args: argparse.Namespace) -> BenchResult:
    """Run one measured window with ``consumers`` workers sharing the group."""
    client = aioredis.from_url(settings.redis_url)
    await client.delete(STREAM)
    broker = CompetingConsumersBroker(client)
    await broker.ensure_group(STREAM, GROUP)

    # task_id -> the future its publisher is waiting on.
    awaiting: dict[str, asyncio.Future[None]] = {}
    handled_by: dict[str, int] = {}

    async def handler(consumer_id: str, task: Task) -> None:
        await asyncio.sleep(WORK_SECONDS)
        handled_by[consumer_id] = handled_by.get(consumer_id, 0) + 1
        future = awaiting.pop(task.task_id, None)
        if future is not None and not future.done():
            future.set_result(None)

    stop = asyncio.Event()
    workers = [
        asyncio.create_task(
            run_consumer(
                broker,
                STREAM,
                GROUP,
                f"bench-worker-{index}",
                handler,
                stop,
                count=10,
                block_ms=20,
            )
        )
        for index in range(consumers)
    ]

    task_ids = itertools.count(1)
    loop = asyncio.get_running_loop()

    async def publish_and_await_handling() -> None:
        task_id = f"bench-task-{next(task_ids):06d}"
        future: asyncio.Future[None] = loop.create_future()
        awaiting[task_id] = future
        await broker.publish(STREAM, Task(task_id, "payload").to_fields())
        await future

    async def sample_pending() -> int:
        return await broker.pending_count(STREAM, GROUP)

    try:
        result = await run_bench(
            "competing-consumers",
            publish_and_await_handling,
            warmup=Window.of_ops(args.warmup),
            measure=Window.of_ops(args.ops),
            concurrency=args.concurrency,
            knobs={"consumers": consumers},
            sampler=sample_pending,
            sample_interval=args.sample_interval,
        )
    finally:
        stop.set()
        await asyncio.gather(*workers)

    result.extra = {
        "in_flight": args.concurrency,
        "handler_work_s": WORK_SECONDS,
        "handled_per_consumer": dict(sorted(handled_by.items())),
        "stream_length": await client.xlen(STREAM),
        "pending_at_end": await broker.pending_count(STREAM, GROUP),
    }

    await client.delete(STREAM)
    await broker.close()
    return result


async def main() -> None:
    args = _parse_args()
    results = [await bench_consumer_count(count, args) for count in args.consumers]
    emit_json(results, stream=sys.stdout)
    if args.csv_dir is not None:
        write_csv_files(results, args.csv_dir)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark competing consumers across one or more consumer counts."
    )
    parser.add_argument(
        "--consumers",
        type=int,
        nargs="+",
        default=DEFAULT_CONSUMERS,
        help=f"consumer count(s) to sweep (default: {DEFAULT_CONSUMERS})",
    )
    add_bench_args(parser, ops=DEFAULT_OPS, warmup=DEFAULT_WARMUP, concurrency=DEFAULT_CONCURRENCY)
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
