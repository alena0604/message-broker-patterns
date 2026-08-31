"""Benchmark the Transactional Outbox relay against its one operational knob.

The knob is the relay's **poll interval**. It is the whole latency budget of the
pattern: an outbox row is invisible to the stream until the next sweep picks it
up, so end-to-end latency is bounded by how often the relay looks, and
throughput is bounded by how much one sweep can drain (``poll_outbox``'s ``LIMIT
10``) divided by that interval.

One operation = insert an order + its outbox row in a single transaction, then
wait until the relay has published it. The relay deletes the outbox row only
after ``publish`` returns, so "row gone" is the exact moment the event is on the
stream — which makes this an outbox-entry-to-published-event latency, not a
sqlite write latency.

Operations run ``--concurrency`` at a time so the relay sees a real backlog and
each sweep publishes a batch, the way a busy service behaves. Queue depth is the
outbox table's row count — the backlog an operator would alert on.

    uv --directory packages/backend run python scripts/bench_outbox_pattern.py
    uv --directory packages/backend run python scripts/bench_outbox_pattern.py \
        --poll-interval 0.5 --ops 40
"""

from message_broker_patterns.logging import init_logger

# WARNING, not INFO: the relay logs a line per relayed entry, which at a few
# hundred ops would cost more than the work being measured.
init_logger("WARNING")

import argparse  # noqa: E402
import asyncio  # noqa: E402
import itertools  # noqa: E402
import logging  # noqa: E402
import sqlite3  # noqa: E402
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
from message_broker_patterns.config.settings import settings  # noqa: E402
from message_broker_patterns.outbox_pattern.broker import RedisBroker  # noqa: E402
from message_broker_patterns.outbox_pattern.models import Order  # noqa: E402
from message_broker_patterns.outbox_pattern.relay import run as relay_run  # noqa: E402
from message_broker_patterns.outbox_pattern.store import (  # noqa: E402
    create_tables,
    insert_order_with_outbox,
)

show_bench_progress()
logger = logging.getLogger("bench_outbox")

# A stream of this bench's own, so a run never disturbs the demo's data.
STREAM = "bench:orders:events"

# How often an operation checks whether the relay has published its entry yet.
# Small relative to any realistic poll interval, so it adds no measurable bias.
WAIT_POLL = 0.001

DEFAULT_POLL_INTERVALS = [0.02, 0.05, 0.1]
DEFAULT_OPS = 100
DEFAULT_WARMUP = 20
# 10 in flight matches `poll_outbox`'s LIMIT 10: one sweep drains the backlog,
# which is the regime the pattern is designed for.
DEFAULT_CONCURRENCY = 10


def _outbox_depth(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()
    return int(row[0])


async def bench_poll_interval(poll_interval: float, args: argparse.Namespace) -> BenchResult:
    """Run one measured window with the relay polling every ``poll_interval``."""
    conn = sqlite3.connect(":memory:")
    create_tables(conn)

    client = aioredis.from_url(settings.redis_url)
    await client.delete(STREAM)
    broker = RedisBroker(client)

    stop = asyncio.Event()
    relay = asyncio.create_task(relay_run(conn, broker, STREAM, stop, poll_interval=poll_interval))

    order_ids = itertools.count(1)

    async def publish_one_order() -> None:
        order = Order(
            order_id=f"bench-ord-{next(order_ids):06d}",
            customer_id="cust-bench",
            amount=99.99,
        )
        entry = insert_order_with_outbox(conn, order)
        # The relay deletes the row only after the publish lands, so waiting for
        # the row to disappear is waiting for the event to exist on the stream.
        while conn.execute("SELECT 1 FROM outbox WHERE id = ?", (entry.id,)).fetchone():
            await asyncio.sleep(WAIT_POLL)

    async def sample_outbox_depth() -> int:
        return _outbox_depth(conn)

    try:
        result = await run_bench(
            "outbox",
            publish_one_order,
            warmup=Window.of_ops(args.warmup),
            measure=Window.of_ops(args.ops),
            concurrency=args.concurrency,
            knobs={"poll_interval": poll_interval},
            sampler=sample_outbox_depth,
            sample_interval=args.sample_interval,
        )
    finally:
        stop.set()
        await relay

    result.extra = {
        "events_on_stream": await client.xlen(STREAM),
        "outbox_rows_remaining": _outbox_depth(conn),
    }

    await client.delete(STREAM)
    await broker.close()
    conn.close()
    return result


async def main() -> None:
    args = _parse_args()
    results = [await bench_poll_interval(interval, args) for interval in args.poll_interval]
    emit_json(results, stream=sys.stdout)
    if args.csv_dir is not None:
        write_csv_files(results, args.csv_dir)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark the outbox relay across one or more poll intervals."
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        nargs="+",
        default=DEFAULT_POLL_INTERVALS,
        help=f"relay poll interval(s) in seconds to sweep (default: {DEFAULT_POLL_INTERVALS})",
    )
    add_bench_args(parser, ops=DEFAULT_OPS, warmup=DEFAULT_WARMUP, concurrency=DEFAULT_CONCURRENCY)
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
