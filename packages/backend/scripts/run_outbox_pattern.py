from message_broker_patterns.logging import init_logger

init_logger()

import argparse  # noqa: E402
import asyncio  # noqa: E402
import sqlite3  # noqa: E402
import uuid  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.chaos import BrokerUnavailableError, broker_unavailable  # noqa: E402
from message_broker_patterns.config.settings import settings  # noqa: E402
from message_broker_patterns.outbox_pattern.broker import RedisBroker  # noqa: E402
from message_broker_patterns.outbox_pattern.models import Order  # noqa: E402
from message_broker_patterns.outbox_pattern.naive import (  # noqa: E402
    count_orders,
    create_orders_table,
    place_order_dual_write,
)
from message_broker_patterns.outbox_pattern.relay import run as relay_run  # noqa: E402
from message_broker_patterns.outbox_pattern.store import (  # noqa: E402
    create_tables,
    insert_order_with_outbox,
)

STREAM = "orders:events"

# --- naive baseline -----------------------------------------------------------
# A dozen orders is enough that a loss *rate* is visible rather than a single
# anecdote. The outage is supplied by chaos.broker_unavailable at p=0.25 on the
# default seed, which fires on calls 6, 10 and 12 — deterministic, so this demo's
# headline number is the same on every machine.
NAIVE_STREAM = "orders:events:naive"
NAIVE_ORDER_COUNT = 12
NAIVE_OUTAGE_PROBABILITY = 0.25


async def main() -> None:
    conn = sqlite3.connect(":memory:")
    create_tables(conn)

    orders = [
        Order(order_id=str(uuid.uuid4()), customer_id="cust-1", amount=99.99),
        Order(order_id=str(uuid.uuid4()), customer_id="cust-2", amount=149.00),
        Order(order_id=str(uuid.uuid4()), customer_id="cust-3", amount=9.99),
    ]
    for order in orders:
        insert_order_with_outbox(conn, order)
    print(f"Inserted {len(orders)} orders into DB + outbox.")

    redis_client = aioredis.from_url(settings.redis_url)
    broker = RedisBroker(redis_client)

    stop = asyncio.Event()
    relay_task = asyncio.create_task(relay_run(conn, broker, STREAM, stop, poll_interval=0.2))
    await asyncio.sleep(1.0)
    stop.set()
    await relay_task

    messages = await redis_client.xrange(STREAM)
    print(f"\nMessages in Redis stream '{STREAM}': {len(messages)}")
    for msg_id, fields in messages:
        print(f"  {msg_id.decode()}: {fields}")

    await broker.close()
    conn.close()


async def run_naive() -> None:
    """INTENTIONALLY BROKEN — the dual write the outbox pattern exists to replace.

    Same shape of workload as :func:`main`, minus the outbox: each order is
    committed and *then* published, as two independent steps. chaos supplies the
    broker outage that turns the gap between them into permanent message loss.
    """
    conn = sqlite3.connect(":memory:")
    create_orders_table(conn)  # ← an `orders` table and, deliberately, no `outbox`.

    redis_client = aioredis.from_url(settings.redis_url)
    await redis_client.delete(NAIVE_STREAM)
    broker = RedisBroker(redis_client)
    # The fault comes from chaos, not from this script: a seeded outage around
    # the real publish call, exactly where the dual write is unprotected.
    publish = broker_unavailable(broker.publish, probability=NAIVE_OUTAGE_PROBABILITY)

    print(f"{'=' * 60}")
    print("  NAIVE outbox — dual write (INTENTIONALLY BROKEN)")
    print(
        f"  {NAIVE_ORDER_COUNT} orders, chaos.broker_unavailable(probability="
        f"{NAIVE_OUTAGE_PROBABILITY}) around the publish"
    )
    print(f"{'=' * 60}")

    lost: list[str] = []
    for index in range(1, NAIVE_ORDER_COUNT + 1):
        order = Order(order_id=f"naive-ord-{index:02d}", customer_id="cust-1", amount=99.99)
        try:
            await place_order_dual_write(conn, publish, order, NAIVE_STREAM)
        except BrokerUnavailableError:
            # The caller's request fails — but the order is already committed.
            lost.append(order.order_id)

    committed = count_orders(conn)
    published = await redis_client.xlen(NAIVE_STREAM)
    tables = sorted(
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    )

    print(f"\n{committed} orders committed, {published} events published, {len(lost)} lost")
    print(f"Lost forever: {', '.join(lost)}")
    print(f"Tables in the database: {tables} — no outbox row records that a publish was owed,")
    print("so there is nothing to retry, nothing to replay, and no way to enumerate the loss.")
    print(
        "\nRun without --naive for the same story with an outbox: every event reaches the stream."
    )

    await redis_client.delete(NAIVE_STREAM)
    await broker.close()
    conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transactional Outbox demo.")
    parser.add_argument(
        "--naive",
        action="store_true",
        help="run the intentionally broken dual-write baseline (naive.py) instead",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_naive() if _parse_args().naive else main())
