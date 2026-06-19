from message_broker_patterns.logging import init_logger

init_logger()

import asyncio  # noqa: E402
import sqlite3  # noqa: E402
import uuid  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.config.settings import settings  # noqa: E402
from message_broker_patterns.outbox_pattern.broker import RedisBroker  # noqa: E402
from message_broker_patterns.outbox_pattern.models import Order  # noqa: E402
from message_broker_patterns.outbox_pattern.relay import run as relay_run  # noqa: E402
from message_broker_patterns.outbox_pattern.store import (  # noqa: E402
    create_tables,
    insert_order_with_outbox,
)

STREAM = "orders:events"


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
    relay_task = asyncio.create_task(
        relay_run(conn, broker, STREAM, stop, poll_interval=0.2)
    )
    await asyncio.sleep(1.0)
    stop.set()
    await relay_task

    messages = await redis_client.xrange(STREAM)
    print(f"\nMessages in Redis stream '{STREAM}': {len(messages)}")
    for msg_id, fields in messages:
        print(f"  {msg_id.decode()}: {fields}")

    await broker.close()
    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
