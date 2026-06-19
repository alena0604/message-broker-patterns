import asyncio
import sqlite3

import fakeredis.aioredis

from message_broker_patterns.outbox_pattern.broker import RedisBroker
from message_broker_patterns.outbox_pattern.models import Order
from message_broker_patterns.outbox_pattern.relay import run as relay_run
from message_broker_patterns.outbox_pattern.store import insert_order_with_outbox, poll_outbox

STREAM = "integration:orders"


async def test_full_outbox_flow(
    db_conn: sqlite3.Connection, fake_redis: fakeredis.aioredis.FakeRedis
) -> None:
    orders = [
        Order(order_id=f"o-int-{i}", customer_id=f"c-{i}", amount=float(i * 10))
        for i in range(1, 4)
    ]
    for order in orders:
        insert_order_with_outbox(db_conn, order)

    broker = RedisBroker(fake_redis)
    stop = asyncio.Event()

    async def _stop_after_relay() -> None:
        await asyncio.sleep(0.1)
        stop.set()

    await asyncio.gather(
        relay_run(db_conn, broker, STREAM, stop, poll_interval=0.01),
        _stop_after_relay(),
    )

    messages = await fake_redis.xrange(STREAM)
    assert len(messages) == 3

    order_ids_published = {fields[b"order_id"].decode() for _, fields in messages}
    assert order_ids_published == {"o-int-1", "o-int-2", "o-int-3"}

    assert poll_outbox(db_conn) == []
