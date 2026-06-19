import asyncio
import sqlite3

import fakeredis.aioredis

from message_broker_patterns.outbox_pattern.broker import RedisBroker
from message_broker_patterns.outbox_pattern.models import Order
from message_broker_patterns.outbox_pattern.relay import run as relay_run
from message_broker_patterns.outbox_pattern.store import insert_order_with_outbox, poll_outbox


async def test_relay_publishes_to_stream(
    db_conn: sqlite3.Connection, fake_redis: fakeredis.aioredis.FakeRedis
) -> None:
    insert_order_with_outbox(db_conn, Order(order_id="o-r1", customer_id="c-1", amount=20.0))

    broker = RedisBroker(fake_redis)
    stop = asyncio.Event()

    async def _stop_after_one_poll() -> None:
        await asyncio.sleep(0.05)
        stop.set()

    await asyncio.gather(
        relay_run(db_conn, broker, "test:stream", stop, poll_interval=0.01),
        _stop_after_one_poll(),
    )

    messages = await fake_redis.xrange("test:stream")
    assert len(messages) == 1
    assert poll_outbox(db_conn) == []


async def test_relay_stops_on_event(
    db_conn: sqlite3.Connection, fake_redis: fakeredis.aioredis.FakeRedis
) -> None:
    broker = RedisBroker(fake_redis)
    stop = asyncio.Event()
    stop.set()  # already set before relay starts
    await relay_run(db_conn, broker, "test:stream", stop, poll_interval=0.01)
    messages = await fake_redis.xrange("test:stream")
    assert messages == []
