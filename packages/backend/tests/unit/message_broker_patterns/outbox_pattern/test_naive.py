"""The naive dual-write baseline must LOSE an event. These tests fail if it stops.

Each assertion is written so that "accidentally fixing" ``outbox_pattern/naive.py``
— adding an outbox table, retrying the publish, publishing before committing —
turns the test red. The failure is the contract.
"""

from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Generator

import fakeredis.aioredis
import pytest

from message_broker_patterns.chaos import BrokerUnavailableError, broker_unavailable
from message_broker_patterns.outbox_pattern.broker import RedisBroker
from message_broker_patterns.outbox_pattern.models import Order
from message_broker_patterns.outbox_pattern.naive import (
    count_orders,
    create_orders_table,
    place_order_dual_write,
)

STREAM = "orders:events"


@pytest.fixture()
def naive_conn() -> Generator[sqlite3.Connection, None, None]:
    """A database with the naive schema: an ``orders`` table and nothing else."""
    conn = sqlite3.connect(":memory:")
    create_orders_table(conn)
    yield conn
    conn.close()


def _order(order_id: str) -> Order:
    return Order(order_id=order_id, customer_id="cust-1", amount=99.99)


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0] for row in rows}


async def test_naive_outbox_loses_event_on_broker_crash(
    naive_conn: sqlite3.Connection,
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    # Arrange — a broker that is down for exactly the second publish.
    broker = RedisBroker(fake_redis)
    publish = broker_unavailable(broker.publish, on_call=2)
    orders = [_order("ord-1"), _order("ord-2"), _order("ord-3")]

    # Act — place three orders; the second one's publish never lands. The
    # caller sees an error it can do nothing useful with — the order is already
    # committed and the event is already unrecoverable.
    for order in orders:
        with contextlib.suppress(BrokerUnavailableError):
            await place_order_dual_write(naive_conn, publish, order, STREAM)

    # Assert — three orders committed, only two events on the stream.
    assert count_orders(naive_conn) == 3
    assert await fake_redis.xlen(STREAM) == 2


async def test_naive_outbox_leaves_nothing_to_recover_the_lost_event_from(
    naive_conn: sqlite3.Connection,
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    # Arrange — the broker is down for the only publish attempt.
    broker = RedisBroker(fake_redis)
    publish = broker_unavailable(broker.publish, on_call=1)

    # Act.
    with pytest.raises(BrokerUnavailableError):
        await place_order_dual_write(naive_conn, publish, _order("ord-1"), STREAM)

    # Assert — the order is durable, the event is gone, and no row anywhere
    # records that a publish was owed. A relay would have nothing to poll.
    assert count_orders(naive_conn) == 1
    assert await fake_redis.xlen(STREAM) == 0
    assert _table_names(naive_conn) == {"orders"}


async def test_naive_outbox_publishes_after_the_commit_so_the_window_exists(
    naive_conn: sqlite3.Connection,
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    """The order is already durable at the instant the broker is called.

    This pins the *ordering* that makes loss possible: if the publish moved
    before the commit the loss window would close (and a different one — ghost
    events — would open).
    """
    # Arrange — a publish that inspects the database at the moment it is called.
    committed_when_publishing: list[int] = []

    async def spy_publish(stream: str, payload: dict[str, str]) -> str:
        committed_when_publishing.append(count_orders(naive_conn))
        return await RedisBroker(fake_redis).publish(stream, payload)

    # Act.
    await place_order_dual_write(naive_conn, spy_publish, _order("ord-1"), STREAM)

    # Assert — the row was visible (committed) before the broker was touched.
    assert committed_when_publishing == [1]
