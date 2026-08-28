"""INTENTIONALLY INCORRECT — the dual-write baseline the outbox pattern replaces.

This module exists to be demonstrated *failing*. Do not copy it into anything
real, and do not "fix" it: its bug is its contract, and
``tests/unit/message_broker_patterns/outbox_pattern/test_naive.py`` fails if the
bug is repaired.

**The invariant it violates.** ``outbox_pattern/README.md``: *"The business write
and the outbox write are one atomic operation."* ``insert_order_with_outbox``
writes the ``orders`` row and the ``outbox`` row inside one ``with conn:``
transaction, and the relay publishes from that durable row afterwards.

**What this does instead.** Two independent steps: ``COMMIT`` the order, then
call the broker. There is no transaction spanning both — there cannot be, a
SQLite commit and a network round-trip have no shared atomic primitive. So there
is a window between them, and a crash or an unreachable broker inside that window
loses the event *permanently*: the order is durable, the event was never sent,
and — because :func:`create_orders_table` creates no ``outbox`` table — there is
no record anywhere that a publish was ever owed. Nothing to retry, nothing to
replay, no way to even enumerate what was lost.

The two obvious rearrangements are no better, which is why the pattern exists:

* publish-then-commit trades a lost event for a ghost event (the downstream
  world hears about an order the database never accepted);
* wrapping the publish in a retry loop shrinks the window but never closes it —
  the process can die between the commit and the first retry.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Awaitable, Callable

from message_broker_patterns.outbox_pattern.models import Order

logger = logging.getLogger(__name__)

# The broker call this baseline makes directly: the same shape as
# ``RedisBroker.publish``, taken as a parameter so a demo or test can wrap it
# with ``chaos.broker_unavailable`` and watch the event disappear.
Publish = Callable[[str, dict[str, str]], Awaitable[str]]


def create_orders_table(conn: sqlite3.Connection) -> None:
    """Create the ``orders`` table — and deliberately *no* ``outbox`` table.

    The missing table is the whole point. With an outbox, a failed publish
    leaves a durable row the relay retries forever. Without one, a failed
    publish leaves nothing at all.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id    TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            amount      REAL NOT NULL,
            created_at  TEXT NOT NULL
        );
    """)


def order_created_event(order: Order) -> dict[str, str]:
    """Build the ``order_created`` event payload for the broker call."""
    return {
        "event": "order_created",
        "order_id": order.order_id,
        "customer_id": order.customer_id,
        "amount": str(order.amount),
    }


async def place_order_dual_write(
    conn: sqlite3.Connection,
    publish: Publish,
    order: Order,
    stream: str,
) -> str:
    """Commit the order, then publish the event — two steps, no atomicity.

    Step 1 commits and is durable from that instant. Step 2 is a separate
    network call. Between them the order exists and the event does not.

    The broker error is **not** swallowed: it propagates, so the caller's
    request fails with a 500 while the order stays committed. That asymmetry is
    the incident — the customer is told the order failed, the database says it
    succeeded, and every downstream consumer is never told anything at all.
    """
    event = order_created_event(order)
    with conn:  # ← step 1: the ONLY transaction. It covers the order alone.
        conn.execute(
            "INSERT INTO orders (order_id, customer_id, amount, created_at) VALUES (?, ?, ?, ?)",
            (order.order_id, order.customer_id, order.amount, order.created_at.isoformat()),
        )
    # ← the window. A crash, a restart, or a down broker here loses the event.
    msg_id = await publish(stream, event)  # ← step 2: unprotected.
    logger.info("Published order_created for %s → stream %s id=%s", order.order_id, stream, msg_id)
    return msg_id


def count_orders(conn: sqlite3.Connection) -> int:
    """Number of committed orders — the figure that will exceed the event count."""
    row = conn.execute("SELECT COUNT(*) FROM orders").fetchone()
    return int(row[0])
