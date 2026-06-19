import json
import logging
import sqlite3
from datetime import UTC, datetime

from message_broker_patterns.outbox_pattern.models import Order, OutboxEntry

logger = logging.getLogger(__name__)


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id    TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            amount      REAL NOT NULL,
            created_at  TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS outbox (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id    TEXT NOT NULL,
            payload     TEXT NOT NULL,
            created_at  TEXT NOT NULL
        );
    """)


def insert_order_with_outbox(conn: sqlite3.Connection, order: Order) -> OutboxEntry:
    payload = json.dumps(
        {
            "event": "order_created",
            "order_id": order.order_id,
            "customer_id": order.customer_id,
            "amount": order.amount,
        }
    )
    entry = OutboxEntry(order_id=order.order_id, payload=payload)
    with conn:
        conn.execute(
            "INSERT INTO orders (order_id, customer_id, amount, created_at) VALUES (?, ?, ?, ?)",
            (order.order_id, order.customer_id, order.amount, order.created_at.isoformat()),
        )
        cursor = conn.execute(
            "INSERT INTO outbox (order_id, payload, created_at) VALUES (?, ?, ?)",
            (entry.order_id, entry.payload, entry.created_at.isoformat()),
        )
        entry.id = cursor.lastrowid
    logger.debug("Inserted order %s with outbox entry %s", order.order_id, entry.id)
    return entry


def poll_outbox(conn: sqlite3.Connection, limit: int = 10) -> list[OutboxEntry]:
    rows = conn.execute(
        "SELECT id, order_id, payload, created_at FROM outbox ORDER BY id LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        OutboxEntry(
            id=row[0],
            order_id=row[1],
            payload=row[2],
            created_at=datetime.fromisoformat(row[3]).replace(tzinfo=UTC),
        )
        for row in rows
    ]


def delete_outbox_entry(conn: sqlite3.Connection, entry_id: int) -> None:
    with conn:
        conn.execute("DELETE FROM outbox WHERE id = ?", (entry_id,))
    logger.debug("Deleted outbox entry %s", entry_id)
