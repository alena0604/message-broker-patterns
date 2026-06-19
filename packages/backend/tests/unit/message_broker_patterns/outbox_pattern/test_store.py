import json
import sqlite3

import pytest

from message_broker_patterns.outbox_pattern.models import Order
from message_broker_patterns.outbox_pattern.store import (
    create_tables,
    delete_outbox_entry,
    insert_order_with_outbox,
    poll_outbox,
)


def test_create_tables_idempotent(db_conn: sqlite3.Connection) -> None:
    create_tables(db_conn)  # second call should not raise


def test_insert_order_with_outbox_atomic(db_conn: sqlite3.Connection) -> None:
    order = Order(order_id="o-1", customer_id="c-1", amount=10.0)
    entry = insert_order_with_outbox(db_conn, order)

    row = db_conn.execute("SELECT order_id FROM orders WHERE order_id = ?", ("o-1",)).fetchone()
    assert row is not None
    assert row[0] == "o-1"

    assert entry.id is not None
    outbox_row = db_conn.execute("SELECT order_id FROM outbox WHERE id = ?", (entry.id,)).fetchone()
    assert outbox_row is not None
    assert outbox_row[0] == "o-1"


def test_insert_rolls_back_on_duplicate_order(db_conn: sqlite3.Connection) -> None:
    order = Order(order_id="o-dup", customer_id="c-1", amount=10.0)
    insert_order_with_outbox(db_conn, order)
    with pytest.raises(sqlite3.IntegrityError):
        insert_order_with_outbox(db_conn, order)
    # outbox should still only have one entry
    entries = poll_outbox(db_conn)
    assert len(entries) == 1


def test_poll_outbox_returns_pending_entries(db_conn: sqlite3.Connection) -> None:
    insert_order_with_outbox(db_conn, Order(order_id="o-2", customer_id="c-1", amount=5.0))
    insert_order_with_outbox(db_conn, Order(order_id="o-3", customer_id="c-2", amount=7.0))
    entries = poll_outbox(db_conn)
    assert len(entries) == 2


def test_delete_outbox_entry(db_conn: sqlite3.Connection) -> None:
    entry = insert_order_with_outbox(db_conn, Order(order_id="o-4", customer_id="c-1", amount=1.0))
    assert entry.id is not None
    delete_outbox_entry(db_conn, entry.id)
    assert poll_outbox(db_conn) == []


def test_outbox_payload_is_valid_json(db_conn: sqlite3.Connection) -> None:
    order = Order(order_id="o-5", customer_id="c-1", amount=42.0)
    entry = insert_order_with_outbox(db_conn, order)
    payload = json.loads(entry.payload)
    assert payload["event"] == "order_created"
    assert payload["order_id"] == "o-5"
