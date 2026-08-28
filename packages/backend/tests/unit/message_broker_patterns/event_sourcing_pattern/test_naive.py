"""The naive CRUD store must destroy history. These tests fail if it stops.

``stored_state`` dumps *every* row in *every* table that mentions the account, so
adding an event log (or keeping prior rows) to ``event_sourcing_pattern/naive.py``
makes two divergent histories distinguishable again and turns these tests red.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Generator

import pytest

from message_broker_patterns.event_sourcing_pattern.naive import (
    create_account,
    create_accounts_table,
    deposit,
    get_balance,
    withdraw,
)


@pytest.fixture()
def naive_conn() -> Generator[sqlite3.Connection, None, None]:
    """A database with the state-only schema and nothing else."""
    conn = sqlite3.connect(":memory:")
    create_accounts_table(conn)
    yield conn
    conn.close()


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0] for row in rows}


def _stored_state(conn: sqlite3.Connection, account_id: str) -> list[tuple[object, ...]]:
    """Every persisted row about this account, across every table, minus its id.

    This is the total information the store retains. Two accounts whose stored
    state is equal are indistinguishable — no query, no tool, no amount of
    cleverness can tell their histories apart afterwards.
    """
    state: list[tuple[object, ...]] = []
    for table in sorted(_table_names(conn)):
        columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if "account_id" not in columns:
            continue
        keep = [column for column in columns if column not in {"account_id", "updated_at"}]
        selected = ", ".join(keep)
        rows = conn.execute(
            f"SELECT {selected} FROM {table} WHERE account_id = ?", (account_id,)
        ).fetchall()
        state.extend((table, *row) for row in rows)
    return state


def test_naive_event_sourcing_overwrites_prior_state_leaving_one_row(
    naive_conn: sqlite3.Connection,
) -> None:
    # Arrange / Act — three state changes on one account.
    create_account(naive_conn, "acc-1", "owner-1")
    deposit(naive_conn, "acc-1", 1000)
    withdraw(naive_conn, "acc-1", 500)
    deposit(naive_conn, "acc-1", 200)

    # Assert — the current balance is right, and it is *all* that survived:
    # one row, one table, no event log to replay.
    assert get_balance(naive_conn, "acc-1") == 700
    assert _table_names(naive_conn) == {"accounts"}
    assert len(_stored_state(naive_conn, "acc-1")) == 1


def test_naive_event_sourcing_cannot_distinguish_two_different_histories(
    naive_conn: sqlite3.Connection,
) -> None:
    """Asking what the balance was at time T is unanswerable — the evidence is gone."""
    # Arrange — two accounts that arrive at 700 by very different routes.
    create_account(naive_conn, "acc-busy", "owner-1")
    deposit(naive_conn, "acc-busy", 1000)
    withdraw(naive_conn, "acc-busy", 500)
    deposit(naive_conn, "acc-busy", 200)

    create_account(naive_conn, "acc-quiet", "owner-1")
    deposit(naive_conn, "acc-quiet", 700)

    # Act.
    busy = _stored_state(naive_conn, "acc-busy")
    quiet = _stored_state(naive_conn, "acc-quiet")

    # Assert — four transactions and one transaction are stored identically.
    # An event-sourced store would have 4 events versus 2 and fail here.
    assert busy == quiet


def test_naive_event_sourcing_cannot_rebuild_a_corrupted_balance(
    naive_conn: sqlite3.Connection,
) -> None:
    # Arrange — a real history, then a bad write corrupts the balance.
    create_account(naive_conn, "acc-1", "owner-1")
    deposit(naive_conn, "acc-1", 1000)
    withdraw(naive_conn, "acc-1", 400)
    state_before_corruption = _stored_state(naive_conn, "acc-1")

    # Act — a buggy release overwrites the balance with nonsense.
    with naive_conn:
        naive_conn.execute("UPDATE accounts SET balance = 0 WHERE account_id = 'acc-1'")

    # Assert — nothing survives that could recompute 600: the only record of the
    # truth was the row the bad write replaced.
    assert get_balance(naive_conn, "acc-1") == 0
    assert _stored_state(naive_conn, "acc-1") != state_before_corruption
    assert len(_stored_state(naive_conn, "acc-1")) == 1
