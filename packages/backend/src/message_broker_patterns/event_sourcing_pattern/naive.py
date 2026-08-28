"""INTENTIONALLY INCORRECT — the state-only CRUD baseline event sourcing replaces.

This module exists to be demonstrated *failing*. Its bug is its contract:
``tests/unit/message_broker_patterns/event_sourcing_pattern/test_naive.py`` fails
if the bug is repaired.

**The invariant it violates.** ``event_sourcing_pattern/README.md``: *"store every
state change as an immutable event; current state is a fold over the event
history"*, on an append-only store where *"events are never updated or deleted"*.
That is what makes ``BankAccount.replay`` and the projector's read model possible
at all.

**What this does instead.** The ❌ diagram at the top of that README: one
``accounts`` row per account, and every deposit or withdrawal is an
``UPDATE … SET balance = ?``. It is the default design in every framework, it is
fast, and it answers today's question perfectly.

What it destroys is everything *except* today's question. An ``UPDATE`` is
information-lossy by construction — the prior value is overwritten in place, so
after the write there is no representation of it anywhere. Concretely, this store
cannot answer:

* *what was the balance last Tuesday?* — no prior value survives;
* *how did we get to 700?* — one deposit, or a deposit and a withdrawal, are
  literally indistinguishable afterwards (see the tests);
* *rebuild the read model / derive a new view* — there is nothing to replay.

Adding an ``updated_at`` column does not help: it records *when* the last write
happened, not what it changed. Neither does a nightly snapshot — it samples the
state, so everything between two samples is still gone.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


class AccountNotFoundError(KeyError):
    """No row for this account id."""


class InsufficientFundsError(Exception):
    """A withdrawal exceeded the current balance. (The one thing state alone can check.)"""


def create_accounts_table(conn: sqlite3.Connection) -> None:
    """Create the state-only schema: one mutable row per account, no event log.

    The absence of an ``events`` table is the design, not an omission.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            account_id TEXT PRIMARY KEY,
            owner_id   TEXT NOT NULL,
            balance    INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
    """)


def create_account(conn: sqlite3.Connection, account_id: str, owner_id: str) -> None:
    """Insert the account row at zero balance."""
    with conn:
        conn.execute(
            "INSERT INTO accounts (account_id, owner_id, balance, updated_at) VALUES (?, ?, ?, ?)",
            (account_id, owner_id, 0, datetime.now(UTC).isoformat()),
        )
    logger.info("Created account %s for %s", account_id, owner_id)


def deposit(conn: sqlite3.Connection, account_id: str, amount: int) -> int:
    """Overwrite the balance with ``balance + amount``. Returns the new balance."""
    if amount <= 0:
        raise ValueError("Deposit amount must be positive")
    return _overwrite_balance(conn, account_id, get_balance(conn, account_id) + amount)


def withdraw(conn: sqlite3.Connection, account_id: str, amount: int) -> int:
    """Overwrite the balance with ``balance - amount``. Returns the new balance."""
    if amount <= 0:
        raise ValueError("Withdrawal amount must be positive")
    balance = get_balance(conn, account_id)
    if amount > balance:
        raise InsufficientFundsError(f"Cannot withdraw {amount} from balance {balance}")
    return _overwrite_balance(conn, account_id, balance - amount)


def _overwrite_balance(conn: sqlite3.Connection, account_id: str, new_balance: int) -> int:
    """The lossy write. The previous balance is gone the instant this commits."""
    with conn:
        conn.execute(
            "UPDATE accounts SET balance = ?, updated_at = ? WHERE account_id = ?",
            (new_balance, datetime.now(UTC).isoformat(), account_id),
        )
    logger.info("Account %s balance overwritten to %d", account_id, new_balance)
    return new_balance


def get_balance(conn: sqlite3.Connection, account_id: str) -> int:
    """The only question this store can answer: the balance *right now*."""
    row = conn.execute(
        "SELECT balance FROM accounts WHERE account_id = ?", (account_id,)
    ).fetchone()
    if row is None:
        raise AccountNotFoundError(account_id)
    return int(row[0])
