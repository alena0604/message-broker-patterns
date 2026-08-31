from message_broker_patterns.logging import init_logger

init_logger()

import argparse  # noqa: E402
import asyncio  # noqa: E402
import sqlite3  # noqa: E402
import uuid  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.config.settings import settings  # noqa: E402
from message_broker_patterns.event_sourcing_pattern import naive  # noqa: E402
from message_broker_patterns.event_sourcing_pattern.aggregate import (  # noqa: E402
    BankAccount,
    InsufficientFundsError,
)
from message_broker_patterns.event_sourcing_pattern.events import (  # noqa: E402
    account_stream,
    decode_event,
)
from message_broker_patterns.event_sourcing_pattern.projector import project  # noqa: E402
from message_broker_patterns.event_sourcing_pattern.store import EventStore  # noqa: E402


async def _persist(store: EventStore, event: object) -> None:
    await store.append(event.account_id, event.event_type, event.to_dict())  # type: ignore[attr-defined]


async def run_happy_path(redis_client: aioredis.Redis) -> None:
    store = EventStore(redis_client)
    account_id = str(uuid.uuid4())

    print(f"\n{'=' * 60}")
    print("  HAPPY PATH — create, deposit, withdraw")
    print(f"  account_id: {account_id}")
    print(f"{'=' * 60}")

    # --- write side: run commands, persist each produced event ---
    live = BankAccount(account_id=account_id)
    await _persist(store, live.create(owner_id="demo-user"))
    await _persist(store, live.deposit(1500))
    await _persist(store, live.deposit(400))
    await _persist(store, live.withdraw(300))
    await _persist(store, live.withdraw(200))
    await _persist(store, live.withdraw(700))
    print(f"\n(a) in-memory BankAccount balance after commands: {live.balance}")

    # --- replay side: rebuild a fresh aggregate from the stream ---
    raw_events = await store.read(account_id)
    replayed = BankAccount.replay(decode_event(raw) for _, raw in raw_events)
    print(f"(b) replayed BankAccount balance (read from store): {replayed.balance}")

    # --- read side: projector builds an independent read model ---
    stop = asyncio.Event()
    summary = await project(
        store, account_id, stop, expected_version=live.version, poll_interval=0.1
    )
    print(f"(c) projector AccountSummary balance:               {summary.balance}")

    all_equal = live.balance == replayed.balance == summary.balance
    print(
        f"\n  PROOF: (a) == (b) == (c)?  {all_equal}  ({live.balance} == "
        f"{replayed.balance} == {summary.balance})"
    )

    print(f"\nEvent stream '{account_stream(account_id)}':")
    for msg_id, fields in await redis_client.xrange(account_stream(account_id)):
        event_type = fields.get(b"event_type", b"?").decode()
        amount = fields.get(b"amount", b"-").decode()
        print(f"  {msg_id.decode()}  {event_type}  amount={amount}")

    await redis_client.delete(account_stream(account_id))
    await store.close()


async def run_failure_path(redis_client: aioredis.Redis) -> None:
    store = EventStore(redis_client)
    account_id = str(uuid.uuid4())

    print(f"\n{'=' * 60}")
    print("  FAILURE PATH — withdraw more than the balance")
    print(f"  account_id: {account_id}")
    print(f"{'=' * 60}")

    live = BankAccount(account_id=account_id)
    await _persist(store, live.create(owner_id="demo-user"))
    await _persist(store, live.deposit(100))

    length_before = await store.length(account_id)
    print(f"\nbalance: {live.balance}, stream length before overdraw: {length_before}")

    try:
        event = live.withdraw(101)
        await _persist(store, event)
    except InsufficientFundsError as exc:
        print(f"  raised InsufficientFundsError: {exc}")

    length_after = await store.length(account_id)
    print(f"stream length after failed withdraw: {length_after}")
    print(f"  PROOF: no spurious event appended?  {length_after == length_before}")

    await redis_client.delete(account_stream(account_id))
    await store.close()


async def main() -> None:
    redis_client = aioredis.from_url(settings.redis_url)

    await run_happy_path(redis_client)
    await run_failure_path(redis_client)

    await redis_client.aclose()


# --- naive baseline -----------------------------------------------------------
# The same six commands the happy path runs, against a store that keeps only the
# current balance. No chaos wrapper and no broker at all: the loss is caused by
# the write itself — `UPDATE ... SET balance` overwrites the prior value — so
# there is no failure to inject.
NAIVE_ACCOUNT = "acc-naive-1"
NAIVE_TWIN = "acc-naive-2"
NAIVE_COMMANDS = [
    ("deposit", 1500),
    ("deposit", 400),
    ("withdraw", 300),
    ("withdraw", 200),
    ("withdraw", 700),
]


async def run_naive() -> None:
    """INTENTIONALLY BROKEN — state-only CRUD, no event log to replay."""
    conn = sqlite3.connect(":memory:")
    naive.create_accounts_table(conn)  # ← an `accounts` table and, deliberately, no events

    print(f"\n{'=' * 60}")
    print("  NAIVE event sourcing — UPDATE ... SET balance (INTENTIONALLY BROKEN)")
    print(f"{'=' * 60}")

    # The same command sequence the happy path replays from its event stream.
    naive.create_account(conn, NAIVE_ACCOUNT, "demo-user")
    for command, amount in NAIVE_COMMANDS:
        getattr(naive, command)(conn, NAIVE_ACCOUNT, amount)

    # A second account that reached the same balance a completely different way.
    naive.create_account(conn, NAIVE_TWIN, "demo-user")
    naive.deposit(conn, NAIVE_TWIN, 700)

    balance = naive.get_balance(conn, NAIVE_ACCOUNT)
    print(f"\n{NAIVE_ACCOUNT}: create + {len(NAIVE_COMMANDS)} transactions → balance {balance}")
    print(f"{NAIVE_TWIN}: create + 1 transaction  → balance {naive.get_balance(conn, NAIVE_TWIN)}")

    print(
        f"\n(a) balance now:                    {balance}   ← the one question this store answers"
    )
    print("(b) replayed from history:          IMPOSSIBLE — there is no event log to fold")
    print("(c) projector / new read model:     IMPOSSIBLE — there is nothing to project from")

    rows = conn.execute("SELECT account_id, balance FROM accounts ORDER BY account_id").fetchall()
    tables = sorted(
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    )
    print(f"\nstored rows: {rows}")
    print(f"tables:      {tables}")
    print(
        f"\n{len(NAIVE_COMMANDS) + 1} commands on {NAIVE_ACCOUNT} and 2 on {NAIVE_TWIN} are now "
        "stored IDENTICALLY."
    )
    print("'What was the balance last Tuesday?' and 'how did we get to 700?' are unanswerable.")

    # Not a strawman: the one invariant state alone *can* still enforce.
    try:
        naive.withdraw(conn, NAIVE_TWIN, balance + 1)
    except naive.InsufficientFundsError as exc:
        print(f"\nFAILURE PATH: {exc} — overdrafts are still rejected; only the history is gone.")

    print(
        "\nRun without --naive: (a) == (b) == (c), and every one of those commands is still "
        "on the stream."
    )
    conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Event Sourcing bank-account demo.")
    parser.add_argument(
        "--naive",
        action="store_true",
        help="run the intentionally broken state-only CRUD baseline (naive.py) instead",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_naive() if _parse_args().naive else main())
