from message_broker_patterns.logging import init_logger

init_logger()

import asyncio  # noqa: E402
import uuid  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.config.settings import settings  # noqa: E402
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
    summary = await project(store, account_id, stop, expected_version=live.version, poll_interval=0.1)
    print(f"(c) projector AccountSummary balance:               {summary.balance}")

    all_equal = live.balance == replayed.balance == summary.balance
    print(f"\n  PROOF: (a) == (b) == (c)?  {all_equal}  ({live.balance} == "
          f"{replayed.balance} == {summary.balance})")

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


if __name__ == "__main__":
    asyncio.run(main())
