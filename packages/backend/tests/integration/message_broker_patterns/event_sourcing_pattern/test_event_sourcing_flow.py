import asyncio

import pytest

from message_broker_patterns.event_sourcing_pattern.aggregate import (
    BankAccount,
    InsufficientFundsError,
)
from message_broker_patterns.event_sourcing_pattern.events import decode_event
from message_broker_patterns.event_sourcing_pattern.projector import project
from message_broker_patterns.event_sourcing_pattern.store import EventStore


async def _persist(store: EventStore, event: object) -> None:
    # every domain event exposes event_type + to_dict()
    await store.append(event.account_id, event.event_type, event.to_dict())  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("deposits", "withdrawals", "expected_balance"),
    [
        ([1000], [500], 500),
        ([100, 200, 300], [50, 50], 500),
        ([1000], [1000], 0),
    ],
)
async def test_live_replay_projector_balances_match(
    event_store: EventStore,
    deposits: list[int],
    withdrawals: list[int],
    expected_balance: int,
) -> None:
    account_id = "acc-eq"

    # --- write side: run commands, persist each produced event ---
    live = BankAccount(account_id=account_id)
    await _persist(event_store, live.create(owner_id="owner-1"))
    for amount in deposits:
        await _persist(event_store, live.deposit(amount))
    for amount in withdrawals:
        await _persist(event_store, live.withdraw(amount))

    # (b) fresh replay straight from the event store
    raw_events = await event_store.read(account_id)
    replayed = BankAccount.replay(decode_event(raw) for _, raw in raw_events)

    # (c) projector read model
    stop = asyncio.Event()
    summary = await project(
        event_store, account_id, stop, expected_version=live.version, poll_interval=0.01
    )

    assert live.balance == expected_balance
    assert replayed.balance == live.balance
    assert summary.balance == live.balance
    assert summary.version == live.version == replayed.version


async def test_insufficient_funds_leaves_stream_untouched(event_store: EventStore) -> None:
    account_id = "acc-overdraw"

    live = BankAccount(account_id=account_id)
    await _persist(event_store, live.create(owner_id="owner-1"))
    await _persist(event_store, live.deposit(100))

    length_before = await event_store.length(account_id)

    with pytest.raises(InsufficientFundsError):
        event = live.withdraw(101)
        await _persist(event_store, event)  # must not be reached

    length_after = await event_store.length(account_id)
    assert length_after == length_before  # no spurious event appended
    assert live.balance == 100
