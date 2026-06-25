import asyncio

from message_broker_patterns.event_sourcing_pattern.events import (
    AccountCreated,
    MoneyDeposited,
    MoneyWithdrawn,
)
from message_broker_patterns.event_sourcing_pattern.projector import (
    AccountSummary,
    project,
)
from message_broker_patterns.event_sourcing_pattern.store import EventStore


def test_summary_apply_folds_events() -> None:
    summary = AccountSummary(account_id="acc-1")
    for event in [
        AccountCreated(account_id="acc-1", owner_id="owner-1"),
        MoneyDeposited(account_id="acc-1", amount="1000"),
        MoneyWithdrawn(account_id="acc-1", amount="500"),
    ]:
        raw = {k.encode(): v.encode() for k, v in event.to_dict().items()}
        summary.apply(raw)

    assert summary.owner_id == "owner-1"
    assert summary.balance == 500
    assert summary.version == 3
    assert summary.last_updated is not None


async def test_project_catches_up_and_stops(event_store: EventStore) -> None:
    await event_store.append(
        "acc-1", "AccountCreated", {"account_id": "acc-1", "owner_id": "owner-1"}
    )
    await event_store.append("acc-1", "MoneyDeposited", {"account_id": "acc-1", "amount": "1000"})
    await event_store.append("acc-1", "MoneyWithdrawn", {"account_id": "acc-1", "amount": "500"})

    stop = asyncio.Event()
    summary = await project(event_store, "acc-1", stop, expected_version=3, poll_interval=0.01)

    assert summary.balance == 500
    assert summary.version == 3
    assert stop.is_set()


async def test_project_stops_on_external_stop_event(event_store: EventStore) -> None:
    await event_store.append(
        "acc-1", "AccountCreated", {"account_id": "acc-1", "owner_id": "owner-1"}
    )

    stop = asyncio.Event()
    stop.set()  # already stopped before the loop runs
    summary = await project(event_store, "acc-1", stop, expected_version=99, poll_interval=0.01)

    # never processed anything because the loop guard tripped immediately
    assert summary.version == 0
