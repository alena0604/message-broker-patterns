import fakeredis.aioredis

from message_broker_patterns.event_sourcing_pattern.events import (
    MoneyDeposited,
    account_stream,
)
from message_broker_patterns.event_sourcing_pattern.store import EventStore


async def test_append_writes_to_per_account_stream(
    event_store: EventStore,
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    event = MoneyDeposited(account_id="acc-1", amount="1000")
    msg_id = await event_store.append("acc-1", event.event_type, event.to_dict())

    assert msg_id
    msgs = await fake_redis.xrange(account_stream("acc-1"))
    assert len(msgs) == 1
    assert msgs[0][1][b"event_type"] == b"MoneyDeposited"
    assert msgs[0][1][b"amount"] == b"1000"


async def test_read_full_history_from_genesis(event_store: EventStore) -> None:
    await event_store.append("acc-1", "AccountCreated", {"account_id": "acc-1", "owner_id": "o"})
    await event_store.append("acc-1", "MoneyDeposited", {"account_id": "acc-1", "amount": "1000"})

    events = await event_store.read("acc-1")
    assert len(events) == 2
    assert events[0][1][b"event_type"] == b"AccountCreated"
    assert events[1][1][b"event_type"] == b"MoneyDeposited"


async def test_read_only_new_events_since_cursor(event_store: EventStore) -> None:
    first_id = await event_store.append(
        "acc-1", "AccountCreated", {"account_id": "acc-1", "owner_id": "o"}
    )
    await event_store.append("acc-1", "MoneyDeposited", {"account_id": "acc-1", "amount": "1000"})

    events = await event_store.read("acc-1", last_id=first_id)
    assert len(events) == 1
    assert events[0][1][b"event_type"] == b"MoneyDeposited"


async def test_streams_are_isolated_per_account(event_store: EventStore) -> None:
    await event_store.append("acc-1", "MoneyDeposited", {"account_id": "acc-1", "amount": "10"})
    await event_store.append("acc-2", "MoneyDeposited", {"account_id": "acc-2", "amount": "20"})

    acc1 = await event_store.read("acc-1")
    acc2 = await event_store.read("acc-2")
    assert len(acc1) == 1
    assert len(acc2) == 1
    assert acc1[0][1][b"amount"] == b"10"
    assert acc2[0][1][b"amount"] == b"20"


async def test_length_reports_event_count(event_store: EventStore) -> None:
    assert await event_store.length("acc-1") == 0
    await event_store.append("acc-1", "MoneyDeposited", {"account_id": "acc-1", "amount": "10"})
    assert await event_store.length("acc-1") == 1
