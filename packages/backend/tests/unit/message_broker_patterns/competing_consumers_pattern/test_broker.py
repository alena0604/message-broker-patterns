import fakeredis.aioredis
import pytest
from pytest_mock import MockerFixture
from redis.exceptions import ResponseError

from message_broker_patterns.competing_consumers_pattern.broker import (
    CompetingConsumersBroker,
)

STREAM = "test:cc:stream"
GROUP = "workers"


async def test_ensure_group_creates_group_first_time(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)

    created = await broker.ensure_group(STREAM, GROUP)

    assert created is True


async def test_ensure_group_is_idempotent(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(STREAM, GROUP)

    created_again = await broker.ensure_group(STREAM, GROUP)

    assert created_again is False


async def test_ensure_group_reraises_non_busygroup_error(
    fake_redis: fakeredis.aioredis.FakeRedis, mocker: MockerFixture
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    mocker.patch.object(
        fake_redis,
        "xgroup_create",
        side_effect=ResponseError("WRONGTYPE Operation against a key"),
    )

    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await broker.ensure_group(STREAM, GROUP)


async def test_publish_returns_decoded_message_id(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)

    msg_id = await broker.publish(STREAM, {"task_id": "t-1", "payload": "p"})

    assert isinstance(msg_id, str)
    assert "-" in msg_id  # Redis stream ids look like <ms>-<seq>


async def test_read_new_returns_empty_on_timeout(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(STREAM, GROUP)

    batch = await broker.read_new(STREAM, GROUP, "c1", count=10, block_ms=1)

    assert batch == []


async def test_read_new_claims_published_message(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(STREAM, GROUP)
    await broker.publish(STREAM, {"task_id": "t-9", "payload": "p9"})

    batch = await broker.read_new(STREAM, GROUP, "c1", count=10, block_ms=10)

    assert len(batch) == 1
    _msg_id, fields = batch[0]
    assert fields[b"task_id"] == b"t-9"


async def test_ack_removes_message_from_pending(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(STREAM, GROUP)
    await broker.publish(STREAM, {"task_id": "t-a", "payload": "pa"})
    ((msg_id, _fields),) = await broker.read_new(STREAM, GROUP, "c1", count=10, block_ms=10)
    assert await broker.pending_count(STREAM, GROUP) == 1

    acked = await broker.ack(STREAM, GROUP, msg_id)

    assert acked == 1
    assert await broker.pending_count(STREAM, GROUP) == 0


async def test_reclaim_stale_returns_unacked_message(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(STREAM, GROUP)
    await broker.publish(STREAM, {"task_id": "t-stale", "payload": "ps"})
    # c1 reads but never acks — simulating a crash.
    await broker.read_new(STREAM, GROUP, "c1", count=10, block_ms=10)

    # min_idle 0 → immediately reclaimable by a surviving sibling.
    reclaimed = await broker.reclaim_stale(STREAM, GROUP, "c2", min_idle_ms=0, count=10)

    assert len(reclaimed) == 1
    _msg_id, fields = reclaimed[0]
    assert fields[b"task_id"] == b"t-stale"


async def test_reclaim_stale_skips_fresh_pending(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(STREAM, GROUP)
    await broker.publish(STREAM, {"task_id": "t-fresh", "payload": "pf"})
    await broker.read_new(STREAM, GROUP, "c1", count=10, block_ms=10)

    # High idle threshold → the just-read message is not stale yet.
    reclaimed = await broker.reclaim_stale(STREAM, GROUP, "c2", min_idle_ms=60_000, count=10)

    assert reclaimed == []
