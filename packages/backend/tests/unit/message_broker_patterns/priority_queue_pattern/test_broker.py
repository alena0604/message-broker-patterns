import fakeredis.aioredis
from pytest_mock import MockerFixture

from message_broker_patterns.metrics import REGISTRY
from message_broker_patterns.priority_queue_pattern.broker import (
    STREAMS,
    PriorityQueueBroker,
)
from message_broker_patterns.priority_queue_pattern.models import Priority, SupportTicket

GROUP = "support_agents"


def _counter(key: str) -> int:
    """Read a priority_queue counter from the shared registry snapshot (0 if absent).

    The broker hard-codes increments against the global ``REGISTRY``, so tests
    assert on deltas / presence rather than exact totals to stay isolated from
    other tests that touch the same pattern id.
    """
    for entry in REGISTRY.snapshot():
        if entry["id"] == "priority_queue":
            return int(entry["counters"].get(key, 0))
    return 0


def _ticket(priority: Priority, ticket_id: str = "T-1") -> SupportTicket:
    return SupportTicket(
        ticket_id=ticket_id,
        subject="subject",
        priority=priority,
        customer_id="cust-A",
    )


async def test_publish_routes_to_correct_stream(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)

    await broker.publish(_ticket(Priority.HIGH, "T-high"))

    raw = await fake_redis.xread({STREAMS[Priority.HIGH]: "0"})
    assert raw, "expected a message on the high-priority stream"
    _stream_name, entries = raw[0]
    (_msg_id, fields) = entries[0]
    assert fields[b"ticket_id"] == b"T-high"
    assert fields[b"priority"] == b"high"
    # The other two streams stayed empty.
    assert await fake_redis.xlen(STREAMS[Priority.NORMAL]) == 0
    assert await fake_redis.xlen(STREAMS[Priority.LOW]) == 0


async def test_publish_increments_registry(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    before_total = _counter("tickets_published")
    before_high = _counter("high_published")

    await broker.publish(_ticket(Priority.HIGH))

    assert _counter("tickets_published") == before_total + 1
    assert _counter("high_published") == before_high + 1


async def test_ensure_group_creates_group(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)

    created = await broker.ensure_group(Priority.HIGH, GROUP)

    assert created is True


async def test_ensure_group_is_idempotent(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_group(Priority.HIGH, GROUP)

    created_again = await broker.ensure_group(Priority.HIGH, GROUP)

    assert created_again is False


async def test_ensure_all_groups_creates_three_streams(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)

    await broker.ensure_all_groups(GROUP)

    # Publish one ticket per priority and confirm each stream's group can read it.
    for priority in Priority:
        await broker.publish(_ticket(priority, f"T-{priority.value}"))
        batch = await broker.read_new(priority, GROUP, "agent-1", count=10, block_ms=10)
        assert len(batch) == 1
        _msg_id, fields = batch[0]
        assert fields[b"ticket_id"] == f"T-{priority.value}".encode()


async def test_read_new_claims_message(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_group(Priority.NORMAL, GROUP)
    await broker.publish(_ticket(Priority.NORMAL, "T-claim"))

    batch = await broker.read_new(Priority.NORMAL, GROUP, "agent-1", count=10, block_ms=10)

    assert len(batch) == 1
    _msg_id, fields = batch[0]
    assert fields[b"ticket_id"] == b"T-claim"


async def test_read_new_block_zero_is_non_blocking(
    fake_redis: fakeredis.aioredis.FakeRedis,
    mocker: MockerFixture,
) -> None:
    # Regression: BLOCK 0 makes real Redis block *forever*, hanging the strict
    # consumer's non-blocking poll. block_ms=0 must send no BLOCK arg (block=None).
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_group(Priority.HIGH, GROUP)
    spy = mocker.spy(fake_redis, "xreadgroup")

    await broker.read_new(Priority.HIGH, GROUP, "agent-1", count=10, block_ms=0)

    assert spy.call_args.kwargs["block"] is None


async def test_read_new_positive_block_is_forwarded(
    fake_redis: fakeredis.aioredis.FakeRedis,
    mocker: MockerFixture,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_group(Priority.HIGH, GROUP)
    spy = mocker.spy(fake_redis, "xreadgroup")

    await broker.read_new(Priority.HIGH, GROUP, "agent-1", count=10, block_ms=50)

    assert spy.call_args.kwargs["block"] == 50


async def test_ack_removes_from_pending(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_group(Priority.LOW, GROUP)
    await broker.publish(_ticket(Priority.LOW, "T-ack"))
    ((msg_id, _fields),) = await broker.read_new(
        Priority.LOW, GROUP, "agent-1", count=10, block_ms=10
    )
    assert await broker.pending_count(Priority.LOW, GROUP) == 1

    acked = await broker.ack(Priority.LOW, GROUP, msg_id)

    assert acked == 1
    assert await broker.pending_count(Priority.LOW, GROUP) == 0
