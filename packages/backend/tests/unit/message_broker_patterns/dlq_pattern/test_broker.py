import fakeredis.aioredis

from message_broker_patterns.dlq_pattern.broker import (
    DLQ_STREAM,
    MAIN_STREAM,
    DLQBroker,
)
from message_broker_patterns.dlq_pattern.models import Payment

GROUP = "payment_workers"


def _payment(payment_id: str = "PAY-1", amount_cents: int = 9900) -> Payment:
    return Payment(
        payment_id=payment_id,
        amount_cents=amount_cents,
        customer_id="cust-A",
        currency="USD",
    )


async def test_publish_appends_to_main_stream(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)

    await broker.publish(_payment("PAY-main"))

    raw = await fake_redis.xread({MAIN_STREAM: "0"})
    assert raw, "expected a message on the main stream"
    _stream_name, entries = raw[0]
    (_msg_id, fields) = entries[0]
    assert fields[b"payment_id"] == b"PAY-main"
    assert await fake_redis.xlen(DLQ_STREAM) == 0


async def test_move_to_dlq_acks_main_and_adds_dlq_entry(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_payment("PAY-poison", -1))
    ((msg_id, fields),) = await broker.read_new(GROUP, "worker-1", count=10, block_ms=10)
    payment = Payment.from_fields(fields)

    dlq_id = await broker.move_to_dlq(GROUP, msg_id, payment, "ValueError", 2)

    assert dlq_id
    # Acked off the main stream — no longer pending.
    pending = await fake_redis.xpending(MAIN_STREAM, GROUP)
    assert pending["pending"] == 0
    # Landed on the DLQ stream with reason + attempt fields.
    raw = await fake_redis.xread({DLQ_STREAM: "0"})
    _stream_name, entries = raw[0]
    (_dlq_msg_id, dlq_fields) = entries[0]
    assert dlq_fields[b"payment_id"] == b"PAY-poison"
    assert dlq_fields[b"amount_cents"] == b"-1"
    assert dlq_fields[b"reason"] == b"ValueError"
    assert dlq_fields[b"attempt"] == b"2"


async def test_is_processed_and_mark_processed_round_trip(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)

    assert await broker.is_processed("PAY-1") is False

    await broker.mark_processed("PAY-1")

    assert await broker.is_processed("PAY-1") is True
    assert await broker.is_processed("PAY-2") is False


async def test_ensure_group_is_idempotent(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)

    created = await broker.ensure_group(MAIN_STREAM, GROUP)
    created_again = await broker.ensure_group(MAIN_STREAM, GROUP)

    assert created is True
    assert created_again is False


async def test_ensure_all_groups_covers_both_streams(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)

    await broker.ensure_all_groups(GROUP)

    # Both streams accept the group — reading returns empty, not an error.
    assert await broker.read_new(GROUP, "worker-1", count=10, block_ms=10) == []
    assert await broker.read_dlq(GROUP, "worker-1", count=10) == []


async def test_read_dlq_and_ack_dlq(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_payment("PAY-poison", -1))
    ((msg_id, fields),) = await broker.read_new(GROUP, "worker-1", count=10, block_ms=10)
    await broker.move_to_dlq(GROUP, msg_id, Payment.from_fields(fields), "ValueError", 2)

    dlq_batch = await broker.read_dlq(GROUP, "worker-1", count=10)

    assert len(dlq_batch) == 1
    dlq_msg_id, dlq_fields = dlq_batch[0]
    assert dlq_fields[b"payment_id"] == b"PAY-poison"

    acked = await broker.ack_dlq(GROUP, dlq_msg_id)

    assert acked == 1
