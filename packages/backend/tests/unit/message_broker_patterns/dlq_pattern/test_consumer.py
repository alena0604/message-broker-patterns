import asyncio

import fakeredis.aioredis

from message_broker_patterns.dlq_pattern.broker import DLQ_STREAM, MAIN_STREAM, DLQBroker
from message_broker_patterns.dlq_pattern.consumer import run_idempotent_consumer
from message_broker_patterns.dlq_pattern.models import Payment
from message_broker_patterns.metrics import REGISTRY

GROUP = "payment_workers"


def _skipped_count() -> int:
    for entry in REGISTRY.snapshot():
        if entry["id"] == "dlq":
            return int(entry["counters"].get("skipped_duplicate", 0))
    return 0


def _payment(payment_id: str, amount_cents: int = 9900) -> Payment:
    return Payment(
        payment_id=payment_id,
        amount_cents=amount_cents,
        customer_id="cust-A",
        currency="USD",
    )


async def test_consumer_processes_payment_marks_and_acks(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_payment("PAY-1"))

    handled: list[Payment] = []

    async def handler(consumer_id: str, payment: Payment) -> None:
        handled.append(payment)

    stop = asyncio.Event()

    async def _stop_when_drained() -> None:
        while not handled:
            await asyncio.sleep(0.01)
        stop.set()

    total, _ = await asyncio.gather(
        run_idempotent_consumer(broker, "worker-1", GROUP, handler, stop, block_ms=10),
        _stop_when_drained(),
    )

    assert total == 1
    assert [p.payment_id for p in handled] == ["PAY-1"]
    assert await broker.is_processed("PAY-1") is True


async def test_malformed_payment_lands_in_dlq_after_max_attempts(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_payment("PAY-bad", -1))

    attempts: list[str] = []

    async def handler(consumer_id: str, payment: Payment) -> None:
        attempts.append(payment.payment_id)
        raise ValueError("negative amount")

    stop = asyncio.Event()

    async def _stop_when_dlq() -> None:
        while await fake_redis.xlen(DLQ_STREAM) == 0:
            await asyncio.sleep(0.01)
        stop.set()

    total, _ = await asyncio.gather(
        run_idempotent_consumer(
            broker, "worker-1", GROUP, handler, stop, max_attempts=2, block_ms=10
        ),
        _stop_when_dlq(),
    )

    assert total == 0
    # Handler ran exactly max_attempts times before the message was DLQ'd.
    assert len(attempts) == 2
    raw = await fake_redis.xread({DLQ_STREAM: "0"})
    _stream, entries = raw[0]
    (_dlq_id, dlq_fields) = entries[0]
    assert dlq_fields[b"payment_id"] == b"PAY-bad"
    assert dlq_fields[b"reason"] == b"ValueError"
    assert dlq_fields[b"attempt"] == b"2"
    assert await broker.is_processed("PAY-bad") is False


async def test_intermediate_failure_does_not_ack(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_payment("PAY-bad", -1))

    call_count = 0

    async def handler(consumer_id: str, payment: Payment) -> None:
        nonlocal call_count
        call_count += 1
        # Yield so the stop-checker task can run — bounds the retry rate and
        # keeps the run well under max_attempts before we halt it.
        await asyncio.sleep(0.002)
        raise ValueError("negative amount")

    stop = asyncio.Event()

    async def _stop_after_retries() -> None:
        # Wait for a few redeliveries, then halt long before max_attempts.
        while call_count < 3:
            await asyncio.sleep(0.005)
        stop.set()

    # max_attempts high enough that the message is never DLQ'd during the run —
    # it should keep being redelivered (never acked) instead.
    await asyncio.gather(
        run_idempotent_consumer(
            broker, "worker-1", GROUP, handler, stop, max_attempts=50, block_ms=10
        ),
        _stop_after_retries(),
    )

    # Redelivered multiple times: proves the intermediate failures were not
    # acked, and it never reached the DLQ because attempts were not exhausted.
    assert call_count >= 3
    assert await fake_redis.xlen(DLQ_STREAM) == 0
    pending = await fake_redis.xpending(MAIN_STREAM, GROUP)
    assert pending["pending"] == 1


async def test_idempotency_skips_already_processed_payment(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.mark_processed("PAY-dup")
    await broker.publish(_payment("PAY-dup"))

    called = False

    async def handler(consumer_id: str, payment: Payment) -> None:
        nonlocal called
        called = True

    stop = asyncio.Event()
    baseline = _skipped_count()

    async def _stop_when_skipped() -> None:
        while _skipped_count() == baseline:
            await asyncio.sleep(0.01)
        stop.set()

    total, _ = await asyncio.gather(
        run_idempotent_consumer(broker, "worker-1", GROUP, handler, stop, block_ms=10),
        _stop_when_skipped(),
    )

    assert total == 0
    assert called is False, "handler must not run for an already-processed payment"
    # The duplicate was acked off the main stream without invoking the handler.
    summary = await fake_redis.xpending(MAIN_STREAM, GROUP)
    assert summary["pending"] == 0


async def test_consumer_stops_on_stop_event(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)
    stop = asyncio.Event()
    stop.set()  # set before the loop body runs

    async def handler(consumer_id: str, payment: Payment) -> None:
        raise AssertionError("handler must not run when already stopped")

    total = await run_idempotent_consumer(broker, "worker-1", GROUP, handler, stop, block_ms=10)

    assert total == 0
