import asyncio

import fakeredis.aioredis

from message_broker_patterns.dlq_pattern.broker import DLQ_STREAM, MAIN_STREAM, DLQBroker
from message_broker_patterns.dlq_pattern.consumer import run_idempotent_consumer
from message_broker_patterns.dlq_pattern.models import Payment

GROUP = "payment_workers"
CONSUMER = "worker-1"
OPERATOR = "dlq-operator"
MAX_ATTEMPTS = 3

POISON_ID = "PAY-poison"
HEALTHY_IDS = ["PAY-1", "PAY-2", "PAY-3", "PAY-4", "PAY-5"]


def _payment(payment_id: str, amount_cents: int = 9900) -> Payment:
    return Payment(
        payment_id=payment_id,
        amount_cents=amount_cents,
        customer_id="cust-A",
        currency="USD",
    )


def _charging_handler(charged: list[str], attempted: list[str]):
    """A realistic payment handler: it refuses to charge a negative amount.

    Every delivery is recorded in ``attempted`` so the test can count how many
    times the poison message was retried before it was dead-lettered.
    """

    async def handler(consumer_id: str, payment: Payment) -> None:
        attempted.append(payment.payment_id)
        if payment.amount_cents <= 0:
            raise ValueError("amount must be positive")
        charged.append(payment.payment_id)

    return handler


async def test_poison_payment_reaches_the_dlq_while_healthy_payments_settle(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)

    # A poison payment in the middle of an otherwise healthy batch.
    await broker.publish(_payment(HEALTHY_IDS[0]))
    await broker.publish(_payment(HEALTHY_IDS[1]))
    await broker.publish(_payment(POISON_ID, -1))
    for payment_id in HEALTHY_IDS[2:]:
        await broker.publish(_payment(payment_id))

    charged: list[str] = []
    attempted: list[str] = []
    stop = asyncio.Event()

    async def _stop_when_settled() -> None:
        while len(charged) < len(HEALTHY_IDS) or await fake_redis.xlen(DLQ_STREAM) == 0:
            await asyncio.sleep(0.01)
        stop.set()

    processed, _ = await asyncio.gather(
        run_idempotent_consumer(
            broker,
            CONSUMER,
            GROUP,
            _charging_handler(charged, attempted),
            stop,
            max_attempts=MAX_ATTEMPTS,
            block_ms=10,
        ),
        _stop_when_settled(),
    )

    # The poison message did not take the healthy batch down with it.
    assert processed == len(HEALTHY_IDS)
    assert charged == HEALTHY_IDS
    for payment_id in HEALTHY_IDS:
        assert await broker.is_processed(payment_id) is True

    # It was retried exactly max_attempts times, then dead-lettered — never
    # retried forever, never marked processed.
    assert attempted.count(POISON_ID) == MAX_ATTEMPTS
    assert await broker.is_processed(POISON_ID) is False

    # The retry budget is what drained the main stream's pending list: nothing
    # is left in flight, and the main stream still holds all six originals.
    summary = await fake_redis.xpending(MAIN_STREAM, GROUP)
    assert summary["pending"] == 0
    assert await fake_redis.xlen(MAIN_STREAM) == len(HEALTHY_IDS) + 1

    # An operator reads the DLQ through the same broker and sees why it failed.
    assert await fake_redis.xlen(DLQ_STREAM) == 1
    dead_letters = await broker.read_dlq(GROUP, OPERATOR, count=10)
    assert len(dead_letters) == 1
    dlq_id, fields = dead_letters[0]
    assert fields[b"payment_id"] == POISON_ID.encode()
    assert fields[b"reason"] == b"ValueError"
    assert fields[b"attempt"] == str(MAX_ATTEMPTS).encode()
    # The original payment is intact in the DLQ entry, so it can be replayed.
    assert Payment.from_fields(fields) == _payment(POISON_ID, -1)
    assert await broker.ack_dlq(GROUP, dlq_id) == 1


async def test_operator_replays_a_corrected_payment_off_the_dlq(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_payment(POISON_ID, -1))

    charged: list[str] = []
    attempted: list[str] = []
    handler = _charging_handler(charged, attempted)

    stop = asyncio.Event()

    async def _stop_when_dead_lettered() -> None:
        while await fake_redis.xlen(DLQ_STREAM) == 0:
            await asyncio.sleep(0.01)
        stop.set()

    await asyncio.gather(
        run_idempotent_consumer(
            broker, CONSUMER, GROUP, handler, stop, max_attempts=MAX_ATTEMPTS, block_ms=10
        ),
        _stop_when_dead_lettered(),
    )
    assert charged == []

    # --- operator drains the DLQ, fixes the amount, and replays it ---
    dead_letters = await broker.read_dlq(GROUP, OPERATOR, count=10)
    dlq_id, fields = dead_letters[0]
    broken = Payment.from_fields(fields)
    await broker.ack_dlq(GROUP, dlq_id)
    corrected = _payment(broken.payment_id, 4500)
    await broker.publish(corrected)

    replay_stop = asyncio.Event()

    async def _stop_when_charged() -> None:
        while not charged:
            await asyncio.sleep(0.01)
        replay_stop.set()

    replayed, _ = await asyncio.gather(
        run_idempotent_consumer(
            broker, CONSUMER, GROUP, handler, replay_stop, max_attempts=MAX_ATTEMPTS, block_ms=10
        ),
        _stop_when_charged(),
    )

    # The replayed payment settles, and nothing new lands in the DLQ.
    assert replayed == 1
    assert charged == [POISON_ID]
    assert await broker.is_processed(POISON_ID) is True
    assert await fake_redis.xlen(DLQ_STREAM) == 1
    summary = await fake_redis.xpending(MAIN_STREAM, GROUP)
    assert summary["pending"] == 0
