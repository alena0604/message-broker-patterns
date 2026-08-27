"""A pattern surviving `crash_before_ack` against real consumer-group redelivery.

Both flows here drive a pattern's own consumer loop, unmodified, with the
handler wrapped in :func:`~message_broker_patterns.chaos.crash_before_ack`. The
crash lands after the work and before the ack, so the message stays in the
consumer group's pending entries list and Redis Streams (here ``fakeredis``)
redelivers it — via the consumer's own pending re-read in the DLQ pattern, and
via ``XAUTOCLAIM`` by a surviving sibling in the competing-consumers pattern.
"""

import asyncio
from datetime import timedelta

import fakeredis.aioredis
import pytest

from message_broker_patterns.chaos import ConsumerCrashError, crash_before_ack
from message_broker_patterns.competing_consumers_pattern.broker import (
    CompetingConsumersBroker,
)
from message_broker_patterns.competing_consumers_pattern.consumer import run_consumer
from message_broker_patterns.competing_consumers_pattern.models import Task
from message_broker_patterns.dlq_pattern.broker import DLQ_STREAM, MAIN_STREAM, DLQBroker
from message_broker_patterns.dlq_pattern.consumer import run_idempotent_consumer
from message_broker_patterns.dlq_pattern.models import Payment
from message_broker_patterns.entities import InMemoryIdempotencyStore

GROUP = "payment_workers"
CONSUMER = "worker-1"
MAX_ATTEMPTS = 3
PAYMENT_IDS = ["PAY-1", "PAY-2", "PAY-3"]

CC_STREAM = "integration:chaos:tasks"
CC_GROUP = "workers"


def _payment(payment_id: str) -> Payment:
    return Payment(
        payment_id=payment_id,
        amount_cents=9900,
        customer_id="cust-A",
        currency="USD",
    )


async def test_dlq_consumer_survives_a_crash_before_ack_and_settles_the_payment(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    for payment_id in PAYMENT_IDS:
        await broker.publish(_payment(payment_id))

    charged: list[str] = []

    async def handler(consumer_id: str, payment: Payment) -> None:
        charged.append(payment.payment_id)

    # The first delivery charges the card, then the consumer dies before acking.
    crashing = crash_before_ack(handler, on_call=1)

    stop = asyncio.Event()

    async def _stop_when_settled() -> None:
        while (
            len(set(charged)) < len(PAYMENT_IDS)
            or (await fake_redis.xpending(MAIN_STREAM, GROUP))["pending"] > 0
        ):
            await asyncio.sleep(0.01)
        stop.set()

    processed, _ = await asyncio.gather(
        run_idempotent_consumer(
            broker, CONSUMER, GROUP, crashing, stop, max_attempts=MAX_ATTEMPTS, block_ms=10
        ),
        _stop_when_settled(),
    )

    # The crash fired exactly once, on the first delivery.
    assert (crashing.calls, crashing.fires) == (len(PAYMENT_IDS) + 1, 1)

    # The crashed payment came back and settled — nothing was lost, nothing was
    # dead-lettered, and the crash cost one retry out of the budget.
    assert processed == len(PAYMENT_IDS)
    assert charged == [*PAYMENT_IDS, PAYMENT_IDS[0]]
    for payment_id in PAYMENT_IDS:
        assert await broker.is_processed(payment_id) is True
    assert await fake_redis.xlen(DLQ_STREAM) == 0
    assert (await fake_redis.xpending(MAIN_STREAM, GROUP))["pending"] == 0


async def test_an_idempotent_handler_charges_once_despite_the_crash(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    """At-least-once redelivery is only safe if the handler dedups its own work.

    The broker's processed set is written *after* the handler, so a crash
    between the two leaves it unwritten — the consumer's own guard cannot help.
    A dedup store inside the handler can.
    """
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_payment(PAYMENT_IDS[0]))

    charged: list[str] = []
    deliveries: list[str] = []
    store = InMemoryIdempotencyStore(ttl=timedelta(minutes=5))

    async def handler(consumer_id: str, payment: Payment) -> None:
        deliveries.append(payment.payment_id)
        if await store.mark_if_new(payment.payment_id):
            charged.append(payment.payment_id)

    crashing = crash_before_ack(handler, on_call=1)

    stop = asyncio.Event()

    async def _stop_when_settled() -> None:
        while (await fake_redis.xpending(MAIN_STREAM, GROUP))["pending"] > 0 or not charged:
            await asyncio.sleep(0.01)
        stop.set()

    processed, _ = await asyncio.gather(
        run_idempotent_consumer(
            broker, CONSUMER, GROUP, crashing, stop, max_attempts=MAX_ATTEMPTS, block_ms=10
        ),
        _stop_when_settled(),
    )

    # Delivered twice, charged once.
    assert deliveries == [PAYMENT_IDS[0], PAYMENT_IDS[0]]
    assert charged == [PAYMENT_IDS[0]]
    assert processed == 1
    assert await fake_redis.xlen(DLQ_STREAM) == 0


async def test_competing_consumer_crash_before_ack_is_reclaimed_by_a_sibling(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    """The crash kills the consumer outright; the pending message survives it."""
    broker = CompetingConsumersBroker(fake_redis)
    await broker.ensure_group(CC_STREAM, CC_GROUP)
    await broker.publish(CC_STREAM, Task("t-crash", "needs-recovery").to_fields())

    handled: list[tuple[str, str]] = []

    async def handler(consumer_id: str, task: Task) -> None:
        handled.append((consumer_id, task.task_id))

    # This consumer has no try/except around its handler, so the injected crash
    # takes the whole consumer down — exactly what a real process crash does.
    crashing = crash_before_ack(handler, on_call=1)
    with pytest.raises(ConsumerCrashError):
        await run_consumer(
            broker,
            CC_STREAM,
            CC_GROUP,
            "crashed",
            crashing,
            asyncio.Event(),
            block_ms=10,
            reclaim_min_idle_ms=0,
        )

    # The work ran, the ack did not: the message is still pending for "crashed".
    assert handled == [("crashed", "t-crash")]
    assert await broker.pending_count(CC_STREAM, CC_GROUP) == 1

    stop = asyncio.Event()

    async def _stop_when_reclaimed() -> None:
        while len(handled) < 2:
            await asyncio.sleep(0.01)
        stop.set()

    await asyncio.gather(
        run_consumer(
            broker,
            CC_STREAM,
            CC_GROUP,
            "survivor",
            handler,
            stop,
            block_ms=10,
            reclaim_min_idle_ms=0,
        ),
        _stop_when_reclaimed(),
    )

    # Redelivered to a survivor and finished — at-least-once, visibly.
    assert handled == [("crashed", "t-crash"), ("survivor", "t-crash")]
    assert await broker.pending_count(CC_STREAM, CC_GROUP) == 0
