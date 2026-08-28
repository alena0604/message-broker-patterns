"""The naive retry-forever consumer must starve the pipeline behind a poison message.

Adding an attempt budget, a ``move_to_dlq`` call, or skipping past the failed
message (``continue`` instead of ``break``) turns these tests red.
"""

from __future__ import annotations

import fakeredis.aioredis

from message_broker_patterns.dlq_pattern.broker import DLQ_STREAM, MAIN_STREAM, DLQBroker
from message_broker_patterns.dlq_pattern.models import Payment
from message_broker_patterns.dlq_pattern.naive import run_retry_forever_consumer

GROUP = "naive_payment_workers"
CONSUMER = "worker-0"
POLLS = 5


def _payment(payment_id: str, amount_cents: int = 9900) -> Payment:
    return Payment(
        payment_id=payment_id,
        amount_cents=amount_cents,
        customer_id="cust-A",
        currency="USD",
    )


async def _reject_negative(consumer_id: str, payment: Payment) -> None:
    """The demo handler: a negative amount is unprocessable, forever."""
    if payment.amount_cents < 0:
        raise ValueError(f"malformed amount: {payment.amount_cents}")


async def test_naive_dlq_retries_the_poison_message_without_end(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    # Arrange — one poison payment, nothing behind it.
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_payment("PAY-POISON", amount_cents=-1))

    # Act.
    result = await run_retry_forever_consumer(
        broker, CONSUMER, GROUP, _reject_negative, max_polls=POLLS
    )

    # Assert — retried once per poll, with no budget that could ever stop it.
    assert result.attempts_for("PAY-POISON") == POLLS


async def test_naive_dlq_starves_every_message_behind_the_poison_one(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    # Arrange — a poison payment at the head, two healthy ones behind it.
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_payment("PAY-POISON", amount_cents=-1))
    await broker.publish(_payment("PAY-2"))
    await broker.publish(_payment("PAY-3"))

    # Act.
    result = await run_retry_forever_consumer(
        broker, CONSUMER, GROUP, _reject_negative, max_polls=POLLS
    )

    # Assert — head-of-line blocking: nothing at all got through.
    assert result.processed == []


async def test_naive_dlq_never_parks_the_poison_message(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    # Arrange.
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_payment("PAY-POISON", amount_cents=-1))

    # Act.
    await run_retry_forever_consumer(broker, CONSUMER, GROUP, _reject_negative, max_polls=POLLS)

    # Assert — the dead-letter queue stays empty and the message stays pending
    # on the main stream: no operator will ever be told which payment is broken.
    assert await fake_redis.xlen(DLQ_STREAM) == 0
    assert (await fake_redis.xpending(MAIN_STREAM, GROUP))["pending"] == 1


async def test_naive_dlq_drains_cleanly_when_no_message_is_poison(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    """The baseline is not a strawman — it is correct until a poison message arrives."""
    # Arrange — three healthy payments.
    broker = DLQBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    for payment_id in ("PAY-1", "PAY-2", "PAY-3"):
        await broker.publish(_payment(payment_id))

    # Act.
    result = await run_retry_forever_consumer(
        broker, CONSUMER, GROUP, _reject_negative, max_polls=POLLS
    )

    # Assert.
    assert result.processed == ["PAY-1", "PAY-2", "PAY-3"]
    assert result.failures == {}
