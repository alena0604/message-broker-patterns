"""INTENTIONALLY INCORRECT — the retry-forever baseline the DLQ pattern replaces.

This module exists to be demonstrated *failing*. Its bug is its contract:
``tests/unit/message_broker_patterns/dlq_pattern/test_naive.py`` fails if the bug
is repaired.

**The invariant it violates.** ``dlq_pattern/README.md``: *"'Retry' with no bound
is not a failure policy — a message that can never succeed needs somewhere to
go."* The real consumer counts attempts and calls ``move_to_dlq`` once the budget
is spent, which acks the poison message off the main stream.

**What this does instead.** The loop the README's ❌ diagram draws. A failed
message is left unacked — correct so far, that is how redelivery works — but
nothing counts the attempts and nothing ever parks it. Two consequences, and the
second is the one that pages you:

* the poison message is retried without end, burning CPU and broker round-trips;
* because the consumer processes its pending backlog **in order** and stops the
  batch at the first failure, every message queued behind the poison one is
  *starved* — head-of-line blocking. A single malformed payment stalls the whole
  pipeline, and nothing anywhere records which payment is broken.

``max_polls`` exists only so a test or demo terminates. The design it models has
no bound; that parameter is the scaffolding, not the behaviour.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from message_broker_patterns.dlq_pattern.broker import DLQBroker
from message_broker_patterns.dlq_pattern.models import Payment

logger = logging.getLogger(__name__)

# Same shape as the real consumer's handler: raising means the payment failed.
Handler = Callable[[str, Payment], Awaitable[None]]


@dataclass
class NaiveRunResult:
    """What one run of the retry-forever consumer managed to achieve.

    ``processed`` is the honest measure of the damage: with a poison message at
    the head it stays empty however long the consumer runs.
    """

    processed: list[str] = field(default_factory=list)
    failures: dict[str, int] = field(default_factory=dict)
    polls: int = 0

    def attempts_for(self, payment_id: str) -> int:
        """How many times a single payment was retried."""
        return self.failures.get(payment_id, 0)


async def run_retry_forever_consumer(
    broker: DLQBroker,
    consumer_id: str,
    group: str,
    handler: Handler,
    *,
    max_polls: int,
    count: int = 10,
    idle_sleep: float = 0.0,
) -> NaiveRunResult:
    """Consume ``payments:main`` with no retry budget and no dead-letter queue.

    Each poll re-reads this consumer's unacked backlog first (``XREADGROUP … 0``,
    the only way Redis redelivers) and walks it in stream order. A handler
    failure ``break``\\ s the batch so ordering is preserved — the intuitive
    choice, and the one that converts "one bad message" into "no messages at
    all".
    """
    await broker.ensure_all_groups(group)
    result = NaiveRunResult()
    logger.info("consumer=%s started retry-forever loop (group=%s)", consumer_id, group)

    while result.polls < max_polls:
        result.polls += 1
        batch = await broker.read_pending(group, consumer_id, count)
        if not batch:
            batch = await broker.read_new(group, consumer_id, count, 0)
        if not batch:
            await asyncio.sleep(idle_sleep)
            continue
        for msg_id, fields in batch:
            payment = Payment.from_fields(fields)
            try:
                await handler(consumer_id, payment)
            except Exception as exc:
                # No attempt counter, no budget, no `move_to_dlq`: leave it
                # unacked and come back to the very same message next poll.
                attempts = result.failures.get(payment.payment_id, 0) + 1
                result.failures[payment.payment_id] = attempts
                logger.warning(
                    "consumer=%s failed payment=%s msg=%s attempt=%d — retrying forever: %s",
                    consumer_id,
                    payment.payment_id,
                    msg_id,
                    attempts,
                    exc,
                )
                break  # ← head-of-line blocking: everything behind it waits.
            await broker.ack(group, msg_id)
            result.processed.append(payment.payment_id)
            logger.info(
                "consumer=%s processed payment=%s msg=%s", consumer_id, payment.payment_id, msg_id
            )

    logger.info(
        "consumer=%s stopped after %d poll(s) — processed %d payment(s)",
        consumer_id,
        result.polls,
        len(result.processed),
    )
    return result
