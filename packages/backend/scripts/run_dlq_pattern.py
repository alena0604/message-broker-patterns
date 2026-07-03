from message_broker_patterns.logging import init_logger

init_logger()

import asyncio  # noqa: E402
import logging  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.config.settings import settings  # noqa: E402
from message_broker_patterns.dlq_pattern.broker import (  # noqa: E402
    DLQ_STREAM,
    MAIN_STREAM,
    PROCESSED_SET,
    DLQBroker,
)
from message_broker_patterns.dlq_pattern.consumer import run_idempotent_consumer  # noqa: E402
from message_broker_patterns.dlq_pattern.models import Payment  # noqa: E402
from message_broker_patterns.metrics import REGISTRY  # noqa: E402

logger = logging.getLogger("run_dlq")
GROUP = "payment_workers"
MAX_ATTEMPTS = 2

# 6 payments: 4 normal + 2 malformed (negative amount).
PAYMENTS = [
    Payment("PAY-001", 9900, "cust-A", "USD"),  # normal
    Payment("PAY-002", 4500, "cust-B", "EUR"),  # normal
    Payment("PAY-003", -1, "cust-C", "USD"),  # MALFORMED — negative amount
    Payment("PAY-004", 12000, "cust-D", "GBP"),  # normal
    Payment("PAY-005", -99, "cust-E", "USD"),  # MALFORMED
    Payment("PAY-006", 7800, "cust-F", "EUR"),  # normal
]


async def handler(consumer_id: str, payment: Payment) -> None:
    """Reject malformed payments; simulate work for valid ones."""
    if payment.amount_cents < 0:
        raise ValueError(f"malformed amount: {payment.amount_cents}")
    await asyncio.sleep(0.01)  # simulate work
    logger.info(
        "[%s] charged %s — %s %d (%s)",
        consumer_id,
        payment.payment_id,
        payment.currency,
        payment.amount_cents,
        payment.customer_id,
    )


def _terminal_outcomes() -> int:
    """Count payments that have reached a terminal state (processed/skipped/DLQ'd)."""
    for entry in REGISTRY.snapshot():
        if entry["id"] == "dlq":
            counters = entry["counters"]
            return (
                counters.get("payments_processed", 0)
                + counters.get("skipped_duplicate", 0)
                + counters.get("moved_to_dlq", 0)
            )
    return 0


async def _drain(broker: DLQBroker, consumer_id: str, expected: int) -> int:
    """Run one consumer until ``expected`` payments have left the main stream."""
    stop = asyncio.Event()
    baseline = _terminal_outcomes()

    async def _stop_when_drained() -> None:
        while _terminal_outcomes() - baseline < expected:
            await asyncio.sleep(0.02)
        stop.set()

    total, _ = await asyncio.gather(
        run_idempotent_consumer(
            broker, consumer_id, GROUP, handler, stop, max_attempts=MAX_ATTEMPTS, block_ms=20
        ),
        _stop_when_drained(),
    )
    return total


async def main() -> None:
    client = aioredis.from_url(settings.redis_url)
    broker = DLQBroker(client)
    # Clean any leftovers so the demo is reproducible.
    await client.delete(MAIN_STREAM, DLQ_STREAM, PROCESSED_SET)
    await broker.ensure_all_groups(GROUP)

    logger.info("=== Dead Letter Queue Demo: Payment Pipeline ===")

    # --- Phase 1: normal processing -------------------------------------------
    logger.info("--- Phase 1: publish %d payments (4 normal, 2 malformed) ---", len(PAYMENTS))
    for payment in PAYMENTS:
        await broker.publish(payment)

    processed = await _drain(broker, "worker-1", expected=len(PAYMENTS))
    dlq_ids = [
        Payment.from_fields(fields).payment_id
        for _msg_id, fields in await broker.read_dlq(GROUP, "inspector-1", count=100)
    ]
    logger.info("Phase 1 done: %d processed, %d in DLQ %s", processed, len(dlq_ids), dlq_ids)

    # --- Phase 2: replay from DLQ ---------------------------------------------
    logger.info("--- Phase 2: replay every original payment to prove idempotency ---")
    replayed = 0
    for payment in PAYMENTS:
        await broker.publish(payment)
        replayed += 1
    logger.info("Re-queued %d payments onto %s", replayed, MAIN_STREAM)

    processed_2 = await _drain(broker, "worker-2", expected=replayed)
    logger.info(
        "Phase 2 done: %d NEW charges (already-processed payments were skipped, not double-charged)",
        processed_2,
    )
    logger.info(
        "Key insight: normal payments were skipped by the idempotency guard; "
        "malformed payments failed again and returned to the DLQ."
    )

    await client.delete(MAIN_STREAM, DLQ_STREAM, PROCESSED_SET)
    await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
