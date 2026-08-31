from message_broker_patterns.logging import init_logger

init_logger()

import argparse  # noqa: E402
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
from message_broker_patterns.dlq_pattern.naive import (  # noqa: E402
    run_retry_forever_consumer,
)
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


# --- naive baseline -----------------------------------------------------------
# The same six payments, through a consumer with no attempt budget and no DLQ.
# No chaos wrapper: the poison is already in the workload — PAY-003's negative
# amount makes `handler` raise every single time, which is exactly what an
# unbounded retry loop cannot survive. Injecting a fault would only hide that.
NAIVE_GROUP = "naive_payment_workers"
NAIVE_MAX_POLLS = 10  # scaffolding so the demo terminates; the design has no bound


async def run_naive() -> None:
    """INTENTIONALLY BROKEN — retry forever, no budget, no dead-letter queue."""
    client = aioredis.from_url(settings.redis_url)
    broker = DLQBroker(client)
    await client.delete(MAIN_STREAM, DLQ_STREAM, PROCESSED_SET)
    await broker.ensure_all_groups(NAIVE_GROUP)

    logger.info("=== NAIVE dlq — retry forever (INTENTIONALLY BROKEN) ===")
    logger.info("publishing %d payments (4 normal, 2 malformed)", len(PAYMENTS))
    for payment in PAYMENTS:
        await broker.publish(payment)

    result = await run_retry_forever_consumer(
        broker, "naive-worker-1", NAIVE_GROUP, handler, max_polls=NAIVE_MAX_POLLS
    )

    poison_id, attempts = max(result.failures.items(), key=lambda item: item[1])
    starved = [
        p.payment_id
        for p in PAYMENTS
        if p.payment_id not in result.processed and p.payment_id not in result.failures
    ]
    dlq_length = await client.xlen(DLQ_STREAM)
    pending = await client.xpending(MAIN_STREAM, NAIVE_GROUP)

    logger.info("=== Results ===")
    logger.info(
        "%d payments published, %d processed after %d poll(s): %s",
        len(PAYMENTS),
        len(result.processed),
        result.polls,
        result.processed,
    )
    logger.info("%s retried %d times and is still at the head of the queue", poison_id, attempts)
    logger.info("starved behind it, never attempted once: %s", starved or "none")
    logger.info("%s length: %d — nothing was ever dead-lettered", DLQ_STREAM, dlq_length)
    logger.info("%s pending: %d message(s) still unacked", MAIN_STREAM, pending["pending"])
    logger.info(
        "Run without --naive: the same 6 payments, an attempt budget of %d, and the 2 "
        "malformed ones parked in %s so the other 4 flow through.",
        MAX_ATTEMPTS,
        DLQ_STREAM,
    )

    await client.delete(MAIN_STREAM, DLQ_STREAM, PROCESSED_SET)
    await broker.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dead Letter Queue payment-pipeline demo.")
    parser.add_argument(
        "--naive",
        action="store_true",
        help="run the intentionally broken retry-forever baseline (naive.py) instead",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_naive() if _parse_args().naive else main())
