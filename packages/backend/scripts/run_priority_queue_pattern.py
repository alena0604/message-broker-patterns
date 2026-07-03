from message_broker_patterns.logging import init_logger

init_logger()

import asyncio  # noqa: E402
import logging  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.config.settings import settings  # noqa: E402
from message_broker_patterns.priority_queue_pattern.broker import (  # noqa: E402
    STREAMS,
    PriorityQueueBroker,
)
from message_broker_patterns.priority_queue_pattern.consumer import (  # noqa: E402
    run_strict_priority_consumer,
)
from message_broker_patterns.priority_queue_pattern.models import (  # noqa: E402
    Priority,
    SupportTicket,
)

logger = logging.getLogger("run_priority_queue")
GROUP = "support_agents"

# 10 tickets across priorities: 3 HIGH, 4 NORMAL, 3 LOW
TICKETS = [
    SupportTicket("T-001", "FRAUD ALERT: suspicious transaction", Priority.HIGH, "cust-A"),
    SupportTicket("T-002", "Card stolen, need immediate block", Priority.HIGH, "cust-B"),
    SupportTicket("T-003", "Account locked, cannot login", Priority.HIGH, "cust-C"),
    SupportTicket("T-004", "Billing discrepancy on last invoice", Priority.NORMAL, "cust-D"),
    SupportTicket("T-005", "Password reset not working", Priority.NORMAL, "cust-E"),
    SupportTicket("T-006", "Update shipping address", Priority.NORMAL, "cust-F"),
    SupportTicket("T-007", "API docs unclear", Priority.NORMAL, "cust-G"),
    SupportTicket("T-008", "Feature request: dark mode", Priority.LOW, "cust-H"),
    SupportTicket("T-009", "Nightly report not emailed", Priority.LOW, "cust-I"),
    SupportTicket("T-010", "Export CSV missing column", Priority.LOW, "cust-J"),
]


async def main() -> None:
    client = aioredis.from_url(settings.redis_url)
    broker = PriorityQueueBroker(client)
    # Clean any leftovers so the demo is reproducible.
    await client.delete(*STREAMS.values())
    await broker.ensure_all_groups(GROUP)

    logger.info("=== Priority Queue Demo: Support Ticket System ===")
    logger.info("Publishing %d tickets (3 HIGH, 4 NORMAL, 3 LOW)", len(TICKETS))
    for ticket in TICKETS:
        await broker.publish(ticket)

    # A single strict-priority consumer polls HIGH → NORMAL → LOW, draining each
    # tier fully before descending. This yields a totally-ordered drain: every
    # HIGH ticket finishes before the first NORMAL starts, and every NORMAL
    # before the first LOW — the production-like strict scheduling this demo
    # exists to show. (Running several such consumers concurrently against one
    # group would relax this to *per-consumer* strict order: an idle consumer
    # would descend to a lower tier while a peer is still working a higher one,
    # reintroducing the cross-tier races this pattern is meant to eliminate.)
    handled_by: dict[str, list[str]] = {}
    lock = asyncio.Lock()

    def make_handler(cid: str):
        async def handler(consumer_id: str, ticket: SupportTicket) -> None:
            await asyncio.sleep(0.01)  # simulate work
            async with lock:
                handled_by.setdefault(consumer_id, []).append(ticket.ticket_id)
            logger.info(
                "[%s] %s — %s (%s)",
                consumer_id,
                ticket.ticket_id,
                ticket.subject[:40],
                ticket.priority.value,
            )

        return handler

    stop = asyncio.Event()
    total_expected = len(TICKETS)

    async def _stop_when_drained() -> None:
        while sum(len(v) for v in handled_by.values()) < total_expected:
            await asyncio.sleep(0.02)
        stop.set()

    cid = "strict-agent"
    await asyncio.gather(
        run_strict_priority_consumer(broker, cid, GROUP, make_handler(cid), stop),
        _stop_when_drained(),
    )

    logger.info("=== Results ===")
    for cid, tickets in sorted(handled_by.items()):
        logger.info("%s handled: %s", cid, tickets)
    await client.delete(*STREAMS.values())
    await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
