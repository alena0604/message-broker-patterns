from message_broker_patterns.logging import init_logger

init_logger()

import argparse  # noqa: E402
import asyncio  # noqa: E402
import logging  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.config.settings import settings  # noqa: E402
from message_broker_patterns.priority_queue_pattern import naive  # noqa: E402
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


# --- naive baseline -----------------------------------------------------------
# The same 10 tickets on one stream, with `priority` demoted to a field nobody
# reads. No chaos wrapper: priority inversion needs no injected fault, only the
# arrival order a real support desk sees — routine tickets first, the urgent ones
# whenever they happen to arrive.
NAIVE_GROUP = "naive_support_agents"
NAIVE_MAX_POLLS = 2  # scaffolding so the demo terminates; the design has no bound


def naive_arrival_order() -> tuple[list[SupportTicket], list[SupportTicket]]:
    """The same 10 tickets, split into the order they would really arrive in.

    Routine traffic accumulates first; an urgent ticket shows up whenever it
    shows up. The correct path is indifferent to this — it routes at publish
    time — which is exactly what a single FIFO lane cannot do.
    """
    routine = [t for t in TICKETS if t.priority is not Priority.HIGH]
    urgent = [t for t in TICKETS if t.priority is Priority.HIGH]
    return routine, urgent


async def run_naive() -> None:
    """INTENTIONALLY BROKEN — one FIFO lane for every priority."""
    client = aioredis.from_url(settings.redis_url)
    await client.delete(naive.SINGLE_STREAM, *STREAMS.values())

    routine, urgent = naive_arrival_order()
    arrivals = routine + urgent
    logger.info("=== NAIVE priority queue — one FIFO lane (INTENTIONALLY BROKEN) ===")
    logger.info(
        "publishing %d tickets to the single stream %s, priority carried as a message field",
        len(arrivals),
        naive.SINGLE_STREAM,
    )
    logger.info(
        "arrival order: %d routine tickets, then the %d urgent ones",
        len(routine),
        len(urgent),
    )
    for ticket in arrivals:
        await naive.publish(client, ticket)

    async def handler(consumer_id: str, ticket: SupportTicket) -> None:
        await asyncio.sleep(0.01)  # simulate work

    served = await naive.run_fifo_consumer(
        client, NAIVE_GROUP, "naive-agent", handler, max_polls=NAIVE_MAX_POLLS
    )

    logger.info("=== Results ===")
    for position, ticket in enumerate(served, start=1):
        logger.info(
            "%3d  %-6s %-6s %s",
            position,
            ticket.ticket_id,
            ticket.priority.value,
            ticket.subject[:40],
        )
    first_high = next(t for t in served if t.priority is Priority.HIGH)
    position = served.index(first_high) + 1
    logger.info(
        "%s (%s, '%s') served at position %d of %d — behind %d routine tickets",
        first_high.ticket_id,
        first_high.priority.value,
        first_high.subject[:30],
        position,
        len(served),
        position - 1,
    )
    logger.info(
        "adding agents cannot help: they drain the same lane in the same order, so the "
        "urgent ticket's position in line never improves"
    )
    missing = [stream for stream in STREAMS.values() if not await client.exists(stream)]
    logger.info("per-priority streams that were never created: %s", missing)
    logger.info(
        "Run without --naive: the same %d tickets routed at publish time, HIGH served "
        "at positions 1-3 whatever order they arrived in.",
        len(TICKETS),
    )

    await client.delete(naive.SINGLE_STREAM)
    await client.aclose()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Priority Queue support-ticket demo.")
    parser.add_argument(
        "--naive",
        action="store_true",
        help="run the intentionally broken single-FIFO baseline (naive.py) instead",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_naive() if _parse_args().naive else main())
