import asyncio

import fakeredis.aioredis

from message_broker_patterns.priority_queue_pattern.broker import PriorityQueueBroker
from message_broker_patterns.priority_queue_pattern.consumer import run_consumer
from message_broker_patterns.priority_queue_pattern.models import Priority, SupportTicket

GROUP = "support_agents"


def _ticket(priority: Priority, ticket_id: str) -> SupportTicket:
    return SupportTicket(
        ticket_id=ticket_id,
        subject=f"subject-{ticket_id}",
        priority=priority,
        customer_id="cust-A",
    )


async def test_high_priority_consumers_process_all_high_tickets(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_group(Priority.HIGH, GROUP)

    total_tickets = 10
    for i in range(total_tickets):
        await broker.publish(_ticket(Priority.HIGH, f"H-{i}"))

    handled_by: dict[str, list[str]] = {"high-1": [], "high-2": []}
    lock = asyncio.Lock()

    def make_handler(consumer_id: str):
        async def handler(cid: str, ticket: SupportTicket) -> None:
            await asyncio.sleep(0)  # interleave the two consumers
            async with lock:
                handled_by[consumer_id].append(ticket.ticket_id)

        return handler

    stop = asyncio.Event()

    async def _stop_when_drained() -> None:
        while (
            await broker.pending_count(Priority.HIGH, GROUP) > 0
            or sum(len(v) for v in handled_by.values()) < total_tickets
        ):
            await asyncio.sleep(0.01)
        stop.set()

    results = await asyncio.gather(
        run_consumer(
            broker,
            Priority.HIGH,
            "high-1",
            GROUP,
            make_handler("high-1"),
            stop,
            count=3,
            block_ms=10,
        ),
        run_consumer(
            broker,
            Priority.HIGH,
            "high-2",
            GROUP,
            make_handler("high-2"),
            stop,
            count=3,
            block_ms=10,
        ),
        _stop_when_drained(),
    )

    all_handled = [tid for ids in handled_by.values() for tid in ids]
    expected = {f"H-{i}" for i in range(total_tickets)}

    # exactly once: every ticket processed, no duplicates
    assert len(all_handled) == total_tickets
    assert set(all_handled) == expected

    # each consumer's tickets are disjoint
    seen: set[str] = set()
    for ids in handled_by.values():
        consumer_set = set(ids)
        assert consumer_set.isdisjoint(seen), "a ticket was processed by two consumers"
        seen |= consumer_set

    # the two run_consumer return values sum to the total
    assert sum(results[:2]) == total_tickets
    assert await broker.pending_count(Priority.HIGH, GROUP) == 0


async def test_consumers_only_process_their_own_priority(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_group(Priority.HIGH, GROUP)
    await broker.ensure_group(Priority.LOW, GROUP)

    for i in range(3):
        await broker.publish(_ticket(Priority.HIGH, f"H-{i}"))
        await broker.publish(_ticket(Priority.LOW, f"L-{i}"))

    high_handled: list[str] = []
    low_handled: list[str] = []

    async def high_handler(cid: str, ticket: SupportTicket) -> None:
        high_handled.append(ticket.ticket_id)

    async def low_handler(cid: str, ticket: SupportTicket) -> None:
        low_handled.append(ticket.ticket_id)

    stop = asyncio.Event()

    async def _stop_when_drained() -> None:
        while (
            await broker.pending_count(Priority.HIGH, GROUP) > 0
            or await broker.pending_count(Priority.LOW, GROUP) > 0
            or len(high_handled) < 3
            or len(low_handled) < 3
        ):
            await asyncio.sleep(0.01)
        stop.set()

    await asyncio.gather(
        run_consumer(broker, Priority.HIGH, "high-agent", GROUP, high_handler, stop, block_ms=10),
        run_consumer(broker, Priority.LOW, "low-agent", GROUP, low_handler, stop, block_ms=10),
        _stop_when_drained(),
    )

    assert set(high_handled) == {"H-0", "H-1", "H-2"}
    assert set(low_handled) == {"L-0", "L-1", "L-2"}
    # No cross-contamination between priority lanes.
    assert all(tid.startswith("H-") for tid in high_handled)
    assert all(tid.startswith("L-") for tid in low_handled)
