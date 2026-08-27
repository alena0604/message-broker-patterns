import asyncio

import fakeredis.aioredis

from message_broker_patterns.priority_queue_pattern.broker import PriorityQueueBroker
from message_broker_patterns.priority_queue_pattern.consumer import (
    run_consumer,
    run_strict_priority_consumer,
)
from message_broker_patterns.priority_queue_pattern.models import Priority, SupportTicket

GROUP = "support_agents"


def _ticket(priority: Priority, ticket_id: str) -> SupportTicket:
    return SupportTicket(
        ticket_id=ticket_id,
        subject="subject",
        priority=priority,
        customer_id="cust-A",
    )


async def test_consumer_processes_ticket_and_acks(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_group(Priority.HIGH, GROUP)
    await broker.publish(_ticket(Priority.HIGH, "T-1"))

    handled: list[tuple[str, SupportTicket]] = []

    async def handler(consumer_id: str, ticket: SupportTicket) -> None:
        handled.append((consumer_id, ticket))

    stop = asyncio.Event()

    async def _stop_when_drained() -> None:
        while not handled:
            await asyncio.sleep(0.01)
        stop.set()

    total, _ = await asyncio.gather(
        run_consumer(broker, Priority.HIGH, "agent-1", GROUP, handler, stop, block_ms=10),
        _stop_when_drained(),
    )

    assert total == 1
    assert len(handled) == 1
    assert handled[0][1].ticket_id == "T-1"
    assert await broker.pending_count(Priority.HIGH, GROUP) == 0


async def test_consumer_stops_on_stop_event(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    stop = asyncio.Event()
    stop.set()  # set before the loop body runs

    async def handler(consumer_id: str, ticket: SupportTicket) -> None:
        raise AssertionError("handler must not run when already stopped")

    total = await run_consumer(broker, Priority.HIGH, "agent-1", GROUP, handler, stop, block_ms=10)

    assert total == 0


async def test_strict_priority_drains_high_then_normal_then_low(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    # Publish interleaved so ordering can only come from strict scheduling.
    for i in range(3):
        await broker.publish(_ticket(Priority.LOW, f"L-{i}"))
        await broker.publish(_ticket(Priority.NORMAL, f"N-{i}"))
        await broker.publish(_ticket(Priority.HIGH, f"H-{i}"))

    handled: list[SupportTicket] = []

    async def handler(consumer_id: str, ticket: SupportTicket) -> None:
        handled.append(ticket)

    stop = asyncio.Event()
    total_expected = 9

    async def _stop_when_drained() -> None:
        while len(handled) < total_expected:
            await asyncio.sleep(0.001)
        stop.set()

    total, _ = await asyncio.gather(
        run_strict_priority_consumer(broker, "agent-1", GROUP, handler, stop),
        _stop_when_drained(),
    )

    assert total == total_expected
    priorities_in_order = [t.priority for t in handled]
    high_indices = [i for i, p in enumerate(priorities_in_order) if p is Priority.HIGH]
    normal_indices = [i for i, p in enumerate(priorities_in_order) if p is Priority.NORMAL]
    low_indices = [i for i, p in enumerate(priorities_in_order) if p is Priority.LOW]
    # Every HIGH before every NORMAL; every NORMAL before every LOW.
    assert max(high_indices) < min(normal_indices)
    assert max(normal_indices) < min(low_indices)
    assert len(handled) == total_expected


async def test_strict_priority_processes_low_only(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)
    await broker.publish(_ticket(Priority.LOW, "L-1"))
    await broker.publish(_ticket(Priority.LOW, "L-2"))

    handled: list[SupportTicket] = []

    async def handler(consumer_id: str, ticket: SupportTicket) -> None:
        handled.append(ticket)

    stop = asyncio.Event()

    async def _stop_when_drained() -> None:
        while len(handled) < 2:
            await asyncio.sleep(0.001)
        stop.set()

    total, _ = await asyncio.gather(
        run_strict_priority_consumer(broker, "agent-1", GROUP, handler, stop),
        _stop_when_drained(),
    )

    assert total == 2
    assert {t.ticket_id for t in handled} == {"L-1", "L-2"}
    assert await broker.pending_count(Priority.LOW, GROUP) == 0


async def test_strict_priority_preempts_low_when_high_arrives_late(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    await broker.ensure_all_groups(GROUP)

    # Pre-publish only LOW work; the consumer will start draining it.
    for i in range(3):
        await broker.publish(_ticket(Priority.LOW, f"L-{i}"))

    handled: list[SupportTicket] = []
    high_published = asyncio.Event()

    async def handler(consumer_id: str, ticket: SupportTicket) -> None:
        handled.append(ticket)
        # After the first LOW is processed, inject HIGH work mid-stream.
        if not high_published.is_set() and ticket.priority is Priority.LOW:
            await broker.publish(_ticket(Priority.HIGH, "H-0"))
            await broker.publish(_ticket(Priority.HIGH, "H-1"))
            high_published.set()

    stop = asyncio.Event()
    total_expected = 5  # 3 LOW + 2 late HIGH

    async def _stop_when_drained() -> None:
        # Wait until the late HIGH tickets exist before counting toward the
        # final total — otherwise we could stop at the 3 pre-published LOWs.
        await high_published.wait()
        while len(handled) < total_expected:
            await asyncio.sleep(0.001)
        stop.set()

    # count=1 forces a full HIGH->NORMAL->LOW rescan after every single message,
    # so a HIGH ticket that appears mid-drain preempts the remaining LOW work.
    total, _ = await asyncio.gather(
        run_strict_priority_consumer(broker, "agent-1", GROUP, handler, stop, count=1),
        _stop_when_drained(),
    )

    assert total == total_expected
    priorities = [t.priority for t in handled]
    high_indices = [i for i, p in enumerate(priorities) if p is Priority.HIGH]
    # L-0 triggered the publish; only L-1 and L-2 should be preempted.
    remaining_low_indices = [i for i, p in enumerate(priorities) if p is Priority.LOW][1:]
    assert max(high_indices) < min(remaining_low_indices), (
        f"Expected HIGH tickets to preempt remaining LOW. Order: {[t.ticket_id for t in handled]}"
    )


async def test_strict_priority_stops_on_stop_event(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    broker = PriorityQueueBroker(fake_redis)
    stop = asyncio.Event()
    stop.set()  # set before the loop body runs

    async def handler(consumer_id: str, ticket: SupportTicket) -> None:
        raise AssertionError("handler must not run when already stopped")

    total = await run_strict_priority_consumer(broker, "agent-1", GROUP, handler, stop)

    assert total == 0
