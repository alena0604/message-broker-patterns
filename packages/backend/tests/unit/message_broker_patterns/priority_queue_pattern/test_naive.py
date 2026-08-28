"""The naive single-FIFO queue must invert priority: urgent work served last.

Sorting the batch by priority, or splitting into per-level streams, moves the
urgent ticket forward and turns these tests red.
"""

from __future__ import annotations

import fakeredis.aioredis

from message_broker_patterns.priority_queue_pattern.broker import STREAMS
from message_broker_patterns.priority_queue_pattern.models import Priority, SupportTicket
from message_broker_patterns.priority_queue_pattern.naive import (
    SINGLE_STREAM,
    publish,
    run_fifo_consumer,
)

GROUP = "naive_support"
CONSUMER = "agent-0"
ROUTINE_BACKLOG = 10


def _ticket(ticket_id: str, priority: Priority) -> SupportTicket:
    return SupportTicket(
        ticket_id=ticket_id,
        subject=f"{priority.value} request",
        priority=priority,
        customer_id="cust-1",
    )


async def _publish_backlog_then_urgent(client: fakeredis.aioredis.FakeRedis) -> None:
    """A realistic arrival pattern: routine flood first, fraud alert after."""
    for index in range(ROUTINE_BACKLOG):
        await publish(client, _ticket(f"routine-{index}", Priority.LOW))
    await publish(client, _ticket("fraud-alert", Priority.HIGH))


async def test_naive_priority_queue_serves_the_urgent_ticket_last(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    # Arrange — 10 routine tickets queued ahead of one HIGH-priority alert.
    await _publish_backlog_then_urgent(fake_redis)

    async def handler(consumer_id: str, ticket: SupportTicket) -> None:
        return None

    # Act.
    served = await run_fifo_consumer(fake_redis, GROUP, CONSUMER, handler, max_polls=3)

    # Assert — priority inversion: the alert waits behind the entire backlog.
    served_ids = [ticket.ticket_id for ticket in served]
    assert len(served_ids) == ROUTINE_BACKLOG + 1
    assert served_ids.index("fraud-alert") == ROUTINE_BACKLOG  # i.e. dead last.


async def test_naive_priority_queue_adding_agents_does_not_move_the_urgent_ticket(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    """The obvious fix — more consumers — cannot help; they drain the same line."""
    # Arrange.
    await _publish_backlog_then_urgent(fake_redis)
    order: list[str] = []

    async def handler(consumer_id: str, ticket: SupportTicket) -> None:
        order.append(ticket.ticket_id)

    # Act — three agents on the same queue, one ticket at a time each.
    for agent in range(3):
        await run_fifo_consumer(fake_redis, GROUP, f"agent-{agent}", handler, max_polls=1, count=1)
    await run_fifo_consumer(fake_redis, GROUP, CONSUMER, handler, max_polls=2)

    # Assert — the alert is still dead last; only its *position* mattered.
    assert order[-1] == "fraud-alert"
    assert order.index("fraud-alert") == ROUTINE_BACKLOG


async def test_naive_priority_queue_has_no_per_priority_lanes(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    # Arrange / Act — publish one ticket of each priority.
    for priority in Priority:
        await publish(fake_redis, _ticket(f"t-{priority.value}", priority))

    # Assert — everything landed on one stream; the pattern's three lanes were
    # never created, so there is nothing for a dedicated pool to drain.
    assert await fake_redis.xlen(SINGLE_STREAM) == len(Priority)
    for stream in STREAMS.values():
        assert await fake_redis.exists(stream) == 0
