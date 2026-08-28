"""INTENTIONALLY INCORRECT — the single-FIFO baseline the priority queue replaces.

This module exists to be demonstrated *failing*. Its bug is its contract:
``tests/unit/message_broker_patterns/priority_queue_pattern/test_naive.py`` fails
if the bug is repaired.

**The invariant it violates.** ``priority_queue_pattern/README.md``: *"each
priority level gets its own stream and its own pool of consumers… routing is
decided once, at publish time"*, so an urgent ticket *"is never behind a routine
one — it is in a different queue entirely"*.

**What this does instead.** One stream for everything, with ``priority`` carried
as a *field on the message*. This is the design that feels right: the data is
richer, the topology is simpler, and priority is "just a column". But a stream is
a queue, and a queue is ordered by arrival — so the field is never read on the
delivery path and cannot be. A HIGH ticket published after 200 routine ones is
delivered 201st.

That is **priority inversion**, and the README names the two fixes that are not
fixes:

* **Add consumers.** They drain the same single queue in the same order, so the
  urgent ticket's *position in line* never improves.
* **Sort the backlog.** A stream has no reorder operation; a consumer that reads
  a batch and sorts it only reorders the batch it already holds, which is the
  head of the queue — not the ticket still waiting behind 200 others.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from message_broker_patterns.priority_queue_pattern.models import SupportTicket

logger = logging.getLogger(__name__)

# One stream for every priority — the whole design, in one constant. Compare
# ``priority_queue_pattern.broker.STREAMS``, which has one per level.
SINGLE_STREAM = "support:all"

_BUSYGROUP = "BUSYGROUP"

Handler = Callable[[str, SupportTicket], Awaitable[None]]


async def ensure_group(client: aioredis.Redis, group: str) -> None:
    """Create the one consumer group on the one stream. Idempotent."""
    try:
        await client.xgroup_create(SINGLE_STREAM, group, id="0", mkstream=True)
    except ResponseError as exc:
        if _BUSYGROUP not in str(exc):
            raise


async def publish(client: aioredis.Redis, ticket: SupportTicket) -> str:
    """Append a ticket to the single stream, priority and all.

    The priority is written into the message — and then never consulted again.
    Routing that happens after the message is queued is not routing.
    """
    msg_id: bytes | str = await client.xadd(SINGLE_STREAM, ticket.to_fields())
    decoded = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
    logger.debug(
        "Published ticket %s (%s) → %s id=%s",
        ticket.ticket_id,
        ticket.priority.value,
        SINGLE_STREAM,
        decoded,
    )
    return decoded


async def run_fifo_consumer(
    client: aioredis.Redis,
    group: str,
    consumer_id: str,
    handler: Handler,
    *,
    max_polls: int,
    count: int = 10,
    idle_sleep: float = 0.0,
) -> list[SupportTicket]:
    """Drain the single stream in arrival order; return tickets in served order.

    The loop is a correct consumer-group consumer — it reads with ``>``, runs the
    handler, acks. Everything about it is right except that there is only one
    queue, so ``ticket.priority`` never influences when a ticket is served.
    """
    await ensure_group(client, group)
    served: list[SupportTicket] = []
    logger.info("consumer=%s joined the single FIFO queue (group=%s)", consumer_id, group)

    for _poll in range(max_polls):
        results = await client.xreadgroup(
            group, consumer_id, {SINGLE_STREAM: ">"}, count=count, block=None
        )
        if not results:
            await asyncio.sleep(idle_sleep)
            continue
        for msg_id, fields in results[0][1]:
            ticket = SupportTicket.from_fields(fields)
            await handler(consumer_id, ticket)  # ← served in arrival order, full stop.
            await client.xack(SINGLE_STREAM, group, msg_id)
            served.append(ticket)
            logger.info(
                "consumer=%s served ticket=%s (%s) at position %d",
                consumer_id,
                ticket.ticket_id,
                ticket.priority.value,
                len(served),
            )

    logger.info("consumer=%s stopping — served %d ticket(s)", consumer_id, len(served))
    return served
