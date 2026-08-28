"""INTENTIONALLY INCORRECT — the no-consumer-group baseline competing consumers replaces.

This module exists to be demonstrated *failing*. Its bug is its contract:
``tests/unit/message_broker_patterns/competing_consumers_pattern/test_naive.py``
fails if the bug is repaired.

**The invariant it violates.** ``competing_consumers_pattern/README.md``: a
consumer group is *"what turns a single stream into a load balancer: every
consumer reads with ``XREADGROUP … >`` and the broker hands each new message to
exactly one consumer"*, and an unacked message stays pending for a sibling to
reclaim.

**What this does instead.** Each worker does the read that the tutorial shows
first — a bare ``XREAD`` from its own cursor, no group, no ack. It looks like
scaling: start three processes, all attached to the same stream. It is not. A
plain ``XREAD`` is a *broadcast* read, so every worker independently sees every
message and the work is not split at all. Three workers do 3x the work, not a
third each; adding workers adds duplicate side effects, not throughput.

Two properties are missing, and neither is recoverable by tuning:

* **Load balancing** — the application, not the broker, would have to decide who
  takes what. There is no way to express "give this to exactly one of us" with a
  bare ``XREAD``.
* **Redelivery** — with no group there is no pending-entries list. The cursor
  advances on *read*, so a worker that dies mid-task leaves nothing behind for a
  sibling to reclaim; that message is simply never processed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import redis.asyncio as aioredis

from message_broker_patterns.competing_consumers_pattern.models import Task

logger = logging.getLogger(__name__)

Handler = Callable[[str, Task], Awaitable[None]]


async def run_bare_read_consumer(
    client: aioredis.Redis,
    stream: str,
    consumer_id: str,
    handler: Handler,
    stop_event: asyncio.Event,
    *,
    count: int = 10,
    idle_sleep: float = 0.005,
) -> int:
    """Read the whole stream from the start with ``XREAD``; no group, no ack.

    ``last_id`` is process-local state, which is the tell: the *worker* is
    tracking its position rather than the broker tracking the group's. Every
    worker starts at ``"0"`` and walks the entire stream, so N workers each
    handle all M messages — N*M side effects for M units of work.

    Returns the number of messages this worker handled (which will equal the
    stream length, not its share of it).
    """
    last_id = "0"  # ← every worker starts from the beginning. All of them.
    handled = 0
    logger.info("consumer=%s bare-reading stream=%s (no group)", consumer_id, stream)

    while not stop_event.is_set():
        results = await client.xread({stream: last_id}, count=count)
        if not results:
            await asyncio.sleep(idle_sleep)
            continue
        for msg_id, fields in results[0][1]:
            last_id = msg_id.decode()  # ← cursor advances on READ, before the work.
            task = Task.from_fields(fields)
            await handler(consumer_id, task)
            # No ack: there is no group to ack to, so a crash after this point
            # leaves no pending entry for anyone to reclaim.
            handled += 1
            logger.info("consumer=%s handled task=%s msg=%s", consumer_id, task.task_id, last_id)
        await asyncio.sleep(idle_sleep)

    logger.info("consumer=%s stopping — handled %d message(s)", consumer_id, handled)
    return handled
