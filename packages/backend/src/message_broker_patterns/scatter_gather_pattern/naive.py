"""INTENTIONALLY INCORRECT — the sequential fan-in baseline scatter-gather replaces.

This module exists to be demonstrated *failing*. Its bug is its contract:
``tests/unit/message_broker_patterns/scatter_gather_pattern/test_naive.py`` fails
if the bug is repaired.

**The invariant it violates.** ``scatter_gather_pattern/README.md``: *"The timeout
is a hard deadline… a slow/broken recipient can't extend it or block it"*, and a
partial result is a normal outcome. ``ScatterGatherCoordinator.gather`` collects
replies off one shared queue under a single deadline, so total latency is bounded
by the timeout no matter how many recipients there are.

**What this does instead.** The loop everyone writes first::

    for lookup in lookups:
        quotes.append(await lookup(request))

It is *correct* — it returns every quote — and it is what a competent engineer
reaches for before the first incident. Two things are missing and neither is
visible on a healthy day:

* **No concurrency.** Latency is the *sum* of every recipient's latency, so the
  search gets linearly slower with each airline added. Ten 200 ms partners is a
  2 s page.
* **No deadline.** There is no per-call timeout and no overall budget, so one
  hanging recipient blocks the whole search *indefinitely* — and because the
  awaits are sequential, every recipient after it is never even asked.

A raised exception has the same effect: without a try/except one unavailable
airline sinks a search the other four could have answered. Bigger timeouts on the
caller do not fix this — unbounded fan-in has no bounded latency to tune.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence

from message_broker_patterns.scatter_gather_pattern.models import FlightQuote, SearchRequest

logger = logging.getLogger(__name__)

# One airline's async lookup. Async, unlike the real ``QuoteLookup``, because the
# naive design calls the airline's API in-line from the request path instead of
# publishing onto a topic.
AsyncQuoteLookup = Callable[[SearchRequest], Awaitable[FlightQuote]]


async def gather_quotes_sequentially(
    request: SearchRequest,
    lookups: Sequence[AsyncQuoteLookup],
) -> list[FlightQuote]:
    """Ask each airline one at a time; return every quote. No timeout, anywhere.

    Note what is *absent*: no ``asyncio.gather``, no ``asyncio.timeout``, no
    ``try``/``except``. Each ``await`` blocks the next one, so the caller pays
    the sum of all latencies, waits forever on the first hang, and gets nothing
    at all if any single airline raises.
    """
    quotes: list[FlightQuote] = []
    for lookup in lookups:
        quote = await lookup(request)  # ← blocks here until THIS airline answers.
        quotes.append(quote)
        logger.info(
            "naive search %s got %s at %d (%d/%d)",
            request.correlation_id,
            quote.airline,
            quote.price_cents,
            len(quotes),
            len(lookups),
        )
    return quotes
