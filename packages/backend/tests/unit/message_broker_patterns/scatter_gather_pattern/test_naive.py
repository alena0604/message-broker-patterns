"""The naive sequential fan-in must be serial, unbounded, and all-or-nothing.

Converting ``gather_quotes_sequentially`` to ``asyncio.gather``, adding a
per-call ``asyncio.timeout``, or wrapping the lookup in ``try``/``except`` turns
each of these tests red — which is the point of keeping them.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from message_broker_patterns.scatter_gather_pattern.models import FlightQuote, SearchRequest
from message_broker_patterns.scatter_gather_pattern.naive import (
    AsyncQuoteLookup,
    gather_quotes_sequentially,
)

LATENCY = 0.02


def _lookup(
    make_quote: Callable[..., FlightQuote],
    airline: str,
    *,
    latency: float = 0.0,
    trace: list[str] | None = None,
) -> AsyncQuoteLookup:
    """An airline whose call optionally sleeps and records its own start/end."""

    async def _call(request: SearchRequest) -> FlightQuote:
        if trace is not None:
            trace.append(f"start:{airline}")
        if latency:
            await asyncio.sleep(latency)
        if trace is not None:
            trace.append(f"end:{airline}")
        return make_quote(correlation_id=request.correlation_id, airline=airline)

    return _call


async def test_naive_scatter_gather_asks_airlines_strictly_one_at_a_time(
    make_request: Callable[..., SearchRequest],
    make_quote: Callable[..., FlightQuote],
) -> None:
    # Arrange — three airlines that each yield to the event loop mid-call, so a
    # concurrent implementation would visibly interleave.
    trace: list[str] = []
    lookups = [
        _lookup(make_quote, name, latency=LATENCY, trace=trace)
        for name in ("Alpha", "Beta", "Gamma")
    ]

    # Act.
    quotes = await gather_quotes_sequentially(make_request(), lookups)

    # Assert — no interleaving at all: each airline ends before the next starts.
    assert len(quotes) == 3
    assert trace == [
        "start:Alpha",
        "end:Alpha",
        "start:Beta",
        "end:Beta",
        "start:Gamma",
        "end:Gamma",
    ]


async def test_naive_scatter_gather_latency_is_the_sum_of_every_airline(
    make_request: Callable[..., SearchRequest],
    make_quote: Callable[..., FlightQuote],
) -> None:
    # Arrange — three airlines, each LATENCY seconds slow.
    lookups = [_lookup(make_quote, name, latency=LATENCY) for name in ("Alpha", "Beta", "Gamma")]
    loop = asyncio.get_running_loop()

    # Act.
    started = loop.time()
    await gather_quotes_sequentially(make_request(), lookups)
    elapsed = loop.time() - started

    # Assert — the search costs 3 * LATENCY, not LATENCY. Fanning out in
    # parallel would finish in roughly one airline's latency and fail here.
    assert elapsed >= 3 * LATENCY


async def test_naive_scatter_gather_hangs_forever_on_one_slow_airline(
    make_request: Callable[..., SearchRequest],
    make_quote: Callable[..., FlightQuote],
) -> None:
    # Arrange — the second airline never answers; the third is healthy.
    never = asyncio.Event()
    asked: list[str] = []

    async def hanging(request: SearchRequest) -> FlightQuote:
        asked.append("Hanging")
        await never.wait()  # a recipient that simply never replies
        raise AssertionError("unreachable")

    async def healthy(request: SearchRequest) -> FlightQuote:
        asked.append("Healthy")
        return make_quote(correlation_id=request.correlation_id, airline="Healthy")

    lookups: list[AsyncQuoteLookup] = [
        _lookup(make_quote, "Alpha"),
        hanging,
        healthy,
    ]

    # Act — only the *caller's* timeout ever fires; the function has none.
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(gather_quotes_sequentially(make_request(), lookups), timeout=0.05)

    # Assert — no partial result was returned, and the healthy airline behind
    # the hang was never even asked.
    assert asked == ["Hanging"]


async def test_naive_scatter_gather_returns_nothing_when_one_airline_raises(
    make_request: Callable[..., SearchRequest],
    make_quote: Callable[..., FlightQuote],
) -> None:
    # Arrange — the middle airline is offline.
    async def offline(request: SearchRequest) -> FlightQuote:
        raise RuntimeError("inventory system offline")

    lookups: list[AsyncQuoteLookup] = [
        _lookup(make_quote, "Alpha"),
        offline,
        _lookup(make_quote, "Gamma"),
    ]

    # Act / Assert — one dead recipient sinks the whole search; the quote Alpha
    # already returned is discarded with the stack unwind.
    with pytest.raises(RuntimeError, match="inventory system offline"):
        await gather_quotes_sequentially(make_request(), lookups)
