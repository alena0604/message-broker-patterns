from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from message_broker_patterns.scatter_gather_pattern.combining import (
    cheapest,
    filter_departing_after,
    sort_by_price,
)
from message_broker_patterns.scatter_gather_pattern.models import FlightQuote


def test_sort_by_price_orders_cheapest_first(make_quote: Callable[..., FlightQuote]) -> None:
    quotes = [
        make_quote(airline="C", price_cents=30_000),
        make_quote(airline="A", price_cents=10_000),
        make_quote(airline="B", price_cents=20_000),
    ]

    ordered = sort_by_price(quotes)

    assert [q.airline for q in ordered] == ["A", "B", "C"]


def test_sort_by_price_is_stable_on_ties(make_quote: Callable[..., FlightQuote]) -> None:
    quotes = [
        make_quote(airline="first", price_cents=10_000),
        make_quote(airline="second", price_cents=10_000),
    ]

    ordered = sort_by_price(quotes)

    assert [q.airline for q in ordered] == ["first", "second"]


def test_cheapest_returns_lowest_price(make_quote: Callable[..., FlightQuote]) -> None:
    quotes = [
        make_quote(airline="A", price_cents=30_000),
        make_quote(airline="B", price_cents=10_000),
        make_quote(airline="C", price_cents=20_000),
    ]

    best = cheapest(quotes)

    assert best is not None
    assert best.airline == "B"


def test_cheapest_of_empty_is_none() -> None:
    assert cheapest([]) is None


def test_filter_departing_after_keeps_only_later_flights(
    make_quote: Callable[..., FlightQuote],
) -> None:
    early = make_quote(airline="early", depart_at=datetime(2026, 8, 1, 6, 0, tzinfo=UTC))
    late = make_quote(airline="late", depart_at=datetime(2026, 8, 1, 12, 0, tzinfo=UTC))
    threshold = datetime(2026, 8, 1, 9, 0, tzinfo=UTC)

    kept = filter_departing_after(threshold)([early, late])

    assert [q.airline for q in kept] == ["late"]
