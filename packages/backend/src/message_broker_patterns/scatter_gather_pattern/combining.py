from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime

from message_broker_patterns.scatter_gather_pattern.models import FlightQuote

logger = logging.getLogger(__name__)

# The final "gather" step: fold the collected quotes into the answer the caller
# wants. Kept as a plain callable so new business rules slot in without touching
# the aggregator.
CombineStrategy = Callable[[list[FlightQuote]], list[FlightQuote]]


def sort_by_price(quotes: list[FlightQuote]) -> list[FlightQuote]:
    """Return the quotes ordered cheapest-first (stable on ties)."""
    return sorted(quotes, key=lambda quote: quote.price_cents)


def cheapest(quotes: list[FlightQuote]) -> FlightQuote | None:
    """Return the single lowest-priced quote, or ``None`` if there are none."""
    if not quotes:
        return None
    return min(quotes, key=lambda quote: quote.price_cents)


def filter_departing_after(threshold: datetime) -> CombineStrategy:
    """Build a strategy that keeps only quotes departing at/after ``threshold``.

    Demonstrates that combining is extensible: the aggregator takes any
    :data:`CombineStrategy`, so a new rule is just another function.
    """

    def _combine(quotes: list[FlightQuote]) -> list[FlightQuote]:
        return [quote for quote in quotes if quote.depart_at >= threshold]

    return _combine
