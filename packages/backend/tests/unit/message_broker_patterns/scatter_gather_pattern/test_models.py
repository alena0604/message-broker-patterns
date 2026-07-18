from __future__ import annotations

import dataclasses
from datetime import UTC, datetime

import pytest

from message_broker_patterns.scatter_gather_pattern.models import (
    DistributionStrategy,
    FlightQuote,
    SearchRequest,
)


def test_distribution_strategy_values() -> None:
    assert DistributionStrategy.RECIPIENT_LIST.value == "recipient_list"
    assert DistributionStrategy.PUBLISH_SUBSCRIBE.value == "publish_subscribe"


def test_search_request_defaults_are_sensible() -> None:
    request = SearchRequest(
        correlation_id="corr-1",
        origin="JFK",
        destination="LAX",
        departure_date="2026-08-01",
    )

    assert request.passengers == 1
    assert request.created_at.tzinfo is UTC


def test_search_request_created_at_is_timezone_aware() -> None:
    request = SearchRequest(
        correlation_id="corr-1",
        origin="JFK",
        destination="LAX",
        departure_date="2026-08-01",
    )

    # Timezone-aware datetimes have a non-None utcoffset — naive ones would raise.
    assert request.created_at.utcoffset() is not None


@pytest.mark.parametrize("field_name", ["correlation_id", "airline", "price_cents"])
def test_flight_quote_is_frozen(field_name: str) -> None:
    quote = FlightQuote(
        correlation_id="corr-1",
        airline="AirAlpha",
        flight_number="AA100",
        price_cents=10_000,
        depart_at=datetime(2026, 8, 1, 9, 0, tzinfo=UTC),
    )

    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(quote, field_name, "mutated")
