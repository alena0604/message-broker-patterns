from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from datetime import UTC, datetime

import pytest

from message_broker_patterns.scatter_gather_pattern.aggregator import ScatterGatherCoordinator
from message_broker_patterns.scatter_gather_pattern.broker import InMemoryTopicBroker
from message_broker_patterns.scatter_gather_pattern.models import (
    DistributionStrategy,
    FlightQuote,
    SearchRequest,
)
from message_broker_patterns.scatter_gather_pattern.service import AirlineService

DEFAULT_DEPART = datetime(2026, 8, 1, 9, 0, tzinfo=UTC)


@pytest.fixture()
def broker() -> InMemoryTopicBroker:
    return InMemoryTopicBroker()


@pytest.fixture()
def make_request() -> Callable[..., SearchRequest]:
    def _make(correlation_id: str = "corr-1", **overrides: object) -> SearchRequest:
        params: dict[str, object] = {
            "correlation_id": correlation_id,
            "origin": "JFK",
            "destination": "LAX",
            "departure_date": "2026-08-01",
        }
        params.update(overrides)
        return SearchRequest(**params)  # type: ignore[arg-type]

    return _make


@pytest.fixture()
def make_quote() -> Callable[..., FlightQuote]:
    def _make(
        correlation_id: str = "corr-1",
        airline: str = "AirAlpha",
        flight_number: str = "AA100",
        price_cents: int = 10_000,
        depart_at: datetime | None = None,
    ) -> FlightQuote:
        return FlightQuote(
            correlation_id=correlation_id,
            airline=airline,
            flight_number=flight_number,
            price_cents=price_cents,
            depart_at=depart_at or DEFAULT_DEPART,
        )

    return _make


@pytest.fixture()
def drive() -> Callable[..., object]:
    """Run a full scatter-gather cycle with services alive as background tasks.

    Starts each service's ``serve`` loop, runs the coordinator's
    ``scatter_gather``, then signals stop and joins the services cleanly (no
    task cancellation, so no unawaited-coroutine warnings under
    ``filterwarnings = ["error"]``).
    """

    async def _drive(
        coordinator: ScatterGatherCoordinator,
        request: SearchRequest,
        strategy: DistributionStrategy,
        services: Iterable[AirlineService],
        expected: int,
        timeout: float,
        *,
        recipients: Iterable[str] | None = None,
    ) -> list[FlightQuote]:
        stop = asyncio.Event()
        tasks = [asyncio.create_task(service.serve(stop)) for service in services]
        try:
            return await coordinator.scatter_gather(
                request, strategy, expected, timeout, recipients=recipients
            )
        finally:
            stop.set()
            await asyncio.gather(*tasks)

    return _drive
