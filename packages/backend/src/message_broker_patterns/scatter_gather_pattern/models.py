from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

logger = logging.getLogger(__name__)


class DistributionStrategy(StrEnum):
    """How the scatter component fans a request out to recipients.

    ``RECIPIENT_LIST`` — the scatterer holds explicit knowledge of every
    recipient and addresses each one directly (more control, tighter coupling).
    ``PUBLISH_SUBSCRIBE`` — the scatterer broadcasts to a single topic and any
    subscribed recipient may answer, without the scatterer knowing about it in
    advance (less coupling, less control).
    """

    RECIPIENT_LIST = "recipient_list"
    PUBLISH_SUBSCRIBE = "publish_subscribe"


@dataclass(frozen=True)
class SearchRequest:
    """A flight search broadcast to the airline services.

    ``correlation_id`` is the thread that stitches every response back to this
    request. The aggregator only accepts responses carrying a matching
    correlation id, so two concurrent searches never cross-contaminate.
    """

    correlation_id: str
    origin: str
    destination: str
    departure_date: str
    passengers: int = 1
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class FlightQuote:
    """One airline's answer to a :class:`SearchRequest`.

    Carries the same ``correlation_id`` as the request it answers so the
    aggregator can route it back to the right search. ``price_cents`` is the
    integer fare used by the combining strategies (sort-by-price / cheapest);
    ``depart_at`` is timezone-aware UTC so departure-time filtering is unambiguous.
    """

    correlation_id: str
    airline: str
    flight_number: str
    price_cents: int
    depart_at: datetime
