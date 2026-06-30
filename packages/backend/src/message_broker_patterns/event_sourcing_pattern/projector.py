from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from message_broker_patterns.event_sourcing_pattern.events import (
    AccountCreated,
    MoneyDeposited,
    MoneyWithdrawn,
    decode_event,
)
from message_broker_patterns.event_sourcing_pattern.store import EventStore
from message_broker_patterns.metrics import REGISTRY

logger = logging.getLogger(__name__)


@dataclass
class AccountSummary:
    """Query-optimized read model built independently from the event stream.

    Deliberately depends only on decoded event types — never on the write-side
    ``BankAccount`` aggregate — to genuinely demonstrate an independent read side.
    """

    account_id: str = ""
    owner_id: str = ""
    balance: int = 0
    version: int = 0
    last_updated: datetime | None = None

    def apply(self, raw: dict[bytes, bytes]) -> None:
        event = decode_event(raw)
        if isinstance(event, AccountCreated):
            self.account_id = event.account_id
            self.owner_id = event.owner_id
        elif isinstance(event, MoneyDeposited):
            self.balance += int(event.amount)
        elif isinstance(event, MoneyWithdrawn):
            self.balance -= int(event.amount)
        self.version += 1
        self.last_updated = datetime.now(UTC)


async def project(
    store: EventStore,
    account_id: str,
    stop_event: asyncio.Event,
    expected_version: int,
    poll_interval: float = 0.05,
) -> AccountSummary:
    """Poll the account's event stream and fold events into an AccountSummary.

    Stops when ``stop_event`` is set or the read model has caught up to
    ``expected_version`` (mirrors the terminal-state check in saga_pattern).
    """
    summary = AccountSummary(account_id=account_id)
    last_id = "0"
    logger.info("Projector started for account %s", account_id)

    while not stop_event.is_set():
        for msg_id, raw in await store.read(account_id, last_id):
            last_id = msg_id
            summary.apply(raw)
            REGISTRY.increment("event_sourcing", "read_model_events_applied")

        if summary.version >= expected_version:
            logger.info(
                "Projector for account %s caught up at version %d",
                account_id,
                summary.version,
            )
            stop_event.set()
            break

        await asyncio.sleep(poll_interval)

    logger.info("Projector stopped for account %s", account_id)
    return summary
