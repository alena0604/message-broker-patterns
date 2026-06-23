from __future__ import annotations

from dataclasses import dataclass


def account_stream(account_id: str) -> str:
    """Stream key for a single aggregate instance — one stream per account.

    Unlike saga/outbox which use a shared stream per pattern, event sourcing
    uses one stream per aggregate so each account's history is independently
    replayable. See docs/adr/0003-one-stream-per-aggregate-for-event-sourcing.md.
    """
    return f"account:{account_id}:events"


@dataclass(frozen=True)
class AccountCreated:
    event_type: str = "AccountCreated"
    account_id: str = ""
    owner_id: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "event_type": self.event_type,
            "account_id": self.account_id,
            "owner_id": self.owner_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> AccountCreated:
        return cls(
            account_id=data["account_id"],
            owner_id=data["owner_id"],
        )


@dataclass(frozen=True)
class MoneyDeposited:
    event_type: str = "MoneyDeposited"
    account_id: str = ""
    amount: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "event_type": self.event_type,
            "account_id": self.account_id,
            "amount": self.amount,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> MoneyDeposited:
        return cls(
            account_id=data["account_id"],
            amount=data["amount"],
        )


@dataclass(frozen=True)
class MoneyWithdrawn:
    event_type: str = "MoneyWithdrawn"
    account_id: str = ""
    amount: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "event_type": self.event_type,
            "account_id": self.account_id,
            "amount": self.amount,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> MoneyWithdrawn:
        return cls(
            account_id=data["account_id"],
            amount=data["amount"],
        )


AccountEvent = AccountCreated | MoneyDeposited | MoneyWithdrawn

_EVENT_CLASSES: dict[str, type[AccountCreated] | type[MoneyDeposited] | type[MoneyWithdrawn]] = {
    "AccountCreated": AccountCreated,
    "MoneyDeposited": MoneyDeposited,
    "MoneyWithdrawn": MoneyWithdrawn,
}


def decode_event(raw: dict[bytes, bytes]) -> AccountEvent:
    """Dispatch a raw Redis stream entry to its domain event class by event_type.

    `raw` has the shape produced by Redis (`dict[bytes, bytes]`), matching what
    `SagaBroker.consume` / `EventStore.read` return. Both the aggregate replay
    path and the projector decode events through this single helper.
    """
    data = {k.decode(): v.decode() for k, v in raw.items()}
    event_type = data.get("event_type", "")
    try:
        event_cls = _EVENT_CLASSES[event_type]
    except KeyError as exc:
        raise ValueError(f"Unknown event_type: {event_type!r}") from exc
    return event_cls.from_dict(data)
