from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from message_broker_patterns.event_sourcing_pattern.events import (
    AccountCreated,
    AccountEvent,
    MoneyDeposited,
    MoneyWithdrawn,
)


class InsufficientFundsError(Exception):
    """Raised when a withdrawal exceeds the available balance."""


@dataclass
class BankAccount:
    """Write-side aggregate. Pure and synchronous — no Redis, no asyncio.

    Command methods (`create`/`deposit`/`withdraw`) validate invariants and
    *produce* an event; the caller persists it via ``EventStore.append``.
    State is mutated by folding each produced event through ``_apply`` — the
    same fold ``replay`` uses to reconstruct an account from history.
    """

    account_id: str = ""
    owner_id: str = ""
    balance: int = 0
    version: int = 0
    created: bool = False

    def create(self, owner_id: str) -> AccountCreated:
        if self.created:
            raise ValueError("Account already created")
        event = AccountCreated(account_id=self.account_id, owner_id=owner_id)
        self._apply(event)
        return event

    def deposit(self, amount: int) -> MoneyDeposited:
        self._require_created()
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")
        event = MoneyDeposited(account_id=self.account_id, amount=str(amount))
        self._apply(event)
        return event

    def withdraw(self, amount: int) -> MoneyWithdrawn:
        self._require_created()
        if amount <= 0:
            raise ValueError("Withdrawal amount must be positive")
        if amount > self.balance:
            raise InsufficientFundsError(f"Cannot withdraw {amount} from balance {self.balance}")
        event = MoneyWithdrawn(account_id=self.account_id, amount=str(amount))
        self._apply(event)
        return event

    def _require_created(self) -> None:
        if not self.created:
            raise ValueError("Account not created")

    def _apply(self, event: AccountEvent) -> None:
        """Single source of truth for state transitions — reused by replay."""
        if isinstance(event, AccountCreated):
            self.account_id = event.account_id
            self.owner_id = event.owner_id
            self.created = True
        elif isinstance(event, MoneyDeposited):
            self.balance += int(event.amount)
        elif isinstance(event, MoneyWithdrawn):
            self.balance -= int(event.amount)
        self.version += 1

    @classmethod
    def replay(cls, events: Iterable[AccountEvent]) -> BankAccount:
        """Rebuild an account by folding already-decoded domain events."""
        account = cls()
        for event in events:
            account._apply(event)
        return account
