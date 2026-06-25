import pytest

from message_broker_patterns.event_sourcing_pattern.aggregate import (
    BankAccount,
    InsufficientFundsError,
)
from message_broker_patterns.event_sourcing_pattern.events import (
    AccountCreated,
    MoneyDeposited,
    MoneyWithdrawn,
)


def _new_account(account_id: str = "acc-1") -> BankAccount:
    account = BankAccount(account_id=account_id)
    account.create(owner_id="owner-1")
    return account


def test_create_produces_account_created_event() -> None:
    account = BankAccount(account_id="acc-1")
    event = account.create(owner_id="owner-1")
    assert isinstance(event, AccountCreated)
    assert event.account_id == "acc-1"
    assert event.owner_id == "owner-1"
    assert account.created is True
    assert account.version == 1


def test_create_twice_raises() -> None:
    account = _new_account()
    with pytest.raises(ValueError, match="already created"):
        account.create(owner_id="owner-2")


def test_deposit_increases_balance_and_produces_event() -> None:
    account = _new_account()
    event = account.deposit(1000)
    assert isinstance(event, MoneyDeposited)
    assert event.amount == "1000"
    assert account.balance == 1000
    assert account.version == 2


def test_withdraw_decreases_balance_and_produces_event() -> None:
    account = _new_account()
    account.deposit(1000)
    event = account.withdraw(500)
    assert isinstance(event, MoneyWithdrawn)
    assert event.amount == "500"
    assert account.balance == 500
    assert account.version == 3


def test_withdraw_more_than_balance_raises_insufficient_funds() -> None:
    account = _new_account()
    account.deposit(100)
    with pytest.raises(InsufficientFundsError):
        account.withdraw(101)
    # balance and version unchanged after the failed command
    assert account.balance == 100
    assert account.version == 2


@pytest.mark.parametrize("amount", [0, -1, -100])
def test_deposit_rejects_non_positive(amount: int) -> None:
    account = _new_account()
    with pytest.raises(ValueError, match="positive"):
        account.deposit(amount)


@pytest.mark.parametrize("amount", [0, -1, -100])
def test_withdraw_rejects_non_positive(amount: int) -> None:
    account = _new_account()
    account.deposit(1000)
    with pytest.raises(ValueError, match="positive"):
        account.withdraw(amount)


def test_deposit_before_create_raises() -> None:
    account = BankAccount(account_id="acc-1")
    with pytest.raises(ValueError, match="not created"):
        account.deposit(100)


def test_replay_reconstructs_state() -> None:
    events = [
        AccountCreated(account_id="acc-1", owner_id="owner-1"),
        MoneyDeposited(account_id="acc-1", amount="1000"),
        MoneyWithdrawn(account_id="acc-1", amount="500"),
    ]
    account = BankAccount.replay(events)
    assert account.account_id == "acc-1"
    assert account.owner_id == "owner-1"
    assert account.balance == 500
    assert account.version == 3
    assert account.created is True


@pytest.mark.parametrize(
    ("deposits", "withdrawals", "expected_balance"),
    [
        ([1000], [500], 500),
        ([100, 200, 300], [50], 550),
        ([1000], [1000], 0),
        ([10, 20, 30, 40], [], 100),
    ],
)
def test_replay_matches_live_command_application(
    deposits: list[int],
    withdrawals: list[int],
    expected_balance: int,
) -> None:
    # Apply commands live, collecting events as we go.
    live = BankAccount(account_id="acc-1")
    events = [live.create(owner_id="owner-1")]
    for amount in deposits:
        events.append(live.deposit(amount))
    for amount in withdrawals:
        events.append(live.withdraw(amount))

    # Replaying the same events must reproduce identical state.
    replayed = BankAccount.replay(events)
    assert live.balance == expected_balance
    assert replayed.balance == live.balance
    assert replayed.version == live.version
