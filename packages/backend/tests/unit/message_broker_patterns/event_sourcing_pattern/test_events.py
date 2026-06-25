import pytest

from message_broker_patterns.event_sourcing_pattern.events import (
    AccountCreated,
    MoneyDeposited,
    MoneyWithdrawn,
    account_stream,
    decode_event,
)


def test_account_stream_is_per_aggregate() -> None:
    assert account_stream("acc-1") == "account:acc-1:events"
    assert account_stream("acc-2") == "account:acc-2:events"


def test_account_created_roundtrip() -> None:
    event = AccountCreated(account_id="acc-1", owner_id="owner-1")
    restored = AccountCreated.from_dict(event.to_dict())
    assert restored == event


def test_money_deposited_roundtrip() -> None:
    event = MoneyDeposited(account_id="acc-1", amount="1000")
    restored = MoneyDeposited.from_dict(event.to_dict())
    assert restored == event


def test_money_withdrawn_roundtrip() -> None:
    event = MoneyWithdrawn(account_id="acc-1", amount="500")
    restored = MoneyWithdrawn.from_dict(event.to_dict())
    assert restored == event


def test_to_dict_values_are_strings() -> None:
    event = MoneyDeposited(account_id="acc-1", amount="1000")
    assert all(isinstance(v, str) for v in event.to_dict().values())


@pytest.mark.parametrize(
    ("event", "expected_type"),
    [
        (AccountCreated(account_id="a", owner_id="o"), AccountCreated),
        (MoneyDeposited(account_id="a", amount="1000"), MoneyDeposited),
        (MoneyWithdrawn(account_id="a", amount="500"), MoneyWithdrawn),
    ],
)
def test_decode_event_dispatches_by_type(
    event: AccountCreated | MoneyDeposited | MoneyWithdrawn,
    expected_type: type,
) -> None:
    raw = {k.encode(): v.encode() for k, v in event.to_dict().items()}
    decoded = decode_event(raw)
    assert isinstance(decoded, expected_type)
    assert decoded == event


def test_decode_event_rejects_unknown_type() -> None:
    raw = {b"event_type": b"NopeEvent", b"account_id": b"a"}
    with pytest.raises(ValueError, match="Unknown event_type"):
        decode_event(raw)
