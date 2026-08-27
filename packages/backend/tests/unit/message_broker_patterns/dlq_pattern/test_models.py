import pytest

from message_broker_patterns.dlq_pattern.models import Payment


def test_to_fields_serializes_to_string_mapping() -> None:
    payment = Payment(
        payment_id="PAY-001",
        amount_cents=9900,
        customer_id="cust-A",
        currency="USD",
    )

    fields = payment.to_fields()

    assert fields == {
        "payment_id": "PAY-001",
        "amount_cents": "9900",
        "customer_id": "cust-A",
        "currency": "USD",
    }


@pytest.mark.parametrize("amount_cents", [9900, 0, -1, -99, 12000])
def test_round_trip_preserves_payment(amount_cents: int) -> None:
    original = Payment(
        payment_id="PAY-9",
        amount_cents=amount_cents,
        customer_id="cust-Z",
        currency="EUR",
    )

    # to_fields yields str->str; Redis stores it as bytes->bytes on read-back.
    encoded = {k.encode(): v.encode() for k, v in original.to_fields().items()}

    assert Payment.from_fields(encoded) == original


def test_negative_amount_round_trips() -> None:
    original = Payment("PAY-003", -1, "cust-C", "USD")

    encoded = {k.encode(): v.encode() for k, v in original.to_fields().items()}
    restored = Payment.from_fields(encoded)

    assert restored.amount_cents == -1
    assert restored == original
