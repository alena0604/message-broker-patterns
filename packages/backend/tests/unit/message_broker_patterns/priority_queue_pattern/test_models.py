import pytest

from message_broker_patterns.priority_queue_pattern.models import Priority, SupportTicket


def test_to_fields_serializes_to_string_mapping() -> None:
    ticket = SupportTicket(
        ticket_id="T-1",
        subject="Card stolen",
        priority=Priority.HIGH,
        customer_id="cust-A",
    )

    fields = ticket.to_fields()

    assert fields == {
        "ticket_id": "T-1",
        "subject": "Card stolen",
        "priority": "high",
        "customer_id": "cust-A",
    }


@pytest.mark.parametrize("priority", list(Priority))
def test_round_trip_preserves_ticket_for_every_priority(priority: Priority) -> None:
    original = SupportTicket(
        ticket_id="T-9",
        subject="round-trip",
        priority=priority,
        customer_id="cust-Z",
    )

    # to_fields yields str->str; Redis stores it as bytes->bytes on read-back.
    encoded = {k.encode(): v.encode() for k, v in original.to_fields().items()}

    assert SupportTicket.from_fields(encoded) == original


def test_priority_values() -> None:
    assert Priority.HIGH.value == "high"
    assert Priority.NORMAL.value == "normal"
    assert Priority.LOW.value == "low"
