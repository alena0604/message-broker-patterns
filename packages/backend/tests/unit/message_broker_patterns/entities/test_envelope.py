from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest
from pytest_mock import MockerFixture

from message_broker_patterns.entities import Envelope, new_message_id

MODULE = "message_broker_patterns.entities.envelope"


def _redis_read_back(fields: dict[str, str]) -> dict[bytes, bytes]:
    """Mimic Redis: a str->str XADD mapping comes back out as bytes->bytes."""
    return {k.encode(): v.encode() for k, v in fields.items()}


def test_to_fields_flattens_headers_and_payload() -> None:
    envelope = Envelope(
        message_id="msg-1",
        correlation_id="corr-1",
        causation_id="cause-1",
        occurred_at=datetime(2026, 8, 26, 9, 30, tzinfo=UTC),
        payload={"payment_id": "PAY-001", "amount_cents": "9900"},
    )

    fields = envelope.to_fields()

    assert fields == {
        "message_id": "msg-1",
        "correlation_id": "corr-1",
        "causation_id": "cause-1",
        "occurred_at": "2026-08-26T09:30:00+00:00",
        "payment_id": "PAY-001",
        "amount_cents": "9900",
    }


def test_to_fields_values_are_all_strings() -> None:
    envelope = Envelope(payload={"amount_cents": "9900"})

    fields = envelope.to_fields()

    assert all(isinstance(k, str) and isinstance(v, str) for k, v in fields.items())


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"payment_id": "PAY-001"},
        {"payment_id": "PAY-002", "amount_cents": "-1", "currency": "EUR"},
        {"subject": "unicode ✉ payload", "note": ""},
    ],
)
def test_round_trip_recovers_the_original_envelope(payload: dict[str, str]) -> None:
    original = Envelope(
        message_id="msg-7",
        correlation_id="corr-7",
        causation_id="cause-7",
        occurred_at=datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
        payload=payload,
    )

    restored = Envelope.from_fields(_redis_read_back(original.to_fields()))

    assert restored == original


def test_round_trip_accepts_str_keyed_fields() -> None:
    original = Envelope(payload={"payment_id": "PAY-003"})

    restored = Envelope.from_fields(original.to_fields())

    assert restored == original


def test_from_fields_recovers_payload_without_header_keys() -> None:
    original = Envelope(payload={"payment_id": "PAY-004", "currency": "USD"})

    restored = Envelope.from_fields(_redis_read_back(original.to_fields()))

    assert restored.payload == {"payment_id": "PAY-004", "currency": "USD"}


def test_from_fields_rejects_missing_header() -> None:
    fields = Envelope(payload={"payment_id": "PAY-005"}).to_fields()
    del fields["correlation_id"]

    with pytest.raises(KeyError):
        Envelope.from_fields(_redis_read_back(fields))


@pytest.mark.parametrize(
    "reserved_key",
    ["message_id", "correlation_id", "causation_id", "occurred_at"],
)
def test_payload_key_colliding_with_a_header_is_rejected(reserved_key: str) -> None:
    with pytest.raises(ValueError, match=reserved_key):
        Envelope(payload={reserved_key: "shadowed"})


def test_naive_occurred_at_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Envelope(occurred_at=datetime(2026, 8, 26, 9, 30), payload={})


def test_naive_occurred_at_from_fields_is_rejected() -> None:
    fields = Envelope(payload={}).to_fields()
    fields["occurred_at"] = "2026-08-26T09:30:00"

    with pytest.raises(ValueError, match="timezone-aware"):
        Envelope.from_fields(_redis_read_back(fields))


def test_non_utc_offset_is_normalized_to_utc() -> None:
    kyiv = timezone(timedelta(hours=3))

    envelope = Envelope(occurred_at=datetime(2026, 8, 26, 12, 30, tzinfo=kyiv), payload={})

    assert envelope.occurred_at == datetime(2026, 8, 26, 9, 30, tzinfo=UTC)
    assert envelope.occurred_at.tzinfo is UTC
    assert envelope.to_fields()["occurred_at"] == "2026-08-26T09:30:00+00:00"


def test_message_id_is_generated_deterministically_under_mock(mocker: MockerFixture) -> None:
    mocker.patch(f"{MODULE}.new_message_id", return_value="deadbeef")
    mocker.patch(f"{MODULE}.utc_now", return_value=datetime(2026, 8, 26, 9, 30, tzinfo=UTC))

    envelope = Envelope(payload={"payment_id": "PAY-006"})

    assert envelope.message_id == "deadbeef"
    assert envelope.occurred_at == datetime(2026, 8, 26, 9, 30, tzinfo=UTC)
    assert envelope.to_fields() == {
        "message_id": "deadbeef",
        "correlation_id": "deadbeef",
        "causation_id": "",
        "occurred_at": "2026-08-26T09:30:00+00:00",
        "payment_id": "PAY-006",
    }


def test_new_message_id_is_a_hex_uuid(mocker: MockerFixture) -> None:
    mocker.patch(f"{MODULE}.uuid4", return_value=UUID(int=0xABC))

    assert new_message_id() == "00000000000000000000000000000abc"


def test_correlation_id_defaults_to_the_message_id() -> None:
    envelope = Envelope(message_id="msg-8", payload={})

    assert envelope.correlation_id == "msg-8"


def test_explicit_correlation_id_is_preserved() -> None:
    envelope = Envelope(message_id="msg-9", correlation_id="corr-root", payload={})

    assert envelope.correlation_id == "corr-root"


def test_causation_id_defaults_to_empty_for_a_flow_root() -> None:
    envelope = Envelope(payload={})

    assert envelope.causation_id == ""


def test_envelope_is_frozen() -> None:
    envelope = Envelope(payload={"payment_id": "PAY-007"})

    with pytest.raises(AttributeError):
        envelope.message_id = "tampered"  # type: ignore[misc]


def test_payload_is_copied_so_the_caller_cannot_mutate_the_envelope() -> None:
    payload = {"payment_id": "PAY-008"}
    envelope = Envelope(payload=payload)

    payload["payment_id"] = "PAY-999"

    assert envelope.payload == {"payment_id": "PAY-008"}


def test_two_envelopes_built_with_the_same_inputs_are_equal() -> None:
    occurred_at = datetime(2026, 8, 26, 9, 30, tzinfo=UTC)
    kwargs = {
        "message_id": "msg-10",
        "correlation_id": "corr-10",
        "causation_id": "cause-10",
        "occurred_at": occurred_at,
        "payload": {"payment_id": "PAY-010"},
    }

    assert Envelope(**kwargs) == Envelope(**kwargs)  # type: ignore[arg-type]
