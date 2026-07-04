from datetime import UTC

from message_broker_patterns.claim_check_pattern.models import ClaimCheck, Payload


def test_payload_reports_its_size() -> None:
    payload = Payload(data=b"hello world", content_type="text/plain", original_name="note.txt")

    assert payload.size_bytes == len(b"hello world")


def test_claim_check_defaults_created_at_to_utc() -> None:
    claim = ClaimCheck(
        claim_id="abc123",
        content_type="text/plain",
        original_name="note.txt",
        size_bytes=11,
    )

    assert claim.created_at.tzinfo is UTC


def test_claim_check_carries_metadata_not_payload() -> None:
    claim = ClaimCheck(
        claim_id="abc123",
        content_type="application/pdf",
        original_name="report.pdf",
        size_bytes=4096,
    )

    # The claim check is the lightweight reference — it holds metadata only,
    # never the bytes themselves.
    assert not hasattr(claim, "data")
    assert claim.claim_id == "abc123"
    assert claim.size_bytes == 4096


def test_wire_size_is_much_smaller_than_the_payload_it_stands_in_for() -> None:
    claim = ClaimCheck(
        claim_id="abc123",
        content_type="video/mp4",
        original_name="product-demo.mp4",
        size_bytes=5_000_000,
    )

    assert claim.wire_size_bytes() < 200
    assert claim.wire_size_bytes() < claim.size_bytes


def test_wire_size_reflects_metadata_length() -> None:
    short = ClaimCheck(claim_id="a", content_type="t", original_name="a.txt", size_bytes=1)
    long = ClaimCheck(
        claim_id="a-much-longer-claim-identifier",
        content_type="application/vnd.custom-type",
        original_name="a-descriptive-original-file-name.dat",
        size_bytes=1,
    )

    assert long.wire_size_bytes() > short.wire_size_bytes()
