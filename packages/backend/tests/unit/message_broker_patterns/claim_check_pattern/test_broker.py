from message_broker_patterns.claim_check_pattern.broker import ClaimCheckBroker
from message_broker_patterns.claim_check_pattern.models import ClaimCheck
from message_broker_patterns.metrics import REGISTRY


def _claim(claim_id: str = "c-1", size_bytes: int = 11) -> ClaimCheck:
    return ClaimCheck(
        claim_id=claim_id,
        content_type="text/plain",
        original_name="note.txt",
        size_bytes=size_bytes,
    )


def _counter(key: str) -> int:
    """Read a claim_check counter from the shared registry snapshot (0 if absent).

    The broker hard-codes increments against the global ``REGISTRY``, so tests
    assert on deltas rather than exact totals to stay isolated from other
    tests that touch the same pattern id.
    """
    for entry in REGISTRY.snapshot():
        if entry["id"] == "claim_check":
            return int(entry["counters"].get(key, 0))
    return 0


async def test_publish_then_get_returns_the_same_claim() -> None:
    broker = ClaimCheckBroker()
    claim = _claim()

    await broker.publish(claim)
    received = await broker.get(timeout=0.1)

    assert received is claim


async def test_get_returns_none_on_timeout_when_empty() -> None:
    broker = ClaimCheckBroker()

    received = await broker.get(timeout=0.01)

    assert received is None


async def test_broker_preserves_publish_order() -> None:
    broker = ClaimCheckBroker()
    await broker.publish(_claim("c-1"))
    await broker.publish(_claim("c-2"))

    first = await broker.get(timeout=0.1)
    second = await broker.get(timeout=0.1)

    assert first is not None
    assert second is not None
    assert [first.claim_id, second.claim_id] == ["c-1", "c-2"]


async def test_qsize_and_empty_reflect_queued_claims() -> None:
    broker = ClaimCheckBroker()

    assert broker.empty() is True
    await broker.publish(_claim())
    assert broker.empty() is False
    assert broker.qsize() == 1


async def test_publish_reports_bytes_saved_versus_the_full_payload() -> None:
    broker = ClaimCheckBroker()
    claim = _claim(size_bytes=5_000_000)
    before = _counter("broker_bytes_saved")

    await broker.publish(claim)

    saved = _counter("broker_bytes_saved") - before
    assert saved == claim.size_bytes - claim.wire_size_bytes()
    assert saved > 4_999_000
