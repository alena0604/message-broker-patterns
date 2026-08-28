"""The naive inline publish must hit the broker's size ceiling and haul every byte.

Storing the payload and publishing a token instead — i.e. implementing the claim
check — turns these tests red: nothing would be rejected and the broker would
carry a few hundred bytes rather than megabytes.
"""

from __future__ import annotations

import pytest

from message_broker_patterns.claim_check_pattern.models import ClaimCheck, Payload
from message_broker_patterns.claim_check_pattern.naive import (
    MAX_MESSAGE_BYTES,
    InlinePayloadBroker,
    MessageTooLargeError,
    publish_payload_inline,
)


def _payload(size_bytes: int, name: str = "hero-banner.png") -> Payload:
    return Payload(data=b"\x00" * size_bytes, content_type="image/png", original_name=name)


def _claim_for(payload: Payload) -> ClaimCheck:
    """The claim check the real producer would have published for this payload."""
    return ClaimCheck(
        claim_id="0123456789abcdef0123456789abcdef",
        content_type=payload.content_type,
        original_name=payload.original_name,
        size_bytes=payload.size_bytes,
    )


async def test_naive_claim_check_rejects_payload_over_the_broker_size_limit() -> None:
    # Arrange — a 2 MiB upload against Kafka's default 1 MiB ceiling.
    broker = InlinePayloadBroker()
    payload = _payload(2 * MAX_MESSAGE_BYTES, name="product-demo.mp4")

    # Act / Assert — a hard rejection at publish time, not a slow publish.
    with pytest.raises(MessageTooLargeError):
        await publish_payload_inline(broker, payload)

    # The message never made it onto the queue: no consumer will ever see it,
    # and the producer holds bytes it has nowhere to put.
    assert broker.qsize() == 0
    assert broker.bytes_transferred == 0


async def test_naive_claim_check_hauls_the_whole_payload_across_the_broker() -> None:
    # Arrange — three payloads that each squeak under the limit.
    broker = InlinePayloadBroker()
    payloads = [_payload(500_000, name=f"scan-{index}.png") for index in range(3)]

    # Act.
    for payload in payloads:
        await publish_payload_inline(broker, payload)

    # Assert — every byte crossed the broker, versus ~200 bytes per claim check.
    total_payload_bytes = sum(payload.size_bytes for payload in payloads)
    claim_bytes = sum(_claim_for(payload).wire_size_bytes() for payload in payloads)

    assert broker.bytes_transferred >= total_payload_bytes
    assert broker.bytes_transferred > 100 * claim_bytes


async def test_naive_claim_check_delivers_the_payload_when_it_fits() -> None:
    """The baseline is not a strawman — inline publishing works, until it doesn't."""
    # Arrange — a small payload well under the ceiling.
    broker = InlinePayloadBroker()
    payload = _payload(1_024, name="thumbnail.png")

    # Act.
    wire_size = await publish_payload_inline(broker, payload)
    message = await broker.get()

    # Assert — the raw bytes arrive intact, carried by the broker itself.
    assert wire_size > payload.size_bytes  # header + every payload byte
    assert message.endswith(payload.data)
