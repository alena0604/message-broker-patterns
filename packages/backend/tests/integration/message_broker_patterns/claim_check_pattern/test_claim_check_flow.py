from __future__ import annotations

import asyncio
from pathlib import Path

from message_broker_patterns.claim_check_pattern.broker import ClaimCheckBroker
from message_broker_patterns.claim_check_pattern.consumer import run_consumer
from message_broker_patterns.claim_check_pattern.models import ClaimCheck, Payload
from message_broker_patterns.claim_check_pattern.producer import ClaimCheckProducer
from message_broker_patterns.claim_check_pattern.storage import FilesystemPayloadStore

KIB = 1024
MIB = 1024 * KIB

# A claim check is metadata only, so what crosses the broker stays inside this
# bound no matter how large the payload behind it is.
MAX_CLAIM_WIRE_BYTES = 512


def _bytes_of(size: int) -> bytes:
    """Deterministic, non-uniform filler so a truncated round-trip is detectable."""
    block = bytes(range(256))
    return (block * (size // len(block) + 1))[:size]


async def _drain(
    broker: ClaimCheckBroker,
    store: FilesystemPayloadStore,
    expected: int,
    *,
    delete_after: bool = True,
) -> list[tuple[ClaimCheck, Payload]]:
    """Run the real consumer loop until ``expected`` claims have been redeemed."""
    redeemed: list[tuple[ClaimCheck, Payload]] = []

    async def handler(consumer_id: str, claim: ClaimCheck, payload: Payload) -> None:
        redeemed.append((claim, payload))

    stop = asyncio.Event()

    async def _stop_when_redeemed() -> None:
        while len(redeemed) < expected:
            await asyncio.sleep(0.01)
        stop.set()

    handled, _ = await asyncio.gather(
        run_consumer(broker, store, handler, stop, delete_after=delete_after, poll_timeout=0.01),
        _stop_when_redeemed(),
    )
    assert handled == expected
    return redeemed


async def test_large_payload_round_trips_through_the_claim_check(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path / "payloads")
    broker = ClaimCheckBroker()
    producer = ClaimCheckProducer(broker, store)
    data = _bytes_of(MIB)
    payload = Payload(data=data, content_type="video/mp4", original_name="keynote.mp4")

    claim = await producer.publish(payload)

    # --- only the claim is on the broker; the megabyte went to storage ---
    assert broker.qsize() == 1
    assert claim.size_bytes == MIB
    assert claim.wire_size_bytes() < MAX_CLAIM_WIRE_BYTES
    assert claim.wire_size_bytes() * 1000 < payload.size_bytes
    assert not any(isinstance(value, bytes | bytearray) for value in vars(claim).values())
    stored_path = store.path_for(claim.claim_id)
    assert stored_path.stat().st_size == MIB

    # --- the consumer redeems the claim and gets the whole payload back ---
    redeemed = await _drain(broker, store, expected=1)
    ((received_claim, received_payload),) = redeemed

    assert received_claim == claim
    assert received_payload.data == data
    assert received_payload.size_bytes == MIB
    assert received_payload.content_type == "video/mp4"
    assert received_payload.original_name == "keynote.mp4"

    # Staging storage is reclaimed once the payload has been consumed.
    assert broker.empty() is True
    assert store.exists(claim.claim_id) is False
    assert stored_path.exists() is False


async def test_broker_traffic_stays_flat_as_payload_size_grows(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path / "payloads")
    broker = ClaimCheckBroker()
    producer = ClaimCheckProducer(broker, store)
    # Same-length names, 256x spread in payload size.
    sizes = {"tiny.bin": 4 * KIB, "midi.bin": 256 * KIB, "huge.bin": MIB}
    payloads = {
        name: Payload(
            data=_bytes_of(size), content_type="application/octet-stream", original_name=name
        )
        for name, size in sizes.items()
    }

    claims = [await producer.publish(payload) for payload in payloads.values()]

    wire_sizes = [claim.wire_size_bytes() for claim in claims]
    assert all(size < MAX_CLAIM_WIRE_BYTES for size in wire_sizes)
    # Broker cost is O(1) in payload size: 256x more payload, near-identical wire bytes.
    assert max(wire_sizes) - min(wire_sizes) <= 16
    assert sum(wire_sizes) < min(sizes.values())

    redeemed = await _drain(broker, store, expected=len(payloads))

    # Every payload comes back byte-exact, in publish order, and storage is empty.
    assert [claim.original_name for claim, _ in redeemed] == list(sizes)
    for claim, payload in redeemed:
        assert payload.data == payloads[claim.original_name].data
        assert payload.size_bytes == sizes[claim.original_name]
        assert store.exists(claim.claim_id) is False
    assert list((tmp_path / "payloads").iterdir()) == []
