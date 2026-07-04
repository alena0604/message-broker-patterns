import asyncio
from pathlib import Path

import pytest

from message_broker_patterns.claim_check_pattern.broker import ClaimCheckBroker
from message_broker_patterns.claim_check_pattern.consumer import run_consumer
from message_broker_patterns.claim_check_pattern.models import ClaimCheck, Payload
from message_broker_patterns.claim_check_pattern.producer import ClaimCheckProducer
from message_broker_patterns.claim_check_pattern.storage import FilesystemPayloadStore


def _setup(tmp_path: Path) -> tuple[ClaimCheckBroker, FilesystemPayloadStore, ClaimCheckProducer]:
    store = FilesystemPayloadStore(tmp_path)
    broker = ClaimCheckBroker()
    producer = ClaimCheckProducer(broker, store)
    return broker, store, producer


async def _drain_then_stop(handled: list[object], expected: int, stop: asyncio.Event) -> None:
    while len(handled) < expected:
        await asyncio.sleep(0.005)
    stop.set()


@pytest.mark.parametrize(
    "data",
    [b"small", b"y" * 50_000, "unicode ☃".encode(), b""],
)
async def test_consumer_resolves_payload_from_claim(tmp_path: Path, data: bytes) -> None:
    broker, _store, producer = _setup(tmp_path)
    payload = Payload(data=data, content_type="text/plain", original_name="f.txt")
    await producer.publish(payload)

    received: list[Payload] = []

    async def handler(consumer_id: str, claim: ClaimCheck, resolved: Payload) -> None:
        received.append(resolved)

    stop = asyncio.Event()
    total, _ = await asyncio.gather(
        run_consumer(broker, _store, handler, stop, poll_timeout=0.01),
        _drain_then_stop(received, 1, stop),
    )

    assert total == 1
    assert received[0].data == data
    assert received[0].original_name == "f.txt"


async def test_delete_after_true_removes_payload_from_storage(tmp_path: Path) -> None:
    broker, store, producer = _setup(tmp_path)
    claim = await producer.publish(
        Payload(data=b"temp", content_type="text/plain", original_name="t.txt")
    )

    seen: list[str] = []

    async def handler(consumer_id: str, c: ClaimCheck, resolved: Payload) -> None:
        seen.append(c.claim_id)

    stop = asyncio.Event()
    await asyncio.gather(
        run_consumer(broker, store, handler, stop, delete_after=True, poll_timeout=0.01),
        _drain_then_stop(seen, 1, stop),
    )

    assert store.exists(claim.claim_id) is False


async def test_delete_after_false_keeps_payload_in_storage(tmp_path: Path) -> None:
    broker, store, producer = _setup(tmp_path)
    claim = await producer.publish(
        Payload(data=b"keep me", content_type="text/plain", original_name="k.txt")
    )

    seen: list[str] = []

    async def handler(consumer_id: str, c: ClaimCheck, resolved: Payload) -> None:
        seen.append(c.claim_id)

    stop = asyncio.Event()
    await asyncio.gather(
        run_consumer(broker, store, handler, stop, delete_after=False, poll_timeout=0.01),
        _drain_then_stop(seen, 1, stop),
    )

    assert store.exists(claim.claim_id) is True
    assert store.get(claim.claim_id) == b"keep me"


async def test_consumer_processes_multiple_payloads_in_order(tmp_path: Path) -> None:
    broker, store, producer = _setup(tmp_path)
    for i in range(3):
        await producer.publish(
            Payload(data=f"payload-{i}".encode(), content_type="text/plain", original_name="p.txt")
        )

    received: list[bytes] = []

    async def handler(consumer_id: str, c: ClaimCheck, resolved: Payload) -> None:
        received.append(resolved.data)

    stop = asyncio.Event()
    total, _ = await asyncio.gather(
        run_consumer(broker, store, handler, stop, poll_timeout=0.01),
        _drain_then_stop(received, 3, stop),
    )

    assert total == 3
    assert received == [b"payload-0", b"payload-1", b"payload-2"]


async def test_consumer_stops_on_stop_event(tmp_path: Path) -> None:
    broker, store, _producer = _setup(tmp_path)
    stop = asyncio.Event()
    stop.set()

    async def handler(consumer_id: str, c: ClaimCheck, resolved: Payload) -> None:
        raise AssertionError("handler must not run when already stopped")

    total = await run_consumer(broker, store, handler, stop, poll_timeout=0.01)

    assert total == 0
