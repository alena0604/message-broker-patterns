import logging
from pathlib import Path

import pytest

from message_broker_patterns.claim_check_pattern.broker import ClaimCheckBroker
from message_broker_patterns.claim_check_pattern.models import Payload
from message_broker_patterns.claim_check_pattern.producer import ClaimCheckProducer
from message_broker_patterns.claim_check_pattern.storage import FilesystemPayloadStore


async def test_publish_stores_payload_and_returns_claim(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path)
    broker = ClaimCheckBroker()
    producer = ClaimCheckProducer(broker, store)
    payload = Payload(data=b"a large report", content_type="text/plain", original_name="r.txt")

    claim = await producer.publish(payload)

    # The bytes live in storage, addressable by the claim id.
    assert store.get(claim.claim_id) == b"a large report"


async def test_published_claim_carries_correct_metadata(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path)
    broker = ClaimCheckBroker()
    producer = ClaimCheckProducer(broker, store)
    payload = Payload(data=b"x" * 4096, content_type="application/pdf", original_name="doc.pdf")

    claim = await producer.publish(payload)

    assert claim.size_bytes == 4096
    assert claim.content_type == "application/pdf"
    assert claim.original_name == "doc.pdf"


async def test_publish_puts_only_the_claim_on_the_broker(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path)
    broker = ClaimCheckBroker()
    producer = ClaimCheckProducer(broker, store)
    payload = Payload(data=b"heavy", content_type="text/plain", original_name="h.txt")

    returned = await producer.publish(payload)
    on_broker = await broker.get(timeout=0.1)

    # The broker only ever carries the lightweight claim, never the payload.
    assert on_broker is returned
    assert not hasattr(on_broker, "data")


async def test_publish_logs_the_storage_path(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = FilesystemPayloadStore(tmp_path)
    broker = ClaimCheckBroker()
    producer = ClaimCheckProducer(broker, store)
    payload = Payload(data=b"heavy", content_type="text/plain", original_name="h.txt")

    with caplog.at_level(
        logging.INFO, logger="message_broker_patterns.claim_check_pattern.producer"
    ):
        claim = await producer.publish(payload)

    expected_path = str(store.path_for(claim.claim_id))
    assert any(expected_path in record.message for record in caplog.records)
