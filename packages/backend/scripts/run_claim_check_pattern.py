from message_broker_patterns.logging import init_logger

init_logger()

import asyncio  # noqa: E402
import logging  # noqa: E402
import tempfile  # noqa: E402
from pathlib import Path  # noqa: E402

from message_broker_patterns.claim_check_pattern.broker import ClaimCheckBroker  # noqa: E402
from message_broker_patterns.claim_check_pattern.consumer import run_consumer  # noqa: E402
from message_broker_patterns.claim_check_pattern.models import ClaimCheck, Payload  # noqa: E402
from message_broker_patterns.claim_check_pattern.producer import ClaimCheckProducer  # noqa: E402
from message_broker_patterns.claim_check_pattern.storage import (  # noqa: E402
    FilesystemPayloadStore,
)

logger = logging.getLogger("run_claim_check")

# Three "large" payloads that would blow past a typical broker message-size
# limit. Only their claim checks travel through the broker.
PAYLOADS = [
    Payload(b"A" * 2_000_000, "image/png", "hero-banner.png"),
    Payload(b"B" * 5_000_000, "video/mp4", "product-demo.mp4"),
    Payload(b"C" * 1_000_000, "application/pdf", "annual-report.pdf"),
]


async def handler(consumer_id: str, claim: ClaimCheck, payload: Payload) -> None:
    """Process a resolved payload — here, just report what was fetched back."""
    await asyncio.sleep(0.01)  # simulate work on the large object
    logger.info(
        "[%s] processed %s — %s, %d bytes fetched from storage (claim=%s)",
        consumer_id,
        payload.original_name,
        payload.content_type,
        payload.size_bytes,
        claim.claim_id,
    )


async def main() -> None:
    with tempfile.TemporaryDirectory(prefix="claim-check-") as tmp:
        store = FilesystemPayloadStore(Path(tmp))
        broker = ClaimCheckBroker()
        producer = ClaimCheckProducer(broker, store)

        logger.info("=== Claim Check Demo: Large Media Pipeline ===")
        logger.info("External storage: %s", tmp)

        # --- Producer side: store the heavy payloads, publish only claims -----
        logger.info("--- Producer: checking in %d large payloads ---", len(PAYLOADS))
        claims: list[ClaimCheck] = []
        for payload in PAYLOADS:
            claim = await producer.publish(payload)
            claims.append(claim)
            logger.info(
                "Published claim=%s for %s (%d bytes) — broker carried the claim, not the bytes",
                claim.claim_id,
                claim.original_name,
                claim.size_bytes,
            )

        # --- Consumer side: redeem claims, fetch payloads, then clean up ------
        logger.info("--- Consumer: redeeming claims and reclaiming storage ---")
        processed: list[str] = []

        async def counting_handler(cid: str, claim: ClaimCheck, payload: Payload) -> None:
            await handler(cid, claim, payload)
            processed.append(claim.claim_id)

        stop = asyncio.Event()

        async def _stop_when_drained() -> None:
            while len(processed) < len(PAYLOADS):
                await asyncio.sleep(0.02)
            stop.set()

        total, _ = await asyncio.gather(
            run_consumer(broker, store, counting_handler, stop, delete_after=True, poll_timeout=0.05),
            _stop_when_drained(),
        )

        logger.info("=== Results ===")
        logger.info("Processed %d payload(s)", total)
        remaining = [c.claim_id for c in claims if store.exists(c.claim_id)]
        logger.info(
            "Storage reclaimed: %d/%d payloads deleted after processing (remaining: %s)",
            len(claims) - len(remaining),
            len(claims),
            remaining or "none",
        )
        total_payload_bytes = sum(c.size_bytes for c in claims)
        total_wire_bytes = sum(c.wire_size_bytes() for c in claims)
        total_saved = total_payload_bytes - total_wire_bytes
        logger.info(
            "Broker traffic saved: %d bytes (%.2f MB) — %d bytes of payload data "
            "vs %d bytes of claim checks actually sent over the broker",
            total_saved,
            total_saved / 1_000_000,
            total_payload_bytes,
            total_wire_bytes,
        )
        logger.info(
            "Key insight: the broker only ever carried tiny claim checks; the "
            "multi-megabyte payloads lived in external storage the whole time."
        )


if __name__ == "__main__":
    asyncio.run(main())
