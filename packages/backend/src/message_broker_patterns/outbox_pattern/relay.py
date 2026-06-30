import asyncio
import json
import logging
import sqlite3

from message_broker_patterns.metrics import REGISTRY
from message_broker_patterns.outbox_pattern.broker import RedisBroker
from message_broker_patterns.outbox_pattern.store import delete_outbox_entry, poll_outbox

logger = logging.getLogger(__name__)


async def run(
    conn: sqlite3.Connection,
    broker: RedisBroker,
    stream: str,
    stop_event: asyncio.Event,
    poll_interval: float = 1.0,
) -> None:
    logger.info("Relay started — polling outbox every %.1fs", poll_interval)
    while not stop_event.is_set():
        entries = poll_outbox(conn)
        for entry in entries:
            payload = json.loads(entry.payload)
            str_payload = {k: str(v) for k, v in payload.items()}
            await broker.publish(stream, str_payload)
            assert entry.id is not None
            delete_outbox_entry(conn, entry.id)
            REGISTRY.increment("transactional_outbox", "entries_relayed")
            logger.info("Relayed outbox entry %s → stream %s", entry.id, stream)
        await asyncio.sleep(poll_interval)
    logger.info("Relay stopped")
