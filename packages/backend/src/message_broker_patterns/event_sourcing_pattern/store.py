import logging

import redis.asyncio as aioredis

from message_broker_patterns.event_sourcing_pattern.events import account_stream
from message_broker_patterns.metrics import REGISTRY

logger = logging.getLogger(__name__)


class EventStore:
    """Append-only event store backed by one Redis stream per aggregate instance."""

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def append(self, account_id: str, event_type: str, payload: dict[str, str]) -> str:
        stream = account_stream(account_id)
        fields = {"event_type": event_type, **payload}
        msg_id: str = await self._client.xadd(stream, fields)
        REGISTRY.increment("event_sourcing", "events_appended")
        logger.debug("Appended %s to stream %s: id=%s", event_type, stream, msg_id)
        return msg_id

    async def read(
        self, account_id: str, last_id: str = "0"
    ) -> list[tuple[str, dict[bytes, bytes]]]:
        """Read events for one account.

        ``last_id="0"`` reads the full history from genesis; pass a cursor
        (a previously returned message id) to read only new events since then.
        """
        stream = account_stream(account_id)
        results = await self._client.xread({stream: last_id}, count=100)
        if not results:
            return []
        # results: list of [stream_name, [(msg_id, fields), ...]]
        return [(msg_id.decode(), fields) for msg_id, fields in results[0][1]]

    async def length(self, account_id: str) -> int:
        return int(await self._client.xlen(account_stream(account_id)))

    async def close(self) -> None:
        await self._client.aclose()
