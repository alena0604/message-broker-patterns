import logging

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class SagaBroker:
    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def publish(self, stream: str, event_type: str, payload: dict[str, str]) -> str:
        fields = {"event_type": event_type, **payload}
        msg_id: str = await self._client.xadd(stream, fields)
        logger.debug("Published %s to stream %s: id=%s", event_type, stream, msg_id)
        return msg_id

    async def consume(
        self, stream: str, last_id: str = "0"
    ) -> list[tuple[str, dict[bytes, bytes]]]:
        results = await self._client.xread({stream: last_id}, count=100)
        if not results:
            return []
        # results: list of [stream_name, [(msg_id, fields), ...]]
        return [(msg_id.decode(), fields) for msg_id, fields in results[0][1]]

    async def close(self) -> None:
        await self._client.aclose()
