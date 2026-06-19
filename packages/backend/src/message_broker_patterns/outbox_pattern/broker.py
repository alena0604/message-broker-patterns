import logging

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class RedisBroker:
    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def publish(self, stream: str, payload: dict[str, str]) -> str:
        msg_id: str = await self._client.xadd(stream, payload)
        logger.debug("Published to stream %s: id=%s", stream, msg_id)
        return msg_id

    async def close(self) -> None:
        await self._client.aclose()
