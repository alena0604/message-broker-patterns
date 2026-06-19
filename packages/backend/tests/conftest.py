import sqlite3
from collections.abc import AsyncGenerator, Generator

import fakeredis.aioredis
import pytest

from message_broker_patterns.outbox_pattern.store import create_tables
from message_broker_patterns.saga_pattern.broker import SagaBroker


@pytest.fixture()
def db_conn() -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    yield conn
    conn.close()


@pytest.fixture()
async def fake_redis() -> AsyncGenerator[fakeredis.aioredis.FakeRedis, None]:
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.aclose()


@pytest.fixture()
async def saga_broker(fake_redis: fakeredis.aioredis.FakeRedis) -> AsyncGenerator[SagaBroker, None]:
    broker = SagaBroker(fake_redis)
    yield broker
    await broker.close()
