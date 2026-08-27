import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import fakeredis.aioredis
import pytest
from pytest_mock import MockerFixture

from message_broker_patterns.entities import (
    IdempotencyStore,
    InMemoryIdempotencyStore,
    RedisIdempotencyStore,
)

MODULE = "message_broker_patterns.entities.idempotency"

TTL = timedelta(hours=1)
START = datetime(2026, 8, 26, 9, 30, tzinfo=UTC)
TEST_SET = "test:processed"


class FakeClock:
    """A hand-cranked ``utc_now`` replacement, so TTL tests never sleep."""

    def __init__(self, now: datetime = START) -> None:
        self._now = now

    def __call__(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        self._now += delta


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def memory_store(clock: FakeClock) -> InMemoryIdempotencyStore:
    return InMemoryIdempotencyStore(ttl=TTL, clock=clock)


@pytest.fixture()
def redis_store(fake_redis: fakeredis.aioredis.FakeRedis) -> RedisIdempotencyStore:
    return RedisIdempotencyStore(fake_redis, key=TEST_SET, ttl=TTL)


@pytest.fixture(params=["memory", "redis"])
async def store(
    request: pytest.FixtureRequest,
    fake_redis: fakeredis.aioredis.FakeRedis,
    clock: FakeClock,
) -> AsyncGenerator[IdempotencyStore, None]:
    """Every implementation of the protocol, so the shared contract is tested once."""
    if request.param == "memory":
        yield InMemoryIdempotencyStore(ttl=TTL, clock=clock)
    else:
        yield RedisIdempotencyStore(fake_redis, key=TEST_SET, ttl=TTL)


# --- shared contract -------------------------------------------------------


async def test_unmarked_key_is_not_processed(store: IdempotencyStore) -> None:
    assert await store.is_processed("PAY-001") is False


async def test_marked_key_is_processed(store: IdempotencyStore) -> None:
    await store.mark_processed("PAY-001")

    assert await store.is_processed("PAY-001") is True


async def test_distinct_keys_are_independent(store: IdempotencyStore) -> None:
    await store.mark_processed("PAY-001")

    assert await store.is_processed("PAY-002") is False


async def test_marking_the_same_key_twice_is_idempotent(store: IdempotencyStore) -> None:
    await store.mark_processed("PAY-001")
    await store.mark_processed("PAY-001")

    assert await store.is_processed("PAY-001") is True


async def test_mark_if_new_wins_once_then_loses(store: IdempotencyStore) -> None:
    assert await store.mark_if_new("PAY-001") is True
    assert await store.mark_if_new("PAY-001") is False


async def test_mark_if_new_records_the_key(store: IdempotencyStore) -> None:
    await store.mark_if_new("PAY-001")

    assert await store.is_processed("PAY-001") is True


@pytest.mark.parametrize("key", ["PAY-001", "order/42", "unicode-✉", "", " spaced "])
async def test_any_string_key_round_trips(store: IdempotencyStore, key: str) -> None:
    await store.mark_processed(key)

    assert await store.is_processed(key) is True


async def test_implementations_satisfy_the_protocol(store: IdempotencyStore) -> None:
    assert isinstance(store, IdempotencyStore)


async def test_concurrent_mark_if_new_elects_exactly_one_winner(store: IdempotencyStore) -> None:
    results = await asyncio.gather(*(store.mark_if_new("PAY-001") for _ in range(50)))

    assert sum(results) == 1


async def test_concurrent_marks_of_distinct_keys_all_land(store: IdempotencyStore) -> None:
    keys = [f"PAY-{index:03d}" for index in range(100)]

    await asyncio.gather(*(store.mark_processed(key) for key in keys))

    assert all(await asyncio.gather(*(store.is_processed(key) for key in keys)))


async def test_concurrent_check_and_mark_never_reports_a_missing_key(
    store: IdempotencyStore,
) -> None:
    """A racing reader sees a key as either unprocessed or processed — never corrupt."""
    await store.mark_processed("PAY-001")

    reads = await asyncio.gather(
        *(store.is_processed("PAY-001") for _ in range(25)),
        *(store.mark_processed(f"PAY-{index}") for index in range(25)),
    )

    assert all(read for read in reads[:25])


# --- in-memory: TTL --------------------------------------------------------


async def test_entry_expires_once_the_ttl_elapses(
    memory_store: InMemoryIdempotencyStore, clock: FakeClock
) -> None:
    await memory_store.mark_processed("PAY-001")

    clock.advance(TTL)

    assert await memory_store.is_processed("PAY-001") is False


async def test_entry_is_still_processed_just_before_the_ttl_elapses(
    memory_store: InMemoryIdempotencyStore, clock: FakeClock
) -> None:
    await memory_store.mark_processed("PAY-001")

    clock.advance(TTL - timedelta(seconds=1))

    assert await memory_store.is_processed("PAY-001") is True


async def test_expired_key_can_be_claimed_as_new_again(
    memory_store: InMemoryIdempotencyStore, clock: FakeClock
) -> None:
    await memory_store.mark_if_new("PAY-001")

    clock.advance(TTL)

    assert await memory_store.mark_if_new("PAY-001") is True


async def test_re_marking_a_key_refreshes_its_ttl(
    memory_store: InMemoryIdempotencyStore, clock: FakeClock
) -> None:
    await memory_store.mark_processed("PAY-001")

    clock.advance(TTL - timedelta(minutes=1))
    await memory_store.mark_processed("PAY-001")
    clock.advance(TTL - timedelta(minutes=1))

    assert await memory_store.is_processed("PAY-001") is True


async def test_expired_entries_are_purged_so_the_store_stays_bounded(
    memory_store: InMemoryIdempotencyStore, clock: FakeClock
) -> None:
    for index in range(100):
        await memory_store.mark_processed(f"PAY-{index:03d}")

    clock.advance(TTL)
    await memory_store.mark_processed("PAY-fresh")

    assert memory_store.size() == 1


async def test_size_counts_only_live_entries(
    memory_store: InMemoryIdempotencyStore, clock: FakeClock
) -> None:
    await memory_store.mark_processed("PAY-001")
    clock.advance(TTL)

    assert memory_store.size() == 0


async def test_default_clock_is_the_module_level_utc_now(mocker: MockerFixture) -> None:
    utc_now = mocker.patch(f"{MODULE}.utc_now", return_value=START)
    store = InMemoryIdempotencyStore(ttl=TTL)

    await store.mark_processed("PAY-001")

    assert utc_now.called
    assert await store.is_processed("PAY-001") is True


async def test_default_clock_expires_entries_by_wall_clock(mocker: MockerFixture) -> None:
    utc_now = mocker.patch(f"{MODULE}.utc_now", return_value=START)
    store = InMemoryIdempotencyStore(ttl=TTL)
    await store.mark_processed("PAY-001")

    utc_now.return_value = START + TTL

    assert await store.is_processed("PAY-001") is False


# --- redis-set backed ------------------------------------------------------


async def test_marked_key_becomes_a_member_of_the_redis_set(
    redis_store: RedisIdempotencyStore, fake_redis: fakeredis.aioredis.FakeRedis
) -> None:
    await redis_store.mark_processed("PAY-001")

    assert await fake_redis.smembers(TEST_SET) == {b"PAY-001"}


async def test_ttl_is_applied_to_the_processed_set(
    redis_store: RedisIdempotencyStore, fake_redis: fakeredis.aioredis.FakeRedis
) -> None:
    await redis_store.mark_processed("PAY-001")

    ttl_seconds = await fake_redis.ttl(TEST_SET)

    assert 0 < ttl_seconds <= TTL.total_seconds()


async def test_ttl_is_anchored_on_the_first_write_not_refreshed_by_later_ones(
    redis_store: RedisIdempotencyStore, fake_redis: fakeredis.aioredis.FakeRedis
) -> None:
    """Refreshing the expiry on every write would make the set grow forever."""
    await redis_store.mark_processed("PAY-001")
    await fake_redis.expire(TEST_SET, 10)

    await redis_store.mark_processed("PAY-002")

    assert 0 < await fake_redis.ttl(TEST_SET) <= 10


async def test_expired_set_no_longer_reports_its_keys_as_processed(
    redis_store: RedisIdempotencyStore, fake_redis: fakeredis.aioredis.FakeRedis
) -> None:
    await redis_store.mark_processed("PAY-001")

    await fake_redis.pexpire(TEST_SET, 1)
    await asyncio.sleep(0.05)

    assert await redis_store.is_processed("PAY-001") is False


async def test_stores_on_different_keys_do_not_share_state(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    payments = RedisIdempotencyStore(fake_redis, key="payments:processed", ttl=TTL)
    orders = RedisIdempotencyStore(fake_redis, key="orders:processed", ttl=TTL)

    await payments.mark_processed("ID-1")

    assert await orders.is_processed("ID-1") is False


# --- ttl granularity and validation ----------------------------------------


@pytest.mark.parametrize(
    "ttl",
    [
        timedelta(milliseconds=100),
        timedelta(milliseconds=500),
        timedelta(seconds=0.9),
    ],
)
async def test_sub_second_ttl_still_dedups(
    fake_redis: fakeredis.aioredis.FakeRedis, ttl: timedelta
) -> None:
    """A ttl under a second must still remember keys — not delete the set on write.

    Regression: ``EXPIRE`` truncates a ``timedelta`` to whole seconds, so any
    sub-second ttl became ``EXPIRE key 0``, which deletes the key outright and
    silently re-admitted every duplicate.
    """
    store = RedisIdempotencyStore(fake_redis, key=TEST_SET, ttl=ttl)

    first = await store.mark_if_new("PAY-001")
    second = await store.mark_if_new("PAY-001")
    third = await store.mark_if_new("PAY-001")

    assert (first, second, third) == (True, False, False)
    assert await store.is_processed("PAY-001") is True


async def test_sub_second_ttl_matches_in_memory_semantics(
    fake_redis: fakeredis.aioredis.FakeRedis, clock: FakeClock
) -> None:
    """The two implementations stay interchangeable at sub-second ttls."""
    ttl = timedelta(milliseconds=500)
    redis_store = RedisIdempotencyStore(fake_redis, key=TEST_SET, ttl=ttl)
    memory_store = InMemoryIdempotencyStore(ttl=ttl, clock=clock)

    redis_claims = [await redis_store.mark_if_new("PAY-001") for _ in range(3)]
    memory_claims = [await memory_store.mark_if_new("PAY-001") for _ in range(3)]

    assert redis_claims == memory_claims == [True, False, False]
    assert await redis_store.is_processed("PAY-001") == await memory_store.is_processed("PAY-001")


@pytest.mark.parametrize("ttl", [timedelta(0), timedelta(seconds=-1)])
def test_non_positive_ttl_is_rejected_by_the_in_memory_store(ttl: timedelta) -> None:
    """A non-positive ttl degrades the store into a silent no-op — reject it early."""
    with pytest.raises(ValueError, match="ttl must be positive"):
        InMemoryIdempotencyStore(ttl=ttl)


@pytest.mark.parametrize("ttl", [timedelta(0), timedelta(seconds=-1)])
def test_non_positive_ttl_is_rejected_by_the_redis_store(
    fake_redis: fakeredis.aioredis.FakeRedis, ttl: timedelta
) -> None:
    with pytest.raises(ValueError, match="ttl must be positive"):
        RedisIdempotencyStore(fake_redis, key=TEST_SET, ttl=ttl)
