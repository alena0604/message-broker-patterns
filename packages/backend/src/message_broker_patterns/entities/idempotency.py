"""Dedup stores — the idempotency guard shared by every pattern.

A consumer that can be redelivered a message (Redis Streams redelivers anything
never acked; an in-process broker can be handed the same request twice) must be
able to answer "have I already handled this?" before it runs a side effect. That
question is the whole of the :class:`IdempotencyStore` contract.

Two implementations ship: :class:`InMemoryIdempotencyStore` for the stdlib
patterns that own no broker, and :class:`RedisIdempotencyStore` for the
broker-backed ones (a Redis Set, mirroring ``SADD`` / ``SISMEMBER``). Both are
async so a consumer can swap one for the other without changing a call site.

**Retention is bounded.** Every entry carries a TTL — a processed-key set that
only ever grows is a leak, not a design. The TTL is an *upper* bound on how long
a key is remembered, not a floor: see each implementation for the granularity it
expires at.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Protocol, runtime_checkable

import redis.asyncio as aioredis

from message_broker_patterns.entities.envelope import utc_now

logger = logging.getLogger(__name__)

# Long enough that a demo (or a redelivery storm) never outlives it, short
# enough that the set is not a permanent record. Callers pick their own.
DEFAULT_TTL = timedelta(hours=1)

# Default Redis Set holding processed keys. Give each pattern its own key so two
# pipelines never dedup each other's ids.
DEFAULT_PROCESSED_SET = "idempotency:processed"


def _validate_ttl(ttl: timedelta) -> timedelta:
    """Reject a ttl that would make the store a no-op instead of a dedup guard.

    A zero or negative ttl expires every entry the instant it is written, so
    ``mark_if_new`` would return ``True`` forever and duplicates would run their
    side effects — silently. Fail at construction, where the caller can see it.
    """
    if ttl <= timedelta(0):
        raise ValueError(f"ttl must be positive, got {ttl!r}")
    return ttl


@runtime_checkable
class IdempotencyStore(Protocol):
    """The dedup contract a consumer codes against.

    ``is_processed`` / ``mark_processed`` mirror the check-then-record shape the
    DLQ consumer already uses. ``mark_if_new`` collapses the two into one atomic
    step, which is what a consumer racing another consumer for the same message
    actually needs — a separate check and mark can both observe "not processed"
    and both run the side effect.
    """

    async def is_processed(self, key: str) -> bool:
        """Return whether ``key`` was already recorded and has not expired."""
        ...

    async def mark_processed(self, key: str) -> None:
        """Record ``key`` as processed."""
        ...

    async def mark_if_new(self, key: str) -> bool:
        """Record ``key`` and return whether *this* caller was the one to record it.

        ``True`` means the caller holds the claim and should run the side
        effect; ``False`` means someone already did.
        """
        ...


class InMemoryIdempotencyStore:
    """Process-local dedup store for the stdlib/asyncio patterns.

    Keys map to their expiry instant, so entries expire *individually* and
    exactly at ``marked_at + ttl``; re-marking a key restarts its TTL. Expired
    entries are dropped lazily — on read for the key being read, and in a full
    sweep on write — so the dict cannot grow past the number of keys seen within
    one TTL window.

    Concurrency here means asyncio tasks on one event loop, not OS threads: the
    patterns in this repo are all `async`. An :class:`asyncio.Lock` guards the
    compound read-modify-write in :meth:`mark_if_new`, so two tasks racing for
    the same key cannot both win the claim.

    The clock is injected (defaulting to the module-level :func:`utc_now`) so
    TTL behaviour is testable without sleeping.
    """

    def __init__(
        self,
        ttl: timedelta = DEFAULT_TTL,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._ttl = _validate_ttl(ttl)
        self._clock = clock
        self._expiry: dict[str, datetime] = {}
        self._lock = asyncio.Lock()

    def _now(self) -> datetime:
        """Current instant — the injected clock, else the patchable module default."""
        return self._clock() if self._clock is not None else utc_now()

    async def is_processed(self, key: str) -> bool:
        async with self._lock:
            return self._is_live(key, self._now())

    async def mark_processed(self, key: str) -> None:
        async with self._lock:
            self._record(key, self._now())

    async def mark_if_new(self, key: str) -> bool:
        async with self._lock:
            now = self._now()
            if self._is_live(key, now):
                return False
            self._record(key, now)
            return True

    def size(self) -> int:
        """Number of keys still within their TTL (expired entries excluded)."""
        now = self._now()
        return sum(1 for expires_at in self._expiry.values() if expires_at > now)

    def _is_live(self, key: str, now: datetime) -> bool:
        expires_at = self._expiry.get(key)
        if expires_at is None:
            return False
        if expires_at > now:
            return True
        del self._expiry[key]
        logger.debug("Idempotency key %s expired", key)
        return False

    def _record(self, key: str, now: datetime) -> None:
        self._purge_expired(now)
        self._expiry[key] = now + self._ttl

    def _purge_expired(self, now: datetime) -> None:
        expired = [key for key, expires_at in self._expiry.items() if expires_at <= now]
        for key in expired:
            del self._expiry[key]
        if expired:
            logger.debug("Purged %d expired idempotency key(s)", len(expired))


class RedisIdempotencyStore:
    """Dedup store backed by one Redis Set, shared across processes.

    ``SADD`` / ``SISMEMBER`` is the encoding the DLQ pattern already uses, and
    ``SADD`` returning the number of members actually added is what makes
    :meth:`mark_if_new` atomic across competing consumers without a Lua script.

    Expiry is applied to the *set key*, once, on the write that creates it
    (``PEXPIRE ... NX``). Redis expires keys, not individual set members, so the
    whole window is dropped together and the next write starts a fresh one.
    Refreshing the TTL on every write instead would keep the key alive forever
    under steady traffic — exactly the unbounded set this TTL exists to prevent.
    The trade-off: a key written near the end of a window is remembered for less
    than ``ttl``, so pick a ``ttl`` comfortably longer than the redelivery
    window you need to survive.

    Expiry is set in *milliseconds* (``PEXPIRE``, not ``EXPIRE``): redis-py
    truncates a ``timedelta`` to whole seconds for ``EXPIRE``, which turns any
    sub-second ``ttl`` into ``EXPIRE key 0`` — an immediate delete of the set the
    write just created, silently disabling dedup. A non-positive ``ttl`` is
    rejected at construction for the same reason.

    Because the ``NX`` flag only sets an expiry on a key that has none, two
    stores sharing one ``key`` name share one window: the first writer's ``ttl``
    governs it and the other's is ignored, in either direction. Intentional —
    give each pattern its own ``key`` and it cannot arise.

    The caller owns the client's lifecycle; this store never closes it.
    """

    def __init__(
        self,
        client: aioredis.Redis,
        *,
        key: str = DEFAULT_PROCESSED_SET,
        ttl: timedelta = DEFAULT_TTL,
    ) -> None:
        self._client = client
        self._key = key
        self._ttl = _validate_ttl(ttl)

    async def is_processed(self, key: str) -> bool:
        member: int = await self._client.sismember(self._key, key)
        return bool(member)

    async def mark_processed(self, key: str) -> None:
        await self.mark_if_new(key)

    async def mark_if_new(self, key: str) -> bool:
        added: int = await self._client.sadd(self._key, key)
        await self._client.pexpire(self._key, self._ttl, nx=True)
        return bool(added)
