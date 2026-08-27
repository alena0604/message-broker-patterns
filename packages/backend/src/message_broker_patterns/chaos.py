"""Failure injection — the four faults every messaging pattern must survive.

`scripts/run_scatter_gather_pattern.py` hardcodes two of them: ``GhostAir``,
whose lookup always raises, and ``SlowAir``, whose ``latency=0.5`` guarantees it
misses the aggregator's 200 ms deadline. Both are one-off code inside one demo.
This module promotes the idea into wrappers any pattern's demo or test can put
around any async callable:

- :func:`broker_unavailable` — the broker is down; the call never lands.
- :func:`duplicate_delivery` — at-least-once redelivery; the call lands twice.
- :func:`crash_before_ack` — the work is done, then the consumer dies before
  acking; the broker will redeliver.
- :func:`slow_consumer` — the call takes long enough to blow a caller's timeout.

Each is a *wrapper*, not a subclass or a broker feature: it takes an async
callable and returns an async callable with the same signature, so it drops in
at a call site — ``handler = crash_before_ack(handler, on_call=1)`` — without any
pattern module knowing it exists. That is why they compose::

    handler = slow_consumer(crash_before_ack(handler, on_call=1), latency=0.5,
                            probability=0.25)

**Deterministic by default.** A fault fires either on a fixed call ordinal
(``on_call=2`` fires on exactly the 2nd call) or with a probability drawn from a
seeded :class:`random.Random` (:data:`DEFAULT_SEED` unless told otherwise). Two
runs of the same configuration therefore inject the same faults in the same
order — a demo's annotated output stays quotable, and a test asserting on it
does not flake. Pass ``seed=`` for a different-but-still-reproducible run, or
share one ``rng=`` across several injectors to draw them from one stream.

A consequence worth knowing: two injectors left on the default seed draw the
*same* numbers, so at equal probabilities they fire on the same calls. Give them
distinct ``seed=`` values, or one shared ``rng=``, when the faults are meant to
be independent.

Injected faults are logged at WARNING so a demo's output shows the exact line
where the failure was introduced.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

# The seed every injector uses unless the caller picks one. A fixed default is
# the point: "deterministic by default" means a reader who copies a demo command
# sees the output the post quoted.
DEFAULT_SEED = 1729


def new_rng(seed: int = DEFAULT_SEED) -> random.Random:
    """Build the seeded RNG an injector draws from.

    Indirected through a module-level function (rather than constructing
    :class:`random.Random` inline) so a test can patch it and pin the draws —
    the same convention as ``new_message_id`` / ``utc_now`` in
    :mod:`message_broker_patterns.entities.envelope`.
    """
    return random.Random(seed)


async def sleep(seconds: float) -> None:
    """Await real time. Patchable, so a latency test never has to wait."""
    await asyncio.sleep(seconds)


class ChaosError(Exception):
    """Base for every fault this module injects.

    Catching :class:`ChaosError` tells a caller the failure was deliberate. The
    concrete errors also inherit the builtin a real failure would raise, so
    ordinary handling paths — ``except ConnectionError`` — still catch them.
    """


class BrokerUnavailableError(ChaosError, ConnectionError):
    """The broker is unreachable. A builtin :class:`ConnectionError`, deliberately.

    Redis-py's own ``ConnectionError`` is not used: patterns in this repo that
    own no broker (scatter-gather runs on :class:`asyncio.Queue`) must be able to
    inject this fault without importing a Redis client.
    """


class ConsumerCrashError(ChaosError, RuntimeError):
    """The consumer died after doing the work but before acking."""


class FirePolicy:
    """When a fault fires: on a fixed call ordinal, or with a probability.

    Exactly one of the two must be configured. Silently defaulting to "never"
    would make a misconfigured injector a no-op that still looks armed — the
    worst outcome for a module whose whole job is to make failures visible.
    """

    def __init__(self, probability: float | None, on_call: int | None) -> None:
        if probability is not None and on_call is not None:
            raise ValueError("configure probability or on_call, not both")
        if probability is None and on_call is None:
            raise ValueError("configure a trigger: probability=<0.0-1.0> or on_call=<n>")
        if probability is not None and not 0.0 <= probability <= 1.0:
            raise ValueError(f"probability must be between 0.0 and 1.0, got {probability!r}")
        if on_call is not None and on_call < 1:
            raise ValueError(f"on_call must be a call ordinal >= 1, got {on_call!r}")
        self.probability = probability
        self.on_call = on_call

    def should_fire(self, call_ordinal: int, rng: random.Random) -> bool:
        """Decide whether the ``call_ordinal``-th call (1-based) fires a fault.

        The probability branch draws unconditionally — no shortcut for 0.0 or
        1.0 — so one RNG stream advances at the same rate whatever the
        probability is. ``rng.random()`` returns ``[0.0, 1.0)``, which makes
        ``probability=1.0`` always fire and ``0.0`` never fire, exactly.
        """
        if self.probability is not None:
            return rng.random() < self.probability
        return call_ordinal == self.on_call

    def describe(self) -> str:
        if self.on_call is not None:
            return f"on_call={self.on_call}"
        return f"probability={self.probability}"


class ChaosInjector[T](ABC):
    """An async callable that wraps another and sometimes injects a fault.

    Subclasses implement :meth:`_fire` — what the fault actually does. Anything
    the policy does not fire is passed straight through: same arguments, same
    result, same exception. ``calls`` and ``fires`` are public so a demo can
    print "injected 3 faults in 40 deliveries" and a test can assert on it.

    The wrapper is a class rather than a closure so that state (the call count
    and the RNG) is inspectable, and so a call site reads as one substitution:
    ``handler = crash_before_ack(handler, on_call=1)``.
    """

    #: Short name used in the log line when a fault fires.
    fault = "chaos"

    def __init__(
        self,
        call: Callable[..., Awaitable[T]],
        *,
        probability: float | None = None,
        on_call: int | None = None,
        seed: int = DEFAULT_SEED,
        rng: random.Random | None = None,
    ) -> None:
        self.wrapped = call
        self.policy = FirePolicy(probability, on_call)
        self.calls = 0
        self.fires = 0
        self._rng = rng if rng is not None else new_rng(seed)

    async def __call__(self, *args: object, **kwargs: object) -> T:
        self.calls += 1
        if not self.policy.should_fire(self.calls, self._rng):
            return await self.wrapped(*args, **kwargs)
        self.fires += 1
        logger.warning(
            "chaos: injecting %s on call %d (%s)",
            self.fault,
            self.calls,
            self.policy.describe(),
        )
        return await self._fire(*args, **kwargs)

    @abstractmethod
    async def _fire(self, *args: object, **kwargs: object) -> T:
        """Run the faulty version of the wrapped call."""


class BrokerUnavailable[T](ChaosInjector[T]):
    """Raise :class:`BrokerUnavailableError` *instead of* making the call.

    The wrapped callable is never invoked — that is the difference between a
    broker being down and a handler failing. Nothing was published, nothing was
    read, no side effect ran.
    """

    fault = "broker_unavailable"

    async def _fire(self, *args: object, **kwargs: object) -> T:
        raise BrokerUnavailableError("chaos: broker unavailable")


class DuplicateDelivery[T](ChaosInjector[T]):
    """Invoke the wrapped call twice — at-least-once redelivery, in one wrapper.

    The two invocations are sequential and the *second* result is returned, so a
    non-idempotent handler runs its side effect twice, which is precisely the
    failure this fault exists to expose. If the first invocation raises, the
    second never happens and the error propagates — a redelivery is not a retry.
    """

    fault = "duplicate_delivery"

    async def _fire(self, *args: object, **kwargs: object) -> T:
        await self.wrapped(*args, **kwargs)
        return await self.wrapped(*args, **kwargs)


class CrashBeforeAck[T](ChaosInjector[T]):
    """Let the wrapped call finish, then raise :class:`ConsumerCrashError`.

    Ordering is the whole point: the work *has* been done when the crash lands.
    A consumer that acks after its handler returns will therefore never ack, the
    broker redelivers, and the side effect runs a second time — unless the
    consumer deduplicates (see
    :mod:`message_broker_patterns.entities.idempotency`). The wrapped call's
    result is discarded; a crashed consumer returns nothing.
    """

    fault = "crash_before_ack"

    async def _fire(self, *args: object, **kwargs: object) -> T:
        await self.wrapped(*args, **kwargs)
        raise ConsumerCrashError("chaos: consumer crashed before ack")


class SlowConsumer[T](ChaosInjector[T]):
    """Sleep ``latency`` seconds, then make the call.

    The delay comes *first* so a caller's deadline can expire before the call is
    even attempted — that is how ``SlowAir`` misses the scatter-gather
    aggregator's timeout. A non-positive ``latency`` is rejected at construction:
    it would leave an injector that fires, logs, and changes nothing. So is a
    non-finite one — ``nan`` slips past a bare ``latency <= 0`` guard (every
    comparison with ``nan`` is false) and ``sleep(nan)``/``sleep(inf)`` never
    wakes, wedging the wrapped call instead of merely delaying it.
    """

    fault = "slow_consumer"

    def __init__(
        self,
        call: Callable[..., Awaitable[T]],
        *,
        latency: float,
        probability: float | None = None,
        on_call: int | None = None,
        seed: int = DEFAULT_SEED,
        rng: random.Random | None = None,
    ) -> None:
        if not math.isfinite(latency) or latency <= 0:
            raise ValueError(f"latency must be finite positive seconds, got {latency!r}")
        super().__init__(call, probability=probability, on_call=on_call, seed=seed, rng=rng)
        self.latency = latency

    async def _fire(self, *args: object, **kwargs: object) -> T:
        await sleep(self.latency)
        return await self.wrapped(*args, **kwargs)


def broker_unavailable[T](
    call: Callable[..., Awaitable[T]],
    *,
    probability: float | None = None,
    on_call: int | None = None,
    seed: int = DEFAULT_SEED,
    rng: random.Random | None = None,
) -> BrokerUnavailable[T]:
    """Wrap ``call`` so it raises :class:`BrokerUnavailableError` when it fires."""
    return BrokerUnavailable(call, probability=probability, on_call=on_call, seed=seed, rng=rng)


def duplicate_delivery[T](
    call: Callable[..., Awaitable[T]],
    *,
    probability: float | None = None,
    on_call: int | None = None,
    seed: int = DEFAULT_SEED,
    rng: random.Random | None = None,
) -> DuplicateDelivery[T]:
    """Wrap ``call`` so it is invoked twice when it fires."""
    return DuplicateDelivery(call, probability=probability, on_call=on_call, seed=seed, rng=rng)


def crash_before_ack[T](
    call: Callable[..., Awaitable[T]],
    *,
    probability: float | None = None,
    on_call: int | None = None,
    seed: int = DEFAULT_SEED,
    rng: random.Random | None = None,
) -> CrashBeforeAck[T]:
    """Wrap ``call`` so it completes, then raises :class:`ConsumerCrashError`."""
    return CrashBeforeAck(call, probability=probability, on_call=on_call, seed=seed, rng=rng)


def slow_consumer[T](
    call: Callable[..., Awaitable[T]],
    *,
    latency: float,
    probability: float | None = None,
    on_call: int | None = None,
    seed: int = DEFAULT_SEED,
    rng: random.Random | None = None,
) -> SlowConsumer[T]:
    """Wrap ``call`` so it is preceded by ``latency`` seconds of delay when it fires."""
    return SlowConsumer(
        call, latency=latency, probability=probability, on_call=on_call, seed=seed, rng=rng
    )
