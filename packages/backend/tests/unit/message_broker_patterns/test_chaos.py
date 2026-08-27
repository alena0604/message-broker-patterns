import asyncio
import contextlib
import random

import pytest
from pytest_mock import MockerFixture

from message_broker_patterns.chaos import (
    DEFAULT_SEED,
    BrokerUnavailableError,
    ChaosError,
    ChaosInjector,
    ConsumerCrashError,
    broker_unavailable,
    crash_before_ack,
    duplicate_delivery,
    new_rng,
    slow_consumer,
)

MODULE = "message_broker_patterns.chaos"

# Used where the sleep is patched out, so the value is asserted, never waited on.
LATENCY = 0.25
# Used by the shared-contract tests, which do sleep for real.
TINY_LATENCY = 0.001


class Recorder:
    """A stand-in for a pattern handler: records every delivery it receives."""

    def __init__(self, *, fail_on: str | None = None) -> None:
        self.deliveries: list[str] = []
        self._fail_on = fail_on

    async def __call__(self, consumer_id: str, message: str) -> str:
        self.deliveries.append(message)
        if message == self._fail_on:
            raise ValueError(f"handler rejected {message}")
        return f"{consumer_id}:{message}"


@pytest.fixture()
def handler() -> Recorder:
    return Recorder()


FACTORIES = {
    "broker_unavailable": broker_unavailable,
    "duplicate_delivery": duplicate_delivery,
    "crash_before_ack": crash_before_ack,
    "slow_consumer": slow_consumer,
}


def build(name: str, call, **kwargs) -> ChaosInjector:
    """Construct injector ``name`` around ``call``; ``slow_consumer`` needs a latency."""
    if name == "slow_consumer":
        kwargs.setdefault("latency", TINY_LATENCY)
    return FACTORIES[name](call, **kwargs)


async def fire_sequence(injector: ChaosInjector, calls: int) -> list[bool]:
    """Call ``injector`` ``calls`` times; return which calls fired a fault."""
    sequence: list[bool] = []
    for index in range(calls):
        before = injector.fires
        with contextlib.suppress(ChaosError):
            await injector("worker-1", f"m-{index}")
        sequence.append(injector.fires > before)
    return sequence


ALL_INJECTORS = ["broker_unavailable", "duplicate_delivery", "crash_before_ack", "slow_consumer"]


# --- shared contract: firing semantics --------------------------------------


@pytest.mark.parametrize("name", ALL_INJECTORS)
async def test_probability_one_fires_on_every_call(name: str, handler: Recorder) -> None:
    injector = build(name, handler, probability=1.0)

    assert await fire_sequence(injector, 10) == [True] * 10
    assert injector.fires == 10


@pytest.mark.parametrize("name", ALL_INJECTORS)
async def test_probability_zero_never_fires(name: str, handler: Recorder) -> None:
    injector = build(name, handler, probability=0.0)

    assert await fire_sequence(injector, 10) == [False] * 10
    assert injector.fires == 0


@pytest.mark.parametrize("name", ALL_INJECTORS)
async def test_trigger_count_fires_on_exactly_that_call(name: str, handler: Recorder) -> None:
    injector = build(name, handler, on_call=2)

    assert await fire_sequence(injector, 5) == [False, True, False, False, False]
    assert injector.fires == 1


@pytest.mark.parametrize("name", ALL_INJECTORS)
@pytest.mark.parametrize("trigger", [1, 3, 7])
async def test_trigger_count_honours_any_ordinal(
    name: str, trigger: int, handler: Recorder
) -> None:
    injector = build(name, handler, on_call=trigger)

    sequence = await fire_sequence(injector, 8)

    assert [index + 1 for index, fired in enumerate(sequence) if fired] == [trigger]


@pytest.mark.parametrize("name", ALL_INJECTORS)
async def test_call_counter_counts_every_invocation(name: str, handler: Recorder) -> None:
    injector = build(name, handler, probability=0.0)

    await fire_sequence(injector, 4)

    assert injector.calls == 4


@pytest.mark.parametrize("name", ALL_INJECTORS)
async def test_quiet_call_passes_arguments_and_result_through(name: str, handler: Recorder) -> None:
    injector = build(name, handler, probability=0.0)

    result = await injector("worker-9", "PAY-1")

    assert result == "worker-9:PAY-1"
    assert handler.deliveries == ["PAY-1"]


@pytest.mark.parametrize("name", ALL_INJECTORS)
async def test_handler_errors_are_not_swallowed(name: str) -> None:
    injector = build(name, Recorder(fail_on="PAY-boom"), probability=0.0)

    with pytest.raises(ValueError, match="handler rejected PAY-boom"):
        await injector("worker-1", "PAY-boom")


# --- configuration validation ----------------------------------------------


@pytest.mark.parametrize("name", ALL_INJECTORS)
async def test_no_trigger_configured_is_rejected(name: str, handler: Recorder) -> None:
    with pytest.raises(ValueError, match="probability"):
        build(name, handler)


@pytest.mark.parametrize("name", ALL_INJECTORS)
async def test_two_triggers_configured_is_rejected(name: str, handler: Recorder) -> None:
    with pytest.raises(ValueError, match="not both"):
        build(name, handler, probability=0.5, on_call=2)


@pytest.mark.parametrize("name", ALL_INJECTORS)
@pytest.mark.parametrize("probability", [-0.1, 1.1, float("nan")])
async def test_probability_outside_the_unit_interval_is_rejected(
    name: str, probability: float, handler: Recorder
) -> None:
    with pytest.raises(ValueError, match="probability"):
        build(name, handler, probability=probability)


@pytest.mark.parametrize("name", ALL_INJECTORS)
@pytest.mark.parametrize("trigger", [0, -1])
async def test_non_positive_trigger_count_is_rejected(
    name: str, trigger: int, handler: Recorder
) -> None:
    with pytest.raises(ValueError, match="on_call"):
        build(name, handler, on_call=trigger)


@pytest.mark.parametrize(
    "latency",
    [0.0, -0.5, float("nan"), float("inf"), float("-inf")],
)
async def test_non_finite_or_non_positive_latency_is_rejected(
    latency: float, handler: Recorder
) -> None:
    """``nan``/``inf`` must be rejected too: ``sleep(nan)`` never wakes, wedging the call.

    Every comparison with ``nan`` is false, so a bare ``latency <= 0`` guard fails
    open on it.
    """
    with pytest.raises(ValueError, match="latency"):
        slow_consumer(handler, latency=latency, probability=1.0)


# --- broker_unavailable -----------------------------------------------------


async def test_broker_unavailable_raises_a_connection_style_error(handler: Recorder) -> None:
    injector = broker_unavailable(handler, probability=1.0)

    with pytest.raises(BrokerUnavailableError) as excinfo:
        await injector("worker-1", "PAY-1")

    assert isinstance(excinfo.value, ConnectionError)
    assert isinstance(excinfo.value, ChaosError)


async def test_broker_unavailable_never_reaches_the_wrapped_call(handler: Recorder) -> None:
    injector = broker_unavailable(handler, probability=1.0)

    with pytest.raises(BrokerUnavailableError):
        await injector("worker-1", "PAY-1")

    assert handler.deliveries == []


# --- duplicate_delivery -----------------------------------------------------


async def test_duplicate_delivery_invokes_the_wrapped_call_twice(handler: Recorder) -> None:
    injector = duplicate_delivery(handler, probability=1.0)

    result = await injector("worker-1", "PAY-1")

    assert handler.deliveries == ["PAY-1", "PAY-1"]
    assert result == "worker-1:PAY-1"


async def test_duplicate_delivery_delivers_once_when_quiet(handler: Recorder) -> None:
    injector = duplicate_delivery(handler, on_call=2)

    await injector("worker-1", "PAY-1")
    await injector("worker-1", "PAY-2")

    assert handler.deliveries == ["PAY-1", "PAY-2", "PAY-2"]


async def test_duplicate_delivery_propagates_a_failure_on_the_first_attempt() -> None:
    handler = Recorder(fail_on="PAY-boom")
    injector = duplicate_delivery(handler, probability=1.0)

    with pytest.raises(ValueError):
        await injector("worker-1", "PAY-boom")

    assert handler.deliveries == ["PAY-boom"]


# --- crash_before_ack -------------------------------------------------------


async def test_crash_before_ack_runs_the_work_then_crashes(handler: Recorder) -> None:
    injector = crash_before_ack(handler, probability=1.0)

    with pytest.raises(ConsumerCrashError) as excinfo:
        await injector("worker-1", "PAY-1")

    # The side effect happened; only the ack was lost.
    assert handler.deliveries == ["PAY-1"]
    assert isinstance(excinfo.value, ChaosError)


async def test_crash_before_ack_raises_a_runtime_style_error(handler: Recorder) -> None:
    """A crash is a builtin :class:`RuntimeError`, so ordinary handling paths catch it."""
    injector = crash_before_ack(handler, probability=1.0)

    with pytest.raises(RuntimeError) as excinfo:
        await injector("worker-1", "PAY-1")

    assert isinstance(excinfo.value, ConsumerCrashError)
    assert isinstance(excinfo.value, ChaosError)


async def test_crash_before_ack_crashes_only_on_the_configured_call(handler: Recorder) -> None:
    injector = crash_before_ack(handler, on_call=1)

    with pytest.raises(ConsumerCrashError):
        await injector("worker-1", "PAY-1")
    assert await injector("worker-1", "PAY-1") == "worker-1:PAY-1"

    assert handler.deliveries == ["PAY-1", "PAY-1"]


# --- slow_consumer ----------------------------------------------------------


async def test_slow_consumer_sleeps_for_the_configured_latency(
    handler: Recorder, mocker: MockerFixture
) -> None:
    sleep = mocker.patch(f"{MODULE}.sleep")
    injector = slow_consumer(handler, latency=LATENCY, probability=1.0)

    await injector("worker-1", "PAY-1")

    sleep.assert_awaited_once_with(LATENCY)


async def test_slow_consumer_does_not_sleep_when_quiet(
    handler: Recorder, mocker: MockerFixture
) -> None:
    sleep = mocker.patch(f"{MODULE}.sleep")
    injector = slow_consumer(handler, latency=LATENCY, probability=0.0)

    await injector("worker-1", "PAY-1")

    sleep.assert_not_awaited()


async def test_slow_consumer_delays_before_delivering(mocker: MockerFixture) -> None:
    events: list[str] = []

    async def record_sleep(seconds: float) -> None:
        events.append(f"slept:{seconds}")

    async def call(consumer_id: str, message: str) -> None:
        events.append(f"delivered:{message}")

    mocker.patch(f"{MODULE}.sleep", side_effect=record_sleep)
    injector = slow_consumer(call, latency=LATENCY, probability=1.0)

    await injector("worker-1", "PAY-1")

    assert events == [f"slept:{LATENCY}", "delivered:PAY-1"]


async def test_slow_consumer_latency_really_delays_the_caller(handler: Recorder) -> None:
    """The latency is real time, so a caller's timeout can trip on it."""
    injector = slow_consumer(handler, latency=0.05, probability=1.0)

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.01):
            await injector("worker-1", "PAY-1")


# --- determinism ------------------------------------------------------------


@pytest.mark.parametrize("name", ALL_INJECTORS)
async def test_same_seed_produces_an_identical_fire_sequence(name: str) -> None:
    first = await fire_sequence(build(name, Recorder(), probability=0.5, seed=7), 200)
    second = await fire_sequence(build(name, Recorder(), probability=0.5, seed=7), 200)

    assert first == second
    # A p=0.5 run that fired never or always would make the comparison vacuous.
    assert 0 < sum(first) < 200


async def test_different_seeds_produce_different_fire_sequences() -> None:
    first = await fire_sequence(crash_before_ack(Recorder(), probability=0.5, seed=7), 200)
    second = await fire_sequence(crash_before_ack(Recorder(), probability=0.5, seed=8), 200)

    assert first != second


async def test_the_default_seed_is_deterministic() -> None:
    first = await fire_sequence(crash_before_ack(Recorder(), probability=0.5), 200)
    second = await fire_sequence(crash_before_ack(Recorder(), probability=0.5), 200)

    assert first == second
    assert 0 < sum(first) < 200


async def test_default_seed_matches_an_explicitly_seeded_injector() -> None:
    implicit = await fire_sequence(crash_before_ack(Recorder(), probability=0.5), 50)
    explicit = await fire_sequence(
        crash_before_ack(Recorder(), probability=0.5, seed=DEFAULT_SEED), 50
    )

    assert implicit == explicit


async def test_injectors_left_on_the_default_seed_fire_in_lockstep() -> None:
    """Documented consequence of a fixed default seed — bound by a test, not prose."""
    left = await fire_sequence(crash_before_ack(Recorder(), probability=0.5), 50)
    right = await fire_sequence(broker_unavailable(Recorder(), probability=0.5), 50)

    assert left == right


async def test_injectors_sharing_one_rng_draw_from_one_stream() -> None:
    shared = new_rng(7)
    left = crash_before_ack(Recorder(), probability=0.5, rng=shared)
    right = crash_before_ack(Recorder(), probability=0.5, rng=shared)

    interleaved = [*(await fire_sequence(left, 1)), *(await fire_sequence(right, 1))]
    solo = await fire_sequence(crash_before_ack(Recorder(), probability=0.5, seed=7), 2)

    assert interleaved == solo


async def test_a_counted_injector_consumes_no_randomness() -> None:
    shared = new_rng(7)
    counted = crash_before_ack(Recorder(), on_call=1, rng=shared)
    probabilistic = crash_before_ack(Recorder(), probability=0.5, rng=shared)

    await fire_sequence(counted, 5)
    after_counted = await fire_sequence(probabilistic, 10)
    untouched = await fire_sequence(crash_before_ack(Recorder(), probability=0.5, seed=7), 10)

    assert after_counted == untouched


async def test_rng_construction_is_patchable(handler: Recorder, mocker: MockerFixture) -> None:
    """The house convention: RNG creation is indirected so a test can pin it."""
    expected = await fire_sequence(crash_before_ack(Recorder(), probability=0.5, seed=7), 20)

    mocker.patch(f"{MODULE}.new_rng", return_value=random.Random(7))
    pinned = await fire_sequence(crash_before_ack(handler, probability=0.5, seed=999), 20)

    assert pinned == expected


# --- composition ------------------------------------------------------------


async def test_injectors_compose_around_one_handler(mocker: MockerFixture) -> None:
    sleep = mocker.patch(f"{MODULE}.sleep")
    handler = Recorder()
    wrapped = slow_consumer(crash_before_ack(handler, on_call=1), latency=LATENCY, probability=1.0)

    with pytest.raises(ConsumerCrashError):
        await wrapped("worker-1", "PAY-1")
    assert await wrapped("worker-1", "PAY-1") == "worker-1:PAY-1"

    assert handler.deliveries == ["PAY-1", "PAY-1"]
    assert sleep.await_count == 2


async def test_composed_injectors_keep_independent_counters(handler: Recorder) -> None:
    inner = crash_before_ack(handler, on_call=1)
    outer = duplicate_delivery(inner, on_call=2)

    with pytest.raises(ConsumerCrashError):
        await outer("worker-1", "PAY-1")
    await outer("worker-1", "PAY-2")

    assert (outer.calls, outer.fires) == (2, 1)
    assert (inner.calls, inner.fires) == (3, 1)
    assert handler.deliveries == ["PAY-1", "PAY-2", "PAY-2"]


@pytest.mark.parametrize("name", ALL_INJECTORS)
async def test_injector_exposes_the_callable_it_wraps(name: str, handler: Recorder) -> None:
    injector = build(name, handler, probability=1.0)

    assert injector.wrapped is handler
