"""Benchmark the dead-letter queue against its one knob: ``max_attempts``.

``max_attempts`` is the retry budget: how many times a payment may fail before
the consumer stops redelivering it and parks it in ``payments:dlq``. It is the
knob that decides how long a poison message is allowed to hold up the pipeline,
so the bench holds the poison rate fixed (one payment in ``POISON_EVERY`` has a
negative amount and can never succeed) and varies only the budget.

What the numbers show: p50 barely moves, because four in five payments succeed
on their first attempt; the tail does, because every poison payment costs
``max_attempts`` deliveries before it is dead-lettered. A generous budget buys
resilience against transient failures and pays for it in tail latency — that is
the trade-off the knob *is*. The depth series is ``XLEN payments:dlq`` over
time: the same number of payments end up dead-lettered whatever the budget, they
just take longer to get there.

One operation = publish one payment and wait for its terminal outcome. Terminal
means charged, or — for a poison payment — its final failed attempt, at which
point the consumer moves it to the DLQ microseconds later; the depth series is
where that landing is visible.

Operations run one at a time on purpose. Redis only redelivers an unacked
message when the consumer explicitly re-reads its pending entries, which
``run_idempotent_consumer`` does *only* when no new work is waiting — keeping
the pipeline full would starve the retries this bench exists to measure.

**Run this bench alone on a given Redis.** The DLQ pattern hardcodes
``payments:main`` / ``payments:dlq`` / ``payments:processed``, so a run can
namespace its own consumer group, consumer name and payment ids — which is what
keeps two runs from resolving each other's futures and hanging — but not the
streams. Two concurrent runs therefore still share a stream: each one's group
also reads the other's payments, inflating ``payments_charged``, ``dlq_length``
and latency, and whichever run clears the keys next between its own
configurations takes the other's consumer group with it, at which point that run
fails fast with ``NOGROUP`` rather than reporting numbers. Namespacing the
streams too would mean making them configurable in ``dlq_pattern.broker``.

    uv --directory packages/backend run python scripts/bench_dlq_pattern.py
    uv --directory packages/backend run python scripts/bench_dlq_pattern.py --max-attempts 3
"""

from message_broker_patterns.logging import init_logger

# WARNING would still let through the consumer's per-failure retry warnings and
# every DLQ move — at a fixed poison rate that is one line per attempt. ERROR
# keeps the JSON on stdout readable.
init_logger("ERROR")

import argparse  # noqa: E402
import asyncio  # noqa: E402
import itertools  # noqa: E402
import logging  # noqa: E402
import sys  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.bench import (  # noqa: E402
    BenchResult,
    Window,
    add_bench_args,
    emit_json,
    new_run_id,
    run_bench,
    show_bench_progress,
    write_csv_files,
)
from message_broker_patterns.config.settings import settings  # noqa: E402
from message_broker_patterns.dlq_pattern.broker import (  # noqa: E402
    DLQ_STREAM,
    MAIN_STREAM,
    PROCESSED_SET,
    DLQBroker,
)
from message_broker_patterns.dlq_pattern.consumer import run_idempotent_consumer  # noqa: E402
from message_broker_patterns.dlq_pattern.models import Payment  # noqa: E402

show_bench_progress()
logger = logging.getLogger("bench_dlq")
# This script's own progress line, like the harness's, survives the deliberately
# quiet root level set above.
logger.setLevel(logging.INFO)

# The broker hardcodes its stream names, so this bench runs on the same keys as
# the demo and cleans them up either side of every configuration — exactly what
# `scripts/run_dlq_pattern.py` does.
#
# What this run *can* own is its identity on those shared keys: the consumer
# group, the consumer name and the payment ids all carry a per-process run id.
# Without it, two concurrent runs share a group and a consumer name, so Redis
# hands one run's payment to the other's identically-named consumer — and share
# payment ids, so the *shared* `payments:processed` set makes one run's payment
# look like a duplicate of the other's, at which point the consumer skips the
# handler entirely and the publisher's future is never resolved. Both are hangs;
# namespacing all three is what avoids them.
RUN_ID = new_run_id()
GROUP = f"bench_payment_workers_{RUN_ID}"
CONSUMER = f"bench-worker-1-{RUN_ID}"

# Every Nth payment is malformed (negative amount) and can never be charged.
# Fixed, so `max_attempts` is the only thing changing between configurations.
POISON_EVERY = 5

# Simulated per-payment work for the payments that do succeed.
WORK_SECONDS = 0.001

# A short block keeps a redelivery cheap: the consumer only re-reads its pending
# entries after a `>` read comes back empty, and that read blocks this long.
BLOCK_MS = 5

DEFAULT_MAX_ATTEMPTS = [1, 3, 5]
DEFAULT_OPS = 200
DEFAULT_WARMUP = 20
DEFAULT_SAMPLE_INTERVAL = 0.02


async def bench_max_attempts(max_attempts: int, args: argparse.Namespace) -> BenchResult:
    """Run one measured window with the consumer allowed ``max_attempts`` tries."""
    client = aioredis.from_url(settings.redis_url)
    await client.delete(MAIN_STREAM, DLQ_STREAM, PROCESSED_SET)
    broker = DLQBroker(client)
    await broker.ensure_all_groups(GROUP)

    # payment_id -> the future its publisher is waiting on.
    awaiting: dict[str, asyncio.Future[None]] = {}
    attempts: dict[str, int] = {}
    charged = 0
    poisoned = 0

    def _resolve(payment_id: str) -> None:
        future = awaiting.pop(payment_id, None)
        if future is not None and not future.done():
            future.set_result(None)

    async def handler(consumer_id: str, payment: Payment) -> None:
        nonlocal charged
        if payment.amount_cents < 0:
            attempt = attempts.get(payment.payment_id, 0) + 1
            attempts[payment.payment_id] = attempt
            if attempt >= max_attempts:
                # The consumer dead-letters this payment the instant the raise
                # below reaches it, so this failure is its terminal outcome.
                _resolve(payment.payment_id)
            raise ValueError(f"malformed amount: {payment.amount_cents}")
        await asyncio.sleep(WORK_SECONDS)
        charged += 1
        _resolve(payment.payment_id)

    stop = asyncio.Event()
    consumer = asyncio.create_task(
        run_idempotent_consumer(
            broker,
            CONSUMER,
            GROUP,
            handler,
            stop,
            max_attempts=max_attempts,
            block_ms=BLOCK_MS,
        )
    )

    consumer_failure: BaseException | None = None

    def _consumer_died(task: asyncio.Task[int]) -> None:
        """Fail the publishers instead of leaving them awaiting a dead consumer.

        Nothing resolves a payment's future once the consumer task is gone, so a
        crash there would otherwise show up as a bench that hangs forever. It is
        reachable: the streams belong to the pattern, not to this run, so a
        second bench process clearing them between its own configurations takes
        this run's group with them and the next read raises ``NOGROUP``.
        """
        nonlocal consumer_failure
        if task.cancelled():
            return
        consumer_failure = task.exception()
        if consumer_failure is None:
            return
        for pending in awaiting.values():
            if not pending.done():
                pending.set_exception(consumer_failure)

    consumer.add_done_callback(_consumer_died)

    payment_ids = itertools.count(1)
    loop = asyncio.get_running_loop()

    async def publish_and_await_outcome() -> None:
        nonlocal poisoned
        if consumer_failure is not None:
            # The consumer died between operations; there is nobody left to
            # resolve a future registered from here.
            raise consumer_failure
        index = next(payment_ids)
        poison = index % POISON_EVERY == 0
        poisoned += poison
        payment = Payment(
            payment_id=f"BENCH-{RUN_ID}-{max_attempts}-{index:06d}",
            amount_cents=-1 if poison else 9_900,
            customer_id="cust-bench",
            currency="USD",
        )
        future: asyncio.Future[None] = loop.create_future()
        awaiting[payment.payment_id] = future
        await broker.publish(payment)
        await future

    async def sample_dlq_depth() -> int:
        return int(await client.xlen(DLQ_STREAM))

    try:
        result = await run_bench(
            "dlq",
            publish_and_await_outcome,
            warmup=Window.of_ops(args.warmup),
            measure=Window.of_ops(args.ops),
            concurrency=args.concurrency,
            knobs={"max_attempts": max_attempts},
            sampler=sample_dlq_depth,
            sample_interval=args.sample_interval,
        )
    finally:
        stop.set()
        await consumer

    result.extra = {
        "poison_every": POISON_EVERY,
        "payments_published": args.warmup + result.ops,
        "poison_published": poisoned,
        "payments_charged": charged,
        "dlq_length": int(await client.xlen(DLQ_STREAM)),
        "main_pending": (await client.xpending(MAIN_STREAM, GROUP))["pending"],
    }

    await client.delete(MAIN_STREAM, DLQ_STREAM, PROCESSED_SET)
    await broker.close()
    return result


async def main() -> None:
    args = _parse_args()
    # Logged, not emitted in the JSON: the operator needs it to tell this run's
    # payments apart in `payments:main`, but it is not a measurement.
    logger.info("bench run %s — group=%s consumer=%s", RUN_ID, GROUP, CONSUMER)
    results = [await bench_max_attempts(budget, args) for budget in args.max_attempts]
    emit_json(results, stream=sys.stdout)
    if args.csv_dir is not None:
        write_csv_files(results, args.csv_dir)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark the dead-letter queue across one or more retry budgets."
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        nargs="+",
        default=DEFAULT_MAX_ATTEMPTS,
        help=f"retry budget(s) before a payment is dead-lettered (default: {DEFAULT_MAX_ATTEMPTS})",
    )
    add_bench_args(
        parser,
        ops=DEFAULT_OPS,
        warmup=DEFAULT_WARMUP,
        sample_interval=DEFAULT_SAMPLE_INTERVAL,
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
