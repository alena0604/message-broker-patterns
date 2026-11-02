from message_broker_patterns.logging import init_logger

init_logger()

import asyncio  # noqa: E402
import contextlib  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
import signal  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine  # noqa: E402
from datetime import UTC, datetime  # noqa: E402
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer  # noqa: E402
from types import FrameType  # noqa: E402
from typing import Any  # noqa: E402

from message_broker_patterns.metrics import REGISTRY, render_prometheus  # noqa: E402

logger = logging.getLogger(__name__)

HOST = "localhost"
PORT = 8000
LOOP_INTERVAL_SECONDS = 2.0

# The JSON dashboard feed and the Prometheus scrape of the same snapshot.
JSON_PATH = "/metrics"
PROMETHEUS_PATH = "/metrics/prometheus"
PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# How often the stop-event bridge samples the threading event, and how long a
# loop waits for its pattern tasks to wind down before cancelling them.
STOP_POLL_INTERVAL_SECONDS = 0.1
TASK_SHUTDOWN_TIMEOUT_SECONDS = 2.0

# How long Ctrl-C waits for every loop to notice the stop event (up to one
# LOOP_INTERVAL_SECONDS sleep) and release the resources it owns.
SHUTDOWN_GRACE_SECONDS = LOOP_INTERVAL_SECONDS + TASK_SHUTDOWN_TIMEOUT_SECONDS + 1.0


@contextlib.asynccontextmanager
async def _asyncio_stop_event(stop: threading.Event) -> AsyncIterator[asyncio.Event]:
    """Bridge the loop's ``threading.Event`` into an ``asyncio.Event``.

    Several pattern entry points (``AirlineService.serve``,
    ``run_idempotent_consumer``, ``run_consumer``) take an ``asyncio.Event``,
    while ``_run_in_thread`` hands each loop a ``threading.Event``. A watcher
    task polls the threading event and sets the asyncio one; it is always
    cancelled and awaited on exit so no task outlives the loop.
    """
    async_stop = asyncio.Event()

    async def _watch() -> None:
        while not stop.is_set():
            await asyncio.sleep(STOP_POLL_INTERVAL_SECONDS)
        async_stop.set()

    watcher = asyncio.create_task(_watch())
    try:
        yield async_stop
    finally:
        async_stop.set()
        watcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher


def _log_task_exception(task: asyncio.Task[int]) -> None:
    """Report a pattern task that died on its own instead of swallowing it.

    A consumer that dies mid-run would otherwise leave the dashboard quietly
    misreporting — its producer side keeps climbing while the consumed counters
    freeze, with nothing in the log to say why.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("Pattern task %s died: %r", task.get_name(), exc, exc_info=exc)


def _spawn(coro: Coroutine[Any, Any, int], name: str) -> asyncio.Task[int]:
    """Start a pattern task whose failure is always logged, never silent."""
    task = asyncio.create_task(coro, name=name)
    task.add_done_callback(_log_task_exception)
    return task


async def _stop_and_wait(async_stop: asyncio.Event, tasks: list[asyncio.Task[int]]) -> None:
    """Signal pattern tasks to stop, then await them (cancelling any stragglers)."""
    async_stop.set()
    _done, pending = await asyncio.wait(tasks, timeout=TASK_SHUTDOWN_TIMEOUT_SECONDS)
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def competing_consumers_loop(redis_url: str, stop: threading.Event) -> None:
    """Publish + consume tasks in a loop so metrics accumulate from real executions."""
    import redis.asyncio as aioredis

    from message_broker_patterns.competing_consumers_pattern.broker import (
        CompetingConsumersBroker,
    )
    from message_broker_patterns.competing_consumers_pattern.models import Task

    stream = "metrics_server:cc"
    group = "metrics_workers"
    client = aioredis.from_url(redis_url)
    broker = CompetingConsumersBroker(client)
    await client.delete(stream)
    await broker.ensure_group(stream, group)
    batch_n = 0

    while not stop.is_set():
        # Publish 5 tasks
        for i in range(5):
            task = Task(f"batch{batch_n}-task{i}", f"payload-{i}")
            await broker.publish(stream, task.to_fields())

        # Two consumers each read up to 3 messages and ack them
        for cid in ["worker-0", "worker-1"]:
            batch = await broker.read_new(stream, group, cid, 3, 100)
            for msg_id, _ in batch:
                await broker.ack(stream, group, msg_id)
                REGISTRY.increment("competing_consumers", "messages_consumed")

        batch_n += 1
        await asyncio.sleep(LOOP_INTERVAL_SECONDS)

    await client.delete(stream)
    await client.aclose()


async def event_sourcing_loop(redis_url: str, stop: threading.Event) -> None:
    import redis.asyncio as aioredis

    from message_broker_patterns.event_sourcing_pattern.aggregate import BankAccount
    from message_broker_patterns.event_sourcing_pattern.store import EventStore

    client = aioredis.from_url(redis_url)
    store = EventStore(client)
    idx = 0

    while not stop.is_set():
        account_id = f"metrics-acc-{idx}"
        acct = BankAccount(account_id=account_id)
        created = acct.create(f"owner-{idx}")
        await store.append(account_id, created.event_type, created.to_dict())
        deposited = acct.deposit(100)
        await store.append(account_id, deposited.event_type, deposited.to_dict())
        withdrawn = acct.withdraw(30)
        await store.append(account_id, withdrawn.event_type, withdrawn.to_dict())
        idx += 1
        await asyncio.sleep(LOOP_INTERVAL_SECONDS)

    await client.aclose()


async def outbox_loop(redis_url: str, stop: threading.Event) -> None:
    import json as json_mod
    import sqlite3

    import redis.asyncio as aioredis

    from message_broker_patterns.outbox_pattern.broker import RedisBroker
    from message_broker_patterns.outbox_pattern.models import Order
    from message_broker_patterns.outbox_pattern.store import (
        create_tables,
        delete_outbox_entry,
        insert_order_with_outbox,
        poll_outbox,
    )

    client = aioredis.from_url(redis_url)
    broker = RedisBroker(client)
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    stream = "metrics_server:outbox"
    idx = 0

    while not stop.is_set():
        order = Order(
            order_id=f"m-order-{idx}",
            customer_id=f"cust-{idx}",
            amount=99.0,
            created_at=datetime.now(UTC),
        )
        insert_order_with_outbox(conn, order)
        # Relay any pending entries inline (avoids relay.run()'s blocking sleep)
        entries = poll_outbox(conn)
        for entry in entries:
            payload = json_mod.loads(entry.payload)
            await broker.publish(stream, {k: str(v) for k, v in payload.items()})
            assert entry.id is not None
            delete_outbox_entry(conn, entry.id)
            REGISTRY.increment("transactional_outbox", "entries_relayed")
        idx += 1
        await asyncio.sleep(LOOP_INTERVAL_SECONDS)

    await client.delete(stream)
    await client.aclose()
    conn.close()


async def saga_loop(redis_url: str, stop: threading.Event) -> None:
    import uuid

    import redis.asyncio as aioredis

    from message_broker_patterns.saga_pattern.broker import SagaBroker
    from message_broker_patterns.saga_pattern.events import (
        STREAM_ORDERS,
        STREAM_PAYMENTS,
        STREAM_SHIPPING,
    )
    from message_broker_patterns.saga_pattern.models import Order
    from message_broker_patterns.saga_pattern.runner import run_saga
    from message_broker_patterns.saga_pattern.services.order_service import OrderService
    from message_broker_patterns.saga_pattern.services.payment_service import PaymentService
    from message_broker_patterns.saga_pattern.services.shipping_service import ShippingService

    client = aioredis.from_url(redis_url)
    broker = SagaBroker(client)
    idx = 0

    while not stop.is_set():
        # Clear streams between runs
        await client.delete(STREAM_ORDERS, STREAM_PAYMENTS, STREAM_SHIPPING)

        order = Order(
            order_id=str(uuid.uuid4()),
            customer_id=f"cust-{idx}",
            amount=50.0,
            created_at=datetime.now(UTC),
        )
        order_svc = OrderService(broker)
        payment_svc = PaymentService(broker)
        shipping_svc = ShippingService(broker)
        saga_stop = asyncio.Event()
        await run_saga(order_svc, payment_svc, shipping_svc, broker, order, saga_stop)
        idx += 1
        await asyncio.sleep(LOOP_INTERVAL_SECONDS)

    await client.delete(STREAM_ORDERS, STREAM_PAYMENTS, STREAM_SHIPPING)
    await client.aclose()


async def priority_queue_loop(redis_url: str, stop: threading.Event) -> None:
    import redis.asyncio as aioredis

    from message_broker_patterns.priority_queue_pattern.broker import (
        STREAMS,
        PriorityQueueBroker,
    )
    from message_broker_patterns.priority_queue_pattern.models import (
        Priority,
        SupportTicket,
    )

    client = aioredis.from_url(redis_url)
    broker = PriorityQueueBroker(client)
    group = "metrics_agents"
    await client.delete(*STREAMS.values())
    await broker.ensure_all_groups(group)
    idx = 0

    while not stop.is_set():
        # Publish one ticket per priority each iteration, then read + ack it.
        for priority, prefix in [
            (Priority.HIGH, "H"),
            (Priority.NORMAL, "N"),
            (Priority.LOW, "L"),
        ]:
            ticket = SupportTicket(
                ticket_id=f"{prefix}-{idx:04d}",
                subject=f"ticket {idx}",
                priority=priority,
                customer_id=f"cust-{idx}",
            )
            await broker.publish(ticket)
            batch = await broker.read_new(priority, group, "metrics-agent", 10, 50)
            for msg_id, _ in batch:
                await broker.ack(priority, group, msg_id)
                REGISTRY.increment("priority_queue", "tickets_consumed")
                REGISTRY.increment("priority_queue", f"{priority.value}_consumed")
        idx += 1
        await asyncio.sleep(LOOP_INTERVAL_SECONDS)

    await client.delete(*STREAMS.values())
    await client.aclose()


async def scatter_gather_loop(redis_url: str, stop: threading.Event) -> None:
    """Run flight searches so scatter-gather counters accumulate.

    Pure stdlib/asyncio — ``redis_url`` is unused, kept for signature
    compatibility with the other loops. Three airlines answer each broadcast:
    one healthy, one too slow for the gather deadline, one whose inventory
    lookup raises — so ``partial_gathers`` and ``service_failures`` move too.
    """
    del redis_url  # this pattern needs no broker

    import uuid

    from message_broker_patterns.scatter_gather_pattern.aggregator import (
        BROADCAST_REQUEST_TOPIC,
        RESPONSE_TOPIC,
        ScatterGatherCoordinator,
    )
    from message_broker_patterns.scatter_gather_pattern.broker import InMemoryTopicBroker
    from message_broker_patterns.scatter_gather_pattern.models import (
        DistributionStrategy,
        FlightQuote,
        SearchRequest,
    )
    from message_broker_patterns.scatter_gather_pattern.service import AirlineService

    gather_timeout = 0.2

    def make_lookup(airline: str, price_cents: int) -> Callable[[SearchRequest], FlightQuote]:
        def _lookup(request: SearchRequest) -> FlightQuote:
            return FlightQuote(
                correlation_id=request.correlation_id,
                airline=airline,
                flight_number=f"{airline[:2].upper()}{price_cents // 100}",
                price_cents=price_cents,
                depart_at=datetime.now(UTC),
            )

        return _lookup

    def ghost_lookup(request: SearchRequest) -> FlightQuote:
        raise RuntimeError("inventory system offline")

    broker = InMemoryTopicBroker()
    services = [
        AirlineService(
            "BudgetJet",
            broker,
            make_lookup("BudgetJet", 18_900),
            BROADCAST_REQUEST_TOPIC,
            RESPONSE_TOPIC,
        ),
        # Too slow to make the gather deadline → the search ends up partial.
        AirlineService(
            "SlowAir",
            broker,
            make_lookup("SlowAir", 15_000),
            BROADCAST_REQUEST_TOPIC,
            RESPONSE_TOPIC,
            latency=gather_timeout * 2,
        ),
        # Always errors → service_failures.
        AirlineService("GhostAir", broker, ghost_lookup, BROADCAST_REQUEST_TOPIC, RESPONSE_TOPIC),
    ]
    coordinator = ScatterGatherCoordinator(broker)
    idx = 0

    async with _asyncio_stop_event(stop) as async_stop:
        tasks = [
            _spawn(service.serve(async_stop), f"scatter_gather:{service.name}")
            for service in services
        ]
        try:
            while not stop.is_set():
                request = SearchRequest(
                    correlation_id=str(uuid.uuid4()),
                    origin="New York (JFK)",
                    destination="Los Angeles (LAX)",
                    departure_date="2026-08-01",
                    passengers=1 + idx % 3,
                )
                await coordinator.scatter_gather(
                    request,
                    DistributionStrategy.PUBLISH_SUBSCRIBE,
                    expected=len(services),
                    timeout=gather_timeout,
                )
                idx += 1
                await asyncio.sleep(LOOP_INTERVAL_SECONDS)
        finally:
            await _stop_and_wait(async_stop, tasks)


async def dlq_loop(redis_url: str, stop: threading.Event) -> None:
    """Run the payment pipeline so DLQ counters accumulate.

    Payment ids carry a per-process unique prefix and the streams plus the
    ``payments:processed`` set are deleted at start (and on exit). Without both,
    a second server run would find every id already processed and report nothing
    but ``skipped_duplicate`` while looking wired.
    """
    import uuid

    import redis.asyncio as aioredis

    from message_broker_patterns.dlq_pattern.broker import (
        DLQ_STREAM,
        MAIN_STREAM,
        PROCESSED_SET,
        DLQBroker,
    )
    from message_broker_patterns.dlq_pattern.consumer import run_idempotent_consumer
    from message_broker_patterns.dlq_pattern.models import Payment

    group = "metrics_payment_workers"
    run_id = uuid.uuid4().hex[:8]
    client = aioredis.from_url(redis_url)
    broker = DLQBroker(client)
    await client.delete(MAIN_STREAM, DLQ_STREAM, PROCESSED_SET)
    await broker.ensure_all_groups(group)
    logger.info("dlq loop run=%s — cleared %s and its idempotency set", run_id, MAIN_STREAM)

    async def handler(consumer_id: str, payment: Payment) -> None:
        if payment.amount_cents < 0:
            raise ValueError(f"malformed amount: {payment.amount_cents}")
        await asyncio.sleep(0.01)  # simulate charging the card

    idx = 0
    async with _asyncio_stop_event(stop) as async_stop:
        consumer = _spawn(
            run_idempotent_consumer(
                broker, "metrics-worker", group, handler, async_stop, max_attempts=2, block_ms=20
            ),
            "dlq:metrics-worker",
        )
        try:
            while not stop.is_set():
                # 3 valid payments + 1 poison one, which exhausts its 2 attempts
                # and lands in the DLQ.
                payments = [
                    Payment(f"metrics-{run_id}-{idx}-{i}", 1_000 + i, f"cust-{i}", "USD")
                    for i in range(3)
                ]
                payments.append(Payment(f"metrics-{run_id}-{idx}-poison", -1, "cust-X", "USD"))
                for payment in payments:
                    await broker.publish(payment)
                idx += 1
                await asyncio.sleep(LOOP_INTERVAL_SECONDS)
        finally:
            await _stop_and_wait(async_stop, [consumer])
            await client.delete(MAIN_STREAM, DLQ_STREAM, PROCESSED_SET)
            await client.aclose()


async def claim_check_loop(redis_url: str, stop: threading.Event) -> None:
    """Check ~100 KB payloads in and out so claim-check counters accumulate.

    Stdlib + filesystem — ``redis_url`` is unused, kept for signature
    compatibility. All state lives in a ``TemporaryDirectory`` so the loop
    leaves nothing behind when the server stops.
    """
    del redis_url  # this pattern needs no broker

    import tempfile
    from pathlib import Path

    from message_broker_patterns.claim_check_pattern.broker import ClaimCheckBroker
    from message_broker_patterns.claim_check_pattern.consumer import run_consumer
    from message_broker_patterns.claim_check_pattern.models import ClaimCheck, Payload
    from message_broker_patterns.claim_check_pattern.producer import ClaimCheckProducer
    from message_broker_patterns.claim_check_pattern.storage import FilesystemPayloadStore

    async def handler(consumer_id: str, claim: ClaimCheck, payload: Payload) -> None:
        await asyncio.sleep(0.01)  # simulate work on the large object

    with tempfile.TemporaryDirectory(prefix="metrics-claim-check-") as tmp:
        logger.info("claim_check loop external storage: %s", tmp)
        store = FilesystemPayloadStore(Path(tmp))
        broker = ClaimCheckBroker()
        producer = ClaimCheckProducer(broker, store)
        idx = 0

        async with _asyncio_stop_event(stop) as async_stop:
            consumer = _spawn(
                run_consumer(broker, store, handler, async_stop, consumer_id="metrics-consumer"),
                "claim_check:metrics-consumer",
            )
            try:
                while not stop.is_set():
                    # ~100 KB — far past any broker message limit, so
                    # broker_bytes_saved is a visibly large number.
                    payload = Payload(b"M" * 100_000, "image/png", f"scan-{idx:05d}.png")
                    await producer.publish(payload)
                    idx += 1
                    await asyncio.sleep(LOOP_INTERVAL_SECONDS)
            finally:
                await _stop_and_wait(async_stop, [consumer])
    logger.info("claim_check loop removed external storage: %s", tmp)


def _run_in_thread(
    coro_fn: Callable[[str, threading.Event], Awaitable[None]],
    redis_url: str,
    stop: threading.Event,
) -> None:
    try:
        asyncio.run(coro_fn(redis_url, stop))
    except Exception:
        logger.exception("Pattern loop %s crashed", coro_fn.__name__)


class MetricsHandler(BaseHTTPRequestHandler):
    def _write_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")

    def do_OPTIONS(self) -> None:
        if self.path not in (JSON_PATH, PROMETHEUS_PATH):
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self._write_cors_headers()
        self.end_headers()

    def do_GET(self) -> None:
        # Prometheus renders the same snapshot on its own route; the JSON branch
        # below is the frozen frontend contract and stays untouched.
        if self.path == PROMETHEUS_PATH:
            body = render_prometheus(REGISTRY.snapshot()).encode()
            self.send_response(200)
            self.send_header("Content-Type", PROMETHEUS_CONTENT_TYPE)
            self._write_cors_headers()
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps(
            {
                "patterns": REGISTRY.snapshot(),
                "server_time": datetime.now(UTC).isoformat(),
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self._write_cors_headers()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)


def _ignore_shutdown_signals() -> None:
    """Make shutdown uninterruptible once it has started.

    Cleanup takes a second or two (each loop has to notice the stop event and
    release its Redis keys / temp dir). A second Ctrl-C — or a SIGTERM — in that
    window must not abort it, so both signals are ignored from here on.
    """
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal.SIG_IGN)


def _shutdown_on_signal(signum: int, _frame: FrameType | None) -> None:
    """Turn SIGTERM into the same KeyboardInterrupt path Ctrl-C takes."""
    _ignore_shutdown_signals()
    logger.info("Received %s", signal.Signals(signum).name)
    raise KeyboardInterrupt


def main() -> None:
    from message_broker_patterns.config.settings import settings

    # Pre-register patterns so the snapshot lists them even before the first
    # loop iteration increments any counter.
    REGISTRY.register("competing_consumers", "Competing Consumers")
    REGISTRY.register("event_sourcing", "Event Sourcing")
    REGISTRY.register("transactional_outbox", "Transactional Outbox")
    REGISTRY.register("saga", "Saga (Choreography)")
    REGISTRY.register("priority_queue", "Priority Queue")
    REGISTRY.register("scatter_gather", "Scatter-Gather")
    REGISTRY.register("dlq", "Dead Letter Queue")
    REGISTRY.register("claim_check", "Claim Check")

    stop = threading.Event()
    loops = [
        competing_consumers_loop,
        event_sourcing_loop,
        outbox_loop,
        saga_loop,
        priority_queue_loop,
        scatter_gather_loop,
        dlq_loop,
        claim_check_loop,
    ]
    threads = [
        threading.Thread(target=_run_in_thread, args=(fn, settings.redis_url, stop), daemon=True)
        for fn in loops
    ]
    for t in threads:
        t.start()

    server = ThreadingHTTPServer((HOST, PORT), MetricsHandler)
    logger.info(
        "Metrics server on http://%s:%d/metrics — running real pattern loops (Redis: %s)",
        HOST,
        PORT,
        settings.redis_url,
    )
    # SIGTERM (`kill <pid>`) takes the same route as Ctrl-C so it releases the
    # loops' resources instead of dropping them on the floor.
    signal.signal(signal.SIGTERM, _shutdown_on_signal)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        _ignore_shutdown_signals()
        logger.info("Shutting down (further Ctrl-C ignored until cleanup finishes)")
    finally:
        # Cleanup must be uninterruptible: a second Ctrl-C landing in the join
        # below would otherwise raise straight out of main() and leave the DLQ
        # loop's Redis keys and the claim-check temp dir behind.
        _ignore_shutdown_signals()
        stop.set()
        server.shutdown()
        # The loops are daemon threads: without an explicit join the interpreter
        # exits before they run their cleanup, leaving the DLQ loop's Redis keys
        # behind. One shared deadline — the loops all wind down in parallel.
        deadline = time.monotonic() + SHUTDOWN_GRACE_SECONDS
        for t in threads:
            t.join(timeout=max(0.0, deadline - time.monotonic()))
        logger.info("Stopped")


if __name__ == "__main__":
    main()
