from message_broker_patterns.logging import init_logger

init_logger()

import asyncio  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
import threading  # noqa: E402
from collections.abc import Awaitable, Callable  # noqa: E402
from datetime import UTC, datetime  # noqa: E402
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer  # noqa: E402

from message_broker_patterns.metrics import REGISTRY  # noqa: E402

logger = logging.getLogger(__name__)

HOST = "localhost"
PORT = 8000
LOOP_INTERVAL_SECONDS = 2.0


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
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self._write_cors_headers()
        self.end_headers()

    def do_GET(self) -> None:
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


def main() -> None:
    from message_broker_patterns.config.settings import settings

    # Pre-register patterns so the snapshot lists them even before the first
    # loop iteration increments any counter.
    REGISTRY.register("competing_consumers", "Competing Consumers")
    REGISTRY.register("event_sourcing", "Event Sourcing")
    REGISTRY.register("transactional_outbox", "Transactional Outbox")
    REGISTRY.register("saga", "Saga (Choreography)")
    REGISTRY.register("priority_queue", "Priority Queue")

    stop = threading.Event()
    loops = [
        competing_consumers_loop,
        event_sourcing_loop,
        outbox_loop,
        saga_loop,
        priority_queue_loop,
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
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        stop.set()
        server.shutdown()


if __name__ == "__main__":
    main()
