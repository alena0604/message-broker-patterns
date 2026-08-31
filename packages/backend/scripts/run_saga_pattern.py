from message_broker_patterns.logging import init_logger

init_logger()

import argparse  # noqa: E402
import asyncio  # noqa: E402
import uuid  # noqa: E402

import redis.asyncio as aioredis  # noqa: E402

from message_broker_patterns.config.settings import settings  # noqa: E402
from message_broker_patterns.saga_pattern.broker import SagaBroker  # noqa: E402
from message_broker_patterns.saga_pattern.events import (  # noqa: E402
    STREAM_ORDERS,
    STREAM_PAYMENTS,
    STREAM_SHIPPING,
)
from message_broker_patterns.saga_pattern.models import Order  # noqa: E402
from message_broker_patterns.saga_pattern.naive import (  # noqa: E402
    NaiveOrderService,
    NaivePaymentService,
    NaiveShippingService,
    ShippingUnavailableError,
    place_order_without_compensation,
)
from message_broker_patterns.saga_pattern.runner import run_saga  # noqa: E402
from message_broker_patterns.saga_pattern.services.order_service import OrderService  # noqa: E402
from message_broker_patterns.saga_pattern.services.payment_service import (  # noqa: E402
    PaymentService,
)
from message_broker_patterns.saga_pattern.services.shipping_service import (  # noqa: E402
    ShippingService,
)


async def run_demo(redis_client: aioredis.Redis, label: str, amount: float) -> None:
    broker = SagaBroker(redis_client)
    order_svc = OrderService(broker)
    payment_svc = PaymentService(broker)
    shipping_svc = ShippingService(broker)

    order = Order(order_id=str(uuid.uuid4()), customer_id="demo-user", amount=amount)
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"  order_id: {order.order_id}  amount: {amount}")
    print(f"{'=' * 60}")

    stop = asyncio.Event()
    await run_saga(order_svc, payment_svc, shipping_svc, broker, order, stop, poll_interval=0.1)

    final = order_svc.get_order(order.order_id)
    print(f"\nFinal order status: {final.status if final else 'unknown'}")  # type: ignore[union-attr]

    for stream, label_ in [
        (STREAM_ORDERS, "saga:orders"),
        (STREAM_PAYMENTS, "saga:payments"),
        (STREAM_SHIPPING, "saga:shipping"),
    ]:
        msgs = await redis_client.xrange(stream)
        print(f"\n{label_} ({len(msgs)} message(s)):")
        for msg_id, fields in msgs:
            event_type = fields.get(b"event_type", b"?").decode()
            order_id = fields.get(b"order_id", b"?").decode()
            print(f"  {msg_id.decode()}  {event_type}  order_id={order_id}")

    # clean up streams for next demo run
    await redis_client.delete(STREAM_ORDERS, STREAM_PAYMENTS, STREAM_SHIPPING)
    await broker.close()


async def main() -> None:
    redis_client = aioredis.from_url(settings.redis_url)

    await run_demo(redis_client, "HAPPY PATH — payment succeeds", amount=150.0)
    await run_demo(redis_client, "FAILURE PATH — payment fails (compensation)", amount=-1.0)

    await redis_client.aclose()


# --- naive baseline -----------------------------------------------------------
# Three orders through the same create → charge → ship sequence the saga runs,
# one of which the shipping service refuses *after* the money has already moved.
# No chaos wrapper here: `NaiveShippingService.reject_for` is the baseline's own
# knob for "step 3 fails", and it is synchronous, so there is no async call site
# for a chaos injector to wrap. The failure is deterministic without one.
NAIVE_ORDERS = [
    ("naive-ord-1", 150.00),
    ("naive-ord-2", 42.00),  # ← shipping refuses this one
    ("naive-ord-3", 79.50),
]
NAIVE_REJECTED_ORDER = "naive-ord-2"


async def run_naive() -> None:
    """INTENTIONALLY BROKEN — the no-compensation baseline the saga replaces."""
    order_svc = NaiveOrderService()
    payment_svc = NaivePaymentService()
    shipping_svc = NaiveShippingService(reject_for={NAIVE_REJECTED_ORDER})

    print(f"\n{'=' * 60}")
    print("  NAIVE saga — no compensating transaction (INTENTIONALLY BROKEN)")
    print(f"  {len(NAIVE_ORDERS)} orders; shipping refuses {NAIVE_REJECTED_ORDER} after the charge")
    print(f"{'=' * 60}")

    failures: list[tuple[str, str]] = []
    for order_id, amount in NAIVE_ORDERS:
        order = Order(order_id=order_id, customer_id="demo-user", amount=amount)
        try:
            await place_order_without_compensation(order_svc, payment_svc, shipping_svc, order)
        except ShippingUnavailableError as exc:
            # Nothing above this line is undone: there is no `except` that
            # refunds, because the naive payment service has no refund at all.
            failures.append((order_id, str(exc)))

    print(f"\n{len(NAIVE_ORDERS)} orders placed, {len(shipping_svc.shipments)} shipped")
    for order_id, reason in failures:
        stranded = order_svc.get_order(order_id)
        print(f"\n{order_id}: shipping failed — {reason}")
        print(f"  money still held:  {payment_svc.net_cents(order_id)} cents (never refunded —")
        print("                     NaivePaymentService has a charge() and no refund())")
        print(
            f"  order status:      {stranded.status.value if stranded else '?'} "
            "(not completed, not cancelled)"
        )
    print(f"\nledger:    {payment_svc.ledger}")
    print(f"shipments: {shipping_svc.shipments}")
    print(
        "\nRun without --naive: a failed step publishes a compensation and the order "
        "reaches cancelled with the money returned."
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Saga (orchestrated distributed transaction) demo."
    )
    parser.add_argument(
        "--naive",
        action="store_true",
        help="run the intentionally broken no-compensation baseline (naive.py) instead",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_naive() if _parse_args().naive else main())
