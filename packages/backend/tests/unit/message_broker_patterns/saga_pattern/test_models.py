from message_broker_patterns.saga_pattern.events import OrderCreated
from message_broker_patterns.saga_pattern.models import Order, SagaStatus


def test_order_default_status_is_pending() -> None:
    order = Order(order_id="o-1", customer_id="c-1", amount=50.0)
    assert order.status == SagaStatus.PENDING


def test_order_created_at_is_utc_aware() -> None:
    order = Order(order_id="o-1", customer_id="c-1", amount=50.0)
    assert order.created_at.tzinfo is not None


def test_event_roundtrip() -> None:
    event = OrderCreated(order_id="o-1", customer_id="c-1", amount="99.99")
    restored = OrderCreated.from_dict(event.to_dict())
    assert restored.order_id == event.order_id
    assert restored.customer_id == event.customer_id
    assert restored.amount == event.amount
