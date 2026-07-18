from __future__ import annotations

from message_broker_patterns.scatter_gather_pattern.broker import InMemoryTopicBroker


def test_subscribe_returns_distinct_queues() -> None:
    broker = InMemoryTopicBroker()

    first = broker.subscribe("topic")
    second = broker.subscribe("topic")

    assert first is not second
    assert broker.subscriber_count("topic") == 2


async def test_publish_fans_out_to_every_subscriber() -> None:
    broker = InMemoryTopicBroker()
    first = broker.subscribe("topic")
    second = broker.subscribe("topic")

    reached = await broker.publish("topic", "hello")

    assert reached == 2
    assert await first.get() == "hello"
    assert await second.get() == "hello"


async def test_publish_with_no_subscribers_returns_zero() -> None:
    broker = InMemoryTopicBroker()

    reached = await broker.publish("empty-topic", "hello")

    assert reached == 0


async def test_unsubscribe_removes_queue_from_fan_out() -> None:
    broker = InMemoryTopicBroker()
    keep = broker.subscribe("topic")
    drop = broker.subscribe("topic")

    broker.unsubscribe("topic", drop)
    reached = await broker.publish("topic", "hello")

    assert reached == 1
    assert broker.subscriber_count("topic") == 1
    assert await keep.get() == "hello"
    assert drop.empty()


def test_unsubscribe_unknown_queue_is_noop() -> None:
    broker = InMemoryTopicBroker()
    stray = broker.subscribe("other")

    # Unsubscribing from a topic it was never on must not raise.
    broker.unsubscribe("topic", stray)

    assert broker.subscriber_count("topic") == 0
