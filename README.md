# message-broker-patterns

An educational Python library demonstrating canonical message broker patterns. Each pattern is self-contained, runnable, and documented with the problem it solves, a flow diagram, and working code examples.

Patterns that need durable messaging use **Redis Streams**; patterns that don't use Python stdlib primitives (`asyncio.Queue`, `queue.Queue`). The test suite uses `fakeredis` — no real Redis needed for `make test`.

## Patterns

| Pattern | Broker | Delivery semantics | Ordering | Description |
|---|---|---|---|---|
| [Transactional Outbox](packages/backend/src/message_broker_patterns/outbox_pattern/README.md) | Redis Streams | At-least-once — the relay deletes the outbox row only after a successful `XADD` | FIFO per stream — the relay drains the outbox in insertion order (`ORDER BY id`) | Atomic DB write + guaranteed message delivery via an outbox table and async relay |
| [Choreography Saga](packages/backend/src/message_broker_patterns/saga_pattern/README.md) | Redis Streams | At-least-once — the per-stream `last_id` advances only after an event is processed | FIFO per stream (monotonic `XREAD` cursor); no ordering across the three saga streams | Distributed transaction across services via local transactions + compensating events |
| [Claim Check](packages/backend/src/message_broker_patterns/claim_check_pattern/README.md) | stdlib `asyncio.Queue` + filesystem payload store | At-most-once — no ack and no persistence; a dequeued claim is never redelivered | FIFO per queue with one consumer; parallel consumers complete out of order | Keep large payloads out of the broker: store the bytes, publish only a lightweight claim check |
| [Competing Consumers](packages/backend/src/message_broker_patterns/competing_consumers_pattern/README.md) | Redis Streams (consumer groups) | At-least-once — ack after the handler; `XAUTOCLAIM` reclaims a crashed consumer's in-flight work | None — parallel consumers process messages out of order | Scale a queue horizontally: many consumers compete, each message handled by exactly one |
| [Dead Letter Queue](packages/backend/src/message_broker_patterns/dlq_pattern/README.md) | Redis Streams (consumer groups) | At-least-once at the broker; effectively-once for the side effect via the `payment_id` dedup guard | None across consumers, and none under retry — a redelivered message returns after newer ones | Bound retries and park poison messages on a dead-letter stream with reason + attempt count |
| [Event Sourcing (CQRS)](packages/backend/src/message_broker_patterns/event_sourcing_pattern/README.md) | Redis Streams (one stream per aggregate) | At-least-once to the read side; idempotent, because the projector re-folds the history from genesis | Strict total order per aggregate; no ordering across aggregates | Store immutable events as the source of truth; derive state by replay and project read models |
| [Priority Queue](packages/backend/src/message_broker_patterns/priority_queue_pattern/README.md) | Redis Streams (one stream + group per priority) | At-least-once — a ticket is acked only after the handler completes | FIFO per priority stream; none across priorities (by design) or within a level's parallel pool | Give each priority its own queue and consumer pool so urgent work never waits behind routine work |
| [Scatter-Gather](packages/backend/src/message_broker_patterns/scatter_gather_pattern/README.md) | stdlib asyncio (in-memory topic broker) | At-most-once — no ack, no persistence; replies arriving after the deadline are dropped | None — quotes are gathered as they arrive, then reordered by a combining strategy | Broadcast one request to many recipients and aggregate the replies under a hard timeout |

## Quick start

```bash
# install deps
make install

# run tests (no Redis needed)
make test

# run the outbox pattern demo against real Redis
docker run -p 6379:6379 redis:7-alpine
uv --directory packages/backend run python scripts/run_outbox_pattern.py
```

## Development

See [AGENTS.md](AGENTS.md) for architecture, conventions, and the agent-team workflow.
