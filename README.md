# message-broker-patterns

An educational Python library demonstrating canonical message broker patterns. Each pattern is self-contained, runnable, and documented with the problem it solves, a flow diagram, and working code examples.

Patterns that need durable messaging use **Redis Streams**; patterns that don't use Python stdlib primitives (`asyncio.Queue`, `queue.Queue`). The test suite uses `fakeredis` — no real Redis needed for `make test`.

## Patterns

| Pattern | Broker | Description |
|---|---|---|
| [Transactional Outbox](packages/backend/src/message_broker_patterns/outbox_pattern/README.md) | Redis Streams | Atomic DB write + guaranteed message delivery via an outbox table and async relay |
| [Choreography Saga](packages/backend/src/message_broker_patterns/saga_pattern/README.md) | Redis Streams | Distributed transaction across services via local transactions + compensating events |

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
