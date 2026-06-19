# Saga Pattern (Choreography)

## The problem: distributed transactions across services

In a monolith, a business operation (place order → charge payment → ship item) can be wrapped in a single database transaction. If anything fails, the whole thing rolls back — atomicity is free.

In a microservices architecture, each service owns its own database. There is no shared transaction boundary. If the Order Service writes its row, the Payment Service charges the card, and the Shipping Service fails, you cannot simply "roll back" — the payment already happened, the order is in the database, and the customer is waiting.

Two-phase commit (2PC) exists but is rarely practical: it requires all services to participate in a distributed protocol, blocks resources across service boundaries, and most message brokers don't support it. You need a different approach.

---

## The solution: break the transaction into local steps with compensation

A **Saga** replaces a single distributed transaction with a sequence of **local transactions**, each isolated within one service. Each local transaction produces an event that triggers the next step. If a step fails, **compensating transactions** undo the work already done.

This implementation uses the **Choreography variant**: services react to events published on Redis Streams — there is no central coordinator. Each service listens on its input stream, does its local work, and publishes to its output stream.

---

## Flow diagrams

### Happy path

```
  ┌─────────────────┐
  │  Order Service  │
  │                 │
  │ create_order()  │
  │ status: PENDING │
  │                 │
  │ publish_created │──► OrderCreated ──► saga:orders
  │ status:         │
  │  PAYMENT_PROC.  │
  └────────┬────────┘
           │
           │ (relay reads saga:orders)
           ▼
  ┌─────────────────┐
  │ Payment Service │
  │                 │
  │ amount > 0?     │
  │   → YES         │──► PaymentProcessed ──► saga:payments
  └────────┬────────┘
           │
           │ (relay reads saga:payments)
           ├──────────────────────────────────┐
           ▼                                  ▼
  ┌─────────────────┐              ┌──────────────────┐
  │  Order Service  │              │ Shipping Service  │
  │                 │              │                   │
  │ status: PAID    │              │ publish OrderShipped
  └─────────────────┘              │    ──► saga:shipping
                                   └──────────┬────────┘
                                              │
                                   (relay reads saga:shipping)
                                              │
                                              ▼
                                   ┌──────────────────┐
                                   │  Order Service   │
                                   │                  │
                                   │ status: COMPLETED│
                                   └──────────────────┘
```

### Failure path + compensation

```
  ┌─────────────────┐
  │  Order Service  │
  │                 │
  │ publish_created │──► OrderCreated ──► saga:orders
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │ Payment Service │
  │                 │
  │ amount <= 0?    │
  │   → YES (fail)  │──► PaymentFailed ──► saga:payments
  └─────────────────┘
           │
           │ (relay reads saga:payments)
           ▼
  ┌─────────────────┐
  │  Order Service  │    ← compensation step
  │                 │
  │ status: CANCELLED
  │ publish         │──► OrderCancelled ──► saga:orders
  └─────────────────┘

  saga:shipping ─── EMPTY (Shipping Service never invoked)
```

---

## Step-by-step

**Step 1 — Start the saga**

The Order Service stores the order in memory (status `PENDING`) and publishes `OrderCreated` to `saga:orders`. Status advances to `PAYMENT_PROCESSING`.

```python
order_svc.create_order(order)
await order_svc.publish_created(order)  # → saga:orders
```

**Step 2 — Payment Service reacts**

The runner reads `saga:orders`, finds an `OrderCreated` event, and routes it to the Payment Service. If `amount > 0`, payment succeeds and `PaymentProcessed` is published. If `amount <= 0`, `PaymentFailed` is published.

```python
# in payment_service.py
if float(event.amount) <= 0:
    await broker.publish(STREAM_PAYMENTS, "PaymentFailed", ...)
else:
    await broker.publish(STREAM_PAYMENTS, "PaymentProcessed", ...)
```

**Step 3a — Happy path: Shipping + Order update**

The runner reads `saga:payments`, finds `PaymentProcessed`, and routes it to both the Order Service (status → `PAID`) and the Shipping Service (publishes `OrderShipped` to `saga:shipping`). The runner then reads `saga:shipping`, routes `OrderShipped` to the Order Service (status → `COMPLETED`). Terminal state reached — saga stops.

**Step 3b — Failure path: Compensation**

The runner reads `saga:payments`, finds `PaymentFailed`, routes it to the Order Service. The Order Service runs the compensating transaction: sets status to `CANCELLED` and publishes `OrderCancelled` to `saga:orders`. Terminal state reached — saga stops. The Shipping Service is never called.

---

## When to use it

| Situation | Use Saga? |
|---|---|
| Business operation spans multiple services, each with its own DB | **Yes** |
| Steps can fail independently and need structured rollback | **Yes** |
| You need long-running transactions (minutes to hours) | **Yes** |
| Operations are idempotent or can be made so | **Yes** |
| All data lives in one DB and ACID transactions are available | No — use a plain transaction |
| You need strict read isolation across steps | No — Sagas are eventually consistent |

### Classic scenarios

**E-commerce order fulfilment** — Order Service, Inventory Service, Payment Service, and Shipping Service each own their data. A Saga coordinates the whole flow: reserve inventory → charge card → dispatch shipment. If payment fails, reserved inventory is released (compensating transaction).

**Travel booking** — Book flight, hotel, car rental across three independent providers. If the car rental fails, cancel the hotel and flight (compensation). Each cancellation is itself a local transaction on the provider's system.

**Bank transfer** — Debit source account → credit destination account. If credit fails, reverse the debit. Without a Saga, you'd need 2PC between two databases.

---

## Implementation overview

```
saga_pattern/
├── models.py           # SagaStatus (StrEnum), Order dataclass
├── events.py           # Frozen event dataclasses + stream constants + to_dict/from_dict
├── broker.py           # SagaBroker: publish (XADD) + consume (XREAD)
├── services/
│   ├── order_service.py    # In-memory order store; handles all events that affect order state
│   ├── payment_service.py  # Handles OrderCreated; simulates payment; publishes outcome
│   └── shipping_service.py # Handles PaymentProcessed; publishes OrderShipped
└── runner.py           # Poll loop: reads three streams, routes events, detects terminal state
```

### Key invariants

- **No central coordinator.** The runner is just a poll loop that routes events to the right service method — it contains no business logic itself.
- **Deterministic failure.** `amount <= 0` always fails payment. Tests can control the flow without mocking.
- **Terminal state detection.** The runner stops automatically when the order reaches `COMPLETED` or `CANCELLED` — no manual timeout needed in tests.
- **Stream isolation.** Each service publishes to exactly one stream; the runner is the only consumer. This mirrors the real-world shape where each service owns its input queue.
- **At-least-once delivery.** `last_id` per stream advances only after processing — if the runner restarts, it re-reads from the last committed position.

---

## Trade-offs

| | Choreography (this impl.) | Orchestration |
|---|---|---|
| Coordinator | None — services react to events | Central orchestrator issues commands |
| Coupling | Services know each other's event schemas | Services know only the orchestrator's schema |
| Visibility | Hard to trace the full flow | Easy — orchestrator has the whole picture |
| Scalability | Each service scales independently | Orchestrator can become a bottleneck |
| Debugging | Requires distributed tracing | Easier — one place to inspect state |

**At-least-once delivery** — if the runner crashes after publishing an event but before advancing `last_id`, the event will be re-published on restart. Consumers must be idempotent (use `order_id` + `event_type` as a deduplication key in a real system).

**No saga log** — this demo holds order state in memory. A production implementation would persist a `sagas` table recording each step, enabling crash recovery without re-running completed steps.

---

## Running the demo

```bash
docker run -p 6379:6379 redis:7-alpine

uv --directory packages/backend run python scripts/run_saga_pattern.py
```

Expected output (abbreviated):

```
============================================================
  HAPPY PATH — payment succeeds
============================================================
Final order status: completed

saga:orders    (1): OrderCreated
saga:payments  (1): PaymentProcessed
saga:shipping  (1): OrderShipped

============================================================
  FAILURE PATH — payment fails (compensation)
============================================================
Final order status: cancelled

saga:orders    (2): OrderCreated, OrderCancelled
saga:payments  (1): PaymentFailed
saga:shipping  (0): —
```
