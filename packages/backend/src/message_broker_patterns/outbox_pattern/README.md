# Transactional Outbox Pattern

## The problem: the dual-write trap

When a service needs to both **save data to a database** and **publish an event to a message broker**, the two operations are independent — and one can fail after the other succeeds.

```
❌ Naive approach (broken)

  Service
    │
    ├─► INSERT order INTO database   ✓ succeeds
    │
    └─► PUBLISH event to broker      ✗ crashes here
                                       → order saved, event never sent
                                       → downstream services never notified
                                       → data inconsistency
```

You cannot wrap a database transaction and a network call to a broker in a single atomic unit. If the broker is temporarily unavailable, your process crashes, or the network times out after the DB commit — the event is lost forever.

Common workarounds all have holes:
- **Publish first, then save** — if the DB write fails, you sent a ghost event.
- **Save first, then publish** — if the publish fails, you have a silent gap.
- **Two-phase commit** — works but is complex, slow, and most brokers don't support it.

---

## The solution: write to the outbox, relay asynchronously

The Transactional Outbox pattern eliminates the dual-write problem by turning the message publish into a **database write** that participates in the same transaction as the business data.

```
✓ Transactional Outbox approach

  Service
    │
    └─► BEGIN TRANSACTION
          INSERT order INTO orders table
          INSERT event  INTO outbox table   ← same transaction
        COMMIT
          → either both succeed or both roll back
          → no partial state possible

  Relay (separate process)
    │
    ├─► POLL outbox table for new entries
    ├─► PUBLISH each entry to broker (XADD → Redis Stream)
    └─► DELETE entry from outbox on success
```

The business write and the outbox write are **one atomic operation**. The relay is a simple background loop — if it crashes, it restarts and re-processes any un-deleted outbox entries. Delivery is **at-least-once**: the event will eventually reach the broker, guaranteed.

---

## Flow diagram

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  Application (e.g. order service)                                │
  │                                                                  │
  │   order = Order(order_id="abc", customer_id="u-1", amount=99.0) │
  │                                                                  │
  │   insert_order_with_outbox(conn, order)                          │
  │      │                                                           │
  │      └─► BEGIN TRANSACTION ──────────────────────┐              │
  │                                                   │              │
  │                                          ┌────────▼───────┐     │
  │                                          │   SQLite DB    │     │
  │                                          │                │     │
  │                                          │ ┌────────────┐ │     │
  │                                          │ │   orders   │ │     │
  │                                          │ │ order_id   │ │     │
  │                                          │ │ customer   │ │     │
  │                                          │ │ amount     │ │     │
  │                                          │ └────────────┘ │     │
  │                                          │                │     │
  │                                          │ ┌────────────┐ │     │
  │                                          │ │   outbox   │ │     │
  │                                          │ │ id         │ │     │
  │                                          │ │ order_id   │ │     │
  │                                          │ │ payload    │ │     │ 
  │                                          │ └────────────┘ │     │
  │                                          └────────┬───────┘     │
  │                                                   │              │
  │                                          COMMIT TRANSACTION      │
  └──────────────────────────────────────────────────────────────────┘
                                                      │
                                           ┌──────────┘
                                           │ (polls every N seconds)
                              ┌────────────▼────────────┐
                              │     Relay Process        │
                              │                          │
                              │  entries = poll_outbox() │
                              │  for entry in entries:   │
                              │    broker.publish(entry) │
                              │    delete_outbox(entry)  │
                              └────────────┬────────────┘
                                           │
                                        XADD
                                           │
                              ┌────────────▼────────────┐
                              │    Redis Stream          │
                              │    "orders:events"       │
                              │                          │
                              │  1781707417466-0         │
                              │  1781707417467-0         │
                              │  1781707417467-1         │
                              └────────────┬────────────┘
                                           │
                          ┌────────────────┼────────────────┐
                          │                │                 │
               ┌──────────▼──┐   ┌─────────▼──┐   ┌────────▼────┐
               │  Email Svc  │   │ Inventory  │   │  Analytics  │
               │ (consumer)  │   │  (consumer)│   │  (consumer) │
               └─────────────┘   └────────────┘   └─────────────┘
```

---

## Step-by-step flow

**Step 1 — Atomic write**

The application writes the business record and an event payload to the outbox table in a single database transaction. If anything fails (DB constraint, network blip, process crash), both writes roll back — no partial state.

```python
order = Order(order_id="ord-123", customer_id="user-42", amount=99.99)
entry = insert_order_with_outbox(conn, order)
# → orders table: 1 new row
# → outbox table: 1 new row with JSON payload
# → both in one COMMIT, or both rolled back
```

**Step 2 — Relay polls the outbox**

A background process (the relay) runs a polling loop. It reads unprocessed rows from the outbox table ordered by insertion time.

```python
entries = poll_outbox(conn)
# → [OutboxEntry(id=1, order_id="ord-123", payload='{"event":"order_created",...}')]
```

**Step 3 — Publish to Redis Stream**

For each entry the relay calls `XADD` on the Redis Stream. Redis assigns a message ID (`<milliseconds>-<sequence>`) and appends the message permanently to the stream.

```python
await broker.publish("orders:events", {"event": "order_created", "order_id": "ord-123", ...})
# → Redis: XADD orders:events * event order_created order_id ord-123 ...
# → returns message id: "1781707417466-0"
```

**Step 4 — Delete from outbox**

Only after the broker confirms receipt does the relay delete the entry. If the relay crashes between publish and delete, the entry will be re-published on restart — **at-least-once delivery**.

```python
delete_outbox_entry(conn, entry.id)
# → outbox table: row deleted
# → stream: message already persisted in Redis
```

**Step 5 — Consumers read the stream**

Downstream services (`XREAD` / `XREADGROUP`) consume events from the Redis Stream independently. Because Redis Streams are persistent, each consumer group maintains its own cursor — a slow consumer doesn't block a fast one, and a crashed consumer resumes exactly where it left off.

---

## When to use it

| Situation | Use outbox? |
|---|---|
| You write to a DB and need downstream services notified — reliably | **Yes** |
| A failure between DB commit and broker publish would cause silent data loss | **Yes** |
| Your business operation is the single source of truth and events must reflect it exactly | **Yes** |
| You need at-least-once delivery with no broker dependency at write time | **Yes** |
| You're doing a fire-and-forget notification where loss is acceptable | No — plain publish is simpler |
| You have distributed transactions / Saga coordination already | Probably not — Saga handles ordering itself |

### Classic scenarios

**E-commerce order placement**
Customer places an order. The service inserts the order and a `order_placed` event atomically. The relay notifies the inventory service (to reserve stock), the email service (to send confirmation), and the analytics service — all independently, all reliably.

**Financial transfer**
A debit and credit are recorded in the DB. An `account_debited` event is placed in the outbox in the same transaction. Downstream ledger services and fraud-detection consumers receive the event even if the application crashes immediately after the commit.

**User registration**
A new user row and a `user_registered` event are written atomically. The relay publishes to a stream consumed by the welcome-email service, the CRM sync service, and the feature-flag service — each at their own pace.

---

## Implementation overview

```
outbox_pattern/
├── models.py   # Order and OutboxEntry dataclasses (UTC-aware datetimes)
├── store.py    # SQLite: create_tables / insert_order_with_outbox / poll_outbox / delete_outbox_entry
├── broker.py   # redis.asyncio wrapper: publish() → XADD
└── relay.py    # async relay loop: poll → publish → delete, stops on asyncio.Event
```

### Key invariants

- `insert_order_with_outbox()` uses `with conn:` — a single SQLite transaction covers both inserts. A `UNIQUE` violation on `order_id` rolls back the outbox insert too, leaving no orphan entries.
- The relay deletes **only after** a successful `XADD` — re-delivery on restart is safe because Redis Streams are append-only and consumers use consumer groups with explicit acknowledgement.
- All datetimes are UTC-aware (`datetime.now(UTC)`). Never naive `datetime.now()`.

---

## Running the demo

```bash
# start Redis
docker run -p 6379:6379 redis:7-alpine

# run the demo (inserts 3 orders, runs the relay, prints the stream)
uv --directory packages/backend run python scripts/run_outbox_pattern.py
```

Expected output:

```
Inserted 3 orders into DB + outbox.
INFO relay Relay started — polling outbox every 0.2s
INFO relay Relayed outbox entry 1 → stream orders:events
INFO relay Relayed outbox entry 2 → stream orders:events
INFO relay Relayed outbox entry 3 → stream orders:events
INFO relay Relay stopped

Messages in Redis stream 'orders:events': 3
  1781707417466-0: {b'event': b'order_created', b'order_id': b'...', b'customer_id': b'cust-1', b'amount': b'99.99'}
  1781707417467-0: {b'event': b'order_created', b'order_id': b'...', b'customer_id': b'cust-2', b'amount': b'149.0'}
  1781707417467-1: {b'event': b'order_created', b'order_id': b'...', b'customer_id': b'cust-3', b'amount': b'9.99'}
```

---

## Trade-offs

**Pros**
- Zero message loss — the outbox is as durable as your database.
- No broker dependency at write time — the application commits even if Redis is down; the relay catches up when Redis recovers.
- Simple to reason about — one transaction, one relay loop, no distributed protocol.

**Cons**
- At-least-once delivery — consumers must be idempotent (use the `order_id` as a deduplication key).
- Polling latency — the relay adds a small delay (configurable `poll_interval`). Change-data-capture (CDC) tools like Debezium can replace polling for near-zero latency.
- Extra outbox table — a minor schema addition and one extra write per business operation.
- Relay is a single point of failure — run multiple relay instances with a distributed lock (e.g. Redis `SET NX`) if you need high availability.
