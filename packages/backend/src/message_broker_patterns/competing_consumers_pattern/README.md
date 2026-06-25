# Competing Consumers Pattern

## The problem: one consumer can't keep up

A single consumer reading from a queue is a bottleneck. When the producer is
faster than the consumer, messages back up and end-to-end latency grows without
bound.

```
❌ Single consumer (bottleneck)

  Producer  ──fast──►  [ m9 m8 m7 m6 m5 m4 m3 ]  ──slow──►  Consumer
                              backlog grows                  (falls behind)
```

You can't just "process faster" — the work per message is fixed. What you need
is more hands on the same queue.

---

## The solution: many consumers competing for the same queue

The Competing Consumers pattern runs **multiple consumer instances in parallel**,
all attached to the same queue. The broker acts as a load balancer: each message
is delivered to exactly **one** of the available consumers. Add consumers to add
throughput.

```
✓ Competing consumers (horizontal scale)

                          ┌──►  Consumer A   (gets m1, m4, m7 …)
  Producer ──► [ queue ] ─┼──►  Consumer B   (gets m2, m5, m8 …)
                          └──►  Consumer C   (gets m3, m6, m9 …)

  • each message handled by exactly one consumer  → no double-processing
  • a crashed consumer's in-flight message is reclaimed by a sibling
```

Two properties make this valuable, and a naive `asyncio.Queue` gives you neither
across processes:

1. **Load balancing** — the broker, not the application, decides who gets the
   next message, so consumers naturally pull work at their own pace.
2. **Redelivery / crash recovery** — a message claimed by a consumer that dies
   before acking is not lost; a surviving sibling reclaims and finishes it.

This implementation uses **Redis Streams consumer groups**, the same broker the
[Transactional Outbox](../outbox_pattern/README.md) pattern uses.

---

## How Redis Streams consumer groups deliver it

```
  XADD tasks:work * task_id task-0 payload …      ← producer appends

  ┌──────────────────────  consumer group "workers"  ──────────────────────┐
  │                                                                         │
  │   XGROUP CREATE tasks:work workers $ MKSTREAM    (idempotent setup)     │
  │                                                                         │
  │   worker-0 ─ XREADGROUP GROUP workers worker-0 … STREAMS tasks:work >   │
  │   worker-1 ─ XREADGROUP GROUP workers worker-1 … STREAMS tasks:work >   │
  │   worker-2 ─ XREADGROUP GROUP workers worker-2 … STREAMS tasks:work >   │
  │        │            the ">" id means "messages never delivered          │
  │        │             to any consumer in this group" → load balanced     │
  │        ▼                                                                 │
  │   process(task)                                                         │
  │        │                                                                 │
  │   XACK tasks:work workers <id>    ← message leaves the Pending List      │
  │                                                                         │
  │   ── crash recovery ──                                                   │
  │   XAUTOCLAIM tasks:work workers worker-N <min-idle> 0                    │
  │        reclaims messages a dead consumer read but never XACKed          │
  └─────────────────────────────────────────────────────────────────────────┘
```

| Command | Role in the pattern |
|---|---|
| `XGROUP CREATE … MKSTREAM` | Create the group (and stream). Idempotent — `BUSYGROUP` is swallowed so every consumer can call it on startup. |
| `XREADGROUP … >` | Each consumer claims **new** (never-delivered) messages — the load-balancing primitive. |
| `XACK` | Acknowledge a processed message so it leaves the group's Pending Entries List. |
| `XAUTOCLAIM` / `XCLAIM` | Reclaim messages idle longer than a threshold — the crash-recovery primitive. |
| `XPENDING` | Inspect delivered-but-unacked messages (used here to observe progress). |

---

## Step-by-step flow

**Step 1 — Set up the consumer group (idempotent)**

```python
await broker.ensure_group("tasks:work", "workers")
# XGROUP CREATE tasks:work workers 0 MKSTREAM
# returns True if created, False if it already existed (BUSYGROUP swallowed)
```

**Step 2 — Producer appends tasks**

```python
await broker.publish("tasks:work", Task("task-0", "payload-0").to_fields())
# XADD tasks:work * task_id task-0 payload payload-0  → "1782369042840-0"
```

**Step 3 — Each consumer claims new messages**

```python
batch = await broker.read_new("tasks:work", "workers", "worker-0", count=4, block_ms=50)
# XREADGROUP GROUP workers worker-0 COUNT 4 BLOCK 50 STREAMS tasks:work >
# the broker hands these messages to worker-0 and to no other consumer
```

**Step 4 — Process, then acknowledge**

```python
await handler("worker-0", task)
await broker.ack("tasks:work", "workers", message_id)
# XACK tasks:work workers 1782369042840-0  → message leaves the pending list
```

**Step 5 — Reclaim work from a crashed sibling**

```python
reclaimed = await broker.reclaim_stale("tasks:work", "workers", "worker-1", min_idle_ms=5000, count=10)
# XAUTOCLAIM tasks:work workers worker-1 5000 0
# any message a dead consumer read but never acked (idle > 5s) is now worker-1's
```

---

## When to use it

| Situation | Use competing consumers? |
|---|---|
| One consumer can't keep up with the producer | **Yes** — scale out horizontally |
| Work items are independent and order-insensitive | **Yes** |
| You need a crashed worker's in-flight work to be retried, not lost | **Yes** — that's what the pending list + `XAUTOCLAIM` give you |
| Every consumer must see every message (fan-out) | No — that's Pub/Sub, not competing consumers |
| Strict global ordering of all messages is required | No — parallel consumers process out of order |

### Classic scenarios

**Image / video processing** — a queue of "resize this upload" jobs fanned out
across a pool of workers; add workers to clear a backlog faster.

**Email / notification sending** — many senders pull from one outbound queue; if
a sender crashes mid-batch, a sibling reclaims the unsent messages.

**Order fulfilment** — independent "fulfil order" tasks balanced across workers,
with crash recovery so no order is silently dropped.

---

## Implementation overview

```
competing_consumers_pattern/
├── models.py     # Task dataclass + to_fields()/from_fields() stream (de)serialization
├── broker.py     # redis.asyncio wrapper: ensure_group / publish / read_new / ack / reclaim_stale
└── consumer.py   # async worker loop: reclaim stale → read new → handle → ack, until stop_event
```

### Key invariants

- `ensure_group()` is **idempotent** — the `BUSYGROUP` error is swallowed so any
  number of consumers can call it on startup without coordination.
- A message is **acked only after** the handler completes. If the consumer dies
  first, the message stays on the pending list and is reclaimable — at-least-once
  delivery, so handlers should be idempotent (use `task_id` to dedupe).
- Each iteration **reclaims stale work first, then reads new** — a crashed
  sibling's backlog is drained even while fresh messages keep arriving.
- The loop takes a short cooperative `idle_sleep` when a sweep finds no work, so
  one consumer can't hot-spin and starve its siblings or the stop signal.

---

## Running the demo

```bash
# start Redis
docker run -p 6379:6379 redis:7-alpine

# run the demo (fast producer + 3 competing consumers, then crash recovery)
uv --directory packages/backend run python scripts/run_competing_consumers_pattern.py
```

Expected output (abridged):

```
=== Demo 1: load balancing across 3 consumers ===
producer pushed 30 tasks onto stream tasks:work
consumer=worker-0 handled task=task-0  msg=1782369042840-0
consumer=worker-1 handled task=task-4  msg=1782369042841-2
consumer=worker-2 handled task=task-8  msg=1782369042842-2
... (each task handled by exactly one worker) ...
--- distribution ---
worker-0 handled 10 task(s): [...]
worker-1 handled 12 task(s): [...]
worker-2 handled 8 task(s): [...]
processed 30 task(s), 30 unique → exactly-once: True

=== Demo 2: crash recovery via XAUTOCLAIM ===
doomed-worker read 1 message(s) then crashed (no ack); pending=1
consumer=survivor-worker reclaimed 1 stale message(s)
consumer=survivor-worker handled task=orphan-1 msg=1782369043123-0
survivor-worker reclaimed and processed orphaned task=orphan-1; pending=0
```

---

## Trade-offs

**Pros**
- Horizontal scale — throughput grows with the number of consumers.
- Crash recovery — in-flight work is reclaimed, not lost (`XAUTOCLAIM`).
- The broker does the load balancing; consumers stay simple.

**Cons**
- At-least-once delivery — a reclaimed message may be processed twice if the
  first consumer crashed *after* the side effect but *before* the ack. Handlers
  must be idempotent.
- No global ordering — parallel consumers process messages out of order.
- A perpetually-failing message ("poison pill") will be reclaimed forever; a
  production system pairs this with a max-delivery count and a dead-letter stream.
