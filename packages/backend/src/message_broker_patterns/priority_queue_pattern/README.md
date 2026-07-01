# Priority Queue Pattern

## The problem: one queue treats every message the same

A single shared queue is fair but blind. A fraud alert and a "please update my
shipping address" request land in the same line and are served in arrival order.
When the queue is busy, the urgent ticket waits behind a backlog of routine ones.

```
❌ One queue, FIFO

  Producer ──► [ shipping  docs  FRAUD!  password  feature ] ──► Consumers
                                  ▲
                       urgent ticket stuck behind routine work
```

You can't fix this by adding consumers to the single queue — they still drain it
in order, so the urgent ticket's position in line doesn't improve.

---

## The solution: a queue per priority, drained independently

The Priority Queue pattern gives each priority level **its own stream and its own
pool of consumers**. Routing happens at publish time: a ticket goes straight to
its level's queue. Each consumer is dedicated to a single level, so an urgent
ticket is never behind a routine one — it is in a different queue entirely.

```
✓ One queue per priority

                     ┌──► [ support:high   ] ──► H1 H2 H3 H4   (4 agents)
  Support Portal ──► ┼──► [ support:normal ] ──► N1 N2 N3      (3 agents)
                     └──► [ support:low    ] ──► L1 L2 L3      (3 agents)

  • priority is decided once, at publish time → no scanning, no reordering
  • each level gets its own throughput → HIGH never waits behind LOW
  • size each pool proportionally to the priority's volume + SLA
```

This implementation uses **Redis Streams**, one stream + one consumer group per
priority — the same broker the
[Competing Consumers](../competing_consumers_pattern/README.md) pattern uses,
applied once per level.

---

## How it maps to Redis Streams

| Command | Role in the pattern |
|---|---|
| `XADD support:<level>` | Route a ticket to its priority's stream (chosen by `SupportTicket.priority`). |
| `XGROUP CREATE … MKSTREAM` | Create the group (and stream) per level. Idempotent — `BUSYGROUP` swallowed. |
| `XREADGROUP … >` | A consumer claims new tickets from **its own level's** stream only. |
| `XACK` | Acknowledge a processed ticket so it leaves the level's pending list. |
| `XPENDING` | Inspect delivered-but-unacked tickets per level (used to observe progress). |

---

## Implementation overview

```
priority_queue_pattern/
├── models.py     # Priority (StrEnum) + SupportTicket dataclass + (de)serialization
├── broker.py     # redis.asyncio wrapper: publish / ensure_group(s) / read_new / ack / pending_count
└── consumer.py   # async worker loop dedicated to ONE priority: read new → handle → ack, until stop_event
```

### Key invariants

- **Routing is decided once, at publish time.** `publish()` picks the stream from
  `ticket.priority`; there is no priority comparison on the read path.
- **A consumer only ever touches its own priority.** That dedication is what gives
  high-priority tickets their own throughput, independent of lower levels.
- `ensure_group()` is **idempotent** — `BUSYGROUP` is swallowed so any number of
  consumers can call it on startup without coordination.
- A ticket is **acked only after** the handler completes — at-least-once delivery,
  so handlers should be idempotent (use `ticket_id` to dedupe).

---

## Running the demo

```bash
# start Redis
docker run -p 6379:6379 redis:7-alpine

# 10 tickets (3 HIGH, 4 NORMAL, 3 LOW) drained by proportional consumer pools
uv --directory packages/backend run python scripts/run_priority_queue_pattern.py
```

Expected output (abridged):

```
=== Priority Queue Demo: Support Ticket System ===
Publishing 10 tickets (3 HIGH, 4 NORMAL, 3 LOW)
[high-agent-0] T-001 — FRAUD ALERT: suspicious transaction (high)
[normal-agent-1] T-004 — Billing discrepancy on last invoice (normal)
[low-agent-0] T-008 — Feature request: dark mode (low)
...
=== Results ===
high-agent-0 handled: ['T-001', 'T-002', 'T-003']
normal-agent-1 handled: ['T-004', 'T-005', 'T-006', 'T-007']
low-agent-0 handled: ['T-008', 'T-009', 'T-010']
```

---

## When to use it

| Situation | Use a priority queue? |
|---|---|
| Some messages are genuinely more urgent and must not wait | **Yes** |
| Each priority has a different SLA / staffing level | **Yes** — size each pool independently |
| Priority levels are few and known up front | **Yes** — one stream per level is simple |
| Every message is equally important | No — a single Competing-Consumers queue is simpler |
| Priorities are a continuous score, not a few buckets | No — a sorted structure (e.g. a Redis sorted set) fits better |

---

## Trade-offs

**Pros**
- Urgent work is isolated — HIGH never waits behind NORMAL/LOW.
- Each level scales independently; pool size follows volume and SLA.
- Reuses the proven Streams consumer-group machinery, once per level.

**Cons**
- Risk of **starvation**: if HIGH is saturated and you statically split agents,
  LOW can stall. Production systems add aging (promote old low tickets) or let
  idle HIGH agents help drain LOW.
- A fixed number of levels — a continuous priority score needs a different
  structure.
- At-least-once delivery — handlers must be idempotent.
```
