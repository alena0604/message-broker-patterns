# Dead Letter Queue Pattern

## The problem: one poison message stalls the whole pipeline

A payment consumer reads from a stream, charges the customer, and acknowledges
the message. Then a malformed payment arrives — a negative amount, a currency
the processor rejects, a field the producer stopped sending. The handler raises.

The message is never acked, so the broker redelivers it. The handler raises
again. Forever.

```
❌ Naive approach: retry forever, no escape hatch

  Producer ──► [ payments:main ]
                     │
                     ▼
             ┌───────────────────┐
             │  consumer         │
             │  handler(payment) │──► raises ValueError (malformed amount)
             └───────────────────┘
                     │  no ack
                     ▼
             redelivered ──► raises ──► redelivered ──► raises ──► …
                     │
                     └─► retry loop never terminates
                         • the poison message is never resolved
                         • retries burn CPU and broker round-trips
                         • the pending list grows and never drains
                         • nobody is told which payment is broken
```

Two things are missing, and they are separate problems:

1. **No terminal state for a failure.** "Retry" with no bound is not a failure
   policy — a message that can never succeed needs somewhere to go.
2. **No protection against redelivery of a message that already succeeded.**
   The broker guarantees *at-least-once* delivery, so a crash after the side
   effect but before the ack replays the payment — and charges the customer
   twice.

The obvious workarounds fail:

- **Ack on failure** (drop it) — no double-charge, but the payment is silently
  lost and no operator ever finds out.
- **Ack before handling** — turns at-least-once into at-most-once: a consumer
  crash loses the payment.
- **Bigger retry budget** — a malformed payment is malformed on attempt 1000
  too. Bounding the *count* only helps if there is somewhere for the message to
  land afterwards.

---

## The solution: a retry budget with a dead-letter destination, behind an idempotency guard

Every payment enters `payments:main`. The consumer runs three steps per
delivery: **dedupe → handle → ack**, with failures charged against a per-message
retry budget. When a message exhausts `max_attempts` it is acked off the main
stream and appended to `payments:dlq` — with the failure reason and attempt
count attached — where an operator can inspect it without the pipeline stalling.

```
✓ Dead Letter Queue: bounded retries + an idempotency guard

  Producer ──► [ payments:main ]
                     │  XREADGROUP >  (new)  /  XREADGROUP 0  (own unacked backlog)
                     ▼
     ┌───────────────────────────────────────────────────────────┐
     │  consumer                                                 │
     │                                                           │
     │  1. SISMEMBER payments:processed <payment_id>             │
     │        hit  ──► XACK + skip  (already charged — no        │
     │                              double-charge on replay)     │
     │        miss ──► continue                                  │
     │                                                           │
     │  2. await handler(payment)                                │
     │        ok   ──► SADD payments:processed <payment_id>      │
     │                 XACK                                      │
     │                                                           │
     │  3. raises ──► attempts[msg_id] += 1                      │
     │        attempt < max_attempts ──► leave UNACKED           │
     │                                   (redelivered next pass) │
     │        attempt == max_attempts ┐                          │
     └────────────────────────────────┼──────────────────────────┘
                                      │  XACK main + XADD dlq
                                      ▼
                        [ payments:dlq ]  ──► operator / replay tool
                         payment fields + reason + attempt
```

This implementation uses **Redis Streams consumer groups** — the same broker as
the [Transactional Outbox](../outbox_pattern/README.md) and
[Competing Consumers](../competing_consumers_pattern/README.md) patterns
(see [ADR-0002](../../../../../docs/adr/0002-use-redis-streams-for-broker-backed-patterns.md)).
It needs a running Redis; the test suite uses `fakeredis`.

---

## How it maps to the components

| Component | Role in the pattern |
|---|---|
| `payments:main` | The pipeline stream every payment enters (`XADD`). |
| `payments:dlq` | The dead-letter stream — poison messages plus `reason` and `attempt`. |
| `payments:processed` | Redis Set of successfully processed `payment_id`s — the idempotency guard. |
| `DLQBroker.read_new` | `XREADGROUP … >` — claims never-delivered messages. |
| `DLQBroker.read_pending` | `XREADGROUP … 0` — re-reads this consumer's own delivered-but-unacked backlog. This is what actually redelivers a failed payment; a `>` read never does. |
| `DLQBroker.is_processed` / `mark_processed` | `SISMEMBER` / `SADD` on the processed set. |
| `DLQBroker.move_to_dlq` | `XACK` off main **then** `XADD` to the DLQ, carrying the failure reason and attempt count. |
| `DLQBroker.read_dlq` / `ack_dlq` | Operator-side inspection of the dead-letter stream. |

---

## Step-by-step flow

**Step 1 — Idempotency check (before any side effect)**

```python
if await broker.is_processed(payment.payment_id):
    await broker.ack(group, msg_id)
    continue  # already charged by an earlier delivery or an earlier run
```

The check runs *first*, on every delivery. A replayed payment is skipped, not
re-charged.

**Step 2 — Handle, then record, then ack — in that order**

```python
await handler(consumer_id, payment)
await broker.mark_processed(payment.payment_id)
await broker.ack(group, msg_id)
```

The ack is last. If the consumer dies between the side effect and the ack, the
message is redelivered — and Step 1 catches it.

**Step 3 — On failure, spend one attempt**

```python
attempt = attempts.get(msg_id, 0) + 1
attempts[msg_id] = attempt
if attempt < max_attempts:
    continue  # leave UNACKED — read_pending() brings it back next pass
await broker.move_to_dlq(group, msg_id, payment, type(exc).__name__, attempt)
```

While budget remains the message is simply left unacked. Once the budget is
spent it is acked off `payments:main` and appended to `payments:dlq` with the
exception type as `reason` and the exhausted `attempt` count — so an operator
can see *why* before deciding to fix, replay, or discard.

**Step 4 — Inspect the dead letters**

```python
for msg_id, fields in await broker.read_dlq(group, "inspector-1", count=100):
    payment = Payment.from_fields(fields)  # plus fields["reason"], fields["attempt"]
```

---

## Delivery semantics & correctness contract

| Property | Guarantee |
|---|---|
| **Delivery** | **At-least-once.** The handler runs before the ack, so a crash in between causes a redelivery. Duplicates are expected, not exceptional. |
| **Effective processing** | Effectively-once *for the side effect*, because of the dedup guard — never exactly-once at the broker level. |
| **Dedup key** | **`payment_id`.** It is unique per payment and is the member recorded in the `payments:processed` Redis Set. Every handler in this pattern must key its idempotency off `payment_id`. |
| **Ordering** | None across consumers, and none under retry — a failed payment is re-read from the pending list after newer payments have already been processed. |
| **Retry budget** | `max_attempts` deliveries per message. Attempt `max_attempts` is terminal: DLQ. |
| **Terminal states** | Processed (acked + in the processed set), skipped as duplicate (acked), or dead-lettered (acked off main, present in `payments:dlq`). No message stays in-flight forever. |

### Failure matrix

| Failure | What the system does |
|---|---|
| Handler raises, budget remains | Message left unacked; `read_pending` redelivers it on a later pass. |
| Handler raises, budget exhausted | `XACK` main + `XADD` to `payments:dlq` with `reason` and `attempt`. Pipeline keeps moving. |
| Consumer crashes after the side effect, before the ack | Message stays on the pending list and is redelivered; the dedup guard skips it, so no double charge. |
| Payment republished after a successful run | `is_processed()` hits — acked and skipped. |
| Consumer restarts mid-retry | The processed set survives (it lives in Redis) — but the attempt counter does not. See the caveat below. |

### Caveat: the attempt counter is in-memory and per-run

`attempts` is a plain `dict[str, int]` local to `run_idempotent_consumer`. It is
**ephemeral and per-consumer-run** — correct for a single consumer for the
lifetime of one process, and nothing more:

- Restart the consumer and every message's retry budget resets to zero. A poison
  message can then be retried indefinitely across restarts and never reach the
  DLQ — exactly the failure this pattern exists to prevent.
- Run two consumers in the same group and each keeps its own private counter, so
  the effective budget is `max_attempts` × the number of consumers that happen to
  see the message.

**A real deployment must keep the counter in Redis**, not in process memory — so
that the budget is shared across consumers and survives a restart. Redis already
tracks a per-message delivery count in the pending entries list (`XPENDING`),
and an explicit counter (e.g. a `HINCRBY` keyed by `payment_id`, with a TTL) is
the other common choice. The in-memory dict here keeps the demo to one moving
part; do not copy it into production.

---

## Implementation overview

```
dlq_pattern/
├── models.py     # Payment dataclass; payment_id doubles as the idempotency key
├── broker.py     # redis.asyncio wrapper over payments:main + payments:dlq + the processed set
└── consumer.py   # dedupe → handle → ack loop, with the retry budget and the DLQ move
```

See [`models.py`](models.py), [`broker.py`](broker.py) and
[`consumer.py`](consumer.py).

### Key invariants

- **The idempotency check runs before the handler, on every delivery.** That is
  what makes at-least-once delivery safe for a side effect as unforgiving as a
  charge.
- **The ack is the last step of a success.** Handler → `mark_processed` → `ack`.
  Any crash inside that sequence degrades to a redelivery, which the guard
  absorbs.
- **A failing message is left unacked, never acked-and-dropped.** Redelivery
  comes from an explicit `read_pending` (`XREADGROUP … 0`); a `>` read only ever
  returns new messages.
- **`move_to_dlq` acks main before adding to the DLQ**, so a message is never
  simultaneously pending on main and present in the DLQ.
- **`ensure_all_groups` is idempotent** — `BUSYGROUP` is swallowed, so any number
  of consumers can call it on startup without coordination.
- **The attempt counter is in-memory and per-run** — see the caveat above.

---

## Running the demo

```bash
# start Redis
docker run -p 6379:6379 redis:7-alpine

# run the demo (6 payments — 4 normal, 2 malformed — then replay all 6)
uv --directory packages/backend run python scripts/run_dlq_pattern.py
```

Source: [`scripts/run_dlq_pattern.py`](../../../scripts/run_dlq_pattern.py)
(`max_attempts=2`).

Expected output (abridged):

```
=== Dead Letter Queue Demo: Payment Pipeline ===
--- Phase 1: publish 6 payments (4 normal, 2 malformed) ---
consumer=worker-1 joined idempotent pipeline (group=payment_workers)
[worker-1] charged PAY-001 — USD 9900 (cust-A)
consumer=worker-1 failed PAY-003 msg=1787731913832-2 attempt=1/2 — will redeliver: malformed amount: -1
[worker-1] charged PAY-004 — GBP 12000 (cust-D)
Moved payment PAY-003 → payments:dlq id=1787731913876-0 (reason=ValueError attempt=2)
consumer=worker-1 exhausted retries for PAY-003 msg=1787731913832-2 → DLQ
Phase 1 done: 4 processed, 2 in DLQ ['PAY-003', 'PAY-005']

--- Phase 2: replay every original payment to prove idempotency ---
Re-queued 6 payments onto payments:main
consumer=worker-2 skipped duplicate PAY-001 msg=1787731913898-0
consumer=worker-2 skipped duplicate PAY-002 msg=1787731913898-1
consumer=worker-2 failed PAY-005 msg=1787731913898-4 attempt=1/2 — will redeliver: malformed amount: -99
Moved payment PAY-005 → payments:dlq id=1787731913900-1 (reason=ValueError attempt=2)
Phase 2 done: 0 NEW charges (already-processed payments were skipped, not double-charged)
```

Phase 2 is the point of the demo: replaying all six payments produces **zero**
new charges. The four valid payments are skipped by the `payment_id` guard; the
two malformed ones burn their budget again and return to the DLQ.

---

## When to use it

| Situation | Use a DLQ? |
|---|---|
| Some messages can never succeed — malformed, unprocessable, permanently rejected | **Yes** — bound the retries and park them |
| A stalled poison message would block or starve the rest of the pipeline | **Yes** |
| You need an operator to see *what* failed and *why*, not just that throughput dropped | **Yes** — `reason` + `attempt` travel with the message |
| Failures are transient only (a brief network blip) | Retry with backoff may be enough — a DLQ is for the failures retries can't fix |
| Losing the message is genuinely acceptable | No — just ack and drop, and say so explicitly |

Pairs naturally with
[Competing Consumers](../competing_consumers_pattern/README.md): a perpetually
reclaimed poison message is exactly the failure a max-delivery count plus a
dead-letter stream removes.

---

## Trade-offs

**Pros**
- The pipeline never stalls on a single unprocessable message.
- Failures are preserved with their cause, not dropped — inspectable and
  replayable.
- The idempotency guard makes at-least-once delivery safe for non-idempotent
  side effects like a charge.

**Cons**
- At-least-once means duplicates are guaranteed to happen eventually; every
  handler must dedupe on `payment_id`.
- The DLQ is not self-clearing — it needs an owner, an alert on its depth, and a
  replay path, or it becomes a queue nobody reads.
- The processed set grows without bound here (no TTL) — a real deployment bounds
  it, e.g. a TTL keyed per payment.
- The retry budget is tracked in process memory, so it is neither shared nor
  durable (see the caveat above).
