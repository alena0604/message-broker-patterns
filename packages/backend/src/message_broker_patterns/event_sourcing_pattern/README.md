# Event Sourcing with CQRS

## The problem: state-only storage forgets how it got there

Most systems store only the **current** state. An account row says `balance = $500` — and that's all you have.

```
❌ State-only storage

  accounts table
  ┌────────────┬─────────┐
  │ account_id │ balance │
  ├────────────┼─────────┤
  │   acc-1    │  $500   │   ← how did we get to $500? unknown.
  └────────────┴─────────┘
```

Was it one $500 deposit? A $1000 deposit and a $500 withdrawal? A dozen transactions? The history is gone. You can't audit it, you can't answer "what was the balance last Tuesday," you can't rebuild a corrupted read model, and you can't derive a new view (e.g. monthly statements) you didn't think of up front.

---

## The solution: store the events, derive the state

Event sourcing stores every state change as an **immutable event**. Current state is a *fold* over the event history, computed on demand.

```
✓ Event-sourced storage

  account:acc-1:events  (append-only stream)
  ┌──────────────────────────────────────────┐
  │ AccountCreated   owner=demo-user          │
  │ MoneyDeposited   amount=$1000             │
  │ MoneyWithdrawn   amount=$500              │
  └──────────────────────────────────────────┘
            │
            └─► replay (fold) ─► balance = 0 + 1000 - 500 = $500
```

The full history is the source of truth. The $500 is just one projection of it — and because the events are kept, you can rebuild that projection at any time, or build entirely new ones.

**CQRS** (Command Query Responsibility Segregation) splits this into two independent sides:

- **Write side** — the *aggregate* (`BankAccount`). Validates commands (`create`/`deposit`/`withdraw`), enforces invariants (no overdraft, no negative amounts), and **emits events**. It never serves queries.
- **Read side** — the *projector*. Reads the event stream and folds it into a query-optimized **read model** (`AccountSummary`). It never validates commands and never imports the aggregate — it depends only on the event types.

---

## Flow diagram

```
  ┌─────────────────────────── WRITE SIDE ───────────────────────────┐
  │                                                                   │
  │   account = BankAccount(account_id="acc-1")                       │
  │   account.create(owner_id="demo-user") ─► AccountCreated          │
  │   account.deposit(1000)                ─► MoneyDeposited          │
  │   account.withdraw(500)                ─► MoneyWithdrawn          │
  │        │  (aggregate validates, then produces an event)           │
  │        │                                                          │
  │        └─► EventStore.append(account_id, event) ── XADD ──┐       │
  └───────────────────────────────────────────────────────────┼──────┘
                                                               │
                                          ┌────────────────────▼────────────────────┐
                                          │   Redis Stream (one per aggregate)        │
                                          │   "account:acc-1:events"                  │
                                          │                                           │
                                          │   1781707417466-0  AccountCreated         │
                                          │   1781707417467-0  MoneyDeposited 1000    │
                                          │   1781707417467-1  MoneyWithdrawn  500     │
                                          └────────────────────┬────────────────────┘
                                                               │ XREAD (poll, cursor)
  ┌─────────────────────────── READ SIDE ────────────────────┼───────────────────────┐
  │                                                            │                       │
  │   project(store, "acc-1", ...) ◄───────────────────────────┘                       │
  │        │  decode_event(raw) ─► fold into AccountSummary                            │
  │        ▼                                                                           │
  │   AccountSummary(account_id="acc-1", owner_id="demo-user",                         │
  │                  balance=500, version=3, last_updated=...)                         │
  └───────────────────────────────────────────────────────────────────────────────────┘
```

---

## Step-by-step flow

**Step 1 — Command validated on the write side**

The aggregate checks invariants *before* producing an event. A withdrawal larger than the balance raises `InsufficientFundsError` and **no event is emitted** — invalid history can never be written.

```python
account = BankAccount(account_id="acc-1")
account.create(owner_id="demo-user")   # → AccountCreated
account.deposit(1000)                   # → MoneyDeposited(amount="1000")
account.withdraw(500)                   # → MoneyWithdrawn(amount="500")
```

**Step 2 — Append the event to the stream**

Each produced event is appended (`XADD`) to that account's own stream. The store is append-only — events are never updated or deleted.

```python
await store.append(account_id, event.event_type, event.to_dict())
# → XADD account:acc-1:events * event_type MoneyDeposited account_id acc-1 amount 1000
```

**Step 3 — Rebuild state by replaying**

A fresh aggregate is reconstructed by folding the decoded events from genesis. The same `_apply` step the command methods use is reused here — no duplicated folding logic.

```python
raw_events = await store.read(account_id)            # full history from "0"
replayed = BankAccount.replay(decode_event(raw) for _, raw in raw_events)
assert replayed.balance == 500
```

**Step 4 — Project an independent read model**

The projector polls the stream, decoding and folding events into an `AccountSummary`. It catches up when its `version` reaches the `expected_version`, then stops — mirroring the terminal-state check in the Saga runner. It never touches `BankAccount`.

```python
stop = asyncio.Event()
summary = await project(store, account_id, stop, expected_version=3)
assert summary.balance == 500
```

---

## One stream per aggregate

Unlike `saga_pattern` (`saga:orders`, …) and `outbox_pattern` (`orders:events`), which use **one shared stream per pattern**, event sourcing uses **one stream per aggregate instance**: `account:{account_id}:events`.

Replay and incremental projection are inherently per-aggregate operations: rebuilding `acc-1` must read *only* `acc-1`'s events, in order, with nothing interleaved. A shared stream would force O(all events) reads plus filtering to reconstruct a single aggregate, and would make per-aggregate truncation impossible. See [ADR-0003](../../../../docs/adr/0003-one-stream-per-aggregate-for-event-sourcing.md) for the full rationale and the production trade-off (unbounded stream count → needs an archival policy).

---

## Implementation overview

```
event_sourcing_pattern/
├── events.py      # frozen events (AccountCreated/MoneyDeposited/MoneyWithdrawn),
│                  #   account_stream() helper, decode_event() dispatch
├── store.py       # EventStore: append() → XADD, read() → XREAD (full or incremental)
├── aggregate.py   # BankAccount write side — pure/sync, validates + produces events,
│                  #   replay() folds history; InsufficientFundsError
└── projector.py   # async read side: project() folds the stream into AccountSummary
```

### Key invariants

- The aggregate is **pure and synchronous** — no Redis, no asyncio. I/O is the caller's job (`EventStore.append`). This keeps the business rules trivially unit-testable.
- A single `_apply` step is the only place state transitions live; both the command methods and `replay()` go through it, so live state and replayed state can never diverge.
- The projector depends **only** on decoded event types — never on `BankAccount` — so the read side is genuinely independent of the write side.
- Invalid commands raise before any event is appended: the stream length is unchanged after an `InsufficientFundsError`.
- All datetimes are UTC-aware (`datetime.now(UTC)`).

---

## Running the demo

```bash
# start Redis
docker run -p 6379:6379 redis:7-alpine

# run the demo (happy path + failure path)
uv --directory packages/backend run python scripts/run_event_sourcing_pattern.py
```

The happy path proves the core guarantee — three independently-computed balances agree:

```
============================================================
  HAPPY PATH — create, deposit $1000, withdraw $500
  account_id: ...
============================================================

(a) in-memory BankAccount balance after commands: 500
(b) replayed BankAccount balance (read from store): 500
(c) projector AccountSummary balance:               500

  PROOF: (a) == (b) == (c)?  True  (500 == 500 == 500)

Event stream 'account:...:events':
  ...-0  AccountCreated  amount=-
  ...-0  MoneyDeposited  amount=1000
  ...-1  MoneyWithdrawn  amount=500
```

The failure path shows that an overdraft raises `InsufficientFundsError` and leaves the stream untouched (no spurious event).

---

## Trade-offs

**Pros**
- Full audit trail — every state change is preserved and replayable.
- Rebuild any read model from history — derive new views you didn't plan for, or recover a corrupted projection.
- Clean separation — the write side enforces rules; the read side optimizes for queries; they evolve independently.
- Temporal queries — "what was the balance at version N / time T" is just a partial fold.

**Cons**
- Reads require a fold (or a maintained projection) — current state isn't a single row lookup. Snapshots mitigate long histories.
- Eventual consistency — the read model lags the write side by the projection latency (configurable `poll_interval`).
- Unbounded streams — one per aggregate needs a cleanup/archival policy in production (see ADR-0003).
- Schema evolution — old events are immutable, so new event versions must stay backward-compatible with `decode_event`.
