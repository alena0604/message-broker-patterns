# Scatter-Gather Pattern

## The problem: one answer needs many sources

Some requests can't be answered by a single service. A flight search from New
York to Los Angeles needs live fares from *every* airline before it can show the
cheapest option. Asking each airline one after another is slow, and a single
airline being down or slow must not sink the whole search.

```
❌ Ask airlines one at a time

  User ──► AirlineA ──► AirlineB ──► AirlineC ──► combine
           (wait)       (wait)       (down… hang?)
```

## The solution: scatter the request, gather the replies

The Scatter-Gather pattern broadcasts one request to many recipients in
parallel, then aggregates whatever comes back into a single answer — bounded by
a timeout so a missing or slow recipient can never stall the result.

```
✓ Scatter once, gather in parallel

                    ┌──► BudgetJet ─┐
  User ──► scatter ─┼──► SkyHigh   ─┼──► gather (by correlation id, until
                    └──► Nimbus    ─┘     all replies OR timeout) ──► sort/cheapest
                         GhostAir ✗ (errors — ignored)
                         SlowAir  ⏳ (too slow — ignored)
```

Every reply carries the request's **correlation id**, so the aggregator routes
each quote back to the right search and two concurrent searches never
cross-contaminate.

This pattern needs neither persistence nor acknowledgement, so per
[ADR-0002](../../../../docs/adr/0002-use-redis-streams-for-broker-backed-patterns.md)
it uses **stdlib asyncio primitives** (an in-memory fan-out broker), not Redis.

---

## Two distribution strategies

Both are driven off the same in-memory broker; only *where* the scatter publishes
differs.

| Strategy | How scatter addresses recipients | Trade-off |
|---|---|---|
| **Recipient list** | Publishes to each recipient's **own** topic, from an explicit list the coordinator holds. | More control, tighter coupling — the coordinator must know every recipient. |
| **Publish-subscribe** | Publishes once to a **shared broadcast** topic; any subscribed airline answers. | Less coupling — new airlines join by subscribing, no coordinator change. |

```python
# recipient list — coordinator knows each airline
await coordinator.scatter_gather(request, DistributionStrategy.RECIPIENT_LIST,
                                 expected=5, timeout=0.2, recipients=airlines)

# publish-subscribe — coordinator just broadcasts
await coordinator.scatter_gather(request, DistributionStrategy.PUBLISH_SUBSCRIBE,
                                 expected=5, timeout=0.2)
```

---

## Partial failures are absorbed by the timeout

`gather` returns as soon as the expected number of matching quotes arrive **or**
the timeout expires — whichever comes first (`asyncio.timeout` on each receive,
against a fixed deadline). An airline that errors simply publishes nothing; an
airline that is too slow replies after the deadline and is ignored. Either way
the aggregator returns the quotes it *did* get — it never hangs and never raises
on a missing recipient.

---

## Implementation overview

```
scatter_gather_pattern/
├── models.py        # DistributionStrategy enum + SearchRequest / FlightQuote (frozen)
├── broker.py        # InMemoryTopicBroker: asyncio fan-out pub/sub (subscribe/publish/unsubscribe)
├── service.py       # AirlineService: subscribe → stub inventory lookup → publish quote
├── combining.py     # combining strategies: sort_by_price, cheapest, filter_departing_after
└── aggregator.py    # ScatterGatherCoordinator: scatter (both strategies) + gather (correlation + timeout)
```

### Key invariants

- **Correlation id isolates searches.** `gather` only accepts quotes whose
  correlation id matches the request; foreign quotes are discarded and never
  count toward the expected total.
- **Subscribe before you scatter.** `publish` only reaches queues that already
  exist, so recipients subscribe at construction and the coordinator subscribes
  its response queue before scattering.
- **The timeout is a hard deadline.** A partial result is a normal outcome, not
  an error — a slow/broken recipient can't extend it or block it.

---

## Running the demo

```bash
uv --directory packages/backend run python scripts/run_scatter_gather_pattern.py
```

Runs the flight search under both distribution strategies against 5 airlines —
3 healthy, 1 too slow, 1 offline — and logs the sorted quotes plus the cheapest
fare gathered before the deadline.

---

## When to use it

| Situation | Use scatter-gather? |
|---|---|
| One answer aggregates many independent sources | **Yes** |
| Sources are slow/unreliable and a partial answer is acceptable | **Yes** — the timeout bounds the wait |
| Recipients change often / you want loose coupling | **Yes** — use the publish-subscribe strategy |
| A single service can answer the whole request | No — just call it |
| Every recipient's reply is mandatory (no partial answers) | No — you need a different completion guarantee |

---

## Trade-offs

**Pros**
- Parallel fan-out — total latency is the slowest *accepted* reply, not the sum.
- Resilient — a missing recipient degrades the answer, it doesn't break it.
- Two coupling levels from one broker: broadcast (loose) or recipient list (tight).

**Cons**
- Answers can be **partial** — callers must handle fewer replies than recipients.
- Picking the timeout is a latency-vs-completeness trade-off.
- Correlation ids must be unique per in-flight request, or replies cross wires.
