# Claim Check Pattern

## The problem: big payloads don't belong in the broker

Message brokers are built for lots of small messages, and almost all of them
cap message size (Kafka defaults to 1 MB, SQS to 256 KB, RabbitMQ degrades
badly on large frames). Push a 5 MB video or a 20 MB PDF through the broker and
you get rejected messages, blown-out memory, and a broker doing a file server's
job — slowly.

```
❌ Payload travels through the broker

  Producer ──[ 5 MB video ]──► [ BROKER ]──[ 5 MB video ]──► Consumer
                                   ▲
                        over the size limit / memory blowup
```

## The solution: check the payload into storage, send only the claim

Like a coat check at a theatre: you hand over the heavy coat, get a small
numbered ticket, and carry only the ticket. The Claim Check pattern stores the
large payload in **external storage**, then publishes a lightweight **claim
check** — just the storage key plus a little metadata — through the broker. The
consumer redeems the claim check against storage to fetch the real payload.

```
✓ Only the claim check travels through the broker

                  ┌─────────────── external storage ───────────────┐
                  │  claim_id.bin  ◄── put(bytes) ── retrieve ──►    │
                  └────────▲───────────────────────────┬────────────┘
                           │ store                      │ fetch
   Producer ── payload ───►│                            │
                           └── ClaimCheck ─► [ BROKER ] ─► Consumer ─► handler
                                (tiny: key + metadata)         │
                                                               └─► optional delete
```

This implementation uses a stdlib **`asyncio.Queue`** as the broker (a claim
check is tiny — no real broker needed) and a **filesystem-backed store** built on
`pathlib` standing in for something like S3 or a shared file server.

---

## How it maps to the components

| Component | Role in the pattern |
|---|---|
| `FilesystemPayloadStore.put(bytes)` | Write the large payload to storage; mint and return the `claim_id`. |
| `ClaimCheck` | The lightweight token published on the broker — storage key + metadata, never bytes. |
| `ClaimCheckBroker.publish/get` | `asyncio.Queue` carrying claim checks only. |
| `FilesystemPayloadStore.get(claim_id)` | Consumer redeems the claim to fetch the real payload back. |
| `FilesystemPayloadStore.delete(claim_id)` | Optional cleanup after processing — configurable, never forced. |

---

## Implementation overview

```
claim_check_pattern/
├── models.py     # Payload (heavy: bytes + metadata) + ClaimCheck (light: key + metadata)
├── storage.py    # FilesystemPayloadStore: put / get / delete / exists, keyed by claim id
├── broker.py     # asyncio.Queue wrapper carrying claim checks only
├── producer.py   # store payload → wrap key in ClaimCheck → publish the claim
└── consumer.py   # redeem claim → fetch payload → handle → optionally delete
```

### Key invariants

- **The payload never touches the broker.** `publish()` writes bytes to storage
  and puts only the `ClaimCheck` on the queue.
- **The store mints the claim id.** It is the single authority for the storage
  token; the producer just wraps it with metadata.
- **Deletion is configurable, never forced.** `run_consumer(delete_after=...)`
  defaults to `True` (claim-check storage is usually temporary staging worth
  reclaiming), but a consumer that needs to keep the payload — multiple readers,
  audit, reprocessing — passes `delete_after=False`.

---

## Running the demo

```bash
# no broker or storage service to start — stdlib asyncio.Queue + a temp dir
uv --directory packages/backend run python scripts/run_claim_check_pattern.py
```

Expected output (abridged):

```
=== Claim Check Demo: Large Media Pipeline ===
--- Producer: checking in 3 large payloads ---
Published claim=... for hero-banner.png (2000000 bytes) — broker carried the claim, not the bytes
--- Consumer: redeeming claims and reclaiming storage ---
[consumer-0] processed hero-banner.png — image/png, 2000000 bytes fetched from storage (claim=...)
=== Results ===
Processed 3 payload(s)
Storage reclaimed: 3/3 payloads deleted after processing (remaining: none)
```

---

## When to use it

| Situation | Use a claim check? |
|---|---|
| Payloads exceed (or approach) the broker's message-size limit | **Yes** |
| Large blobs — media, documents, ML artifacts, DB dumps | **Yes** |
| Consumers already have access to shared storage | **Yes** |
| Messages are small (a few KB) | No — the extra round-trip to storage isn't worth it |
| Storage is unavailable / consumers can't reach it | No — the claim would be unredeemable |

---

## Trade-offs

**Pros**
- The broker stays fast and within its limits — it only ever carries tiny claims.
- Payload size is bounded by storage, not the broker.
- Storage and broker scale independently.

**Cons**
- Two systems to operate (broker **and** storage) and a two-step flow to reason about.
- Lifecycle management: orphaned payloads pile up if claims are lost — hence the
  configurable cleanup, plus a real deployment usually adds storage TTLs.
- An extra storage round-trip per message adds latency for small payloads.
