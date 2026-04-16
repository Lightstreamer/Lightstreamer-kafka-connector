# Snapshot Cache — Detailed Design

> **Status:** Future iteration — not included in v1.
> **Prerequisite:** Base snapshot configuration (`snapshot.enable`, `snapshot.timeout.ms`) and `SubscriptionsHandler` snapshot infrastructure. Replaces `TopicSnapshotConsumer` entirely.
> **Parent document:** [SNAPSHOT_DESIGN.md](SNAPSHOT_DESIGN.md), Section 11.2.

---

## 1. Motivation

The v1 `TopicSnapshotConsumer` reads the topic from beginning on every subscription batch. For large compacted topics this is expensive (see [SNAPSHOT_CONSUMER_DESIGN.md](SNAPSHOT_CONSUMER_DESIGN.md), Pass Efficiency Analysis). The Snapshot Cache **replaces** `TopicSnapshotConsumer` with an **in-memory materialized view** of the compacted topic, making snapshot delivery an instant lookup and eliminating the multi-pass design along with its associated complexity (drain-exit race, `pendingItems` queue, per-topic consumer map, snapshot thread pool).

### Before (TopicSnapshotConsumer only)

```
Client subscribes to stock-[symbol=AAPL]
        │
        ▼
  TopicSnapshotConsumer
  seekToBeginning() → poll 1M records → find 1 match → endOfSnapshot()
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  Time: 80 seconds (1M keys, 12 partitions)
```

### After (with Snapshot Cache)

```
Client subscribes to stock-[symbol=AAPL]
        │
        ▼
  SnapshotCache.lookup("stock-[symbol=AAPL]")
  → HashMap get → found → sendSnapshotEvent() → endOfSnapshot()
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  Time: < 1 millisecond
```

---

## 2. Concept

The Snapshot Cache is a **continuously updated in-memory copy** of the compacted topic's latest state, keyed by canonical item name. It behaves like Kafka Streams' KTable or a Change Data Capture (CDC) materialized view.

```
Kafka Compacted Topic                    Snapshot Cache (in-memory)
┌──────────────────────┐                 ┌──────────────────────────────────┐
│ key=AAPL → {p:150}   │   ──────────►  │ "stock-[symbol=AAPL]" → {p:150} │
│ key=GOOG → {p:2800}  │   continuous   │ "stock-[symbol=GOOG]" → {p:2800}│
│ key=TSLA → {p:900}   │   consumption  │ "stock-[symbol=TSLA]" → {p:900} │
│ ...                   │                │ ...                              │
└──────────────────────┘                 └──────────────────────────────────┘
```

Key distinction from the v1 `TopicSnapshotConsumer` (which it replaces):
- **TopicSnapshotConsumer** (v1): Reads the topic on-demand, per subscription batch. Ephemeral. Multi-pass loop with drain-exit race.
- **Snapshot Cache**: Reads the topic once at startup, then stays in sync. Long-lived. Synchronous lookup on subscribe thread.

---

## 3. Architecture

### 3.1 Components

```
                        ┌──────────────────────────┐
                        │    SubscriptionsHandler   │
                        │                          │
                        │  subscribe(item) ────────┼──► cache.lookup(item)
                        │                          │         │
                        │                          │    found?
                        │                          │   ┌─yes──┴──no─┐
                        │                          │   │            │
                        │                          │   ▼            ▼
                        │                          │ instant     empty
                        │                          │ snapshot    snapshot
                        │                          │ delivery    (client catches
                        │                          │             up via realtime)
                        └──────────────────────────┘
                                    ▲
                                    │ cache ready notification
                        ┌──────────┴───────────────┐
                        │     SnapshotCache<K,V>    │
                        │                          │
                        │  ┌────────────────────┐  │
                        │  │  CacheConsumer      │  │     Kafka
                        │  │  (dedicated thread) │◄─┼──── Compacted
                        │  │  seekToBeginning()  │  │     Topic
                        │  │  poll continuously  │  │
                        │  └────────┬───────────┘  │
                        │           │              │
                        │           ▼              │
                        │  ┌────────────────────┐  │
                        │  │  ConcurrentHashMap  │  │
                        │  │  canonicalName →    │  │
                        │  │    fieldsMap        │  │
                        │  └────────────────────┘  │
                        │                          │
                        │  State: READY            │
                        └──────────────────────────┘
```

### 3.2 `SnapshotCache<K, V>`

```java
class SnapshotCache<K, V> {

    enum State { READY, FAILED, CLOSED }

    private final String topic;
    private final ConcurrentHashMap<String, Map<String, String>> cache;
    // canonicalItemName → latest fieldsMap

    private volatile State state;

    private final Consumer<byte[], byte[]> consumer;
    private final RecordMapper<K, V> recordMapper;

    /**
     * Loads the full compacted state from Kafka. Called synchronously during
     * adapter init() — blocks until all partitions are consumed up to their
     * end offsets. On success, state = READY. On failure, throws.
     */
    void load() { ... }

    /**
     * Lookup a snapshot for a subscribed item.
     * Returns Optional.empty() if item not found in cache.
     * Always succeeds when state == READY (guaranteed after init).
     */
    Optional<Map<String, String>> lookup(SubscribedItem item) {
        return Optional.ofNullable(cache.get(item.asCanonicalItemName()));
    }

    /** Start the tailing thread to keep the cache in sync. */
    void startSync() { ... }

    /** Shutdown the cache consumer and release resources. */
    void close() { ... }
}
```

No `LOADING` state, no `CountDownLatch`, no `awaitReady()`. By the time any `subscribe()` call arrives, `load()` has already completed successfully during `init()`.

### 3.3 CacheConsumer — Lifecycle

The cache consumer has two phases: Phase 1 (initial load, synchronous during `init()`) and Phase 2 (continuous tailing, background thread).

#### Phase 1: Initial Load (catch-up) — synchronous during `init()`

Phase 1 runs **synchronously** in `SnapshotCache.load()`, called from the adapter's `init()` method. Lightstreamer guarantees that no `subscribe()` calls arrive until `init()` returns, so the cache is fully populated before any snapshot lookup.

```
assign(allPartitions)
seekToBeginning(allPartitions)
endOffsets ← consumer.endOffsets(allPartitions)

while (!allPartitionsConsumed(endOffsets)):
    records = consumer.poll(timeout)
    for record in records:
        mapped = recordMapper.map(record)
        for canonicalName in mapped.canonicalItemNames():
            if record.value() == null:
                cache.remove(canonicalName)    // tombstone → delete
            else:
                cache.put(canonicalName, mapped.fieldsMap())
        trackOffset(record)

state = READY
lastRefreshOffset = currentPositions()   // remember where we stopped
lastRefreshTime = now()
```

After this phase, the cache holds the **complete compacted state** at the time `endOffsets` was captured. If Phase 1 fails (Kafka unreachable, authentication error, deserialization failure), `load()` throws → `init()` propagates the exception → adapter fails to start → Lightstreamer logs the error. **No degraded runtime state to manage.**

---

#### Phase 2: Continuous Tailing

After Phase 1 completes, a dedicated background thread continues polling from where `load()` left off to keep the cache in sync with Kafka in real time.

```
// Continue polling from where Phase 1 left off — no seek needed
while (!closed):
    records = consumer.poll(tailPollTimeout)
    for record in records:
        mapped = recordMapper.map(record)
        for canonicalName in mapped.canonicalItemNames():
            if record.value() == null:
                cache.remove(canonicalName)    // tombstone
            else:
                cache.put(canonicalName, mapped.fieldsMap())
```

**Properties:**

| Aspect | Value |
|---|---|
| Staleness | Poll interval (~100ms–5s) |
| Broker I/O | Continuous tailing read; near-zero when topic is idle |
| Resource usage | 1 dedicated thread + 1 TCP connection per topic, permanently |
| Snapshot delivery | Always sub-millisecond lookup — cache is always current |
| Implementation | Simple poll loop (~10 lines) |

#### Why Continuous Tailing Is Necessary

Without tailing, the cache freezes at startup time and becomes increasingly stale:

```
t0:   Cache loads. AAPL={price:150}. Consumer closed.
t50:  Kafka record produced: AAPL → {price:175}
t100: Client subscribes to stock-[symbol=AAPL]
      Cache delivers snapshot: {price:150}  ← stale by $25
      enableRealtimeEvents()
      Main consumer delivers from t100 onward...
      BUT: the {price:175} record at t50 is in a dead zone —
      too new for the cache, too old for the main consumer.
      The client NEVER sees {price:175} unless AAPL gets
      another update after t100.
```

Continuous tailing prevents this entirely — the cache sees the t50 record before any subscription arrives, so the lookup at t100 returns the current value.

---

## 4. Snapshot Delivery with Cache

### 4.1 Flow

Since the cache is guaranteed READY after `init()`, the subscribe path has no state checks or fallback logic:

```
subscribe(item):
    │
    ├── cached = cache.lookup(item)
    │       │
    │  found?
    │  ┌─yes──┴──no─┐
    │  │            │
    │  ▼            ▼
    │  sendSnapshot   (skip)
    │  Event(cached)
    │  │
    ├── endOfSnapshot()
    ├── enableRealtimeEvents()
    │
    ▼
 real-time events flow
```

### 4.2 Delivery Code

```java
void deliverSnapshot(SubscribedItem item, EventListener listener) {
    Optional<Map<String, String>> cached = snapshotCache.lookup(item);
    if (cached.isPresent()) {
        item.sendSnapshotEvent(cached.get(), listener);
    }
    // Always signal end of snapshot — empty snapshot is valid
    item.endOfSnapshot(listener);
    item.enableRealtimeEvents(listener);
}
```

**Key property:** This runs **synchronously** on the subscribe thread — no background consumer, no waiting, no state checks. Sub-millisecond delivery.

---

## 5. Memory Model

### 5.1 Estimation

Each cache entry stores:
- Canonical item name (String key): ~50–200 bytes
- Fields map (Map<String, String>): ~100–2000 bytes depending on schema

| Topic unique keys | Avg entry size | Cache memory |
|---|---|---|
| 10K | 500 bytes | ~5 MB |
| 100K | 500 bytes | ~50 MB |
| 1M | 500 bytes | ~500 MB |
| 10M | 500 bytes | ~5 GB |

**ConcurrentHashMap overhead** adds ~30-50% on top due to node objects, hash buckets, and padding.

### 5.2 Memory Controls

| Control | Parameter | Description |
|---|---|---|
| **Max entries** | `snapshot.cache.max.entries` | Evict oldest entries (LRU) when exceeded |
| **Max memory** | `snapshot.cache.max.memory.mb` | Approximate memory cap; triggers eviction |
| **Disable for topic** | `snapshot.cache.enable` | Opt-in per adapter; falls back to `TopicSnapshotConsumer` |

#### Eviction Policy

When the cache exceeds its limit, entries must be evicted. Options:

| Policy | Behavior | Pros | Cons |
|---|---|---|---|
| **LRU** | Evict least-recently-looked-up entry | Keeps hot data | Requires access tracking |
| **Random** | Evict random entry | Simplest | May evict hot data |
| **None (reject)** | Stop caching new entries; existing entries stay | Predictable | Cache becomes stale for new keys |

> Recommendation: **None (reject)** for simplicity — if the topic exceeds the entry limit, new keys are not cached but existing ones remain valid. Items not in cache receive an empty snapshot and catch up via real-time updates.

### 5.3 Tombstone Handling

Kafka compacted topics use `null` values as tombstones to delete keys. The cache must handle these:

```java
if (record.value() == null) {
    cache.remove(canonicalName);
} else {
    cache.put(canonicalName, mapped.fieldsMap());
}
```

This ensures the cache accurately reflects deletions. A `lookup()` for a deleted key returns `Optional.empty()`, resulting in an empty snapshot.

---

## 6. Consistency

### 6.1 Staleness Window

The cache staleness is bounded by the tailing consumer's poll interval (~100ms–5s). Between a Kafka write and the next poll, the cache may serve a slightly older value.

This is **acceptable** because:

1. The real-time main consumer will immediately push the latest update after snapshot completes.
2. Compacted topics are inherently "eventually consistent" — the snapshot represents a recent-enough state.

### 6.2 Consistency Between Cache and Real-time Stream

```
Timeline:
  t0: Cache has stock-[symbol=AAPL] → {price: 150}
  t1: New Kafka record: AAPL → {price: 155}
  t2: Client subscribes to stock-[symbol=AAPL]
  t3: Cache delivers snapshot: {price: 150}   ← slightly stale (tailing hasn't polled t1 yet)
  t4: endOfSnapshot()
  t5: enableRealtimeEvents() → drains queue
  t6: Main consumer delivers: {price: 155}    ← client catches up
```

The client always converges to the latest value.

### 6.3 Multi-Item Consistency

If a client subscribes to multiple items (e.g., `stock-[symbol=AAPL]` and `stock-[symbol=GOOG]`), their snapshot values come from different points in time (the latest cache entry for each). There is **no cross-item transactional consistency** — but this is inherent to Kafka compacted topics, which offer per-key guarantees only.

---

## 7. Cache Lifecycle

### 7.1 Startup

```
Adapter init()
   │
   ├── snapshot.cache.enable == true?
   │      │
   │     yes ──► Create SnapshotCache per topic
   │      │        │
   │      │        ├── snapshotCache.load()   [SYNCHRONOUS]
   │      │        │    seekToBeginning → endOffsets → poll all records
   │      │        │    On failure: throws → init() fails → adapter does not start
   │      │        │
   │      │        ├── State = READY (cache fully populated)
   │      │        │
   │      │        └── snapshotCache.startSync()
   │      │              │
   │      │              └─► dedicated thread starts tailing from current position
   │      │
   │     no ──► Use TopicSnapshotConsumer (v1 behavior)
   │
   ▼
 Adapter ready for subscriptions (cache guaranteed READY)
```

**Startup latency:** Phase 1 adds to `init()` time. For a 1M-key topic (~80s), this is a one-time cost before any client connects. The adapter operator expects startup to involve Kafka connectivity, and Lightstreamer logs adapter init progress. This single read serves every subsequent subscription instantly, forever.

### 7.2 Shutdown

```
Adapter shutdown / unsubscribe all
   │
   ├── snapshotCache.close()
   │      │
   │      ├── Set closed = true
   │      ├── consumer.wakeup()
   │      ├── cacheThread.join()
   │      ├── consumer.close()
   │      └── cache.clear()
   │
   ▼
 Resources released
```

### 7.3 Failure Recovery

**Phase 1 failure (during `init()`):** `load()` throws → `init()` propagates the exception → adapter fails to start. Clean failure — no degraded runtime state. The operator sees the error in Lightstreamer logs and fixes the Kafka connectivity issue before retrying.

**Sync failure (after `init()`, during tailing):**

```
CacheConsumer tailing thread:
  catch (KafkaException e):
      state = FAILED
      log.error("Snapshot cache tailing failed, snapshots will be stale", e)
```

When `state == FAILED` during sync, the cache still contains valid data from Phase 1 (and any successful sync updates). `lookup()` continues to return cached entries — they may be stale, but the client converges to the correct value via real-time updates from the main consumer. **No subscription is lost or blocked.**

---

## 8. Interaction with Existing Components

### 8.1 Integration Point: `SubscriptionsHandler`

`SubscriptionsHandler` was refactored into a flat `DefaultSubscriptionsHandler implements SubscriptionsHandler` (see [SNAPSHOT_CONSUMER_DESIGN.md](SNAPSHOT_CONSUMER_DESIGN.md), Section 5). The cache **replaces** the `TopicSnapshotConsumer` infrastructure entirely. The `enqueueForSnapshot()` method is replaced by a synchronous cache lookup:

```java
// In DefaultSubscriptionsHandler — replaces enqueueForSnapshot():

void deliverSnapshot(SubscribedItem item) {
    Optional<Map<String, String>> cached = snapshotCache.lookup(item);
    if (cached.isPresent()) {
        item.sendSnapshotEvent(cached.get(), eventListener);
    }
    // Always finalize — empty snapshot is valid
    item.endOfSnapshot(eventListener);
    item.enableRealtimeEvents(eventListener);
}
```

No state checks, no blocking, no fallback. The cache is guaranteed READY by the time any `subscribe()` call arrives (Phase 1 completed synchronously during `init()`).

**What this eliminates from `SubscriptionsHandler`:**

- `snapshotConsumers` (`ConcurrentHashMap<String, TopicSnapshotConsumer>`) — no per-topic consumer map
- `pendingSnapshotTopics` (`ConcurrentHashMap<SubscribedItem, AtomicInteger>`) — no per-item topic countdown
- `snapshotPool` (`ExecutorService`) — no snapshot thread pool
- `onItemSnapshotComplete()` callback — no multi-topic finalization coordination
- `shutdownSnapshotConsumers()` — no wakeup-based shutdown
- The drain-exit race documented in [SNAPSHOT_CONSUMER_DESIGN.md](SNAPSHOT_CONSUMER_DESIGN.md), Section 7 — no `pendingItems` queue, no multi-pass loop, no `computeIfAbsent`

The entire snapshot path becomes a synchronous `HashMap` lookup on the subscribe thread.

### 8.2 Interaction with Main Consumer

The cache consumer and the main consumer are **completely independent**:

| Aspect | Main Consumer | Cache Consumer |
|---|---|---|
| Consumer group | Configured `group.id` | None (`assign()`) |
| Offset management | Managed (committed) | Internal tracking only |
| Purpose | Real-time updates | Snapshot state maintenance |
| Lifecycle | Starts/stops with subscriptions | Starts with adapter, runs continuously |
| Thread | Shared poll thread | Dedicated tailing thread |

### 8.3 Relationship to `TopicSnapshotConsumer`

The cache is a **standalone replacement** for `TopicSnapshotConsumer`, not a layer on top of it. When the cache is enabled:

- `TopicSnapshotConsumer` is not instantiated
- No `snapshotConsumers` map, no `snapshotPool`, no `pendingSnapshotTopics`
- The entire multi-pass coordination model is removed

The v1 `TopicSnapshotConsumer` remains available as the snapshot delivery mechanism when the cache is **not enabled** (i.e., `snapshot.cache.enable=false`, which is the default). The two approaches are mutually exclusive at configuration time, not runtime fallback alternatives.

---

## 9. Configuration

| Parameter | Key | Type | Default | Description |
|---|---|---|---|---|
| Cache Enable | `snapshot.cache.enable` | `BOOL` | `false` | Enable the in-memory snapshot cache (replaces `TopicSnapshotConsumer`) |
| Max Entries | `snapshot.cache.max.entries` | `NON_NEGATIVE_INT` | `0` (unlimited) | Maximum number of entries in the cache. `0` means no limit |

---

## 10. Trade-off Summary

| Dimension | TopicSnapshotConsumer (v1) | Snapshot Cache |
|---|---|---|
| **Snapshot latency** | Seconds to minutes | Sub-millisecond (always) |
| **Broker I/O per snapshot** | Full topic read per pass | Zero |
| **Memory usage** | Minimal | Proportional to topic size |
| **Continuous broker I/O** | None | Tailing polls (~0 when idle) |
| **Dedicated threads** | 1 per topic (ephemeral) | 1 per topic (permanent) |
| **TCP connections** | Ephemeral | 1 permanent per topic |
| **Staleness** | None | Poll interval (~100ms–5s) |
| **Complexity** | Low–Medium | Low (simpler than v1) |
| **Failure mode** | Per-subscription (each pass independent) | Stale cache (still serves data from Phase 1) |
| **Drain-exit race** | Present (see [SNAPSHOT_CONSUMER_DESIGN.md](SNAPSHOT_CONSUMER_DESIGN.md#7-known-limitation-drain-exit-race)) | Eliminated |
| **Subscribe-path concurrency** | Multi-threaded (snapshot pool) | Synchronous on subscribe thread |
| **Startup cost** | None (on-demand) | Phase 1 in `init()`: full topic read (blocking) |
| **LOADING state handling** | N/A | None needed — `init()` guarantees READY |

### When to Use

The v1 `TopicSnapshotConsumer` multi-pass design has a self-regulating property (see [SNAPSHOT_CONSUMER_DESIGN.md](SNAPSHOT_CONSUMER_DESIGN.md), Pass Efficiency Analysis): large topics absorb bursts naturally (few passes), and small topics are fast enough that many passes are negligible. The **medium-topic regime** (10K–100K keys, pass duration ≈ inter-arrival time) is where the cache provides the most value — it eliminates repeated reads entirely.

The choice between `TopicSnapshotConsumer` (v1) and the cache is made at **configuration time** via `snapshot.cache.enable`. They are mutually exclusive, not runtime fallback alternatives.

| Scenario | Recommended Approach |
|---|---|
| Small compacted topics (< 10K keys) | `TopicSnapshotConsumer` (v1) — passes are sub-millisecond, many passes are negligible; no memory overhead |
| Medium topics (10K–100K keys), subscription bursts | Snapshot Cache — eliminates the problematic regime where pass duration ≈ inter-arrival time; also eliminates the drain-exit race |
| Large topics (100K–1M keys), frequent subscriptions | Snapshot Cache — always current, instant lookups |
| Very large topics (> 1M keys) with limited memory | `TopicSnapshotConsumer` (v1) with timeout — cache would consume too much memory |
