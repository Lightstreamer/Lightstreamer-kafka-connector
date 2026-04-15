# Snapshot Cache — Detailed Design

> **Status:** Future iteration — not included in v1.
> **Prerequisite:** Base snapshot support (`TopicSnapshotConsumer` with partition pruning and timeout).
> **Parent document:** [SNAPSHOT_DESIGN.md](SNAPSHOT_DESIGN.md), Section 7.3 Mitigation D.

---

## 1. Motivation

The `TopicSnapshotConsumer` reads the topic from beginning on every subscription (or batch of subscriptions). For large compacted topics this is expensive. The Snapshot Cache eliminates repeated reads by maintaining an **in-memory materialized view** of the compacted topic, making snapshot delivery an instant lookup.

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

Key distinction from the base `TopicSnapshotConsumer`:
- **TopicSnapshotConsumer**: Reads the topic on-demand, per subscription batch. Ephemeral.
- **Snapshot Cache**: Reads the topic once at startup, then stays in sync via continuous consumption. Long-lived.

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
                        │                          │ instant    fallback to
                        │                          │ snapshot   TopicSnapshot
                        │                          │ delivery   Consumer
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
                        │  State: LOADING │ READY  │
                        └──────────────────────────┘
```

### 3.2 `SnapshotCache<K, V>`

```java
class SnapshotCache<K, V> {

    enum State { LOADING, READY, FAILED, CLOSED }

    private final String topic;
    private final ConcurrentHashMap<String, Map<String, String>> cache;
    // canonicalItemName → latest fieldsMap

    private volatile State state = State.LOADING;
    private final CountDownLatch readyLatch = new CountDownLatch(1);

    private final Consumer<byte[], byte[]> consumer;
    private final RecordMapper<K, V> recordMapper;
    private final Thread cacheThread;

    /**
     * Lookup a snapshot for a subscribed item.
     * Returns Optional.empty() if cache is not ready or item not found.
     */
    Optional<Map<String, String>> lookup(SubscribedItem item) {
        if (state != State.READY) {
            return Optional.empty();
        }
        return Optional.ofNullable(cache.get(item.asCanonicalItemName()));
    }

    /**
     * Block until the initial load is complete (or timeout).
     */
    boolean awaitReady(Duration timeout) throws InterruptedException {
        return readyLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Start the cache consumer thread. */
    void start() { ... }

    /** Shutdown the cache consumer and release resources. */
    void close() { ... }
}
```

### 3.3 CacheConsumer — Lifecycle

The cache consumer always starts with Phase 1 (initial load). After that, two **sync strategies** determine how the cache stays current.

#### Phase 1: Initial Load (catch-up) — common to both strategies

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
readyLatch.countDown()
lastRefreshOffset = currentPositions()   // remember where we stopped
lastRefreshTime = now()
```

After this phase, the cache holds the **complete compacted state** at the time `endOffsets` was captured.

---

#### Sync Strategy A: Continuous Tailing

A dedicated consumer thread keeps polling after Phase 1 to stay in sync with Kafka in real time.

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
| Best for | High-write-throughput topics; many subscriptions over time |

---

#### Sync Strategy B: On-Demand Refresh

No continuous thread. The cache consumer is **dormant** after Phase 1. When a subscription arrives, the cache checks its freshness and performs an incremental catch-up **only if needed**.

```
lookup(item):
    age = now() - lastRefreshTime

    if age < freshnessThreshold:
        // Cache is fresh enough — serve directly
        return cache.get(item.asCanonicalItemName())

    // Cache is stale — incremental catch-up before serving
    synchronized (refreshLock):
        // Double-check: another thread may have already refreshed
        if (now() - lastRefreshTime < freshnessThreshold):
            return cache.get(item.asCanonicalItemName())

        // Resume from last known offset → current end offsets
        newEndOffsets = consumer.endOffsets(allPartitions)
        while (!allPartitionsConsumed(newEndOffsets)):
            records = consumer.poll(timeout)
            for record in records:
                // same update logic as Phase 1
                ...
        lastRefreshTime = now()

    return cache.get(item.asCanonicalItemName())
```

**Properties:**

| Aspect | Value |
|---|---|
| Staleness | Up to `freshnessThreshold`; zero after refresh |
| Broker I/O | Zero when idle; incremental read on subscribe (only new records since last refresh) |
| Resource usage | No dedicated thread; consumer kept open but idle |
| Snapshot delivery | Sub-millisecond if fresh; milliseconds–seconds if refresh needed |
| Best for | Low-subscription-rate topics; resource-constrained environments |

**Incremental cost depends on write rate:**

| Topic write rate | Records since last refresh (30s threshold) | Refresh time |
|---|---|---|
| 1 msg/s | ~30 records | < 1ms |
| 100 msg/s | ~3,000 records | ~10ms |
| 10,000 msg/s | ~300,000 records | ~seconds |

For high-throughput topics the incremental catch-up approaches the cost of a full read, making continuous tailing the better choice. For quiet topics, on-demand refresh avoids the idle resource cost entirely.

---

#### Why the cache needs one of these strategies

Without any sync strategy, the cache freezes at startup time and becomes increasingly stale:

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

Both strategies close this gap — tailing prevents it entirely, on-demand refresh closes it at lookup time.

---

#### Strategy Comparison

| Dimension | Continuous Tailing | On-Demand Refresh |
|---|---|---|
| **Thread usage** | 1 permanent thread per topic | 0 (refresh on calling thread) |
| **TCP connections** | 1 permanent per topic | 1 kept open but idle |
| **Staleness window** | ~poll interval (100ms–5s) | 0 at lookup time (blocks to catch up) |
| **Lookup latency** | Always sub-millisecond | Sub-millisecond if fresh; variable if stale |
| **Broker I/O when idle** | Periodic empty polls | Zero |
| **Broker I/O on subscribe** | Zero | Incremental catch-up |
| **Implementation complexity** | Low (poll loop) | Medium (synchronization, incremental seek) |
| **Risk** | Idle resource waste on quiet topics | Slow lookup on high-throughput topics |

> **Recommendation:** Default to **on-demand refresh** for simplicity and lower resource usage. Switch to **continuous tailing** (via configuration) for high-write-throughput topics where lookup latency must be guaranteed sub-millisecond.

---

## 4. Snapshot Delivery with Cache

### 4.1 Flow

```
subscribe(item):
    │
    ├── cache.state == READY?
    │       │
    │      yes ──► cached = cache.lookup(item)
    │       │          │
    │       │     found?
    │       │    ┌─yes──┴──no─┐
    │       │    │            │
    │       │    ▼            ▼
    │       │  sendSnapshot   endOfSnapshot()
    │       │  Event(cached)  (empty snapshot)
    │       │  endOfSnapshot  enableRealtime
    │       │  enableRealtime Events()
    │       │  Events()
    │       │
    │      no ──► fallback to TopicSnapshotConsumer
    │              (base v1 behavior)
    │
    ▼
 real-time events flow
```

### 4.2 Delivery Code

```java
void deliverSnapshotFromCache(SubscribedItem item, EventListener listener) {
    Optional<Map<String, String>> cached = snapshotCache.lookup(item);
    if (cached.isPresent()) {
        item.sendSnapshotEvent(cached.get(), listener);
    }
    // Always signal end of snapshot — empty snapshot is valid
    item.endOfSnapshot(listener);
    item.enableRealtimeEvents(listener);
}
```

**Key property:** This runs **synchronously** on the subscribe thread — no background consumer, no waiting. Sub-millisecond delivery.

### 4.3 Race Condition: Subscribe During Phase 1 (LOADING)

If a client subscribes before the cache finishes its initial load:

| Strategy | Behavior |
|---|---|
| **Block** | `awaitReady(timeout)` — subscribe thread blocks until cache is ready or timeout |
| **Fallback** | Use `TopicSnapshotConsumer` for items that arrive during LOADING |
| **Hybrid** | Block for a short window (e.g., 5s); if not ready, fallback |

> Recommendation: **Hybrid** — brief blocking with fallback. Avoids complexity of running both paths permanently while being responsive.

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

> Recommendation: **None (reject)** for simplicity — if the topic exceeds the entry limit, new keys are not cached but existing ones remain valid. Items not in cache fall back to `TopicSnapshotConsumer`.

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

The staleness depends on the chosen sync strategy:

| Strategy | Staleness | When stale data is possible |
|---|---|---|
| **Continuous tailing** | Consumer poll interval (~100ms–5s) | Between Kafka write and next poll |
| **On-demand refresh** | Zero at lookup time | Never — refresh blocks until caught up |

In both cases, staleness is **acceptable** because:

1. The real-time main consumer will immediately push the latest update after snapshot completes.
2. Compacted topics are inherently "eventually consistent" — the snapshot represents a recent-enough state.

### 6.2 Consistency Between Cache and Real-time Stream

**With continuous tailing** (slight staleness possible):

```
Timeline:
  t0: Cache has stock-[symbol=AAPL] → {price: 150}
  t1: New Kafka record: AAPL → {price: 155}
  t2: Client subscribes to stock-[symbol=AAPL]
  t3: Cache delivers snapshot: {price: 150}   ← slightly stale
  t4: endOfSnapshot()
  t5: enableRealtimeEvents() → drains queue
  t6: Main consumer delivers: {price: 155}    ← client catches up
```

**With on-demand refresh** (no staleness):

```
Timeline:
  t0: Cache has stock-[symbol=AAPL] → {price: 150}  (from initial load)
  t1: New Kafka record: AAPL → {price: 155}
  t2: Client subscribes to stock-[symbol=AAPL]
  t3: On-demand refresh: reads records since last refresh → cache updated to {price: 155}
  t4: Cache delivers snapshot: {price: 155}   ← current
  t5: endOfSnapshot()
  t6: enableRealtimeEvents() → drains queue
  t7: Direct real-time dispatch
```

In both cases, the client always converges to the latest value.

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
   │      │        ├── Start CacheConsumer thread
   │      │        ├── Phase 1: initial load (seekToBeginning → endOffsets)
   │      │        ├── State = READY
   │      │        └── snapshot.cache.sync.strategy?
   │      │              │
   │      │           tailing ──► Phase 2: continuous tailing (dedicated thread)
   │      │              │
   │      │          on-demand ──► Consumer idle, refresh triggered by lookup()
   │      │
   │     no ──► Use TopicSnapshotConsumer (v1 behavior)
   │
   ▼
 Adapter ready for subscriptions
```

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

If the cache consumer encounters an unrecoverable error during Phase 1 or sync:

```
CacheConsumer thread (tailing) / lookup() thread (on-demand):
  catch (KafkaException e):
      state = FAILED
      readyLatch.countDown()  // unblock any awaitReady() callers
      log.error("Snapshot cache failed, falling back to TopicSnapshotConsumer", e)
```

When `state == FAILED`, all `lookup()` calls return `Optional.empty()`, causing the `SubscriptionsHandler` to fall back to the `TopicSnapshotConsumer` path transparently. **No subscription is lost or blocked.**

---

## 8. Interaction with Existing Components

### 8.1 Integration Point: `SubscriptionsHandler`

```java
// In SubscriptionsHandler.onSubscribedItem(item):

if (snapshotEnabled) {
    if (snapshotCache != null && snapshotCache.isReady()) {
        // Fast path: deliver from cache
        deliverSnapshotFromCache(item, eventListener);
    } else {
        // Slow path: use TopicSnapshotConsumer
        topicSnapshotConsumer.enqueue(item);
    }
}
```

The cache is an **optional accelerator** layered on top of the `TopicSnapshotConsumer`. The two paths are interchangeable from the client's perspective.

### 8.2 Interaction with Main Consumer

The cache consumer and the main consumer are **completely independent**:

| Aspect | Main Consumer | Cache Consumer (tailing) | Cache Consumer (on-demand) |
|---|---|---|---|
| Consumer group | Configured `group.id` | None (`assign()`) | None (`assign()`) |
| Offset management | Managed (committed) | Internal tracking only | Internal tracking only |
| Purpose | Real-time updates | Snapshot state maintenance | Snapshot state maintenance |
| Lifecycle | Starts/stops with subscriptions | Starts with adapter, runs continuously | Starts with adapter, refreshes on demand |
| Thread | Shared poll thread | Dedicated thread | Calling thread (subscribe path) |

### 8.3 Interaction with TopicSnapshotConsumer

When the cache is active and ready, `TopicSnapshotConsumer` is not used at all. When the cache is loading, failed, or disabled, `TopicSnapshotConsumer` handles snapshot delivery. The selection is per-subscription at lookup time.

---

## 9. Configuration

| Parameter | Key | Type | Default | Description |
|---|---|---|---|---|
| Cache Enable | `snapshot.cache.enable` | `BOOL` | `false` | Enable the in-memory snapshot cache |
| Sync Strategy | `snapshot.cache.sync.strategy` | `TEXT` | `on-demand` | `tailing` (continuous poll thread) or `on-demand` (incremental refresh at lookup) |
| Max Entries | `snapshot.cache.max.entries` | `NON_NEGATIVE_INT` | `0` (unlimited) | Maximum number of entries in the cache. `0` means no limit |
| Initial Load Timeout | `snapshot.cache.init.timeout.ms` | `POSITIVE_INT` | `60000` | Max time to wait for Phase 1 completion before accepting subscriptions |
| Freshness Threshold | `snapshot.cache.freshness.ms` | `POSITIVE_INT` | `30000` | On-demand strategy only: max age before a refresh is triggered on lookup |

---

## 10. Trade-off Summary

| Dimension | TopicSnapshotConsumer (v1) | Cache + Tailing | Cache + On-Demand Refresh |
|---|---|---|---|
| **Snapshot latency** | Seconds to minutes | Sub-millisecond (always) | Sub-millisecond if fresh; variable if stale |
| **Broker I/O per snapshot** | Full topic read per pass | Zero | Zero if fresh; incremental if stale |
| **Memory usage** | Minimal | Proportional to topic size | Proportional to topic size |
| **Continuous broker I/O** | None | Tailing polls (~0 when idle) | None |
| **Dedicated threads** | 1 per topic (ephemeral) | 1 per topic (permanent) | 0 |
| **TCP connections** | Ephemeral | 1 permanent per topic | 1 kept open but idle |
| **Staleness** | None | Poll interval (~100ms–5s) | None at lookup time |
| **Complexity** | Low–Medium | High | High |
| **Failure mode** | Per-subscription fallback | Global fallback | Global fallback |

### When to Use

| Scenario | Recommended Approach |
|---|---|
| Small compacted topics (< 100K keys) | `TopicSnapshotConsumer` with partition pruning — simple and fast enough |
| Large topics (100K–1M keys), frequent subscriptions, high write rate | Cache + **tailing** — always current, instant lookups |
| Large topics (100K–1M keys), infrequent subscriptions, low write rate | Cache + **on-demand refresh** — saves resources, catches up only when needed |
| Very large topics (> 1M keys) with limited memory | `TopicSnapshotConsumer` with timeout — cache would consume too much memory |
| Mixed workload or uncertain | Cache + **on-demand refresh** (default) — adapts to usage patterns with no wasted resources |
