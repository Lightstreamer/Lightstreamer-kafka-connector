# TopicSnapshotConsumer — Detailed Design

> **Status:** v1 implementation — included in initial snapshot support.
> **Parent document:** [SNAPSHOT_DESIGN.md](SNAPSHOT_DESIGN.md)

---

## Table of Contents

- [1. Overview](#1-overview)
  - [1.1 Why a Shared Consumer (not per-item)](#11-why-a-shared-consumer-not-per-item)
- [2. High-Level Flow](#2-high-level-flow)
- [3. Component Design: `TopicSnapshotConsumer<K, V>`](#3-component-design-topicsnapshotconsumerk-v)
  - [3.1 Characteristics](#31-characteristics)
  - [3.2 Why Not Reuse `RecordProcessor` / `RecordConsumerSupport`?](#32-why-not-reuse-recordprocessor--recordconsumersupport)
  - [3.3 Coordination Model](#33-coordination-model)
  - [3.4 Pseudocode](#34-pseudocode)
- [4. Pass Efficiency Analysis](#4-pass-efficiency-analysis)
- [5. Changes to `SubscriptionsHandler`](#5-changes-to-subscriptionshandler)
  - [5.1 State Machine (per topic)](#51-state-machine-per-topic)
- [6. Thread Safety](#6-thread-safety)
- [7. Known Limitation: Drain-Exit Race](#7-known-limitation-drain-exit-race)
- [8. Large Compacted Topics](#8-large-compacted-topics)
  - [8.1 Mitigation A — Partition Pruning](#81-mitigation-a--partition-pruning)
  - [8.2 Mitigation B — Timeout](#82-mitigation-b--timeout)
  - [8.3 Mitigation C — Early Completion per Item (future)](#83-mitigation-c--early-completion-per-item-future)
  - [8.4 Mitigation Summary](#84-mitigation-summary)
- [9. Configuration](#9-configuration)
- [10. Design Decisions](#10-design-decisions)
  - [10.1 Consumer Reuse](#101-consumer-reuse--decided)
  - [10.2 Continuous Tailing — Not in v1](#102-continuous-tailing--decided-not-in-v1)

---

## 1. Overview

`TopicSnapshotConsumer` is the v1 snapshot delivery mechanism. It is a **shared, per-topic** Kafka consumer that serves snapshot data to **all pending subscribed items** for a given topic. It reads the full compacted state from the beginning on each pass and routes matching records to subscribed items via the existing `RecordMapper`.

For shared context (problem statement, scope, constraints, canonical item alignment, event ordering), see [SNAPSHOT_DESIGN.md](SNAPSHOT_DESIGN.md).

### 1.1 Why a Shared Consumer (not per-item)

Consider 100 clients subscribing to 100 different items on the same compacted topic:

| Strategy | Kafka consumers | Topic reads | TCP connections | Records read |
|---|---|---|---|---|
| **One consumer per item** | 100 | 100 × N | 100 | 100 × N (99% wasted) |
| **Shared per topic** | 1 | 1–few passes | 1 | 1–few × N (routed to all) |

With per-item consumers, each reads the **entire** topic to find ~1 matching record — a waste of broker I/O, network, and memory that grows linearly with subscription count. A shared consumer reads the topic **once** and routes each record to all pending items via the existing `RecordMapper`, reducing broker load by orders of magnitude.

---

## 2. High-Level Flow

```
Clients subscribe to stock-[symbol=AAPL], stock-[symbol=GOOG], ...
         │
         ▼
  SubscribedItems created (one per subscription)
  (pendingRealtimeEvents queues start buffering)
         │
         ├──────────────────────────────────────┐
         ▼                                      ▼
  Main Consumer                          TopicSnapshotConsumer
  (real-time, from latest)               (shared per topic)
  Events → queued in memory              assign() all partitions
                                         seekToBeginning()
                                         poll → route to ALL pending items
                                         sendSnapshotEvent() per match
                                         poll until endOffsets reached
                                         ┌───────────────────────────┐
                                         │ For each served item:     │
                                         │ onItemComplete(item)      │
                                         │ (via ItemFinalization-    │
                                         │  Callback to handler)     │
                                         └───────────────────────────┘
                                         late arrivals? → new pass
                                         no more items → close consumer
         │                                      │
         └──────────────────────────────────────┘
                          │
                          ▼
               Real-time events flow directly
```

---

## 3. Component Design: `TopicSnapshotConsumer<K, V>`

### 3.1 Characteristics

| Aspect | Design Choice | Rationale |
|---|---|---|
| **Scope** | One instance per topic (not per item) | Reads the topic once; routes to all pending items |
| **Consumer group** | None — uses `assign()` | Avoids polluting the main consumer's group offsets |
| **Partition assignment** | All partitions of the topic | Ensures complete snapshot from all partitions |
| **Starting offset** | `seekToBeginning()` | Reads the full compacted state |
| **Completion detection** | `endOffsets()` — stop when all partitions reach their end offset | Deterministic completion without waiting for idle timeouts |
| **Record routing** | Reuses `RecordMapper.map()` + `route()` against a scoped `SubscribedItems` set | Each record checked against all pending items; only matching items receive the event |
| **Dispatch** | `sendSnapshotEvent()` directly per matched item | Simple inline dispatch — no `RecordProcessor`/`ProcessUpdatesStrategy` involvement (see rationale below) |
| **Finalization** | Delegates to `ItemFinalizationCallback` | The consumer does **not** call `endOfSnapshot()` or `enableRealtimeEvents()` directly. The caller coordinates finalization across all topic consumers via a per-item countdown (see Section 5) |
| **Deserialization** | Eager, per-record, via `DeserializerPair<K, V>` | Single-threaded pass; records need immediate evaluation for routing |
| **Poll error handling** | 3-tier: `WakeupException` → rethrow, `KafkaException` → log + rethrow, unexpected → wrap + rethrow | Same pattern as `KafkaConsumerWrapper.doPoll()` |
| **Shutdown** | `consumer.wakeup()` via `shutdown()` method; `WakeupException` breaks the pass loop | Enables graceful cancellation from `SubscriptionsHandler.stopConsuming()` |
| **Late arrivals** | Items arriving during a pass are queued; trigger a new pass when the current one finishes | No item is left behind; no artificial accumulation delay |
| **Lifecycle** | Runs passes until no pending items remain, then closes | Self-draining; thread returns to pool |
| **Thread model** | One thread per topic from a shared `ExecutorService` | Bounded concurrency across topics |

### 3.2 Why Not Reuse `RecordProcessor` / `RecordConsumerSupport`?

The `RecordConsumer`/`RecordProcessor` infrastructure provides batch orchestration (single-threaded vs parallel), ordering strategies, offset commits, command modes, and monitoring — all irrelevant for snapshot delivery. `TopicSnapshotConsumer` has its own orthogonal orchestration: multi-pass coordination, scoped item sets, timeout, and finalization.

Both consumers share `RecordMapper` (the mapping and routing abstraction). Forcing both through a shared processor would add indirection without benefit — the snapshot consumer always sends snapshots, never real-time, never commands. The inline 3-line dispatch (`map()` → `route()` → `sendSnapshotEvent()`) is the right level of abstraction.

### 3.3 Coordination Model

```
                    SubscriptionsHandler
                           │
          subscribe(item1) │  subscribe(item2)   subscribe(item3)
                ▼          │        ▼                   ▼
         pendingItems queue (thread-safe, per topic)
                │
                ▼
       TopicSnapshotConsumer (one per topic)
       ┌───────────────────────────────────┐
       │ Pass 1:                           │
       │  snapshot items = {item1, item2}  │  ← item3 arrives mid-pass
       │  seekToBeginning()                │    → queued for next pass
       │  poll → route → sendSnapshot      │
       │  endOffsets reached               │
       │  onItemComplete(item1)  ──────────│──→ SubscriptionsHandler
       │  onItemComplete(item2)  ──────────│──→   decrements per-item counter
       │                                   │      only finalizes when counter = 0
       ├───────────────────────────────────┤
       │ Pass 2:                           │
       │  snapshot items = {item3}         │  ← no more arrivals
       │  seekToBeginning()                │
       │  poll → route → sendSnapshot      │
       │  endOffsets reached               │
       │  onItemComplete(item3)  ──────────│──→ finalize (counter 0)
       ├───────────────────────────────────┤
       │ pendingItems empty → close        │
       └───────────────────────────────────┘
```

#### Multi-Topic Finalization

When an item is mapped to multiple topics (e.g. many-to-one routing), each topic has its own `TopicSnapshotConsumer`. The item is enqueued into all of them and must wait for **all** to complete before `endOfSnapshot()` is called:

```
item "AAPL" mapped to topics: nyse-stocks, nasdaq-stocks
  → pendingSnapshotTopics[AAPL] = AtomicInteger(2)

TopicSnapshotConsumer("nyse-stocks")     TopicSnapshotConsumer("nasdaq-stocks")
────────────────────────────────         ────────────────────────────────────
poll → route → sendSnapshot(AAPL)        poll → route → sendSnapshot(AAPL)
onItemComplete(AAPL)                     onItemComplete(AAPL)
  └─ count: 2→1 (skip)                    └─ count: 1→0 → FINALIZE
                                                endOfSnapshot(AAPL)
                                                enableRealtimeEvents(AAPL)
```

### 3.4 Pseudocode

```java
class TopicSnapshotConsumer<K, V> implements Runnable {

    private final String topic;
    private final ConcurrentLinkedQueue<SubscribedItem> pendingItems;
    private final ItemFinalizationCallback itemFinalizationCallback;
    // RecordMapper, deserializers, event listener, etc.

    /** Called by SubscriptionsHandler from any thread. */
    void enqueue(SubscribedItem item) {
        pendingItems.add(item);
    }

    void run() {
        Consumer<byte[], byte[]> consumer = createConsumer();  // no group.id
        try {
            List<TopicPartition> partitions = getPartitionsFor(topic);
            consumer.assign(partitions);

            // Keep running passes as long as items need snapshots
            while (true) {
                // 1. Drain pending items into the current pass set
                Set<SubscribedItem> currentPassItems = drainPendingItems();
                if (currentPassItems.isEmpty()) {
                    break;  // No more items → done
                }

                // 2. Build a scoped SubscribedItems for routing
                SubscribedItems snapshotScope = SubscribedItems.from(currentPassItems);

                // 3. Seek to beginning and capture end offsets
                consumer.seekToBeginning(partitions);
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

                // 4. Poll until all partitions reach their end offset
                while (!allPartitionsConsumed(endOffsets)) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
                    for (ConsumerRecord record : records) {
                        KafkaRecord<K, V> kafkaRecord = deserialize(record);
                        MappedRecord mapped = recordMapper.map(kafkaRecord);
                        Set<SubscribedItem> routed = mapped.route(snapshotScope);
                        for (SubscribedItem item : routed) {
                            item.sendSnapshotEvent(mapped.fieldsMap(), eventListener);
                        }
                        trackOffset(record);
                    }
                }

                // 5. Finalize all items served in this pass
                for (SubscribedItem item : currentPassItems) {
                    itemFinalizationCallback.onItemComplete(item);
                }
            }
        } finally {
            consumer.close();
        }
    }

    private Set<SubscribedItem> drainPendingItems() {
        Set<SubscribedItem> drained = new LinkedHashSet<>();
        SubscribedItem item;
        while ((item = pendingItems.poll()) != null) {
            drained.add(item);
        }
        return drained;
    }
}
```

---

## 4. Pass Efficiency Analysis

The number of full topic reads depends on the relationship between the **burst duration** (D — time from first to last item arrival) and the **pass duration** (T — time to read the full topic from beginning to end offsets).

| Burst pattern | D vs T | Passes | Rationale |
|---|---|---|---|
| Instant burst | D ≈ 0 | **2** | All items enqueue before or during first drain; pass 2 serves the rest |
| Fast burst | D < T | **2** | Pass 1 runs longer than the burst lasts; all remaining items batch into pass 2 |
| Slow trickle | D > T | **~⌈D/T⌉ + 1** | Each pass absorbs items that arrived during the previous pass |
| Steady state | No arrivals | **0** | Consumer closed, thread returned to pool |

### Example: 1000 Items, Large Topic (1M keys, ~1KB each, 12 partitions, T ≈ 80s)

**Burst over 2 seconds (D < T):**

```
t=0s:    Item 1 → consumer starts, drain picks up ~5 items
t=0–2s:  Items 2–1000 enqueue (pass 1 still running — takes 80s to read 1M keys)
t=80s:   Pass 1 ends. Drain picks up remaining ~995 items.
t=160s:  Pass 2 ends. Drain empty → consumer exits.
Total: 2 passes, 2 × 1M records read
```

**Trickle over 200 seconds at 5 items/sec (D > T):**

```
Pass 1: drain ~1,  reads 1M keys in 80s → ~400 items accumulate
Pass 2: drain ~400, reads 1M keys in 80s → ~400 items accumulate
Pass 3: drain ~400, reads 1M keys in 80s → ~199 items accumulate (burst ends at 200s)
Pass 4: drain ~199, reads 1M keys in 80s → 0 items
Total: 4 passes, 4 × 1M records read
```

### Self-Regulating Property

For **large topics** (1M+ keys, T is tens of seconds or more), the pass duration naturally absorbs the burst — nearly all items batch into a single follow-up pass regardless of burst size. 1000 items over 2 seconds on a 1M-key topic → 2 passes.

For **small topics** (10–100 keys, T is sub-millisecond), passes complete faster than items arrive, yielding more passes — but each pass is so fast that the total I/O is negligible. A 10-key topic with 1000 items trickling might produce many passes, but each reads only 10 records in microseconds.

For **medium topics** (10K–100K keys, T ≈ 0.1–1s), multiple passes with non-trivial I/O per pass can occur. For example, a 50K-key topic (T ≈ 0.5s) with 1000 items arriving at 100/sec: D = 10s → ~⌈10/0.5⌉ + 1 = 21 passes, each reading 50K records. Total: ~1M records read — manageable, but the [Snapshot Cache](SNAPSHOT_CACHE_DESIGN.md) eliminates repeated reads entirely for this case.

---

## 5. Changes to `SubscriptionsHandler`

`SubscriptionsHandler` was refactored from an abstract class hierarchy (`AbstractSubscriptionsHandler` → `DefaultSubscriptionsHandler`) into a flat `DefaultSubscriptionsHandler` that directly implements the `SubscriptionsHandler` interface. The `consumerLock` now guards both the item counter and the main consumer lifecycle atomically.

```
Current (with snapshot):
  subscribe() → create item → addItem()
             → incrementAndMaybeStartConsuming()   [under consumerLock]
             → enqueueForSnapshot(item)

  unsubscribe() → removeItem()
               → decrementAndMaybeStopConsuming()  [under consumerLock]
                   → if itemsCount == 0:
                       shutdownSnapshotConsumers()
                       shutdown main consumer

  TopicSnapshotConsumer pass completes → onItemComplete() via SnapshotListener
    → SubscriptionsHandler.onItemSnapshotComplete(item)
    → decrements per-item topic counter
    → when counter reaches 0 → endOfSnapshot() + enableRealtimeEvents()
```

The key changes:

1. **Defer `enableRealtimeEvents()`** until after the snapshot completes. During the gap, real-time events from the main consumer are naturally buffered by the `pendingRealtimeEvents` queue (already implemented in `DefaultSubscribedItem`).

2. **Manage `TopicSnapshotConsumer` instances**: `SubscriptionsHandler` maintains a `Map<String, TopicSnapshotConsumer>` (topic → consumer). On the first snapshot request for a topic, it creates and starts the consumer. Subsequent requests for the same topic just enqueue. When the consumer drains all items and exits, the entry is removed.

3. **Coordinate multi-topic finalization**: `SubscriptionsHandler` maintains a `Map<SubscribedItem, AtomicInteger>` (`pendingSnapshotTopics`) that tracks how many topic consumers still need to complete for each item. When `enqueueForSnapshot(item)` is called, the counter is set to the number of matching topics. Each `TopicSnapshotConsumer` calls `onItemComplete(item)` via its `SnapshotListener`; `SubscriptionsHandler.onItemSnapshotComplete(item)` decrements the counter. Only when it reaches zero are `endOfSnapshot()` and `enableRealtimeEvents()` called. This ensures correct behavior for many-to-one topic routing.

4. **Submit after enqueue**: The snapshot consumer is submitted to the thread pool **after** the first item is enqueued (not during `computeIfAbsent`). This eliminates a race condition where the consumer's `run()` → `drainPendingItems()` could find an empty queue and exit before `enqueue()` was called.

5. **Pool lifecycle**: The snapshot pool (`CachedThreadPool` with daemon threads) lives for the lifetime of the handler — it is never shut down and recreated. When the last item unsubscribes, `shutdownSnapshotConsumers()` wakes up active consumers via `TopicSnapshotConsumer.shutdown()`, but the pool threads are reaped naturally after 60 seconds of inactivity.

### 5.1 State Machine (per topic)

```
  [No Consumer]  ──subscribe(item)──▶  [Consumer Running]
       ▲                                      │
       │                               poll + route
       │                               pass complete
       │                                      │
       │                               pending empty?
       │                              ╱            ╲
       │                           yes              no
       │                            │                │
       └────── close consumer ◄─────┘         new pass
                                               │
                                               └──▶ [Consumer Running]
```

---

## 6. Thread Safety

The subscribe/unsubscribe path involves three categories of concurrent access:

1. **Main consumer lifecycle** (`consumer`, `futureStatus`, `itemsCount`): All guarded by `consumerLock`. Both `incrementAndMaybeStartConsuming()` and `decrementAndMaybeStopConsuming()` perform the counter check and conditional create/destroy atomically under the lock. A concurrent subscribe cannot interleave with the teardown path.

2. **`enqueueForSnapshot()` vs `shutdownSnapshotConsumers()`**: These operate on the same `ConcurrentHashMap` (`snapshotConsumers`) but **cannot overlap**. `shutdownSnapshotConsumers()` only fires when `itemsCount` reaches 0. Lightstreamer guarantees per-item serialization: `unsubscribe(X)` is never called until `subscribe(X)` returns. Therefore, while any `subscribe()` is in progress, the subscribing thread's +1 contribution to `itemsCount` cannot be reversed, and the counter stays ≥ 1. The `shutdownSnapshotConsumers()` precondition (`itemsCount == 0`) is unreachable while any `subscribe()` is executing.

3. **Stale item finalization**: When `shutdownSnapshotConsumers()` wakes up a running `TopicSnapshotConsumer`, it triggers `drainAndFinalizeAll()` → `onItemSnapshotComplete()` on the snapshot thread. This may call `endOfSnapshot()` and `enableRealtimeEvents()` on items that have already been unsubscribed. This is **benign**: the Lightstreamer SDK's `smartEndOfSnapshot()` and `smartUpdate()` silently ignore calls with stale item handles.

| Concern | Status | Mechanism |
|---|---|---|
| Main consumer lifecycle | Safe | `consumerLock` serializes counter + create/destroy |
| enqueue vs shutdown | Safe | Lightstreamer per-item serialization prevents `itemsCount == 0` during `subscribe()` |
| Stale item finalization | Benign | SDK ignores calls on unsubscribed item handles |

**`forceUnsubscriptionAll()` and the error path**: When `incrementAndMaybeStartConsuming()` catches a `KafkaException` (e.g., unable to connect to Kafka), it calls `metadataListener.forceUnsubscriptionAll()`. This does **not** synchronously trigger `unsubscribe()` calls. Instead, the server sends a force-unsubscription command to the client over the network; the client receives it and sends unsubscribe requests back; the server then calls `unsubscribe()` for each item on separate threads. The resulting `decrementAndMaybeStopConsuming()` calls arrive later, well after `incrementAndMaybeStartConsuming()` has returned and released the lock. At that point, `consumer` is `null` (never created), `snapshotConsumers` is empty (`enqueueForSnapshot` was skipped by the exception), and the decrements cleanly reach zero with no state to tear down.

---

## 7. Known Limitation: Drain-Exit Race

There is a narrow race window between `TopicSnapshotConsumer.drainPendingItems()` returning empty and the consumer exiting its loop. If a subscribe thread calls `computeIfAbsent` on `snapshotConsumers` during that window, it finds the existing (but exiting) consumer, enqueues an item, and the item is never served — no `endOfSnapshot()` or `enableRealtimeEvents()` is called.

**Why this is acceptable for v1:**

- The window is microseconds wide (between the empty `poll()` on `ConcurrentLinkedQueue` and the `break` statement).
- It requires a subscribe for the same topic to land in exactly that moment.
- Items for the same topic **can** arrive in bursts (e.g., 1000 clients subscribing to 1000 symbols on the same topic), but even so, the window is narrow enough that it is unlikely for a subscribe to land in the exact gap between drain and exit.
- The symptom is a single item stuck in buffering mode — recoverable by the client resubscribing.

**Reassessment with bursts:** Per-item serialization only prevents overlap on the *same* item. Different items for the same topic can arrive concurrently in large bursts (e.g., market open, page load with many widgets). In a burst of 1000 subscribes, the consumer runs a long pass absorbing most items, but the **tail of the burst** — the last few items arriving as the consumer finishes its final pass — is the realistic trigger. It is no longer "a subscribe must land in a microsecond window" but rather "the last straggler in a batch of 1000 arrives as the consumer is wrapping up." Over many burst cycles in production, this becomes plausible.

However, the self-regulating property of the multi-pass design (see [Section 4](#4-pass-efficiency-analysis)) limits exposure: on **large topics**, the pass takes so long that stragglers batch into the next pass well before drain-exit. On **small topics**, the pass is sub-millisecond, so the drain-exit window is correspondingly tiny. The medium-topic regime (pass duration ≈ inter-arrival time) has the highest exposure, but even there the window is a single `ConcurrentLinkedQueue.poll()` → `break` gap.

**Future fix:** A "close-the-gate" pattern where `TopicSnapshotConsumer` sets a `completed` volatile flag and re-drains, and `enqueue()` returns `false` if the flag is set, signalling the caller to create a fresh consumer. The fix is ~20 lines of code and eliminates the race entirely:

```java
// TopicSnapshotConsumer:
private final AtomicBoolean completed = new AtomicBoolean(false);

boolean tryEnqueue(SubscribedItem item) {
    if (completed.get()) return false;
    pendingItems.add(item);
    if (completed.get()) { pendingItems.remove(item); return false; }
    return true;
}

// In run(), replacing the drain-break:
Set<SubscribedItem> passItems = drainPendingItems();
if (passItems.isEmpty()) {
    completed.set(true);
    passItems = drainPendingItems();  // re-drain: catches adds between drain and flag
    if (passItems.isEmpty()) break;
    completed.set(false);             // not done after all
}

// enqueueForSnapshot() in SubscriptionsHandler:
snapshotConsumers.compute(topic, (t, existing) -> {
    if (existing != null && existing.tryEnqueue(item)) return existing;
    var newConsumer = createSnapshotConsumer(topic);
    newConsumer.enqueue(item);
    snapshotPool.submit(newConsumer);
    return newConsumer;
});
```

---

## 8. Large Compacted Topics

The `TopicSnapshotConsumer` must read **every record** from the beginning of the topic, even though the client may care about only 1 key out of millions. Routing via `RecordMapper` filters at the application level — but the I/O and deserialization have already happened.

### Impact at Scale

| Topic size (compacted keys) | Partitions | Approx. read time (1MB/s per partition) | Records evaluated | Records delivered (1 item) |
|---|---|---|---|---|
| 10K keys, ~1KB each | 3 | < 1s | 10K | ~1 |
| 1M keys, ~1KB each | 12 | ~80s | 1M | ~1 |
| 10M keys, ~1KB each | 24 | ~7 min | 10M | ~1 |

The waste ratio approaches 100% for selective subscriptions on very large topics.

#### Specific Concerns

**1. Time to first snapshot (client-facing latency):**
The client blocks until `endOfSnapshot()`. On a 1M-key topic, this could mean tens of seconds of waiting — unacceptable for interactive applications.

**2. Broker read amplification:**
Each pass reads the entire topic. With the multi-pass design, if subscriptions trickle in, the total I/O is N passes × full topic size.

**3. Memory pressure from `pendingRealtimeEvents`:**
While the snapshot pass runs (potentially minutes), the main consumer continues polling and real-time events accumulate in `DefaultSubscribedItem.pendingRealtimeEvents` (an unbounded `ConcurrentLinkedQueue`). On a high-throughput topic this queue can grow large.

**4. Deserialization overhead:**
Every record must be deserialized (Avro, Protobuf, JSON) to evaluate the template expression, even if the record doesn't match. On complex schemas this is non-trivial CPU.

**5. Compaction lag:**
"Compacted" doesn't mean "already compacted." Kafka compaction runs asynchronously. The topic may contain many duplicate keys that haven't been cleaned up yet, amplifying the actual record count well beyond the unique key count.

### 8.1 Mitigation A — Partition Pruning

If the item template extracts from `KEY.*` and the topic uses Kafka's default partitioner (`murmur2`), we can **compute the target partition** from the key and only assign/read that single partition:

```java
// Instead of reading ALL partitions:
consumer.assign(allPartitions);

// Only read the partition where this key lives:
int partition = Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
consumer.assign(List.of(new TopicPartition(topic, partition)));
```

| Topic: 1M keys, 12 partitions | Without pruning | With pruning |
|---|---|---|
| Partitions read | 12 | 1 |
| Records evaluated | 1M | ~83K |
| Speedup | 1× | ~12× |

**Limitations:**
- Only works when (a) the template parameters fully determine the Kafka key, (b) the partitioner is known/default, and (c) the subscription is for a specific key (not a wildcard).
- For shared-consumer passes serving multiple items that hash to different partitions, we assign the **union of their target partitions** rather than all partitions.

> **Current v1 state:** Deferred — requires mapping subscribed item key values back to partition numbers, which depends on knowing the partitioner. Can be added without changes to `TopicSnapshotConsumer`'s external interface.

### 8.2 Mitigation B — Timeout

Configurable maximum duration per snapshot pass. If the timeout fires before all partitions are consumed:

- Deliver `endOfSnapshot()` for items that received at least one matching event (best-effort snapshot)
- Deliver `endOfSnapshot()` for items that received nothing (empty snapshot — client transitions to real-time only)
- Log a warning

```
snapshot.timeout.ms=30000    # Max 30 seconds per pass
```

**Trade-off:** The client may receive an incomplete snapshot but will catch up via real-time updates. For compacted topics this is acceptable — the "missing" snapshot data will eventually arrive as a real-time update if the key is updated.

> **Current v1 state:** Implemented (`snapshot.timeout.ms`, default `Duration.ZERO` = no limit).

### 8.3 Mitigation C — Early Completion per Item (future)

Within a pass, once an item has received its snapshot event(s), call `endOfSnapshot()` + `enableRealtimeEvents()` for that item immediately without waiting for the full pass to finish. The pass continues for remaining items.

```
Pass reading 1M records:
  - At record 500:     item "stock-[symbol=AAPL]"  matched → endOfSnapshot(AAPL) immediately
  - At record 12000:   item "stock-[symbol=GOOG]"  matched → endOfSnapshot(GOOG) immediately
  - At record 999999:  pass complete, remaining items get empty endOfSnapshot()
```

**Caveat:** On a compacted topic with compaction lag, the same key may appear multiple times. Completing early means the client gets an older value, then later receives the newer value as a real-time update (since `enableRealtimeEvents()` was already called). This is **semantically correct** but the snapshot is stale.

### 8.4 Mitigation Summary

| Mitigation | Complexity | Impact | Included in v1 |
|---|---|---|---|
| **Partition pruning** | Medium | High (÷ partition count) | No — deferred to v1.1 |
| **Timeout** | Low | Medium (safety net) | Yes — implemented |
| **Early completion** | Medium | High (reduces client latency) | No — future iteration |
| **[Snapshot Cache](SNAPSHOT_CACHE_DESIGN.md)** | High | Very high | No — future iteration |

---

## 9. Configuration

### Snapshot Timeout

| Parameter | Key | Type | Default | Description |
|---|---|---|---|---|
| Snapshot Timeout | `snapshot.timeout.ms` | `NON_NEGATIVE_INT` | `0` (no timeout) | Maximum duration in milliseconds per snapshot pass. `0` means no limit |

See [SNAPSHOT_DESIGN.md](SNAPSHOT_DESIGN.md) for the shared `snapshot.enable` parameter and enabling conditions.

### Snapshot Consumer Kafka Properties

The snapshot consumer inherits all Kafka properties (SSL, SASL, Schema Registry, etc.) from the main consumer configuration, with the following overrides:

| Property | Override Value | Reason |
|---|---|---|
| `group.id` | Not set | Uses `assign()`, no consumer group needed |
| `enable.auto.commit` | `false` | Ephemeral consumer, no offset management |
| `auto.offset.reset` | `earliest` | Redundant with `seekToBeginning()` but safe default |

---

## 10. Design Decisions

### 10.1 Consumer Reuse — DECIDED

The design uses a **shared consumer per topic** (`TopicSnapshotConsumer`). Each topic has at most one active snapshot consumer at a time. All pending subscribed items for that topic are served in the same pass, with late arrivals handled via subsequent passes.

This resolves the scalability concern: 100 subscriptions on the same topic require 1 Kafka consumer and 1–few topic reads, not 100.

#### Future Optimization: Accumulation Window

If profiling shows excessive passes due to rapid-fire subscriptions arriving just after a pass starts, an optional accumulation delay (e.g., `snapshot.accumulation.ms=500`) could batch subscriptions before starting each pass. This is **not included in the initial implementation** to keep the design simple.

### 10.2 Continuous Tailing — DECIDED (Not in v1)

An alternative design would have `TopicSnapshotConsumer` continue polling (tailing) after completing a pass, instead of exiting when the pending queue is empty. This would keep the consumer alive at the tail of the topic, ready to serve late-arriving subscriptions without re-reading from the beginning.

**This is deliberately not implemented.** Tailing is only useful when paired with retained state — an in-memory key→fields map that accumulates the full topic state. Without such a map, a tailing consumer that receives a late subscription still has to `seekToBeginning()` because the item needs the complete compacted state, not just records from the tail position.

#### What Not Tailing Costs

When a subscription arrives after the consumer has already exited, a new `TopicSnapshotConsumer` is created and re-reads the entire topic from the beginning:

```
  t0: TopicSnapshotConsumer reads topic → delivers snapshot for item A
      endOffsets reached → consumer exits, resources freed
                    ▼
  t1: ────── consumer is GONE ──────
                    │
  t2: Kafka record produced: AAPL → {price:175}
                    │
  t3: ────── no one is reading ──────
                    │
  t4: Client subscribes to item B (same topic)
                    │
                    ▼
  t5: NEW TopicSnapshotConsumer created
      seekToBeginning() → re-reads ENTIRE topic
      (including the t2 record — no data loss)
      delivers snapshot → exits
```

#### Why This Is Not a Problem

```
  t0: (hypothetical) TopicSnapshotConsumer tails after item A
      consumer KEEPS POLLING from tail position
                    ▼
  t2: Kafka record: AAPL → {price:175}
      consumer sees it — but has no state to store it in
      (no cache map — records are discarded)
                    │
  t4: Client subscribes to item B
      Item B needs the FULL topic state, not just tail records
      → must seekToBeginning() anyway
      → tailing saved nothing
```

1. **No data loss.** The re-read from beginning picks up everything, including records produced while no consumer was active. The compacted topic is the durable state.

2. **Batching already mitigates the cost.** Subscriptions arriving close together are served in the same pass or the next pass (via the `pendingItems` queue). The re-read penalty only applies when subscriptions arrive after the consumer has exited — meaning there was an idle period with no demand.

3. **Tailing without a cache is useless.** A tailing consumer with no in-memory map has nothing to serve from. When a new item arrives, it still needs the full topic state — the tail position only has recent records. The resource cost (a thread + TCP connection held open permanently per topic) yields zero benefit.

4. **Tailing with a cache IS the Snapshot Cache.** Adding an in-memory `ConcurrentHashMap<canonicalName, fieldsMap>` to make tailing useful creates a fundamentally different component with different lifecycle, memory profile, and failure semantics. This is the [Snapshot Cache](SNAPSHOT_CACHE_DESIGN.md) — a separate, opt-in optimization for a future iteration.

> **Decision:** Keep `TopicSnapshotConsumer` ephemeral. When repeated full reads become a bottleneck, implement the [Snapshot Cache](SNAPSHOT_CACHE_DESIGN.md) as a standalone replacement.
