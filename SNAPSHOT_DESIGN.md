# Snapshot Support for Compacted Topics — Design Document

## Table of Contents

- [1. Problem Statement](#1-problem-statement)
- [2. Current Architecture Summary](#2-current-architecture-summary)
- [3. Proposed Design](#3-proposed-design)
  - [3.1 High-Level Flow (with Snapshot)](#31-high-level-flow-with-snapshot)
  - [3.2 New Component: `TopicSnapshotConsumer<K, V>`](#32-new-component-topicsnapshotconsumerk-v)
  - [3.3 Changes to `SubscriptionsHandler`](#33-changes-to-subscriptionshandler)
  - [3.4 Configuration](#34-configuration)
- [4. Event Ordering Guarantees](#4-event-ordering-guarantees)
- [5. Subscription Lifecycle Comparison](#5-subscription-lifecycle-comparison)
- [6. Affected Files Summary](#6-affected-files-summary)
- [7. Design Decisions and Open Questions](#7-design-decisions-and-open-questions)
  - [7.1 Scope of `snapshot.enable`](#71-scope-of-snapshotenable--decided)
  - [7.2 Snapshot for Non-Parameterized Items](#72-snapshot-for-non-parameterized-items--decided)
  - [7.3 Large Compacted Topics](#73-large-compacted-topics)
  - [7.4 Error Handling](#74-error-handling--decided)
  - [7.5 Consumer Reuse](#75-consumer-reuse--decided)
  - [7.6 Continuous Tailing](#76-continuous-tailing--decided-not-in-v1)
  - [7.7 COMMAND Mode Mutual Exclusion](#77-command-mode-mutual-exclusion--decided)
- [8. Non-Compacted Topics — Out of Scope for v1](#8-non-compacted-topics--out-of-scope-for-v1)
  - [8.1 Fundamental Difference](#81-fundamental-difference)
  - [8.2 Impact on Design Components](#82-impact-on-design-components)
  - [8.3 Summary](#83-summary)
  - [8.4 What Non-Compacted Support Would Require](#84-what-non-compacted-support-would-require)
- [9. Canonical Item / Compaction Key Alignment](#9-canonical-item--compaction-key-alignment)
  - [9.1 Case Analysis](#91-case-analysis)
  - [9.2 Summary Matrix](#92-summary-matrix)
  - [9.3 v1 Constraints](#93-v1-constraints)
- [10. Regex Topic Subscriptions — v2 Improvement](#10-regex-topic-subscriptions--v2-improvement)
  - [10.1 Why Regex Is Excluded from v1](#101-why-regex-is-excluded-from-v1)
  - [10.2 Future Approaches](#102-future-approaches)

---

## 1. Problem Statement

When a Lightstreamer client subscribes to an item mapped to a Kafka compacted topic, it currently receives only **new** records produced after the subscription starts (the main consumer uses `auto.offset.reset=latest` by default). Customers need to receive the **current state** of the compacted topic as a snapshot upon subscription, optionally filtered by key criteria.

### Example

A compacted topic `stocks` holds the latest price for each stock symbol. A client subscribing to `stock-[symbol=AAPL]` should immediately receive the most recent `AAPL` record as a snapshot, then transition to real-time updates.

### Canonical Item Name

A **canonical item name** is the fully-resolved item name produced by applying a template expression to a Kafka record. Templates use `#{}` syntax to define extraction parameters; the resolved name uses `[]` syntax:

```
Template:   stock-#{symbol=KEY.symbol}
Record key: {symbol: "AAPL"}
Canonical:  stock-[symbol=AAPL]
```

`RecordMapper.map()` extracts template parameters from the record's key (or value) fields and produces the canonical name. Routing works by matching this name against active `SubscribedItem` instances — clients subscribe using the canonical form directly (e.g., `stock-[symbol=AAPL]`), and records that produce the same canonical name are routed to that subscription.

This concept is central to the snapshot design: the snapshot consumer reads records, maps them to canonical names via `RecordMapper`, and routes matching records to subscribed items.

### Scope

This feature targets **compacted topics only** in v1, with **literal (non-regex) topic names** and **KEY-only item template parameters**. The design relies on properties unique to compacted topics — bounded data volume (one value per key), "read from beginning = current state" semantics, and safe duplicate delivery. Non-compacted topics break these assumptions; see [Section 8](#8-non-compacted-topics--out-of-scope-for-v1) for a detailed analysis.

Additionally, snapshot correctness depends on the **alignment between Kafka's compaction key and the item template parameters**. See [Section 9](#9-canonical-item--compaction-key-alignment) for a detailed analysis of which template configurations produce correct snapshots. Regex topic subscriptions are excluded from v1 due to resource safety concerns; see [Section 10](#10-regex-topic-subscriptions--v2-improvement) for the rationale and future approach.

---

## 2. Current Architecture Summary

### Subscription Flow (today)

```
Client subscribes to "stock-[symbol=AAPL]"
        │
        ▼
MetadataAdapter.getItems() → canonicalizes item name
        │
        ▼
DataAdapter.subscribe(itemName, itemHandle)
        │
        ▼
SubscribedItem created + added to SubscribedItems
        │
        ▼
enableRealtimeEvents(listener)  ← called immediately
        │
        ▼
If first subscription → startConsuming() → KafkaConsumerWrapper polling loop
        │
        ▼
Records polled → RecordMapper.map() → route to matching items → sendRealtimeEvent()
```

### Key Classes

| Class | Responsibility |
|---|---|
| `KafkaConnectorDataAdapter` | Implements `SmartDataProvider`; manages subscribe/unsubscribe lifecycle |
| `SubscriptionsHandler` | Manages consumer lifecycle; starts/stops consumer based on subscription count |
| `KafkaConsumerWrapper` | Wraps Kafka `Consumer`; runs polling loop; delegates to `RecordConsumer` |
| `RecordConsumer` / `RecordConsumerSupport` | Processes batches; maps records to items; dispatches updates via strategies |
| `RecordMapper` | Maps a `KafkaRecord` to canonical item names + fields; routes to `SubscribedItems` |
| `Items.SubscribedItem` | Represents an active subscription; tracks snapshot state; queues or dispatches events |
| `EventListener` / `SmartEventListener` | Bridges to Lightstreamer's `ItemEventListener` for update delivery |
| `ConnectorConfig` | Parses adapter parameters; holds topic mappings, field mappings, command mode settings |

### Existing Snapshot Infrastructure

The codebase already contains snapshot plumbing built for **Command Mode** (where the Kafka producer sends explicit `CS`/`EOS` control records). The following components are fully functional but only activated in Command Mode (`ENFORCE`):

| Component | Location | What It Does |
|---|---|---|
| `SubscribedItem.isSnapshot()` / `setSnapshot(flag)` | `Items.java` | Tracks whether the item is in snapshot phase |
| `SubscribedItem.sendSnapshotEvent(event, listener)` | `Items.java` | Dispatches an event with `isSnapshot=true` to Lightstreamer |
| `SubscribedItem.endOfSnapshot(listener)` | `Items.java` | Signals Lightstreamer that snapshot delivery is complete |
| `SubscribedItem.clearSnapshot(listener)` | `Items.java` | Signals Lightstreamer to clear previously cached snapshot data |
| `RecordProcessor.processAsSnapshot(record, item)` | `RecordConsumerSupport.java` | Maps a record and dispatches it as a snapshot event (exists, used only internally) |
| `DefaultUpdatesStrategy.sendUpdatesAsSnapshot()` | `RecordConsumerSupport.java` | Calls `sendSnapshotEvent()` on a `SubscribedItem` |
| `pendingRealtimeEvents` queue | `Items.java` (`DefaultSubscribedItem`) | Buffers real-time events before `enableRealtimeEvents()` is called |
| `isSnapshotAvailable()` | `KafkaConnectorDataAdapter.java` | Returns `true` only when Command Mode `ENFORCE` is active |
| Commented placeholder | `SubscriptionsHandler.java` | `// consumer.consumeRecordsAsSnapshot(null, item);` — planned entry point |

### Filtering via Item Templates

Item templates already provide implicit key-based filtering:

```
Template:  stock-#{symbol=KEY.symbol}
Record:    key={symbol: "AAPL"}, value={price: 150.25}
Canonical: stock-[symbol=AAPL]
```

Only records whose extracted canonical name matches a subscribed item are routed. This means **no new filter syntax is needed** — the existing template mechanism naturally filters snapshot records by key.

---

## 3. Proposed Design

### 3.1 High-Level Flow (with Snapshot)

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

### 3.1.1 Why a Shared Consumer (not per-item)

Consider 100 clients subscribing to 100 different items on the same compacted topic:

| Strategy | Kafka consumers | Topic reads | TCP connections | Records read |
|---|---|---|---|---|
| **One consumer per item** | 100 | 100 × N | 100 | 100 × N (99% wasted) |
| **Shared per topic** | 1 | 1–few passes | 1 | 1–few × N (routed to all) |

With per-item consumers, each reads the **entire** topic to find ~1 matching record — a waste of broker I/O, network, and memory that grows linearly with subscription count. A shared consumer reads the topic **once** and routes each record to all pending items via the existing `RecordMapper`, reducing broker load by orders of magnitude.

### 3.2 New Component: `TopicSnapshotConsumer<K, V>`

A **shared, per-topic** Kafka consumer that serves snapshot data to **all pending subscribed items** for a given topic in a single pass.

#### Characteristics

| Aspect | Design Choice | Rationale |
|---|---|---|
| **Scope** | One instance per topic (not per item) | Reads the topic once; routes to all pending items |
| **Consumer group** | None — uses `assign()` | Avoids polluting the main consumer's group offsets |
| **Partition assignment** | All partitions of the topic | Ensures complete snapshot from all partitions |
| **Starting offset** | `seekToBeginning()` | Reads the full compacted state |
| **Completion detection** | `endOffsets()` — stop when all partitions reach their end offset | Deterministic completion without waiting for idle timeouts |
| **Record routing** | Reuses `RecordMapper.map()` + `route()` against a scoped `SubscribedItems` set | Each record checked against all pending items; only matching items receive the event |
| **Dispatch** | `sendSnapshotEvent()` directly per matched item | Simple inline dispatch — no `RecordProcessor`/`ProcessUpdatesStrategy` involvement (see rationale below) |
| **Finalization** | Delegates to `ItemFinalizationCallback` | The consumer does **not** call `endOfSnapshot()` or `enableRealtimeEvents()` directly. The caller coordinates finalization across all topic consumers via a per-item countdown (see Section 3.3) |
| **Deserialization** | Eager, per-record, via `DeserializerPair<K, V>` | Single-threaded pass; records need immediate evaluation for routing |
| **Poll error handling** | 3-tier: `WakeupException` → rethrow, `KafkaException` → log + rethrow, unexpected → wrap + rethrow | Same pattern as `KafkaConsumerWrapper.doPoll()` |
| **Shutdown** | `consumer.wakeup()` via `shutdown()` method; `WakeupException` breaks the pass loop | Enables graceful cancellation from `SubscriptionsHandler.stopConsuming()` |
| **Late arrivals** | Items arriving during a pass are queued; trigger a new pass when the current one finishes | No item is left behind; no artificial accumulation delay |
| **Lifecycle** | Runs passes until no pending items remain, then closes | Self-draining; thread returns to pool |
| **Thread model** | One thread per topic from a shared `ExecutorService` | Bounded concurrency across topics |

#### Why Not Reuse `RecordProcessor` / `RecordConsumerSupport`?

The `RecordConsumer`/`RecordProcessor` infrastructure provides batch orchestration (single-threaded vs parallel), ordering strategies, offset commits, command modes, and monitoring — all irrelevant for snapshot delivery. `TopicSnapshotConsumer` has its own orthogonal orchestration: multi-pass coordination, scoped item sets, timeout, and finalization.

Both consumers share `RecordMapper` (the mapping and routing abstraction). Forcing both through a shared processor would add indirection without benefit — the snapshot consumer always sends snapshots, never real-time, never commands. The inline 3-line dispatch (`map()` → `route()` → `sendSnapshotEvent()`) is the right level of abstraction.

#### Coordination Model

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

##### Multi-Topic Finalization

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

#### Pseudocode

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

#### Pass Efficiency Analysis

The number of full topic reads depends on subscription arrival timing:

| Scenario | Passes | Topic reads |
|---|---|---|
| All 100 items subscribe before first poll starts | 1 | 1 × N |
| 80 arrive before, 20 arrive mid-pass | 2 | 2 × N |
| Items trickle in one by one over 10 seconds | ~few | few × N |
| Steady state (no new subscriptions) | 0 | Consumer closed |

In the worst case (subscriptions trickling), the topic is read a handful of times — still dramatically better than 100 individual consumers.

### 3.3 Changes to `SubscriptionsHandler`

```
Current:
  subscribe() → create item → enableRealtimeEvents() → onSubscribedItem()

Proposed (when snapshot enabled):
  subscribe() → create item → [DO NOT call enableRealtimeEvents() yet] → onSubscribedItem()
  onSubscribedItem() → start main consumer (if first)
                     → enqueue item into TopicSnapshotConsumer (created on first enqueue per topic)
  TopicSnapshotConsumer pass completes → onItemComplete() callback
    → SubscriptionsHandler decrements per-item topic counter
    → When counter reaches 0 → endOfSnapshot() + enableRealtimeEvents()
```

The key changes:

1. **Defer `enableRealtimeEvents()`** until after the snapshot completes. During the gap, real-time events from the main consumer are naturally buffered by the `pendingRealtimeEvents` queue (already implemented in `DefaultSubscribedItem`).

2. **Manage `TopicSnapshotConsumer` instances**: `SubscriptionsHandler` maintains a `Map<String, TopicSnapshotConsumer>` (topic → consumer). On the first snapshot request for a topic, it creates and starts the consumer. Subsequent requests for the same topic just enqueue. When the consumer drains all items and exits, the entry is removed.

3. **Coordinate multi-topic finalization**: `SubscriptionsHandler` maintains a `Map<SubscribedItem, AtomicInteger>` (`pendingSnapshotTopics`) that tracks how many topic consumers still need to complete for each item. When `enqueueForSnapshot(item)` is called, the counter is set to the number of matching topics. Each `TopicSnapshotConsumer` calls `onItemComplete(item)` via its `ItemFinalizationCallback`; `SubscriptionsHandler.onItemSnapshotComplete(item)` decrements the counter. Only when it reaches zero are `endOfSnapshot()` and `enableRealtimeEvents()` called. This ensures correct behavior for many-to-one topic routing.

4. **Submit after enqueue**: The snapshot consumer is submitted to the thread pool **after** the first item is enqueued (not during `computeIfAbsent`). This eliminates a race condition where the consumer's `run()` → `drainPendingItems()` could find an empty queue and exit before `enqueue()` was called.

5. **Pool lifecycle**: The snapshot pool (`CachedThreadPool` with daemon threads) lives for the lifetime of the handler — it is never shut down and recreated. When the last item unsubscribes, `shutdownSnapshotConsumers()` wakes up active consumers via `TopicSnapshotConsumer.shutdown()`, but the pool threads are reaped naturally after 60 seconds of inactivity.

#### State Machine (per topic)

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

### 3.4 Configuration

#### New Parameters

| Parameter | Key | Type | Default | Description |
|---|---|---|---|---|
| Snapshot Enable | `snapshot.enable` | `BOOL` | `false` | Enables snapshot delivery from topic beginning on subscription |
| Snapshot Timeout | `snapshot.timeout.ms` | `NON_NEGATIVE_INT` | `0` (no timeout) | Maximum duration in milliseconds per snapshot pass. `0` means no limit |

#### Impact on `isSnapshotAvailable()`

```java
// Current (temporary — snapshot always enabled)
public boolean isSnapshotAvailable(String itemName) {
    return true;
}

// Target (when snapshot.enable configuration is implemented)
public boolean isSnapshotAvailable(String itemName) {
    return connectorConfig.getCommandModeStrategy().manageSnapshot()
        || connectorConfig.isSnapshotEnabled();
}
```

#### Snapshot Consumer Kafka Properties

The snapshot consumer inherits all Kafka properties (SSL, SASL, Schema Registry, etc.) from the main consumer configuration, with the following overrides:

| Property | Override Value | Reason |
|---|---|---|
| `group.id` | Not set | Uses `assign()`, no consumer group needed |
| `enable.auto.commit` | `false` | Ephemeral consumer, no offset management |
| `auto.offset.reset` | `earliest` | Redundant with `seekToBeginning()` but safe default |

---

## 4. Event Ordering Guarantees

### The Deduplication Problem

Since the main consumer and snapshot consumer read independently, the same record could appear in both the snapshot and the real-time stream if it was produced between snapshot start and the main consumer's first poll.

#### Solution: Natural Deduplication via Compacted Topics

For compacted topics, duplicates are **semantically safe**: the client receives the same key twice with the same (or newer) value. Lightstreamer's update mechanism handles this correctly — a duplicate update for the same fields is a no-op from the client's perspective.

### Event Ordering Guarantee

```
Timeline:
  t0: Subscription created, queue starts buffering
  t1: Main consumer starts (real-time events → queued)
  t2: SnapshotConsumer starts (reads from beginning)
  t3: SnapshotConsumer delivers snapshot events (isSnapshot=true)
  t4: SnapshotConsumer completes → endOfSnapshot()
  t5: enableRealtimeEvents() → drains queue (isSnapshot=false)
  t6: Direct real-time dispatch from main consumer
```

All snapshot events are delivered before any real-time events. The `pendingRealtimeEvents` queue ensures no real-time events are lost or reordered.

---

## 5. Subscription Lifecycle Comparison

### Without Snapshot (current behavior)

| Step | Action |
|---|---|
| 1 | Client subscribes → `SubscribedItem` created |
| 2 | `enableRealtimeEvents()` called immediately |
| 3 | If first item: start main consumer |
| 4 | Records → `sendRealtimeEvent()` → client |

### With Snapshot (proposed)

| Step | First Subscription | Subsequent Subscriptions (same topic) |
|---|---|---|
| 1 | `SubscribedItem` created (queue buffering) | `SubscribedItem` created (queue buffering) |
| 2 | Start main consumer (async, from latest) | Main consumer already running |
| 3 | Create `TopicSnapshotConsumer`, enqueue item, start thread | Enqueue item into existing `TopicSnapshotConsumer` |
| 4 | Snapshot pass reads from beginning, routes to **all** pending items | Item served in current or next pass |
| 5 | `endOfSnapshot()` per item → Lightstreamer knows snapshot is done | Same |
| 6 | `enableRealtimeEvents()` per item → drain buffered events | Same |
| 7 | Direct real-time dispatch | Direct real-time dispatch |

---

## 6. Affected Files Summary

| File | Change Type | Status | Description |
|---|---|---|---|
| `TopicSnapshotConsumer.java` | **New** | **Done** | Shared per-topic consumer with timeout, multi-pass coordination, `DeserializerPair`, structured `doPoll()`, wakeup-based `shutdown()`, `ItemFinalizationCallback` for caller-coordinated finalization, tombstone skipping, `AUTO` mode event decoration |
| `CommandMode.java` | **New** | **Done** | Extracted from `RecordConsumerSupport` to `adapters.consumers` package; provides `decorate()`, `deleteEvent()`, `Command` and `Key` enums shared by both `processor` and `snapshot` sub-packages |
| `RecordMapper.java` | Modified | **Done** | Added `RecordMapper.from(ItemTemplates, FieldsExtractor)` convenience factory method |
| `RecordConsumerSupport.java` | Modified | **Done** | Removed nested `CommandMode` interface; imports extracted `CommandMode` from `adapters.consumers` package |
| `SubscriptionsHandler.java` | Modified | **Done** | Defers `enableRealtimeEvents()`; manages per-topic `TopicSnapshotConsumer` map; per-item topic countdown via `pendingSnapshotTopics` map; `onItemSnapshotComplete()` callback; submit-after-enqueue pattern; `RecordMapper` and `DeserializerPair` centralized in constructor; regex topic guard |
| `Items.java` | Modified | **Done** | Added `topicsFor(SubscribedItem)` and `isRegexEnabled()` to `ItemTemplates` interface and `DefaultItemTemplates`; used by `enqueueForSnapshot()` to resolve matching topics and guard against regex subscriptions |
| `TopicConfigurations.java` | Modified | **Done** | Added `isRegexEnabled()` method to expose whether topic mappings use regex patterns |
| `StreamingDataAdapter.java` | Modified | **Done** | Refactored to use `RecordMapper.from(ItemTemplates, FieldsExtractor)` convenience factory |
| `KafkaConsumerWrapper.java` | Modified | **Done** | Accepts `RecordMapper` as constructor parameter (was built internally) |
| `KafkaConnectorDataAdapter.java` | Modified | **Done** | `isSnapshotAvailable()` returns `true` (temporary — TODO: gate on `snapshot.enable`) |
| `ConnectorConfig.java` | Modified | **Pending** | Add `snapshot.enable` and `snapshot.timeout.ms` parameters |
| `ConnectorConfigurator.java` | Modified | **Pending** | Pass snapshot config to consumer config; validate KEY-only templates and non-regex topics |
| `KafkaConsumerWrapperConfig.java` | Modified | **Pending** | Expose snapshot flag and timeout in `Config` record |

---

## 7. Design Decisions and Open Questions

### 7.1 Scope of `snapshot.enable` — DECIDED

Adapter-wide. A single boolean flag enables snapshot for all topic mappings in the adapter.

> Per-topic control (`map.<topic>.snapshot=true`) can be added in a future iteration if needed.

### 7.2 Snapshot for Non-Parameterized Items — DECIDED

If an item has no template parameters (e.g., a plain `stocks` item mapped to a compacted topic), the snapshot delivers **all** records from the topic — but this is not useful in MERGE mode.

Consider topic `stocks` (compacted) with 3 keys:

```
key=AAPL → {symbol: "AAPL", price: 150}
key=GOOG → {symbol: "GOOG", price: 2800}
key=TSLA → {symbol: "TSLA", price: 900}
```

Template: `map.stocks.to = stocks` (no parameters). A client subscribes to `stocks`:

```
poll → key=AAPL → canonical = "stocks" → sendSnapshotEvent({symbol:AAPL, price:150})
poll → key=GOOG → canonical = "stocks" → sendSnapshotEvent({symbol:GOOG, price:2800})
poll → key=TSLA → canonical = "stocks" → sendSnapshotEvent({symbol:TSLA, price:900})
endOfSnapshot()
```

All three records produce the same canonical name (`stocks`) because the template has no parameters to differentiate them. In MERGE mode, each `sendSnapshotEvent()` overwrites the previous values on the same Lightstreamer item:

```
After event 1: client sees {symbol: AAPL, price: 150}
After event 2: client sees {symbol: GOOG, price: 2800}   ← AAPL is gone
After event 3: client sees {symbol: TSLA, price: 900}     ← GOOG is gone
Final snapshot = {symbol: TSLA, price: 900}  ← arbitrary, depends on poll order
```

The client ends up with only the **last record polled** — which key that is depends on partition/poll order across partitions. The snapshot of a 3-key topic collapsed into 1 arbitrary key.

The only mode where delivering all records to a plain item makes sense is **COMMAND mode** (`ADD`/`UPDATE`/`DELETE`), where each record represents a row in a table. But COMMAND mode has its own snapshot lifecycle (CS/EOS records) and is mutually exclusive with this feature (see Section 7.7).

> **Note on DISTINCT mode:** In theory, a client subscribing in DISTINCT mode would receive each `sendSnapshotEvent()` as a separate, individually visible event — no overwriting occurs. The full topic content would actually reach the client. However, we apply the exclusion regardless of subscription mode because: (a) the adapter does not know which mode the client will use at subscription time, (b) dumping an entire compacted topic into a single non-parameterized item is an edge case better served by the future Snapshot Cache design, and (c) a mode-independent rule is simpler to reason about and validate. This decision can be revisited if a concrete use case emerges.

> **Decision:** Snapshot requires templates with at least one `KEY.*` parameter. Non-parameterized items fall back to immediate real-time mode when snapshot is enabled. This is validated at configuration time.

### 7.3 Large Compacted Topics

The `TopicSnapshotConsumer` must read **every record** from the beginning of the topic, even though the client may care about only 1 key out of millions. Routing via `RecordMapper` filters at the application level — but the I/O and deserialization have already happened.

#### Impact at Scale

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

#### Mitigation A — Partition Pruning (recommended for v1)

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

#### Mitigation B — Timeout (recommended for v1)

Configurable maximum duration per snapshot pass. If the timeout fires before all partitions are consumed:

- Deliver `endOfSnapshot()` for items that received at least one matching event (best-effort snapshot)
- Deliver `endOfSnapshot()` for items that received nothing (empty snapshot — client transitions to real-time only)
- Log a warning

```
snapshot.timeout.ms=30000    # Max 30 seconds per pass
```

**Trade-off:** The client may receive an incomplete snapshot but will catch up via real-time updates. For compacted topics this is acceptable — the "missing" snapshot data will eventually arrive as a real-time update if the key is updated.

#### Mitigation C — Early Completion per Item (future)

Within a pass, once an item has received its snapshot event(s), call `endOfSnapshot()` + `enableRealtimeEvents()` for that item immediately without waiting for the full pass to finish. The pass continues for remaining items.

```
Pass reading 1M records:
  - At record 500:     item "stock-[symbol=AAPL]"  matched → endOfSnapshot(AAPL) immediately
  - At record 12000:   item "stock-[symbol=GOOG]"  matched → endOfSnapshot(GOOG) immediately
  - At record 999999:  pass complete, remaining items get empty endOfSnapshot()
```

**Caveat:** On a compacted topic with compaction lag, the same key may appear multiple times. Completing early means the client gets an older value, then later receives the newer value as a real-time update (since `enableRealtimeEvents()` was already called). This is **semantically correct** but the snapshot is stale.

#### Mitigation D — In-Memory Snapshot Cache (future)

Cache the fully-read compacted state in memory (`Map<canonicalItemName, Map<String,String>>`). Subsequent subscriptions look up the cache instead of re-reading the topic.

| Pros | Cons |
|---|---|
| Instant snapshot for cached items | Memory proportional to topic size |
| No repeated topic reads | Staleness: cache must be invalidated/refreshed |
| | Couples snapshot lifecycle to adapter lifecycle |

This is essentially building a **materialized view** of the compacted topic. Powerful but significantly more complex.

#### Mitigation Summary

| Mitigation | Complexity | Impact | Included in v1 |
|---|---|---|---|
| **Partition pruning** | Medium | High (÷ partition count) | No — deferred to v1.1 |
| **Timeout** | Low | Medium (safety net) | Yes — implemented |
| **Early completion** | Medium | High (reduces client latency) | No — future iteration |
| **Snapshot cache** | High | Very high | No — future iteration |

> **Current v1 state:** Timeout is implemented (`snapshot.timeout.ms`, default `Duration.ZERO` = no limit). Partition pruning is deferred — requires mapping subscribed item key values back to partition numbers, which depends on knowing the partitioner. Can be added without changes to `TopicSnapshotConsumer`'s external interface.

### 7.4 Error Handling — DECIDED

If the snapshot consumer fails mid-way (e.g., deserialization error, Kafka unavailable): **skip snapshot and fall back to real-time only**. Log a warning and finalize all pending items via `drainAndFinalizeAll()`, which delegates to the `ItemFinalizationCallback` for each item. The callback triggers the per-item countdown in `SubscriptionsHandler.onItemSnapshotComplete()`, and when the counter reaches zero, `endOfSnapshot()` with an empty snapshot and `enableRealtimeEvents()` are called. This is implemented in `TopicSnapshotConsumer.run()` via the `drainAndFinalizeAll()` fallback in all catch blocks.

### 7.5 Consumer Reuse — DECIDED

The design uses a **shared consumer per topic** (`TopicSnapshotConsumer`). Each topic has at most one active snapshot consumer at a time. All pending subscribed items for that topic are served in the same pass, with late arrivals handled via subsequent passes.

This resolves the scalability concern: 100 subscriptions on the same topic require 1 Kafka consumer and 1–few topic reads, not 100.

#### Future Optimization: Accumulation Window

If profiling shows excessive passes due to rapid-fire subscriptions arriving just after a pass starts, an optional accumulation delay (e.g., `snapshot.accumulation.ms=500`) could batch subscriptions before starting each pass. This is **not included in the initial implementation** to keep the design simple.

### 7.6 Continuous Tailing — DECIDED (Not in v1)

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

> **Decision:** Keep `TopicSnapshotConsumer` ephemeral. When repeated full reads become a bottleneck, implement the Snapshot Cache as a layered optimization.

### 7.7 COMMAND Mode Mutual Exclusion — DECIDED

The compacted-topic snapshot (`TopicSnapshotConsumer`) and COMMAND mode (`CommandModeStrategy.ENFORCE`) manage snapshot lifecycle through **independent, incompatible mechanisms**:

| Aspect | Compacted-Topic Snapshot | COMMAND Mode Snapshot |
|---|---|---|
| **Who controls lifecycle** | The adapter (reads from beginning, signals EOS) | The Kafka producer (sends CS/EOS control records) |
| **Snapshot start** | On subscribe → `TopicSnapshotConsumer` reads topic | On CS record in the main consumer stream |
| **Snapshot end** | `endOfSnapshot()` when end offsets reached | `endOfSnapshot()` when EOS record received |
| **Real-time enable** | `enableRealtimeEvents()` after snapshot pass | Implicit (main consumer continues) |
| **Item state management** | `sendSnapshotEvent()` → `endOfSnapshot()` → `enableRealtimeEvents()` | `clearSnapshot()` → `setSnapshot(true)` → `endOfSnapshot()` |

If both are active on the same item, two independent actors call `endOfSnapshot()`, `setSnapshot()`, and `enableRealtimeEvents()` concurrently — a race that corrupts the item's snapshot state.

> **Decision:** `snapshot.enable` and `CommandModeStrategy.ENFORCE` are mutually exclusive. Validated at configuration time.

#### `CommandModeStrategy.AUTO` Compatibility

`AUTO` mode does **not** manage snapshot lifecycle — `manageSnapshot()` returns `false`, and it does not process CS/EOS control records. There is no lifecycle race with `TopicSnapshotConsumer`. However, `AUTO` decorates real-time events with `command=ADD` (or `command=DELETE` for null payloads). To maintain consistent event schema between snapshot and real-time delivery, `TopicSnapshotConsumer` applies two mode-aware behaviors:

1. **Tombstone skipping (all modes).** Records with `value == null` (tombstones) are skipped during snapshot passes. A tombstone signals that a key was deleted from the compacted topic — there is nothing to snapshot for that key. Kafka retains tombstones for `delete.retention.ms` (default 24h) before removing them, so they will be encountered during snapshot passes. Without this skip, the client would receive an event with empty/partial value fields for a key that no longer exists.

2. **Event decoration (AUTO only).** When `CommandModeStrategy == AUTO`, snapshot events are decorated with `command=ADD` via `CommandMode.decorate(fields, Command.ADD)`. This ensures the client receives the same event schema in both snapshot and real-time paths:

```
// AUTO mode — consistent schema:
Snapshot:   {key: "AAPL", symbol: "AAPL", price: 150, command: "ADD"}
Real-time:  {key: "AAPL", symbol: "AAPL", price: 175, command: "ADD"}

// Without decoration — schema mismatch:
Snapshot:   {key: "AAPL", symbol: "AAPL", price: 150}                ← missing command
Real-time:  {key: "AAPL", symbol: "AAPL", price: 175, command: "ADD"}
```

The `CommandMode` utility interface (containing `decorate()`, `Command`, `Key`) was extracted from `RecordConsumerSupport` to the shared `adapters.consumers` package so both `processor` and `snapshot` sub-packages can use it without cyclic dependencies.

| Strategy | `manageSnapshot()` | Lifecycle conflict | Snapshot behavior |
|---|---|---|---|
| `NONE` | `false` | None | Raw `fieldsMap()`, tombstones skipped |
| `AUTO` | `false` | None | `fieldsMap()` + `command=ADD`, tombstones skipped |
| `ENFORCE` | `true` | **Yes — excluded** | N/A (mutually exclusive with `snapshot.enable`) |

#### Consolidated Enabling Conditions

All four conditions must be met for the compacted-topic snapshot to be active:

| # | Condition | Validated At | Rationale |
|---|---|---|---|
| 1 | `snapshot.enable = true` | Config time | Opt-in feature flag |
| 2 | `CommandModeStrategy != ENFORCE` | Config time | Mutual exclusion with COMMAND mode snapshot |
| 3 | Template has at least one `KEY.*` parameter (no `VALUE.*` parameters) | Config time | Ensures canonical item name aligns with compaction key; non-parameterized items collapse all records into one item (Section 7.2) |
| 4 | Literal topic names (no regex) | Config time | `TopicSnapshotConsumer` requires concrete topic names for `assign()` (Section 10) |

```java
// Validation pseudocode in ConnectorConfigurator
if (snapshotEnabled) {
    if (commandModeStrategy == ENFORCE) {
        throw new ConfigException(
            "snapshot.enable is incompatible with command mode ENFORCE. "
            + "COMMAND mode manages its own snapshot lifecycle via CS/EOS records.");
    }
    if (topicMappings.isRegexEnabled()) {
        throw new ConfigException(
            "Snapshot mode requires literal topic names.");
    }
    for (TemplateExpression template : itemTemplates) {
        if (template.parameters().isEmpty()) {
            throw new ConfigException(
                "Snapshot mode requires at least one KEY parameter in each item template. "
                + "Non-parameterized items cannot produce meaningful snapshots.");
        }
        for (ParameterExpression param : template.parameters()) {
            if (param.referencesValue()) {
                throw new ConfigException(
                    "Snapshot mode requires all item template parameters to reference "
                    + "KEY fields only.");
            }
        }
    }
}
```

---

## 8. Non-Compacted Topics — Out of Scope for v1

The design assumes compacted topics throughout. Several foundational assumptions break on non-compacted (retention-based) topics. This section documents why and outlines what would be required for future support.

### 8.1 Fundamental Difference

| Aspect | Compacted Topic | Non-Compacted Topic |
|---|---|---|
| **What "from beginning" yields** | Current state (latest value per key) | Full event history (every version of every key) |
| **Bounded by** | Unique key count | Retention time × write rate |
| **Record count** | ~N (unique keys) | Unbounded within retention |
| **Duplicate keys** | Collapsed to latest by Kafka | All versions retained |

On a compacted topic, `seekToBeginning()` → `endOffsets()` reads the **state**. On a non-compacted topic, the same operation reads the **entire history** — a fundamentally different semantic.

### 8.2 Impact on Design Components

#### Snapshot Semantics — what does the client receive?

**Compacted:** One snapshot event per matching key (the latest value). Well-defined "current state."

**Non-compacted:** Every matching record ever written within retention. If `AAPL` had 10,000 price updates in 7 days, the client gets 10,000 snapshot events — almost certainly not what the client wants.

Would require either:
- **Last-value-per-key deduplication** during the snapshot pass (maintain `Map<key, latestValue>`, emit only at pass end)
- **Configurable snapshot depth** ("last 1 record per key", or "last N seconds")
- Both amount to **building compaction logic in the adapter**

#### Data Volume

| Topic | Records to Read | Time (12 partitions, 1MB/s each) |
|---|---|---|
| Compacted, 1M keys, ~1KB each | ~1M | ~80s |
| Non-compacted, 1M keys, 100 updates each, 7-day retention | ~100M | ~2.3 hours |

The `TopicSnapshotConsumer` multi-pass design becomes impractical. Even a single pass may not complete within any reasonable timeout.

#### Deduplication Safety

Section 4 of this document states:

> *For compacted topics, duplicates are semantically safe: the client receives the same key twice with the same (or newer) value.*

On non-compacted topics this breaks. The snapshot consumer may deliver an old value for key X, while the main consumer's queue holds a newer value. After `endOfSnapshot()` + `enableRealtimeEvents()`, the client sees:

```
Snapshot:  AAPL → {price: 100}  (from 3 days ago)
Real-time: AAPL → {price: 175}  (from today)
```

The client converges to the correct value, but if the snapshot replays all historical values in order, the client sees a confusing replay of the full price history before settling.

#### Partition Pruning

Still reduces the partition set, but each partition contains the full history. Savings are proportional to partition count, not to total volume per key — much less effective.

#### Snapshot Cache

For compacted topics, the cache is a simple `put(key, latest)` — Kafka already deduplicated. For non-compacted topics, the cache must implement its own deduplication during load, effectively **performing compaction in memory**. The initial load reads vastly more data to arrive at the same result.

### 8.3 Summary

| Design Element | Compacted | Non-Compacted | Gap |
|---|---|---|---|
| "From beginning" semantics | Current state | Full history | Needs deduplication or depth limit |
| Records to read | ~N (keys) | ~N × updates × retention | Orders of magnitude more I/O |
| Snapshot events per item | 1 (latest value) | All matching records | Needs last-value-only or depth config |
| Duplicate safety | Safe (same/newer) | Unsafe (stale history replayed) | Needs deduplication layer |
| Partition pruning | Effective | Less effective | Volume per partition is large |
| Timeout/limits | Safety net | Practically mandatory | Must be default-on |
| Cache initial load | Fast (bounded by keys) | Slow (bounded by retention) | In-memory compaction required |

### 8.4 What Non-Compacted Support Would Require

If needed in a future iteration, non-compacted topic snapshot would require a **deduplication layer** added to `TopicSnapshotConsumer`:

1. During the pass, maintain a `Map<String, Map<String, String>>` (canonicalItemName → latest fieldsMap)
2. On each record: `map.put(canonicalName, fields)` — overwriting previous values for the same key
3. After `endOffsets()` reached: emit one `sendSnapshotEvent()` per entry in the map, then `endOfSnapshot()`

This is essentially building Kafka compaction in the adapter. It adds memory proportional to unique matched keys (manageable) but read I/O proportional to the full topic history (potentially huge).

Alternative: use Kafka's consumer `offsetsForTimes()` API to start from a recent timestamp instead of the beginning, limiting the read window. This trades completeness for speed.

> **Decision:** Snapshot support in v1 is scoped to compacted topics. Non-compacted support is deferred pending concrete customer requirements and acceptable trade-off definition.

---

## 9. Canonical Item / Compaction Key Alignment

Kafka compaction deduplicates based on the **record key** (raw serialized bytes). One surviving record per unique key. Canonical item names, however, are derived from template expressions that can reference **any part** of the record — `KEY.*`, `VALUE.*`, or both. The snapshot design is only correct when these two concepts are aligned.

### 9.1 Case Analysis

#### Case 1: Template params extract from KEY only, params = full key — CORRECT

```
Template: stock-#{symbol=KEY.symbol}
Key:      {symbol: "AAPL"}
Value:    {price: 150}
```

- Compaction key and canonical item name are **perfectly aligned**: one unique key → one canonical item → one snapshot event
- Partition pruning works (key fully determines partition)
- **This is the ideal case. Snapshot works exactly as designed.**

#### Case 2: Template params extract from KEY only, params ⊂ key — CONSISTENT

```
Template: stock-#{symbol=KEY.symbol}
Key:      {symbol: "AAPL", exchange: "NASDAQ"}
Value:    {price: 150}
```

The template extracts only `symbol` from the key, ignoring `exchange`. Two records with different keys can produce the **same** canonical item:

```
Record A:  key={symbol:"AAPL", exchange:"NASDAQ"}, value={price:150}
Record B:  key={symbol:"AAPL", exchange:"NYSE"},   value={price:151}
```

From Kafka's perspective these are **two different keys** — compaction keeps both. From the template's perspective both produce `stock-[symbol=AAPL]`. During the snapshot pass, both route to the same subscribed item. The client receives two snapshot events; the second overwrites the first. Which value the client ends up with depends on poll/partition order.

**However, this is not a snapshot-specific problem.** The same non-determinism already exists in real-time mode today:

```
Real-time Record A arrives: client sees {price: 150}
Real-time Record B arrives: client sees {price: 151}
```

The order depends on partition/poll timing regardless of whether it's snapshot or real-time. The snapshot behavior is **consistent** with the existing real-time behavior — it doesn't make things worse.

**This is a template design concern, not a snapshot concern.** If `{AAPL, NASDAQ}` and `{AAPL, NYSE}` represent different entities, the template should be `stock-#{symbol=KEY.symbol,exchange=KEY.exchange}` to distinguish them. If the user intentionally collapses them (they don't care about exchange), the non-determinism is acceptable.

#### Case 3: Template params extract from VALUE only — BROKEN

```
Template: stock-#{symbol=VALUE.symbol}
Key:      (some UUID or unrelated key)
Value:    {symbol: "AAPL", price: 150}
```

There is **no correlation** between the compaction key and the canonical item name. Multiple records with different Kafka keys but the same `VALUE.symbol` all survive compaction and all map to the same item. Effectively equivalent to a **non-compacted topic** from the snapshot perspective — the pass reads everything and delivers multiple events per item.

**Snapshot is broken in spirit even though the topic is technically compacted.**

#### Case 4: Template params extract from both KEY and VALUE — SILENTLY WRONG

```
Template: item-#{id=KEY.id,status=VALUE.status}
Key:      {id: 1}
```

The canonical item name changes as the value changes:

```
t0: key={id:1}, value={status:"ACTIVE"}  → item-[id=1,status=ACTIVE]
t1: key={id:1}, value={status:"CLOSED"}  → item-[id=1,status=CLOSED]
```

Compaction keeps only the t1 record (same key). A client subscribed to `item-[id=1,status=ACTIVE]` would **never** get a snapshot match — the record that would have matched was compacted away. The snapshot is empty, which is **silently incorrect**.

This is the most dangerous case because it produces wrong results without any error or warning.

#### Case 5: No template parameters (plain item name) — CORRECT but delivers all records

```
Mapping: map.stocks.to=all-stocks
```

Every record routes to the same item. Snapshot delivers ALL records from the compacted topic. This could be intentional (e.g., COMMAND mode where each record represents a row in a table with ADD/UPDATE/DELETE semantics).

### 9.2 Summary Matrix

| Template Type | Example | Snapshot Correctness | Partition Pruning | Notes |
|---|---|---|---|---|
| KEY only, params = full key | `#{s=KEY.s}` | **Correct** | Works | Ideal case |
| KEY only, params ⊂ key | `#{s=KEY.s}` (key has extra fields) | **Consistent** — same as real-time | Partial | Non-determinism is pre-existing, not snapshot-specific |
| VALUE only | `#{s=VALUE.s}` | **Broken** — behaves like non-compacted | N/A | Compaction doesn't help |
| KEY + VALUE | `#{id=KEY.id,st=VALUE.st}` | **Silently wrong** — matching records compacted away | N/A | Most dangerous |
| No params | `all-stocks` | **Correct** — all records delivered | N/A | Useful in COMMAND mode |

### 9.3 v1 Constraints

When `snapshot.enable=true`, the adapter validates at configuration time:

1. **KEY-only template parameters.** All item template parameters must reference only `KEY.*` expressions. Templates with `VALUE.*` parameters are rejected with a clear error message. This ensures only Cases 1, 2, and 5 are allowed — all of which produce correct or consistent snapshot behavior.

2. **Literal topic names (no regex).** Topic mappings must use literal topic names, not regular expressions. `TopicSnapshotConsumer` uses `consumer.assign()` with specific `TopicPartition` objects discovered via `partitionsFor(topicName)` — this requires a concrete topic name. With regex-enabled topic subscriptions, the configured entries are patterns (e.g., `stock.*`), and the actual matching topics are only known at runtime after the main consumer resolves them. Supporting regex topics would require a runtime topic discovery step (`listTopics()` + pattern matching) before launching snapshot consumers — deferred to a future iteration.

```
// Validation pseudocode in ConnectorConfig
if (snapshotEnabled) {
    if (topicMappings.isRegexEnabled()) {
        throw new ConfigException(
            "Snapshot mode requires literal topic names. "
            + "Regex-based topic subscriptions are not supported with snapshot.enable=true.");
    }

    for (TemplateExpression template : itemTemplates) {
        for (ParameterExpression param : template.parameters()) {
            if (param.referencesValue()) {
                throw new ConfigException(
                    "Snapshot mode requires all item template parameters to reference KEY fields only. "
                    + "Parameter '%s' references VALUE, which is incompatible with compacted topic snapshot semantics."
                    .formatted(param.name()));
            }
        }
    }
}
```

> **Decision:** v1 enforces KEY-only template parameters and literal (non-regex) topic names when snapshot is enabled. Both are validated at configuration time.

---

## 10. Regex Topic Subscriptions — v2 Improvement

### 10.1 Why Regex Is Excluded from v1

The exclusion of regex-based topic subscriptions from v1 snapshot support is not merely an implementation gap — it is a **deliberate resource safety boundary**.

`TopicSnapshotConsumer` uses `consumer.assign()` with specific `TopicPartition` objects, which requires literal topic names. Kafka's only regex-aware API is `consumer.subscribe(Pattern)`, which is tied to consumer groups and mutually exclusive with `assign()`. Supporting regex topics therefore requires **manual topic resolution**: calling `consumer.listTopics()`, filtering by pattern, and creating one `TopicSnapshotConsumer` per matched topic.

The problem is that a single regex pattern can match an unbounded number of topics. A pattern like `stock.*` matching 500 topics would launch **500 concurrent snapshot consumers**, each with its own:

- TCP connection to the broker cluster
- Dedicated thread in the snapshot pool
- Full-topic read from beginning to end offsets

This creates an explosion of broker I/O, network connections, and memory that scales with the number of matched topics — a resource profile fundamentally different from the literal topic case, where the count is bounded by explicit configuration (typically single digits).

### 10.2 Future Approaches

| Approach | Snapshot consumers | Broker connections | Trade-off |
|---|---|---|---|
| **One consumer per matched topic** (naive) | Up to N (matched topics) | N | Simple but unbounded resource usage |
| **Bounded pool with topic queue** | At most `pool-size` (e.g., 5) | `pool-size` | Caps resources; longer total snapshot time as topics are processed in waves |
| **Single multi-topic consumer** | 1 | 1 | Most efficient; significantly more complex pass logic (per-topic end-offset tracking, per-topic item routing) |

#### Recommended: Bounded Pool with Topic Queue

A fixed-size `ExecutorService` (e.g., 5 threads) processes matched topics in waves. Remaining topics are queued and served as pool slots become available:

```
Pattern "stock.*" matches 500 topics
  → Resolve via listTopics() + pattern matching
  → Queue all 500 topics
  → Process 5 at a time via bounded thread pool
  → Each completes → next topic dequeued
  → Total: 100 waves × 5 concurrent consumers
```

This provides:
- **Bounded resource usage** regardless of how many topics the pattern matches
- **Configurable concurrency** via a `snapshot.max.concurrent.topics` parameter
- **Same `TopicSnapshotConsumer` implementation** — no changes to per-topic pass logic
- **Predictable broker impact** — at most N concurrent full-topic reads

The primary cost is **longer total wall-clock time** for snapshot delivery across all topics, which is acceptable given the alternative is resource exhaustion.
