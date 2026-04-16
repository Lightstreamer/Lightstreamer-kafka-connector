# Snapshot Support for Compacted Topics — Design Document

## Table of Contents

- [1. Problem Statement](#1-problem-statement)
- [2. Current Architecture Summary](#2-current-architecture-summary)
- [3. Snapshot Flow Overview](#3-snapshot-flow-overview)
- [4. Event Ordering Guarantees](#4-event-ordering-guarantees)
- [5. Subscription Lifecycle Comparison](#5-subscription-lifecycle-comparison)
- [6. Configuration](#6-configuration)
  - [6.1 Shared Parameters](#61-shared-parameters)
  - [6.2 Impact on `isSnapshotAvailable()`](#62-impact-on-issnapshotavailable)
  - [6.3 Enabling Conditions](#63-enabling-conditions)
- [7. Design Decisions](#7-design-decisions)
  - [7.1 Scope of `snapshot.enable`](#71-scope-of-snapshotenable--decided)
  - [7.2 Snapshot for Non-Parameterized Items](#72-snapshot-for-non-parameterized-items--decided)
  - [7.3 Error Handling](#73-error-handling--decided)
  - [7.4 COMMAND Mode Mutual Exclusion](#74-command-mode-mutual-exclusion--decided)
- [8. Canonical Item / Compaction Key Alignment](#8-canonical-item--compaction-key-alignment)
  - [8.1 Case Analysis](#81-case-analysis)
  - [8.2 Summary Matrix](#82-summary-matrix)
  - [8.3 v1 Constraints](#83-v1-constraints)
- [9. Non-Compacted Topics — Out of Scope for v1](#9-non-compacted-topics--out-of-scope-for-v1)
  - [9.1 Fundamental Difference](#91-fundamental-difference)
  - [9.2 Impact on Design Components](#92-impact-on-design-components)
  - [9.3 Summary](#93-summary)
  - [9.4 What Non-Compacted Support Would Require](#94-what-non-compacted-support-would-require)
- [10. Regex Topic Subscriptions — Out of Scope for v1](#10-regex-topic-subscriptions--out-of-scope-for-v1)
  - [10.1 Why Regex Is Excluded from v1](#101-why-regex-is-excluded-from-v1)
  - [10.2 Future Approaches](#102-future-approaches)
- [11. Snapshot Delivery Alternatives](#11-snapshot-delivery-alternatives)
  - [11.1 TopicSnapshotConsumer (v1)](#111-topicsnapshotconsumer-v1)
  - [11.2 Snapshot Cache (future)](#112-snapshot-cache-future)
  - [11.3 Comparison](#113-comparison)
  - [11.4 When to Use](#114-when-to-use)
- [12. Affected Files Summary](#12-affected-files-summary)

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

This concept is central to the snapshot design: the snapshot mechanism reads records, maps them to canonical names via `RecordMapper`, and routes matching records to subscribed items.

### Scope

This feature targets **compacted topics only** in v1, with **literal (non-regex) topic names** and **KEY-only item template parameters**. The design relies on properties unique to compacted topics — bounded data volume (one value per key), "read from beginning = current state" semantics, and safe duplicate delivery. Non-compacted topics break these assumptions; see [Section 9](#9-non-compacted-topics--out-of-scope-for-v1) for a detailed analysis.

Additionally, snapshot correctness depends on the **alignment between Kafka's compaction key and the item template parameters**. See [Section 8](#8-canonical-item--compaction-key-alignment) for a detailed analysis of which template configurations produce correct snapshots. Regex topic subscriptions are excluded from v1 due to resource safety concerns; see [Section 10](#10-regex-topic-subscriptions--out-of-scope-for-v1) for the rationale and future approach.

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

## 3. Snapshot Flow Overview

Regardless of the delivery mechanism, the snapshot flow follows the same pattern:

```
Client subscribes to stock-[symbol=AAPL]
        │
        ▼
  SubscribedItem created (pendingRealtimeEvents queue starts buffering)
        │
        ├──────────────────────────────────────┐
        ▼                                      ▼
  Main Consumer                         Snapshot Delivery
  (real-time, from latest)              ┌─────────────────────────┐
  Events → queued in memory             │ Resolve snapshot data   │
                                        │ for the subscribed item │
                                        └────────────┬────────────┘
                                                     │
                                        sendSnapshotEvent(data)
                                        endOfSnapshot()
                                        enableRealtimeEvents()
                                        → drain buffered events
        │                                      │
        └──────────────────────────────────────┘
                          │
                          ▼
               Real-time events flow directly
```

The **Snapshot Delivery** box is where the two alternatives differ:

- **[TopicSnapshotConsumer](SNAPSHOT_CONSUMER_DESIGN.md)** (v1): A shared per-topic Kafka consumer reads the compacted topic from beginning, routes matching records to subscribed items. Multi-pass design handles late arrivals.
- **[Snapshot Cache](SNAPSHOT_CACHE_DESIGN.md)** (future): An in-memory materialized view of the compacted topic. Snapshot delivery is a synchronous `HashMap` lookup — sub-millisecond.

See [Section 11](#11-snapshot-delivery-alternatives) for a detailed comparison.

---

## 4. Event Ordering Guarantees

### The Deduplication Problem

Since the main consumer and snapshot mechanism read independently, the same record could appear in both the snapshot and the real-time stream if it was produced between snapshot start and the main consumer's first poll.

#### Solution: Natural Deduplication via Compacted Topics

For compacted topics, duplicates are **semantically safe**: the client receives the same key twice with the same (or newer) value. Lightstreamer's update mechanism handles this correctly — a duplicate update for the same fields is a no-op from the client's perspective.

### Event Ordering Guarantee

```
Timeline:
  t0: Subscription created, queue starts buffering
  t1: Main consumer starts (real-time events → queued)
  t2: Snapshot delivery begins (reads compacted state)
  t3: Snapshot events delivered (isSnapshot=true)
  t4: Snapshot complete → endOfSnapshot()
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

| Step | Action |
|---|---|
| 1 | `SubscribedItem` created (queue buffering starts) |
| 2 | If first item: start main consumer (async, from latest) — events buffered |
| 3 | Deliver snapshot data for the item (mechanism depends on alternative — see [Section 11](#11-snapshot-delivery-alternatives)) |
| 4 | `endOfSnapshot()` → Lightstreamer knows snapshot is done |
| 5 | `enableRealtimeEvents()` → drain buffered events |
| 6 | Direct real-time dispatch |

---

## 6. Configuration

### 6.1 Shared Parameters

| Parameter | Key | Type | Default | Description |
|---|---|---|---|---|
| Snapshot Enable | `snapshot.enable` | `BOOL` | `false` | Enables snapshot delivery from compacted topics on subscription |

Each delivery alternative defines its own additional parameters:
- **TopicSnapshotConsumer:** `snapshot.timeout.ms` — see [SNAPSHOT_CONSUMER_DESIGN.md](SNAPSHOT_CONSUMER_DESIGN.md#9-configuration)
- **Snapshot Cache:** `snapshot.cache.enable`, `snapshot.cache.max.entries` — see [SNAPSHOT_CACHE_DESIGN.md](SNAPSHOT_CACHE_DESIGN.md#9-configuration)

### 6.2 Impact on `isSnapshotAvailable()`

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

### 6.3 Enabling Conditions

All four conditions must be met for the compacted-topic snapshot to be active:

| # | Condition | Validated At | Rationale |
|---|---|---|---|
| 1 | `snapshot.enable = true` | Config time | Opt-in feature flag |
| 2 | `CommandModeStrategy != ENFORCE` | Config time | Mutual exclusion with COMMAND mode snapshot (Section 7.4) |
| 3 | Template has at least one `KEY.*` parameter (no `VALUE.*` parameters) | Config time | Ensures canonical item name aligns with compaction key; non-parameterized items collapse all records into one item (Section 7.2) |
| 4 | Literal topic names (no regex) | Config time | Snapshot requires concrete topic names for `assign()` (Section 10) |

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

## 7. Design Decisions

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

The only mode where delivering all records to a plain item makes sense is **COMMAND mode** (`ADD`/`UPDATE`/`DELETE`), where each record represents a row in a table. But COMMAND mode has its own snapshot lifecycle (CS/EOS records) and is mutually exclusive with this feature (see Section 7.4).

> **Note on DISTINCT mode:** In theory, a client subscribing in DISTINCT mode would receive each `sendSnapshotEvent()` as a separate, individually visible event — no overwriting occurs. The full topic content would actually reach the client. However, we apply the exclusion regardless of subscription mode because: (a) the adapter does not know which mode the client will use at subscription time, (b) dumping an entire compacted topic into a single non-parameterized item is an edge case better served by the Snapshot Cache design, and (c) a mode-independent rule is simpler to reason about and validate. This decision can be revisited if a concrete use case emerges.

> **Decision:** Snapshot requires templates with at least one `KEY.*` parameter. Non-parameterized items fall back to immediate real-time mode when snapshot is enabled. This is validated at configuration time.

### 7.3 Error Handling — DECIDED

If the snapshot mechanism fails mid-way (e.g., deserialization error, Kafka unavailable): **skip snapshot and fall back to real-time only**. Log a warning, deliver `endOfSnapshot()` with an empty snapshot, and call `enableRealtimeEvents()`. The client transitions to real-time updates immediately — it may miss the initial state but will converge to the correct value as updates arrive.

This policy applies to both delivery alternatives, though the failure mechanics differ (see each alternative's document for details).

### 7.4 COMMAND Mode Mutual Exclusion — DECIDED

The compacted-topic snapshot and COMMAND mode (`CommandModeStrategy.ENFORCE`) manage snapshot lifecycle through **independent, incompatible mechanisms**:

| Aspect | Compacted-Topic Snapshot | COMMAND Mode Snapshot |
|---|---|---|
| **Who controls lifecycle** | The adapter (reads from beginning, signals EOS) | The Kafka producer (sends CS/EOS control records) |
| **Snapshot start** | On subscribe → snapshot delivery mechanism | On CS record in the main consumer stream |
| **Snapshot end** | `endOfSnapshot()` when delivery completes | `endOfSnapshot()` when EOS record received |
| **Real-time enable** | `enableRealtimeEvents()` after snapshot delivery | Implicit (main consumer continues) |
| **Item state management** | `sendSnapshotEvent()` → `endOfSnapshot()` → `enableRealtimeEvents()` | `clearSnapshot()` → `setSnapshot(true)` → `endOfSnapshot()` |

If both are active on the same item, two independent actors call `endOfSnapshot()`, `setSnapshot()`, and `enableRealtimeEvents()` concurrently — a race that corrupts the item's snapshot state.

> **Decision:** `snapshot.enable` and `CommandModeStrategy.ENFORCE` are mutually exclusive. Validated at configuration time.

#### `CommandModeStrategy.AUTO` Compatibility

`AUTO` mode does **not** manage snapshot lifecycle — `manageSnapshot()` returns `false`, and it does not process CS/EOS control records. There is no lifecycle race with snapshot delivery. However, `AUTO` decorates real-time events with `command=ADD` (or `command=DELETE` for null payloads). To maintain consistent event schema between snapshot and real-time delivery, snapshot events are decorated with the same schema:

1. **Tombstone skipping (all modes).** Records with `value == null` (tombstones) are skipped during snapshot delivery. A tombstone signals that a key was deleted from the compacted topic — there is nothing to snapshot for that key. Kafka retains tombstones for `delete.retention.ms` (default 24h) before removing them, so they will be encountered during snapshot delivery. Without this skip, the client would receive an event with empty/partial value fields for a key that no longer exists.

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

---

## 8. Canonical Item / Compaction Key Alignment

Kafka compaction deduplicates based on the **record key** (raw serialized bytes). One surviving record per unique key. Canonical item names, however, are derived from template expressions that can reference **any part** of the record — `KEY.*`, `VALUE.*`, or both. The snapshot design is only correct when these two concepts are aligned.

### 8.1 Case Analysis

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

From Kafka's perspective these are **two different keys** — compaction keeps both. From the template's perspective both produce `stock-[symbol=AAPL]`. During snapshot delivery, both route to the same subscribed item. The client receives two snapshot events; the second overwrites the first. Which value the client ends up with depends on poll/partition order.

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

### 8.2 Summary Matrix

| Template Type | Example | Snapshot Correctness | Partition Pruning | Notes |
|---|---|---|---|---|
| KEY only, params = full key | `#{s=KEY.s}` | **Correct** | Works | Ideal case |
| KEY only, params ⊂ key | `#{s=KEY.s}` (key has extra fields) | **Consistent** — same as real-time | Partial | Non-determinism is pre-existing, not snapshot-specific |
| VALUE only | `#{s=VALUE.s}` | **Broken** — behaves like non-compacted | N/A | Compaction doesn't help |
| KEY + VALUE | `#{id=KEY.id,st=VALUE.st}` | **Silently wrong** — matching records compacted away | N/A | Most dangerous |
| No params | `all-stocks` | **Correct** — all records delivered | N/A | Useful in COMMAND mode |

### 8.3 v1 Constraints

When `snapshot.enable=true`, the adapter validates at configuration time:

1. **KEY-only template parameters.** All item template parameters must reference only `KEY.*` expressions. Templates with `VALUE.*` parameters are rejected with a clear error message. This ensures only Cases 1, 2, and 5 are allowed — all of which produce correct or consistent snapshot behavior.

2. **Literal topic names (no regex).** Topic mappings must use literal topic names, not regular expressions. Snapshot delivery requires concrete topic names for Kafka `assign()` — this requires a concrete topic name. With regex-enabled topic subscriptions, the configured entries are patterns (e.g., `stock.*`), and the actual matching topics are only known at runtime after the main consumer resolves them. See [Section 10](#10-regex-topic-subscriptions--out-of-scope-for-v1) for details.

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

## 9. Non-Compacted Topics — Out of Scope for v1

The design assumes compacted topics throughout. Several foundational assumptions break on non-compacted (retention-based) topics. This section documents why and outlines what would be required for future support.

### 9.1 Fundamental Difference

| Aspect | Compacted Topic | Non-Compacted Topic |
|---|---|---|
| **What "from beginning" yields** | Current state (latest value per key) | Full event history (every version of every key) |
| **Bounded by** | Unique key count | Retention time × write rate |
| **Record count** | ~N (unique keys) | Unbounded within retention |
| **Duplicate keys** | Collapsed to latest by Kafka | All versions retained |

On a compacted topic, `seekToBeginning()` → `endOffsets()` reads the **state**. On a non-compacted topic, the same operation reads the **entire history** — a fundamentally different semantic.

### 9.2 Impact on Design Components

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

Reading from beginning becomes impractical. Even a single pass may not complete within any reasonable timeout.

#### Deduplication Safety

Section 4 of this document states:

> *For compacted topics, duplicates are semantically safe: the client receives the same key twice with the same (or newer) value.*

On non-compacted topics this breaks. The snapshot may deliver an old value for key X, while the main consumer's queue holds a newer value. After `endOfSnapshot()` + `enableRealtimeEvents()`, the client sees:

```
Snapshot:  AAPL → {price: 100}  (from 3 days ago)
Real-time: AAPL → {price: 175}  (from today)
```

The client converges to the correct value, but if the snapshot replays all historical values in order, the client sees a confusing replay of the full price history before settling.

#### Partition Pruning

Still reduces the partition set, but each partition contains the full history. Savings are proportional to partition count, not to total volume per key — much less effective.

#### Snapshot Cache

For compacted topics, the cache is a simple `put(key, latest)` — Kafka already deduplicated. For non-compacted topics, the cache must implement its own deduplication during load, effectively **performing compaction in memory**. The initial load reads vastly more data to arrive at the same result.

### 9.3 Summary

| Design Element | Compacted | Non-Compacted | Gap |
|---|---|---|---|
| "From beginning" semantics | Current state | Full history | Needs deduplication or depth limit |
| Records to read | ~N (keys) | ~N × updates × retention | Orders of magnitude more I/O |
| Snapshot events per item | 1 (latest value) | All matching records | Needs last-value-only or depth config |
| Duplicate safety | Safe (same/newer) | Unsafe (stale history replayed) | Needs deduplication layer |
| Partition pruning | Effective | Less effective | Volume per partition is large |
| Timeout/limits | Safety net | Practically mandatory | Must be default-on |
| Cache initial load | Fast (bounded by keys) | Slow (bounded by retention) | In-memory compaction required |

### 9.4 What Non-Compacted Support Would Require

If needed in a future iteration, non-compacted topic snapshot would require a **deduplication layer**:

1. During the pass, maintain a `Map<String, Map<String, String>>` (canonicalItemName → latest fieldsMap)
2. On each record: `map.put(canonicalName, fields)` — overwriting previous values for the same key
3. After all records read: emit one `sendSnapshotEvent()` per entry in the map, then `endOfSnapshot()`

This is essentially building Kafka compaction in the adapter. It adds memory proportional to unique matched keys (manageable) but read I/O proportional to the full topic history (potentially huge).

Alternative: use Kafka's consumer `offsetsForTimes()` API to start from a recent timestamp instead of the beginning, limiting the read window. This trades completeness for speed.

> **Decision:** Snapshot support in v1 is scoped to compacted topics. Non-compacted support is deferred pending concrete customer requirements and acceptable trade-off definition.

---

## 10. Regex Topic Subscriptions — Out of Scope for v1

### 10.1 Why Regex Is Excluded from v1

The exclusion of regex-based topic subscriptions from v1 snapshot support is not merely an implementation gap — it is a **deliberate resource safety boundary**.

Both snapshot delivery alternatives use `consumer.assign()` with specific `TopicPartition` objects, which requires literal topic names. Kafka's only regex-aware API is `consumer.subscribe(Pattern)`, which is tied to consumer groups and mutually exclusive with `assign()`. Supporting regex topics therefore requires **manual topic resolution**: calling `consumer.listTopics()`, filtering by pattern, and creating one snapshot delivery instance per matched topic.

The problem is that a single regex pattern can match an unbounded number of topics. A pattern like `stock.*` matching 500 topics would launch **500 concurrent snapshot instances**, each with its own:

- TCP connection to the broker cluster
- Dedicated thread
- Full-topic read from beginning to end offsets

This creates an explosion of broker I/O, network connections, and memory that scales with the number of matched topics — a resource profile fundamentally different from the literal topic case, where the count is bounded by explicit configuration (typically single digits).

### 10.2 Future Approaches

| Approach | Snapshot instances | Broker connections | Trade-off |
|---|---|---|---|
| **One instance per matched topic** (naive) | Up to N (matched topics) | N | Simple but unbounded resource usage |
| **Bounded pool with topic queue** | At most `pool-size` (e.g., 5) | `pool-size` | Caps resources; longer total snapshot time as topics are processed in waves |
| **Single multi-topic instance** | 1 | 1 | Most efficient; significantly more complex pass logic (per-topic end-offset tracking, per-topic item routing) |

#### Recommended: Bounded Pool with Topic Queue

A fixed-size `ExecutorService` (e.g., 5 threads) processes matched topics in waves. Remaining topics are queued and served as pool slots become available:

```
Pattern "stock.*" matches 500 topics
  → Resolve via listTopics() + pattern matching
  → Queue all 500 topics
  → Process 5 at a time via bounded thread pool
  → Each completes → next topic dequeued
  → Total: 100 waves × 5 concurrent instances
```

This provides:
- **Bounded resource usage** regardless of how many topics the pattern matches
- **Configurable concurrency** via a `snapshot.max.concurrent.topics` parameter
- **Same delivery implementation** — no changes to per-topic logic
- **Predictable broker impact** — at most N concurrent full-topic reads

The primary cost is **longer total wall-clock time** for snapshot delivery across all topics, which is acceptable given the alternative is resource exhaustion.

---

## 11. Snapshot Delivery Alternatives

Two mutually exclusive snapshot delivery mechanisms are available, selected at configuration time. They share all the infrastructure described in this document (problem scope, constraints, event ordering, configuration validation) and differ only in **how** snapshot data is resolved and delivered to the subscribed item.

### 11.1 TopicSnapshotConsumer (v1)

A **shared, per-topic** Kafka consumer that reads the full compacted state from the beginning on each subscription batch. Multi-pass design handles late arrivals. Ephemeral — the consumer exits and releases resources when no more items are pending.

**Key properties:**
- Zero memory overhead (no cached state)
- Snapshot latency proportional to topic size
- Full topic read per subscription batch
- Drain-exit race (known limitation, fixable)

See **[SNAPSHOT_CONSUMER_DESIGN.md](SNAPSHOT_CONSUMER_DESIGN.md)** for the full design.

### 11.2 Snapshot Cache (future)

An **in-memory materialized view** of the compacted topic, loaded synchronously during `init()` and kept in sync via a continuous tailing thread. Snapshot delivery is a synchronous `ConcurrentHashMap` lookup — sub-millisecond.

**Key properties:**
- Memory proportional to topic size
- Sub-millisecond snapshot delivery (always)
- One-time startup cost (full topic read during `init()`)
- Eliminates multi-pass design, drain-exit race, snapshot thread pool

See **[SNAPSHOT_CACHE_DESIGN.md](SNAPSHOT_CACHE_DESIGN.md)** for the full design.

### 11.3 Comparison

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

### 11.4 When to Use

The v1 `TopicSnapshotConsumer` multi-pass design has a self-regulating property (see [SNAPSHOT_CONSUMER_DESIGN.md](SNAPSHOT_CONSUMER_DESIGN.md#4-pass-efficiency-analysis)): large topics absorb bursts naturally (few passes), and small topics are fast enough that many passes are negligible. The **medium-topic regime** (10K–100K keys, pass duration ≈ inter-arrival time) is where the cache provides the most value — it eliminates repeated reads entirely.

The choice between `TopicSnapshotConsumer` (v1) and the cache is made at **configuration time** via `snapshot.cache.enable`. They are mutually exclusive, not runtime fallback alternatives.

| Scenario | Recommended Approach |
|---|---|
| Small compacted topics (< 10K keys) | `TopicSnapshotConsumer` (v1) — passes are sub-millisecond, many passes are negligible; no memory overhead |
| Medium topics (10K–100K keys), subscription bursts | Snapshot Cache — eliminates the problematic regime where pass duration ≈ inter-arrival time; also eliminates the drain-exit race |
| Large topics (100K–1M keys), frequent subscriptions | Snapshot Cache — always current, instant lookups |
| Very large topics (> 1M keys) with limited memory | `TopicSnapshotConsumer` (v1) with timeout — cache would consume too much memory |

---

## 12. Affected Files Summary

| File | Change Type | Status | Description |
|---|---|---|---|
| `TopicSnapshotConsumer.java` | **New** | **Done** | Shared per-topic consumer with timeout, multi-pass coordination, `DeserializerPair`, structured `doPoll()`, wakeup-based `shutdown()`, `ItemFinalizationCallback` for caller-coordinated finalization, tombstone skipping, `AUTO` mode event decoration |
| `CommandMode.java` | **New** | **Done** | Extracted from `RecordConsumerSupport` to `adapters.consumers` package; provides `decorate()`, `deleteEvent()`, `Command` and `Key` enums shared by both `processor` and `snapshot` sub-packages |
| `RecordMapper.java` | Modified | **Done** | Added `RecordMapper.from(ItemTemplates, FieldsExtractor)` convenience factory method |
| `RecordConsumerSupport.java` | Modified | **Done** | Removed nested `CommandMode` interface; imports extracted `CommandMode` from `adapters.consumers` package |
| `SubscriptionsHandler.java` | Modified | **Done** | Refactored from abstract class hierarchy to flat `DefaultSubscriptionsHandler implements SubscriptionsHandler`; `consumerLock` guards `itemsCount` + consumer lifecycle atomically via `incrementAndMaybeStartConsuming()` / `decrementAndMaybeStopConsuming()`; manages per-topic `TopicSnapshotConsumer` map; per-item topic countdown via `pendingSnapshotTopics` map; `onItemSnapshotComplete()` callback; submit-after-enqueue pattern; `RecordMapper` and `DeserializerPair` centralized in constructor; regex topic guard |
| `Items.java` | Modified | **Done** | Added `topicsFor(SubscribedItem)` and `isRegexEnabled()` to `ItemTemplates` interface and `DefaultItemTemplates`; used by `enqueueForSnapshot()` to resolve matching topics and guard against regex subscriptions |
| `TopicConfigurations.java` | Modified | **Done** | Added `isRegexEnabled()` method to expose whether topic mappings use regex patterns |
| `StreamingDataAdapter.java` | Modified | **Done** | Refactored to use `RecordMapper.from(ItemTemplates, FieldsExtractor)` convenience factory |
| `KafkaConsumerWrapper.java` | Modified | **Done** | Accepts `RecordMapper` as constructor parameter (was built internally) |
| `KafkaConnectorDataAdapter.java` | Modified | **Done** | `isSnapshotAvailable()` returns `true` (temporary — TODO: gate on `snapshot.enable`) |
| `ConnectorConfig.java` | Modified | **Pending** | Add `snapshot.enable` and `snapshot.timeout.ms` parameters |
| `ConnectorConfigurator.java` | Modified | **Pending** | Pass snapshot config to consumer config; validate KEY-only templates and non-regex topics |
| `KafkaConsumerWrapperConfig.java` | Modified | **Pending** | Expose snapshot flag and timeout in `Config` record |
