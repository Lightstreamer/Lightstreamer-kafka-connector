# Item Matching Mechanism

## 1. Overview

The item matching mechanism bridges Kafka's record-based model with Lightstreamer's
item-based pub/sub model. There are two distinct phases:

1. **Subscription-time matching** — Does this client subscription correspond to any
   configured item template? (structural/schema check)
2. **Record-time routing** — Given a Kafka record, which subscribed items should receive
   it? (value-based lookup)

```
  Configuration                      Runtime
  ─────────────                      ───────
  item-template.T = prefix-#{p=EXPR} ──► ItemTemplate (Schema + CanonicalItemExtractor)
        ↓
  map.topic.to = item-template.T     ──► ItemTemplates  (topic → extractors)

  Client subscribes "prefix-[p=val]" ──► SubscribedItem (Schema + canonical name)
        ↓
  Schema("prefix",{p}) == Schema("prefix",{p})  →  MATCH ✓
        ↓
  SubscribedItems stores "prefix-[p=val]" → SubscribedItem

  Kafka record arrives (topic, key, value)
        ↓
  CanonicalItemExtractor evaluates EXPR on record → "prefix-[p=extracted]"
        ↓
  MappedRecord.route() looks up "prefix-[p=extracted]" in SubscribedItems
        ↓
  If "prefix-[p=extracted]" == "prefix-[p=val]"  →  DELIVER ✓
```

---

## 2. Configuration

### 2.1 Item Template Definition

```xml
<param name="item-template.stock">stock-#{index=KEY.name}</param>
<param name="item-template.user">user-#{userId=VALUE.id,accountId=VALUE.accountId}</param>
```

**Syntax**: `PREFIX-#{param1=EXPR1, param2=EXPR2, ...}`

- **PREFIX** — A name identifier (alphanumeric, underscore, hyphen).
  Becomes the schema name.
- **Parameters** — Key-value pairs inside `#{...}`.
  - **Key** — Parameter name (e.g., `index`). Becomes a schema key.
  - **Value** — Extraction expression rooted at one of:
    `KEY`, `VALUE`, `HEADERS`, `TIMESTAMP`, `PARTITION`, `OFFSET`, `TOPIC`.
    Supports dot-notation (`VALUE.symbol`) and indexed access (`KEY[0]`).
- **Zero parameters** is valid: `simple-item` (schema with empty key set).

Parsed by `Expressions.Template(String)` → `TemplateExpression(prefix, SortedMap<String, ExtractionExpression>)`.

### 2.2 Topic-to-Template Mapping

```xml
<param name="map.stocks.to">item-template.stock</param>
<param name="map.users.to">item-template.by-name,item-template.by-age</param>
<param name="map.events.to">item-template.event,simple-item-1</param>
```

- One topic can map to **multiple** templates.
- Templates and plain item names can be mixed.
- Multiple topics can share the same template.
- Topic names can be regex patterns (when `isRegexEnabled()` is true).

Processed by `TopicConfigurations` → list of `TopicConfiguration(topic, List<TemplateExpression>)`.

---

## 3. Key Data Structures

### 3.1 Schema

```java
// Schema.java
interface Schema {
    String name();       // e.g., "stock"
    Set<String> keys();  // e.g., {"index"}
}
```

Equality is based on **name + keys** (not values).
Two schemas are equal if they have the same name and the same set of parameter keys.

### 3.2 ItemTemplate

```java
// Items.java (inner class)
class ItemTemplate<K, V> {
    Schema schema;                         // from the CanonicalItemExtractor
    String topic;                          // literal or regex topic
    CanonicalItemExtractor<K, V> extractor; // extracts canonical item names from records

    boolean matches(SubscribedItem item) {
        return schema.equals(item.schema());
    }
}
```

Created at startup by `Items.templatesFrom(TopicConfigurations, KeyValueSelectorSuppliers)`.

### 3.3 ItemTemplates

```java
// Items.java (inner class DefaultItemTemplates)
class DefaultItemTemplates<K, V> implements ItemTemplates<K, V> {
    List<ItemTemplate<K, V>> templates;
    boolean regexEnabled;

    boolean matches(SubscribedItem item) {
        return templates.stream().anyMatch(t -> t.matches(item));
    }

    Set<String> topics() { ... }            // all topic names/patterns
    Map<String, Set<CanonicalItemExtractor<K, V>>> groupExtractors() { ... }
}
```

### 3.4 SubscribedItem

```java
// Items.java (inner class DefaultSubscribedItem)
class DefaultSubscribedItem implements SubscribedItem {
    String canonicalItemName;    // e.g., "stock-[index=AAPL]"
    Schema schema;               // e.g., Schema("stock", {"index"})
    Object itemHandle;           // Lightstreamer handle
    Queue<...> pendingRealtimeEvents;
    BiConsumer<...> realtimeEventConsumer;
}
```

Created by `Items.subscribedFrom(String input, Object itemHandle)`:
1. Parses input via `Expressions.Subscription(input)` → `SubscriptionExpression`.
2. Extracts `prefix` and `SortedSet<Data>` (key-value pairs).
3. Derives `Schema` from prefix + data key names.
4. Builds canonical item name: `prefix-[key1=val1,key2=val2]` (alphabetically sorted).

### 3.5 SubscribedItems

```java
// Items.java (inner class DefaultSubscribedItems)
class DefaultSubscribedItems implements SubscribedItems {
    Map<String, SubscribedItem> sourceItems;  // ConcurrentHashMap

    void addItem(SubscribedItem item) {
        sourceItems.put(item.asCanonicalItemName(), item);
    }
    SubscribedItem getItem(String itemName) {
        return sourceItems.get(itemName);      // O(1) lookup by canonical name
    }
}
```

Items are stored and looked up by their **canonical item name**.

---

## 4. Subscription Expression Syntax

When a client subscribes, Lightstreamer passes the item name string:

```
stock-[index=AAPL]
user-[accountId=456,userId=123]
simple-item
```

**Format**: `PREFIX-[param1=value1,param2=value2]`

- Same PREFIX as the template.
- Parameter **keys** must match the template's parameter keys (set equality).
- Parameter **values** carry the client's filter criteria.
- Parameters are sorted **alphabetically by key** in the canonical form.
- No parameters is valid: `simple-item` → Schema("simple-item", {}).

Parsed by `Expressions.Subscription(String)` → `SubscriptionExpression(prefix, SortedSet<Data>)`.

---

## 5. Phase 1: Subscription-Time Matching

Triggered from `SubscriptionsHandler.subscribe(item, itemHandle)`:

```java
SubscribedItem newItem = Items.subscribedFrom(item, itemHandle);
if (!config.itemTemplates().matches(newItem)) {
    throw new SubscriptionException("Item does not match any defined item templates");
}
subscribedItems.addItem(newItem);
```

### Matching Logic

```
ItemTemplates.matches(subscribedItem)
  └─► for each ItemTemplate:
        template.schema.equals(subscribedItem.schema())
        i.e., template.name == item.prefix
              AND template.keys == item.keys (set equality)
```

**Important**: Matching is purely **structural** — it compares the schema (name + parameter key
names), **not** the parameter values. Any value for a matching key set is accepted.

### Examples

| Template                          | Subscription             | Schema Comparison                                        | Result |
|-----------------------------------|--------------------------|----------------------------------------------------------|--------|
| `stock-#{index=KEY.name}`         | `stock-[index=AAPL]`    | `("stock",{index})` vs `("stock",{index})`               | ✓      |
| `stock-#{index=KEY.name}`         | `stock-[index=MSFT]`    | `("stock",{index})` vs `("stock",{index})`               | ✓      |
| `stock-#{index=KEY.name}`         | `stock-[symbol=AAPL]`   | `("stock",{index})` vs `("stock",{symbol})`              | ✗      |
| `stock-#{index=KEY.name}`         | `bond-[index=1]`        | `("stock",{index})` vs `("bond",{index})`                | ✗      |
| `user-#{userId=V.id,acct=V.acct}` | `user-[acct=A,userId=U]`| `("user",{userId,acct})` vs `("user",{acct,userId})`    | ✓      |

---

## 6. Phase 2: Record-Time Routing

When a Kafka record arrives, `RecordMapper.map(record)` produces a `MappedRecord`:

```java
// RecordMapper (DefaultRecordMapper)
MappedRecord map(KafkaRecord<K, V> record) {
    Set<CanonicalItemExtractor<K, V>> extractors = getExtractors(record.topic());
    String[] canonicalItems = new String[extractors.size()];
    for (CanonicalItemExtractor extractor : extractors) {
        canonicalItems[i] = extractor.extractCanonicalItem(record);
    }
    return new MappedRecord(canonicalItems, () -> fieldExtractor.extractMap(record), ...);
}
```

### CanonicalItemExtractor

For a template `stock-#{index=KEY.name}` and a record with `KEY.name = "AAPL"`:
1. Evaluates `KEY.name` on the record → `"AAPL"`
2. Creates `Data("index", "AAPL")`
3. Builds canonical name: `Data.buildItemName(...)` → `"stock-[index=AAPL]"`

### MappedRecord.route()

```java
Set<SubscribedItem> route(SubscribedItems items) {
    Set<SubscribedItem> result = new HashSet<>();
    for (String itemName : itemNames) {
        SubscribedItem item = items.getItem(itemName);  // O(1) HashMap lookup
        if (item != null) {
            result.add(item);
        }
    }
    return result;
}
```

The routing is an **exact string match** of the canonical item name produced by the extractor
against the canonical item names stored in `SubscribedItems`.

---

## 7. Canonical Item Name as the Join Key

The entire mechanism hinges on canonical item name equality:

- **Template side** (record → canonical name): `CanonicalItemExtractor` evaluates expressions
  on the record and builds `prefix-[key1=extractedVal1,key2=extractedVal2]`.
- **Subscription side** (client → canonical name): `SubscriptionExpression` parses the client
  input and builds `prefix-[key1=clientVal1,key2=clientVal2]`.

Both sides sort parameters **alphabetically by key name**, so order in the original input
doesn't matter.

A record reaches a subscriber if the extractor produces a canonical name that exactly equals
the subscriber's canonical name. This means the extracted value from the record must equal the
value the client specified in its subscription.

---

## 8. Topic Relationships

### 8.1 One Topic → Multiple Templates

```xml
<param name="item-template.by-name">user-#{name=VALUE.name}</param>
<param name="item-template.by-age">user-#{age=VALUE.age}</param>
<param name="map.users.to">item-template.by-name,item-template.by-age</param>
```

A single record from topic `users` produces **two** canonical item names (one per extractor).

### 8.2 Multiple Topics → Same Template

```xml
<param name="item-template.order">order-#{id=KEY.id}</param>
<param name="map.new_orders.to">item-template.order</param>
<param name="map.past_orders.to">item-template.order</param>
```

Records from different topics can produce the same canonical item name and route to the same
subscriber.

### 8.3 Regex Topics

When enabled, topic names in the mapping are compiled as `Pattern` and matched against the
incoming record's topic. `ItemTemplates.isRegexEnabled()` returns `true` and
`subscriptionPattern()` returns a combined pattern for all topics.

---

## 9. Class Summary

| Class                      | File             | Role                                              |
|----------------------------|------------------|----------------------------------------------------|
| `Schema`                   | Schema.java      | Structural identity: name + parameter key names    |
| `TemplateExpression`       | Expressions.java | Parsed template: prefix + extraction expressions   |
| `SubscriptionExpression`   | Expressions.java | Parsed subscription: prefix + key-value pairs      |
| `ItemTemplate`             | Items.java       | Binds a topic to a schema + extractor              |
| `ItemTemplates`            | Items.java       | Collection of templates; subscription-time matching |
| `SubscribedItem`           | Items.java       | A live subscription with schema + canonical name   |
| `SubscribedItems`          | Items.java       | Concurrent map: canonical name → SubscribedItem    |
| `CanonicalItemExtractor`   | DataExtractors.java | Evaluates expressions on records → canonical name |
| `RecordMapper`             | RecordMapper.java | Maps records to MappedRecord (canonical names + fields) |
| `MappedRecord.route()`     | RecordMapper.java | Looks up canonical names in SubscribedItems        |
| `TopicConfigurations`      | TopicConfigurations.java | Parsed config: topic → template expressions  |
