---
applyTo: "**/*.java"
---

# Builder Conventions

This document defines the conventions adopted across the Kafka Connector codebase for **builder
classes** and **step builders** (typesafe staged builders). The goal is to keep call sites
self-documenting, eliminate boolean grammar traps, and ensure that field names, method names,
parameter names, and `@param` Javadoc tags agree end-to-end.

## Classic Builder

A classic builder is a single `Builder` class with chainable setters and a terminal `build()`.

### Method naming

- **Do not use the `with*` prefix.** Setters are named for the **property** they set, not for an
  imperative verb. The fluent API reads as a sequence of properties, not as a sequence of commands.

  ```java
  // Correct
  builder.consumerFactory(factory)
         .connectionSpec(spec)
         .snapshotEnabled(true)
         .itemSnapshotMaxIdleSeconds(30)
         .build();

  // Wrong
  builder.withConsumerFactory(factory)
         .withConnectionSpec(spec)
         .withEnabledSnapshot(true)              // verb + noun, and "with"
         .withItemSnapshotMaxIdle(30)
         .build();
  ```

- **Use the `add*` prefix for accumulator methods** — methods that contribute to a collection
  rather than overwrite a property. Pair singular and plural forms when both exist:

  ```java
  .addCanonicalItemExtractor(subscription, extractor)   // single
  .addCanonicalItemExtractors(map)                      // bulk
  ```

  Do not use `add*` for property setters; do not use property-style names for accumulators.

### Booleans must be adjective-form, not verb-form

A boolean setter named with an imperative verb (`enable*`, `prefer*`, `use*`) becomes ungrammatical
when called with `false`:

```java
.enableSnapshot(false)        // reads as "enable snapshot... false"
.preferSingleThread(false)    // reads as "prefer single thread... false"
```

Use the **adjective form** instead. The property name reads cleanly in both directions:

```java
.snapshotEnabled(true)
.snapshotEnabled(false)
.singleThreadPreferred(true)
.singleThreadPreferred(false)
```

Common adjective suffixes: `*Enabled`, `*Required`, `*Preferred`, `*Allowed`.

### Method, parameter, field, and Javadoc must agree

For every setter the **field name, method name, parameter name, and `@param` tag** must all use
the same identifier:

```java
// Correct — four-way agreement
private boolean snapshotEnabled;

/**
 * @param snapshotEnabled {@code true} to enable snapshot support, {@code false} otherwise
 */
public Builder<K, V> snapshotEnabled(boolean snapshotEnabled) {
    this.snapshotEnabled = snapshotEnabled;
    return this;
}
```

```java
// Wrong — three different names for the same value
private boolean snapshot;

/**
 * @param enabled {@code true} to enable snapshot support
 */
public Builder<K, V> enableSnapshot(boolean enabled) {
    this.snapshot = enabled;
    return this;
}
```

### Encode units in the identifier

When a numeric property carries a unit (seconds, millis, bytes, etc.), put the unit in the
identifier so the IDE parameter hint surfaces it at every call site:

```java
// Correct — unit visible everywhere
private long itemSnapshotMaxIdleSeconds;
public Builder<K, V> itemSnapshotMaxIdleSeconds(long itemSnapshotMaxIdleSeconds) { ... }

// Wrong — unit lost; "30 what?"
private long itemSnapshotMaxIdle;
public Builder<K, V> itemSnapshotMaxIdle(long maxIdle) { ... }
```

### Internal field names follow the public API

The fields of the impl class that backs a builder must use the same vocabulary as the public
setters that write them. Do not introduce alternate internal names (e.g. `mapper` internally for
a public `recordMapper` setter, or `subscribed` for `subscribedItems`).

## Step Builder

A step builder enforces ordering of required setters at compile time by returning a different
interface from each step.

### Step interface naming

Use the **`With*` convention** — each step interface is named after the property that has just
been provided. The interface's single (or first) method names the property to set next:

```java
RecordMapperStep<K,V>       // initial step, after RecordMapper is provided
  → WithSubscribedItems<K,V>  // SubscribedItems just set
  → WithEventListener<K,V>    // ItemEventListener just set
  → WithOffsetService<K,V>    // OffsetService just set
  → WithOptionals<K,V>        // Logger just set; only optionals + build() remain
```

The initial step is exempt from the `With*` form and is named after the property that bootstraps
the chain (e.g. `RecordMapperStep`), not after a verb phrase like `StartBuildingProcessor`.

### Step interface Javadoc

The class-level Javadoc summary must follow the pattern **"Builder step reached after setting X;
sets Y next."** This keeps the name (what was just set) and the summary (what is set next)
mutually consistent and tells the reader the whole story at a glance:

```java
/**
 * Builder step reached after setting the {@link SubscribedItems}; sets the
 * {@link ItemEventListener}.
 */
interface WithSubscribedItems<K, V> {
    WithEventListener<K, V> eventListener(ItemEventListener eventListener);
}
```

### Step method conventions

Within each step, the method-naming, parameter-naming, boolean-adjective, and unit-encoding rules
of the classic builder apply unchanged. The terminal optionals step (`WithOptionals`) exposes the
`build()` method.

### Entry point

Expose a single static factory method on the produced type, named after the first property:

```java
public interface RecordConsumer<K, V> {
    public static <K, V> RecordMapperStep<K, V> recordMapper(RecordMapper<K, V> recordMapper) {
        return RecordConsumerSupport.recordMapper(recordMapper);
    }
}
```

The public factory, the internal delegate, and the impl constructor should share the same name and
parameter identifier; do not introduce indirection vocabulary like `startBuildingProcessor` for the
internal layer.

## Summary Rules

1. **No `with*` prefix** on setters; setters are named for the property being set.
2. **`add*` prefix** for accumulator methods (single and plural forms).
3. **Adjective form for booleans** — never `enable*`/`prefer*`/`use*` for a setter that accepts
   `false`.
4. **Four-way agreement**: field == method == parameter == `@param` identifier.
5. **Units in identifiers** for numeric properties whose unit isn't self-evident from the type.
6. **Internal fields mirror the public API**; no vocabulary drift between layers.
7. **Step builders use `With*`** for non-initial step interfaces, with Javadoc summaries of the
   form *"Builder step reached after setting X; sets Y next."*
