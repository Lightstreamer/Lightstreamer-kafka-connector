---
applyTo: "**/*.java"
---

# Javadoc Conventions

This document defines the Javadoc conventions adopted across the Kafka Connector codebase.

## `{@link}` vs `{@code}` for Type References

### Use `{@link ClassName}` for:

- **Other types** — any class, interface, or enum that is *not* the containing type:
  ```java
  // In NotifyingRecordBatch.java
  /**
   * Base implementation of {@link RecordBatch} that notifies a listener ...
   */
  ```
- **External library types** (Kafka, JDK, etc.):
  ```java
  /**
   * ... adds a {@link CountDownLatch} to enable synchronous waiting ...
   */
  ```
- **Method references**, including same-class methods:
  ```java
  /**
   * ... via the {@link #join()} method.
   */
  ```
- **First mention of a type** in `@param` and `@return` tags when the type is navigable
  and not the containing class:
  ```java
  /**
   * @param record the Kafka Connect {@link SinkRecord} to wrap
   * @return a new {@link KafkaRecord} wrapping the given {@code SinkRecord}
   */
  ```

### Use `{@code ClassName}` for:

- **Self-references** — referring to the containing class or interface:
  ```java
  // In JoinableRecordBatch.java
  /**
   * Constructs a new {@code JoinableRecordBatch} with the specified capacity.
   */
  ```
- **Subsequent mentions** of a type already linked earlier in the same Javadoc block:
  ```java
  /**
   * Creates a {@link KafkaRecord} from a Kafka Connect {@link SinkRecord}.
   *
   * <p>The created record wraps the {@code SinkRecord} and provides access ...
   *
   * @return a new {@link KafkaRecord} wrapping the given {@code SinkRecord}
   */
  ```
- **Literals and keywords**: `{@code true}`, `{@code false}`, `{@code null}`,
  `{@code volatile}`.
- **Code expressions and identifiers** used in narrative:
  ```java
  /**
   * ... via factory method ({@code batchFromEager} or {@code batchFromDeferred})
   */
  ```

### Summary Rule

> **First mention** of an _external_ type → `{@link}`.
> **Self-reference** or **repeated mention** → `{@code}`.
> **Literals and code tokens** → always `{@code}`.

## `@param` and `@return` Tags

- **Generic type parameters** use plain text (no tag):
  ```java
  /**
   * @param <K> the type of the key in the Kafka record
   * @param <V> the type of the value in the Kafka record
   */
  ```
- **Boolean returns** spell out both states:
  ```java
  /**
   * @return {@code true} if the value is null, {@code false} otherwise
   */
  ```
- **Object returns** reference the return type with `{@link}` (first mention) or
  `{@code}` (self-reference/repeat):
  ```java
  /**
   * @return a new {@link KafkaRecord} with deferred deserialization
   */
  ```
- **Nullable returns** append `, or {@code null} if ...`:
  ```java
  /**
   * @return the parent {@link RecordBatch}, or {@code null} if not associated with a batch
   */
  ```

## `@throws` Clauses

- Use the **simple name** when the exception type is **imported** or is a standard JDK exception:
  ```java
  /**
   * @throws KafkaException if a Kafka operation fails during initialization
   * @throws IllegalStateException if the batch is in an invalid state
   */
  ```
- Use the **fully qualified class name** when the exception type is **not imported**
  (e.g., a transitive exception thrown by a callee). Do **not** use `{@link}`:
  ```java
  /**
   * @throws org.apache.kafka.common.errors.SerializationException if the key or value
   *     cannot be deserialized
   */
  ```
- Start the description with a conditional **"if"** clause.

## Paragraph and List Structure

- Separate major sections with `<p>`:
  ```java
  /**
   * First paragraph describing the class.
   *
   * <p>Second paragraph with additional context.
   */
  ```
- Use `<strong>Label:</strong>` for section headers within a
  Javadoc block:
  ```java
  /**
   * <p><strong>Thread Safety:</strong> Safe for concurrent calls from multiple worker threads.
   */
  ```
- Use `<ul>`/`<li>` for unordered lists and `<ol>`/`<li>` for numbered sequences:
  ```java
  /**
   * <ul>
   *   <li><strong>Eager deserialization:</strong> Key/value decoding is performed immediately ...
   *   <li><strong>Deferred deserialization:</strong> Key/value decoding is delayed ...
   * </ul>
   */
  ```

## Class and Interface Javadoc

- **Opening sentence**: concise summary of the type's purpose.
- **`@param <K>`, `@param <V>`**: always present for generic types, using the form
  `the type of the key/value in the Kafka record`.
- **`@see`**: reference closely related types (e.g., sibling implementations).

## Javadoc Coverage Requirements

### Required

- All `public` and `protected` types (classes, interfaces, enums, records)
- All `public` and `protected` methods and constructors
- All `public` and `protected` fields and constants
- Package-private classes that serve as primary implementations of public interfaces

### Recommended

- Package-private methods with non-obvious behavior or contracts
- **Sibling consistency**: when an interface or its first implementation has Javadoc, give all
  sibling implementations a brief Javadoc too — even when private. This avoids visually-asymmetric
  groups where one nested type is documented and adjacent ones are bare.

### Exempt

- `@Override` methods where the parent Javadoc is sufficient (unless adding
  implementation-specific detail)
- Trivial getters/setters with self-evident semantics
- Test classes and test methods (the `should...` name is the documentation)
- Private members (inline comments suffice)

### Class-level Javadoc Structure

1. **Summary sentence** — one sentence describing the type's responsibility
2. **Extended description** — behavioral contracts, threading guarantees, lifecycle notes
   (only when non-trivial)
3. **`@see`** — related types for navigation
4. **`@param <T>`** — for each generic type parameter

### Method-level Javadoc Structure

1. **Summary sentence** — verb phrase: "Creates...", "Returns...", "Throws..."
2. **`@param`** — one per parameter
3. **`@return`** — unless `void`
4. **`@throws`** — one per declared or significant unchecked exception

### Guiding Principle

> Javadoc describes *what* and *why* (the contract), not *how* (implementation details).
> If the name and signature already communicate the full contract, Javadoc adds no value and
> can be omitted for non-public members.

## Field Comments: `/** */` vs `//`

- Use `/** Javadoc */` only on `public` and `protected` fields — these appear in generated
  API documentation and are part of the visible contract.
- Use `//` inline comments on package-private and `private` fields — they are implementation
  details, not API surface. Javadoc on private fields is misleading (signals API documentation
  where none exists).

```java
// Correct: public field with Javadoc
public final Duration timeout;

// Correct: private field with inline comment
// The lock used to synchronize updater replacement.
private final ReentrantLock updaterLock = new ReentrantLock();

// Incorrect: Javadoc on a private field
/** The lock used to synchronize updater replacement. */
private final ReentrantLock updaterLock = new ReentrantLock();
```
