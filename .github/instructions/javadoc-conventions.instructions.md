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

- Use the **fully qualified class name** for external exceptions, **no** `{@link}`:
  ```java
  /**
   * @throws org.apache.kafka.common.errors.SerializationException if the key or value
   *     cannot be deserialized
   */
  ```
- Use the **simple name** for standard JDK exceptions:
  ```java
  /**
   * @throws IllegalStateException if the batch is in an invalid state
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
