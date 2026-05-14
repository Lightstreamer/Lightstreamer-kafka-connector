---
applyTo: "**/*.java"
---

# Class Organization Conventions

This document defines the class member ordering conventions adopted across the Kafka Connector
codebase, based on the
[Oracle Code Conventions for the Java Programming Language, Section 3.1.3](https://www.oracle.com/java/technologies/javase/codeconventions-fileorganization.html).

## Member Ordering

Class and interface declarations follow this order:

1. **Class (`static`) variables** — `public`, then `protected`, then package-private, then `private`
2. **Instance variables** — `public`, then `protected`, then package-private, then `private`
3. **Constructors**
4. **Methods** — grouped by functionality, not by access level

## Field Ordering Within Each Access Level

Within each access level (`protected`, `private`, etc.), fields are ordered:

1. `final` fields first (immutable after construction)
2. `volatile` fields next (mutable, thread-shared)
3. Non-`final`, non-`volatile` fields last

Within each modifier group, fields are **logically grouped**: related fields are kept together, and
dependencies are declared before their dependents.

## Example

```java
class MyClass {

    // 1. Static variables (public → private)
    public static final String PUBLIC_CONSTANT = "value";
    private static final Duration TIMEOUT = Duration.ofMillis(5000);

    // 2. Instance variables (public → protected → package → private)
    //    Within each level: final → volatile → plain
    protected final Logger logger;
    protected final RecordMapper<K, V> recordMapper;
    protected volatile Consumer<K, V> consumer;

    private final Config config;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile FutureStatus status;

    // 3. Constructors
    MyClass(Config config) { ... }

    // 4. Methods (grouped by functionality)
    ...
}
```

## Methods

Methods are grouped by **functionality**, not by scope or accessibility. A `private` helper method
should be placed near the `public` or `protected` method that calls it, not separated into a
distant `private` section.

> *"These methods should be grouped by functionality rather than by scope or accessibility. For
> example, a private class method can be in between two public instance methods. The goal is to
> make reading and understanding the code easier."*
> — Oracle Code Conventions, Section 3.1.3

## Interface Method Ordering

Within an interface, order methods by category:

1. **Static factory methods** first (e.g., `static Foo of(...)`)
2. **Queries / accessors** (read-only methods that return state)
3. **Mutators** (methods that change state)

Within each category, group by functionality as usual.

## Constructor Initialization Order

Constructor assignments should follow field declaration order. When a data dependency requires a
field to be initialized before fields declared above it (e.g., a `private` config field used to
derive `protected` fields), initialize the dependency first without reordering the field
declarations — the field declaration order reflects the public API surface and takes precedence.

## Nested/Inner Class Ordering

When a file contains multiple nested classes implementing the same interface or extending the same
base, group them together. Order nested types by their relationship:

1. **Interface/abstract type** first
2. **Implementations** of that interface/abstract type immediately after, grouped together

Do not interleave unrelated types between implementations of the same contract. For example, if a
file contains both `SubscribedItem` implementations and `ItemTemplates` implementations, all
`SubscribedItem` implementations should be contiguous — not separated by the `ItemTemplates`
interface.
