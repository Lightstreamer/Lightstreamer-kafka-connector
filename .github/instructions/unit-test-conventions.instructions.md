---
applyTo: "**/src/test/**/*.java"
---

# Unit Test Conventions

This document defines the unit test conventions adopted across the Kafka Connector codebase.

## Class Visibility

All test classes must be declared `public`:

```java
public class MyConnectorTest { ... }
```

## Test Method Visibility

All test methods must be declared `public`:

```java
@Test
public void shouldCommitOffsetsOnShutdown() { ... }
```

## Test Method Naming

Test methods use **camelCase** with the `should` prefix, describing the expected behavior:

```java
@Test
public void shouldReturnTrueWhenThresholdExceeded() { ... }

@Test
public void shouldThrowOnInvalidConfiguration() { ... }

@ParameterizedTest
public void shouldHandleVariousInputFormats() { ... }
```

Where applicable, prefer `@ParameterizedTest` over separate test methods when verifying the same
behavior with different inputs or edge values.

## Assertion Framework

Use [Google Truth](https://truth.dev/) for all assertions:

```java
import static com.google.common.truth.Truth.assertThat;

assertThat(result).isEqualTo(expected);
assertThat(list).containsExactly("a", "b");
assertThat(optional).isPresent();
```

Do not use JUnit's `assertEquals`/`assertTrue` or Hamcrest matchers.

## Mocking

Do not use external mock frameworks (Mockito, EasyMock, etc.). Use hand-written test doubles
(stubs, fakes, spies) defined in the test class or in shared test utilities.

## Inline Comments

Inline comments within test methods are complete sentences and must end with a period:

```java
// The item name does not match any configured template, triggering a SubscriptionException
// that causes an immediate failure without creating the internal consumer, so the future
// is never completed and remains null.
assertThat(subscriptionHandler.getFutureStatus()).isNull();
```
