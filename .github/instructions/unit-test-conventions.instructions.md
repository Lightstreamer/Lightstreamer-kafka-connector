---
applyTo: "**/src/test/**/*.java"
---

# Unit Test Conventions

This document defines the unit test conventions adopted across the Kafka Connector codebase.

## Class Visibility

Test classes are declared **package-private** (no modifier) — the JUnit Jupiter idiom. Never
`private`; `public` is only justified when the class must be subclassed from another package.

```java
// Correct
class MyConnectorTest { ... }

// Wrong
public class MyConnectorTest { ... }
```

Test support classes under `test_utils/` (e.g. `Mocks`, `Records`, `ConnectorConfigProvider`)
are `public` because they are consumed across test packages.

## Test Method Visibility

Test methods are declared **package-private** (no modifier) — the JUnit Jupiter idiom. Never
`private`.

```java
// Correct
@Test
void shouldCommitOffsetsOnShutdown() { ... }

// Wrong
@Test
public void shouldCommitOffsetsOnShutdown() { ... }
```

The JUnit Jupiter engine invokes tests reflectively, so `public` conveys nothing. The `public`
requirement was a JUnit 4 constraint and is no longer needed.

## Test Method Naming

Test methods use **camelCase** with the `should` prefix, describing the expected behavior:

```java
@Test
void shouldReturnTrueWhenThresholdExceeded() { ... }

@Test
void shouldThrowOnInvalidConfiguration() { ... }

@ParameterizedTest
void shouldHandleVariousInputFormats() { ... }
```

Where applicable, prefer `@ParameterizedTest` over separate test methods when verifying the same
behavior with different inputs or edge values.

## Lifecycle Method Naming

Methods annotated with `@BeforeEach`, `@AfterEach`, `@BeforeAll`, or `@AfterAll` are named after
the *lifecycle phase*, not after imperative verbs like `setUp`, `tearDown`, `setup`, or `cleanup`.
Like test methods, they are declared **package-private**:

```java
// Correct
@BeforeEach
void before() { ... }

@AfterEach
void after() { ... }

@BeforeAll
static void beforeAll() { ... }

@AfterAll
static void afterAll() { ... }
```

```java
// Wrong
@BeforeEach
public void setUp() { ... }

@AfterEach
public void tearDown() { ... }
```

The annotation already describes the intent; the method name should stay short and mirror the
phase. Only one `@BeforeEach` and one `@AfterEach` method per class.

## Instance-Field Initialization

Assign instance fields inside lifecycle methods without the `this.` qualifier. It is redundant
when no local variable or parameter shadows the field, which is the normal case for
`@BeforeEach` and `@AfterEach`. Use `this.` **only** to disambiguate a shadowed name (e.g. in
a constructor or setter whose parameter matches the field).

```java
// Correct
private ForceableSubscribedItems items;
private MockItemEventListener listener;

@BeforeEach
void before() {
    listener = new MockItemEventListener();
    items = SubscribedItems.forceable(listener, logger);
}
```

```java
// Wrong
@BeforeEach
void before() {
    this.listener = new MockItemEventListener();
    this.items = SubscribedItems.forceable(this.listener, logger);
}
```

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
