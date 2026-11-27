---
applyTo: "**/*.java"
---

# Logging Conventions

## Fluent API (LoggingEventBuilder)

Always use the SLF4J fluent logging API via `LoggingEventBuilder` (the object returned by
`logger.atInfo()`, `logger.atDebug()`, etc.) rather than the traditional `logger.info(...)` methods.

```java
// Correct
logger.atInfo().log("Starting connection to broker at {}", address);
logger.atError().setCause(e).log("Unrecoverable exception during poll");

// Wrong — do not use the traditional API
logger.info("Starting connection to broker at {}", address);
logger.error("Unrecoverable exception during poll", e);
```

### Exception: code run by the Kafka Connect sink

The Kafka Connect plugin packages every class outside the `com.lightstreamer.kafka.adapters`
package: `connect`, `common` and any other package. In the classes that the Connect sink runs, use
the **traditional API** (`logger.info(...)`, `logger.error("message", e)`), not the fluent one.
This means all of `connect/` and the shared `common/` code that the sink calls. The `adapters`
package is not affected.

The fluent API exists only in SLF4J 2.x, while Kafka Connect always loads `org.slf4j` from the
worker (which still provides SLF4J 1.7) and does not let a plugin override it. A fluent call
fails at runtime on the worker with `NoSuchMethodError: org.slf4j.Logger.atDebug()`, although
it compiles and passes the unit tests, which run with SLF4J 2.x.

```java
// Correct in connect/ and in the common/ code used by the sink
logger.info("Subscribed to item [{}]", item);
logger.error("Handling RemotingException", exception);

// Wrong in connect/ and in the common/ code used by the sink
logger.atInfo().log("Subscribed to item [{}]", item);
logger.atError().setCause(exception).log("Handling RemotingException");
```

Some `common/` classes are used only by the adapter and keep the fluent API: currently
`common.mapping.Items` and `common.monitors.KafkaConnectorMonitor`. The unit test
`ConnectPluginLoggingTest` fails if any other class in `connect/` or `common/` uses the fluent API.
If the Connect sink starts using one of the exempt classes, convert its logging calls and remove it
from the exemption list in that test.
