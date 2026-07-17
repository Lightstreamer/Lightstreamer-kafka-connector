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
