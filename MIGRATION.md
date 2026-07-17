# Migration guide

This document collects upgrade instructions for users moving between major versions of the Lightstreamer Kafka Connector.

For the full per-release change list, see [`CHANGELOG.md`](CHANGELOG.md).

## Table of contents

- [Migrating from 1.x to 2.0](#migrating-from-1x-to-20)
  - [Prerequisites](#prerequisites)
  - [Configuration changes](#configuration-changes)
  - [Producer-side changes](#producer-side-changes)
  - [Behavioral changes under snapshot](#behavioral-changes-under-snapshot)

## Migrating from 1.x to 2.0

Release [2.0.0] introduces first-class **connector-managed snapshot** support and reorganizes a number of pre-existing COMMAND-mode parameters around the new unified [`item.snapshot.enabled.mode`](README.md#itemsnapshotenabledmode) parameter. This section walks through the steps required to upgrade an existing 1.x deployment.

### Prerequisites

- **Lightstreamer Broker version**. The Broker (also referred to as _Lightstreamer Server_) must be on version **7.4.8 or newer**. The connector-managed snapshot feature relies on SDK APIs introduced in that release. Running 2.0 against an older Broker will fail at adapter initialization. See the [Lightstreamer Download page](https://lightstreamer.com/download/).
- **Java**. No JDK change relative to 1.x.

### Configuration changes

1. **If you used `fields.evaluate.as.command.enable = true`** (parameter introduced in [1.2.4](CHANGELOG.md#124-2025-04-08), applies if upgrading from 1.2.4 or newer)**:**
   - Remove the parameter.
   - Then decide:
     - You want the connector to manage the snapshot ➜ set `item.snapshot.enabled.mode = COMMAND` and remove any `field.command` mapping (the `command` field will be synthesized automatically as `ADD` / `UPDATE` / `DELETE`).
     - You want manual COMMAND mode (no connector-managed snapshot) ➜ leave `item.snapshot.enabled.mode = NONE` and map `field.command` explicitly alongside `field.key`.

2. **If you used `fields.auto.command.mode.enable = true`** (parameter introduced in [1.3.2](CHANGELOG.md#132-2026-01-26), applies if upgrading from 1.3.2 or newer)**:**
   - Remove the parameter.
   - Set `item.snapshot.enabled.mode = COMMAND`. The `command` field will be synthesized exactly as before.

3. **If you used `record.consume.from` and want to enable snapshot:**
   - The parameter is **ineffective** when `item.snapshot.enabled.mode != NONE`. Under snapshot, partition positioning is connector-managed: newly assigned partitions seek to the beginning, re-assigned partitions resume from the committed offset.

4. **If you used `record.extraction.error.strategy = FORCE_UNSUBSCRIPTION` and want to enable snapshot:**
   - The strategy is **silently overridden to `IGNORE_AND_CONTINUE`** when `item.snapshot.enabled.mode != NONE`.
   - Rationale: under snapshot the eager consumer runs continuously, and a `FORCE_UNSUBSCRIPTION` on a single bad record would tear down the per-item store permanently, breaking all future snapshots.
   - The new [Poison-pill tolerance](README.md#poison-pill-tolerance) feature covers single malformed records safely (WARN-logged with topic / partition / offset and skipped), and still fails fast when _all_ records in a batch fail (likely misconfiguration).

### Producer-side changes

5. **If you produced sentinel records with `key = snapshot` and `command ∈ {CS, EOS}`** (only relevant when paired with `fields.evaluate.as.command.enable = true`, i.e. if upgrading from [1.2.4](CHANGELOG.md#124-2025-04-08) or newer)**:**
   - Stop producing them — they are now treated as regular records.
   - Snapshot lifecycle is driven entirely by `item.snapshot.enabled.mode` on the connector side. Producers no longer participate in snapshot signaling.

### Behavioral changes under snapshot

When `item.snapshot.enabled.mode` is set to any value other than `NONE`:

- The internal Kafka Consumer starts **eagerly at bind time**, replays the topic from the beginning to pre-seed the Lightstreamer Server per-item store, then transitions to realtime tailing. Late subscribers receive the materialized snapshot followed by realtime updates.
- Partition position management is fully connector-managed (see point 3 above).
- `record.extraction.error.strategy` is constrained as described in point 4 above.
- The subscription `Mode` of any affected item is **pinned** to the value implied by the configured snapshot mode (`MERGE`, `DISTINCT`, or `COMMAND`); a client requesting a different `Mode` will be rejected.
- For `item.snapshot.enabled.mode = COMMAND`, `field.key` must be the constant expression `#{KEY}` and `field.command` must **not** be mapped (the `command` field is synthesized).
- Optional [`item.snapshot.max.idle.seconds`](README.md#itemsnapshotmaxidleseconds) discards the snapshot of an item after a configurable idle period; the next incoming record starts a fresh one.

See [Snapshot Management](README.md#snapshot-management) and [Connector-Managed Snapshot](README.md#connector-managed-snapshot) for the full reference.
