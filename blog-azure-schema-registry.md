# Lightstreamer Kafka Connector 1.5.0: Native Azure Schema Registry Support for Real-Time Kafka Streaming

---

## Introducing Azure Schema Registry Support in Lightstreamer Kafka Connector 1.5.0

Real-time data streaming just got a lot more powerful for teams building on Microsoft Azure. **Lightstreamer Kafka Connector version 1.5.0**, released March 18, 2026, ships with native support for **Azure Schema Registry** — the enterprise-grade schema management service built into Azure Event Hubs namespaces.

If your organization runs Apache Kafka workloads on **Azure Event Hubs** and enforces schema contracts using Avro or JSON, you can now stream those validated, strongly-typed Kafka records all the way to web browsers, mobile apps, and IoT devices — with zero polling and zero compromise on schema governance.

---

## What Is Azure Schema Registry and Why Does It Matter?

[Azure Schema Registry](https://learn.microsoft.com/en-us/azure/event-hubs/schema-registry-overview) is a centralized repository for managing message schemas in Azure Event Hubs. It ensures that Kafka producers and consumers agree on the structure of messages flowing through your event-driven architecture.

By enforcing schema validation at the serialization layer, Azure Schema Registry:

- **Prevents breaking changes** from propagating silently through a data pipeline
- **Reduces message payload size** by sending a compact schema reference instead of embedding the full schema in every record
- **Enables schema evolution** with forward and backward compatibility modes
- **Aligns with enterprise governance** requirements for regulated industries
ada
---

## How Lightstreamer Bridges Kafka and End-User Clients

Kafka is designed for high-throughput internal data movement — not for direct access from internet-connected devices. Opening Kafka directly to browsers or mobile apps would expose your brokers to the public internet, overwhelm them with concurrent consumer connections, and ignore the realities of unreliable networks, firewalls, and NAT traversal.

[Lightstreamer Kafka Connector](https://lightstreamer.com/products/kafka-connector/) solves this by acting as an intelligent last-mile proxy between your Kafka cluster and remote clients:

- **Subscribes to Kafka topics** as a native consumer or as a Kafka Connect Sink Connector
- **Applies server-side filtering and conflation**, reducing bandwidth consumption and delivering only the data each client needs
- **Fans out messages** to millions of concurrent WebSocket clients with adaptive bandwidth management
- **Manages client lifecycle transparently** — handling reconnections, disconnections, and slow consumers without impacting your Kafka infrastructure

With version 1.5.0, this delivery pipeline is now fully schema-aware when running on Event Hubs — the connector deserializes Avro and JSON payloads using schemas fetched directly from Azure Schema Registry before forwarding them to clients.

### End-to-End Data Flow

Here is how data moves through the system with Azure Schema Registry enabled:

1. A **Kafka producer** serializes a record using the Azure Schema Registry serializer, which registers (or looks up) the schema and embeds a compact schema reference in the message payload
2. The record is published to an **Event Hubs topic** (Kafka-compatible)
3. **Lightstreamer Kafka Connector** consumes the record, reads the schema reference from the payload header, and fetches the corresponding schema from Azure Schema Registry using Entra ID credentials
4. The connector **deserializes** the payload into structured fields and maps them to Lightstreamer items based on the topic mapping configuration
5. Lightstreamer **streams the structured data** over WebSocket to subscribed web and mobile clients in real time

The entire flow preserves schema governance end-to-end — from the moment the producer writes a record to the moment a browser renders it.

---

## What's New in Version 1.5.0: Azure Schema Registry Integration

### Schema Registry Provider Selection

The connector now supports two Schema Registry providers, configurable via the `schema.registry.provider` parameter in `adapters.xml`:

| Provider | Value | Use Case |
|---|---|---|
| Confluent Schema Registry | `CONFLUENT` (default) | Self-hosted or Confluent Cloud |
| Azure Schema Registry | `AZURE` | Azure Event Hubs namespaces |

Setting `schema.registry.provider` to `AZURE` activates the full Azure Schema Registry client, including Microsoft Entra ID (formerly Azure Active Directory) authentication.

> **Note:** Azure Schema Registry is an Azure-native service tied to Event Hubs namespaces and is not available outside of Azure. If you are running self-hosted Kafka or Confluent Cloud, use the `CONFLUENT` provider instead.

### Supported Serialization Formats

Azure Schema Registry supports **Avro** and **JSON** schema formats. Consequently, when using the `AZURE` provider, the `record.value.evaluator.type` parameter must be set to either `AVRO` or `JSON`. **Protobuf is not supported** by Azure Schema Registry and is not available with this provider. If your pipeline requires Protobuf deserialization, use the `CONFLUENT` provider with Confluent Schema Registry instead.

#### Avro vs JSON: Which Format Should You Choose?

Both formats are fully supported, but they serve different needs:

- **Avro** is a compact binary format that produces smaller payloads, offers faster serialization, and enforces strict schema validation at write time. It is the preferred choice for high-throughput pipelines such as financial market data, telemetry ingestion, and event sourcing — where every byte and millisecond counts.

- **JSON Schema** produces human-readable payloads that are easier to inspect and debug. It is well-suited for integration scenarios where downstream consumers (or the producers themselves) work with JSON natively, or where teams are transitioning incrementally toward schema governance.

The connector handles both formats transparently — you only need to set `record.value.evaluator.type` to `AVRO` or `JSON` and ensure your producers use the matching Azure Schema Registry serializer.

### Choosing Between Confluent and Azure Schema Registry

With version 1.5.0, the connector supports both Confluent and Azure Schema Registry. The right choice depends on your Kafka infrastructure:

| Consideration | Confluent | Azure |
|---|---|---|
| **Kafka platform** | Self-hosted Kafka, Confluent Cloud, or any standard Kafka broker | Azure Event Hubs (Kafka-compatible) |
| **Schema formats** | Avro, JSON Schema, Protobuf | Avro, JSON Schema |
| **Authentication** | HTTP basic auth, mTLS, OAuth | Microsoft Entra ID (service principal) |
| **Schema management** | Confluent Control Center or REST API | Azure Portal or Azure CLI |
| **Best for** | Multi-cloud or hybrid Kafka deployments | Azure-native architectures using Event Hubs |

If your Kafka workloads run on Event Hubs and your organization already uses Entra ID for identity management, Azure Schema Registry is the natural fit — it eliminates the need to deploy and manage a separate schema registry service.

### Microsoft Entra ID Authentication

The connector authenticates to Azure Schema Registry using a **Microsoft Entra ID service principal**. Authentication requires three configuration parameters:

- `schema.registry.azure.tenant.id` — the Directory (tenant) ID
- `schema.registry.azure.client.id` — the Application (client) ID
- `schema.registry.azure.client.secret` — the client secret value

Version 1.5.0 uses client secret credentials. Support for Managed Identity authentication may be added in a future release.

---

## Step-by-Step Configuration Guide

### Step 1: Azure Prerequisites

Before configuring the connector, complete the following in the Azure Portal:

**1.1 — Create or identify your Event Hubs namespace**
Your namespace URL will follow the pattern `https://<namespace>.servicebus.windows.net`. This is your `schema.registry.url`.

**1.2 — Create a Schema Group**
Inside your Event Hubs namespace, navigate to the Schema Registry section and create a Schema Group (e.g., `my-schema-group`). Select the serialization type (Avro or JSON) and configure its compatibility mode \u2014 None, Backward, Forward, or Full \u2014 depending on how strictly you want to enforce schema evolution rules across your producer teams.

**1.3 — Register a Microsoft Entra application**
In Microsoft Entra ID (Azure Active Directory):
- Create an App Registration and note the **Directory (tenant) ID** and **Application (client) ID**
- Under _Certificates & secrets_, create a new client secret and copy its value immediately

**1.4 — Assign IAM roles**
On your Event Hubs namespace (not just the hub), assign:
- **Schema Registry Reader** to the service principal used by the connector
- **Schema Registry Contributor** to the service principal used by any Kafka producer that registers schemas at runtime (`auto.register.schemas=true`)

---

### Step 2: Configure the Lightstreamer Kafka Connector

In your `adapters.xml` file, add the following parameters to your Kafka connector adapter configuration:

```xml
<!-- Enable Schema Registry deserialization -->
<param name="record.value.evaluator.schema.registry.enable">true</param>

<!-- Serialization format: AVRO or JSON (Protobuf not supported with Azure) -->
<param name="record.value.evaluator.type">AVRO</param>

<!-- Azure Schema Registry provider -->
<param name="schema.registry.provider">AZURE</param>

<!-- Azure Event Hubs namespace URL -->
<param name="schema.registry.url">$env.SCHEMA_REGISTRY_URL</param>

<!-- Microsoft Entra ID service principal credentials -->
<param name="schema.registry.azure.tenant.id">$env.AZURE_TENANT_ID</param>
<param name="schema.registry.azure.client.id">$env.AZURE_CLIENT_ID</param>
<param name="schema.registry.azure.client.secret">$env.AZURE_CLIENT_SECRET</param>
```

Lightstreamer supports environment variable substitution in `adapters.xml` using the `$env.VARIABLE_NAME` syntax. The corresponding values are then provided at runtime — for example, through Docker Compose:

```yaml
environment:
  - SCHEMA_REGISTRY_URL=https://my-namespace.servicebus.windows.net
  - AZURE_TENANT_ID=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
  - AZURE_CLIENT_ID=11111111-2222-3333-4444-555555555555
  - AZURE_CLIENT_SECRET=<your-client-secret>
```

This approach keeps sensitive credentials out of your configuration files.

---

### Step 3: Tune Kafka Consumer Settings for Azure Event Hubs

Azure Event Hubs imposes specific timeout constraints on Kafka consumers that differ from the defaults used by standard Kafka brokers. If these timeouts are not configured correctly, the Event Hubs service may trigger premature consumer group rebalances — causing the connector to temporarily stop receiving messages and disrupting streaming delivery to connected clients.

Version 1.5.0 introduces two new parameters that are particularly recommended when connecting to Event Hubs:

```xml
<param name="record.consume.with.max.poll.interval.ms">50000</param>
<param name="record.consume.with.session.timeout.ms">30000</param>
```

The [`record.consume.with.max.poll.interval.ms`](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/blob/main/README.md#recordconsumewithmaxpollintervalms) parameter controls the maximum time between consecutive poll calls before the consumer is considered unresponsive, while [`record.consume.with.session.timeout.ms`](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/blob/main/README.md#recordconsumewithsessiontimeoutms) sets the timeout for detecting consumer failures. The values above align with Event Hubs' service-side expectations. Adjust them if your environment has different latency characteristics, but keep them within the ranges documented by Azure.

For the full list of configuration parameters, refer to the [Schema Registry](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/blob/main/README.md#schema-registry) and [Azure Schema Registry Parameters](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/blob/main/README.md#azure-schema-registry-parameters) sections in the official documentation.

---

### Step 4: Configure Your Java Kafka Producer

Your Kafka producer must use the Microsoft Azure Schema Registry serializers (Avro or JSON) so that messages are encoded with the schema references the connector expects. A complete Java producer configuration example — including Event Hubs SASL authentication and Entra ID credentials — is available in the [Azure Event Hubs Quickstart](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/tree/main/examples/vendors/azure/quickstart-azure).

---

## Real-World Use Cases

### Financial Market Data on Azure
Before 1.5.0, institutions streaming market data from Event Hubs had to build custom middleware to strip Azure's schema envelope before Lightstreamer could deliver tick data to trading dashboards. This middleware added latency, operational complexity, and a single point of failure in the data path. Now the connector handles Avro deserialization natively — eliminating that middleware layer and preserving end-to-end schema validation from the moment a market data producer writes a trade event to the moment a trader sees it in the browser.

### Connected IoT Telemetry
IoT platforms previously had to choose between schema governance (using the registry to enforce device telemetry contracts) and real-time delivery (using Lightstreamer to push updates to mobile apps) — bridging the two required a manual translation step that increased latency and introduced the risk of schema drift. With native Azure Schema Registry support, sensor telemetry published in schema-validated JSON flows directly from Event Hubs through the connector to monitoring apps, with every field correctly deserialized and mapped. Operations teams see validated, structured data in their dashboards without writing a single line of deserialization code.

### Logistics and Supply Chain
Multi-team supply chain pipelines benefit most from schema governance — multiple producer teams across warehouses, transportation, and inventory systems must agree on event formats. But before 1.5.0, enforcing schemas via Azure Schema Registry while streaming updates to operations dashboards through Lightstreamer meant duplicating deserialization logic in a custom adapter. The connector now owns that responsibility natively, letting teams focus on schema design, compatibility policies, and business logic rather than infrastructure plumbing.

---

## Get Started

With version 1.5.0, Lightstreamer Kafka Connector closes a critical integration gap for teams building real-time applications on Azure — enabling end-to-end schema-governed pipelines from Event Hubs all the way to web and mobile clients, without custom deserialization middleware.

**Download Lightstreamer Kafka Connector 1.5.0** from the official product page at [lightstreamer.com/products/kafka-connector](https://lightstreamer.com/products/kafka-connector/) or pull the official Docker image:

```bash
docker pull ghcr.io/lightstreamer/lightstreamer-kafka-connector:1.5.0
```

The complete source, configuration references, and quickstart examples are available on the [GitHub repository](https://github.com/Lightstreamer/Lightstreamer-kafka-connector). The `examples/vendors/azure/quickstart-azure/` directory contains a ready-to-run Docker Compose-based quickstart for Event Hubs, including a full schema registry integration walkthrough. For the complete list of changes in this release, see the [CHANGELOG](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/blob/main/CHANGELOG.md).

Have feedback or questions? Open an issue on the [GitHub repository](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/issues) — we'd love to hear how you're using the connector with Azure.

---

*Tags: Apache Kafka, Azure Event Hubs, Azure Schema Registry, Avro, JSON Schema, Real-Time Streaming, WebSocket, Lightstreamer, Kafka Connector, Microsoft Entra ID, Last-Mile Streaming, Event-Driven Architecture*
