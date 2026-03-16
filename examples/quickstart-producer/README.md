# Quickstart Producer

This folder contains the Gradle project for the Kafka producer used across all _Quickstart_ examples. The producer simulates a real-time stock market feed and continuously publishes stock price update events to a configured Kafka topic.

It is automatically built and packaged into a Docker image as part of the `start.sh` script used by every example, but can also be run standalone. See the [Quick Start](../../README.md#quick-start-set-up-in-5-minutes) section for more details.

## Supported Serialization Formats

The producer automatically detects the serialization format from the `value.serializer` property in the configuration file and supports:

- **Protobuf** — via the Confluent Schema Registry serializer or a custom serializer (not supported by Azure Schema Registry)
- **Avro** — via the Confluent or Azure Schema Registry serializer
- **JSON** — via the Confluent plain JSON serializer
- **JSON Schema** — via the Confluent or Azure Schema Registry JSON serializer

## Building

To build the fat jar:

```sh
$ ./build.sh
```

This runs the Gradle build and generates `build/libs/quickstart-producer-<version>-all.jar`.

To then build the Docker image (done automatically by `docker compose up --build` in each example):

```sh
$ docker build -t quickstart-producer .
```

> [!IMPORTANT]
> The `Dockerfile` copies the pre-built jar from `build/libs/`, so `./build.sh` must be run first.

## Running Standalone

```sh
$ java -jar build/libs/quickstart-producer-<version>-all.jar \
    --topic <topic> \
    [--bootstrap-servers <kafka.connection.string>] \
    [--config-file <config.properties>]
```

| Argument | Required | Description |
|---|---|---|
| `--topic` | Yes | The target Kafka topic to publish stock events to |
| `--bootstrap-servers` | No | The Kafka broker connection string; can be provided via the config file instead |
| `--config-file` | No | Path to a Kafka producer configuration file (properties format) |

## Configuration Examples

Pick one configuration from each section and combine them in your `config.properties` file, then pass it via `--config-file`.

### Connection & Authentication

#### No Authentication (Plaintext)

No config file is needed. Pass the broker address directly on the command line:

```sh
$ java -jar build/libs/quickstart-producer-<version>-all.jar \
    --bootstrap-servers localhost:9092 \
    --topic stocks
```

#### Mutual TLS (SSL)

Full example: [quickstart-ssl/docker-compose.yml](../quickstart-ssl/docker-compose.yml#L70).

```properties
security.protocol=SSL

# Trust store to authenticate the broker
ssl.truststore.location=/path/to/producer.truststore.jks
ssl.truststore.password=<truststore-password>

# Key store for client certificate (if broker requires mTLS)
ssl.keystore.location=/path/to/producer.keystore.jks
ssl.keystore.password=<keystore-password>
ssl.key.password=<key-password>

# Disable hostname verification (if using self-signed certs)
ssl.endpoint.identification.algorithm=
```

#### SASL/PLAIN over SSL

Full example: [quickstart-confluent-cloud/docker-compose.yml](../vendors/confluent/quickstart-confluent-cloud/docker-compose.yml#L33).

```properties
sasl.mechanism=PLAIN
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="<api-key>" \
    password="<api-secret>";
```

For **Azure Event Hubs** (see [quickstart-azure/docker-compose.yml](../vendors/azure/quickstart-azure/docker-compose.yml#L32)), use `$ConnectionString` as the username and the full connection string as the password:

```properties
sasl.mechanism=PLAIN
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="$ConnectionString" \
    password="Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
```

#### SASL/SCRAM over SSL

Full examples:
- [quickstart-aiven/docker-compose.yml](../vendors/aiven/quickstart-aiven/docker-compose.yml#L37)
- [quickstart-redpanda-serverless/docker-compose.yml](../vendors/redpanda/quickstart-redpanda-serverless/docker-compose.yml#L34)
- [quickstart-axual/docker-compose.yml](../vendors/axual/quickstart-axual/docker-compose.yml#L37)

Use `SCRAM-SHA-256` or `SCRAM-SHA-512` depending on the broker configuration:

```properties
# Use SCRAM-SHA-512 for Axual; SCRAM-SHA-256 for Aiven and Redpanda Serverless
sasl.mechanism=SCRAM-SHA-256
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="<username>" \
    password="<password>";
```

#### SASL/IAM over SSL

Full example: [quickstart-msk/docker-compose.yml](../vendors/aws/quickstart-msk/docker-compose.yml#L39).

```properties
sasl.mechanism=AWS_MSK_IAM
security.protocol=SASL_SSL
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required \
    awsDebugCreds=true \
    awsProfileName="msk_client";
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

### Serialization Format

By default (no `value.serializer` specified), messages are sent as plain JSON using the built-in Kafka serializer. To use a schema registry, add one of the following blocks to your config.

#### Confluent Schema Registry

```properties
# Choose one serializer:
value.serializer=io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
# value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
# value.serializer=io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer

schema.registry.url=https://<schema-registry-host>:<port>

# Optional: mTLS for Schema Registry
schema.registry.ssl.truststore.location=/path/to/producer.truststore.jks
schema.registry.ssl.truststore.password=<truststore-password>
schema.registry.ssl.keystore.location=/path/to/producer.keystore.jks
schema.registry.ssl.keystore.password=<keystore-password>
schema.registry.ssl.key.password=<key-password>
```

#### Azure Schema Registry

> [!NOTE]
> Azure Schema Registry does not support Protobuf serialization.

```properties
# Choose one serializer:
value.serializer=com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer
# value.serializer=com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer

auto.register.schemas=true  # set to false or omit if the schema is already registered
schema.registry.url=https://<namespace>.servicebus.windows.net/
schema.group=<schema-group>

# Microsoft Entra ID service principal credentials
tenant.id=<tenant-id>
client.id=<client-id>
client.secret=<client-secret>
```
