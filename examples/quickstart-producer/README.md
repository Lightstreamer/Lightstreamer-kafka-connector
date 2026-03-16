# Quickstart Producer

This folder contains the Gradle project for the Kafka producer used across all _Quickstart_ examples. The producer simulates a real-time stock market feed and continuously publishes stock price update events to a configured Kafka topic.

It is automatically built and packaged into a Docker image as part of the `start.sh` script used by every example, but can also be run standalone. See the [Quick Start](../../README.md#quick-start-set-up-in-5-minutes) section for more details.

## Supported Serialization Formats

The producer automatically detects the serialization format from the `value.serializer` property in the configuration file and supports:

- **Protobuf** — via the Confluent Schema Registry serializer or a custom serializer
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

> **Note:** The `Dockerfile` copies the pre-built jar from `build/libs/`, so `./build.sh` must be run first.

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
