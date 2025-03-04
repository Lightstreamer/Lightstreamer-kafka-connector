# Lightstreamer Kafka Connector

_Lightstreamer Kafka Connector_ is a ready-made pluggable Lightstreamer Adapter that enables event streaming from a Kafka broker to the internet.

## Getting Started

### Requirements

- [Lightstreamer Server](https://lightstreamer.com/download/) version 7.4.2 or later (check the `LS_HOME/GETTING_STARTED.TXT` file for the instructions)
- A running Kafka broker or Kafka Cluster

### Deploy

Unzip the `lightstreamer-kafka-connector-<version>.zip` file into the `adapters` folder of the Lightstreamer Server installation. Check that the Lightstreamer layout looks like the following:

```sh
LS_HOME/
...
├── adapters
│   ├── lightstreamer-kafka-connector-<version>
│   │   ├── LICENSE
│   │   ├── README.md
│   │   ├── adapters.xml
│   │   ├── javadoc
│   │   ├── lib
│   │   ├── log4j.properties
│   └── welcome_res
...
├── audit
├── bin
...
```

### Configure

1. Update the [`bootstrap.servers`](https://github.com/Lightstreamer/Lightstreamer-kafka-connector?tab=readme-ov-file#bootstrapservers) parameter with the connection string of Kafka:

  ```xml
  <param name="bootstrap.servers">kafka.connection.string</param>
  ```

2. Optionally customize the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/log4j.properties` file (the current settings produce the additional `quickstart.log` file).

3. See the [Configuration](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/?tab=readme-ov-file#configuration) section of the Kafka Connector GitHub [repository](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/) for all possible configuration options.

### Start

1. Launch Lightstreamer Server.

   From the `LS_HOME/bin/unix-like` directory, run the following:

   ```sh
   $ ./start_background.sh
   ```

2. Verify that the server is up and running by looking at the logs from the `LS_HOME/logs` directory:

   ```sh
   $ tail -f LS.out

   14.Mar.24 17:55:12,875 < INFO> Created selector thread: NIO CHECK SELECTOR 15.
   14.Mar.24 17:55:12,875 < INFO> Created selector thread: NIO CHECK SELECTOR 16.
   14.Mar.24 17:55:12,876 < INFO> Created selector thread: NIO CHECK SELECTOR 17.
   14.Mar.24 17:55:12,876 < INFO> Created selector thread: NIO CHECK SELECTOR 18.
   14.Mar.24 17:55:12,877 < INFO> Created selector thread: NIO CHECK SELECTOR 19.
   14.Mar.24 17:55:12,877 < INFO> Created selector thread: NIO CHECK SELECTOR 20.
   14.Mar.24 17:55:12,880 < INFO> Lightstreamer Server initialized.
   14.Mar.24 17:55:12,880 < INFO> Lightstreamer Server 7.4.0 build 2326 starting...
   14.Mar.24 17:55:12,887 < INFO> Session creation on socket 'Lightstreamer HTTP Server' will be bound to any queue limits for SERVER pool.
   14.Mar.24 17:55:12,888 < INFO> Server 'Lightstreamer HTTP Server' listening to *:8080 ...
   ```

## Docs

You may find the full documentation of Kafka Connector on the GitHub [repository](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/).

In addition, the local [javadoc](javadoc/) folder contains the complete [Kafka Connector API Specification](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc) for developing a custom [Kafka Connector Metadata Adapter class](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/?tab=readme-ov-file#customize-the-kafka-connector-metadata-adapter-class).

## Examples

The GitHub repository hosts several [examples](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/tree/main/examples) showing you how to use Kafka Connector. In particular, you may explore the [_Airport Demo_](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/tree/main/examples/airport-demo) for deeper insights into various usage and configuration options of Kafka Connector.
