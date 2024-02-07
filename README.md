# Lightstreamer Kafka Connector

## Introduction

The _Lightstreamer Kafka Connector_ is a ready-made pluggable Lighstreamer Adapter that enables event streaming from a Kafka broker to the internet.

[Insert Diagram here]

With Kafka Connector, any internet client connected to the Lightstreamer Server can consume events from Kafka topics like any other Kafka client. The Connector takes care of processing records received from Kafka to adapt them as real-time updates for the clients.

The Kafka Connector allows to move high volume data out of Kafka by leveraging the battle-tested ability of the Lightstreamer real-time engine to deliver live data reliably and efficiently over internet protocols.

### Features

[...]

### Quick Start

#### Requirements

- JDK version 17 or later.
- A [Lightstreamer Server](https://lightstreamer.com/download/) installation (check the `<LS_HOME>/GETTING_STARTED.TXT` file for the instructions).
- A running Kafka Cluster.
- The [JBang](https://www.jbang.dev/documentation/guide/latest/installation.html) tool for running the consumer/producer example clients.
 
#### Deploy

Get the deployment package from the [latest release page](releases). Alternatively, check out this repository and run the following command from the project root.

`./gradlew distribute`

which will produce the bundle `build/distributions/lightstreamer-kafka-connector-<version>.zip`.

Then, unzip it into the `adapters` folder of the Lightstreamer Server installation.
Check that the final Lightstreamer layout looks like the following:

```bash
<LS_HOME>/
...
├── adapters
│   ├── lightstreamer-kafka-connector-0.1.0
│   │   ├── README.md
│   │   ├── adapters.xml
│   │   ├── log4j.properties
│   │   ├── lib
│   └── welcome_res
│       ├── adapters.xml
...
├── audit
├── bin
...
```

#### Configure

Edit the `QuickStart` configuration in the `<LS_HOME>/lightstreamer-kafka-connectors/adapters.xml` file as follows:
    - Update the `bootstrap.servers` parameter with the connection string of the external Kafka cluster.
    - Optionally customize the `<LS_HOME>/lightstreamer-kafka-connectors/log4j.properties` file (the current settings produce the additional `quickstart.log` file).

4. Ensure your Kafka Cluster is up and running.

5. Launch Lightstremaer Server.

6. Connect to Lightstreamer Server with the provided minimal [JBang](https://www.jbang.dev/) script  it if required):
    
    
    ```sh
    jbang run src/clients/lsclient.java --adapter-set KafkaConnector --data-adapter QuickStart --items sample --fields key,value,partition,offset
    ```



### Configuration

### Build from Source

To build the Lightstreamer Kafka Connector deployment package, run;

`./gradlew distribute`

which will produce the `lightstreamer-kafka-connector-<version>.zip` file under the `build/distributions `folder.

