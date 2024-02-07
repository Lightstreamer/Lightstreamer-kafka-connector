# Lightstreamer Kafka Connector

## Introduction

The _Lightstreamer Kafka Connector_ is a ready-made pluggable Lighstreamer Adapter that enables event streaming from a Kafka broker to the internet.

[Insert Diagram here]

With Kafka Connector, any internet client connected to the Lightstreamer Server can consume events from Kafka topics like any other Kafka client. The Connector takes care of processing records received from Kafka to adapt them as real-time updates for the clients.

The Kafka Connector allows to move high volume data out of Kafka by leveraging the battle-tested ability of the Lightstreamer real-time engine to deliver live data reliably and efficiently over internet protocols.

### Features

[...] TO TO

### Quick Start

#### Requirements

- JDK version 17 or later.
- [Lightstreamer Server](https://lightstreamer.com/download/) version 7.4.1  or later (check the `<LS_HOME>/GETTING_STARTED.TXT` file for the instructions).
- A running Kafka Cluster.
- The [JBang](https://www.jbang.dev/documentation/guide/latest/installation.html) tool for running the consumer/producer example clients.
 
#### Deploy

Get the deployment package from the [latest release page](releases). Alternatively, check out this repository and run the following command from the project root;

`./gradlew distribute`

which generated the `build/distributions/lightstreamer-kafka-connector-<version>.zip` bundle.

Then, unzip it into the `adapters` folder of the Lightstreamer Server installation.
Check that the final Lightstreamer layout looks like the following:

```sh
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

- Update the `bootstrap.servers` parameter with the connection string of the Kafka Cluster.
- Optionally customize the `<LS_HOME>/lightstreamer-kafka-connectors/log4j.properties` file (the current settings produce the additional `quickstart.log` file).

### Start

1. Launch Lightstreamer Server.

2. Attach a Lightstreamer Consumer.

   The _Consumer_ is a simple Lightstreamer Java client that subscribes to the `sample` item to receive real-time data through the fields ....

   In the `QuickStart` configuration, the `sample` item is mapped by the Kafka topic `sample-topic` through the following section:
   
   ```xml
   <!-- TOPIC MAPPING SECTION -->

   <!-- Define a "sample" item-template, which is simply made of the "sample" item name to be used by the Lighstreamer Client subscription. -->
   <param name="item-template.sample">sample</param>

   <!-- Map the Kafka topic "sample-topic" to the previous defined "sample" item template. -->
   <param name="map.sample-topic.to">item-template.sample</param>
   ```
   
   Every single event published to `sample-topic` will be processed and then routed by the _Kafka Connector_ to the `sample` item.

   The fields mapping section configures how the record is mapped to tabular form of Lightstreamer fields.
   
  
   

   To launch the Consumer, execute the provided minimal [`lsclient.java`](src/clients/lsclient.java) script to connect to Lighstreamer and subscribe to `sample`:

    ```sh
    jbang run src/clients/lsclient.java --address http://localhost:8080 --adapter-set KafkaConnector --data-adapter QuickStart --items sample --fields key,value,partition,offset
    ```
    
    As you can see, you have to specify a few parameters:

    - `--address`, the Lightstreamer Server address.
    -  `--adapter-set`, the name of the requested Adapter Set, which triggers Ligthtreamer to look at the KafakConnectored deployed into the `adapters` folder.
    - `--data-adapter`, the name of the requested Data Adapter, which identifies the selected Kafka connection configuration.
    - `--items`, the list of items to subscribe to.
    - `--fields`, the list of requested fields for the items.

    **NOTE:** As the _Lightstreamer Kafka Connector_ is built around the standard _Java In-Process Adapter SDK_, every remote client based on the Lightstreamer Client SDK, like the _lsclient.java_ script, can interact with it.
    
4. Publish Events.

   From another shell, execute the simple [`kafka-producer.java `](src/clients/kafka-producer.java) script to start publishing events to the Kafka Cluster:

   ```sh
   jbang src/clients/kafka-producer.java --bootstrap-servers <kafka_cluster_address> --topic sample-topic
   ```

   which will send a simple random string every 250 ms to the `sample-topic`.

5. Check Consumed Events.

   After starting the publisher, from the consumer shell, you should immediately see the real-time updates flowing from the consumer shell:



### Configuration

### Build from Source

To build the Lightstreamer Kafka Connector deployment package, run;

`./gradlew distribute`

which will produce the `lightstreamer-kafka-connector-<version>.zip` file under the `build/distributions `folder.

