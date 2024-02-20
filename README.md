# Lightstreamer Kafka Connector

## Introduction

The _Lightstreamer Kafka Connector_ is a ready-made pluggable Lighstreamer Adapter that enables event streaming from a Kafka broker to the internet.

[Insert Diagram here]

With Kafka Connector, any internet client connected to the Lightstreamer Server can consume events from Kafka topics like any other Kafka client. The Connector takes care of processing records received from Kafka to adapt them as real-time updates for the clients.

The Kafka Connector allows to move high volume data out of Kafka by leveraging the battle-tested ability of the Lightstreamer real-time engine to deliver live data reliably and efficiently over internet protocols.

### Features

[...] TO TDO

### Quick Start

#### Requirements

- JDK version 17 or later.
- [Lightstreamer Server](https://lightstreamer.com/download/) version 7.4.1  or later (check the `LS_HOME/GETTING_STARTED.TXT` file for the instructions).
- A running Kafka Cluster.
- The [JBang](https://www.jbang.dev/documentation/guide/latest/installation.html) tool for running the consumer/producer example clients.
 
#### Deploy

Get the deployment package from the [latest release page](releases). Alternatively, check out this repository and run the following command from the project root;

`./gradlew distribuite`

which generated the `build/distributions/lightstreamer-kafka-connector-<version>.zip` bundle.

Then, unzip it into the `adapters` folder of the Lightstreamer Server installation.
Check that the final Lightstreamer layout looks like the following:

```sh
LS_HOME/
...
├── adapters
│   ├── lightstreamer-kafka-connector-<version>
│   │   ├── README.md
│   │   ├── adapters.xml
│   │   ├── log4j.properties
│   │   ├── lib
│   └── welcome_res
...
├── audit
├── bin
...
```

#### Configure

Edit the `QuickStart` configuration in the `LS_HOME/adapters/lightstreamer-kafka-connector/adapters.xml` file as follows:

- Update the `bootstrap.servers` parameter with the connection string of the Kafka Cluster.
- Optionally customize the `LS_HOME/adapters//lightstreamer-kafka-connectors/log4j.properties` file (the current settings produce the additional `quickstart.log` file).

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

   The following section defines how the record is mapped to the tabular form of Lightstreamer fields, by using an intuitive set of _Selector Keys_ (denoted with `#{}`)  through which each part of a Kafka Record can be extracted.

   ```xml
   <!-- FIELDS MAPPING SECTION -->

   <!-- Extraction of the record key mapped to the field "key". -->
   <param name="field.key">#{KEY}</param>

   <!-- Extraction of the record value mapped to the field "value". -->
   <param name="field.value">#{VALUE}</param>

   <!-- Extraction of the record timestamp to the field "ts". -->
   <param name="field.ts">#{TIMESTAMP}</param>

   <!-- Extraction of the record partition mapped to the field "partition". -->
   <param name="field.partition">#{PARTITION}</param>

   <!-- Extraction of the record offset mapped to the field "offset". -->
   <param name="field.offset">#{OFFSET}</param>  
   ```
   
   To launch the Consumer, execute the provided minimal [`lsclient.java`](src/clients/lsclient.java) script to connect to Lighstreamer and subscribe to `sample`:

    ```sh
    jbang run src/clients/lsclient.java --address http://localhost:8080 --adapter-set KafkaConnector --data-adapter QuickStart --items sample --fields key,value,partition,offset
    ```
    
    As you can see, you have to specify a few parameters:

    - `--address`, the Lightstreamer Server address.
    -  `--adapter-set`, the name of the requested Adapter Set, which triggers Ligthtreamer to look at the KafakConnector deployed into the `adapters` folder.
    - `--data-adapter`, the name of the requested Data Adapter, which identifies the selected Kafka connection configuration.
    - `--items`, the list of items to subscribe to.
    - `--fields`, the list of requested fields for the items.

    **NOTE:** As the _Lightstreamer Kafka Connector_ is built around the [_Lightreamer Java In-Process Adapter SDK_](https://github.com/Lightstreamer/Lightstreamer-lib-adapter-java-inprocess), every remote client based on any _Lightstreamer Client SDK_, like the _lsclient.java_ script, can interact with it.
    
4. Publish Events.

   From another shell, execute the simple [`kafka-producer.java `](src/clients/kafka-producer.java) script to start publishing events to the Kafka Cluster:

   ```sh
   jbang src/clients/kafka-producer.java --bootstrap-servers <kafka_cluster_address> --topic sample-topic
   ```

   which will send a simple random string every 250 ms to the `sample-topic`.

5. Check Consumed Events.

   After starting the publisher, from the consumer shell, you should immediately see the real-time updates flowing from the consumer shell:

   INSERT VIDEO HERE

### Configuration

As already anticipated, the Lightstreamer Kafka Connector is a Lightstreamer Adapter Set, which means it is made up of a Metadata Adapter and one or more Data Adapters, whose settings are defined in the `LS_HOME/adapters/lightstreamer-kafka-connector/adapters.xml` file.

The following sections will guide you through the configuration details.

#### General Configuration

- (Mandatory) **Kafka Connector Identifier**

  The `id` attribute of the `adapters_conf` root tag defines the _Kafka Connector identifier_, which will be used by the Clients to request this Adapter Set while setting up the connection to a Lighstreamer Server through a _LightstreamerClient_ object.

  The factory value is set to `KafkaConnector` for convenience, but you are free to change it as per your requirements.

  Example:

  ```xml
  <adapters_conf id="KafkaConnector">
  ```

- (Mandatory) **Adapter Class**

  The `adapter_class` tag, specified inside the `metadata_provider` block, defines the Java class name of the Metadata Adapter. 

  The factory value is set to `com.lightstreamer.kafka_connector.adapters.ConnectorMetadataAdapter`, which implements the Kafka Connector logic.
  
  It is possible to provide a custom implementation by extending the `KafakConnectorMetadataAdapter` class: just package your new class in a jar file and deploy it along with all required dependencies into the `LS_HOME/adapters/lightstreamer-kafka-connector/lib` folder.

  Example:

  ```xml
  ...
  <metadata_provider>
      ...
      <adapter_class>your.custom.class</adapter_class>
      ...
  </metadata_provider>
  ...
  ```

- (Mandatory) **`logging.configuration.file`**

  The path of the [reload4j](https://reload4j.qos.ch/) configuration file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector`).

  The parameter is specified inside the `metadata_provider` block.

  The factory value points to the predefined file `LS_HOME/adapters/lightstreamer-kafka-connector/log4g.properties`.

  Example:

  ```xml
  ...
  <metadata_provider>
      ...
      <param name="logging.configuration.file">log4j.properties</param>
      ...
  </metadata_provider>
  ...
  ```

#### Connection Configuration

The Lightstreamer Kafka Connector allows the configuration of separate independent connections to different Kafka clusters. 

Every single connection is configured via the definition of its own Data Adapter through the `data_provider` block. At least one connection must be provided.

Since the Kafka Connector manages the physical connection to Kafka by wrapping an internal Kafka Consumer, many configuration settings in the Data Adapter are identical to those required by the usual Kafka Consumer configuration.

- (Optional) **Connection Name**
  
  The Kafka Connector leverages the `name` attribute of the `data_provider` tag as the connection name, which will be used by the Clients to request real-time data from this specific Kafka connection through a _Subscription_ object.

  The connection name is also used to group all logging messages belonging to the same connection

  Example:

  ```xml
  <data_provider name="BrokerConnection">
  ```

  Default value: `DEFAULT`, but only one `DEFAULT` configuration is permitted.

- (Optional) **`enable`**

  The `enable` parameter specifies whether this connection is enabled or not. Can be one of the following:
  - `true`
  - `false`
  
  Default value: `true`.

  If disabled, Lightstreamer Server will automatically deny every subscription made to this connection.

  Example:

  ```xml
  <param name="enable">false</param>
  ```

- (Mandatory) **`bootstrap.servers`**

  The Kafka Cluster bootstrap server endpoint expressed as the list of host/port pairs used to establish the initial connection.
  
  The parameter sets the value of the [`bootstrap.servers`](https://kafka.apache.org/documentation/#consumerconfigs_bootstrap.) key to configure the internal Kafka Consumer.

  Example:

  ```xml
  <param name="bootstrap.servers">broker:29092,broker:29093</param>
  ```
  
- (Optional) **`group.id`**

  The name of the consumer group this connection belongs to.

  The parameter sets the value for the [`group.id`](https://kafka.apache.org/documentation/#consumerconfigs_group.id) key to configure the internal Kafka Consumer.

  Default value: _KafkaConnector Identifier_ + _Connection Name_ + _Randomly generated suffix_.

  ```xml
  <param name="group.id">kafka-connector-group</param>
  ```

- (Optional) **`record.extraction.error.strategy`**

  The error handling strategy to be used if an error occurs while extracting data from incoming records. Can be one of the following:

  - `IGNORE_AND_CONTINUE`, ignore the error and continue to process the next record.
  - `FORCE_UNSUBSCRIPTION`, stop processing records and force unsubscription of the items requested by all the Clients subscribed to this connection.
                
  Default value: `IGNORE_AND_CONTINUE`.

  Example:
  
  ```xml
  <param name="record.extraction.error.strategy">FORCE_UNSUBSCRIPTION</param>
  ```

##### Topic Mapping

  
- (Mandatory) **`map.<topic>.to`** and **`item-template.<name>`**

  Am item template instructs the Kafka Connector on how to route a subscribed item how a subscribed item is 
  
  Example:

  
  ```xml
  <param name="item-template.stock">stock</param>
  <param name="item-template.stock">stock</param>


##### Encryption


##### Authentication


   



