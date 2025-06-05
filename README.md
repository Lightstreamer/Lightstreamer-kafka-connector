<img src="/pictures/lightstreamer.png" width="250">

# Lightstreamer Kafka Connector
_Last-mile data streaming. Stream real-time Kafka data to mobile and web apps, anywhere. Scale Kafka to millions of clients._

- [Introduction](#introduction)
  - [Last-Mile Integration](#last-mile-integration)
  - [Intelligent Streaming](#intelligent-streaming)
  - [Comprehensive Client SDKs](#comprehensive-client-sdks)
  - [Massive Scalability](#massive-scalability)
  - [Other Features](#other-features)
- [Architecture](#architecture)
  - [Kafka Client vs. Kafka Connect](#kafka-client-vs-kafka-connect)
    - [Lightstreamer Kafka Connector as a Kafka Client](#lightstreamer-kafka-connector-as-a-kafka-client)
    - [Lightstreamer Kafka Connector as a Kafka Connect Sink Connector](#lightstreamer-kafka-connector-as-a-kafka-connect-sink-connector)
- [QUICK START: Set up in 5 minutes](#quick-start-set-up-in-5-minutes)
  - [Run](#run)
- [Installation](#installation)
  - [Requirements](#requirements)
  - [Deploy](#deploy)
  - [Configure](#configure)
    - [Connection with Confluent Cloud](#connection-with-confluent-cloud)
    - [Connection with Redpanda Cloud](#connection-with-redpanda-cloud)
  - [Start](#start)
    - [1. Launch Lightstreamer Server](#1-launch-lightstreamer-server)
    - [2. Attach a Lightstreamer consumer](#2-attach-a-lightstreamer-consumer)
    - [3. Publish the Events](#3-publish-the-events)
    - [4. Check the Consumed Events](#4-check-the-consumed-events)
- [Configuration](#configuration)
- [Global Settings](#global-settings)
- [Connection Settings](#connection-settings)
  - [General Parameters](#general-parameters)
  - [Encryption Parameters](#encryption-parameters)
  - [Broker Authentication Parameters](#broker-authentication-parameters)
  - [Record Evaluation](#record-evaluation)
  - [Topic Mapping](#topic-mapping)
    - [Data Extraction Language](#data-extraction-language)
    - [Record Routing (`map.TOPIC_NAME.to`)](#record-routing-maptopic_nameto)
    - [Record Mapping (`field.FIELD_NAME`)](#record-mapping-fieldfield_name)
    - [Filtered Record Routing (`item-template.TEMPLATE_NAME`)](#filtered-record-routing-item-templatetemplate_name)
  - [Schema Registry](#schema-registry)
    - [`schema.registry.url`](#schemaregistryurl)
    - [Basic HTTP Authentication Parameters](#basic-http-authentication-parameters)
    - [Encryption Parameters](#encryption-parameters-1)
    - [Quick Start Schema Registry Example](#quick-start-schema-registry-example)
- [Client Side Error Handling](#client-side-error-handling)
- [Customizing the Kafka Connector Metadata Adapter Class](#customizing-the-kafka-connector-metadata-adapter-class)
  - [Develop the Extension](#develop-the-extension)
- [Kafka Lightstreamer Sink Connector](#kafka-connect-lightstreamer-sink-connector)
  - [Usage](#usage)
    - [Lightstreamer Setup](#lightstreamer-setup)
    - [Running](#running)
    - [Running in Docker](#running-in-docker)
  - [Supported Converters](#supported-converters)
  - [Configuration Reference](#configuration-reference)
- [Docs](#docs)
- [Examples](#examples)

# Introduction

Is your product struggling to deliver Kafka events to remote users? The [Lightstreamer Kafka Connector](https://lightstreamer.com/products/kafka-connector/) is an intelligent proxy that bridges the gap, providing seamless, real-time data streaming to web and mobile applications with unmatched ease and reliability. It streams data in real time to your apps over WebSockets, eliminating the need for polling a REST proxy and surpassing the limitations of MQTT.

## Last-Mile Integration

Kafka, while powerful, isn’t designed for direct internet access—particularly when it comes to the **last mile**, the critical network segment that extends beyond enterprise boundaries and edges (LAN or WAN) to reach end users. Last-mile integration is essential for delivering real-time Kafka data to mobile, web, and desktop applications, addressing challenges that go beyond Kafka’s typical scope, such as:
- Disruptions from corporate firewalls and client-side proxies blocking Kafka connections.
- Performance issues due to unpredictable internet bandwidth, including packet loss and disconnections.
- User interfaces struggling with large data volumes.
- The need for scalable solutions capable of supporting millions of concurrent users.

![High-Level Architecture](/pictures/architecture.png)

## Intelligent Streaming

With **Intelligent Streaming**, Lightstreamer dynamically adjusts the data flow to match each user’s network conditions, ensuring all users stay in sync regardless of connection quality. By resampling and conflating data on the fly, it delivers real-time updates with adaptive throttling, effectively handling packet loss without buffering delays. It also manages disconnections and reconnection seamlessly, keeping your users connected and up-to-date.

## Comprehensive Client SDKs

The rich set of supplied client libraries makes it easy to consume real-time Kafka data across a variety of platforms and languages.

![Client APIs](/pictures/client-platforms.png)

## Massive Scalability

Connect millions of clients without compromising performance. Fanout real-time messages published on Kafka topics efficiently, preventing overload on the Kafka brokers. Check out the [load tests performed on the Lightstreamer Kafka Connector vs. plain Kafka](https://github.com/Lightstreamer/lightstreamer-kafka-connector-loadtest).

## Other Features

The Lightstreamer Kafka Connector provides a wide range of powerful features, including firewall and proxy traversal, server-side filtering, advanced topic mapping, record evaluation, Schema Registry support, push notifications, and maximum security. [Explore more details](https://lightstreamer.com/products/kafka-connector/).

# Architecture

![Architecture](/pictures/architecture-full.png)

The Lightstreamer Kafka Connector seamlessly integrates the [Lightstreamer Broker](https://lightstreamer.com/products/lightstreamer/) with any Kafka broker. While existing producers and consumers continue connecting directly to the Kafka broker, internet-based applications connect through the Lightstreamer Broker, which efficiently handles last-mile data delivery. Authentication and authorization for internet-based clients are managed via a custom Metadata Adapter, created using the [Metadata Adapter API Extension](#customize-the-kafka-connector-metadata-adapter-class) and integrated into the Lightstreamer Broker.

Both the Kafka Connector and the Metadata Adapter run in-process with the Lightstreamer Broker, which can be deployed in the cloud or on-premises.

## Kafka Client vs. Kafka Connect

The Lightstreamer Kafka Connector can operate in two distinct modes: as a direct Kafka client or as a Kafka Connect connector.

### Lightstreamer Kafka Connector as a Kafka Client

In this mode, the Lightstreamer Kafka Connector uses the Kafka client API to communicate directly with the Kafka broker. This approach is typically lighter, faster, and more scalable, as it avoids the additional layer of Kafka Connect. All sections of this documentation refer to this mode, except for the section specifically dedicated to the Sink Connector.

### Lightstreamer Kafka Connector as a Kafka Connect Sink Connector

In this mode, the Lightstreamer Kafka Connector integrates with the Kafka Connect framework, acting as a sink connector. While this introduces an additional messaging layer, there are scenarios where the standardized deployment provided by Kafka Connect is required. For more details on using the Lightstreamer Kafka Connector as a Kafka Connect sink connector, please refer to this section: [Kafka Connect Lightstreamer Sink Connector](#kafka-connect-lightstreamer-sink-connector).

# QUICK START: Set up in 5 minutes

To efficiently showcase the functionalities of the Lightstreamer Kafka Connector, we have prepared an accessible quickstart application located in the [`examples/quickstart`](/examples/quickstart/) directory. This streamlined application facilitates real-time streaming of data from a Kafka topic directly to a web interface. It leverages a modified version of the [Stock List Demo](https://github.com/Lightstreamer/Lightstreamer-example-StockList-client-javascript?tab=readme-ov-file#basic-stock-list-demo---html-client), specifically adapted to demonstrate Kafka integration. This setup is designed for rapid comprehension, enabling you to swiftly grasp and observe the connector's performance in a real-world scenario.

![Quickstart Diagram](/pictures/quickstart-diagram.png)

The diagram above illustrates how, in this setup, a stream of simulated market events is channeled from Kafka to the web client via the Lightstreamer Kafka Connector.

To provide a complete stack, the app is based on _Docker Compose_. The [Docker Compose file](/examples/quickstart/docker-compose.yml) comprises the following services:

1. _broker_: a Kafka broker, based on the [Docker Image for Apache Kafka](https://kafka.apache.org/documentation/#docker). Please notice that other versions of this quickstart are available in the in the [`examples`](/examples/) directory, specifically targeted to other brokers, including [`Confluent Cloud`](/examples/vendors/confluent/README.md), [`Redpanda Serverless`](/examples/vendors/redpanda/quickstart-redpanda-serverless), [`Redpanda Self-hosted`](/examples/vendors/redpanda/quickstart-redpanda-selfhosted), [`Aiven`](/examples/quickstart-aiven), and more.
2. _kafka-connector_: Lightstreamer Server with the Kafka Connector, based on the [Lightstreamer Kafka Connector Docker image example](/examples/docker/), which also includes a web client mounted on `/lightstreamer/pages/QuickStart`
3. _producer_: a native Kafka Producer, based on the provided [`Dockerfile`](/examples/quickstart-producer/Dockerfile) file from the [`quickstart-producer`](/examples/quickstart-producer/) sample client

## Run

1. Make sure you have Docker, Docker Compose, and a JDK (Java Development Kit) v17 or newer installed on your local machine.
2. From the [`examples/quickstart`](/examples/quickstart/) folder, run the following:

   ```sh
   $ ./start.sh
   ...
    ⠏ Network quickstart_default  Created
    ✔ Container broker            Started
    ✔ Container producer          Started
    ✔ Container kafka-connector   Started
   ...
   Services started. Now you can point your browser to http://localhost:8080/QuickStart to see real-time data.
   ...
   ```

3. Once all containers are ready, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).

4. After a few moments, the user interface starts displaying the real-time stock data.

   ![Demo](/pictures/quickstart.gif)

5. To shutdown Docker Compose and clean up all temporary resources:

   ```sh
   $ ./stop.sh
   ```

# Installation

This section will guide you through the installation of the Kafka Connector to get it up and running in a very short time.

## Requirements

- JDK (Java Development Kit) v17 or newer
- [Lightstreamer Broker](https://lightstreamer.com/download/) (also referred to as _Lightstreamer Server_) v7.4.2 or newer. Follow the installation instructions in the `LS_HOME/GETTING_STARTED.TXT` file included in the downloaded package.
- A running Kafka broker or Kafka cluster

## Deploy

Download the deployment archive `lightstreamer-kafka-connector-<version>.zip` from the [Releases](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/releases/) page. Alternatively, check out this repository and execute the following command from the [`kafka-connector-project`](/kafka-connector-project/) folder:

```sh
$ ./gradlew adapterDistZip
```

which generates the archive file under the `kafka-connector-project/kafka-connector/build/distributions` folder.

Then, unpack it into the `adapters` folder of the Lightstreamer Server installation:

```sh
$ unzip lightstreamer-kafka-connector-<version>.zip -d LS_HOME/adapters
```

Finally, check that the Lightstreamer layout looks like the following:

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

## Configure

Before starting the Kafka Connector, you need to properly configure the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/adapters.xml` file. For convenience, the package comes with a predefined configuration (the same used in the [_Quick Start_](#quick-start-set-up-in-5-minutes) app), which can be customized in all its aspects as per your requirements. Of course, you may add as many different connection configurations as desired to fit your needs.

To quickly complete the installation and verify the successful integration with Kafka, edit the _data_provider_ block `QuickStart` in the file as follows:

- Update the [`bootstrap.servers`](#bootstrapservers) parameter with the connection string of Kafka:

  ```xml
  <param name="bootstrap.servers">kafka.connection.string</param>
  ```


- Configure topic and record mapping.

  To enable a generic Lightstreamer client to receive real-time updates, it needs to subscribe to one or more items. Therefore, the Kafka Connector provides suitable mechanisms to map Kafka topics to Lightstreamer items effectively.

  The `QuickStart` [factory configuration](/kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml#L39) comes with a straightforward mapping defined through the following settings:

  - An item template:
    ```xml
    <param name="item-template.stock">stock-#{index=KEY}</param>
    ```

    which defines the general format name of the items a client must subscribe to to receive updates from the Kafka Connector. The [_extraction expression_](#filtered-record-routing-item-templatetemplate_name) syntax used here - denoted within `#{...}` -  permits the clients to specify filtering values to be compared against the actual contents of a Kafka record, evaluated through [_Extraction Keys_](#data-extraction-language) used to extract each part of a record. In this case, the `KEY` predefined constant extracts the key part of Kafka records.

  - A topic mapping:
    ```xml
    <param name="map.stocks.to">item-template.stock</param>
    ```
    which maps the topic `stocks` to the provided item template.

  This configuration instructs the Kafka Connector to analyze every single event published to the topic `stocks` and check if it matches against any item subscribed by the client as:

  - `stock-[index=1]`: an item with the `index` parameter bound to a record key equal to `1`
  - `stock-[index=2]`: an item with the `index` parameter bound to a record key equal to `2`
  - ...

  The Kafka Connector will then route the event to all matched items.

  In addition, the following section defines how to map the record to the tabular form of Lightstreamer fields, by using the aforementioned _Extraction Keys_. In this case, the `VALUE` predefined constant extracts the value part of Kafka records.

  ```xml
  <param name="field.stock_name">#{VALUE.name}</param>
  <param name="field.last_price">#{VALUE.last_price}</param>
  <param name="field.ask">#{VALUE.ask}</param>
  <param name="field.ask_quantity">#{VALUE.ask_quantity}</param>
  <param name="field.bid">#{VALUE.bid}</param>
  <param name="field.bid_quantity">#{VALUE.bid_quantity}</param>
  <param name="field.pct_change">#{VALUE.pct_change}</param>
  <param name="field.min">#{VALUE.min}</param>
  <param name="field.max">#{VALUE.max}</param>
  <param name="field.ref_price">#{VALUE.ref_price}</param>
  <param name="field.open_price">#{VALUE.open_price}</param>
  <param name="field.item_status">#{VALUE.item_status}</param>
  ```

  This way, the routed event is transformed into a flat structure, which can be forwarded to the clients.

- Optionally, customize the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/log4j.properties` file (the current settings produce the `quickstart.log` file).

You can get more details about all possible settings in the [Configuration](#configuration) section.

### Connection with Confluent Cloud

If your target Kafka cluster is _Confluent Cloud_, you also need to properly configure TLS 1.2 encryption and SASL/PLAIN authentication, as follows:

```xml
<param name="encryption.enable">true</param>
<param name="encryption.protocol">TLSv1.2</param>
<param name="encryption.hostname.verification.enable">true</param>

<param name="authentication.enable">true</param>
<param name="authentication.mechanism">PLAIN</param>
<param name="authentication.username">API.key</param>
<param name="authentication.password">secret</param>
...
```

where you have to replace `API.key` and `secret` with the _API Key_ and _secret_ generated on the _Confluent CLI_ or from the _Confluent Cloud Console_.

### Connection with Redpanda Cloud

If your target Kafka cluster is _Redpanda Cloud_, you also need to properly configure TLS 1.2 encryption and SASL/SCRAM authentication, as follows:

```xml
<param name="encryption.enable">true</param>
<param name="encryption.protocol">TLSv1.2</param>
<param name="encryption.hostname.verification.enable">true</param>

<param name="authentication.enable">true</param>
<param name="authentication.mechanism">SCRAM-SHA-256</param>
<!-- <param name="authentication.mechanism">SCRAM-SHA-512</param> -->
<param name="authentication.username">username</param>
<param name="authentication.password">password</param>
...
```

where you have to replace `username` and `password` with the credentials generated from the _Redpanda Console_.

## Start

### 1. Launch Lightstreamer Server

   From the `LS_HOME/bin/unix-like` directory, run the following:

   ```sh
   $ ./background_start.sh
   ```

### 2. Attach a Lightstreamer Consumer

   The [`kafka-connector-utils`](/kafka-connector-project/kafka-connector-utils) submodule hosts a simple Lightstreamer Java client that can be used to test the consumption of Kafka events from any Kafka topics.

   Before launching the consumer, you first need to build it from the [`kafka-connector-project`](/kafka-connector-project/) folder with the command:

   ```sh
   $ ./gradlew kafka-connector-utils:build
   ```

   which generates the `lightstreamer-kafka-connector-utils-consumer-all-<version>.jar` file under the `kafka-connector-project/kafka-connector-utils/build/libs` folder.

   Then, launch it with:

   ```sh
   $ java -jar kafka-connector-utils/build/libs/lightstreamer-kafka-connector-utils-consumer-all-<version>.jar --address http://localhost:8080 --adapter-set KafkaConnector --data-adapter QuickStart --items stock-[index=1],stock-[index=2],stock-[index=3] --fields stock_name,ask,bid,min,max
   ```

   As you can see, you have to specify a few parameters:

   - `--address`: the Lightstreamer Server address
   - `--adapter-set`: the name of the requested Adapter Set, which triggers Lightstreamer to activate the Kafka Connector deployed into the `adapters` folder
   - `--data-adapter`: the name of the requested Data Adapter, which identifies the selected Kafka connection configuration
   - `--items`: the list of items to subscribe to
   - `--fields`: the list of requested fields for the items

  > [!NOTE]
  > While we've provided examples in JavaScript (suitable for web browsers) and Java (geared towards desktop applications), you are encouraged to utilize any of the [Lightstreamer client SDKs](https://lightstreamer.com/download/#client-sdks) for developing clients in other environments, including iOS, Android, Python, and more.

### 3. Publish the Events

   The [`examples/quickstart-producer`](/examples/quickstart-producer/) folder hosts a simple Kafka producer to publish simulated market events for the _Quick Start_ app.

   Before launching the producer, you first need to build it. Open a new shell from the folder and execute the command:

   ```sh
   $ cd examples/quickstart-producer
   $ ./gradlew build
   ```

   which generates the `quickstart-producer-all.jar` file under the `build/libs` folder.

   Then, launch it with:

   ```sh
   $ java -jar build/libs/quickstart-producer-all.jar --bootstrap-servers <kafka.connection.string> --topic stocks
   ```

   ![producer_video](/pictures/producer.gif)

   #### Publishing with Confluent Cloud

   If your target Kafka cluster is _Confluent Cloud_, you also need to provide a properties file that includes encryption and authentication settings, as follows:

   ```java
   security.protocol=SASL_SSL
   sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<API.key>" password="<API.secret>";
   sasl.mechanism=PLAIN
   ...
   ```

   where you have to replace `<API.key>` and `<API.secret>` with the API key and API secret generated on the _Confluent CLI_ or from the _Confluent Cloud Console_.

   ```sh
   $ java -jar build/libs/quickstart-producer-all.jar --bootstrap-servers <kafka.connection.string> --topic stocks --config-file <path/to/config/file>
   ```

   #### Publishing with Redpanda Cloud

   If your target Kafka cluster is _Redpanda Cloud_, you also need to provide a properties file that includes encryption and authentication settings, as follows:

   ```java
   security.protocol=SASL_SSL
   sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="username" password="password";
   sasl.mechanism=SCRAM-SHA-256
   #sasl.mechanism=SCRAM-SHA-512
   ...
   ```

   where you have to replace `username` and `password` with the credentials generated from the _Redpanda Console_, and specify the configured SASL mechanism (`SCRAM-SHA-256` or `SCRAM-SHA-512`).

   ```sh
   $ java -jar build/libs/quickstart-producer-all.jar --bootstrap-servers <kafka.connection.string> --topic stocks --config-file <path/to/config/file>
   ```

### 4. Check the Consumed Events

   After starting the publisher, you should immediately see the real-time updates flowing from the consumer shell:

   ![consumer_video](/pictures/consumer.gif)

# Configuration

As already anticipated, the Kafka Connector is a Lightstreamer Adapter Set, which means it is made up of a Metadata Adapter and one or more Data Adapters, whose settings are defined in the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/adapters.xml` file.

The following sections will guide you through the configuration details.

## Global Settings

### `adapter_conf['id']` - _Kafka Connector Identifier_

  _Mandatory_. The `id` attribute of the `adapters_conf` root tag defines the _Kafka Connector Identifier_, which will be used by the Clients to request this Adapter Set while setting up the connection to a Lightstreamer Server through a _LightstreamerClient_ object.

  The factory value is set to `KafkaConnector` for convenience, but you are free to change it as per your requirements.

  Example:

  ```xml
  <adapters_conf id="KafkaConnector">
  ```

### `adapter_class`

_Mandatory_. The `adapter_class` tag, specified inside the _metadata_provider_ block, defines the Java class name of the Metadata Adapter.

The factory value is set to `com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter`, which implements the internal business of the Kafka Connector.

It is possible to provide a custom implementation by extending this class: just package your new class in a jar file and deploy it along with all required dependencies into the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/lib` folder.

See the [Customizing the Kafka Connector Metadata Class](#customizing-the-kafka-connector-metadata-adapter-class) section for more details.

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

### `logging.configuration.path`

_Mandatory_. The path of the [reload4j](https://reload4j.qos.ch/) configuration file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector-<version>`).

The parameter is specified inside the _metadata_provider_ block.

The factory value points to the predefined file `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/log4g.properties`.

Example:

```xml
...
<metadata_provider>
    ...
    <param name="logging.configuration.path">log4j.properties</param>
    ...
</metadata_provider>
...
```

## Connection Settings

The Kafka Connector allows the configuration of separate independent connections to different Kafka brokers/clusters.

Every single connection is configured via the definition of its own Data Adapter through the _data_provider_ block. At least one connection must be provided.

Since the Kafka Connector manages the physical connection to Kafka by wrapping an internal Kafka Consumer, several configuration settings in the Data Adapter are identical to those required by the usual Kafka Consumer configuration.

### General Parameters

#### `data_provider['name']` - _Kafka Connection Name_

_Optional_. The `name` attribute of the `data_provider` tag defines _Kafka Connection Name_, which will be used by the Clients to request real-time data from this specific Kafka connection through a _Subscription_ object.

Furthermore, the name is also used to group all logging messages belonging to the same connection.

> [!TIP]
> For every Data Adapter connection, add a new logger and its relative file appender to `log4j.properties`, so that you can log to dedicated files all the interactions pertinent to the connection with the Kafka cluster and the message retrieval operations, along with their routing to the subscribed items.
> For example, the factory [logging configuration](/kafka-connector-project/kafka-connector/src/adapter/dist/log4j.properties#L23) provides the logger `QuickStart` to print every log messages relative to the `QuickStart` connection:
> ```java
> ...
> # QuickStart logger
> log4j.logger.QuickStart=INFO, QuickStartFile
> log4j.appender.QuickStartFile=org.apache.log4j.RollingFileAppender
> log4j.appender.QuickStartFile.layout=org.apache.log4j.PatternLayout
> log4j.appender.QuickStartFile.layout.ConversionPattern=[%d] [%-10c{1}] %-5p %m%n
> log4j.appender.QuickStartFile.File=../../logs/quickstart.log
> ```

Example:

```xml
<data_provider name="BrokerConnection">
```

Default value: `DEFAULT`, but only one `DEFAULT` configuration is permitted.

#### `adapter_class`

_Mandatory_. The `adapter_class` tag defines the Java class name of the Data Adapter. DO NOT EDIT IT!.

Factory value: `com.lightstreamer.kafka.adapters.KafkaConnectorDataAdapter`.

#### `enable`

_Optional_. Enable this connection configuration. Can be one of the following:
- `true`
- `false`

If disabled, Lightstreamer Server will automatically deny every subscription made to this connection.

Default value: `true`.

Example:

```xml
<param name="enable">false</param>
```

#### `bootstrap.servers`

_Mandatory_. The Kafka Cluster bootstrap server endpoint expressed as the list of host/port pairs used to establish the initial connection.

The parameter sets the value of the [`bootstrap.servers`](https://kafka.apache.org/documentation/#consumerconfigs_bootstrap.servers) key to configure the internal Kafka Consumer.

Example:

```xml
<param name="bootstrap.servers">broker:29092,broker:29093</param>
```

#### `group.id`

_Optional_. The name of the consumer group this connection belongs to.

The parameter sets the value for the [`group.id`](https://kafka.apache.org/documentation/#consumerconfigs_group.id) key to configure the internal Kafka Consumer.

Default value: _Kafka Connector Identifier_ + _Connection Name_ + _Randomly generated suffix_.

```xml
<param name="group.id">kafka-connector-group</param>
```

### Encryption Parameters

A TCP secure connection to Kafka is configured through parameters with the prefix `encryption`.

#### `encryption.enable`

_Optional_. Enable encryption of this connection. Can be one of the following:
- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="encryption.enable">true</param>
```

#### `encryption.protocol`

_Optional_. The SSL protocol to be used. Can be one of the following:
- `TLSv1.2`
- `TLSv1.3`

Default value: `TLSv1.3` when running on Java 11 or newer, `TLSv1.2` otherwise.

Example:

```xml
<param name="encryption.protocol">TLSv1.2</param>
```

#### `encryption.enabled.protocols`

_Optional_. The list of enabled secure communication protocols.

Default value: `TLSv1.2,TLSv1.3` when running on Java 11 or newer, `TLSv1.2` otherwise.

Example:

```xml
<param name="encryption.enabled.protocols">TLSv1.3</param>
```

#### `encryption.cipher.suites`

_Optional_. The list of enabled secure cipher suites.

Default value: all the available cipher suites in the running JVM.

Example:

```xml
<param name="encryption.cipher.suites">TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA</param>
```

#### `encryption.hostname.verification.enable`

_Optional_. Enable hostname verification. Can be one of the following:
- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="encryption.hostname.verification.enable">true</param>
```

#### `encryption.truststore.path`

_Optional_. The path of the trust store file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector-<version>`).

The trust store is used to validate the certificates provided by the Kafka brokers.

Example:

```xml
<param name="encryption.truststore.path">secrets/kafka-connector.truststore.jks</param>
```

#### `encryption.truststore.password `

_Optional_. The password of the trust store.

If not set, checking the integrity of the trust store file configured will not be possible.

Example:

```xml
<param name="encryption.truststore.password">kafka-connector-truststore-password</param>
```

#### `encryption.keystore.enable`

_Optional_. Enable a key store. Can be one of the following:
- `true`
- `false`

A key store is required if the mutual TLS is enabled on Kafka.

If enabled, the following parameters configure the key store settings:

- `encryption.keystore.path`
- `encryption.keystore.password`
- `encryption.keystore.key.password`

Default value: `false`.

Example:

```xml
<param name="encryption.keystore.enable">true</param>
```

#### `encryption.keystore.path`

_Mandatory if [key store](#encryptionkeystoreenable) is enabled_. The path of the key store file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector-<version>`).

Example:

```xml
<param name="encryption.keystore.path">secrets/kafka-connector.keystore.jks</param>
```

#### `encryption.keystore.password`

_Optional_. The password of the key store.

If not set, checking the integrity of the key store file configured will not be possible.

Example:

```xml
<param name="encryption.keystore.password">keystore-password</param>
```

#### `encryption.keystore.key.password`

_Optional_. The password of the private key in the key store file.

Example:

```xml
<param name="encryption.keystore.key.password">kafka-connector-private-key-password</param>
```

#### Quick Start SSL Example

Check out the [adapters.xml](/examples/quickstart-ssl/adapters.xml#L17) file of the [_Quick Start SSL_](/examples/quickstart-ssl/) app, where you can find an example of encryption configuration.

### Broker Authentication Parameters

Broker authentication is configured through parameters with the prefix `authentication`.

#### `authentication.enable`

_Optional_. Enable the authentication of this connection against the Kafka Cluster. Can be one of the following:
- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="authentication.enable">true</param>
```

#### `authentication.mechanism`

_Mandatory if [authentication](#authenticationenable) is enabled_. The SASL mechanism type. The Kafka Connector accepts the following authentication mechanisms:

- `PLAIN` (the default value)
- `SCRAM-SHA-256`
- `SCRAM-SHA-512`
- `GSSAPI`
- `AWS_MSK_IAM`

In the case of `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512` mechanisms, the credentials must be configured through the following mandatory parameter:

- `authentication.username`: the username
- `authentication.password`: the password

##### `PLAIN`

Example:

```xml
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">PLAIN</param>
<param name="authentication.username">authorized-kafka-user</param>
<param name="authentication.password">authorized-kafka-user-password</param>
```

##### `SCRAM-SHA-256`

Example:

```xml
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">SCRAM-SHA-256</param>
<param name="authentication.username">authorized-kafka-usee</param>
<param name="authentication.password">authorized-kafka-user-password</param>
```

##### `SCRAM-SHA-512`

Example:

```xml
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">SCRAM-SHA-512</param>
<param name="authentication.username">authorized-kafka-username</param>
<param name="authentication.password">authorized-kafka-username-password</param>
```

##### `GSSAPI`

When this mechanism is specified, you can configure the following authentication parameters:

- `authentication.gssapi.key.tab.enable`

  _Optional_. Enable the use of a keytab. Can be one of the following:
  - `true`
  - `false`

  Default value: `false`.

- `authentication.gssapi.key.tab.path`

  _Mandatory if keytab is enabled_. The path to the keytab file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector-<version>`).

- `authentication.gssapi.store.key.enable`

  _Optional_. Enable storage of the principal key. Can be one of the following:
  - `true`
  - `false`

  Default value: `false`.

- `authentication.gssapi.kerberos.service.name`

  _Mandatory_. The name of the Kerberos service.

- `authentication.gssapi.principal`

  _Mandatory if ticket cache is disabled_. The name of the principal to be used.

- `authentication.gssapi.ticket.cache.enable`

  _Optional_. Enable the use of a ticket cache. Can be one of the following:
  - `true`
  - `false`

  Default value: `false`.

Example:

```xml
...
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">GSSAPI</param>
<param name="authentication.gssapi.key.tab.enable">true</param>
<param name="authentication.gssapi.key.tab.path">gssapi/kafka-connector.keytab</param>
<param name="authentication.gssapi.store.key.enable">true</param>
<param name="authentication.gssapi.kerberos.service.name">kafka</param>
<param name="authentication.gssapi.principal">kafka-connector-1@LIGHTSTREAMER.COM</param>
...
```

Example of configuration with the use of a ticket cache:

```xml
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">GSSAPI</param>
<param name="authentication.gssapi.kerberos.service.name">kafka</param>
<param name="authentication.gssapi.ticket.cache.enable">true</param>
```

##### `AWS_MSK_IAM`

The `AWS_MSK_IAM` authentication mechanism enables access to _Amazon Managed Streaming for Apache Kafka (MSK)_ clusters through [IAM access control](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html).

When this mechanism is specified, you can configure the following authentication parameters:

- `iam.credential.profile.name`

  _Optional_. The name of the AWS credential profile to use for authentication. These profiles are defined in the [AWS shared credentials file](https://docs.aws.amazon.com/sdkref/latest/guide/file-location.html).

  Example:

  ```xml
  <param name="iam.credential.profile.name">msk_client<param>
  ```

- `iam.role.arn`

  _Optional_. The Amazon Resource Name (ARN) of the IAM role that the Kafka Connector should assume for authentication with MSK. Use this when you want the connector to assume a specific role with temporary credentials.

  Example:
  
  ```xml
  <param name="iam.role.arn">arn:aws:iam::123456789012:role/msk_client_role<param>
  ```

- `iam.role.session.name`

   _Optional_ but only effective when `iam.role.arn` is set. Specifies a custom session name for the assumed role.
  
  Example:

  ```xml
  <param name="iam.role.session.name">consumer<param>
  ```

- `iam.sts.region`

  _Optional_ but only effective if `iam.role.arn` is set. Specifies the AWS region of the STS endpoint to use when assuming the IAM role.

  Example:

  ```xml
  <param name="iam.sts.region">us-west-1<param>
  ```

> [!IMPORTANT]
> **Authentication Precedence**: If both methods are configured, the `iam.credential.profile.name` parameter takes precedence over `iam.role.arn`. If neither parameter is provided, the Kafka Connector falls back to the [AWS SDK default credential provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html).

#### Quick Start Confluent Cloud Example

Check out the [adapters.xml](/kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml#L541) file of the [_Quick Start Confluent Cloud_](/examples/vendors/confluent/quickstart-confluent/) app, where you can find an example of an authentication configuration that uses SASL/PLAIN.

#### Quick Start with Redpanda Serverless Example

Check out the [adapters.xml](/examples/vendors/redpanda/quickstart-redpanda-serverless/adapters.xml#L22) file of the [_Quick Start with Redpanda Serverless_](/examples/vendors/redpanda/quickstart-redpanda-serverless/) app, where you can find an example of an authentication configuration that uses SASL/SCRAM.

#### Quick Start with MSK Example

Check out the [adapters.xml](/examples/vendors/aws/quickstart-msk/adapters.xml#L21) file of the [_Quick Start with MSK_](/examples/vendors/aws/quickstart-msk/) app, where you can find an example of an authentication configuration that uses AWS_MSK_IAM.

### Record Evaluation

The Kafka Connector can deserialize Kafka records from the following formats:

- _Apache Avro_
- _JSON_
- _Protobuf_
- _String_
- _Integer_
- _Float_

and other scalar types (see [the complete list](#recordkeyevaluatortype-and-recordvalueevaluatortype)).

In particular, the Kafka Connector supports message validation for _Avro_, _JSON_, and _Protobuf_ which can be specified through:

- Local schema files (_Avro_ and _JSON_ only): Use this option when you have predefined schemas stored locally and do not require a centralized schema management system.
- The _Confluent Schema Registry_: Opt for this when you need a centralized repository to manage and validate schemas across multiple applications and environments.

The Kafka Connector enables the independent deserialization of keys and values, allowing them to have different formats. Additionally:

- Message validation against the Confluent Schema Registry can be enabled separately for the key and value (through [`record.key.evaluator.schema.registry.enable` and `record.value.evaluator.schema.registry.enable`](#recordkeyevaluatorschemaregistryenable-and-recordvalueevaluatorschemaregistryenable))
- Message validation against local schema files must be specified separately for the key and the value (through [`record.key.evaluator.schema.path` and `record.value.evaluator.schema.path`](#recordkeyevaluatorschemapath-and-recordvalueevaluatorschemapath))

> [!IMPORTANT]
> When using Avro or Protobuf formats, schema validation is mandatory:
> - For Protobuf: The Confluent Schema Registry must be enabled as the only validation option.
> - For Avro: You can either enable the Confluent Schema Registry or provide local schema files.

#### Support for Key Value Pairs (KVP)

In addition to the above formats, the Kafka Connector also supports the _Key Value Pairs_ (KVP) format. This format allows Kafka records to be represented as a collection of key-value pairs, making it particularly useful for structured data where each key is associated with a specific value.

The Kafka Connector provides flexible configuration options for parsing and extracting data from KVP-formatted records, enabling seamless mapping to Lightstreamer fields. Key-value pairs can be separated by custom delimiters for both the pairs themselves and the key-value separator, ensuring compatibility with diverse data structures. For example:

- A record with the format `key1=value1;key2=value2` uses `=` as the key-value separator and `;` as the pairs separator.
- These separators can be customized using the parameters [`record.key/value.evaluator.kvp.key-value.separator`](#recordkeyevaluatorkvpkey-valueseparator-and-recordvalueevaluatorkvpkey-valueseparator) and [`record.key/value.evaluator.kvp.pairs.separator`](#recordkeyevaluatorkvppairsseparator-and-recordvalueevaluatorkvppairsseparator).

This support for KVP adds to the versatility of the Kafka Connector, allowing it to handle a wide range of data formats efficiently.

#### `record.consume.from`

_Optional_. Specifies where to start consuming events from:

- `LATEST`: start consuming events from the end of the topic partition
- `EARLIEST`: start consuming events from the beginning of the topic partition

The parameter sets the value of the [`auto.offset.reset`](https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset) key to configure the internal Kafka Consumer.

Default value: `LATEST`.

Example:

```xml
<param name="record.consume.from">EARLIEST</param>
```

#### `record.consume.with.num.threads`

_Optional_. The number of threads to be used for concurrent processing of the incoming deserialized records. If set to `-1`, the number of threads will be automatically determined based on the number of available CPU cores.

Default value: `1`.

> [!CAUTION]
> Concurrent processing is not compatible with _log compaction_. When log compaction is enabled in Kafka (which retains only the latest value per key), using multiple processing threads can lead to metadata bloat. This occurs because the offset tracking mechanism accumulates metadata for all processed messages, including those that may later be compacted away. For reliable operation with compacted topics, use a single processing thread (`record.consume.with.num.threads` set to `1`).

Example:

```xml
<param name="record.consume.with.num.threads">4</param>
```

#### `record.consume.with.order.strategy`

_Optional but only effective when [`record.consume.with.num.threads`](#recordconsumewithnumthreads) is set to a value greater than `1` (which includes the default value)_. The order strategy to be used for concurrent processing of the incoming deserialized records. Can be one of the following:

- `ORDER_BY_PARTITION`: maintain the order of records within each partition.

   If you have multiple partitions, records from different partitions can be processed concurrently by different threads, but the order of records from a single partition will always be preserved. This is the default and generally a good balance between performance and order.

- `ORDER_BY_KEY`: maintain the order among the records sharing the same key.

  Different keys can be processed concurrently by different threads. So, while all records with key "A" are processed in order, and all records with key "B" are processed in order, the processing of "A" and "B" records can happen concurrently and interleaved in time. There's no guaranteed order between records of different keys.

- `UNORDERED`: provide no ordering guarantees.

  Records from any partition and with any key can be processed by any thread at any time. This offers the highest throughput when an high number of subscriptions is involved, but the order in which records are delivered to Lightstreamer clients might not match the order they were written to Kafka. This is suitable for use cases where message order is not important.

Default value: `ORDER_BY_PARTITION`.

Example:

```xml
<param name="record.consume.with.order.strategy">ORDER_BY_KEY</param>
```

#### `record.key.evaluator.type` and `record.value.evaluator.type`

_Optional_. The format to be used to deserialize respectively the key and value of a Kafka record. Can be one of the following:

- `AVRO`
- `JSON`
- `PROTOBUF`
- `KVP`
- `STRING`
- `INTEGER`
- `BOOLEAN`
- `BYTE_ARRAY`
- `BYTE_BUFFER`
- `BYTES`
- `DOUBLE`
- `FLOAT`
- `LONG`
- `SHORT`
- `UUID`

Default value: `STRING`.

Examples:

```xml
<param name="record.key.evaluator.type">INTEGER</param>
<param name="record.value.evaluator.type">JSON</param>
```

#### `record.key.evaluator.schema.path` and `record.value.evaluator.schema.path`

_Mandatory if [evaluator type](#recordkeyevaluatortype-and-recordvalueevaluatortype) is set to `AVRO` and the [Confluent Schema Registry](#recordkeyevaluatorschemaregistryenable-and-recordvalueevaluatorschemaregistryenable) is disabled_. The path of the local schema file relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector-<version>`) for message validation respectively of the key and the value.

Examples:

```xml
<param name="record.key.evaluator.schema.path">schema/record_key.avsc</param>
<param name="record.value.evaluator.schema.path">schemas/record_value.avsc</param>
```

#### `record.key.evaluator.schema.registry.enable` and `record.value.evaluator.schema.registry.enable`

_Mandatory when the [evaluator type](#recordkeyevaluatortype-and-recordvalueevaluatortype) is set to `AVRO` and no [local schema paths](#recordkeyevaluatorschemapath-and-recordvalueevaluatorschemapath) are provided, or when the [evaluator type](#recordkeyevaluatortype-and-recordvalueevaluatortype) is set to `PROTOBUF`_. Enable the use of the [Confluent Schema Registry](#schema-registry) for validation respectively of the key and the value. Can be one of the following:
- `true`
- `false`

Default value: `false`.

Examples:

```xml
<param name="record.key.evaluator.schema.registry.enable">true</param>
<param name="record.value.evaluator.schema.registry.enable">true</param>
```

#### `record.key.evaluator.kvp.key-value.separator` and `record.value.evaluator.kvp.key-value.separator`

_Optional but only effective when `record.key/value.evaluator.type` is set to `KVP`_.
Specifies the symbol used to separate keys from values in a record key (or record value) serialized in the KVP format.
        
For example, in the following record value:

```
key1=value1;key2=value2
```

the key-value separator is the `=` symbol.

Default value: `=`.

```xml
<param name="record.key.evaluator.kvp.key-value.separator">-</param>
<param name="record.value.evaluator.kvp.key-value.separator">@</param>
```

#### `record.key.evaluator.kvp.pairs.separator` and `record.value.evaluator.kvp.pairs.separator`

_Optional_ but only effective when `record.key/value.evaluator.type` is set to `KVP`.
Specifies the symbol used to separate multiple key-value pairs in a record key (or record value) serialized in the KVP format.

For example, in the following record value:

```
key1=value1;key2=value2
```

the pairs separator is the `;` symbol, which separates `key1=value1` and `key2=value2`.
        
Default value: `,`.

Examples:

```xml
<param name="record.key.evaluator.kvp.pairs.separator">;</param>
<param name="record.value.evaluator.kvp.pairs.separator">;</param>
```

#### `record.extraction.error.strategy`

_Optional_. The error handling strategy to be used if an error occurs while [extracting data](#data-extraction-language) from incoming deserialized records. Can be one of the following:

- `IGNORE_AND_CONTINUE`: ignore the error and continue to process the next record
- `FORCE_UNSUBSCRIPTION`: stop processing records and force unsubscription of the items requested by all the clients subscribed to this connection (see the [Client Side Error Handling](#client-side-error-handling) section)

Default value: `IGNORE_AND_CONTINUE`.

Example:

```xml
<param name="record.extraction.error.strategy">FORCE_UNSUBSCRIPTION</param>
```

### Topic Mapping

The Kafka Connector allows the configuration of several routing and mapping strategies, thus enabling the convey of Kafka events streams to a potentially huge amount of devices connected to Lightstreamer with great flexibility.

The _Data Extraction Language_ is the _ad hoc_ tool provided for in-depth analysis of Kafka records to extract data that can be used for the following purposes:
- Mapping records to Lightstreamer fields
- Filtering routing to the designated Lightstreamer items

#### Data Extraction Language

To write an extraction expression, the _Data Extraction Language_ provides a pretty minimal syntax with the following basic rules:

- Expressions must be enclosed within `#{...}`
- Expressions use _Extraction Keys_, a set of predefined constants that reference specific parts of the record structure:

  - `#{KEY}`: the key
  - `#{VALUE}`: the value
  - `#{TOPIC}`: the topic
  - `#{TIMESTAMP}`: the timestamp
  - `#{PARTITION}`: the partition
  - `#{OFFSET}`: the offset

- Expressions use the _dot notation_ to access attributes or fields of record keys and record values serialized in JSON or Avro formats:

  ```js
  KEY.attribute1Name.attribute2Name...
  VALUE.attribute1Name.attribute2Name...
  ```

 > [!IMPORTANT]
 > Currently, it is required that the top-level element of either a record key or record value is:
 > - An [Object](https://www.json.org/json-en.html), in the case of JSON format
 > - A [Record](https://avro.apache.org/docs/1.11.1/specification/#schema-record), in the case of Avro format
 >
 > Such a constraint may be removed in a future version of the Kafka Connector.

- Expressions use the _square notation_ to access:

  - Indexed attributes:

    ```js
    KEY.attribute1Name[i].attribute2Name...
    VALUE.attribute1Name[i].attribute2Name...
    ```
    where `i` is a 0-indexed value.

  - Key-based attributes:

    ```js
    KEY.attribute1Name['keyName'].attribute2Name...
    VALUE.attribute1Name['keyName'].attribute2Name...
    ```
    where `keyName` is a string value.

 > [!TIP]
 > For JSON format, accessing a child attribute using either dot notation or square bracket notation is equivalent:
 >
 > ```js
 > VALUE.myProperty.myChild.childProperty
 > VALUE.myProperty['myChild'].childProperty
 > ```

- Expressions must evaluate to a _scalar_ value

  In case of non-scalar value, an error will be thrown during the extraction process and handled as per the [configured strategy](#recordextractionerrorstrategy).

#### Record Routing (`map.TOPIC_NAME.to`)

To configure a simple routing of Kafka event streams to Lightstreamer items, use at least one `map.TOPIC_NAME.TO` parameter. The general format is:

```xml
<param name="map.TOPIC_NAME.to">item1,item2,itemN,...</param>
```

which defines the mapping between the source Kafka topic (`TOPIC_NAME`) and the target items (`item1`, `item2`, `itemN`, etc.).

This configuration enables the implementation of various routing scenarios, as shown by the following examples:

- _One-to-one_

  ```xml
  <param name="map.sample-topic.to">sample-item</param>
  ```

  ![one-to-one](/pictures/one-to-one.png)

  This is the most straightforward scenario one may think of: every record published to the Kafka topic `sample-topic` will simply be routed to the Lightstreamer item `sample-item`. Therefore, messages will be immediately broadcasted as real-time updates to all clients subscribed to such an item.

- _Many-to-one_

  ```xml
  <param name="map.sample-topic1.to">sample-item</param>
  <param name="map.sample-topic2.to">sample-item</param>
  <param name="map.sample-topic3.to">sample-item</param>
  ```

  ![many-to-one](/pictures/many-to-one.png)

  With this scenario, it is possible to broadcast to all clients subscribed to a single item (`sample-item`) every message published to different topics (`sample-topic1`, `sample-topic2`, `sample-topic3`).

- _One-to-many_

  The one-to-many scenario is also supported, though it's often unnecessary. Lightstreamer already provides full control over individual items, such as differentiating access authorization for various users or subscribing with different maximum update frequencies, without requiring data replication across multiple items.

  ```xml
  <param name="map.sample-topic.to">sample-item1,sample-item2,sample-item3</param>
  ```

  Every record published to the Kafka topic `sample-topic` will be routed to the Lightstreamer items `sample-item1`, `sample-item2`, and `sample-item3`.

##### Enable Regular Expression (`map.regex.enable`)

_Optional_. Enable the `TOPIC_NAME` part of the [`map.TOPIC_NAME.to`](#record-routing-maptopic_nameto) parameter to be treated as a regular expression rather than of a literal topic name.
This allows for more flexible routing, where messages from multiple topics matching a specific pattern can be directed to the same Lightstreamer item(s) or item template(s).
Can be one of the following:
- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="map.topic_\d+.to">item</param>
<param name="map.regex.enable">true</param>
```

#### Record Mapping (`field.FIELD_NAME`)

To forward real-time updates to the Lightstreamer clients, a Kafka record must be mapped to Lightstreamer fields, which define the _schema_ of any Lightstreamer item.

![record-mapping](/pictures/record-fields-mapping.png)

To configure the mapping, you define the set of all subscribable fields through parameters with the prefix `field.`:

```xml
<param name="field.fieldName1">extractionExpression1</param>
<param name="field.fieldName2">extractionExpression2<param>
...
<param name="field.fieldNameN">extractionExpressionN<param>
...
```

The configuration specifies that the field `fieldNameX` will contain the value extracted from the deserialized Kafka record through the `extractionExpressionX`, written using the [_Data Extraction Language_](#data-extraction-language). This approach makes it possible to transform a Kafka record of any complexity to the flat structure required by Lightstreamer.

The `QuickStart` [factory configuration](/kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml#L403) shows a basic example, where a simple _direct_ mapping has been defined between every attribute of the JSON record value and a Lightstreamer field with the corresponding name. Of course, thanks to the _Data Extraction Language_, more complex mapping can be employed.

```xml
...
<param name="field.timestamp">#{VALUE.timestamp}</param>
<param name="field.time">#{VALUE.time}</param>
<param name="field.stock_name">#{VALUE.name}</param>
<param name="field.last_price">#{VALUE.last_price}</param>
<param name="field.ask">#{VALUE.ask}</param>
<param name="field.ask_quantity">#{VALUE.ask_quantity}</param>
<param name="field.bid">#{VALUE.bid}</param>
<param name="field.bid_quantity">#{VALUE.bid_quantity}</param>
<param name="field.pct_change">#{VALUE.pct_change}</param>
<param name="field.min">#{VALUE.min}</param>
<param name="field.max">#{VALUE.max}</param>
<param name="field.ref_price">#{VALUE.ref_price}</param>
<param name="field.open_price">#{VALUE.open_price}</param>
<param name="field.item_status">#{VALUE.item_status}</param>
..
```

##### Skip Failed Mapping (`fields.skip.failed.mapping.enable`)

_Optional_. Normally, if a field mapping fails during the extraction from the Kafka record because of an issue with the data, it leads to the entire record being discarded or even cause the subscription to be terminated, depending on the [`record.extraction.error.strategy`](#recordextractionerrorstrategy) setting. By enabling this parameter, the connector becomes more resilient to such errors. If a field mapping fails, that specific field's value will simply be omitted from the update sent to Lightstreamer clients, while other successfully mapped fields from the same record will still be delivered. This allows for partial updates even in the presence of data inconsistencies or transient extraction issues.

Can be one of the following:
- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="fields.skip.failed.mapping.enable">true</param>
```

##### Map Non-Scalar Values (`fields.map.non.scalar.values`)

_Optional_. Enabling this parameter allows mapping of non-scalar values to Lightstreamer fields. 
This means that complex data structures from Kafka records can be mapped directly to Lightstreamer fields without requiring them to be flattened into scalar values.
This can be useful when dealing with nested JSON/Avro objects or other complex data types.

In the following example:

```xml
<param name="field.structured">#{VALUE.complexAttribute}</param>
```

the value of `complexAttribute` will be mapped as generic text (e.g. JSON string) to the `structured` Lightstreamer field, preserving its structure and allowing clients to parse and use the data as needed.

Can be one of the following:
- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="fields.map.non.scalar.values">true</param>
```

##### Evaluate As Command (`fields.evaluate.as.command.enable`)

_Optional_. Enables support for the _COMMAND_ mode. In _COMMAND_ mode, a single Lightstreamer item is typically managed as a dynamic list or table, which can be modified through the following operations:

- **`ADD`**: Insert a new element into the item.
- **`UPDATE`**: Modify an existing element of the item.
- **`DELETE`**: Remove an existing element from the item.

To utilize _COMMAND_ mode, the Lightstreamer Broker requires the following mandatory field names in the item's schema:

- **`key`**: Identifies the unique key for each element in the list generated from the item.
- **`command`**: Specifies the operation (`ADD`, `UPDATE`, `DELETE`) to be performed on the item.

A Kafka record must be structured to allow the Kafka Connector to map the values for the `key` and `command` fields. For example:

```xml
<param name="fields.evaluate.as.command.enable">true</param>
<param name="field.key">#{KEY}</param>
<param name="field.command">#{VALUE.command}</param>
...
```

> [!TIP]
> The `key` and `command` fields can be mapped from any part of the Kafka record.

Additionally, the Lightstreamer Kafka Connector supports specialized snapshot management tailored for _COMMAND_ mode. This involves sending Kafka records where the `key` and `command` mappings are interpreted as special events rather than regular updates. Specifically:

- `key` must contain the special value `snapshot`.
- `command` can contain:
  - **`CS`**: Clears the current snapshot. This event is always communicated to all clients subscribed to the item.
  - **`EOS`**: Marks the end of the snapshot. Communication to clients depends on the internal state reconstructed by the Lightstreamer Broker. If the broker has already determined that the snapshot has ended, the event may be ignored.

For a complete example of configuring _COMMAND_ mode, refer to the [examples/AirportDemo](examples/airport-demo/) folder.

The parameter can be one of the following:
- `true`
- `false`

Default value : `false`.

#### Filtered Record Routing (`item-template.TEMPLATE_NAME`)

Besides mapping topics to statically predefined items, the Kafka Connector allows you to configure the _item templates_,
which specify the rules needed to decide if a message can be forwarded to the items specified by the clients, thus enabling a _filtered routing_.
The item template leverages the [_Data Extraction Language_](#data-extraction-language) to extract data from Kafka records and match them against the _parameterized_ subscribed items.

![filtered-routing](/pictures/filtered-routing.png)

To configure an item template, use the `item-template.TEMPLATE_NAME` parameter:

```xml
<param name="item-template.TEMPLATE_NAME">ITEM_PREFIX-EXPRESSIONS</param>
```

Then, map one (or more) topic to the template by referencing it in the `map.TOPIC_NAME.to` parameter:

```xml
<param name="map.TOPIC_NAME.to">item-template.TEMPLATE_NAME</param>
```

> [!TIP]
> It is allowed to mix references to simple item names and item templates in the same topic mapping configuration:
>
> ```xml
> <param name="map.sample-topic.to">item-template.template1,item1,item2</param>
> ```

The item template is made of:
- `ITEM_PREFIX`: the prefix of the item name
- `EXPRESSIONS`: a sequence of _extraction expressions_, which define filtering rules specified as:

  ```js
  #{paramName1=<extractionExpression1>,paramName2=<extractionExpression2>,...}
  ```

  where `paramNameX` is a _bind parameter_ to be specified by the clients and whose actual value will be extracted from the deserialized Kafka record by evaluating the `<extractionExpressionX>` expression (written using the _Data Extraction Language_).

To activate the filtered routing, the Lightstreamer clients must subscribe to a parameterized item that specifies a filtering value for every bind parameter defined in the template:

```js
ITEM_PREFIX-[paramName1=filterValue_1,paramName2=filerValue_2,...]
```

Upon consuming a message, the Kafka Connector _expands_ every item template addressed by the record topic by evaluating each extraction expression and binding the extracted value to the associated parameter. The expanded template will result as:

```js
ITEM_PREFIX-[paramName1=extractedValue_1,paramName2=extractedValue_2,...]
```

Finally, the message will be mapped and routed only in case the subscribed item completely matches the expanded template or, more formally, the following is true:

`filterValue_X == extractValue_X for every paramName_X`

##### Example

Consider the following configuration:

```xml
<param name="item-template.by-name">user-#{firstName=VALUE.name,lastName=VALUE.surname}</param>
<param name="item-template.by-age">user-#{age=VALUE.age}</param>
<param name="map.user.to">item-template.by-name,item-template.by-age</param>
```

which specifies how to route records published from the topic `user` to the item templates defined to extract some personal data.

Let's suppose we have three different Lightstreamer clients:

1. _Client A_ subscribes to the following parameterized items:
   - _SA1_ `user-[firstName=James,lastName=Kirk]` for receiving real-time updates relative to the user `James Kirk`.
   - _SA2_ `user-[age=45]` for receiving real-time updates relative to any 45 year-old user.
2. _Client B_ subscribes to the parameterized item _SB1_ `user-[firstName=Montgomery,lastName=Scotty]` for receiving real-time updates relative to the user `Montgomery Scotty`.
3. _Client C_ subscribes to the parameterized item _SC1_ `user-[age=37]` for receiving real-time updates relative to any 37 year-old user.

Now, let's see how filtered routing works for the following incoming Kafka records published to the topic `user`:

- Record 1:
  ```js
  {
    ...
    "name": "James",
    "surname": "Kirk",
    "age": 37,
    ...
  }
  ```

  | Template  | Expansion                              | Matched Subscribed Item | Routed to Client |
  | ----------| -------------------------------------- | ----------------------- | -----------------|
  | `by-name` | `user-[firstName=James,lastName=Kirk]` | _SA1_                   | _Client A_       |
  | `by-age`  | `user-[age=37]`                        | _SC1_                   | _Client C_       |


- Record 2:
  ```js
  {
    ...
    "name": "Montgomery",
    "surname": "Scotty",
    "age": 45
    ...
  }
  ```

  | Template  | Expansion                                     | Matched Subscribed Item | Routed to Client |
  | --------- | --------------------------------------------- | ----------------------- | -----------------|
  | `by-name` | `user-[firstName=Montgomery,lastName=Scotty]` | _SB1_                   | _Client B_       |
  | `by-age`  | `user-[age=45]`                               | _SA2_                   | _Client A_       |

- Record 3:
  ```js
  {
    ...
    "name": "Nyota",
    "surname": "Uhura",
    "age": 37,
    ...
  }
  ```

  | Template  | Expansion                               | Matched Subscribed Item | Routed to Client |
  | ----------| --------------------------------------- | ----------------------- | -----------------|
  | `by-name` | `user-[firstName=Nyota,lastName=Uhura]` | _None_                  | _None_           |
  | `by-age`  | `user-[age=37]`                         | _SC1_                   | _Client C_       |



### Schema Registry

A _Schema Registry_ is a centralized repository that manages and validates schemas, which define the structure of valid messages.

The Kafka Connector supports integration with the [_Confluent Schema Registry_](https://docs.confluent.io/platform/current/schema-registry/index.html) through the configuration of parameters with the prefix `schema.registry`.

#### `schema.registry.url`

_Mandatory if the [Confluent Schema Registry](#recordkeyevaluatorschemaregistryenable-and-recordvalueevaluatorschemaregistryenable) is enabled_. The URL of the Confluent Schema Registry.

Example:

```xml
<param name="schema.registry.url">http//localhost:8081</param>
```

An encrypted connection is enabled by specifying the `https` protocol (see the [next section](#encryption-parameters-1)).

Example:

```xml
<param name="schema.registry.url">https://localhost:8084</param>
```

#### Basic HTTP Authentication Parameters

[Basic HTTP authentication](https://docs.confluent.io/platform/current/schema-registry/security/index.html#configuring-the-rest-api-for-basic-http-authentication) mechanism is supported through the configuration of parameters with the prefix `schema.basic.authentication`.

##### `schema.registry.basic.authentication.enable`

_Optional_. Enable Basic HTTP authentication of this connection against the Schema Registry. Can be one of the following:
- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="schema.registry.basic.authentication.enable">true</param>
```

##### `schema.registry.basic.authentication.username` and `schema.registry.basic.authentication.password`

_Mandatory if [Basic HTTP Authentication](#schemaregistrybasicauthenticationenable) is enabled_. The credentials.

- `schema.registry.basic.authentication.username`: the username
- `schema.registry.basic.authentication.password`: the password

Example:

```xml
<param name="schema.registry.basic.authentication.username">authorized-schema-registry-user</param>
<param name="schema.registry.basic.authentication.password">authorized-schema-registry-user-password</param>
```

#### Encryption Parameters

A secure connection to the Confluent Schema Registry can be configured through parameters with the prefix `schema.registry.encryption`, each one having the same meaning as the homologous parameters defined in the [Encryption Parameters](#encryption-parameters) section:

- `schema.registry.encryption.protocol` (see [encryption.protocol](#encryptionprotocol))
- `schema.registry.encryption.enabled.protocols` (see [encryption.enabled.protocols](#encryptionenabledprotocols))
- `schema.registry.encryption.cipher.suites` (see [encryption.cipher.suites](#encryptionciphersuites))
- `schema.registry.encryption.truststore.path` (see [encryption.truststore.path](#encryptiontruststorepath))
- `schema.registry.encryption.truststore.password` (see [encryption.truststore.password](#encryptiontruststorepassword))
- `schema.registry.encryption.hostname.verification.enable` (see [encryption.hostname.verification.enable](#encryptionhostnameverificationenable))
- `schema.registry.encryption.keystore.enable` (see [encryption.keystore.enable](#encryptionkeystoreenable))
- `schema.registry.encryption.keystore.path` (see [encryption.keystore.path](#encryptionkeystorepath))
- `schema.registry.encryption.keystore.password` (see [encryption.keystore.password](#encryptionkeystorepassword))
- `schema.registry.encryption.keystore.key.password` (see [encryption.keystore.key.password](#encryptionkeystorekeypassword))

Example:

```xml
<!-- Set the Confluent Schema Registry URL. The https protocol enables encryption parameters -->
<param name="schema.registry.url">https//localhost:8084</param>

<!-- Set general encryption settings -->
<param name="schema.registry.encryption.enabled.protocols">TLSv1.3</param>
<param name="schema.registry.encryption.cipher.suites">TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA</param>
<param name="schema.registry.encryption.hostname.verification.enable">true</param>

<!-- If required, configure the trust store to trust the Confluent Schema Registry certificates -->
<param name="schema.registry.encryption.truststore.path">secrets/kafka-connector.truststore.jks</param>
<param name="schema.registry.encryption.truststore.password">kafka-connector-truststore-password</param>

<!-- If mutual TLS is enabled on the Confluent Schema Registry, enable and configure the key store -->
<param name="schema.registry.encryption.keystore.enable">true</param>
<param name="schema.registry.encryption.keystore.path">secrets/kafka-connector.keystore.jks</param>
<param name="schema.registry.encryption.keystore.password">kafka-connector-password</param>
<param name="schema.registry.encryption.keystore.key.password">kafka-connector-private-key-password</param>
```

#### Quick Start Schema Registry Example

Check out the [adapters.xml](/examples/quickstart-schema-registry/adapters.xml#L58) file of the [_Quick Start Schema Registry_](/examples/quickstart-schema-registry/) app, where you can find an example of Schema Registry settings.

# Client Side Error Handling

When a client sends a subscription to the Kafka Connector, several error conditions can occur:

- Connection issues: the Kafka broker may be unreachable due to network problems or an incorrect configuration of the [`bootstrap.servers`](#bootstrapservers) parameter.
- Non-existent topics: none of the Kafka topics mapped in the [record routing](#record-routing-maptopicto) configurations exist in the broker.
- Data extraction: issues may arise while [extracting data](#data-extraction-language) from incoming records and the [`record.extraction.error.strategy`](#recordextractionerrorstrategy) parameter is set to `FORCE_UNSUBSCRIPTION`.

In these scenarios, the Kafka Connector triggers the unsubscription from all the items that were subscribed to the [target connection](#data_providername---kafka-connection-name). A client can be notified about the unsubscription event by implementing the `onUnsubscription` event handler, as shown in the following Java code snippet:

```java
subscription.addSubscriptionListener(new SubscriptionListener() {
  ...
  public void onUnsubscription() {
      // Manage the unsubscription event.
  }
  ...

});

```

# Customizing the Kafka Connector Metadata Adapter Class

If you need to customize the _Kafka Connector Metadata Adapter_ (e.g., to implement authentication and authorization or to handle client messages), 
you can create your own implementation by extending the factory class [`com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter`](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka/adapters/pub/KafkaConnectorMetadataAdapter.html).

You are free to implement any methods defined in the standard [`MetadataProvider`](https://lightstreamer.com/api/ls-adapter-inprocess/latest/com/lightstreamer/interfaces/metadata/MetadataProvider.html) interface and override implementations provided by its descendant classes ([`MetadataProviderAdapter`](https://lightstreamer.com/api/ls-adapter-inprocess/latest/com/lightstreamer/interfaces/metadata/MetadataProviderAdapter.html) and [`LiteralBasedProvider`](https://lightstreamer.com/api/ls-adapter-inprocess/latest/com/lightstreamer/adapters/metadata/LiteralBasedProvider.html)).

Bear in mind that the `KafkaConnectorMetadataAdapter` class already provides implementations for the following methods: init, notifyNewTables, notifyTablesClose, and wantsTablesNotification.
To extend such methods, the class offers hook methods that you can override to incorporate your custom logic:

- [_postInit_](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka/adapters/pub/KafkaConnectorMetadataAdapter.html#postInit(java.util.Map,java.io.File)):  Called after the initialization phase of the Kafka Connector Metadata Adapter is completed.

- [_onSubscription_](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka/adapters/pub/KafkaConnectorMetadataAdapter.html#onSubscription(java.lang.String,java.lang.String,com.lightstreamer.interfaces.metadata.TableInfo%5B%5D)): Called to notify when a user submits a subscription.

- [_onUnsubscription_](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka/adapters/pub/KafkaConnectorMetadataAdapter.html#onUnsubscription(java.lang.String,com.lightstreamer.interfaces.metadata.TableInfo%5B%5D)): Called to notify when a subscription is removed.

## Develop the Extension

To develop your extension, you need the Kafka Connector jar library, which is hosted on _Github Packages_.

For a Maven project, add the dependency to your _pom.xml_ file:

```xml
<dependency>
    <groupId>com.lightstreamer.kafka</groupId>
    <artifactId>kafka-connector</artifactId>
    <version>VERSION</version>
</dependency>
```

and follow these [instructions](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry#authenticating-to-github-packages) to configure the repository and authentication.

For a Gradle project, edit your _build.gradle_ file as follows:

1. Add the dependency:

   ```groovy
   dependencies {
       implementation group: 'com.lightstreamer.kafka', name: 'kafka-connector', 'version': '<version>'
   }
   ```

2. Add the repository and specify your personal access token:

   ```groovy
   repositories {
       mavenCentral()
       maven {
           name = "GitHubPackages"
           url = uri("https://maven.pkg.github.com/lightstreamer/lightstreamer-kafka-connector")
               credentials {
                   username = project.findProperty("gpr.user") ?: System.getenv("USERNAME")
                   password = project.findProperty("gpr.key") ?: System.getenv("TOKEN")
               }
       }
   }
   ```

In the [examples/custom-kafka-connector-adapter](/examples/custom-kafka-connector-adapter/) folder, you can find a sample Gradle project you may use as a starting point to build and deploy your custom extension.

# Kafka Connect Lightstreamer Sink Connector

The Lightstreamer Kafka Connector is also available as _Sink Connector plugin_ to be installed into [_Kafka Connect_](https://docs.confluent.io/platform/current/connect/index.html).

In this scenario, an instance of the connector plugin acts as a [_Remote Adapter_](https://github.com/Lightstreamer/Lightstreamer-lib-adapter-java-remote) for the Lightstreamer server as depicted in the following picture:

![KafkaConnectArchitecture](/pictures/kafka-connect.png)

The connector has been developed for Kafka Connect framework version 3.7 and requires JDK (Java Development Kit) v17 or newer.

## Usage

### Lightstreamer Setup

Before running the connector, you first need to deploy a Proxy Adapter into the Lightstreamer server instance.

#### Requirements

- JDK (Java Development Kit) v17 or newer
- [Lightstreamer Broker](https://lightstreamer.com/download/) (also referred to as _Lightstreamer Server_) v7.4.2 or newer. Follow the installation instructions in the `LS_HOME/GETTING_STARTED.TXT` file included in the downloaded package.

#### Steps

1. Create a directory within `LS_HOME/adapters` (choose whatever name you prefer, for example `kafka-connect-proxy`).

2. Copy the sample [`adapters.xml`](./kafka-connector-project/config/kafka-connect-proxy/adapters.xml) file to the `kafka-connect-proxy` directory.

3. Edit the file as follows:

   - Update the `id` attribute of the `adapters_conf` root tag. This settings has the same role of the already documented [Kafka Connector Identifier](#adapter_confid---kafka-connector-identifier).

   - Update the `name` attribute of the data_provider tag. This settings has the same role of the already documented [Kafka Connection Name](#data_providername---kafka-connection-name).

   - Update the `request_reply_port` parameter with the listening TCP port:

     ```xml
     <param name="request_reply_port">6661</param>
     ```

   - If authentication is required:

     - Set the `auth` parameter to `Y`:

       ```xml
       <param name="auth">Y</param>
       ```

     - Add the following parameters with the selected credential settings:

       ```xml
       <param name="auth.credentials.1.user">USERNAME</param>
       <param name="auth.credentials.1.password">PASSWORD</param>
       ```

> [!NOTE]
> As the `id` attribute must be unique across all the Adapter Sets deployed in the same Lightstreamer instance, make sure there is no conflict with any previously installed adapters (for example, the factory [adapters.xml](./kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) file included in the _Kafka Connector_ distribution package).

Finally, check that the Lightstreamer layout looks like the following:

```sh
LS_HOME/
...
├── adapters
│   ├── kafka-connect-proxy
│   │   ├── adapters.xml
│   └── welcome_res
...
├── audit
├── bin
...
```
### Running

To manually install the Kafka Connect Lightstreamer Sink Connector to a local Confluent Platform (version 7.6 or later) and run it in [_standalone mode_](https://docs.confluent.io/platform/current/connect/userguide.html#standalone-mode):

1. Download the connector zip file `lightstreamer-kafka-connect-lightstreamer-<version>.zip` from the [Releases](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/releases) page. Alternatively, check out this repository and execute the following command from the [`kafka-connector-project`](/kafka-connector-project/) folder:

   ```sh
   $ ./gradlew connectDistZip
   ```

   which generates the zip file under the `kafka-connector-project/kafka-connector/build/distributions` folder.

2. Extract the zip file into the desired location.

   For example, you can copy the connector contents into a new directory named `CONFLUENT_HOME/share/kafka/plugins`.

3. Edit the worker configuration properties file, ensuring you include the previous path in the `plugin.path` properties, for example:

   ```
   plugins.path=/usr/local/share/kafka/plugins
   ```

   You may want to use the provided [connect-standalone-local.properties](./kafka-connector-project/config/kafka-connect-config/connect-standalone-local.properties) file as a starting point.

3. Edit the connector configuration properties file as detailed in the [Configuration Reference](#configuration-reference) section.

   You may want to use the provided [`quickstart-lightstreamer-local.properties`](./kafka-connector-project/config/kafka-connect-config/quickstart-lightstreamer-local.properties) or [`quickstart-lightstreamer-local.json`](./kafka-connector-project/config/kafka-connect-config/quickstart-lightstreamer-local.json) files as starting pint. This file provides the set of pre-configured settings to feed Lightstreamer with stock market events, as already shown in the [installation instruction](#installation) for the Lightstreamer Kafka Connector.

4. Launch the Lightstreamer Server instance already configured in the [Lightstreamer Setup](#lightstreamer-setup) section.

5. Start the Connect worker with:

   ```sh
   $ bin/connect-standalone.sh connect-standalone-local.properties quickstart-lightstreamer-local.properties
   ```

To verify that an events stream actually flows from Kafka to a Lightstreamer consumer leveraging the same example already shown in the [Start](#start) section:

1. Attach a Lightstreamer consumer as specified in the step 2 of the [Start](#start) section.

2. Make sure that a Schema Registry service is reachable from your local machine.

3. Edit a `producer.properties` file as follows:

   ```
   # JSON serializer with support for the Schema Registry
   value.serializer=io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
   # Schema Registry URL
   schema.registry.url=http://<schema-registry-address>:<schema-registry-port>
   ```

   This configuration enables the producer to leverage the Schema Registry, which is required by Kafka Connect when a connector wants to deserialize JSON messages (unless an embedded schema is provided).

2. Publish events as specified in the step 3 of the [Start](#start) section.

   This time, run the publisher passing as further argument the `producer.properties` file:

   ```sh
   $ java -jar examples/quickstart-producer/build/libs/quickstart-producer-all.jar --bootstrap-servers <kafka.connection.string> --topic stocks --config-file producer.properties
   ```

3. Check the consumed events.

   You should see real-time updated as shown in the step 4 of the [Start](#start) section.

### Running in Docker

If you want to build a local Docker image based on Kafka Connect with the connector plugin, check out the [examples/docker-kafka-connect](/examples/docker-kafka-connect/) folder.

In addition, the [examples/quickstart-kafka-connect](/examples/quickstart-kafka-connect/) folder shows how to use that image in Docker Compose through a Kafka Connect version of the _Quick Start_ app.

## Supported Converters

The Kafka Connect Lightstreamer Sink Connector supports all the [converters](https://docs.confluent.io/platform/current/connect/index.html#converters) that come packaged with the Confluent Platform. These include:

- _AvroConverter_ `io.confluent.connect.avro.AvroConverter`
- _ProtobufConverter_ `io.confluent.connect.protobuf.ProtobufConverter`
- _JsonSchemaConverter_ `io.confluent.connect.json.JsonSchemaConverter`
- _JsonConverter_ `org.apache.kafka.connect.json.JsonConverter`
- _StringConverter_ `org.apache.kafka.connect.storage.StringConverter`
- _ByteArrayConverter_ `org.apache.kafka.connect.converters.ByteArrayConverter`

It also supports the built-in primitive converters:

- `org.apache.kafka.connect.converters.DoubleConverter`
- `org.apache.kafka.connect.converters.FloatConverter`
- `org.apache.kafka.connect.converters.IntegerConverter`
- `org.apache.kafka.connect.converters.LongConverter`
- `org.apache.kafka.connect.converters.ShortConverter`

## Configuration Reference

The Kafka Connect Lightstreamer Sink Connector configuration properties are described below.

### `connector.class`

To use the connector, specify the following setting:
`connector.class=com.lightstreamer.kafka.connect.LightstreamerSinkConnector`

### `tasks.max`

Due to the one-to-one relationship between a Proxy Adapter instance (deployed into the Lightstreamer server) and a Remote Adapter instance (a task), configuring more than one task in the `tasks.max` configuration parameter is pointless.

### `lightstreamer.server.proxy_adapter.address`

The Lightstreamer server's Proxy Adapter address to connect to in the format **`host:port`**.

- **Type:** string
- **Default:** none
- **Importance:** high

Example:

```
lightstreamer.server.proxy_adapter.address=lightstreamer.com:6661
```

### `lightstreamer.server.proxy_adapter.socket.connection.setup.timeout.ms`

The (optional) amount of time in milliseconds the connector will wait for the socket connection to be established to the Lightstreamer server's Proxy Adapter before terminating the task. Specify `0` for infinite timeout.

- **Type:** int
- **Default:** 5000 (5 seconds)
- **Valid Values:** [0,...]
- **Importance:** low

Example:

```
lightstreamer.server.proxy_adapter.socket.connection.setup.timeout.ms=15000
```

### `lightstreamer.server.proxy_adapter.socket.connection.setup.max.retries`

The (optional) max number of retries to establish a connection to the Lightstreamer server's Proxy Adapter.

- **Type:** int
- **Default:** 1
- **Valid Values:** [0,...]
- **Importance:** medium

Example:

```
lightstreamer.server.proxy_adapter.socket.connection.setup.max.retries=5
```

### `lightstreamer.server.proxy_adapter.socket.connection.setup.retry.delay.ms`

The (optional) amount of time in milliseconds to wait before retrying to establish a new connection to the Lightstreamer server's Proxy Adapter in case of failure. Only applicable if
`lightstreamer.server.proxy_adapter.socket.connection.setup.max.retries` > 0.

- **Type:** long
- **Default:** 5000 (5 seconds)
- **Valid Values:** [0,...]
- **Importance:** low

Example:

```
lightstreamer.server.proxy_adapter.socket.connection.setup.retry.delay.ms=15000
```

### `lightstreamer.server.proxy_adapter.username`

The username to use for authenticating to the Lightstreamer server's Proxy Adapter. This setting requires authentication to be enabled in the [configuration](#lightstreamer-setup) of the Proxy Adapter.

- **Type:** string
- **Importance:** medium
- **Default:** none

Example:

```
lightstreamer.server.proxy_adapter.username=lightstreamer_user
```

### `lightstreamer.server.proxy_adapter.password`

The password to use for authenticating to the Lightstreamer server's Proxy Adapter. This setting requires authentication to be enabled in the [configuration](#lightstreamer-setup) of the Proxy Adapter.

- **Type:** string
- **Default:** none
- **Importance:** medium

Example:
  ```
  lightstreamer.server.proxy_adapter.password=lightstreamer_password
  ```

### `record.extraction.error.strategy`

The (optional) error handling strategy to be used if an error occurs while extracting data from incoming deserialized records. Can be one of the following:

- `TERMINATE_TASK`: terminate the task immediately
- `IGNORE_AND_CONTINUE`: ignore the error and continue to process the next record
- `FORWARD_TO_DLQ`: forward the record to the dead letter queue

In particular, the `FORWARD_TO_DLQ` value requires a [_dead letter queue_](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/) to be configured; otherwise it will fallback to `TERMINATE_TASK`.


- **Type:** string
- **Default:** `TERMINATE_TASK`
- **Valid Values:** [`IGNORE_AND_CONTINUE`, `FORWARD_TO_DLQ`, `TERMINATE_TASK`]
- **Importance:** medium

Example:

```
record.extraction.error.strategy=FORWARD_TO_DLQ
```

### `topic.mappings`

> [!IMPORTANT]
> This configuration implements the same concepts already presented in the [Record Routing](#record-routing-maptopic_nameto) section.

Semicolon-separated list of mappings between source topics and Lightstreamer items. The list should describe a set of mappings in the form:

`[topicName1]:[mappingList1];[topicName2]:[mappingList2];...[topicNameN]:[mappingListN]`

where every specified topic (`[topicNameX]`) is mapped to the item names or item templates specified as comma-separated list (`[mappingListX]`).

- **Type:** string
- **Default:** none
- **Valid Values:**<br>
  [topicName1]:[mappingList1];<br>
  [topicName2]:[mappingList2];...
- **Importance:** high

Example:

```
topic.mappings=sample-topic:item-template.template1,item1,item2;order-topic:order-item
```

The configuration above specifies:

- A _One-to-many_ mapping between the topic `sample-topic` and the Lightstreamer items `sample-item1`, `sample-item2`, and `sample-item3`
- [_Filtered routing_](#filtered-record-routing-item-templatetemplate_name) through the reference to the item template `template1` (not shown in the snippet)
- A _One-to-one_ mapping between the topic `order-topic` and the Lightstreamer item `order-item`

### `topic.mappings.regex.enable`

The (optional) flag to enable the `topicName` parts of the [`topic.mappings`](#topicmappings) parameter to be treated as a regular expression rather than of a literal topic name.

- **Type:** boolean
- **Default:** false
- **Importance:** medium

Example:

```
topic.mappings.regex.enable=true
```


### `record.mappings`

> [!IMPORTANT]
> This configuration implements the same concepts already presented in the [Record Mapping](#record-mapping-fieldfield_name) section.

The list of mappings between Kafka records and Lightstreamer fields. The list should describe a set of subscribable fields in the following form:

 `[fieldName1]:[extractionExpression1],[fieldName2]:[extractionExpressionN],...,[fieldNameN]:[extractionExpressionN]`

where the Lightstreamer field `[fieldNameX]` will hold the data extracted from a deserialized Kafka record using the
_Data Extraction Language_ `[extractionExpressionX]`.

- **Type:** list
- **Default:** none
- **Valid Values:**<br>
   [fieldName1]:[extractionExpression1],<br>
   [fieldName2]:[extractionExpression2],...
- **Importance:** high

Example:

```
record.mappings=index:#{KEY.}, \
                stock_name:#{VALUE.name}, \
                last_price:#{VALUE.last_price}
```

The configuration above specifies the following mappings:

1. The record key to the Lightstreamer field `index`
2. The `name` attribute of the record value to the Lightstreamer field `stock_name`
3. The `last_price` of the record value to the Lightstreamer field `last_price`

### `record.mappings.skip.failed.enable`

By enabling this (optional) parameter, if a field mapping fails, that specific field's value will simply be omitted from the update sent to Lightstreamer clients, while other successfully mapped fields from the same record will still be delivered.

- **Type:** boolean
- **Default:** false
- **Importance:** medium

Example:

```
record.mappings.skip.failed.enable=true
```

### `record.mappings.map.non.scalar.values.enable`

Enabling this (optional) parameter allows mapping of non-scalar values to Lightstreamer fields. 
This enables complex data structures from Kafka records to be directly mapped to fields without the need to flatten them into scalar values.

- **Type:** boolean
- **Default:** false
- **Importance:** medium

Example:

```
record.mappings.map.non.scalar.values.enable=true
```

### `item.templates`

> [!IMPORTANT]
> This configuration implements the same concepts already presented in the [Filtered Record Routing](#filtered-record-routing-item-templatetemplate_name) section.

Semicolon-separated list of _item templates_, which specify the rules to enable the _filtering routing_. The list should describe a set of templates in the following form:

`[templateName1]:[template1];[templateName2]:[template2];...;[templateNameN]:[templateN]`

where the `[templateX]` configures the item template `[templateName]` defining the general format of the items the Lightstreamer clients must subscribe to to receive updates.

A template is specified in the form:

```
item-prefix-#{paramName1=extractionExpression1,paramName2=extractionExpression2,...}
```

To map a topic to an item template, reference it using the `item-template` prefix in the `topic.mappings` configuration:

```
topic.mappings=some-topic:item-template.templateName1,item-template.templateName2,...
```

- **Type:** string
- **Default:** null
- **Valid Values:**<br>
  [templateName1]:[template1];<br>
  [templateName2]:[template2];...
- **Importance:** high

Example:

```
item.templates=by-name:user-#{firstName=VALUE.name,lastName=VALUE.surname}; \
               by-age:user-#{age=VALUE.age}

topic.mappings=user:item-template.by-name,item-template.by-age
```

The configuration above specifies how to route records published from the topic `user` to the item templates `by-name` and `by-age`, which define the rules to extract some personal data by leveraging _Data Extraction Language_ expressions.

# Docs

The [docs](/docs/) folder contains the complete [Kafka Connector API Reference](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc), which is useful for implementing custom authentication and authorization logic, as described in the [Customizing the Kafka Connector Metadata Adapter Class](#customizing-the-kafka-connector-metadata-adapter-class) section.

To learn more about the [Lightstreamer Broker](https://lightstreamer.com/products/lightstreamer/) and the [Lightstreamer Kafka Connector](https://lightstreamer.com/products/kafka-connector/), visit their respective product pages.

# Examples

The [examples](/examples/) folder contains all the examples referenced throughout this guide, along with additional resources tailored for specific Kafka broker vendors. Additionally, you can explore the [_Airport Demo_](/examples/airport-demo/) for deeper insights into various usage and configuration options of the Lightstreamer Kafka Connector.

For more examples and live demos, visit our [online showcase](https://demos.lightstreamer.com/?p=kafkaconnector&lclient=noone&f=all&lall=all&sallall=all).
