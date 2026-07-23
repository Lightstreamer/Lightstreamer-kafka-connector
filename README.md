<img src="/pictures/lightstreamer.png" width="250">

# Lightstreamer Kafka Connector
_Last-mile data streaming. Stream real-time Kafka data to mobile and web apps, anywhere. Scale Kafka to millions of clients._

- [Introduction](#introduction)
  - [Last-mile integration](#last-mile-integration)
  - [Intelligent streaming](#intelligent-streaming)
  - [Comprehensive client SDKs](#comprehensive-client-sdks)
  - [Massive scalability](#massive-scalability)
  - [Other features](#other-features)
- [Architecture](#architecture)
  - [Kafka Client vs. Kafka Connect](#kafka-client-vs-kafka-connect)
    - [Lightstreamer Kafka Connector as a Kafka Client](#lightstreamer-kafka-connector-as-a-kafka-client)
    - [Lightstreamer Kafka Connector as a Kafka Connect Sink Connector](#lightstreamer-kafka-connector-as-a-kafka-connect-sink-connector)
- [QUICK START: Set up in 5 minutes](#quick-start-set-up-in-5-minutes)
  - [Run](#run)
- [Deployment](#deployment)
  - [Manual deployment](#manual-deployment)
    - [Requirements](#requirements)
    - [Install](#install)
    - [Configure](#configure)
    - [Start](#start)
  - [Docker-based deployment](#docker-based-deployment)
    - [Requirements](#requirements-1)
    - [Get the image](#get-the-image)
    - [Configure](#configure-1)
    - [Start](#start-1)
  - [Kubernetes deployment](#kubernetes-deployment)
  - [End-to-end streaming](#end-to-end-streaming)
    - [Connect a Kafka producer](#connect-a-kafka-producer)
    - [Connect a Lightstreamer consumer](#connect-a-lightstreamer-consumer)
      - [Connect a browser-based consumer](#connect-a-browser-based-consumer)
      - [Connect a Java consumer](#connect-a-java-consumer)
- [Configuration](#configuration)
  - [Global settings](#global-settings)
  - [Connection settings](#connection-settings)
    - [General parameters](#general-parameters)
    - [Encryption parameters](#encryption-parameters)
    - [Kafka broker authentication parameters](#kafka-broker-authentication-parameters)
  - [Record processing](#record-processing)
  - [Topic mapping](#topic-mapping)
    - [Data extraction language](#data-extraction-language)
    - [Record routing (`map.TOPIC_NAME.to`)](#record-routing-maptopic_nameto)
    - [Record mapping (`field.FIELD_NAME`)](#record-mapping-fieldfield_name)
    - [Filtered record routing (`item-template.TEMPLATE_NAME`)](#filtered-record-routing-item-templatetemplate_name)
  - [Item snapshot settings](#item-snapshot-settings)
    - [`item.snapshot.enabled.mode`](#itemsnapshotenabledmode)
    - [`item.snapshot.distinct.length`](#itemsnapshotdistinctlength)
    - [`item.snapshot.max.idle.seconds`](#itemsnapshotmaxidleseconds)
  - [Schema Registry](#schema-registry)
    - [`schema.registry.provider`](#schemaregistryprovider)
    - [`schema.registry.url`](#schemaregistryurl)
    - [Confluent Schema Registry parameters](#confluent-schema-registry-parameters)
      - [Basic HTTP authentication parameters](#basic-http-authentication-parameters)
      - [Encryption parameters](#encryption-parameters-1)
      - [Confluent Schema Registry quickstart](#confluent-schema-registry-quickstart)
    - [Azure Schema Registry parameters](#azure-schema-registry-parameters)
- [Subscription modes](#subscription-modes)
- [Snapshot management](#snapshot-management)
  - [Default behavior (NONE)](#default-behavior-none)
  - [Connector-managed snapshot](#connector-managed-snapshot)
    - [MERGE snapshot](#merge-snapshot)
    - [DISTINCT snapshot](#distinct-snapshot)
    - [COMMAND snapshot](#command-snapshot)
    - [Idle expiration](#idle-expiration)
    - [Caveats](#caveats)
- [Client-side error handling](#client-side-error-handling)
- [Customizing the Kafka Connector Metadata Adapter class](#customizing-the-kafka-connector-metadata-adapter-class)
  - [Develop the extension](#develop-the-extension)
- [Kafka Connect Lightstreamer Sink Connector](#kafka-connect-lightstreamer-sink-connector)
  - [Usage](#usage)
    - [Lightstreamer setup](#lightstreamer-setup)
    - [Running](#running)
    - [Running in Docker](#running-in-docker)
  - [Supported converters](#supported-converters)
  - [Configuration reference](#configuration-reference)
- [Docs](#docs)
- [Examples](#examples)

# Introduction

Is your product struggling to deliver Kafka events to remote users? The [Lightstreamer Kafka Connector](https://lightstreamer.com/products/kafka-connector/) is an intelligent proxy that bridges the gap, providing seamless, real-time data streaming to web and mobile applications with unmatched ease and reliability. It streams data in real time to your apps over WebSockets, eliminating the need for polling a REST proxy and surpassing the limitations of MQTT.

## Last-mile integration

Kafka, while powerful, isn’t designed for direct internet access—particularly when it comes to the **last mile**, the critical network segment that extends beyond enterprise boundaries and edges (LAN or WAN) to reach end users. Last-mile integration is essential for delivering real-time Kafka data to mobile, web, and desktop applications, addressing challenges that go beyond Kafka’s typical scope, such as:
- Disruptions from corporate firewalls and client-side proxies blocking Kafka connections.
- Performance issues due to unpredictable internet bandwidth, including packet loss and disconnections.
- User interfaces struggling with large data volumes.
- The need for scalable solutions capable of supporting millions of concurrent users.

![High-Level Architecture](/pictures/architecture.png)

## Intelligent streaming

With **Intelligent streaming**, Lightstreamer dynamically adjusts the data flow to match each user’s network conditions, ensuring all users stay in sync regardless of connection quality. By resampling and conflating data on the fly, it delivers real-time updates with adaptive throttling, effectively handling packet loss without buffering delays. It also manages disconnections and reconnection seamlessly, keeping your users connected and up-to-date.

## Comprehensive client SDKs

The rich set of supplied client libraries makes it easy to consume real-time Kafka data across a variety of platforms and languages.

![Client APIs](/pictures/client-platforms.png)

## Massive scalability

Connect millions of clients without compromising performance. Fanout real-time messages published on Kafka topics efficiently, preventing overload on the Kafka brokers. Check out the [load tests performed on the Lightstreamer Kafka Connector vs. plain Kafka](https://github.com/Lightstreamer/lightstreamer-kafka-connector-loadtest).

## Other features

The Lightstreamer Kafka Connector provides a wide range of powerful features, including firewall and proxy traversal, server-side filtering, advanced topic mapping, record processing, Schema Registry support, push notifications, and maximum security. [Explore more details](https://lightstreamer.com/products/kafka-connector/).

# Architecture

![Architecture](/pictures/architecture-full.png)

The Lightstreamer Kafka Connector seamlessly integrates the [Lightstreamer Broker](https://lightstreamer.com/products/lightstreamer/) with any Kafka broker. While existing producers and consumers continue connecting directly to the Kafka broker, internet-based applications connect through the Lightstreamer Broker, which efficiently handles last-mile data delivery. Authentication and authorization for internet-based clients are managed via a custom Metadata Adapter, created using the [Metadata Adapter API Extension](#customizing-the-kafka-connector-metadata-adapter-class) and integrated into the Lightstreamer Broker.

Both the Kafka Connector and the Metadata Adapter run in-process with the Lightstreamer Broker, which can be deployed in the cloud or on-premises. For Kubernetes environments, the [Lightstreamer Helm Chart](https://github.com/Lightstreamer/helm-charts/blob/main/DEPLOYMENT.md) provides a streamlined deployment experience.

## Kafka Client vs. Kafka Connect

The Lightstreamer Kafka Connector can operate in two distinct modes: as a direct Kafka client or as a Kafka Connect connector.

### Lightstreamer Kafka Connector as a Kafka Client

In this mode, the Lightstreamer Kafka Connector uses the Kafka client API to communicate directly with the Kafka broker. This approach is typically lighter, faster, and more scalable, as it avoids the additional layer of Kafka Connect. All sections of this documentation refer to this mode, except for the section specifically dedicated to the Sink Connector.

### Lightstreamer Kafka Connector as a Kafka Connect Sink Connector

In this mode, the Lightstreamer Kafka Connector integrates with the Kafka Connect framework, acting as a sink connector. While this introduces an additional messaging layer, there are scenarios where the standardized deployment provided by Kafka Connect is required. For more details on using the Lightstreamer Kafka Connector as a Kafka Connect sink connector, please refer to this section: [Kafka Connect Lightstreamer Sink Connector](#kafka-connect-lightstreamer-sink-connector).

# QUICK START: Set up in 5 minutes

To efficiently showcase the functionalities of the Lightstreamer Kafka Connector, we have prepared an accessible quickstart application located in the [`examples/quickstart`](/examples/quickstart/) directory. This streamlined application facilitates real-time streaming of data from a Kafka topic directly to a web interface. It leverages a modified version of the [Stock List Demo](https://github.com/Lightstreamer/Lightstreamer-example-StockList-client-javascript?tab=readme-ov-file#basic-stock-list-demo---html-client), specifically adapted to demonstrate Kafka integration within the financial market data domain. This demo displays real-time streaming data for ten stocks, generated by a simulated market feed. This setup is designed for rapid comprehension, enabling you to swiftly grasp and observe the connector's performance in a real-world financial scenario.

![Quickstart Diagram](/pictures/quickstart-diagram.png)

The diagram above illustrates how, in this setup, a stream of simulated market events is channeled from Kafka to the web client via the Lightstreamer Kafka Connector.

To provide a complete stack, the app is based on _Docker Compose_. The [Docker Compose file](/examples/quickstart/docker-compose.yml) comprises the following services:

1. _broker_: a Kafka broker, based on the [Docker Image for Apache Kafka](https://kafka.apache.org/documentation/#docker). Please notice that other versions of this quickstart are available in the in the examples directory, specifically targeted to other brokers:
 - [`Confluent Cloud`](/examples/vendors/confluent/quickstart-confluent-cloud/README.md)
 - [`Confluent Platform`](/examples/vendors/confluent/quickstart-confluent-platform/README.md)
 - [`Redpanda Serverless`](/examples/vendors/redpanda/quickstart-redpanda-serverless/README.md)
 - [`Redpanda Self-Managed`](/examples/vendors/redpanda/quickstart-redpanda-self-managed/README.md)
 - [`Aiven`](/examples/vendors/aiven/quickstart-aiven/README.md)
 - [`Axual`](/examples/vendors/axual/quickstart-axual/README.md)
 - [`AutoMQ`](/examples/vendors/automq/quickstart-automq/README.md)
 - [`Amazon MSK`](/examples/vendors/aws/quickstart-msk/README.md)
 - [`Azure Event Hubs`](/examples/vendors/azure/quickstart-azure/README.md)
2. _kafka-connector_: Lightstreamer Server with the Kafka Connector, based on the [Lightstreamer Kafka Connector Docker image](/docker/), which also includes a web client mounted on `/lightstreamer/pages/QuickStart`
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

# Deployment

This section will guide you through deploying the Kafka Connector quickly and easily.

Deployment options:

- **Manual deployment:**
  Download and configure the Lightstreamer Broker and Kafka Connector from their respective archives.

- **Docker-based deployment:**
  Pull or build a Docker image that seamlessly integrates the Lightstreamer Broker and the Kafka Connector.

- **Kubernetes deployment:**
  Deploy the Kafka Connector to a Kubernetes cluster using the Lightstreamer Helm Chart.

## Manual deployment

### Requirements

- JDK (Java Development Kit) v17 or newer
- [Lightstreamer Broker](https://lightstreamer.com/download/) (also referred to as _Lightstreamer Server_) v7.4.8 or newer. Follow the installation instructions in the `LS_HOME/GETTING_STARTED.TXT` file included in the downloaded package.
- A running Kafka broker or Kafka cluster

### Install

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

### Configure

Before starting the Kafka Connector, you need to properly configure the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/adapters.xml` file. For convenience, the package comes with a predefined configuration (the same used in the [_Quickstart_](#quick-start-set-up-in-5-minutes) app), which can be customized in all its aspects as per your requirements. Of course, you may add as many different connection configurations as desired to fit your needs.

To quickly complete the installation and verify the successful integration with Kafka, edit the _data_provider_ block `QuickStart` in the file as follows:

- Update the [`bootstrap.servers`](#bootstrapservers) parameter with the connection string of Kafka:

  ```xml
  <param name="bootstrap.servers">kafka.connection.string</param>
  ```

- Configure [topic and record mapping](#topic-mapping) to route Kafka events to Lightstreamer items.

  To enable a generic Lightstreamer client to receive real-time updates, it needs to subscribe to one or more items. Therefore, the Kafka Connector provides suitable mechanisms to map Kafka topics to Lightstreamer items effectively.

  The `QuickStart` [factory configuration](/kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml#L41) comes with a straightforward mapping defined through the following settings:

  - An item template:
    ```xml
    <param name="item-template.stock">stock-#{index=KEY}</param>
    ```

    which defines the general format name of the items a client must subscribe to to receive updates from the Kafka Connector. The [_extraction expression_](#filtered-record-routing-item-templatetemplate_name) syntax used here - denoted within `#{...}` - permits the clients to specify filtering values to be compared against the actual contents of a Kafka record, evaluated through [_Extraction Keys_](#data-extraction-language) used to extract each part of a record. In this case, the `KEY` predefined constant extracts the key part of Kafka records.

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
  <param name="field.name">#{VALUE.name}</param>
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

- Optionally, customize the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/log4j.properties` file (the current settings produce the `quickstart.log` and `quickstart-monitor.log` files).

You can get more details about all possible settings in the [Configuration](#configuration) section.

### Start

To start the Kafka Connector, run the following from the `LS_HOME/bin/unix-like` directory:

   ```sh
   $ ./background_start.sh
   ```

Then, point your browser to [http://localhost:8080](http://localhost:8080) and see a welcome page with some demos running out of the box.

## Docker-based deployment

### Requirements

- Docker

### Get the image

Images are published to GitHub Container Registry on each release. You can pull an image from the registry or build it locally.

**Pull from GitHub Container Registry:**

```sh
# Pull the latest image
$ docker pull ghcr.io/lightstreamer/lightstreamer-kafka-connector:latest

# Or pull a specific version
$ docker pull ghcr.io/lightstreamer/lightstreamer-kafka-connector:2.0.0
```

**Alternatively, build locally from source:**

See [/docker](/docker/) for build instructions.

### Configure

Prepare your `adapters.xml` configuration file. You can start from the factory [adapters.xml](/kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) file and customize it following the guidelines in the [Configure](#configure) section above.

You can also optionally prepare a `log4j.properties` file for custom logging configuration.

### Start

Launch the container with your configuration files mounted as Docker volumes:

```sh
$ docker run --name kafka-connector -d -p 8080:8080 \
  -v $(pwd)/adapters.xml:/lightstreamer/adapters/lightstreamer-kafka-connector/adapters.xml \
  -v $(pwd)/log4j.properties:/lightstreamer/adapters/lightstreamer-kafka-connector/log4j.properties \
  ghcr.io/lightstreamer/lightstreamer-kafka-connector:latest
```

Then, point your browser to [http://localhost:8080](http://localhost:8080) and see a welcome page with some demos running out of the box.

## Kubernetes deployment

To deploy the Lightstreamer Kafka Connector to a Kubernetes or OpenShift cluster, use the [Lightstreamer Helm Chart](https://github.com/Lightstreamer/helm-charts), which provides built-in support for the Kafka Connector through the `connectors.kafkaConnector` values section.

The Helm Chart handles provisioning, connection setup, topic routing, field mapping, schema registry integration, and logging — all configured declaratively through Helm values. Multiple provisioning methods are available, including the official [Lightstreamer Kafka Connector container image](https://github.com/orgs/Lightstreamer/packages/container/package/lightstreamer-kafka-connector).

For the full deployment guide, see [Deploying Lightstreamer Broker to Kubernetes](https://github.com/Lightstreamer/helm-charts/blob/main/DEPLOYMENT.md), and in particular the [Kafka Connector](https://github.com/Lightstreamer/helm-charts/blob/main/DEPLOYMENT.md#kafka-connector) section for connector-specific configuration.

## End-to-end streaming

Once the Lightstreamer Kafka Connector is up and running—whether deployed manually, using Docker, or on Kubernetes—it's time to publish events and connect a Lightstreamer consumer to experience a basic *end-to-end* streaming flow in action.

### Connect a Kafka producer

The [`examples/quickstart-producer`](/examples/quickstart-producer/) folder contains a simple native Kafka producer designed to publish simulated market events for the *Quickstart* app.

Before launching the producer, you first need to build it. Open a new shell from the folder and execute the following command:

```sh
$ cd examples/quickstart-producer
$ ./gradlew build
```

This command generates the `quickstart-producer-<version>-all.jar` file under the `build/libs` folder.

Then, launch it with:

```sh
$ java -jar build/libs/quickstart-producer-<version>-all.jar --bootstrap-servers <kafka.connection.string> --topic stocks
```

![producer_video](/pictures/producer.gif)

### Connect a Lightstreamer consumer

After starting the publisher, you can connect a client application to consume real-time data and display it in its frontend. Below, we'll demonstrate a browser-based example using **HTML and JavaScript**, and a **Java** example. However, you are encouraged to explore any of the [Lightstreamer client SDKs](https://lightstreamer.com/download/#client-sdks) for developing clients in other environments and languages, including **iOS, Android, Python, and more**.

#### Connect a browser-based consumer

Download the provided [sample web client](/examples/compose-templates/web), based on HTML and JavaScript. Simply open the `index.html` file and watch real-time updates populate the frontend immediately.

![consumer_video](/pictures/end-to-end-streaming.gif)

As shown in the [source code](/examples/compose-templates/web/index.html), consuming live data from the Kafka Connector involves just a few steps:

1. **Establishing a connection:**
   To connect to the Lightstreamer Kafka Connector, a `LightstreamerClient` object is created to connect to the server at `http://localhost:8080` and specifies the adapter set `KafkaConnector`, as [configured](#adapter_confid---kafka-connector-identifier) on the server side through the `id` attribute of the `adapters_conf` root tag in the `adapters.xml` file.

   ```js
   var lsClient = new LightstreamerClient("http://localhost:8080", "KafkaConnector");
   ...
   lsClient.connect();
   ```

2. **Setting up the data grid:**
   To visualize real-time updates, a `StaticGrid` object is instantiated and configured to display data from a `Subscription` into statically prepared HTML rows. This is a simple widget provided by the Lightstreamer client library for demonstration purposes. You are free to use any existing JavaScript framework or library to display the data.

   ```js
   var stocksGrid = new StaticGrid("stocks", true);
   stocksGrid.setAutoCleanBehavior(true, false);
   stocksGrid.addListener({
       onVisualUpdate: function (key, info, pos) {
           ...
           var stockIndex = key.substring(13, key.indexOf(']'));
           var color = (stockIndex % 2 == 1) ? "#fff" : "#e9fbf2";
           info.setAttribute("#fff7d5", color, "backgroundColor");
       }
   });
   ```

3. **Subscribing to live data:**
   To create a subscription, a `Subscription` object is created and configured in `MERGE` mode with a list of items and fields to subscribe to, extracted from the `StaticGrid`.

   The subscription references the `QuickStart` data adapter name, as [configured](#data_providername---kafka-connection-name) on the server side through the `name` attribute of the `data_provider` element in the `adapters.xml` file. The `StaticGrid` is attached as a listener to the subscription to receive and display updates.

   ```js
   var stockSubscription = new Subscription("MERGE", stocksGrid.extractItemList(), stocksGrid.extractFieldList());
   stockSubscription.setDataAdapter("QuickStart");
   stockSubscription.addListener(stocksGrid);
   lsClient.subscribe(stockSubscription);
   ```

#### Connect a Java consumer

In addition to the browser-based consumer above, you can set up a Java consumer. The [`kafka-connector-utils`](/kafka-connector-project/kafka-connector-utils) submodule hosts a simple Lightstreamer Java client that can be used to test the consumption of Kafka events from any Kafka topics.

Before launching the consumer, you first need to build it from the [`kafka-connector-project`](/kafka-connector-project/) folder with the command:

```sh
$ ./gradlew kafka-connector-utils:build
```

This command generates the `lightstreamer-kafka-connector-utils-consumer-all-<version>.jar` file under the `kafka-connector-project/kafka-connector-utils/build/libs` folder.

Then, launch it with:

```sh
$ java -jar kafka-connector-utils/build/libs/lightstreamer-kafka-connector-utils-consumer-all-<version>.jar --address http://localhost:8080 --adapter-set KafkaConnector --data-adapter QuickStart --items stock-[index=1],stock-[index=2],stock-[index=3] --fields name,ask,bid,min,max
```

As you can see, you need to specify a few parameters:

- `--address`: the Lightstreamer Server address
- `--adapter-set`: the name of the requested Adapter Set, which triggers Lightstreamer to activate the Kafka Connector deployed into the `adapters` folder
- `--data-adapter`: the name of the requested Data Adapter, which identifies the selected Kafka connection configuration
- `--items`: the list of items to subscribe to
- `--fields`: the list of requested fields for the items

> [!NOTE]
> While we've provided examples in JavaScript (suitable for web browsers) and Java (geared toward desktop applications), you are encouraged to utilize any of the [Lightstreamer client SDKs](https://lightstreamer.com/download/#client-sdks) for developing clients in other environments, including iOS, Android, Python, and more.

![consumer_video](/pictures/consumer.gif)

# Configuration

As already anticipated, the Kafka Connector is a Lightstreamer Adapter Set, which means it is made up of a Metadata Adapter and one or more Data Adapters, whose settings are defined in the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/adapters.xml` file.

The following sections will guide you through the configuration details.

## Global settings

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

_Mandatory_. The path of the [reload4j](https://reload4j.qos.ch/) configuration file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector-<version>`), or as an absolute path.

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

## Connection settings

The Kafka Connector allows the configuration of separate independent connections to different Kafka brokers/clusters.

Every single connection is configured via the definition of its own Data Adapter through the _data_provider_ block. At least one connection must be provided.

Since the Kafka Connector manages the physical connection to Kafka by wrapping an internal Kafka Consumer, several configuration settings in the Data Adapter are identical to those required by the usual Kafka Consumer configuration.

### General parameters

#### `data_provider['name']` - _Kafka Connection Name_

_Optional_. The `name` attribute of the `data_provider` tag defines _Kafka Connection Name_, which will be used by the Clients to request real-time data from this specific Kafka connection through a _Subscription_ object.

Furthermore, the name is also used to group all logging messages belonging to the same connection.

> [!TIP]
> For every Data Adapter connection, add new loggers and their relative file appenders to `log4j.properties`, so that you can log to dedicated files all the interactions pertinent to the connection with the Kafka cluster and the message retrieval operations, along with their routing to the subscribed items.
> For example, the factory [logging configuration](/kafka-connector-project/kafka-connector/src/adapter/dist/log4j.properties#L23) provides both a regular logger `QuickStart` and a monitor logger `QuickStartMonitor` for the `QuickStart` connection:
> ```java
> ...
> # QuickStart logger
> log4j.logger.QuickStart=INFO, QuickStartFile
> log4j.appender.QuickStartFile=org.apache.log4j.RollingFileAppender
> log4j.appender.QuickStartFile.layout=org.apache.log4j.PatternLayout
> log4j.appender.QuickStartFile.layout.ConversionPattern=[%d] [%-10c{1}] %-5p %m%n
> log4j.appender.QuickStartFile.File=../../logs/quickstart.log
>
> # QuickStart Monitor logger
> log4j.logger.QuickStartMonitor=INFO, QuickStartMonitorFile
> log4j.appender.QuickStartMonitorFile=org.apache.log4j.RollingFileAppender
> log4j.appender.QuickStartMonitorFile.layout=org.apache.log4j.PatternLayout
> log4j.appender.QuickStartMonitorFile.layout.ConversionPattern=[%d] %m%n
> log4j.appender.QuickStartMonitorFile.File=../../logs/quickstart-monitor.log
> ```

Default value: `DEFAULT`, but only one `DEFAULT` configuration is permitted.

Example:

```xml
<data_provider name="BrokerConnection">
```

#### `adapter_class`

_Mandatory_. The `adapter_class` tag defines the Java class name of the Data Adapter. **DO NOT EDIT IT!**.

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

The parameter sets the value of the [`bootstrap.servers`](https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_bootstrap.servers) key to configure the internal Kafka Consumer.

Example:

```xml
<param name="bootstrap.servers">broker:29092,broker:29093</param>
```

#### `group.id`

_Optional but only effective when [`consumer.mode`](#consumermode) is set to `GROUP` (the default)_. The name of the consumer group this connection belongs to.

The parameter sets the value of the [`group.id`](https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_group.id) key to configure the internal Kafka Consumer.

Default value: _Kafka Connector Identifier_ + _Connection Name_ + _Randomly generated suffix_.

```xml
<param name="group.id">kafka-connector-group</param>
```

#### `consumer.mode`

_Optional_. Selects how the internal Kafka Consumer acquires the topic partitions it consumes from. Can be one of the following:

- `GROUP`: The consumer joins a [Kafka consumer group](https://kafka.apache.org/documentation/#intro_consumers) and lets the group coordinator assign partitions dynamically. Partition ownership is redistributed automatically as members of the group join or leave, and offsets are committed to and fetched from the `__consumer_offsets` topic under the configured [`group.id`](#groupid). This is the default and matches the pre-existing behavior of the connector.

- `MANUAL`: The consumer uses manual partition assignment via `KafkaConsumer.assign(...)`. No consumer group is joined, no rebalance protocol runs, and [`group.id`](#groupid) is suppressed (no offsets are committed or fetched). On every startup the connector explicitly seeks each assigned partition to the position dictated by [`record.consume.from`](#recordconsumefrom).

  Use `MANUAL` together with [`map.TOPIC_NAME.from.partitions`](#consume-from-specific-partitions-maptopic_namefrompartitions) to declaratively pin this connector instance to a specific subset of partitions, typically for [partition-affinity sharding](#partition-affinity-sharding) across multiple connector instances.

Default value: `GROUP`.

Example:

```xml
<param name="consumer.mode">MANUAL</param>
```

### Encryption parameters

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

_Optional_. The path of the trust store file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector-<version>`), or as an absolute path.

The trust store is used to validate the certificates provided by the Kafka brokers.

Example:

```xml
<param name="encryption.truststore.path">secrets/kafka-connector.truststore.jks</param>
```

#### `encryption.truststore.type`

_Optional_. The type of the trust store. Can be one of the following:
- `JKS`
- `PKCS12`

Default value: `JKS`.

Example:

```xml
<param name="encryption.truststore.type">PKCS12</param>
```

#### `encryption.truststore.password`

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
- `encryption.keystore.type`
- `encryption.keystore.password`
- `encryption.keystore.key.password`

Default value: `false`.

Example:

```xml
<param name="encryption.keystore.enable">true</param>
```

#### `encryption.keystore.path`

_Mandatory if [key store](#encryptionkeystoreenable) is enabled_. The path of the key store file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector-<version>`), or as an absolute path.

Example:

```xml
<param name="encryption.keystore.path">secrets/kafka-connector.keystore.jks</param>
```

#### `encryption.keystore.type`

_Optional_. The type of the key store. Can be one of the following:
- `JKS`
- `PKCS12`

Default value: `JKS`.

Example:

```xml
<param name="encryption.keystore.type">PKCS12</param>
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

#### SSL quickstart

For an example of an encryption configuration, see the [adapters.xml](/examples/quickstart-ssl/adapters.xml#L17) file of the [_SSL Quickstart_](/examples/quickstart-ssl/) app.

### Kafka broker authentication parameters

Kafka broker authentication is configured through parameters with the prefix `authentication`.

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

- [`PLAIN`](#plain) (the default value)
- [`SCRAM-SHA-256`](#scram-sha-256)
- [`SCRAM-SHA-512`](#scram-sha-512)
- [`GSSAPI`](#gssapi)
- [`AWS_MSK_IAM`](#aws_msk_iam)

In the case of `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512` mechanisms, the credentials must be configured through the following mandatory parameters:

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

For an example of a SASL/PLAIN authentication configuration, see the [adapters.xml](/examples/vendors/confluent/quickstart-confluent-cloud/adapters.xml#L28) file of the [_Confluent Cloud Quickstart_](/examples/vendors/confluent/quickstart-confluent-cloud/) app.

##### `SCRAM-SHA-256`

Example:

```xml
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">SCRAM-SHA-256</param>
<param name="authentication.username">authorized-kafka-usee</param>
<param name="authentication.password">authorized-kafka-user-password</param>
```

For examples of a SCRAM-SHA-256 authentication configuration, check out the `adapters.xml` files of the [_Redpanda Serverless Quickstart_](/examples/vendors/redpanda/quickstart-redpanda-serverless/) ([adapters.xml](/examples/vendors/redpanda/quickstart-redpanda-serverless/adapters.xml#L22)) and the [_Aiven Quickstart_](/examples/vendors/aiven/quickstart-aiven/) ([adapters.xml](/examples/vendors/aiven/quickstart-aiven/adapters.xml#L24)) apps.

##### `SCRAM-SHA-512`

Example:

```xml
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">SCRAM-SHA-512</param>
<param name="authentication.username">authorized-kafka-username</param>
<param name="authentication.password">authorized-kafka-username-password</param>
```

For an example of a SCRAM-SHA-512 authentication configuration, see the [adapters.xml](/examples/vendors/axual/quickstart-axual/adapters.xml#L22) file of the [_Axual Quickstart_](/examples/vendors/axual/quickstart-axual/) app.

##### `GSSAPI`

When this mechanism is specified, you can configure the following authentication parameters:

- `authentication.gssapi.key.tab.enable`

  _Optional_. Enable the use of a keytab. Can be one of the following:
  - `true`
  - `false`

  Default value: `false`.

- `authentication.gssapi.key.tab.path`

  _Mandatory if keytab is enabled_. The path to the keytab file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector-<version>`), or as an absolute path.

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

- `authentication.iam.credential.profile.name`

  _Optional_. The name of the AWS credential profile to use for authentication. These profiles are defined in the [AWS shared credentials file](https://docs.aws.amazon.com/sdkref/latest/guide/file-location.html).

  Example:

  ```xml
  <param name="authentication.iam.credential.profile.name">msk_client<param>
  ```

- `authentication.iam.role.arn`

  _Optional_. The Amazon Resource Name (ARN) of the IAM role that the Kafka Connector should assume for authentication with MSK. Use this when you want the connector to assume a specific role with temporary credentials.

  Example:

  ```xml
  <param name="authentication.iam.role.arn">arn:aws:iam::123456789012:role/msk_client_role<param>
  ```

- `authentication.iam.role.session.name`

   _Optional_ but only effective when `authentication.iam.role.arn` is set. Specifies a custom session name for the assumed role.

  Example:

  ```xml
  <param name="authentication.iam.role.session.name">consumer<param>
  ```

- `authentication.iam.sts.region`

  _Optional_ but only effective when `authentication.iam.role.arn` is set. Specifies the AWS region of the STS endpoint to use when assuming the IAM role.

  Example:

  ```xml
  <param name="iam.sts.region">us-west-1<param>
  ```

> [!NOTE]
> **Authentication precedence**: If both methods are configured, the `iam.credential.profile.name` parameter takes precedence over `iam.role.arn`. If neither parameter is provided, the Kafka Connector falls back to the [AWS SDK default credential provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html).

For an example of an AWS_MSK_IAM authentication configuration, see the [adapters.xml](/examples/vendors/aws/quickstart-msk/adapters.xml#L21) file of the [_MSK Quickstart_](/examples/vendors/aws/quickstart-msk/) app.

## Record processing

The Kafka Connector can deserialize Kafka records from the following formats:

- _Apache Avro_
- _JSON_
- _Protobuf_
- _String_
- _Integer_
- _Float_

and other scalar types (see [the complete list](#recordkeyevaluatortype-and-recordvalueevaluatortype)).

In particular, the Kafka Connector supports message validation for _Avro_, _JSON_, and _Protobuf_ which can be specified through:

- Local schema (or binary descriptor) files: Use this option when you have predefined schemas stored locally and do not require a centralized schema management system.
- A _Schema Registry_: Opt for this when you need a centralized repository to manage and validate schemas across multiple applications and environments.

The Kafka Connector enables the independent deserialization of keys and values, allowing them to have different formats. Additionally:

- Message validation against a Schema Registry can be enabled separately for the key and value (through [`record.key.evaluator.schema.registry.enable` and `record.value.evaluator.schema.registry.enable`](#recordkeyevaluatorschemaregistryenable-and-recordvalueevaluatorschemaregistryenable)).
- Message validation against local schema (or binary descriptor) files must be specified separately for the key and the value (through [`record.key.evaluator.schema.path` and `record.value.evaluator.schema.path`](#recordkeyevaluatorschemapath-and-recordvalueevaluatorschemapath)). In addition, using Protobuf also requires the specification of the [message type](#recordkeyevaluatorprotobufmessagetype-and-recordvalueevaluatorprotobufmessagetype).

**Support for Key Value Pairs (KVP)**

In addition to the above formats, the Kafka Connector also supports the _Key Value Pairs_ (KVP) format. This format allows Kafka records to be represented as a collection of key-value pairs, making it particularly useful for structured data where each key is associated with a specific value.

The Kafka Connector provides flexible configuration options for parsing and extracting data from KVP-formatted records, enabling seamless mapping to Lightstreamer fields. Key-value pairs can be separated by custom delimiters for both the pairs themselves and the key-value separator, ensuring compatibility with diverse data structures. For example:

- A record with the format `key1=value1;key2=value2` uses `=` as the key-value separator and `;` as the pairs separator.
- These separators can be customized using the parameters [`record.key/value.evaluator.kvp.key-value.separator`](#recordkeyevaluatorkvpkey-valueseparator-and-recordvalueevaluatorkvpkey-valueseparator) and [`record.key/value.evaluator.kvp.pairs.separator`](#recordkeyevaluatorkvppairsseparator-and-recordvalueevaluatorkvppairsseparator).

This support for KVP adds to the versatility of the Kafka Connector, allowing it to handle a wide range of data formats efficiently.

#### `record.consume.from`

_Optional but ineffective when [`item.snapshot.enabled.mode`](#itemsnapshotenabledmode) is set to any value other than `NONE` — see [Snapshot management](#snapshot-management) for the partition-position behavior in that case_. Specifies where to start consuming events from. Can be one of the following:

- `LATEST`: Start consuming events from the end of the topic partition.
- `EARLIEST`: Start consuming events from the beginning of the topic partition.

How this parameter is applied depends on the configured [`consumer.mode`](#consumermode):

- In `GROUP` mode, it sets the value of the [`auto.offset.reset`](https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_auto.offset.reset) key on the internal Kafka Consumer, and therefore only takes effect for partitions that have no committed offset yet; partitions with a committed offset resume from there.
- In `MANUAL` mode, since offsets are never committed, the connector seeks every assigned partition to the requested position on every startup. The setting therefore applies uniformly to all assigned partitions on every restart.

Default value: `LATEST`.

Example:

```xml
<param name="record.consume.from">EARLIEST</param>
```

#### `record.consume.with.max.poll.records`

_Optional_. The maximum number of records fetched in each polling cycle.

The parameter sets the value of the [`max.poll.records`](https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_max.poll.records) key to configure the internal Kafka Consumer.

Default value: `500`.

Example:

```xml
<param name="record.consume.with.max.poll.records">200</param>
```

#### `record.consume.with.session.timeout.ms`

_Optional_. The timeout used to detect client failures when using Kafka's group management facility.

The parameter sets the value of the [session.timeout.ms](https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_session.timeout.ms) key to configure the internal Kafka Consumer.

Default value: `45000`.

```xml
<param name="record.consume.with.session.timeout.ms">30000</param>
```

#### `record.consume.with.max.poll.interval.ms`

_Optional_. The maximum delay between invocations of poll() when using consumer group management. This places an upper bound on the amount of time that the consumer can be idle before fetching more records.

The parameter sets the value of the [max.poll.interval.ms](https://kafka.apache.org/41/configuration/consumer-configs/#consumerconfigs_max.poll.interval.ms) key to configure the internal Kafka Consumer.

Default value: `30000`.

```xml
<param name="record.consume.with.max.poll.interval.ms">50000</param>
```

#### `record.consume.with.num.threads`

_Optional_. The number of threads to be used for concurrent processing of the incoming deserialized records. If set to `-1`, the number of threads will be automatically determined based on the number of available CPU cores.

Default value: `1`.

Example:

```xml
<param name="record.consume.with.num.threads">4</param>
```

#### `record.consume.with.order.strategy`

_Optional but only effective when [`record.consume.with.num.threads`](#recordconsumewithnumthreads) is set to a value greater than `1` (which includes the default value)_. The order strategy to be used for concurrent processing of the incoming deserialized records. Can be one of the following:

- `ORDER_BY_PARTITION`: Maintain the order of records within each partition.

   If you have multiple partitions, records from different partitions can be processed concurrently by different threads, but the order of records from a single partition will always be preserved. This is the default and generally a good balance between performance and order.

- `ORDER_BY_KEY`: Maintain the order among the records sharing the same key.

  Different keys can be processed concurrently by different threads. So, while all records with key "A" are processed in order, and all records with key "B" are processed in order, the processing of "A" and "B" records can happen concurrently and interleaved in time. There's no guaranteed order between records of different keys.

- `UNORDERED`: Provide no ordering guarantees.

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

_Mandatory if [evaluator type](#recordkeyevaluatortype-and-recordvalueevaluatortype) is set to `AVRO` or `PROTOBUF` and no [Schema Registry](#recordkeyevaluatorschemaregistryenable-and-recordvalueevaluatorschemaregistryenable) is enabled_. The path of the local schema (or binary descriptor) file relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector-<version>`) or as an absolute path, for message validation respectively of the key and the value.

When using Protobuf, a binary descriptor file is required. This binary file is generated from the source `.proto` file using the _[Protocol Buffer Compiler](https://grpc.io/docs/protoc-installation/)_ (`protoc`).

To generate the descriptor file, use the following command:

```sh
$ protoc --descriptor_set_out=record_value.proto.desc record_value.proto --include_imports
```

This command compiles the source file `record_value.proto` into the binary descriptor file `record_value.proto.desc`, which includes all imported proto definitions (via the `--include_imports` flag) required for proper message validation.

> [!NOTE]
> Using a binary descriptor file also requires specifying the [Protobuf message type](#recordkeyevaluatorprotobufmessagetype-and-recordvalueevaluatorprotobufmessagetype).

Examples:

```xml
<param name="record.key.evaluator.schema.path">schema/record_key.avsc</param>
<param name="record.value.evaluator.schema.path">schemas/record_value.avsc</param>
```

```xml
<param name="record.key.evaluator.schema.path">schema/record_key.proto.desc</param>
<param name="record.value.evaluator.schema.path">schemas/record_value.proto.desc</param>
```

#### `record.key.evaluator.protobuf.message.type` and `record.value.evaluator.protobuf.message.type`

_Mandatory when the [evaluator type](#recordkeyevaluatortype-and-recordvalueevaluatortype) is set to `PROTOBUF` and a binary descriptor file is provided through the [record.key/value.evaluator.schema.path](#recordkeyevaluatorschemapath-and-recordvalueevaluatorschemapath) parameters_. Specifies the name of the Protobuf message type to be used for deserializing the key and value of a Kafka record.

For example, if your `.proto` file contains:

```protobuf
syntax = "proto3";
package com.example.kafka;

message StockUpdate {
    string symbol = 1;
    double price = 2;
    // other fields...
}
```

Then the corresponding message type parameter should be:

```xml
<param name="record.value.evaluator.protobuf.message.type">StockUpdate</param>
```

#### `record.key.evaluator.schema.registry.enable` and `record.value.evaluator.schema.registry.enable`

_Mandatory when the [evaluator type](#recordkeyevaluatortype-and-recordvalueevaluatortype) is set to `AVRO` or `PROTOBUF` and no [local schema paths](#recordkeyevaluatorschemapath-and-recordvalueevaluatorschemapath) are provided_. Enable the use of a [Schema Registry](#schema-registry) for validation respectively of the key and the value. Can be one of the following:
- `true`
- `false`

Default value: `false`.

> [!IMPORTANT]
> When using the Azure Schema Registry, setting the evaluator type to `PROTOBUF` is not supported.

Examples:

```xml
<param name="record.key.evaluator.schema.registry.enable">true</param>
<param name="record.value.evaluator.schema.registry.enable">true</param>
```

#### `record.key.evaluator.kvp.key-value.separator` and `record.value.evaluator.kvp.key-value.separator`

_Optional but only effective when [`record.key/value.evaluator.type`](#recordkeyevaluatortype-and-recordvalueevaluatortype) is set to `KVP`_.
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

_Optional but only effective when [`record.key/value.evaluator.type`](#recordkeyevaluatortype-and-recordvalueevaluatortype) is set to `KVP`_.
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

_Optional but forced to `IGNORE_AND_CONTINUE` when [`item.snapshot.enabled.mode`](#itemsnapshotenabledmode) is set to any value other than `NONE`_. The error handling strategy to be used if an error occurs while [extracting data](#data-extraction-language) from incoming deserialized records. Can be one of the following:

- `IGNORE_AND_CONTINUE`: Ignore the error and continue to process the next record.
- `FORCE_UNSUBSCRIPTION`: Stop processing records and force unsubscription of the items requested by all the clients subscribed to this connection (see the [Client-side error handling](#client-side-error-handling) section).

See [Snapshot management](#snapshot-management) for the rationale of the override.

Default value: `IGNORE_AND_CONTINUE`.

Example:

```xml
<param name="record.extraction.error.strategy">FORCE_UNSUBSCRIPTION</param>
```

## Topic mapping

The Kafka Connector allows the configuration of several routing and mapping strategies, thus enabling the convey of Kafka events streams to a potentially huge amount of devices connected to Lightstreamer with great flexibility.

The _Data Extraction Language_ is the _ad hoc_ tool provided for in-depth analysis of Kafka records to extract data that can be used for the following purposes:
- Mapping records to Lightstreamer fields
- Filtering routing to the designated Lightstreamer items

### Data extraction language

To write an extraction expression, the _Data Extraction Language_ provides a pretty minimal syntax with the following basic rules:

- Expressions must be enclosed within `#{...}`
- Expressions use _Extraction Keys_, a set of predefined constants that reference specific parts of the record structure:

  - **`#{KEY}`**: the key
  - **`#{VALUE}`**: the value
  - **`#{TOPIC}`**: the topic
  - **`#{TIMESTAMP}`**: the timestamp
  - **`#{PARTITION}`**: the partition
  - **`#{OFFSET}`**: the offset
  - **`#{HEADERS}`**: the headers

- Expressions use the _dot notation_ to access nested data structures:

  - **Record data**: Navigate through attributes or fields in JSON, Avro, and Protobuf record values and keys.
  - **Headers**: Retrieve values from record headers.

  ```js
  KEY.attribute1Name.attribute2Name...
  VALUE.attribute1Name.attribute2Name...
  HEADERS.key
  ```

 > [!IMPORTANT]
 > Currently, it is required that the top-level element of either a record key or record value is:
 > - An [**object**](https://www.json.org/json-en.html), for JSON
 > - A [**Record**](https://avro.apache.org/docs/1.11.1/specification/#schema-record), for Avro
 > - A [**message**](https://protobuf.dev/programming-guides/proto3/), for Protobuf
 >
 > Such a constraint may be removed in a future version of the Kafka Connector.

- Expressions use the _square notation_ to access both indexed and key-based attributes:

  - **Indexed attributes:**

    ```js
    KEY.attribute1Name[i].attribute2Name...
    VALUE.attribute1Name[i].attribute2Name...
    HEADERS[i]
    ```

    where `i` is a 0-indexed value.

  - **Key-based attributes:**

    ```js
    KEY.attribute1Name['keyName'].attribute2Name...
    VALUE.attribute1Name['keyName'].attribute2Name...
    HEADERS['keyName']
    ```

    where `keyName` is a string value.

 > [!TIP]
 > Accessing a child attribute using either dot notation or square bracket notation is equivalent:
 >
 > ```js
 > VALUE.myProperty.myChild.childProperty
 > VALUE.myProperty['myChild'].childProperty
 > ```
 >
 > ```js
 > HEADERS.myKey
 > HEADERS.['myKey']
 > ```

- Expressions support **wildcards** to extract multiple values at once:

  - **`#{VALUE.*}`**: Extract all fields from the record value.
  - **`#{KEY.*}`**: Extract all fields from the record key.
  - **`#{HEADERS.*}`**: Extract all headers.
  - **`#{VALUE.nested.*}`**: Extract all elements from any nested non-scalar structure (objects or maps).
  - **`#{VALUE.items.*}`**: Extract all elements from an array.

  Wildcard expressions can be applied at any level to any non-scalar part of the record. For details on how wildcards are used in field mapping, see [Dynamic field discovery](#dynamic-field-discovery-field).

- Scalar vs Non-Scalar Value Extraction

  By default, expressions must evaluate to _scalar_ values. When extracted, these values are converted to strings before being sent to Lightstreamer clients. In particular, the binary header values undergo byte-to-string conversion using UTF-8 encoding.

  When an expression evaluates to a non-scalar value (object, array, or nested structure) and non-scalar mapping is not enabled, the connector will throw an extraction error, which is then processed according to the [`record.extraction.error.strategy`](#recordextractionerrorstrategy) setting.

  To allow complex data structures to be directly mapped to fields (preserving their structure as generic text), enable the [`fields.map.non.scalar.values`](#map-non-scalar-values-fieldsmapnonscalarvalues) parameter.

### Record routing (`map.TOPIC_NAME.to`)

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

#### Consume from specific partitions (`map.TOPIC_NAME.from.partitions`)

_Optional but only effective when [`consumer.mode`](#consumermode) is set to `MANUAL`_. Restrict this consumer's assignment for the topic `TOPIC_NAME` to a specific subset of partitions, instead of all partitions of the topic.

The value is a comma-separated list of non-negative partition numbers and inclusive ranges (whitespace around commas and hyphens is tolerated; duplicates and overlapping ranges are coalesced).

If omitted, all partitions of the topic are assigned.

Default value: _(unset)_.

Examples:

```xml
<param name="map.stocks.from.partitions">0,1,2,3</param>
```

```xml
<param name="map.stocks.from.partitions">0-3,4-6,9</param>
```

##### Partition-affinity sharding

The primary use case for `map.TOPIC_NAME.from.partitions` is **partition-affinity sharding across multiple connector instances**: each instance pins itself to a specific subset of partitions so the total set of partitions is deterministically split, without relying on Kafka's group coordinator.

For example, given a `stocks` topic with 8 partitions and two connector instances (Server A and Server B), you can shard consumption as follows:

**Server A** (partitions 0–3):

```xml
<param name="consumer.mode">MANUAL</param>
<param name="map.stocks.to">item-template.stock</param>
<param name="map.stocks.from.partitions">0-3</param>
```

**Server B** (partitions 4–7):

```xml
<param name="consumer.mode">MANUAL</param>
<param name="map.stocks.to">item-template.stock</param>
<param name="map.stocks.from.partitions">4-7</param>
```

Each server declaratively owns its slice of the topic. Because `MANUAL` mode also suppresses `group.id` and does not commit offsets, the two servers do not interfere with each other's position tracking.

#### Enable regular expression (`map.regex.enable`)

_Optional_. Enable the `TOPIC_NAME` part of the [`map.TOPIC_NAME.to`](#record-routing-maptopic_nameto) parameter to be treated as a regular expression rather than of a literal topic name.
This allows for more flexible routing, where messages from multiple topics matching a specific pattern can be directed to the same Lightstreamer item(s) or item template(s).
Can be one of the following:
- `true`
- `false`

Not supported when [`consumer.mode`](#consumermode) is set to `MANUAL`; the setting will be rejected at startup.

Default value: `false`.

Example:

```xml
<param name="map.topic_\d+.to">item</param>
<param name="map.regex.enable">true</param>
```

### Record mapping (`field.FIELD_NAME`)

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

The `QuickStart` [factory configuration](/kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml#L574) shows a basic example, where a simple _direct_ mapping has been defined between every attribute of the JSON record value and a Lightstreamer field with the corresponding name. Of course, thanks to the _Data Extraction Language_, more complex mapping can be employed.

```xml
...
<param name="field.timestamp">#{VALUE.timestamp}</param>
<param name="field.time">#{VALUE.time}</param>
<param name="field.name">#{VALUE.name}</param>
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

#### COMMAND mode field mapping

When the adapter operates in _COMMAND_ mode (enabled by setting [`item.snapshot.enabled.mode = COMMAND`](#itemsnapshotenabledmode); see also [COMMAND snapshot](#command-snapshot)), each Lightstreamer item is managed as a dynamic table whose rows are inserted, updated, and removed through `ADD`, `UPDATE`, and `DELETE` operations. The Lightstreamer Server requires two mandatory fields in the item's schema — `key` (the row identifier) and `command` (the operation) — and the way they are mapped is a special case of the general `field.FIELD_NAME` mechanism described above:

- **`key`** is mapped explicitly by the user through the `field.key` parameter, like any other field. It identifies the row each record refers to.
- **`command`** is **not** mapped: the connector synthesizes it for every record from the record state and the per-item key history:
  - **`ADD`** — the mapped key has not been seen before on this item.
  - **`UPDATE`** — the mapped key has already been seen on this item.
  - **`DELETE`** — the record has a null payload (_tombstone record_).

Any other field is mapped with the usual `field.FIELD_NAME` parameters.

Example mapping:

```xml
<param name="item-template.command">command-#{key=KEY}</param>
<param name="map.commandTopic.to">item-template.command</param>
<param name="field.key">#{KEY}</param>
```

> [!TIP]
> The `key` field can be mapped from any part of the Kafka record structure.

For a complete example of configuring _COMMAND_ mode, refer to the [examples/AirportDemo](/examples/airport-demo/) folder.

##### Manual COMMAND mapping (without connector-managed snapshot)

The synthesis above is tied to `item.snapshot.enabled.mode = COMMAND`. When snapshot management is left to its default (`NONE`), _COMMAND_-mode subscriptions are still supported: the integrator maps **both** `field.key` and `field.command` explicitly, and the connector forwards events as-is.

```xml
<param name="field.key">#{KEY}</param>
<param name="field.command">#{VALUE.op}</param>
```

> [!IMPORTANT]
> Both mappings are required: the Lightstreamer Server enforces `key` and `command` as the two mandatory fields of a _COMMAND_-mode item and **discards** any update in which either is missing or extracts to `null` (a `ERROR` log is emitted for each dropped update, but no error is surfaced to the client).

This route fits pipelines that already emit explicit `ADD`/`UPDATE`/`DELETE` op-codes (typical of CDC). Tombstone records cannot signal deletion (no `VALUE.op` to extract), and late subscribers see an empty table until realtime activity arrives. For the "latest state per key, deletion via tombstone, full row set on subscribe" shape, prefer `item.snapshot.enabled.mode = COMMAND`.

#### Dynamic field discovery (`field.*`)

Instead of explicitly naming each field, you can configure the connector to automatically discover field names from the record structure at runtime using wildcard expressions. This is particularly useful when:

- The record schema changes frequently and you want automatic adaptation.
- You have many fields and don't want to list them individually.
- You're working with schema-less or variable-structure records.

To enable dynamic field discovery, use wildcard expressions in your field configuration:

```xml
<param name="field.*">#{VALUE.*}</param>
```

This configuration automatically extracts all attributes from the record value and maps them to Lightstreamer fields with matching names. For example, a Kafka record with value:

```json
{
  "symbol": "AAPL",
  "price": 150.25,
  "volume": 1000000,
  "exchange": "NASDAQ"
}
```

will automatically create fields `symbol`, `price`, `volume`, and `exchange` without needing to explicitly configure each one.

**Supported wildcard patterns:**

Wildcards can be applied at any level to extract all fields from non-scalar structures:

- `#{VALUE.*}` - Discover all fields from the root record value.
- `#{KEY.*}` - Discover all fields from the root record key.
- `#{HEADERS.*}` - Discover all headers.
- `#{VALUE.nested.*}` - Discover all fields from a nested non-scalar structure (object or map).
- `#{VALUE.items.*}` - Discover all elements from an array.

**How wildcards map to field names:**

Wildcards can be applied at any level to any non-scalar part of the record. The resulting field names depend on the structure type:

- **Objects and maps**: Extract all fields/entries, using object property names or map keys as field names.
- **Arrays**: Extract all elements with indexed field names (e.g., `array_name[0]`, `array_name[1]`).

Dynamic field discovery automatically handles both scalar and non-scalar values. Non-scalar values (objects, arrays, nested structures) are serialized as generic text (e.g., JSON strings) and mapped to their corresponding field names.

For example, with nested objects or maps:

```json
{
  "trade": {
    "symbol": "AAPL",
    "details": {
      "price": 150.25,
      "volume": 1000000,
      "exchange": "NASDAQ"
    }
  }
}
```

You can extract all fields from the nested `details` structure with:

```xml
<param name="field.*">#{VALUE.trade.details.*}</param>
```

This creates fields `price`, `volume`, and `exchange` from the nested object. The same wildcard pattern works for maps, where the map keys become field names.

For arrays, the wildcard discovers all elements with indexed field names:

```json
{
  "prices": [150.25, 155.50, 148.75, 152.30]
}
```

```xml
<param name="field.*">#{VALUE.prices.*}</param>
```

This creates fields `prices[0]`, `prices[1]`, `prices[2]`, and `prices[3]` containing the individual price values.

You can also combine static field mapping with dynamic discovery:

```xml
<!-- Explicitly map metadata fields -->
<param name="field.timestamp">#{TIMESTAMP}</param>
<param name="field.partition">#{PARTITION}</param>

<!-- Discover all value fields automatically -->
<param name="field.*">#{VALUE.*}</param>
```

When combining both approaches, static field mappings take precedence over dynamic discovery. If a field name is explicitly defined with `field.fieldName`, it will not be overridden by the wildcard `field.*` pattern.

> [!NOTE]
> The `field.*` configuration parameter name is static (defined at configuration time), while the wildcard expression `#{VALUE.*}` dynamically discovers field names at runtime from the actual record content.

> [!IMPORTANT]
> Wildcard expressions can only be used with the `field.*` parameter. They cannot be used in [item templates](#filtered-record-routing-item-templatetemplate_name) or for explicit field mappings like `field.fieldName`.

#### Skip failed mapping (`fields.skip.failed.mapping.enable`)

_Optional_. Normally, if a field mapping fails during the extraction from the Kafka record because of an issue with the data, it leads to the entire record being discarded or even cause the subscription to be terminated, depending on the [`record.extraction.error.strategy`](#recordextractionerrorstrategy) setting. By enabling this parameter, the connector becomes more resilient to such errors. If a field mapping fails, that specific field's value will simply be omitted from the update sent to Lightstreamer clients, while other successfully mapped fields from the same record will still be delivered. This allows for partial updates even in the presence of data inconsistencies or transient extraction issues.

Can be one of the following:
- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="fields.skip.failed.mapping.enable">true</param>
```

#### Map non-scalar values (`fields.map.non.scalar.values`)

_Optional_. Enabling this parameter allows mapping of non-scalar values to Lightstreamer fields.
This means that complex data structures from Kafka records can be mapped directly to Lightstreamer fields without requiring them to be flattened into scalar values.
This can be useful when dealing with nested JSON/Avro/Protobuf objects or other complex data types.

In the following example:

```xml
<param name="field.structured">#{VALUE.complexAttribute}</param>
```

the value of `complexAttribute` will be mapped as generic text (e.g. JSON string) to the `structured` Lightstreamer field, preserving its structure and allowing clients to parse and use the data as needed.

Can be one of the following:
- `true`
- `false`

Default value: `false`.

> [!NOTE]
> This parameter applies only to static field mappings (`field.fieldName`). When using [dynamic field discovery](#dynamic-field-discovery-field) (`field.*`), non-scalar values are always mapped automatically.

Example:

```xml
<param name="fields.map.non.scalar.values">true</param>
```

### Filtered record routing (`item-template.TEMPLATE_NAME`)

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
ITEM_PREFIX-[paramName1=filterValue_1,paramName2=filterValue_2,...]
```

Upon consuming a message, the Kafka Connector _expands_ every item template addressed by the record topic by evaluating each extraction expression and binding the extracted value to the associated parameter. The expanded template will result as:

```js
ITEM_PREFIX-[paramName1=extractedValue_1,paramName2=extractedValue_2,...]
```

Finally, the message will be mapped and routed only in case the subscribed item completely matches the expanded template or, more formally, the following is true:

`filterValue_X == extractValue_X for every paramName_X`

#### Example 1

Consider the following configuration:

```xml
<param name="item-template.currencyPair">pair-#{symbol=KEY}</param>
<param name="map.forex.to">item-template.currencyPair</param>
```

which specifies how to route records from the topic `forex`, whose Kafka key is the currency-pair symbol (e.g. `EURUSD`, `GBPUSD`, `USDJPY`), to the item template `currencyPair`. The template binds the single parameter `symbol` to the Kafka key, so each subscribed item targets exactly one currency pair.

Let's suppose we have two different Lightstreamer clients:

1. _Client A_ subscribes to two parameterized items:
   - _SA1_ `pair-[symbol=EURUSD]` for receiving real-time updates relative to the `EUR/USD` pair.
   - _SA2_ `pair-[symbol=EURGBP]` for receiving real-time updates relative to the `EUR/GBP` pair.
2. _Client B_ subscribes to the parameterized item _SB1_ `pair-[symbol=USDJPY]` for receiving real-time updates relative to the `USD/JPY` pair.

Now, let's see how filtered routing works for the following incoming Kafka records from the topic `forex`:

- **Record 1** — key `EURUSD`:

  | Expansion              | Matched Subscribed Item | Routed to Client |
  | ---------------------- | ----------------------- | ---------------- |
  | `pair-[symbol=EURUSD]` | _SA1_                   | _Client A_       |

- **Record 2** — key `USDJPY`:

  | Expansion              | Matched Subscribed Item | Routed to Client |
  | ---------------------- | ----------------------- | ---------------- |
  | `pair-[symbol=USDJPY]` | _SB1_                   | _Client B_       |

- **Record 3** — key `GBPUSD`:

  | Expansion              | Matched Subscribed Item | Routed to Client |
  | ---------------------- | ----------------------- | ---------------- |
  | `pair-[symbol=GBPUSD]` | _None_                  | _None_           |

- **Record 4** — key `EURGBP`:

  | Expansion              | Matched Subscribed Item | Routed to Client |
  | ---------------------- | ----------------------- | ---------------- |
  | `pair-[symbol=EURGBP]` | _SA2_                   | _Client A_       |

#### Example 2

Consider the following configuration:

```xml
<param name="item-template.by-name">user-#{firstName=VALUE.name,lastName=VALUE.surname}</param>
<param name="item-template.by-age">user-#{age=VALUE.age}</param>
<param name="map.user.to">item-template.by-name,item-template.by-age</param>
```

which specifies how to route records from the topic `user` to the item templates defined to extract some personal data. The two templates bind different parameters extracted from the record value: `by-name` binds `firstName` and `lastName` to `VALUE.name` and `VALUE.surname`, while `by-age` binds `age` to `VALUE.age`. Because the topic maps to both templates, every record is evaluated against both and may match items on either or both axes.

Let's suppose we have three different Lightstreamer clients:

1. _Client A_ subscribes to the following parameterized items:
   - _SA1_ `user-[firstName=James,lastName=Kirk]` for receiving real-time updates relative to the user `James Kirk`.
   - _SA2_ `user-[age=45]` for receiving real-time updates relative to any 45 year-old user.
2. _Client B_ subscribes to the parameterized item _SB1_ `user-[firstName=Montgomery,lastName=Scotty]` for receiving real-time updates relative to the user `Montgomery Scotty`.
3. _Client C_ subscribes to the parameterized item _SC1_ `user-[age=37]` for receiving real-time updates relative to any 37 year-old user.

Now, let's see how filtered routing works for the following incoming Kafka records from the topic `user`:

- **Record 1**:
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
  | --------- | -------------------------------------- | ----------------------- | ---------------- |
  | `by-name` | `user-[firstName=James,lastName=Kirk]` | _SA1_                   | _Client A_       |
  | `by-age`  | `user-[age=37]`                        | _SC1_                   | _Client C_       |

- **Record 2**:
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
  | --------- | --------------------------------------------- | ----------------------- | ---------------- |
  | `by-name` | `user-[firstName=Montgomery,lastName=Scotty]` | _SB1_                   | _Client B_       |
  | `by-age`  | `user-[age=45]`                               | _SA2_                   | _Client A_       |

- **Record 3**:
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
  | --------- | --------------------------------------- | ----------------------- | ---------------- |
  | `by-name` | `user-[firstName=Nyota,lastName=Uhura]` | _None_                  | _None_           |
  | `by-age`  | `user-[age=37]`                         | _SC1_                   | _Client C_       |

## Item snapshot settings

Parameters that control whether the connector manages the _snapshot_ of subscribed items and how that snapshot is shaped. For the underlying concepts (what the snapshot is, what changes when snapshot management is activated, per-_Mode_ snapshot shape and intended use cases), see the [Snapshot management](#snapshot-management) section.

### `item.snapshot.enabled.mode`

_Optional_. Selects the snapshot behavior for subscribed items and, when not set to `NONE`, pins the Lightstreamer subscription _Mode_ the connector is willing to serve. Can be one of the following:

- **`NONE`**: Snapshot management disabled. Lazy consumer, empty snapshot, subscription _Mode_ not constrained by the adapter.
- **`MERGE`**: Pins subscription _Mode_ to _MERGE_. See [MERGE snapshot](#merge-snapshot).
- **`DISTINCT`**: Pins subscription _Mode_ to _DISTINCT_. Bounded by [`item.snapshot.distinct.length`](#itemsnapshotdistinctlength). See [DISTINCT snapshot](#distinct-snapshot).
- **`COMMAND`**: Pins subscription _Mode_ to _COMMAND_. The connector synthesizes the `command` field from each record (`ADD` on first sight, `UPDATE` afterwards, `DELETE` for tombstones); you only map `field.key`. See [COMMAND snapshot](#command-snapshot) and [COMMAND mode field mapping](#command-mode-field-mapping).

Any non-`NONE` value also forces [`record.extraction.error.strategy`](#recordextractionerrorstrategy) to `IGNORE_AND_CONTINUE`, overriding the configured value.

Default value: `NONE`.

Example:

```xml
<param name="item.snapshot.enabled.mode">MERGE</param>
```

### `item.snapshot.distinct.length`

_Optional but only effective when [`item.snapshot.enabled.mode`](#itemsnapshotenabledmode) is set to `DISTINCT`_. The maximum allowed length for the snapshot of an item that has been requested with publishing _Mode_ _DISTINCT_. Must be a positive integer.

Default value: `10`.

Example:

```xml
<param name="item.snapshot.distinct.length">100</param>
```

### `item.snapshot.max.idle.seconds`

_Optional but only effective when [`item.snapshot.enabled.mode`](#itemsnapshotenabledmode) is set to any  value other than `NONE`_. The maximum idle time in seconds after which the snapshot of an item is discarded, so that the next incoming record starts a fresh one. Must be a non-negative integer; a value of `0` disables the idle check.

Default value: `0`.

Example:

```xml
<param name="item.snapshot.max.idle.seconds">30</param>
```

## Schema Registry

A _Schema Registry_ is a centralized repository that manages and validates schemas, which define the structure of valid messages.

The Kafka Connector supports integration with the following Schema Registry providers:

- [_Confluent Schema Registry_](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [_Azure Schema Registry_](https://learn.microsoft.com/en-us/azure/event-hubs/schema-registry-overview)

Configuration is done through parameters with the prefix `schema.registry`. The required settings depend on the chosen provider.

### `schema.registry.provider`

_Optional_. Specifies the Schema Registry provider to use. Can be one of the following:
- `CONFLUENT`: Use the Confluent Schema Registry.
- `AZURE`: Use the Azure Schema Registry.

Default value: `CONFLUENT`.

Example:

```xml
<param name="schema.registry.provider">AZURE</param>
```

### `schema.registry.url`

_Mandatory if a [Schema Registry](#recordkeyevaluatorschemaregistryenable-and-recordvalueevaluatorschemaregistryenable) is enabled_. The URL of the Schema Registry endpoint (either Confluent Schema Registry or Azure Schema Registry).

Example for the Confluent Schema Registry:

```xml
<param name="schema.registry.url">https://schema-registry:8084</param>
```

Example for the Azure Schema Registry (the URL must point to the Azure Event Hubs namespace):

```xml
<param name="schema.registry.url">https://my-namespace.servicebus.windows.net</param>
```

### Confluent Schema Registry parameters

When using Confluent Schema Registry ([`schema.registry.provider`](#schemaregistryprovider) set to `CONFLUENT`), the following parameters could be configured to enable authentication and integration with Azure Event Hubs.

#### Basic HTTP authentication parameters

[Basic HTTP authentication](https://docs.confluent.io/platform/current/schema-registry/security/index.html#configuring-the-rest-api-for-basic-http-authentication) mechanism is supported through the configuration of parameters with the prefix `schema.registry.confluent.basic.authentication`.

- `schema.registry.confluent.basic.authentication.enable`

  _Optional_. Enable Basic HTTP authentication of this connection against the Schema Registry. Can be one of the following:
  - `true`
  - `false`

  Default value: `false`.

  Example:

  ```xml
  <param name="schema.registry.confluent.basic.authentication.enable">true</param>
  ```

- `schema.registry.confluent.basic.authentication.username` and `schema.registry.confluent.basic.authentication.password`

  _Mandatory if [Basic HTTP Authentication](#basic-http-authentication-parameters) is enabled_. The credentials.

  - `schema.registry.confluent.basic.authentication.username`: the username
  - `schema.registry.confluent.basic.authentication.password`: the password

  Example:

  ```xml
  <param name="schema.registry.confluent.basic.authentication.username">authorized-schema-registry-user</param>
  <param name="schema.registry.confluent.basic.authentication.password">authorized-schema-registry-user-password</param>
  ```

#### Encryption parameters

To set up a secure connection to the Schema Registry, specify the `https` protocol in the [`schema.registry.url`](#schemaregistryurl) setting and configure the connection using parameters with the prefix `schema.registry.confluent.encryption`. These parameters are equivalent to those defined in the [Encryption parameters](#encryption-parameters) section:

- `schema.registry.confluent.encryption.protocol` (see [encryption.protocol](#encryptionprotocol))
- `schema.registry.confluent.encryption.enabled.protocols` (see [encryption.enabled.protocols](#encryptionenabledprotocols))
- `schema.registry.confluent.encryption.cipher.suites` (see [encryption.cipher.suites](#encryptionciphersuites))
- `schema.registry.confluent.encryption.truststore.path` (see [encryption.truststore.path](#encryptiontruststorepath))
- `schema.registry.confluent.encryption.truststore.type` (see [encryption.truststore.type](#encryptiontruststoretype))
- `schema.registry.confluent.encryption.truststore.password` (see [encryption.truststore.password](#encryptiontruststorepassword))
- `schema.registry.confluent.encryption.hostname.verification.enable` (see [encryption.hostname.verification.enable](#encryptionhostnameverificationenable))
- `schema.registry.confluent.encryption.keystore.enable` (see [encryption.keystore.enable](#encryptionkeystoreenable))
- `schema.registry.confluent.encryption.keystore.path` (see [encryption.keystore.path](#encryptionkeystorepath))
- `schema.registry.confluent.encryption.keystore.type` (see [encryption.keystore.type](#encryptionkeystoretype))
- `schema.registry.confluent.encryption.keystore.password` (see [encryption.keystore.password](#encryptionkeystorepassword))
- `schema.registry.confluent.encryption.keystore.key.password` (see [encryption.keystore.key.password](#encryptionkeystorekeypassword))

Example:

```xml
<!-- Set the Confluent Schema Registry URL. The https protocol enables encryption parameters -->
<param name="schema.registry.url">https//localhost:8084</param>

<!-- Set general encryption settings -->
<param name="schema.registry.confluent.encryption.enabled.protocols">TLSv1.3</param>
<param name="schema.registry.confluent.encryption.cipher.suites">TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA</param>
<param name="schema.registry.confluent.encryption.hostname.verification.enable">true</param>

<!-- If required, configure the trust store to trust the Confluent Schema Registry certificates -->
<param name="schema.registry.confluent.encryption.truststore.path">secrets/kafka-connector.truststore.jks</param>
<param name="schema.registry.confluent.encryption.truststore.password">kafka-connector-truststore-password</param>

<!-- If mutual TLS is enabled on the Confluent Schema Registry, enable and configure the key store -->
<param name="schema.registry.confluent.encryption.keystore.enable">true</param>
<param name="schema.registry.confluent.encryption.keystore.path">secrets/kafka-connector.keystore.jks</param>
<param name="schema.registry.confluent.encryption.keystore.password">kafka-connector-password</param>
<param name="schema.registry.confluent.encryption.keystore.key.password">kafka-connector-private-key-password</param>
```

#### Confluent Schema Registry quickstart

For an example of Schema Registry settings, see the [adapters.xml](/examples/quickstart-schema-registry/adapters.xml#L58) file of the [_Schema Registry Quickstart_](/examples/quickstart-schema-registry/) app.

### Azure Schema Registry parameters

When using the Azure Schema Registry ([`schema.registry.provider`](#schemaregistryprovider) set to `AZURE`), authentication must be configured through the following parameters:

- `schema.registry.azure.client.id`

  _Mandatory_. The Application (client) ID assigned to the application registered in Microsoft Entra ID with appropriate permissions to access the Schema Registry.

  Example:

  ```xml
  <param name="schema.registry.azure.client.id">87654321-4321-4321-4321-cba987654321</param>
  ```

- `schema.registry.azure.tenant.id`

  _Mandatory_. The Directory (tenant) ID of the Microsoft Entra ID tenant where the application is registered.

  Example:

  ```xml
  <param name="schema.registry.azure.tenant.id">12345678-1234-1234-1234-123456789abc</param>
  ```

- `schema.registry.azure.client.secret`

  _Mandatory_. The client secret value of the application registered in Microsoft Entra ID.

  Example:

  ```xml
  <param name="schema.registry.azure.client.secret">your-azure-client-secret</param>
  ```

See the [Advanced: Schema Registry Integration](/examples/vendors/azure/quickstart-azure/README.md#advanced-schema-registry-integration) section of the _Azure Event Hubs Quickstart_ example for a complete walkthrough on how to register an Azure AD application, grant it access to the Schema Registry, and configure all the parameters above.

# Subscription modes

A Lightstreamer client subscribes to an item by choosing a **subscription _Mode_**, which dictates what each item represents on the wire and how the Lightstreamer Broker stores incoming updates per item. The Server supports four Modes; the brief recap below covers what is needed to follow the Kafka Connector documentation. For the authoritative reference, see the _General Concepts_ guide shipped with the Lightstreamer Broker (`LS_HOME/docs/General Concepts.pdf`).

- **_MERGE_** — the item represents a **single logical entity** whose fields are progressively overwritten by incoming updates. The Server keeps only the latest value of each field. Suitable for current-state feeds (latest quote, latest sensor reading, latest order status).
- **_DISTINCT_** — the item represents a **stream of independent events** that must not be merged: every update is preserved as a separate event on the client side. The Server retains a bounded FIFO of the most recent events per item. Suitable for time series of discrete events (trades, log lines, alerts).
- **_COMMAND_** — the item represents a **dynamic table** whose rows are inserted, updated, and removed through `ADD`, `UPDATE`, and `DELETE` operations. Every update carries two mandatory fields, `key` (the row identifier) and `command` (the operation); the Server applies each operation to a per-item, key-addressed row set. Suitable for changelogs of a keyed entity set (positions in a portfolio, online users, items in a cart).
- **_RAW_** — the item is treated as a pure pass-through: the Server forwards every update without keeping any per-item state. Always compatible with the other Modes on the same item.

The Lightstreamer Broker allows each item to be handled in only one of _MERGE_, _DISTINCT_, or _COMMAND_ at a time (plus _RAW_, which is always compatible): the first subscription request for an item effectively pins its Mode, and subsequent requests for a conflicting Mode are silently ignored.

The Mode also determines how the Server materializes the _snapshot_ delivered to a freshly subscribed client — see [Snapshot management](#snapshot-management). The Kafka Connector can either leave the choice of Mode entirely to the client (the default) or pin it from the adapter side as a side effect of enabling connector-managed snapshot.

# Snapshot management

In Lightstreamer terminology, the _snapshot_ of an item is the set of events a freshly subscribed client receives **before** realtime updates start to flow, so that the client can render a meaningful initial state without having to wait for the next published event. The exact shape and size of this initial set depend on the subscription _Mode_ chosen for the item.

Snapshot behavior is controlled by [`item.snapshot.enabled.mode`](#itemsnapshotenabledmode), which selects one of four modes:

- **`NONE`** — snapshot management disabled; the Lightstreamer Server still applies its own automatic snapshot mechanism. See [Default Behavior (NONE)](#default-behavior-none).
- **`MERGE`**, **`DISTINCT`**, **`COMMAND`** — the connector takes responsibility for materializing the snapshot: it replays the topic from the beginning, pre-seeds a per-item store on the Lightstreamer Server, and serves that store as the snapshot to late subscribers. See [Connector-managed snapshot](#connector-managed-snapshot).

A non-`NONE` value also pins the subscription _Mode_ the adapter is willing to serve and forces [`record.extraction.error.strategy`](#recordextractionerrorstrategy) to `IGNORE_AND_CONTINUE`; the rest of this section covers those side effects, the per-_Mode_ snapshot shape, the extraction layouts that make the snapshot exact, and the operational notes (idle expiration, caveats). For the parameter reference (valid values, defaults, XML examples) see [Item snapshot settings](#item-snapshot-settings).

## Default behavior (NONE)

This is the out-of-the-box behavior, selected by `item.snapshot.enabled.mode = NONE`: the connector does not actively manage the snapshot, but the Lightstreamer Server's built-in snapshot machinery still applies.

- **Lazy consumer.** The internal Kafka Consumer is started on the first client subscription, and every record fetched from Kafka is delivered as a realtime update.
- **Snapshot reflects Server state only.** The connector does not pre-seed any per-item store on the Lightstreamer Server, so the snapshot a new subscriber receives reflects only the current state the Server has accumulated for that item from prior realtime activity — typically empty for the very first subscriber (before any record has been forwarded), but possibly non-empty for later subscribers, depending on what the Server's per-_Mode_ store has retained.
- **Subscription _Mode_.** The adapter does not constrain the _Mode_; each client picks the _Mode_ it wants when it subscribes, within the per-item one-Mode-at-a-time rule recalled in [Subscription modes](#subscription-modes).

## Connector-managed snapshot

When `item.snapshot.enabled.mode` is set to any value other than `NONE`, the connector takes responsibility for materializing and serving the snapshot:

- The internal Kafka Consumer is started _eagerly_ when the Data Adapter binds to the Lightstreamer Server, before any client is allowed to subscribe (clients can only connect — and therefore subscribe — once initialization has completed).
- The connector manages partition positions explicitly, bypassing [`record.consume.from`](#recordconsumefrom): newly assigned partitions are always seeked to the beginning so that the replay covers the full topic history and pre-seeds the per-item store maintained by the Lightstreamer Server, while re-assigned partitions resume from their committed offset.
- Once the historical replay is caught up to the partition end, the consumer transitions to realtime tailing; from that moment on, every new record updates the Server-side store.
- A subscriber that joins later receives the current contents of the per-item store as the snapshot, followed by realtime updates.

Because the per-item store backs every future snapshot and the consumer runs even when no client is subscribed, the [`record.extraction.error.strategy`](#recordextractionerrorstrategy) setting is forced to `IGNORE_AND_CONTINUE` under this mode: `FORCE_UNSUBSCRIPTION` would either have nothing to unsubscribe (during the initial replay) or, on a single bad record during realtime tailing, would tear down the per-item store and permanently break snapshot delivery for every future subscription on this connection.

The chosen value of [`item.snapshot.enabled.mode`](#itemsnapshotenabledmode) also _pins_ the Lightstreamer subscription _Mode_ the adapter is willing to serve for the affected items: a client requesting a different _Mode_ will be refused. The pairing is one-to-one:

| `item.snapshot.enabled.mode` | Subscription _Mode_ pinned by the adapter | Snapshot shape                                                              |
| ---------------------------- | ----------------------------------------- | --------------------------------------------------------------------------- |
| `MERGE`                      | _MERGE_                                   | One event per item (the current value)                                      |
| `DISTINCT`                   | _DISTINCT_                                | Up to [`item.snapshot.distinct.length`](#itemsnapshotdistinctlength) events |
| `COMMAND`                    | _COMMAND_                                 | All rows currently in the per-item table                                    |

**Snapshot correctness.** For the snapshot to be exact, **each Kafka record key must map deterministically to a well-defined snapshot entry per matched item**, where the entry is:

- the item entry itself in _MERGE_;
- an entry of the per-item FIFO in _DISTINCT_;
- a row inside the item's table in _COMMAND_ (rows are addressed by `field.key`).

When one record fans out to multiple items (multiple templates, or `template1,template2` in `map.X.to`), each item receives its own entry under the same rule. When that mapping holds, the snapshot a late subscriber receives is exactly what Kafka still retains on disk. The per-_Mode_ sections below spell out the **extraction layout** and **topic settings** that achieve that mapping for each Mode, together with the runtime behavior of less suitable configurations.

### MERGE snapshot

In _MERGE_ mode each Lightstreamer item represents a **single logical entity** whose fields are progressively overwritten by incoming updates. Accordingly, the snapshot of a _MERGE_ item is a **single event** carrying the most recent value of every mapped field.

With `item.snapshot.enabled.mode = MERGE`:

- The per-item store is a single entry holding the latest value seen for the item.
- A new subscriber receives exactly one snapshot event per item, reflecting that latest value.

#### Recommended extraction layout

Use an item-template whose bind parameters cover the **full Kafka key** with `KEY` / `KEY.*` extractions only. Each distinct Kafka key then maps to a distinct item, and the per-item store ends up holding exactly the latest value per Kafka key.

#### Example

Consider a market data feed that publishes the **current quote** for each listed stock — last price, best bid and ask, volume of the day — with each Kafka record identified by the `(symbol, exchange)` pair that uniquely names the instrument on its trading venue. New quotes for `AAPL` on `NASDAQ` overwrite previous quotes for the same instrument, and a client that opens the page mid-session expects to see the latest snapshot per instrument it is interested in, not the full intraday tape.

Given a topic keyed by `{symbol, exchange}`:

```xml
<param name="item-template.stock">stock-#{symbol=KEY.symbol,exchange=KEY.exchange}</param>
```

A record with key `{symbol=AAPL, exchange=NASDAQ}` routes to `stock-[symbol=AAPL,exchange=NASDAQ]`. The two `KEY.*` extractions together reconstruct the full Kafka key, so every distinct Kafka key produces a distinct item; a late subscriber receives exactly the latest value per `(symbol, exchange)` pair it subscribes to.

#### Other layouts

Accepted at startup but degrade the snapshot:

| Item-name shape | Example | Snapshot correctness | What happens |
| --- | --- | --- | --- |
| Item-name covers the full Kafka key with `KEY` / `KEY.*` | `stock-#{symbol=KEY}` against a string-keyed topic; `stock-#{symbol=KEY.symbol,exchange=KEY.exchange}` against `{symbol, exchange}` keys | **Correct** — the recommended shape | Each distinct Kafka key produces exactly one item, and the snapshot is exact. |
| Covers only a subset of the key | `stock-#{symbol=KEY.symbol}` against `{symbol, exchange}` keys | **Consistent but ordering-sensitive** | Two records with different Kafka keys can map to the same item. The snapshot replay delivers both, and the client sees the second value overwrite the first; the final value depends on poll/partition order. The same non-determinism is visible on the realtime path. |
| Includes any `VALUE.*` extraction | `stock-#{symbol=VALUE.symbol}`; `item-#{id=KEY.id,status=VALUE.status}` | **Broken** / **silently wrong** | Because the Kafka key carries no information about the item name, many Kafka keys collapse onto a single item with no bound. If the item name mutates with the value (mixed `KEY.*` / `VALUE.*`), each value change routes to a different item, so the originally addressed item stops receiving updates — and under log compaction, the matching record can be wiped entirely, leaving the item silently empty. |
| Plain item name, no bind parameters | `<param name="map.stocks.to">all-stocks</param>` | **Degenerate** | Every record collapses onto the same item; the last record polled wins. Avoid for _MERGE_. |

#### Recommended topic shape

A [**log-compacted**](https://kafka.apache.org/documentation/#compaction) topic (`cleanup.policy=compact`) is the natural source: only the latest record per key survives, which is exactly what the connector reconstructs as the snapshot, and the replay cost stays bounded. A non-compacted topic still produces a correct snapshot but scans every retained record — startup time and consumer load grow with the topic size.

This is the appropriate choice when the topic models the **current state** of an entity (for example: latest stock quote, latest sensor reading, latest order status) and clients only care about the most recent value plus the realtime stream of changes.

### DISTINCT snapshot

In _DISTINCT_ mode each Lightstreamer item represents a **stream of independent events** that must not be merged: every event is preserved as a separate update on the client side. Accordingly, the snapshot of a _DISTINCT_ item is a **bounded sequence** of the most recent events delivered on that item.

With `item.snapshot.enabled.mode = DISTINCT`:

- The per-item store is a FIFO of the most recent events, bounded by [`item.snapshot.distinct.length`](#itemsnapshotdistinctlength) (default `10`); the connector does not buffer or truncate on its side.
- A new subscriber receives up to `item.snapshot.distinct.length` snapshot events per item, in the original publish order.

#### Recommended extraction layout

Same as _MERGE_ — an item-template whose binds cover the full Kafka key with `KEY` / `KEY.*` only. Because the per-item store is a bounded FIFO rather than a single current value, partial-key coverage degrades into a merged window across keys rather than into overwrite races, but a full-key layout is still the only shape with a "per Kafka key" semantics.

#### Example

Consider a **trade-execution feed** that publishes one event per fill: every record represents a single trade printed on the tape — price, size, aggressor side, execution timestamp — and each fill is identified by the `(symbol, exchange)` pair that names the traded instrument on its venue. Unlike a quote stream, consecutive trades on the same instrument must not be merged: each one is an independent event a client may want to render as a separate row on a recent-trades blotter, alongside the realtime tape.

Given a topic carrying a stream of trade events keyed by `{symbol, exchange}`:

```xml
<param name="item-template.trades">trades-#{symbol=KEY.symbol,exchange=KEY.exchange}</param>
```

Each distinct Kafka key routes to a distinct item; a late subscriber to `trades-[symbol=AAPL,exchange=NASDAQ]` receives the most recent N trades on that instrument, in the order they were printed.

#### Other layouts

| Item-name shape | Example | Snapshot correctness | What happens |
| --- | --- | --- | --- |
| Item-name covers the full Kafka key with `KEY` / `KEY.*` | `trades-#{symbol=KEY.symbol,exchange=KEY.exchange}` against `{symbol, exchange}` keys | **Correct** — the recommended shape | Each Kafka key produces a distinct item; the per-item FIFO holds the last `item.snapshot.distinct.length` events for that key. |
| Covers only a subset of the key | `trades-#{symbol=KEY.symbol}` against `{symbol, exchange}` keys | **Acceptable but interleaved** | Records from different Kafka keys land on the same item and share a single FIFO. The bound still applies, so the snapshot is not corrupted — it is just a merged window across keys. |
| Includes any `VALUE.*` extraction | `trades-#{symbol=VALUE.symbol}` | **Discouraged** | Same fan-out as _MERGE_: many keys collapse onto one item. The FIFO bound keeps the snapshot sized, but its contents become a value-driven window that does not correspond to any "per Kafka key" notion. |
| Plain item name, no bind parameters | `<param name="map.trades.to">all-trades</param>` | **Global window** | Every record in the topic feeds the same item; the snapshot is the last N events on the whole topic. Valid by construction (the FIFO bound caps it) but rarely the intended semantics. |

#### Recommended topic shape

Because the per-item snapshot is a bounded sequence of recent events rather than a single current value, log compaction is not required: the Lightstreamer Server caps the snapshot at `item.snapshot.distinct.length` regardless of how many records the replay surfaces. A **time- or size-bounded retention policy** sized to cover `item.snapshot.distinct.length` events per item is the natural fit; log compaction is also accepted, since the Server-side cap makes the snapshot shape independent of the topic shape.

This is the appropriate choice when the topic carries a **time series of discrete events** (for example: trades, log lines, alerts) and clients need a short window of recent history alongside the realtime feed.

### COMMAND snapshot

In _COMMAND_ mode each Lightstreamer item represents a **dynamic table**: rows are inserted, updated, and removed through `ADD`, `UPDATE`, and `DELETE` operations identified by a per-row `key`. Accordingly, the snapshot of a _COMMAND_ item is the **full set of rows** currently present in the table.

With `item.snapshot.enabled.mode = COMMAND`:

- The per-item store is a row set keyed by the `field.key` value mapped from each record.
- Only the `key` Lightstreamer field is mapped explicitly (via `field.key`); the connector synthesizes the `command` field for every record from the record state — `ADD` the first time a key is seen on an item, `UPDATE` afterwards, `DELETE` for tombstones (records with a null payload). The Server applies each synthesized operation to the per-item row set, reconstructing the current table.
- A new subscriber receives the **resulting** table state as the snapshot: one event per row currently present, each carrying `command = ADD` (the replay sequence of `ADD`/`UPDATE`/`DELETE` operations collapses into the final set of surviving rows).

#### Recommended extraction layout

Snapshot identity is `(item, row)`, with the row addressed by `field.key`. The **union** of the item-template binds and `field.key` must cover the full Kafka key, with every extraction sourced from `KEY` / `KEY.*` and `field.key` resolving to a scalar.

#### Example

Consider a **brokerage backend** that publishes the **open positions** of each customer account — for every `(account, instrument)` pair the broker holds, a record carries the current quantity, average cost, and realized/unrealized P&L; a tombstone is emitted when a position is fully closed. A client opening the *Portfolio* page for an account expects to see, in one shot, the full set of instruments currently held in that account, and then receive realtime row inserts, updates, and removals as the trader works the book.

Two shapes satisfy the snapshot-identity rule, depending on whether the Kafka key is structured or scalar:

1. **Table per group** — split the structured Kafka key between the item name (the grouping axis, `account`) and `field.key` (the row axis, `instrument`). For example, given a topic keyed by `{account, instrument}`:

   ```xml
   <param name="item-template.positions">positions-#{account=KEY.account}</param>
   <param name="field.key">#{KEY.instrument}</param>
   ```

   A record with key `{account=A1, instrument=AAPL}` routes to item `positions-[account=A1]` and addresses row `AAPL` inside that item's table. The union `KEY.account ∪ KEY.instrument` covers the full Kafka key, so distinct Kafka keys land on distinct `(item, row)` pairs and ADD/UPDATE/DELETE on `(positions-[account=A1], AAPL)` always refer to the same Kafka record stream. A late subscriber receives one `ADD` per instrument currently held in the account.

2. **Single global table** — when the Kafka key is scalar, no grouping axis is needed; map the topic to a plain item and put the scalar key on `field.key`. The same pattern applies outside the financial domain: the [airport-demo](/examples/airport-demo/) illustrates it with a topic where each record reports the current status of a flight, keyed by the scalar flight number (e.g. `"LS123"`):

   ```xml
   <param name="map.flights.to">flights</param>
   <param name="field.key">#{KEY}</param>
   ```

   Every record routes to the single item `flights`; `field.key=#{KEY}` puts each flight number on its own row. The union is just `{KEY}`, which is the full (scalar) Kafka key, so each flight number occupies exactly one row. A late subscriber receives one `ADD` per flight currently active.

If `field.key` would evaluate to a constant on every record reaching the item (e.g. because the item-name binds already pin every key component), the table degenerates to one row per item and _MERGE_ is the better Mode.

#### Other layouts

Verdicts apply to the union "item-name binds ∪ `field.key`":

| Layout | Example | Snapshot correctness | What happens |
| --- | --- | --- | --- |
| Union covers the full Kafka key | `positions-#{account=KEY.account}` + `field.key=#{KEY.instrument}`; or plain item + `field.key=#{KEY}` against a scalar-keyed topic | **Correct** — the recommended shape | Distinct Kafka keys land on distinct `(item, row)` pairs; ADD/UPDATE/DELETE on each row track a single Kafka record stream. |
| Union covers only a subset of the key | `positions-#{account=KEY.account}` + `field.key=#{VALUE.status}` | **Broken** | Two Kafka keys with the same `account` and the same `VALUE.status` collide on the same row of the same item; ADD/UPDATE/DELETE alias against each other. |
| Item-name includes `VALUE.*` | `item-#{id=KEY.id,status=VALUE.status}` + `field.key=#{KEY.id}` | **Broken** / **silently wrong** | Same fan-out as _MERGE_, with the same silently-empty trap when the item name mutates as the value changes. |
| `field.key` does not resolve to a scalar | `field.key=#{KEY}` against a structured Kafka key | **Invalid** | `field.key` must be scalar; a structured `KEY` here is not a valid row identifier. |

#### Recommended topic shape

As for _MERGE_, a [**log-compacted**](https://kafka.apache.org/documentation/#compaction) topic (`cleanup.policy=compact`) is the natural source: only the latest record per key survives, which is exactly what the connector needs to reconstruct the current table. Tombstones are mapped to `DELETE`, so compaction preserves exactly the records required for an accurate snapshot.

This is the appropriate choice when the topic models a **changelog of a keyed entity set** (for example: positions in a portfolio, online users, items in a cart) and clients need both the current contents of the set and the realtime stream of changes.

### Idle expiration

When [`item.snapshot.max.idle.seconds`](#itemsnapshotmaxidleseconds) is set to a value greater than `0`, the connector also discards the per-item snapshot once the item has been idle for longer than the configured interval, so that the next incoming record starts a fresh one rather than being merged on top of stale state. The idle clock is _sliding_: every record routed to an item refreshes the timestamp, so an item that keeps receiving traffic is never considered idle.

This behavior is opt-in (the default value `0` disables the check) and has no effect when `item.snapshot.enabled.mode = NONE`, since there is no connector-managed snapshot to discard. It is meant for topics whose natural cadence makes a long-stale snapshot misleading (for example: end-of-session data, or partitions that go silent between bursts).

### Caveats

A few notes that apply to all three non-`NONE` Modes:

- **Misconfigurations are not rejected at startup.** Any layout that the per-Mode tables above flag as anything other than _Correct_ — including the ones labeled _Broken_, _silently wrong_, _Degenerate_, _Discouraged_, _Global window_, _Invalid_ — is currently accepted by the connector, which starts cleanly and exhibits the runtime behavior described in the table. If your snapshot looks empty or oversized, recheck the extraction layout before chasing the issue elsewhere.
- **Regex topic mappings** ([`map.regex.enable = true`](#enable-regular-expression-mapregexenable)) are similarly accepted but not validated against the snapshot pipeline's assumptions; for snapshot-enabled adapters, prefer literal topic names.

# Client-side error handling

When a client sends a subscription to the Kafka Connector, several error conditions can occur:

- Connection issues: the Kafka broker may be unreachable due to network problems or an incorrect configuration of the [`bootstrap.servers`](#bootstrapservers) parameter.
- Non-existent topics: none of the Kafka topics mapped in the [record routing](#record-routing-maptopic_nameto) configurations exist in the Kafka broker.
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

# Customizing the Kafka Connector Metadata Adapter class

If you need to customize the _Kafka Connector Metadata Adapter_ (e.g., to implement authentication and authorization or to handle client messages),
you can create your own implementation by extending the factory class [`com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter`](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka/adapters/pub/KafkaConnectorMetadataAdapter.html).

You are free to implement any methods defined in the standard [`MetadataProvider`](https://lightstreamer.com/api/ls-adapter-inprocess/latest/com/lightstreamer/interfaces/metadata/MetadataProvider.html) interface and override implementations provided by its descendant classes ([`MetadataProviderAdapter`](https://lightstreamer.com/api/ls-adapter-inprocess/latest/com/lightstreamer/interfaces/metadata/MetadataProviderAdapter.html) and [`LiteralBasedProvider`](https://lightstreamer.com/api/ls-adapter-inprocess/latest/com/lightstreamer/adapters/metadata/LiteralBasedProvider.html)).

Bear in mind that the `KafkaConnectorMetadataAdapter` class already provides implementations for the following methods: init, notifyNewTables, notifyTablesClose, and wantsTablesNotification.
To extend such methods, the class offers hook methods that you can override to incorporate your custom logic:

- [_postInit_](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka/adapters/pub/KafkaConnectorMetadataAdapter.html#postInit(java.util.Map,java.io.File)):  Called after the initialization phase of the Kafka Connector Metadata Adapter is completed.

- [_onSubscription_](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka/adapters/pub/KafkaConnectorMetadataAdapter.html#onSubscription(java.lang.String,java.lang.String,com.lightstreamer.interfaces.metadata.TableInfo%5B%5D)): Called to notify when a user submits a subscription.

- [_onUnsubscription_](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka/adapters/pub/KafkaConnectorMetadataAdapter.html#onUnsubscription(java.lang.String,com.lightstreamer.interfaces.metadata.TableInfo%5B%5D)): Called to notify when a subscription is removed.

## Develop the extension

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

### Lightstreamer setup

Before running the connector, you first need to deploy a Proxy Adapter into the Lightstreamer server instance.

#### Requirements

- JDK (Java Development Kit) v17 or newer
- [Lightstreamer Broker](https://lightstreamer.com/download/) (also referred to as _Lightstreamer Server_) v7.4.8 or newer. Follow the installation instructions in the `LS_HOME/GETTING_STARTED.TXT` file included in the downloaded package.

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

3. Edit the connector configuration properties file as detailed in the [Configuration reference](#configuration-reference) section.

   You may want to use the provided [`quickstart-lightstreamer-local.properties`](./kafka-connector-project/config/kafka-connect-config/quickstart-lightstreamer-local.properties) or [`quickstart-lightstreamer-local.json`](./kafka-connector-project/config/kafka-connect-config/quickstart-lightstreamer-local.json) files as starting pint. This file provides the set of pre-configured settings to feed Lightstreamer with stock market events, as already shown in the [installation instruction](#install) for the Lightstreamer Kafka Connector.

4. Launch the Lightstreamer Server instance already configured in the [Lightstreamer setup](#lightstreamer-setup) section.

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

In addition, the [examples/quickstart-kafka-connect](/examples/quickstart-kafka-connect/) folder shows how to use that image in Docker Compose through a Kafka Connect version of the _Quickstart_ app.

## Supported converters

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

## Configuration reference

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
[`lightstreamer.server.proxy_adapter.socket.connection.setup.max.retries`](#lightstreamerserverproxy_adaptersocketconnectionsetupretrydelayms) > 0.

- **Type:** long
- **Default:** 5000 (5 seconds)
- **Valid Values:** [0,...]
- **Importance:** low

Example:

```
lightstreamer.server.proxy_adapter.socket.connection.setup.retry.delay.ms=15000
```

### `lightstreamer.server.proxy_adapter.username`

The username to use for authenticating to the Lightstreamer server's Proxy Adapter. This setting requires authentication to be enabled in the [Proxy Adapter configuration](#lightstreamer-setup).

- **Type:** string
- **Importance:** medium
- **Default:** none

Example:

```
lightstreamer.server.proxy_adapter.username=lightstreamer_user
```

### `lightstreamer.server.proxy_adapter.password`

The password to use for authenticating to the Lightstreamer server's Proxy Adapter. This setting requires authentication to be enabled in the [Proxy Adapter configuration](#lightstreamer-setup) of the Proxy Adapter.

- **Type:** string
- **Default:** none
- **Importance:** medium

Example:
  ```
  lightstreamer.server.proxy_adapter.password=lightstreamer_password
  ```

### `connection.inversion.enable`

If enabled, inverts the standard connection flow by having the Lightstreamer server's Proxy Adapter initiate the connection as a client to the port specified in [`request_reply.port`](#request_replyport). This inverse connection pattern requires setting the `remote_host` parameter in the [Proxy Adapter configuration](#lightstreamer-setup).

- **Type:** boolean
- **Default:** false
- **Importance:** low

Example:
  ```
  connection.inversion.enable=true
  ```

### `request_reply.port`

The port to use for request-reply communication with the Lightstreamer server's Proxy Adapter when [connection inversion](#connectioninversionenable) is enabled.

- **Type:** int
- **Default:** 6661
- **Importance:** low

Example:
  ```
  request_reply.port=6662
  ```

### `max.proxy.adapter.connections`

The maximum number of allowed remote Proxy Adapter connections when [connection inversion](#connectioninversionenable) is enabled.

- **Type:** int
- **Default:** 1
- **Importance:** low

Example:
  ```
  max.proxy.adapter.connections=5
  ```

### `record.extraction.error.strategy`

The (optional) error handling strategy to be used if an error occurs while extracting data from incoming deserialized records. Can be one of the following:

- `TERMINATE_TASK`: Terminate the task immediately.
- `IGNORE_AND_CONTINUE`: Ignore the error and continue to process the next record.
- `FORWARD_TO_DLQ`: Forward the record to the dead letter queue.

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
> This configuration implements the same concepts already presented in the [Record routing](#record-routing-maptopic_nameto) section.

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
> This configuration implements the same concepts already presented in the [Record mapping](#record-mapping-fieldfield_name) section.

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
                name:#{VALUE.name}, \
                last_price:#{VALUE.last_price}
```

The configuration above specifies the following mappings:

1. The record key to the Lightstreamer field `index`
2. The `name` attribute of the record value to the Lightstreamer field `name`
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
> This configuration implements the same concepts already presented in the [Filtered record routing](#filtered-record-routing-item-templatetemplate_name) section.

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

The configuration above specifies how to route records from the topic `user` to the item templates `by-name` and `by-age`, which define the rules to extract some personal data by leveraging _Data Extraction Language_ expressions.

# Docs

The [docs](/docs/) folder contains the complete [Kafka Connector API Reference](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc), which is useful for implementing custom authentication and authorization logic, as described in the [Customizing the Kafka Connector Metadata Adapter class](#customizing-the-kafka-connector-metadata-adapter-class) section.

To learn more about the [Lightstreamer Broker](https://lightstreamer.com/products/lightstreamer/) and the [Lightstreamer Kafka Connector](https://lightstreamer.com/products/kafka-connector/), visit their respective product pages.

# Examples

The [examples](/examples/) folder contains all the examples referenced throughout this guide, along with additional resources tailored for specific Kafka broker vendors. Additionally, you can explore the [_Airport Demo_](/examples/airport-demo/) for deeper insights into various usage and configuration options of the Lightstreamer Kafka Connector.

For a Kubernetes-based setup, the [Kafka Connector Helm Chart example](https://github.com/Lightstreamer/helm-charts/blob/main/examples/kafka-connector) provides a complete, self-contained deployment that mirrors the [Quickstart](#quick-start-set-up-in-5-minutes) on Kubernetes.

For more examples and live demos, visit our [online showcase](https://demos.lightstreamer.com/?p=kafkaconnector&lclient=noone&f=all&lall=all&sallall=all).
