# Kafka Connector Airport Demo

This project includes the resources needed to develop the Kafka Connector Airport Demo.

![Infrastructure](infrastructure.png)<br>

The Demo simulates a basic departures board consisting of ten rows, each representing flight departure information from a hypothetical airport.
The simulated data, inputted into a [Kafka cluster](https://kafka.apache.org/), is fetched and injected into the Lightstreamer server via [Kafka Connector](https://github.com/Lightstreamer/Lightstreamer-kafka-connector).

The demo project consists of:
- a web client designed to visualize the airport departure board from a browser
- a random flight information generator that acts as a message producer for Kafka
- files to configure Kafka Connector according to the needs of the demo

## The Web Client

The web client, contained in the folder [`client/web`](client/web/) uses the [Web Client SDK API for Lightstreamer](https://lightstreamer.com/api/ls-web-client/latest/) to handle the communications with Lightstreamer Server. A simple user interface is implemented to display the real-time data received from Lightstreamer Server.

![Demo ScreenShot](screen_large.png)<br>
<!-- ### [![](http://demos.lightstreamer.com/site/img/play.png) View live demo]( ... ) -->
<b>Live demo coming soon ...</b>

The demo basically executes a single [Subscription](https://lightstreamer.com/api/ls-web-client/latest/Subscription.html) with ten items subscribed to in **MERGE** mode feeding a [DynaGrid](https://lightstreamer.com/api/ls-web-client/latest/DynaGrid.html) with the current list and status of the next departing flights (according to the simulated time).
The list of the ten Items to subscribe to is as follows:
```javascript
itemsList = ["flights-[key=10]", "flights-[key=1]", "flights-[key=2]", "flights-[key=3]", "flights-[key=4]", "flights-[key=5]", "flights-[key=6]", "flights-[key=7]", "flights-[key=8]", "flights-[key=9]" ];
```
each representing a row on the board. The table is then kept sorted by departure time by setting the [setSort](https://sdk.lightstreamer.com/ls-web-client/9.2.0/api/DynaGrid.html#setSort) call of the DynaGrid object.

## The Producer

The source code of the producer is basically contained in the `producer` package, which generates random information for the flights and acts as the producer versus the Kafka cluster. In particular, the following classes are defined:
- `DemoPublisher.java`: implementing the simulator generating and sending flight monitor data to a Kafka topic; the messages sent to Kafka will also have a key composed simply of a number representing the row in the table to which the information refers
- `FlightInfo.java`: class that defines all the flight-related information to be displayed on the departure board, and will be serialized into JSON format as a Kafka message

## Connector Configurations

In the [`connector`](connector/) folder, we found the configuration files needed to configure Kafka Connector:
- `adapters.xml`: in this file, parameters are essentially configured for the connector to consume messages from Kafka, and the mapping between Kafka cluster topics and Lightstreamer items that the client will subscribe to is defined. In the specific case of this demo, message serialization occurs via JSON objects, and therefore, the mapping of fields from the received JSON object to the Lightstreamer item fields to be sent to clients is also defined. In particular, the section defining the field mapping is this one:
  ```xml
    <data_provider name="AirpotDemo">
      ...
      
      <!-- Extraction of the record key mapped to the field "key". -->
      <param name="field.key">#{KEY}</param>

      <!-- Extraction of the record value mapped to the field "value". -->
      <param name="field.destination">#{VALUE.destination}</param>
      <param name="field.departure">#{VALUE.departure}</param>
      <param name="field.flightNo">#{VALUE.flightNo}</param>
      <param name="field.terminal">#{VALUE.terminal}</param>
      <param name="field.status">#{VALUE.status}</param>
      <param name="field.airline">#{VALUE.airline}</param>
      <param name="field.currentTime">#{VALUE.currentTime}</param>

      ...
    </data_provider>
  ```
- `log4j.properties`: in this file, you'll find the specific configuration for the Kafka Connector log, to obtain details about all interactions with the Kafka cluster and the message retrieval operations, along with their routing to the subscribed items in the Lightstreamer server. In this demo, a specific log file named `airport.log` is configured, destined for the same `logs` folder as the other Lightstreamer logs.

## Setting up the Demo

### Kafka Cluster 

The demo needs a Kafka cluster where a topic `Flights` is created. You can use either a locally installed instance of Kafka in your environment, starting perhaps from the latest release of Apache Kafka as explained [here](https://kafka.apache.org/quickstart), or an installation of Confluent Platform (you can find a quickstart [here](https://docs.confluent.io/platform/current/platform-quickstart.html)). Alternatively, you can use one of the cloud services that offer fully managed services such as [Confluent Cloud](https://docs.confluent.io/cloud/current/get-started/index.html) or [AWS MSK](https://aws.amazon.com/msk/?nc2=type_a).  
Based on this choice, you will need to modify the [`adapters.xml`](connector/adapters.xml) files accordingly, particularly the `bootstrap server` parameter. The proposed configuration assumes a local Kafka installation that does not require authentication or the use of TLS communication:
```xml
<data_provider name="AirpotDemo">
    <!-- ##### GENERAL PARAMETERS ##### -->

    <adapter_class>com.lightstreamer.kafka_connector.adapters.KafkaConnectorDataAdapter</adapter_class>

    <!-- The Kafka cluster address -->
    <param name="bootstrap.servers">localhost:9092</param>

  ...

</data_provider>
```

However, in more complex scenarios where authentication and TLS need to be set up, please refer to the Kafka Connector guide [here](../../README.md#broker-authentication-parameters) and [here](../../README.md#encryption-parameters).

The demo leverages a particular data retention mechanism to ensure simplified snapshot management.
The mechanism is compaction, which takes advantage of the fact that the demo uses key-based messages, allowing the Kafka cluster to maintain only one value per key, the most recent one, in the message history.
Further details on this mechanism can be found [here](https://developer.confluent.io/courses/architecture/compaction/#).

To configure our `Flights` topic to be managed in a compacted manner, the following steps are necessary:

1. set up the Kafka cluster to support this mode, ensuring that the server.properties file contains this setting:
   ```java
   log.cleanup.policy=compact, delete
   ```

2. create the topic with the following configurations:
   ```sh 
   ./bin/kafka-topics.sh --create --topic Flights --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
   ./bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name Flights --add-config cleanup.policy=compact
   ./bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name Flights --add-config delete.retention.ms=30000
   ./bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name Flights --add-config segment.ms=30000
   ./bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name Flights --describe
   ```

### Lightstreamer Server

- Download Lightstreamer Server version 7.4.2 or later (Lightstreamer Server comes with a free non-expiring demo license for 20 connected users) from [Lightstreamer Download page](https://lightstreamer.com/download/), and install it, as explained in the `GETTING_STARTED.TXT` file in the installation home directory.
- Make sure that Lightstreamer Server is not running.
- Deploy a fresh installation of Lightstreamer Kafka Connector following the instructions provided [here](../../README.md#deploy).
- Replace the `adapters.xml` file with the one of this project and in the case update the settings as discussed in the previous section.
- [Optional] Customize the logging settings in the log4j configuration file `log4j.properties`.
- Launch Lightstreamer Server.

### Simulator Producer loop

To build the simulator you have two options: either use [Maven](https://maven.apache.org/) (or other build tools) to take care of dependencies and build (recommended) or gather the necessary jars yourself and build it manually.
For the sake of simplicity, only the Maven case is detailed here.

#### Maven

You can easily build the jar using Maven through the [producer/pom.xml](producer/pom.xml) file. As an alternative, you can use any other build tool (e.g. Gradle, Ivy, etc.).

Assuming Maven is installed and available in your path, you can build the producer by running:

```sh 
$ mvn install dependency:copy-dependencies 
```

If the task completes successfully, it also creates a `target` folder, with the jar of the simulator and all the needed dependencies. Alternatively, you can start the simulator producer loop with this command from the  `producer` folder:

```sh 
$ mvn exec:java localhost:9092 Flights
```
 
where:
- `localhost:9092` is the bootstrap string for connecting to Kafka and for which the same considerations made above apply
- `Flights` is the topic name used to produce the messages with simulated flights info

### Web Client

In order to install a web client for this demo pointing to your local Lightstreamer Server, follow these steps:

* deploy this demo on the Lightstreamer Server (used as Web server) or in any external Web Server. If you choose the former, create the folders `<LS_HOME>/pages/demos/airport70` (you can customize the last two digits based on your favorite movie in the series) and copy here the contents of the `client/web/src` folder of this project

>[!IMPORTANT]
> *The client demo configuration assumes that Lightstreamer Server, Kafka Cluster, and this client are launched on the same machine. If you need to target a different Lightstreamer server, please double check the `LS_HOST` variable in [`client/web/src/js/const.js`](client/web/src/js/const.js) and change it accordingly.*

* open your browser and point it to [http://localhost:8080/airport70](http://localhost:8080/airport70)

## Lightstreamer Compatibility Notes

- Compatible with Lightstreamer SDK for Java In-Process Adapters since 7.4.2.