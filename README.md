# Lightstreamer Kafka Connector

## Introduction

The _Lightstreamer Kafka Connector_ is a ready-made pluggable Lighstreamer Adapter that enables event streaming from a Kafka broker to the internet.

[Insert Diagram here]

With Kafka Connector, any internet client connected to the Lightstreamer Server can consume events from Kafka topics like any other Kafka client. The Connector takes care of processing records received from Kafka to adapt them as real-time updates for the clients.

The Kafka Connector allows to move high volume data out of Kafka by leveraging the battle-tested ability of the Lightstreamer real-time engine to deliver live data reliably and efficiently over internet protocols.

### Features

[...]

### Quick Start

1. Download Lightstreamer Server (Lightstreamer Server comes with a free non-expiring demo license for 20 connected users) from [Lightstreamer Download page](https://lightstreamer.com/download/), and install it, as explained in the `GETTING_STARTED.TXT` file in the installation home directory.

2. Get the deployment package from the [latest release page](releases) and unzip it into the `adapters` folder of the Lightstreamer Server installation.

3. Edit the `QuickStart` configuration in the `<LS_HOME>/lightstreamer-kafka-connectors/adapters.xml` as follows:
    - Update the `bootstrap.servers` parameter with the connection string of the external Kafka cluster.
    - Optionally customize the `<LS_HOME>/lightstreamer-kafka-connectors/log4j.properties` file (the current settings produce a quickstart.log file).

4. Launch Lightstremaer Server.    

### Configuration

### Deployment

To build the Lightstreamer Kafka Connector deployment package, run;

`./gradlew connectorDistZip`

which will produce the `lightstreamer-kafka-connector-<version>.zip` file under the `build/distributions `folder.

