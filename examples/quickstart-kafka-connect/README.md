# Quick Start for Kafka Connect

This folder contains the resources required to build a version of the QuickStart application based on Kafka Connect Lightstreamer Sink Connector.

The diagram above illustrates how in this setup, a stream of simulated market events is channeled from Kafka to the web client via the connector running on Kafka Connect and Lightstreamer Server.

As with the original Quickstart application, this app is structured as a Docker Compose stack. The Docker Compose file comprises the following services:


1. _broker_: a Kafka broker, based on the [Docker Image for Apache Kafka](https://kafka.apache.org/documentation/#docker). 

2. _kafka-connect-lightstreamer-sink_: Lightstreamer Server with Kafka Connector, based on the [Lightstreamer Kafka Connector Docker image example](examples/docker/), which also includes a web client mounted on `/lightstreamer/pages/QuickStart`

3. _lightstreamer_ :

3. _producer_: a native Kafka Producer, based on the provided [`Dockerfile`](examples/quickstart-producer/Dockerfile) file from the [`quickstart-producer`](examples/quickstart-producer/) producer sample client
