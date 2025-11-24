# Confluent Platform Quick Start

To efficiently showcase the functionalities of the Lightstreamer Kafka Connector, we have prepared an accessible quickstart application located in the [`examples/vendors/confluent/quickstart-confluent-platform/`](/examples/vendors/confluent/quickstart-confluent-platform/) directory. This streamlined application facilitates real-time streaming of data from a Kafka topic directly to a web interface. It leverages a modified version of the [Stock List Demo](https://github.com/Lightstreamer/Lightstreamer-example-StockList-client-javascript?tab=readme-ov-file#basic-stock-list-demo---html-client), specifically adapted to demonstrate Kafka integration. This setup is designed for rapid comprehension, enabling you to swiftly grasp and observe the connector's performance in a real-world scenario.

![Quickstart Diagram](/pictures/quickstart-diagram.png)

The diagram above illustrates how, in this setup, a stream of simulated market events is channeled from Kafka to the web client via the Lightstreamer Kafka Connector.

To provide a complete stack, the app is based on _Docker Compose_. The [Docker Compose file](/examples/vendors/confluent/quickstart-confluent-platform/docker-compose.yml) comprises the following services:

1. _broker_: the Kafka broker, based on the [Official Confluent Docker Image for Kafka (Community Version)](https://hub.docker.com/r/confluentinc/cp-kafka)
2. _kafka-connector_: Lightstreamer Broker with the Kafka Connector, based on the [Lightstreamer Kafka Connector Docker image example](/examples/docker/), which also includes a web client mounted on `/lightstreamer/pages/QuickStart`
3. _producer_: a native Kafka Producer, based on the provided [`Dockerfile`](/examples/quickstart-producer/Dockerfile) file from the [`quickstart-producer`](/examples/quickstart-producer/) sample client

### Run

1. Make sure you have Docker, Docker Compose, and a JDK (Java Development Kit) v17 or newer installed on your local machine.
2. From the [`examples/vendors/confluent/quickstart-confluent-platform/`](/examples/vendors/confluent/quickstart-confluent-cloud/) folder, run the following:

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
