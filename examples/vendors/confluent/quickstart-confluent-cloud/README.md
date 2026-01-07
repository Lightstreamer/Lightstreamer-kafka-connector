# Confluent Cloud QuickStart

In this section, we illustrate a variant of the [Confluent Platform Quick Start](../quickstart-confluent-platform/) that involves using _Confluent Cloud_ Kafka brokers, which is a serverless cloud solution that does not require installing and managing a local Kafka broker. We have prepared the resources for this exercise in the [`examples/vendors/confluent/quickstart-confluent-cloud/`](/examples/vendors/confluent/quickstart-confluent-cloud/) folder.

The [docker-compose.yml](./quickstart-confluent-cloud/docker-compose.yml) file has been revised to realize the integration with _Confluent Cloud_ and specifically removed the `broker` service, because replaced by the remote cluster.

To run this quickstart, you need an active Confluent Cloud account. Please refer to the [Deployment](/examples/vendors/confluent#deployment) section of the Specific Confluent Readme for the requirements and how to properly configure your environment.

### Run

1. Make sure you have Docker, Docker Compose, and a JDK (Java Development Kit) v17 or newer installed on your local machine.
2. From the [`examples/vendors/confluent/quickstart-confluent-cloud/`](../quickstart-confluent-cloud/) folder, run the following:
  
   ```sh
   $ bootstrap_server=<bootstrap_server> \
   api_key=<API.key> \
   api_secret=<API.secret> \
   ./start.sh 
   ...
    ⠏ Network quickstart_default  Created
    ✔ Container broker            Started
    ✔ Container producer          Started
    ✔ Container kafka-connector   Started
   ...
   Services started. Now you can point your browser to http://localhost:8080/QuickStart to see real-time data.
   ...
   ```
   where:
      - `<bootstrap_server>` - The bootstrap server endpoint of the Confluent Cloud cluster
      - `<API.key>` and `<API.secret>` - The API key and API secret linked to your Confluent Cloud cluster, which you can generate using the Confluent CLI or from the Confluent Cloud Console.
      - Make also sure you have created a topic named 'stocks' within the cluster.

3. Once all containers are ready, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).

4. After a few moments, the user interface starts displaying the real-time stock data.

   ![Demo](/pictures/quickstart.gif)

5. To shutdown Docker Compose and clean up all temporary resources:

   ```sh
   $ ./stop.sh
   ```