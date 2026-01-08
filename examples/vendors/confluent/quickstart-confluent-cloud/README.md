# Confluent Cloud QuickStart

In this section, we illustrate a variant of the [Confluent Platform QuickStart](../quickstart-confluent-platform/) that involves using _Confluent Cloud_ Kafka brokers, which is a serverless cloud solution that does not require installing and managing a local Kafka broker.

To run this quickstart, you need an active Confluent Cloud account. Please refer to the [Deployment](/examples/vendors/confluent/README.md#deployment) section of the Confluent README file for the requirements and how to properly configure your environment.

The [docker-compose.yml](./docker-compose.yml) file has been revised to realize the integration with _Confluent Cloud_ as follows:

- Removal of the `broker` service, because replaced by the remote cluster.

- _kafka-connector_:

  - Definition of new environment variables to configure remote endpoint and credentials in the `adapters.xml` through the _variable-expansion_ feature of Lightstreamer:

    ```yaml
    ...
    environment:
      - bootstrap_server=${bootstrap_server}
      - api_key=${api_key}
      - api_secret=${api_secret}
    ...
    ```
  - Mounting of a the [`adapters.xml`](./adapters.xml) file with the following relevant changes:

    - Update of the parameter `bootstrap.servers` to the environment variable `bootstrap_server`:

      ```xml
      <param name="bootstrap.servers">$env.bootstrap_server</param>
      ```

    - Configuration of the encryption settings:

      ```xml
      <param name="encryption.enable">true</param>
      <param name="encryption.protocol">TLSv1.2</param>
      <param name="encryption.hostname.verification.enable">false</param>
      ```

    - Configuration of the authentication settings, with the credentials retrieved from environment variables `api_key` and `api_secret`:

      ```xml
      <param name="authentication.enable">true</param>
      <param name="authentication.mechanism">PLAIN</param>
      <param name="authentication.username">$env.api_key</param>
      <param name="authentication.password">env.api_secret</param>
      ```

- _producer_:

  - Update of the parameter `--bootstrap-servers` to the o the environment variable `bootstrap_server`

  - Provisioning of the `producer.properties` configuration file to enable `SASL/PLAN` over TLS, with username and password retrieved from the environment variables `api_key` and `api_secret`:
  
    ```yaml
    # Configure SASL/PLAIN mechanism
    sasl.mechanism=PLAIN
    # Enable SSL encryption
    security.protocol=SASL_SSL
    # JAAS configuration
    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${api_key}" password="${api_secret}";
    ```        

### Run

1. Make sure you have Docker, Docker Compose, and a JDK (Java Development Kit) v17 or newer installed on your local machine.
2. From the [`examples/vendors/confluent/quickstart-confluent-cloud/`](../quickstart-confluent-cloud/) folder, run the following:

   ```sh
   $ bootstrap_server=<bootstrap_server> \
   api_key=<API.key> \
   api_secret=<API.secret> \
   ./start.sh
   ...
    ✔ Network quickstart-kafka-connector-confluent-cloud_default Created
    ✔ Container producer                                         Created 
    ✔ Container kafka-connector                                  Created      
    
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