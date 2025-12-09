# Quick Start with Azure Event Hubs

This folder contains a variant of the [_Quick Start SSL_](../../quickstart-ssl/README.md#quick-start-ssl) app configured to use [_Azure Event Hubs_](https://azure.microsoft.com/en-us/products/event-hubs) as the target cluster. You may follow the [instructions](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-create) to perform the following operations:

 - Create an Event Hubs _namespace_ .
 - Create an Event Hub (one Event Hub per Kafka topic) with name `stocks`.
 - Ensure the Kafka protocol is enabled on the namespace (it should enabled by default depending on pricing tier choosen but you can check in _Settings->Properties _).
 - Retrieve a Shared Access Policy connection string with the required permissions (`Listen` for consumers, `Send` for producers).

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _Azure Event Hubs_ as follows:

- Removal of the `broker` service, because replaced by the remote Event Hub

- _kafka-connector_:

  - Definition of new environment variables to configure remote endpoint, credentials, and topic name in the `adapters.xml` through the _variable-expansion_ feature of Lightstreamer:

    ```yaml
    ...
    environment:
      - bootstrap_server=${bootstrap_server}
      - connection_string=${connection_string}
    ...
    ```

  - Adaption of [`adapters.xml`](./adapters.xml) to include thw following changes:

    - Update of the parameter `bootstrap.servers` to the environment variable `bootstrap_server`:

      ```xml
      <param name="bootstrap.servers">$env.bootstrap_server</param>
      ```

    - Configuration of the encryption settings:

      ```xml
        <param name="encryption.enable">true</param>
        <param name="encryption.protocol">TLSv1.2</param>
        <param name="encryption.hostname.verification.enable">true</param>
      ```

    - Configuration of the authentication settings, with the connection string retrieved from environment variables `connection_string`:

      ```xml
        <param name="authentication.enable">true</param>
        <param name="authentication.mechanism">PLAIN</param>
        <param name="authentication.username">\$ConnectionString</param>
        <param name="authentication.password">$env.connection_string</param>
      ```

    - Add specific Kafka Concumer settings required by _Event Hub_ environment:

      ```xml
		    <!-- ##### Azure Event Hubs specific settings ##### -->
		    <param name="consumer.max.poll.interval.ms">50000</param>
		    <param name="consumer.session.timeout.ms">30000</param>
      ```

- _producer_:

   - Update of the parameter `--bootstrap-servers` from the environment variable `bootstrap_server`

   - Update of the parameter `--topic` from the environment variable `topic`
   
   - Provisioning of the `producer.properties` configuration file to proper configure the access configurations required by __Azure__:
    
     ```yaml
     # Configure SASL/PLAIN mechanism
     sasl.mechanism=PLAIN
     # Enable SSL encryption
     security.protocol=SASL_SSL
     # JAAS configuration
     sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="${connection_string}";
     ```  

## Run

From this directory, run follow the command:

```sh
$ bootstrap_server=<bootstrap_server> connection_string="<connection_string>" topic=<topic> ./start.sh 
```

where:
- `<bootstrap_server>` - The bootstrap server address of the Event Hubs Namespace
- `<connection_string>` - The primary connection string created in the shared access policies from the _Event Hubs console_
- `<topic>` - The name of the topic (ie. Event Hub) created on  _Event Hubs Console_

Then, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart); after a few moments, the user interface starts displaying the real-time stock data.

   ![Demo](/pictures/quickstart.gif)

To shutdown Docker Compose and clean up all temporary resources:

```sh
$ ./stop.sh
```
