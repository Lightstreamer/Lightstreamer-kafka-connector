# Quick Start with Azure Event Hubs

This folder contains a variant of the [_Quick Start SSL_](../../quickstart-ssl/README.md#quick-start-ssl) app configured to use [_Azure Event Hubs_](https://azure.microsoft.com/en-us/products/event-hubs) as the target cluster. Using your Azure account, you may follow the [instructions](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-create) to perform the following operations in the Azure portal:

 - Create an Event Hubs _namespace_.
 - Create an Event Hub (one Event Hub per Kafka topic) with name `stocks`.
 - Ensure the Kafka protocol is enabled on the namespace (it should be enabled by default depending on pricing tier chosen but you can check in _Settings->Properties_).
 - Retrieve a Shared Access Policy connection string with the required permissions (`Listen` for consumers, `Send` for producers).

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _Azure Event Hubs_ as follows:

- Removal of the `broker` service, because replaced by the remote Event Hub

- _kafka-connector_:

  - Definition of new environment variables to configure remote endpoint and credentials in the `adapters.xml` through the _variable-expansion_ feature of Lightstreamer:

    ```yaml
    ...
    environment:
      - bootstrap_server=${bootstrap_server}
      - connection_string=${connection_string}
    ...
    ```

  - Adaptation of [`adapters.xml`](./adapters.xml) to include the following changes:

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

    - Add specific Kafka Consumer settings required by __Azure__ environment:

      ```xml
		    <!-- ##### Azure Event Hubs specific settings ##### -->
		    <param name="consumer.max.poll.interval.ms">50000</param>
		    <param name="consumer.session.timeout.ms">30000</param>
      ```

- _producer_:

   - Update of the parameter `--bootstrap-servers` from the environment variable `bootstrap_server`

   - Update of the parameter `--topic` from the environment variable `topic`
   
   - Provisioning of the `producer.properties` configuration file to properly configure the access configurations required by __Azure__:
    
     ```yaml
     # Configure SASL/PLAIN mechanism
     sasl.mechanism=PLAIN
     # Enable SSL encryption
     security.protocol=SASL_SSL
     # JAAS configuration
     sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$$ConnectionString" password="${connection_string}";
     ```  

> **ℹ️ Schema Registry Support**: This quickstart works out-of-the-box without Schema Registry. If you need AVRO/JSON schema validation with Azure Schema Registry, see the [Advanced: Schema Registry Integration](#advanced-schema-registry-integration) section at the end of this document.

## Run

From this directory, run the following command:

```sh
$ bootstrap_server=<bootstrap_server> connection_string="<connection_string>" topic=<topic> ./start.sh 
```

where:
- `<bootstrap_server>` - The bootstrap server address of the Event Hubs Namespace (something like this: `_my-namespace_.servicebus.windows.net:9093`)
- `<connection_string>` - The primary connection string created in the shared access policies from the _Event Hubs console_ (something like this: `Endpoint=sb://_my-namespace_.servicebus.windows.net/;SharedAccessKeyName=client-consumer;SharedAccessKey=....`)
- `<topic>` - The name of the topic (ie. Event Hub) created on  _Event Hubs Console_

Then, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart); after a few moments, the user interface starts displaying the real-time stock data.

   ![Demo](/pictures/quickstart.gif)

To shutdown Docker Compose and clean up all temporary resources:

```sh
$ ./stop.sh
```

---

## Advanced: Schema Registry Integration

This section is **optional** and only needed if your Azure Event Hubs setup includes Azure Schema Registry for schema validation. The quickstart works perfectly without it.

If you need schema validation, you can enable schema registry support by uncommenting and configuring the appropriate parameters in [`adapters.xml`](./adapters.xml).

### AVRO and JSON Schema

With the current setup, AVRO and JSON schema validation share the same configuration. Both formats leverage the Azure Schema Registry client library for seamless integration.

#### Prerequisites

Before enabling schema registry integration, ensure you have:

1. **Created an Azure Schema Registry group** in your Event Hubs namespace through the Azure portal.
2. **Registered your schemas** (AVRO or JSON) in the Schema Registry group via the Azure portal or SDK.

#### Register an Azure AD Application

The Lightstreamer Kafka Connector needs an Azure AD application identity to authenticate with Azure Schema Registry. Follow these steps:

1. In the Azure portal, navigate to **Azure Active Directory** > **App registrations** > **New registration**.
2. Provide a name for your application (e.g., `LightstreamerKafkaConnector`) and complete the registration.
3. Once created, copy the **Application (client) ID** and the **Directory (tenant) ID** from the application overview page.
4. Navigate to **Certificates & secrets** > **New client secret**, create a new secret, and copy its value immediately (it won't be visible again).

#### Grant Access to Schema Registry

The registered application must have permission to access the Schema Registry:

1. In the Azure portal, navigate to your **Event Hubs namespace**.
2. Go to **Access Control (IAM)** > **Add role assignment**.
3. Select the **Schema Registry Reader** role (sufficient for consuming and validating messages).
4. Assign access to the application you registered in the previous step.

#### Configure Environment Variables

Define the following environment variables with the values obtained during app registration:

```sh
set AZURE_CLIENT_ID=11111111-2222-3333-4444-555555555555
set AZURE_TENANT_ID=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
set AZURE_CLIENT_SECRET=Example~Secret~Value~NotARealSecret12345
```

These credentials will be automatically picked up by the Azure Schema Registry client library at runtime.

> **Note**: These environment variables can be added to your Lightstreamer startup script to ensure they are available when the server launches.

#### Enable Schema Registry in adapters.xml

Finally, enable schema registry support by uncommenting and configuring the following parameters in [`adapters.xml`](./adapters.xml):

```xml
<param name="record.value.evaluator.schema.registry.enable">true</param>
<param name="schema.registry.url">https://myeventhub.servicebus.windows.net</param>
<param name="schema.registry.provider">AZURE</param>
```

Where:
- `record.value.evaluator.schema.registry.enable` - Enables schema registry integration
- `schema.registry.url` - The Azure Event Hubs namespace URL (e.g., `https://<namespace>.servicebus.windows.net`)
- `schema.registry.provider` - Must be set to `AZURE` for Azure Schema Registry
