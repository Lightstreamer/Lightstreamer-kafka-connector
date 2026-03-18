# Azure Event Hubs Quickstart

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
		    <param name="record.consume.with.max.poll.interval.ms">50000</param>
		    <param name="record.consume.with.session.timeout.ms">30000</param>
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

> [!NOTE]
> This quickstart works out-of-the-box without Schema Registry. If you need AVRO/JSON schema validation with Azure Schema Registry, see the [Advanced: Schema Registry Integration](#advanced-schema-registry-integration) section at the end of this document.

## Run

From this directory, run the following command:

```sh
$ bootstrap_server=<bootstrap_server> connection_string="<connection_string>" topic=<topic> ./start.sh 
```

where:
- `<bootstrap_server>` - The bootstrap server address of the Event Hubs Namespace (something like this: `_my-namespace_.servicebus.windows.net:9093`)
- `<connection_string>` - The primary connection string created in the shared access policies from the _Event Hubs console_ (something like this: `Endpoint=sb://_my-namespace_.servicebus.windows.net/;SharedAccessKeyName=client-consumer;SharedAccessKey=....`)
- `<topic>` - The name of Event Hub created on  _Event Hubs Console_

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
2. **Registered your schemas** (AVRO or JSON) in the Schema Registry group via the Azure portal or SDK — this step is optional if you set `auto.register.schemas=true` in the producer configuration (see [Configure the Producer in docker-compose.yml](#configure-the-producer-in-docker-composeyml)).

#### Register a Microsoft Entra Application

The Lightstreamer Kafka Connector needs a Microsoft Entra application to access Azure Schema Registry. Follow these steps:

1. In the Azure portal, navigate to **Microsoft Entra ID** > **+ Add** > **App registration**.
2. Provide a name for your application (e.g., `LightstreamerKafkaConnector`) and complete the registration.
3. Once created, copy the **Application (client) ID** and the **Directory (tenant) ID** from the application overview page.
4. Navigate to **Certificates & secrets** > **New client secret**, create a new secret, and copy its value immediately (it won't be visible again).

#### Grant Access to Schema Registry

The registered application must have permission to access the Schema Registry:

1. In the Azure portal, navigate to your **Event Hubs namespace**.
2. Go to **Access Control (IAM)** > **Add role assignment**.
3. Select the appropriate role:
   - **Schema Registry Reader** — sufficient for the connector (consuming and validating messages only).
   - **Schema Registry Contributor** — required for the producer if `auto.register.schemas=true`, as it needs to register new schemas on first use.
4. Assign access to the application you registered in the previous step.

#### Enable Schema Registry in adapters.xml

Enable schema registry support by uncommenting and configuring the following parameters in [`adapters.xml`](./adapters.xml):

```xml
<param name="record.value.evaluator.schema.registry.enable">true</param>
<param name="schema.registry.url">https://<namespace>.servicebus.windows.net</param>
<param name="schema.registry.provider">AZURE</param>
<param name="schema.registry.azure.tenant.id">aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</param>
<param name="schema.registry.azure.client.id">11111111-2222-3333-4444-555555555555</param>
<param name="schema.registry.azure.client.secret">Example~Secret~Value~NotARealSecret12345</param>
```

Where:
- `record.value.evaluator.schema.registry.enable` — Enables schema registry integration.
- `schema.registry.url` — The Azure Event Hubs namespace URL (e.g., `https://<namespace>.servicebus.windows.net`).
- `schema.registry.provider` — Must be set to `AZURE` for Azure Schema Registry.
- `schema.registry.azure.tenant.id` — The **Directory (tenant) ID** copied from the app registration overview page.
- `schema.registry.azure.client.id` — The **Application (client) ID** copied from the app registration overview page.
- `schema.registry.azure.client.secret` — The **client secret value** generated under _Certificates & secrets_.

The connector uses these three parameters to build a `ClientSecretCredential` at startup, so no environment variables or external credential sources are required.

Also ensure `record.value.evaluator.type` in [`adapters.xml`](./adapters.xml) matches the schema format used by the producer:

- Set to `AVRO` for AVRO schemas
- Set to `JSON` for JSON schemas

#### Configure the Producer in docker-compose.yml

The producer must also be configured to serialize records using Azure Schema Registry. Update the `producer.properties` config block in [`docker-compose.yml`](./docker-compose.yml) with your schema registry credentials and settings:

```yaml
configs:
  producer.properties:
    content: |
      ...
      # Schema Registry configuration
      # For AVRO (must match record.value.evaluator.type=AVRO in adapters.xml):
      value.serializer=com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer
      # For JSON (must match record.value.evaluator.type=JSON in adapters.xml):
      # value.serializer=com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer
      auto.register.schemas=true
      schema.registry.url=https://<namespace>.servicebus.windows.net
      schema.group=<your-schema-group>
      client.id=<application-client-id>
      tenant.id=<directory-tenant-id>
      client.secret=<client-secret-value>
```

Where:
- `value.serializer` — Use `KafkaAvroSerializer` for AVRO or `KafkaJsonSerializer` for JSON. Must match `record.value.evaluator.type` in `adapters.xml`.
- `auto.register.schemas` — When `true`, the producer automatically registers new schemas in the Schema Registry group on first use (requires the **Schema Registry Contributor** role). When `false`, schemas must be pre-registered manually in the Azure portal (the **Schema Registry Reader** role is then sufficient).
- `schema.registry.url` — The same namespace URL configured in `adapters.xml`.
- `schema.group` — The name of the Schema Registry group created in the Azure portal.
- `client.id`, `tenant.id`, `client.secret` — The same Microsoft Entra application credentials configured in `adapters.xml`.
