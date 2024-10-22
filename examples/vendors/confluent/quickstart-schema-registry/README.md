# Quick Start with Schema Registry

This folder contains a variant of the [_Quick Start SSL_](../../../quickstart-ssl/README.md#quick-start-ssl) app configured to use the _Confluent Schema Registry_.

The [docker-compose.yml](docker-compose.yml) file has been revised to configure the integration with [_Confluent Docker Image for Schema Registry_](https://hub.docker.com/r/confluentinc/cp-schema-registry) as follows:

- New `schema-registry` service, pulled from the mentioned Docker image and configured with security settings.
- _kafka-connector_:

  Adaption of [`adapters.xml`](./adapters.xml) to include:
  - Enabling of the Schema Registry:
    ```xml
    <param name="value.evaluator.schema.registry.enable">true</param>
    ```
  - Configuration of the target Schema Registry URL:
    ```xml
    <param name="schema.registry.url">https://schema-registry:8084</param>
    ```
  - Configuration of the trust store to authenticate the Schema Registry.
    ```xml
    <param name="schema.registry.encryption.truststore.path">secrets/kafka-connector.truststore.jks</param>
    <param name="schema.registry.encryption.truststore.password">kafka-connector-truststore-password</param>
    ```
  - Configuration of the key store for client authentication with the Schema Registry.
    ```xml
    <param name="schema.registry.encryption.keystore.enable">true</param>
    <param name="schema.registry.encryption.keystore.path">secrets/kafka-connector.keystore.jks</param>
    <param name="schema.registry.encryption.keystore.password">kafka-connector-password</param>
    <param name="schema.registry.encryption.keystore.key.password">kafka-connector-private-key-password</param>
    ```
- _producer_:

   Extension of the `producer.properties` configuration file with the settings required to communicate with the Schema Registry:
    
   ```yaml
   ...
   # JSON serializer with support for the Schema Registry
   value.serializer=io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
   # Schema Registry URL
   schema.registry.url=https://schema-registry:8084
   # Trust store configuration to authenticate the Schema Registry
   schema.registry.ssl.truststore.location=/usr/app/secrets/producer.truststore.jks
   schema.registry.ssl.truststore.password=producer-truststore-password
   # Key store configuration for client authentication with the Schema Registry
   schema.registry.ssl.keystore.location=/usr/app/secrets/producer.keystore.jks
   schema.registry.ssl.keystore.password=producer-password
   schema.registry.ssl.key.password=producer-private-key-password
   ```  

In addition, the `schema-registry` service references the local [`secrets/schema-registry`](./secrets/schema-registry/) folder to retrieve the following secrets:

- The trust store file [`schema-registry.truststore.jks`](./secrets/schema-registry/schema-registry.truststore.jks)
- The key store file [`schema-registry.keystore.jks`](./secrets/schema-registry/schema-registry.keystore.jks)

You can regenerate all of them with:

```sh
$ ./generate-secrets.sh
```

## Run

From this directory, follow the same instructions you can find in the [Quick Start](../../../../README.md#run) section of the main README file.
