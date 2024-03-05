# Quick Start with Schema Registry

This folder contains a variant of the [_Quick Start_SSL](../quickstart-ssl/README.md#quick-start-ssl) app configured to use the _Confluent Schema Registry_.

The [docker-compose.yml](docker-compose.yml) has been revised to configure the integration with [_Confluent Docker Image for Schema Registry_](https://hub.docker.com/r/confluentinc/cp-schema-registry):

- _schema-registry_
  
  The added service, pointing to the mentioned Docker image, with full configuration of the security settings:

- _kafka-connector_

  The new version of the [`adapters.xml`](./adapters.xml) includes:
  - Enabling of the Confluent Schema Registry:
    ```xml
    <param name="value.evaluator.schema.registry.enable">true</param>
    ```

  - Configuration of the target Schema Registry url:
    ```xml
    <param name="schema.registry.url">https://schema-registry:8084</param>
    ```

  - Configuration of the truststore to authenticate the Schema Registry.
    ```xml
    <param name="schema.registry.encryption.truststore.path">secrets/kafka-connector.truststore.jks</param>
    <param name="schema.registry.encryption.truststore.password">kafka-connector-truststore-password</param>
    ```

   Configuration of the keystore for client authentication with the Schema Registry.
    ```xml
    <param name="schema.registry.encryption.keystore.enable">true</param>
    <param name="schema.registry.encryption.keystore.path">secrets/kafka-connector.keystore.jks</param>
    <param name="schema.registry.encryption.keystore.password">kafka-connector-password</param>
    <param name="schema.registry.encryption.keystore.key.password">kafka-connector-password</param>
    ```

- _producer_
  - Setting to enable the Json schema serializer New settingsThe `producer.properties` configuration file for enabling SSL and configuring truststore and keystore:
    
    ```yaml
    value.serializer=io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
    schema.registry.url=https://schema-registry:8084
    schema.registry.ssl.truststore.location=/usr/app/secrets/producer.truststore.jks
    schema.registry.ssl.truststore.password=producer-truststore-password
    schema.registry.ssl.keystore.location=/usr/app/secrets/producer.keystore.jks
    schema.registry.ssl.keystore.password=producer-password
    schema.registry.ssl.key.password=producer-password
    ```  

In addition, all services reference the local [`secrets`](secrets/) folder to retrieve their secrets:

In particular, 

- _broker_ mounts [`secrets/broker`](secrets/broker/) to `/etc/kafka/secrets` for:
  - the truststore file [`broker.truststore.jks`](secrets/broker/broker.truststore.jks);
  - the keystore file [`broker.keystore.jks`](secrets/broker/broker.keystore.jks);
  - the credentials files [`broker_keystore_credentials`](secrets/broker/broker_keystore_credentials) and [`broker_key_credentials`](secrets/broker/broker_key_credentials).

- _kafka-connector_ mounts [`secrets/kafka-connector`](secrets/kafka-connector/) to `LS_KAFKA_CONNECTOR_HOME/secrets` for:
  -  the truststore file [`kafka-connector.truststore.jks`](secrets/kafka-connector/kafka-connector.truststore.jks);
  -  the keystore file [`kafka-connector.keystore.jks`](secrets/kafka-connector/kafka-connector.keystore.jks);

- _producer_ mounts [`secrets/producer`](secrets/producer/) to `/usr/app/secrets` for:
  -  the truststore file [`producer.truststore.jks`](secrets/producer/producer.truststore.jks);
  -  the keystore file [`producer.keystore.jks`](secrets/producer/producer.keystore.jks);

You can regenerate all of them with:

```sh
./generate-secrets.sh
```

## Run

From this directory, follow the same instructions you can find in the [Quick Start](../../README.md#run) section of the main README file.
