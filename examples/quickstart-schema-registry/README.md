# Schema Registry quickstart

This folder contains a variant of the [_SSL quickstart_](../quickstart-ssl/README.md#quick-start-ssl) app configured to use the _Confluent Schema Registry_.

The [docker-compose.yml](docker-compose.yml) file has been revised to configure the integration with [_Confluent Docker Image for Schema Registry_](https://hub.docker.com/r/confluentinc/cp-schema-registry) as follows:

- New `schema-registry` service, pulled from the mentioned Docker image and configured with security settings.

- _kafka-connector_:

  Mounting of the [`adapters.xml`](./adapters.xml) file with the following changes:

  - Update of the parameter `record.value.evaluator.type` to use Protobuf:

    ```xml
    <param name="record.value.evaluator.type">PROTOBUF</param>
    ```
  
  - Enabling of the Schema Registry:

    ```xml
    <param name="value.evaluator.schema.registry.enable">true</param>
    ```
  
  - Configuration of the target Schema Registry URL:

    ```xml
    <param name="schema.registry.url">https://schema-registry:8084</param>
    ```
  
  - Configuration of the trust store to authenticate the Schema Registry:

    ```xml
    <param name="schema.registry.confluent.encryption.truststore.path">secrets/kafka-connector.truststore.p12</param>
    <param name="schema.registry.confluent.encryption.truststore.password">kafka-connector-truststore-password</param>
    <param name="schema.registry.confluent.encryption.hostname.verification.enable">true</param>
    ```
  
  - Configuration of the key store for client authentication with the Schema Registry:

    ```xml
    <param name="schema.registry.confluent.encryption.keystore.enable">true</param>
    <param name="schema.registry.confluent.encryption.keystore.path">secrets/kafka-connector.keystore.p12</param>
    <param name="schema.registry.confluent.encryption.keystore.password">kafka-connector-keystore-password</param>
    ```

- _producer_:

   Configuration of `producer.properties` to enable Protobuf serialization and Schema Registry communication:
    
   ```yaml
   ...
   # Protobuf serializer with support for the Schema Registry
   value.serializer=io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
   # Schema Registry URL
   schema.registry.url=https://schema-registry:8084
   # Trust store configuration to authenticate the Schema Registry
   schema.registry.ssl.truststore.location=/usr/app/secrets/producer.truststore.p12
   schema.registry.ssl.truststore.type=PKCS12
   schema.registry.ssl.truststore.password=producer-truststore-password
   # Key store configuration for client authentication with the Schema Registry
   schema.registry.ssl.keystore.location=/usr/app/secrets/producer.keystore.p12
   schema.registry.ssl.keystore.type=PKCS12
   schema.registry.ssl.keystore.password=producer-keystore-password
   # Enable hostname verification of the Schema Registry certificate
   schema.registry.ssl.endpoint.identification.algorithm=https
   ```  

In addition, the `schema-registry` service references the local [`secrets/schema-registry`](../compose-templates/secrets/schema-registry/) folder to retrieve the following secrets:

- The trust store file [`schema-registry.truststore.p12`](../compose-templates/secrets/schema-registry/schema-registry.truststore.p12)

- The key store file [`schema-registry.keystore.p12`](../compose-templates/secrets/schema-registry/schema-registry.keystore.p12)

You can regenerate all of them with:

```sh
$ ./generate-secrets.sh
```

The script reuses the existing CA, so the trust stores stay valid. To also create a new CA, run `./generate-secrets.sh --new-ca`. To see when the certificates expire without changing anything, run `./generate-secrets.sh --check`: it fails if any certificate expires within 90 days (configurable with the `CHECK_DAYS` environment variable).

## Run

From this directory, follow the same instructions you can find in the [Quick start](../../README.md#quick-start-set-up-in-5-minutes) section of the main README file.
