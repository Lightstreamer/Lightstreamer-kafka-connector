# Quick Start SSL

This folder contains a variant of the [_Quick Start_](../../README.md#quick-start-set-up-in-5-minutes) app configured to establish a secure connection with the Kafka broker.

The [docker-compose.yml](docker-compose.yml) file has been revised to enable support for SSL, as follows:

- _broker_:
  - Enabling of SSL on port `29094`
  - Definition of new environment variables to configure key store, trust store, client authentication, and secrets:
    - `KAFKA_SSL_TRUSTSTORE_FILENAME`
    - `KAFKA_SSL_TRUSTSTORE_CREDENTIALS`
    - `KAFKA_SSL_KEYSTORE_FILENAME`
    - `KAFKA_SSL_KEYSTORE_CREDENTIALS`
    - `KAFKA_SSL_KEY_CREDENTIALS`
    - `KAFKA_SSL_CLIENT_AUTH`
- _kafka-connector_:

  Adaption of [`adapters.xml`](./adapters.xml) to include the following:

  - New SSL endpoint (`broker:29094`):
    ```xml
    <param name="bootstrap.servers">broker:29094</param>
    ```
  - Encryption settings:
    ```xml
    <param name="encryption.enable">true</param>
    <param name="encryption.protocol">TLSv1.2</param>
    <param name="encryption.hostname.verification.enable">false</param>
    ```
  - Configuration of the trust store to authenticate the broker:
    ```xml
    <param name="encryption.truststore.path">secrets/kafka.connector.truststore.jks</param>
    <param name="encryption.truststore.password">kafka-connector-truststore-password</param>
    ```
  - Configuration of the key store for client authentication with the broker:
    ```xml
    <param name="encryption.keystore.enable">true</param>
    <param name="encryption.keystore.path">secrets/kafka-connector.keystore.jks</param>
    <param name="encryption.keystore.password">kafka-connector-password</param>
    <param name="encryption.keystore.key.password">kafka-connector-private-keypassword</param>
    ```
- _producer_:
  - Setting of the parameter `--bootstrap-servers` to the new SSL endpoint (`broker:29094`)
  - Provisioning of the `producer.properties` configuration file to enable SSL support:
    ```yaml
    # Enable SSL
    security.protocol=SSL
    # Trust store configuration to authenticate the broker
    ssl.truststore.location=/usr/app/secrets/producer.truststore.jks
    ssl.truststore.password=producer-truststore-password
    # Key tore configuration for client authentication with the broker
    ssl.keystore.location=/usr/app/secrets/producer.keystore.jks
    ssl.keystore.password=producer-password
    ssl.key.password=producer-password
    # Disable host name verification
    ssl.endpoint.identification.algorithm=
    ```  

In addition, all services reference the local [`secrets`](../compose-templates/secrets/) folder to retrieve their secrets. In particular:

- _broker_ mounts [`secrets/broker`](../compose-templates/secrets/broker/) to `/etc/kafka/secrets` for the following resources:
  - The trust store file [`broker.truststore.jks`](../compose-templates/secrets/broker/broker.truststore.jks)
  - The key store file [`broker.keystore.jks`](../compose-templates/secrets/broker/broker.keystore.jks)
  - The credentials files [`broker_keystore_credentials`](../compose-templates/secrets/broker/broker_keystore_credentials) and [`broker_key_credentials`](../compose-templates/secrets/broker/broker_key_credentials)

- _kafka-connector_ mounts [`secrets/kafka-connector`](../compose-templates/secrets/kafka-connector/) to `LS_KAFKA_CONNECTOR_HOME/secrets` for the following resources:
  - The trust store file [`kafka-connector.truststore.jks`](../compose-templates/secrets/kafka-connector/kafka-connector.truststore.jks)
  - The key store file [`kafka-connector.keystore.jks`](../compose-templates/secrets/kafka-connector/kafka-connector.keystore.jks)

- _producer_ mounts [`secrets/producer`](../compose-templates/secrets/producer/) to `/usr/app/secrets` for the following resources:
  - The trust store file [`producer.truststore.jks`](../compose-templates/secrets/producer/producer.truststore.jks)
  - The key store file [`producer.keystore.jks`](../compose-templates/secrets/producer/producer.keystore.jks)

You can regenerate all of them with:

```sh
$ ./generate-secrets.sh
```

## Run

From this directory, follow the same instructions you can find in the [Quick Start](../../README.md#run) section of the main README file.
