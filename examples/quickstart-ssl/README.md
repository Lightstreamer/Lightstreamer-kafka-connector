# Quick Start SSL

This folder contains all the resources needed to launch the [_Quick Start_](../../README.md#quick-start) app configured to establish a secure connection with the Kafka broker.

The [docker-compose.yml](docker-compose.yml) has been revised to enable support for SSL:

- _broker_
  - SSL enabled on port 29094.
  - New environment variables to configure keystore, truststore, client authentication, and secrets:
    - `KAFKA_SSL_TRUSTSTORE_FILENAME`
    - `KAFKA_SSL_TRUSTSTORE_CREDENTIALS`
    - `KAFKA_SSL_KEYSTORE_FILENAME`
    - `KAFKA_SSL_KEYSTORE_CREDENTIALS`
    - `KAFKA_SSL_KEY_CREDENTIALS`
    - `KAFKA_SSL_CLIENT_AUTH`

- _kafka-connector_

  The new version of the [`adapters.xml`](./adapters.xml) includes:
  - Parameter `boostrap.servers` pointing to the SSL endpoint (`broker:29094`).
    ```xml
    <param name="bootstrap.servers">broker:29094</param>
    ```

  - Encryption enabled.
    ```xml
    <param name="encryption.enable">true</param>
    <param name="encryption.protocol">TLSv1.2</param>
    <param name="encryption.hostname.verification.enable">false</param>
    ```

  - Configuration of the truststore to authenticate the broker.
    ```xml
    <param name="encryption.truststore.path">secrets/kafka.connector.truststore.jks</param>
    <param name="encryption.truststore.password">kafka-connector-truststore-password</param>
    ```

  - Configuration of the keystore for client authentication with the broker.
    ```xml
    <param name="encryption.keystore.enable">true</param>
    <param name="encryption.keystore.path">secrets/kafka-connector.keystore.jks</param>
    <param name="encryption.keystore.password">kafka-connector-password</param>
    <param name="encryption.keystore.key.password">kafka-connector-password</param>
    ```

- _producer_
  - Inclusion of the `producer.properties` configuration file for enabling SSL and configuring truststore and keystore:
    
    ```yaml
    # Enable SSL
    security.protocol=SSL
    # Truststore configuration to authenticate the broker
    ssl.truststore.location=/usr/app/secrets/producer.truststore.jks
    ssl.truststore.password=producer-truststore-password
    # Keystore configuration for client authentication with the broker
    ssl.keystore.location=/usr/app/secrets/producer.keystore.jks
    ssl.keystore.password=producer-password
    ssl.key.password=producer-password
    # Disable host name verification
    ssl.endpoint.identification.algorithm=
    ```  
  - Changed target broker to new SSL endpoint (`broker:29094`).

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
