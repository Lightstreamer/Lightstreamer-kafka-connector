# SSL quickstart

This folder contains a variant of the [_Quickstart_](../../README.md#quick-start-set-up-in-5-minutes) app configured to establish a secure connection with the Kafka broker.

The [docker-compose.yml](docker-compose.yml) file has been revised to enable support for SSL, as follows:

- _broker_:

  - Enabling of SSL on port `29094`

  - Definition of new environment variables to configure key store, trust store, client authentication, and secrets:

    - `KAFKA_SSL_TRUSTSTORE_FILENAME`
    - `KAFKA_SSL_TRUSTSTORE_TYPE`
    - `KAFKA_SSL_TRUSTSTORE_CREDENTIALS`
    - `KAFKA_SSL_KEYSTORE_FILENAME`
    - `KAFKA_SSL_KEYSTORE_TYPE`
    - `KAFKA_SSL_KEYSTORE_CREDENTIALS`
    - `KAFKA_SSL_KEY_CREDENTIALS`
    - `KAFKA_SSL_CLIENT_AUTH`

- _kafka-connector_:

  Mounting of the [`adapters.xml`](./adapters.xml) file with the following changes:

  - Update of the parameter `bootstrap.servers` to the new SSL endpoint (`broker:29094`):
  
    ```xml
    <param name="bootstrap.servers">broker:29094</param>
    ```

  - Configuration of the encryption settings:

    ```xml
    <param name="encryption.enable">true</param>
    <param name="encryption.protocol">TLSv1.2</param>
    <param name="encryption.hostname.verification.enable">true</param>
    ```

  - Configuration of the trust store to authenticate the broker:

    ```xml
    <param name="encryption.truststore.path">secrets/kafka-connector.truststore.p12</param>
    <param name="encryption.truststore.password">kafka-connector-truststore-password</param>
    ```

  - Configuration of the key store for client authentication with the broker:

    ```xml
    <param name="encryption.keystore.enable">true</param>
    <param name="encryption.keystore.path">secrets/kafka-connector.keystore.p12</param>
    <param name="encryption.keystore.password">kafka-connector-keystore-password</param>
    ```

- _producer_:

  - Update of the parameter `--bootstrap-servers` to the new SSL endpoint (`broker:29094`)

  - Provisioning of the `producer.properties` configuration file to enable SSL support:
  
    ```yaml
    # Enable SSL
    security.protocol=SSL
    # Trust store configuration to authenticate the broker
    ssl.truststore.location=/usr/app/secrets/producer.truststore.p12
    ssl.truststore.type=PKCS12
    ssl.truststore.password=producer-truststore-password
    # Key store configuration for client authentication with the broker
    ssl.keystore.location=/usr/app/secrets/producer.keystore.p12
    ssl.keystore.type=PKCS12
    ssl.keystore.password=producer-keystore-password
    # Enable host name verification of the broker certificate
    ssl.endpoint.identification.algorithm=https
    ```  

In addition, all services reference the local [`secrets`](../compose-templates/secrets/) folder to retrieve their secrets. In particular:

- _broker_ mounts [`secrets/broker`](../compose-templates/secrets/broker/) to `/etc/kafka/secrets` for the following resources:

  - The trust store file [`broker.truststore.p12`](../compose-templates/secrets/broker/broker.truststore.p12)

  - The key store file [`broker.keystore.p12`](../compose-templates/secrets/broker/broker.keystore.p12)
  
  - The credentials files [`broker_keystore_credentials`](../compose-templates/secrets/broker/broker_keystore_credentials) and [`broker_key_credentials`](../compose-templates/secrets/broker/broker_key_credentials) (both contain the key store password, since a PKCS12 key store uses the same password for the private key), and [`broker_truststore_credentials`](../compose-templates/secrets/broker/broker_truststore_credentials)

- _kafka-connector_ mounts [`secrets/kafka-connector`](../compose-templates/secrets/kafka-connector/) to `LS_KAFKA_CONNECTOR_HOME/secrets` for the following resources:

  - The trust store file [`kafka-connector.truststore.p12`](../compose-templates/secrets/kafka-connector/kafka-connector.truststore.p12)

  - The key store file [`kafka-connector.keystore.p12`](../compose-templates/secrets/kafka-connector/kafka-connector.keystore.p12)

- _producer_ mounts [`secrets/producer`](../compose-templates/secrets/producer/) to `/usr/app/secrets` for the following resources:

  - The trust store file [`producer.truststore.p12`](../compose-templates/secrets/producer/producer.truststore.p12)
  
  - The key store file [`producer.keystore.p12`](../compose-templates/secrets/producer/producer.keystore.p12)

You can regenerate all of them with:

```sh
$ ./generate-secrets.sh
```

The script reuses the existing CA, so the trust stores stay valid. To also create a new CA, run `./generate-secrets.sh --new-ca`. To see when the certificates expire without changing anything, run `./generate-secrets.sh --check`: it fails if any certificate expires within 90 days (configurable with the `CHECK_DAYS` environment variable).

## Run

From this directory, follow the same instructions you can find in the [Quick start](../../README.md#quick-start-set-up-in-5-minutes) section of the main README file.
