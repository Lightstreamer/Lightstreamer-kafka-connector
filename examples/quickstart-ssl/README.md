# Quick Start SSL

This folder contains all the resources needed to launch the [_Quick Start_](../../README.md#quick-start) app configured to establish a secure connection with the Kafka broker.

In particular, the [docker-compose.yml](docker-compose.yml) has been revised to enable support for SSL:

- _broker_
  - SSL enabled on port 29094.
  - New environment variables to configure keystore and secrets:
    - `KAFKA_SSL_KEYSTORE_FILENAME`
    - `KAFKA_SSL_KEYSTORE_CREDENTIALS`
    - `KAFKA_SSL_KEY_CREDENTIALS`

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

  - Truststore enabled.
    ```xml
    <param name="encryption.truststore.type">PKCS12</param>
    <param name="encryption.truststore.path">secrets/kafka.connector.truststore.pkcs12</param>
    <param name="encryption.truststore.password">password</param>
    ```

- _producer_
  - Inclusion of the `producer.properties` configuration file for enabling SSL and truststore:
    
    ```yaml
    security.protocol=SSL
    ssl.truststore.type=PKCS12
    ssl.truststore.location=/usr/app/kafka.client.truststore.pkcs12
    ssl.truststore.password=password
    ssl.endpoint.identification.algorithm=  
    ```  

  - Changed target broker to new SSL endpoint (`broker:29094`).

In addition, all services reference the local [`secrets`](secrets/) folder to retrieve their secrets:

In particular, 

- _broker_ mounts [`secrets/broker`](secrets/broker/) to `/etc/kafka/secrets` for:
  - the keystore file [`kafka.broker.keystore.pkcs12`](secrets/broker/kafka.broker.keystore.pkcs12);
  - the credentials files [`broker_keystore_credentials`](secrets/broker/broker_keystore_credentials) and [`broker_key_credentials`](secrets/broker/broker_key_credentials).

- _kafka-connector_ mounts [`secrets/kafka-connector`](secrets/kafka-connector/) to `LS_KAFKA_CONNECTOR_HOME/secrets` for the truststore file [`kafka.connector.truststore.pkcs12`](secrets/kafka-connector/kafka.connector.truststore.pkcs12).

- _producer_ mounts [`secrets/client`](secrets/client/) to `/usr/app/secrets` for the truststore file [`kafka.client.truststore.pkcs12`](secrets/client/kafka.client.truststore.pkcs12).

You can regenerate all of them with:

```sh
./create-ca.sh
```

## Run

From this directory, follow the same instructions you can in the [Quick Start](../../README.md#run) section of the main README file.
