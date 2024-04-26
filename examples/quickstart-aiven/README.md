# Quick Start with Aiven for Apache Kafka

This folder contains a variant of the [_Quick Start SSL_](../quickstart-ssl/README.md#quick-start-ssl) app configured to use [_Aiven for Apache Kafka_](https://aiven.io/docs/products/kafka) as the target cluster. You may follow the [_Getting started_](https://aiven.io/docs/products/kafka/get-started) on the Aiven site to perform the following operations:

- create a new _Apache Kafka_ service
- enable the SASL authentication mechanism
- download the CA certificate to create the trust store file with:
  ```sh
  $ keytool -import -file ca.pem -alias CA -keystore secrets/client.truststore.jks
  ```
- create the topic `stocks`

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _Aiven for Apache Kafka_ as follows:

- removal of the `broker` service, because replaced by the remote cluster
- _kafka-connector_:
  - definition of new environment variables to configure remote endpoint, credentials in the `adapters.xml` through the _variable-expansion_ feature of Lightstreamer:
    ```yaml
    ...
    environment:
      - bootstrap_server=${bootstrap_server}
      - username=${username}
      - password=${password}
      - truststore_password=${truststore_password}
    ...
    ```
  - mounting of the local `secrets` folder to `/lightstreamer/adapters/lightstreamer-kafka-connector-${version}/secrets` in the container:
    ```yaml
    volumes:
      ...
      - ./secrets:/lightstreamer/adapters/lightstreamer-kafka-connector-${version}/secrets
    ```
  - adaption of [`adapters.xml`](./adapters.xml) to include:
    - new Kafka cluster address retrieved from the environment variable `bootstrap_server`:
      ```xml
      <param name="bootstrap.servers">$env.bootstrap_server</param>
      ```

    - encryption settings, with the trust store password retrieved from the environment variable `truststore_password`
      ```xml
      <param name="encryption.enable">true</param>
      <param name="encryption.protocol">TLSv1.2</param>
      <param name="encryption.hostname.verification.enable">false</param>
      <param name="encryption.truststore.path">secrets/client.truststore.jks</param>
      <param name="encryption.truststore.password">$env.truststore_password</param>

      ```

    - authentication settings, with the credentials retrieved from environment variables `username` and `password`:
      ```xml
      <param name="authentication.enable">true</param>
      <param name="authentication.mechanism">SCRAM-SHA-256</param>
      <param name="authentication.username">$env.username</param>
      <param name="authentication.password">$env.password</param>
      ```

- _producer_:
   - mounting of the local `secrets` folder to `/usr/app/secrets` in the container:
   
     ```yaml
     volumes:
       - ./secrets:/usr/app/secrets
     ```
   - parameter `--boostrap-servers` retrieved from the environment variable `bootstrap_server`
   - provisioning of the `producer.properties` configuration file to enable `SASL/SCRAM` over TLS, with username, password, and trust store password retrieved from the environment variables `username`, `password`, and `truststore_password`:
    
   ```yaml
   # Configure SASL/SCRAM mechanism
   sasl.mechanism=SCRAM-SHA-256
   # Enable SSL encryption
   security.protocol=SASL_SSL
   # JAAS configuration
   sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${username}" password="${password}";
   # Trust store configuration to authenticate the broker
   ssl.truststore.location=/usr/app/secrets/client.truststore.jks
   ssl.truststore.password=password   
   ssl.endpoint.identification.algorithm=
   ```  

## Run

From this directory, run follow the command:

```sh
$ bootstrap_server=<bootstrap_server> username=<username> password=<password> truststore_password=<truststore_password> ./start.sh 
```

where:
- `bootstrap_server` is the bootstrap server address of the Apache Kafka service
- `username` and `password` are the credentials of the user automatically created from the _Aiven Console_
- `truststore_password` is the password of the trust store file

Then, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).
