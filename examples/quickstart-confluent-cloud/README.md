# Quick Start with Confluent Cloud

This folder contains a variant of the [_Quick Start_](../quickstart-ssl/README.md#quick-start-ssl) app configured to use _Confluent Cloud_ as the target Kafka cluster.

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with Confluent Cloud as follows:

- Removal of the _broker_ service, because replaced by the remote Kafka cluster.

- _kafka-connector_
  - Definition of new environment variables to configure the remote endpoint and credentials in the `adapters.xml` trough the _variable-expansion_ feature of Lightstreamer.
    ```yaml
    ...
    environment:
      - bootstrap_server=${bootstrap_server}
      - api_key=${api_key}
      - secret=${secret}
    ...
    ```
  - Adaption of [`adapters.xml`](./adapters.xml) to include:
    - New Kafka cluster address retrieved from the environment variable `bootstrap_server`:
      ```xml
      <param name="bootstrap.servers">$env.bootstrap_server</param>
      ```

    - Encryption settings:
      ```xml
      <param name="encryption.enable">true</param>
      <param name="encryption.protocol">TLSv1.2</param>
      <param name="encryption.hostname.verification.enable">true</param>
      ```

    - Authentication settings, with credentials retrieved from environment variables `api_key` and `secret`:
      ```xml
      <param name="authentication.enable">true</param>
      <param name="authentication.mechanism">PLAIN</param>
      <param name="authentication.username">$env.api_key</param>
      <param name="authentication.password">$env.secret</param>
      ```

- _producer_

   Provisioning of the `producer.properties` configuration file to enable `SASL/PLAN` over TLS, with username and password retrieved from the environment variables `api_key` and `secret`:
    
    ```yaml
    # Configure SASL/PLAIN mechanism
    sasl.mechanism=PLAIN
    # Enable SSL encryption
    security.protocol=SASL_SSL
    # JAAS configuration
    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${api_key}" password="${secret}";
    ```  

## Run

From this directory, run follow the command:

```sh
api_key=<API.key> secret=<secret> bootstrap_server=<bootstrap_server> ./start.sh 
```

where 
- `API.key` and `secret` are the credentials generated on the _Confluent CLI_ or from the _Confluent Cloud Console_.
- `bootstrap_server` is the Kafla cluster address.

Then, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).

