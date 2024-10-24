# Quick Start with Confluent Cloud

This folder contains a variant of the [_Quick Start SSL_](../../../quickstart-ssl/README.md#quick-start-ssl) app configured to use [Confluent Cloud](https://www.confluent.io/confluent-cloud/?utm_campaign=tm.pmm_cd.cwc_partner_Lightstreamer_generic&utm_source=Lightstreamer&utm_medium=partnerref) as the target Kafka cluster.

> [!TIP]
> Don't have a Confluent Cloud account yet? Start [your free trial of Confluent Cloud](https://www.confluent.io/confluent-cloud/tryfree/?utm_campaign=tm.pmm_cd.cwc_partner_Lightstreamer_tryfree&utm_source=Lightstreamer&utm_medium=partnerref) today. New signups receive $400 to spend during their first 30 days.

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _Confluent Cloud_ as follows:

- Removal of the `broker` service, because replaced by the remote Kafka cluster.
- _kafka-connector_:
  - Definition of new environment variables to configure remote endpoint, credentials, and topic name in the `adapters.xml` through the _variable-expansion_ feature of Lightstreamer:
    ```yaml
    ...
    environment:
      - bootstrap_server=${bootstrap_server}
      - api_key=${api_key}
      - secret=${secret}
        # adapters.xml uses env variable "topic_mapping", built from env variable "topic"
      - topic_mapping=map.${topic}.to
    ...
    ```
  - Adaption of [`adapters.xml`](./adapters.xml) to include the following changes:
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

    - Configuration of the authentication settings, with the credentials retrieved from environment variables `api_key` and `secret`:
      ```xml
      <param name="authentication.enable">true</param>
      <param name="authentication.mechanism">PLAIN</param>
      <param name="authentication.username">$env.api_key</param>
      <param name="authentication.password">$env.secret</param>
      ```

    - Update of the parameter `map.<topic>.to` to the environment variable `topic_mapping` (which in turn is composed from env variable `topic`)
      ```xml
      <param name="$env.topic_mapping">item-template.stock</param>
      ```

- _producer_:
   - Update of the parameter `--boostrap-servers` to the environment variable `bootstrap_server`
   - Update of the parameter `--topic` to the environment variable `topic`
   - Provisioning of the `producer.properties` configuration file to enable `SASL/PLAN` over TLS, with username and password retrieved from the environment variables `api_key` and `secret`:
    
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
$ bootstrap_server=<bootstrap_server> api_key=<API.key> secret=<secret> topic=<topic> ./start.sh 
```

where:
- `bootstrap_server` is the Kafla cluster address.
- `API.key` and `secret` are the credentials generated on the _Confluent CLI_ or from the _Confluent Cloud Console_.
- `topic` is the name of the topic.

Then, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).
