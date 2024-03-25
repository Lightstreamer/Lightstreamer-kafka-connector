# Quick Start with Redpanda Serverless

This folder contains a variant of the [_Quick Start SSL_](../quickstart-ssl/README.md#quick-start-ssl) app configured to use [_Redpanda Serverless_](https://redpanda.com/redpanda-cloud/serverless) as the target cluster. You may follow the [instructions](https://docs.redpanda.com/current/deploy/deployment-option/cloud/serverless/) on [Redpanda Docs](https://docs.redpanda.com/current/home/) to perform the following operations:

- deploy a _Serverless Cluster_
- create a user that uses `SCRAM-SHA-256` mechanism
- create a topic
- allow `All` permissions to the user on the topic
- allow `All` permissions to the user on the consumer group `quick-start-group`

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _Redpanda Serverless_ as follows:

- removal of the `broker` service, because replaced by the remote cluster
- _kafka-connector_:
  - definition of new environment variables to configure remote endpoint, credentials, and topic name in the `adapters.xml` through the _variable-expansion_ feature of Lightstreamer:
    ```yaml
    ...
    environment:
      - bootstrap_server=${bootstrap_server}
      - username=${username}
      - password=${password}
        # adapters.xml uses env variable "topic_mapping", built from env variable "topic"
      - topic_mapping=map.${topic}.to
    ...
    ```
  - adaption of [`adapters.xml`](./adapters.xml) to include:
    - new Kafka cluster address retrieved from the environment variable `bootstrap_server`:
      ```xml
      <param name="bootstrap.servers">$env.bootstrap_server</param>
      ```

    - encryption settings:
      ```xml
      <param name="encryption.enable">true</param>
      <param name="encryption.protocol">TLSv1.2</param>
      <param name="encryption.hostname.verification.enable">true</param>
      ```

    - authentication settings, with the credentials retrieved from environment variables `username` and `password`:
      ```xml
      <param name="authentication.enable">true</param>
      <param name="authentication.mechanism">SCRAM-SHA-256</param>
      <param name="authentication.username">$env.username</param>
      <param name="authentication.password">$env.password</param>
      ```
    - parameter `map.<topic>.to` built from env variable `topic_mapping`, composed from env variable `topic`
      ```xml
      <param name="$env.topic_mapping">item-template.stock</param>
      ```

- _producer_:
   - parameter `--boostrap-servers` retrieved from the environment variable `bootstrap_server`
   - parameter `--topic` retrieved from the environment variable `topic`
   - provisioning of the `producer.properties` configuration file to enable `SASL/SCRAM` over TLS, with username and password retrieved from the environment variables `username` and `password`:
    
   ```yaml
   # Configure SASL/PLAIN mechanism
   sasl.mechanism=SCRAM-SHA-256
   # Enable SSL encryption
   security.protocol=SASL_SSL
   # JAAS configuration
   sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${username}" password="${password}";
   ```  

## Run

From this directory, run follow the command:

```sh
$ bootstrap_server=<bootstrap_server> username=<username> password=<password> topic=<topic> ./start.sh 
```

where 
- `bootstrap_server` is the bootstrap server address of the Redpanda cluster
- `username` and `password` are the credentials of the user created from the _Redpanda Console_
- `topic` is the name of the topic created on the _rpk_ tool or from the _Redpanda Console_

Then, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).
