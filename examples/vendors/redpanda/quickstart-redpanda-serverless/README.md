# Redpanda Serverless Quick Start

This folder contains a variant of the [_SSL QuickStart_](../../../quickstart-ssl/README.md#quick-start-ssl) app configured to use [_Redpanda Serverless_](https://redpanda.com/redpanda-cloud/serverless) as the target cluster. You may follow the [instructions](https://docs.redpanda.com/current/deploy/deployment-option/cloud/serverless/) on [Redpanda Docs](https://docs.redpanda.com/current/home/) to perform the following operations:

- Deploy a _Serverless Cluster_.
- Create a user that uses `SCRAM-SHA-256` mechanism.
- Create a topic.
- Allow `All` permissions to the user on the topic.
- Allow `All` permissions to the user on the consumer group `quick-start-group`.

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _Redpanda Serverless_ as follows:

- Removal of the `broker` service, because replaced by the remote cluster

- _kafka-connector_:

  - Definition of new environment variables to configure remote endpoint, credentials, and topic name in the `adapters.xml` through the _variable-expansion_ feature of Lightstreamer:

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

  - Mounting of the [`adapters.xml`](./adapters.xml) file with the following changes:

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

    - Configuration of the authentication settings, with the credentials retrieved from environment variables `username` and `password`:

      ```xml
      <param name="authentication.enable">true</param>
      <param name="authentication.mechanism">SCRAM-SHA-256</param>
      <param name="authentication.username">$env.username</param>
      <param name="authentication.password">$env.password</param>
      ```

    - Update of the parameter `map.<topic>.to` to the environment variable `topic_mapping` (which in turn is composed from env variable `topic`):

      ```xml
      <param name="$env.topic_mapping">item-template.stock</param>
      ```

- _producer_:

   - Update of the parameter `--bootstrap-servers` from the environment variable `bootstrap_server`

   - Update of the parameter `--topic` from the environment variable `topic`
   
   - Provisioning of the `producer.properties` configuration file to enable `SASL/SCRAM` over TLS, with username and password retrieved from the environment variables `username` and `password`:
    
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
$ bootstrap_server=<bootstrap_server> \
  username=<username> \
  password=<password> \
  topic=<topic> \
  ./start.sh 
```

where:
- `<bootstrap_server>` - The bootstrap server address of the Redpanda cluster
- `<username>` and `<password>` - The credentials of the user created from the _Redpanda Console_
- `<topic>` - The name of the topic created on the _rpk_ tool or from the _Redpanda Console_

Then, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).
