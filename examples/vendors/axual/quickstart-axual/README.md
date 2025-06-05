# Quick Start with Axual

This folder contains a variant of the [_Quick Start SSL_](../../../quickstart-ssl/README.md#quick-start-ssl) app configured to use a _shared test cluster_ from [_Axual Platform_](https://axual.com/) as the target Kafka cluster. You may follow the [_Getting started_](https://d2-1pz04.eu1.hs-sales-engage.com/Ctc/ZV+23284/d2-1pz04/Jl22-6qcW7lCdLW6lZ3pJW4JD-dV9lv_NVW5RfzgR5G5Qz2W82dqYb3rM1mzN1qy_pspCYJ-W8r6c_51r7pqcW4ZY1tK3bwTs_W5Pg6cg53g8PlW8JVZ1y7CLwr-N927qSsMD9nSV-yPr561z79gW8wwG-73HZX2qW4SMQFd51XYtMW7s3T0P3hyLSrW4lDWsJ89T42wW4h7XtK4sgHbwW4S5cM43pM5mjW1SKj8K713JtsW3pMBf04HVVrnW6l9R804vWlXrW5VwCq-1LgX4FVchR7J4C_cXMW5G1LVY2lj-glW5ntKZm8rGP2VW7SJCXC6r6D76f7TsPJb04) on the Axual site to perform the following operations:

- Add a new topic `stocks` with:
  -  String key type
  -  JSON value type
  -  _delete_ retention policy
  -  1 partition
  -  1 day of retention time
- Add a new application with:
  - ID: `stocks-application`
  - Name: `stocks-application`
  - Short name: `stocks_app`
  - Application type: _Java_
  - Visibility: _Private_
- Generate a new authentication credentials and take note of them.
- Authorize the application to consume and produce data from/to the `stocks` topic.

The [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _Axual Platform_ as follows:

- Removal of the `broker` service, because replaced by the remote cluster

- _kafka-connector_:

  - Definition of new environment variables to configure remote endpoint, credentials, topic name, and consumer group in the `adapters.xml` through the _variable-expansion_ feature of Lightstreamer:

    ```yaml
    ...
    environment:
      - bootstrap_server=${bootstrap_server}
      - username=${username}
      - password=${password}
      - group_id=${group_id}
      - topic_mapping=map.${topic}.to
    ...
    ```

  - Adaption of [`adapters.xml`](./adapters.xml) to include the following changes:

    - Update of the parameter `bootstrap.servers` to the environment variable `bootstrap_server`:

      ```xml
      <param name="bootstrap.servers">$env.bootstrap_server</param>
      ```

    - Update of the parameter `group.id` to the the environment variable `group_id`:

      ```xml
      <param name="group.id">$env.group_id</param>
      ```

    - Configuration of the encryption settings:

      ```xml
      <param name="encryption.enable">true</param>
      <param name="encryption.protocol">TLSv1.3</param>
      <param name="encryption.hostname.verification.enable">false</param>
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

   - Update of the parameter `--bootstrap-servers` to the environment variable `bootstrap_server`

   - Update of the parameter `--topic` to the environment variable `topic`
   
   - Provisioning of the `producer.properties` configuration file to enable `SASL/SCRAM` over TLS, with username and password retrieved from the environment variables `username` and `password`:
    
     ```yaml
     # Configure SASL/SCRAM mechanism
     sasl.mechanism=SCRAM-SHA-512
     # Enable SSL encryption
     security.protocol=SASL_SSL
     # JAAS configuration
     sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${username}" password="${password}";
     ```  

## Run

From this directory, run follow the command:

```sh
$ bootstrap_server=<bootstrap_server> group_id=<group_id> username=<username> password=<password> topic=<topic> ./start.sh
```

where:
- `<bootstrap_server>` - The bootstrap server address of the Axual cluster
- `<group_id>` - The consumer group ID
- `<username>` and `<password>` - The authentication credentials
- `<topic>` - The name of the topic

> [!TIP]
> You can get the correct values for bootstrap_server, group_id, and topic by looking at the _Cluster connectivity Information_ of the `stocks-application` from the Axual _Self-service_ portal.

Then, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).
