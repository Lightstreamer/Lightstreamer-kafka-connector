# Lightstreamer Kafka Connector

- [Lightstreamer Kafka Connector](#lightstreamer-kafka-connector)
  - [Introduction](#introduction)
  - [Features](#features)
  - [Quick Start](#quick-start)
    - [Run](#run)
  - [Installation](#installation)
    - [Requirements](#requirements)
    - [Deploy](#deploy)
    - [Configure](#configure)
      - [Connection with Confluent Cloud](#connection-with-confluent-cloud)
    - [Start](#start)
      - [Publishing with Confluent Cloud](#publishing-with-confluent-cloud)
  - [Configuration](#configuration)
    - [Global Settings](#global-settings)
      - [`adapter_conf['id']` - _Kafka Connector identifier_](#adapter_confid---kafka-connector-identifier)
      - [`adapter_class`](#adapter_class)
      - [`logging.configuration.path`](#loggingconfigurationpath)
    - [Connection Settings](#connection-settings)
      - [General Parameters](#general-parameters)
        - [`data_provider['name']` - _Kafka Connection Name_](#data_providername---kafka-connection-name)
        - [`enable`](#enable)
        - [`bootstrap.servers`](#bootstrapservers)
        - [`group.id`](#groupid)
      - [Record Evaluation](#record-evaluation)
        - [`record.extraction.error.strategy`](#recordextractionerrorstrategy)
        - [`key.evaluator.type` and `value.evaluator.type`](#keyevaluatortype-and-valueevaluatortype)
        - [`key.evaluator.schema.path` and `value.evaluator.schema.path`](#keyevaluatorschemapath-and-valueevaluatorschemapath)
        - [`key.evaluator.schema.registry.enable` and `value.evaluator.schema.registry.enable`](#keyevaluatorschemaregistryenable-and-valueevaluatorschemaregistryenable)
      - [Encryption Parameters](#encryption-parameters)
        - [`encryption.enable`](#encryptionenable)
        - [`encryption.protocol`](#encryptionprotocol)
        - [`encryption.enabled.protocols`](#encryptionenabledprotocols)
        - [`encryption.cipher.suites`](#encryptionciphersuites)
        - [`encryption.truststore.path`](#encryptiontruststorepath)
        - [`encryption.truststore.password `](#encryptiontruststorepassword-)
        - [`encryption.truststore.type`](#encryptiontruststoretype)
        - [`encryption.hostname.verification.enable`](#encryptionhostnameverificationenable)
        - [`encryption.keystore.enable`](#encryptionkeystoreenable)
        - [`encryption.keystore.path`](#encryptionkeystorepath)
        - [`encryption.keystore.password`](#encryptionkeystorepassword)
        - [`encryption.keystore.key.password`](#encryptionkeystorekeypassword)
        - [`encryption.keystore.type`](#encryptionkeystoretype)
        - [Complete Encryption Configuration Example](#complete-encryption-configuration-example)
      - [Broker Authentication Parameters](#broker-authentication-parameters)
        - [`authentication.enable`](#authenticationenable)
        - [`authentication.mechanism`](#authenticationmechanism)
          - [`PLAIN`](#plain)
          - [`SCRAM-256`](#scram-256)
          - [`SCRAM-512`](#scram-512)
          - [`GSSAPI`](#gssapi)
      - [Schema Registry](#schema-registry)
        - [`schema.registry.url`](#schemaregistryurl)
        - [Encryption Parameters](#encryption-parameters-1)
      - [Topic Mapping](#topic-mapping)
        - [template](#template)
    - [Metadata Adapter Customization](#metadata-adapter-customization)

## Introduction

The _Lightstreamer Kafka Connector_ is a ready-made pluggable Lighstreamer Adapter that enables event streaming from a Kafka broker to the internet.

[Insert Diagram here]

With Kafka Connector, any internet client connected to the Lightstreamer Server can consume events from Kafka topics like any other Kafka client. The Connector takes care of processing records received from Kafka to adapt and route them as real-time updates for the clients.

The Kafka Connector allows to move high volume data out of Kafka by leveraging the battle-tested ability of the Lightstreamer real-time engine to deliver live data reliably and efficiently over internet protocols.

## Features

[...] TO TDO

## Quick Start

To rapidly showcase the functioning of the Lighstreamer Kafka Connector, the [`examples/quickstart`](examples/quickstart/) folder hosts all the stuff required to set up a quickstart app to display real-time market data received from Lightstreamer Server. The app is a modified version of the [Stock List](https://github.com/Lightstreamer/Lightstreamer-example-StockList-client-javascript?tab=readme-ov-file#basic-stock-list-demo---html-client) demo.

![quick-start-diagram](quickstart-diagram.png)

As you can see from the diagram above, in this variant the stream of simulated market events is injected from Kafka to the web client through the Ligthstreamer Kafka Connector.

To provide a complete stack, the app is based on _Docker Compose_. The [Docker Compose file](examples/quickstart/docker-compose.yml) comprises the following services:

1. A Kafka broker, based on the [Confluent Local Docker Image](confluentinc/confluent-local:latest).      
2. Lighstreamer Server with Kafka Connector, based on the [Lightstreamer Kafka Connector Docker image example](examples/docker-image/).
3. The web client, mounted on the Lightreamer service.
4. A native Kafka Producer, based on the provided [Dockerfile.producer](examples/quickstart/Dockerfile.producer) file and [kafka-connector-samples](kafka-connector-samples/) submodule of this repository.

### Run

To run the app:

1. Make sure you have Docker, Docker Compose, and Java 17 (or later) installed on your local machine.
2. From the [`examples/quickstart`](examples/quickstart/) folder, run the command:
   
   ```sh
   ./start.sh
   ...
   ⠏ Network quickstart_default  Created
   ✔ Container broker            Started
   ✔ Container producer          Started
   ✔ Container kafka-connector   Started
   ...
   Service started. Now you can point your browser to http://localhost:8080/QuickStart to see real-time data.
   ...
   ```

3. Once all containers are ready, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).
   
   After a few moments, the user interface starts displaying the real-time stock data.

4. To shutdown Docker Compose and clean up all temporary resources:
 
   ```sh
   ./stop.sh
   ```

## Installation

This section will guide you through the installation of the Lightstreamer Kafka Connector to get it up and running in a very short time.

### Requirements

- JDK version 17 or later.
- [Lightstreamer Server](https://lightstreamer.com/download/) version 7.4.2 or later (check the `LS_HOME/GETTING_STARTED.TXT` file for the instructions).
- A running Kafka broker or Kafka Cluster.

### Deploy

Get the deployment package from the [latest release page](releases). Alternatively, check out this repository and run the following command from the project root;

```sh
./gradlew distribuite
```

which generates the `lightstreamer-kafka-connector-<version>.zip` bundle under the `deploy` folder.

Then, unzip it into the `adapters` folder of the Lightstreamer Server installation.
Check that the final Lightstreamer layout looks like the following:

```sh
LS_HOME/
...
├── adapters
│   ├── lightstreamer-kafka-connector-<version>
│   │   ├── README.md
│   │   ├── adapters.xml
│   │   ├── log4j.properties
│   │   ├── lib
│   └── welcome_res
...
├── audit
├── bin
...
```

### Configure

Before starting the Kafka Connector, you need to properly configure the `LS_HOME/adapters/lightstreamer-kafka-connector/adapters.xml` file. For convenience, the package comes with a predefined configuration (the same used in the [Quick Start](#quick-start) app), which can be customized in all its aspects as per your requirements. Obviously, you may add as many different connection configurations as desired to fit your needs.

To quickly complete the installation and verify the successful integration with Kafka, edit the _data_provider_ block `QuickStart` in the file as follows:

- Update the [`bootstrap.servers`](#bootstrapservers) parameter with the connection string of Kafka.

  ```xml
  <param name="bootstrap.servers">kafka.connection.string</param>
  ```
- Optionally customize the `LS_HOME/adapters/lightstreamer-kafka-connectors/log4j.properties` file (the current settings produce the additional `quickstart.log` file).

You can get more details about all possible settings in the [Configuration](#configuration) section.

#### Connection with Confluent Cloud

If your target Kafka cluster is Confluent Cloud, you also need to properly configure `TLS 1.2` encryption and `SASL_PLAIN` authentication, as follows:

```xml
<param name="encryption.enable">true</param>
<param name="encryption.protocol">TLSv1.2</param>
<param name="encryption.hostname.verification.enable">true</param>

<param name="authentication.enable">true</param>
<param name="authentication.mechanism">PLAIN</param>
<param name="authentication.username">API.key</param>
<param name="authentication.password">secret</param>
...
```

where you have to replace `API.key` and `secret` with the _API Key_ and _secret_ generated on the _Confluent CLI_ or from the _Confluent Cloud Console_.

### Start

1. Launch Lightstreamer Server.

2. Attach a Lightstreamer Consumer.

   The [`kafka-connector-samples`](kafka-connector-samples/) submodule hosts a simple 
   Lightstreamer Java client that can be used to test the consumption of Kafka events from any Kafka topics.

   Since a generic Ligthstreamer client needs to subscribe to one or more items to receive real-time updates, the Kafka Connector has to offer proper support to realize a mapping between Kafka topics and Lighstreamer items.

   The `QuickStart` factory configuration comes with a simple mapping through the following settings:

   - An item template
     ```xml
     <param name="item-template.stock">stock-#{index=KEY}</param>
     ```
     
     which defines the general format name of the items a client must subscribe to to receive updates from the Kafka Connector. The optional _Bindable Selector Keys_ syntax used here, denoted within `#{...}`, can bind 
     every part of a Kafka Record to a variable set of input parameters. In this case, the input parameter `index` is bound to the `KEY` predefined constant, which extracts the key part of Kafka records.

   - A topic mapping
     ```xml
     <param name="map.stocks.to">item-template.stock</param>
     ```
     which maps the topic `stocks` to the item names.

   This configuration instructs the Kafka Connector to analyze every single event published to the topic `stocks` and check if it matches against any item subscribed by the client as:
      
   - `stock`, the item `stock`.
   - `stock-[index=1]`, an item with the parameter `index` bound to a record key equal to `1`.
   - `stock-[index=2]`, an item with the parameter `index` bound to a record key equal to `2`.
   - ...
   - `stock-[index=<any record key>]`
   
   The _Kafka Connector_ will then route the event to any matched item.

   In addition, the following section defines how the record is mapped to the tabular form of Lightstreamer fields, by using an intuitive set of _Selector Keys_ (denoted with `#{..}`) through which each part of a Kafka Record can be extracted. In this case, the VALUE predefined constant extracts the value part of Kakfa records.

   ```xml
   <param name="field.stock_name">#{VALUE.name}</param>
   <param name="field.last_price">#{VALUE.last_price}</param>
   <param name="field.ask">#{VALUE.ask}</param>
   <param name="field.ask_quantity">#{VALUE.ask_quantity}</param>
   <param name="field.bid">#{VALUE.bid}</param>
   <param name="field.bid_quantity">#{VALUE.bid_quantity}</param>
   <param name="field.pct_change">#{VALUE.pct_change}</param>
   <param name="field.min">#{VALUE.min}</param>
   <param name="field.max">#{VALUE.max}</param>
   <param name="field.ref_price">#{VALUE.ref_price}</param>
   <param name="field.open_price">#{VALUE.open_price}</param>
   <param name="field.item_status">#{VALUE.item_status}</param>
   ```

   This way, the routed event is transformed into a flat structure, which can be forwarded to the clients.

   Before launching the consumer, you first need to build it with the command:

   ```sh
   ./gradlew distribuiteConsumer 
   ```

   which generates the `lightstreamer-kafka-connector-samples-consumer-all-<version>.jar` under the `deploy` folder.

   Then, launch it with:
   
   ```sh
   java -jar deploy/lightstreamer-kafka-connector-samples-consumer-all-<version>.jar --address http://localhost:8080 --adapter-set KafkaConnector --data-adapter QuickStart --items stock-[index=1] --fields ask,bid,min,max
   ```

   As you can see, you have to specify a few parameters:

   - `--address`, the Lightstreamer Server address.
   - `--adapter-set`, the name of the requested Adapter Set, which triggers Ligthtreamer to activate the Kafka Connector deployed into the `adapters` folder.
   - `--data-adapter`, the name of the requested Data Adapter, which identifies the selected Kafka connection configuration.
   - `--items`, the list of items to subscribe to.
   - `--fields`, the list of requested fields for the items.

   **NOTE:** As the _Lightstreamer Kafka Connector_ is built around the [_Lightreamer Java In-Process Adapter SDK_](https://github.com/Lightstreamer/Lightstreamer-lib-adapter-java-inprocess), every remote client based on any _Lightstreamer Client SDK_ can therefore interact with it.

3. Publish Events.

   The [`kafka-connector-samples`](kafka-connector-samples/) submodule hosts a simple Kafka producer to publish simulated market events for the _QuickStart_ app.

   Before launching the producer, you first need to build it. Open a new shell and execute the command:

   ```sh
   ./gradlew distribuiteProducer 
   ```

   which generates the `lightstreamer-kafka-connector-samples-producer-all-<version>.jar` under the `deploy` folder.

   Then, launch it with:

   ```sh
   java -jar deploy/lightstreamer-kafka-connector-samples-producer-all-<version>.jar --bootstrap-servers <kafka.connection.string> --topic stocks
   ```

   #### Publishing with Confluent Cloud

   If your target Kafka cluster is Confluent Cloud, you also need to provide a properties file that includes encryption and authentication settings, as follows:

   ```java
   security.protocol=SASL_SSL
   sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='API.key' password='secret';
   sasl.mechanism=PLAIN
   ...
   ```

   where you have to replace `API.key` and `secret` with the _API Key_ and _secret_ generated on the _Confluent CLI_ or from the _Confluent Cloud Console_.

   ```sh
   java -jar deploy/lightstreamer-kafka-connector-samples-producer-all-<version>.jar --bootstrap-servers <kafka.connection.string> --topic stocks --config-file <path/to/config/file>
   ```

4. Check Consumed Events.

   After starting the publisher, you should immediately see the real-time updates flowing from the consumer shell:

   ![consumer_video](consumer.gif)

## Configuration

As already anticipated, the Lightstreamer Kafka Connector is a Lightstreamer Adapter Set, which means it is made up of a Metadata Adapter and one or more Data Adapters, whose settings are defined in the `LS_HOME/adapters/lightstreamer-kafka-connector/adapters.xml` file.

The following sections will guide you through the configuration details.

### Global Settings

#### `adapter_conf['id']` - _Kafka Connector identifier_

  _Mandatory_. The `id` attribute of the `adapters_conf` root tag defines the _Kafka Connector identifier_, which will be used by the Clients to request this Adapter Set while setting up the connection to a Lighstreamer Server through a _LightstreamerClient_ object.

  The factory value is set to `KafkaConnector` for convenience, but you are free to change it as per your requirements.

  Example:

  ```xml
  <adapters_conf id="KafkaConnector">
  ```

#### `adapter_class`

_Mandatory_. The `adapter_class` tag, specified inside the `metadata_provider` block, defines the Java class name of the Metadata Adapter.

The factory value is set to `com.lightstreamer.kafka_connector.adapters.ConnectorMetadataAdapter`, which implements the Kafka Connector logic.

It is possible to provide a custom implementation by extending the `KafakConnectorMetadataAdapter` class: just package your new class in a jar file and deploy it along with all required dependencies into the `LS_HOME/adapters/lightstreamer-kafka-connector/lib` folder.

See the [Metadata Adapter Customization](#meta) section for more details.

Example:

```xml
...
<metadata_provider>
    ...
    <adapter_class>your.custom.class</adapter_class>
    ...
</metadata_provider>
...
  ```

#### `logging.configuration.path`

_Mandatory_. The path of the [reload4j](https://reload4j.qos.ch/) configuration file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector`).

The parameter is specified inside the `metadata_provider` block.

The factory value points to the predefined file `LS_HOME/adapters/lightstreamer-kafka-connector/log4g.properties`.

Example:

```xml
...
<metadata_provider>
    ...
    <param name="logging.configuration.path">log4j.properties</param>
    ...
</metadata_provider>
...
```

### Connection Settings

The Lightstreamer Kafka Connector allows the configuration of separate independent connections to different Kafka clusters.

Every single connection is configured via the definition of its own Data Adapter through the `data_provider` block. At least one connection must be provided.

Since the Kafka Connector manages the physical connection to Kafka by wrapping an internal Kafka Consumer, many configuration settings in the Data Adapter are identical to those required by the usual Kafka Consumer configuration.

#### General Parameters

##### `data_provider['name']` - _Kafka Connection Name_

_Optional_. The `name` attribute of the `data_provider` tag defines _Kafka Connection Name_, which will be used by the Clients to request real-time data from this specific Kafka connection through a _Subscription_ object.

The connection name is also used to group all logging messages belonging to the same connection

Example:

```xml
<data_provider name="BrokerConnection">
```

Default value: `DEFAULT`, but only one `DEFAULT` configuration is permitted.

##### `enable`

_Optional_. Enable this connection configuration. Can be one of the following:
- `true`
- `false`

Default value: `true`.

If disabled, Lightstreamer Server will automatically deny every subscription made to this connection.

Example:

```xml
<param name="enable">false</param>
```

##### `bootstrap.servers`

_Mandatory_. The Kafka Cluster bootstrap server endpoint expressed as the list of host/port pairs used to establish the initial connection.

The parameter sets the value of the [`bootstrap.servers`](https://kafka.apache.org/documentation/#consumerconfigs_bootstrap.) key to configure the internal Kafka Consumer.

Example:

```xml
<param name="bootstrap.servers">broker:29092,broker:29093</param>
```

##### `group.id`

_Optional_. The name of the consumer group this connection belongs to.

The parameter sets the value for the [`group.id`](https://kafka.apache.org/documentation/#consumerconfigs_group.id) key to configure the internal Kafka Consumer.

Default value: _KafkaConnector Identifier_ + _Connection Name_ + _Randomly generated suffix_.

```xml
<param name="group.id">kafka-connector-group</param>
```


#### Record Evaluation

The Lightstreamer Kafka Connector offers wide support for deserializing Kafka records. Currently, it allows the following formats:

- _String_
- _Avro_
- _JSON_

In particular, the Kafka Connector supports message validation for Avro and JSON, which can be specified through:

- Local schema files.
- The Confluent Schema Registry.

Kafka Connector supports independent deserialization of keys and values, which means that:

- Key and value can have different formats.
- Message validation against the Confluent Schema Registry can be enabled separately for the Kafka key and Kafka value (through [`key.evaluator.schema.registry.enable`](#) and `value.evaluator.schema.registry.enable`)
- Message validation against local schema files must be specified separately for key and value (through the `key.evaluator.schema.path` and `value.evaluator.schema.path`)

**NOTE** For Avro, schema validation is required, therefore either a local schema file must be provided or the Confluent Schema Registry must be enabled.

In case of a validation failure, the Connector can react by ...

##### `record.extraction.error.strategy`

_Optional_. The error handling strategy to be used if an error occurs while extracting data from incoming records. Can be one of the following:

- `IGNORE_AND_CONTINUE`, ignore the error and continue to process the next record.
- `FORCE_UNSUBSCRIPTION`, stop processing records and force unsubscription of the items requested by all the Clients subscribed to this connection.

Default value: `IGNORE_AND_CONTINUE`.

Example:

```xml
<param name="record.extraction.error.strategy">FORCE_UNSUBSCRIPTION</param>
```

##### `key.evaluator.type` and `value.evaluator.type`

_Optional_. The format to be used to deserialize respectively the key and value of a Kafka record. Can be one of the following:

- `AVRO`
- `JSON`
- `STRING`
- `INTEGER`
- `BOOLEAN`
- `BYTE_ARRAY`
- `BYTE_BUFFER`
- `BYTES`
- `DOUBLE`
- `FLOAT`
- `LONG`
- `SHORT`
- `UUID`

Default value: `STRING`

Examples:

```xml
<param name="key.evaluator.type">INTEGER</param>
<param name="value.evaluator.type">JSON</param>
```

##### `key.evaluator.schema.path` and `value.evaluator.schema.path`

The path of the local schema file for message validation respectively of the Kafka key and the Kafa value.

```xml
<param name="key.evaluator.schema.path">schema/record_key.json</param>
<param name="value.evaluator.schema.path">schemas/record_value.json</param>
```

##### `key.evaluator.schema.registry.enable` and `value.evaluator.schema.registry.enable`

Enable the use of the [Confluent Schema Registry](#schema-registry) for message validation respectively of the Kafka key and the Kafa value.

Default value: `false`

```xml
<param name="key.evaluator.schema.registry.enable">true</param>
<param name="value.evaluator.schema.registry.enable">false</param>
```

#### Encryption Parameters

A TCP secure connection to the Kafka cluster is configured through parameters with the `encryption` prefix.

##### `encryption.enable`

_Optional_. Enable encryption of this connection. Can be one of the following:
- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="encryption.enable">true</param>
```

##### `encryption.protocol`

_Optional_. The SSL protocol to be used. Can be one of the following:
- `TLSv1.2`
- `TLSv1.3`

Default value: `TLSv1.3` when running on Java 11 or newer, `TLSv1.2` otherwise.

Example:

```xml
<param name="encryption.protocol">TLSv1.2</param>
```

##### `encryption.enabled.protocols`

_Optional_. The list of enabled secure communication protocols.

Default value: `TLSv1.2,TLSv1.3` when running on Java 11 or newer, `TLSv1.2` otherwise.

Example:

```xml
<param name="encryption.enabled.protocols">TLSv1.3</param>
```

##### `encryption.cipher.suites`

_Optional_. The list of enabled secure cipher suites.

Default value: all the available cipher suites in the running JVM.

Example:

```xml
<param name="encryption.cipher.suites">TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA</param>
```

##### `encryption.truststore.path`

_Optional_. The path of the trust store file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector`).

Example:

```xml
<param name="encryption.truststore.path">secrets/kafka.connector.truststore.jks</param>
```

##### `encryption.truststore.password `

_Optional_. The password of the trust store.

If not set, checking the integrity of the trust store file configured will not be possible.

Example:

```xml
<param name="encryption.truststore.password">truststore-password</param>
```

##### `encryption.truststore.type`

_Optional_. The type of the trust store. Can be one of the following:

- `JKS`
- `PKCS12`

Default: JKS

Example:

```xml
<param name="encryption.truststore.type">PKCS12</param>
```

##### `encryption.hostname.verification.enable`

_Optional_. Enable hostname verification. Can be one of the following:

- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="encryption.hostname.verification.enable">true</param>
```

##### `encryption.keystore.enable`

_Optional_. Enable a key store. Can be one of the following:
- `true`
- `false`

If enabled, the following parameters configure the key store settings:

- `encryption.keystore.path`
- `encryption.keystore.password`
- `encryption.keystore.type`
- `encryption.keystore.key.password`

Default value: `false`.

Example:

```xml
<param name="encryption.keystore.enable">true</param>
```

##### `encryption.keystore.path`

(_Mandatory if key store is enabled_) The path of the key store file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector`).

Example:

```xml
<param name="encryption.keystore.path">secrets/kafka.connector.keystore.jks</param>
```

##### `encryption.keystore.password`

_Optional_. The password of the key store.

If not set, checking the integrity of the key store file configured will not be possible.

Example:

```xml
<param name="encryption.keystore.password">keystore-password</param>
```

##### `encryption.keystore.key.password`

_Optional_. The password of the private key in the key store file.

Example:

```xml
<param name="encryption.keystore.key.password">private-key-password</param>
```

##### `encryption.keystore.type`

_Optional_. The type of the key store. Can be one of the following:

- `JKS`
- `PKCS12`

Default value: `JKS`.

Example:

```xml
<param name="encryption.keystore.type">PKCS12</param>
```

##### Complete Encryption Configuration Example

The following is a complete example of how to configure encryption through the above parameters.

```xml
...
<!-- Enable encryption -->
<param name="encryption.enable">true</param>

<!-- Set general encryption settings -->
<param name="encryption.protocol">TLSv1.2</param>
<param name="encryption.enabled.protocols">TLSv1.2</param>
<param name="encryption.cipher.suites">TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA</param>
<param name="encryption.hostname.verification.enable">true</param>

<!-- If required, configure the trust store to trust the Kafka Cluster certificates -->
<param name="encryption.truststore.type">JKS</param>
<param name="encryption.truststore.path">secrets/kafka.connector.truststore.pkcs12</param></param>

<!-- If mutual TLS is enabled on the Kafka Cluster, enable and configure the key store -->
<param name="encryption.keystore.enable">true</param>
<param name="encryption.keystore.type">PKCS12</param>
<param name="encryption.keystore.path">secrets/kafka.connector.encryption.keystore.pkcs12</param>
<param name="encryption.keystore.password">schemaregistry-keystore-password</param>
<param name="encryption.keystore.key.password">schemaregistry-private-key-password</param>
...
```

#### Broker Authentication Parameters

Broker authentication is configured by parameters with the `authentication` prefix.

##### `authentication.enable`

_Optional_. Enable the authentication of this connection against the Kafka Cluster. Can be one of the following:
- `true`
- `false`

Default value: `false`.

Example:

```xml
<param name="authentication.enable">true</param>
```

##### `authentication.mechanism`

_Mandatory if [authentication](#authenticationenable) is enabled_. The SASL mechanism type. The Lightstreamer Kafka Connector supports the following authentication mechanisms:

- `PLAIN` (the default value)
- `SCRAM-256`
- `SCRAM-512`
- `GSSAPI`

In the case of `PLAIN`, `SCRAM-256`, and `SCRAM-512` mechanisms, the credentials must be configured through the following mandatory parameters (which are not allowed for `GSSAPI`):

- `authentication.username`, the username.
- `authentication.password`, the password.

###### `PLAIN`

Example:

```xml
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">PLAIN</param>
<param name="authentication.username">authorized-kafka-user</param>
<param name="authentication.password">authorized-kafka-user-password</param>
```

###### `SCRAM-256`

Example: 

```xml
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">SCRAM-256</param>
<param name="authentication.username">authorized-kafka-usee</param>
<param name="authentication.password">authorized-kafka-user-password</param>
```

###### `SCRAM-512`

Example:

```xml
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">SCRAM-512</param>
<param name="authentication.username">authorized-kafka-username</param>
<param name="authentication.password">authorized-kafka-username-password</param>
```

###### `GSSAPI`

In the case of `GSSAPI`, the following parameters will be part of the authentication configuration:

- `authentication.gssapi.key.tab.enable`

  _Optional_. Enable the use of a keytab.

  Default value: `false`.

- `authentication.gssapi.key.tab.path`

  _Mandatory if_ [key tab](#authenticationgssapikeytabenable) is enabled_. The path to the kaytab file, relative to the deployment folder (`LS_HOME/adapters/lightstreamer-kafka-connector`).

- `authentication.gssapi.store.key.enable`

  _Optional_. Enable storage of the principal key.

  Default value: `false`.

- `authentication.gssapi.kerberos.service.name`

  _Mandatory_. The name of the Kerberos service.

- `authentication.gssapi.pricipal`

  _Mandatory if [ticket cache](#authenticationgssapiticketcacheenable) is disabled .The name of the principal to be used.

- `authentication.gssapi.ticket.cache.enable`

  _Optional_. Enable the use of a ticket cache.

  Default value: `false`.

Example:

```xml
...
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">GSSAPI</param>
<param name="authentication.gssapi.key.tab.enable">true</param>
<param name="authentication.gssapi.key.tab.path">gssapi/kafka-connector.keytab</param>
<param name="authentication.gssapi.store.key.enable">true</param>
<param name="authentication.gssapi.kerberos.service.name">kafka</param>
<param name="authentication.gssapi.pricipal">kafka-connector-1@LIGHTSTREAMER.COM</param>
...
```

Example of configuration with the use of a ticket cache:

```xml
<param name="authentication.enable">true</param>
<param name="authentication.mechanism">GSSAPI</param>
<param name="authentication.gssapi.kerberos.service.name">kafka</param>
<param name="authentication.gssapi.ticket.cache.enable">true</param>
```

#### Schema Registry

...

##### `schema.registry.url`

_Mandatory_. The URL of the Confluent schema registry. An encrypted connection is enabled by specifying the `https` protocol.


Example of a plain http url:

```xml
<!-- Use https to enable secure connection to the registry
<param name="schema.registry.url">https://localhost:8081</param>
-->
<param name="schema.registry.url">http//localhost:8081</param>
```

##### Encryption Parameters

A secure connection to the Confluent Schema Registry can be configured through parameters with the `schema.registry.encryption` prefix, each one having the same meaning as the analogous parameter defined in the [Encryption Parameters](#encryption-parameters) section:

- `schema.registry.encryption.protocol` (see [encryption.protocol](#encryptionprotocol))
- `schema.registry.encryption.enabled.protocols` (see [encryption.enabled.protocols](#encryptionenabledprotocols))
- `schema.registry.encryption.cipher.suites` (see [encryption.cipher.suites](#encryptionciphersuites))
- `schema.registry.encryption.truststore.path` (see [encryption.truststore.path](#encryptiontruststorepath))
- `schema.registry.encryption.truststore.password` (see [encryption.truststore.password](#encryptiontruststorepassword))
- `schema.registry.encryption.truststore.type` (see [encryption.truststore.type](#encryptiontruststoretype))
- `schema.registry.encryption.hostname.verification.enable` (see [encryption.hostname.verification.enable](#encryptionhostnameverificationenable))
- `schema.registry.encryption.keystore.enable` (see [encryption.keystore.enable](#encryptionkeystoreenable))
- `schema.registry.encryption.keystore.path` (see [encryption.keystore.path](#encryptionkeystorepath))
- `schema.registry.encryption.keystore.password` (see [encryption.keystore.password](#encryptionkeystorepassword))
- `schema.registry.encryption.keystore.type` (see [encryption.keystore.type](#encryptionkeystoretype))
- `schema.registry.encryption.keystore.key.password` (see [encryption.keystore.key.password](#encryptionkeystorekeypassword))

Example:

```xml
<!-- Set the Confluent Schema registry URL -->
<param name="schema.registry.url">https//localhost:8081</param>

<!-- Set general encryption settings -->
<param name="schema.registry.encryption.enabled.protocols">TLSv1.3</param>
<param name="schema.registry.encryption.cipher.suites">TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA</param>
<param name="schema.registry.encryption.hostname.verification.enable">true</param>

<!-- If required, configure the trust store to trust the Confluent Schema registry certificates -->
<param name="schema.registry.encryption.truststore.type">PKCS12</param>
<param name="schema.registry.encryption.truststore.path">secrets/secrets/kafka.connector.schema.registry.truststore.pkcs12</param></param>

<!-- If mutual TLS is enabled on the Confluent Schema registry, enable and configure the key store -->
<param name="schema.registry.encryption.keystore.enable">true</param>
<param name="schema.registry.encryption.keystore.type">PKCS12</param>
<param name="schema.registry.encryption.keystore.path">secrets/kafka.connector.schema.registry.encryption.keystore.pkcs12</param>
<param name="schema.registry.encryption.keystore.password">schemaregistry-keystore-password</param>
<param name="schema.registry.encryption.keystore.key.password">schemaregistry-private-key-password</param>
```

#### Topic Mapping



##### template

Am item template instructs the Kafka Connector on how to route a subscribed item how a subscribed item is

Example:


```xml
<param name="item-template.stock">stock</param>
<param name="item-template.stock">stock</param>
```

### Metadata Adapter Customization

