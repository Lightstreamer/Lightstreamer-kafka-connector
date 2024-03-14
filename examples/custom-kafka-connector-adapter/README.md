# Custom Kafka Connector Metadata Adapter

This project hosts a sample Gradle project you may use as a starting point to provide your implementation of the _Kafka Connector Metadata Adapter_ class.

To customize, build, and deploy an adapter implementation, follow the steps:

## Develop

Edit the [CustomKafkaConnectorAdapter.java](src/main/java/com/lightstreamer/kafka_connector/examples/CustomKafkaConnectorAdapter.java) file by implementing the required hook methods or provide your completely new custom class that must extend [com.lightstreamer.kafka_connector.adapters.pub.KafkaConnectorMetadataAdapter](https://lightstreamer.github.io/Lightstreamer-kafka-connector/javadoc/com/lightstreamer/kafka_connector/adapters/pub/KafkaConnectorMetadataAdapter.html).

> [!IMPORTANT]
> Add all required dependencies to the [build.gradle](build.gradle) file in the `dependencies` section.


## Build

Build the project with the command:
 
```sh
$ ./gradlew build
```

which generated the file `build/libs/custom-kafka-connector-adapter.jar`.

## Configure

Update the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/adapters.xml` file by editing the parameter [adapter_class](../../README.md#adapter_class) as follows:

```xml
...
<metadata_provider>
    ...
    <adapter_class>com.lightstreamer.kafka_connector.examples.CustomKafkaConnectorAdapter</adapter_class>
    ...
</metadata_provider>
...
```

## Deploy

Copy the generated jar file - along with all dependencies - under the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/lib` folder.

## Start

Start Lighststreamer Server and verify that the console log shows something similar:

```sh
12.Mar.24 16:51:36,169 < INFO> Loading Metadata Adapter for Adapter Set KafkaConnector
...
###### Custom KafkaConnector Adapter initialized ######
12.Mar.24 16:51:36,256 < INFO> Finished loading Metadata Adapter for Adapter Set KafkaConnector
...
```
