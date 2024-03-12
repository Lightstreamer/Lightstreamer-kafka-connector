# Custom Kafka Connector Metadata Adapter

This project hosts a basic gradle project you may use as a starting point to provide your implementation of the [Kafka Connector Metadata Adapter](../../docs/javadoc/com/lightstreamer/kafka_connector/adapters/pub/KafkaConnectorMetadataAdapter.html) class.

To customize and deploy an adapter implementation, follow the steps:

## Edit the Source Code

Edit the [CustomKafakConnectorAdapter.java](src/main/java/com/lightstreamer/kafka_connector/examples/CustomKafkaConnectorAdapter.java) file by implementing the required hook methods or provide your completely new custom class that must extend [com.lightstreamer.kafka_connector.adapters.pub.KafkaConnectorMetadataAdapter](../../docs/javadoc/com/lightstreamer/kafka_connector/adapters/pub/KafkaConnectorMetadataAdapter.html)

> [!IMPORTANT]
> Add all required dependencies to the [build.gradle](build.gradle) file in the `dependencies` section


## Build

Build the project with the command:
 
```sh
./gradlew build
```

which generated the file `build/libs/custom-kafka-connector-adapter.jar`.

## Configure

Update the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/adapters.xml` file by editing the parameter [adapter_class](../../README.md#adapter_class) as follows:

```xml
...
<metadata_provider>
    ...
    <adapter_class>com.lighstreamer.kafka_connector.example.CustomKafkaConnectorAdapter</adapter_class>
    ...
</metadata_provider>
...
```

## Deploy

Copy the generated jar file - along with all dependencies - under the `LS_HOME/adapters/lightstreamer-kafka-connector-<version>/lib` folder.

## Start

Start Lighststreamer Server and verify that the output log shows something similar:




