# Changelog

## [1.0.6] (2024-10-10)

**Documentation**

- Updated the `manifest.json` file embedded in the Sink connector archive. ([#15](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/15))

- Updated the [README.md](README.md) and factory [adapters.xml](kafka-connector-project/kafka-connector/src/adapter/dist/adapters.xml) files to explain how to configure Basic HTTP authentication for the Schema Registry. ([#16](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/16))

## [1.0.5] (2024-09-25)

**Improvements**

- Added the `etc` folder to the Sink connector zip file. ([#14](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/14))


## [1.0.4] (2024-09-10)

**Documentation**

- Updated the [README.md](README.md) file to explain how to use either the configuration properties file or the configuration JSON file to run the Sink connector. In addition, examples of JSON files have been provided under the [kafka-connector-project/config/kafka-connect-config](kafka-connector-project/config/kafka-connect-config/) folder. ([#8](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/8))

- Consistently updated all references to Java as "JDK version 17" throughout the project. ([#9](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/9))

- Clarify the JDK version required to run the Sink connector. ([#10](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/10))

- Updated the `manifest.json` file embedded in the Sink connector archive. ([#13](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/13))

- Made some additional minor fixes. ([#12](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/12))

**Improvements**

- Introduced automatic configuration of the `client.id` consumer property when connected to Confluent Cloud. ([#11](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/11))


## [1.0.3] (2024-09-03)

**Improvements**

- Improved the offsets management and logging in the [ConsumerLoop](kafka-connector-project/kafka-connector/src/main/java/com/lightstreamer/kafka/adapters/consumers/ConsumerLoop.java) class.

- Modified the [`quickstart-producer`](examples/quickstart-producer/) sample client project to make it publishable to _GitHub Packages_.

- Deeply refactored the classes and interfaces of the `com.lightstreamer.kafka.connect` package to enhance loose coupling and facilitate testability.

- Improved the unit tests relative to the Sink connector.

**Documentation**

Modified the [README.md](README.md) file has follows:

- Added the [Supported Converters](README.md#supported-converters) section, clarifying the supported converters for the Sink connector.

- Stated the _Kafka Connect_ framework version for which the Sink connector has been developed.

- Mentioned the _Confluent Platform_ version to which the Sink connector can be deployed.

- Added the [tasks.max](README.md#tasksmax) section, clarifying the impact of the `tasks.max` property on the Sink connector configuration.

- Removed incomplete statements.

**Bug Fixes**

- The Sink connector enabled the creation of multiple tasks, despite the one-to-one relationship between ProxyAdapter and task.

- A _Converter_ configured without schema support triggered an exception, leading to an unexpected interruption of the Sink connector.


## [1.0.2] (2024-08-26)

**Improvements**

- Improved the [README.md](README.md) file by restructring the [Topic Mapping](README.md#topic-mapping) section.

**Bug Fixes**

- Fixed reference to the `com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter` class in several resource files. ([#4](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/4))


## [1.0.1] (2024-08-23)

**Improvements**

- Added the [Project Report Plugin](https://docs.gradle.org/current/userguide/project_report_plugin.html) to the [project](./kafka-connector-project/buildSrc/src/main/groovy/lightstreamer-kafka-connector.gradle#L5) to generate reports contaning information about the build.

- Added dependency on _javadoc_ task to the _build_ task in the [build.gradle](./kafka-connector-project/kafka-connector/build.gradle) file.

**Bug Fixes**

- The following configuration parameters for the Sink connector:
  - `lightstreamer.server.proxy_adapter.username` 
  - `lightstreamer.server.proxy_adapter.password`
  
  were being ignored.

- The [com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfigTest](./kafka-connector-project/kafka-connector/src/test/java/com/lightstreamer/kafka/connect/config/LightstreamerConnectorConfigTest.java) class contained a test producing unwanted output files.

- The [examples/docker-kafka-connect/README.md](./examples/docker-kafka-connect/README.md) file contained a wrong reference to the Docker image name.

- The [gradle.properties](./kafka-connector-project/gradle.properties) file contained an unsed property.

- The [README.md](README.md) file contained broken links.


## [1.0.0] (2024-08-20)

- First official public release


## [0.1.0] (2024-03-17)

- First public pre-release
