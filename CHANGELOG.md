# Changelog

## [1.0.2] (UNRELEASED)

**Improvements**

- Improved [README.md](README.md) by restructring the [Topic Mapping](README.md#topic-mapping) section.

**Bug Fixes**

- Fixed reference to the `com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter` class in several resource files. ([#4](https://github.com/Lightstreamer/Lightstreamer-kafka-connector/pull/4))

## [1.0.1] (2024-08-23)

**Improvements**

- Added the [Project Report Plugin](https://docs.gradle.org/current/userguide/project_report_plugin.html) to the [project](./kafka-connector-project/buildSrc/src/main/groovy/lightstreamer-kafka-connector.gradle#L5) to generate reports contaning information about the build.

- Added dependency on _javadoc_ task to the _build_ task in the [build.gradle](./kafka-connector-project/kafka-connector/build.gradle) file.

**Bug Fixes**

- The following configuration parameters for the Sink Connector:
  - `lightstreamer.server.proxy_adapter.username` 
  - `lightstreamer.server.proxy_adapter.password`
  
  were being ignored.

- The [com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfigTest](./kafka-connector-project/kafka-connector/src/test/java/com/lightstreamer/kafka/connect/config/LightstreamerConnectorConfigTest.java) class contained a test producing unwanted output files.

- The [examples/docker-kafka-connect/README.md](./examples/docker-kafka-connect/README.md) file contained a wrong reference to the Docker image name.

- The [gradle.properties](./kafka-connector-project/gradle.properties) file contained an unsed property.

- The [README.md](README.md) contained broken links.

## [1.0.0] (2024-08-20)

- First official public release

## [0.1.0] (2024-03-17)

- First public pre-release
